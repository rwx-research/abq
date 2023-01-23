use std::io::{self, Read};
use std::path::{Path, PathBuf};

use std::process::Stdio;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use abq_utils::exit::ExitCode;
use buffered_results::BufferedResults;
use message_buffer::{Completed, RefillStrategy};
use mux_output::MuxOutput;
use tokio::io::{Stderr, Stdout};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{self, ChildStderr, ChildStdout, Command};

use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::queue::{AssociatedTestResults, RunAlreadyCompleted};
use abq_utils::net_protocol::runners::{
    CapturedOutput, FastExit, InitSuccessMessage, ManifestMessage, NativeRunnerSpawnedMessage,
    NativeRunnerSpecification, OutOfBandError, ProtocolWitness, RawNativeRunnerSpawnedMessage,
    RawTestResultMessage, Status, TestCase, TestCaseMessage, TestResult, TestResultSpec,
    TestRuntime, ABQ_GENERATE_MANIFEST, ABQ_SOCKET,
};
use abq_utils::net_protocol::work_server::InitContext;
use abq_utils::net_protocol::workers::{
    ManifestResult, NativeTestRunnerParams, NextWork, NextWorkBundle, ReportedManifest, WorkId,
    WorkerTest,
};
use abq_utils::{atomic, net_protocol};
use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use indoc::indoc;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::mux_output::mux_output;

mod buffered_results;
mod message_buffer;
mod mux_output;

pub struct GenericTestRunner;

#[derive(Debug, Error)]
pub enum ProtocolVersionError {
    #[error("Timeout while waiting for protocol version")]
    Timeout,
    #[error("Incompatible native runner protocol version")]
    NotCompatible,
    #[error("Worker quit before sending protocol version")]
    WorkerQuit,
}

#[derive(Debug, Error)]
pub enum ProtocolVersionMessageError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Version(#[from] ProtocolVersionError),
}

#[derive(Debug)]
pub struct RunnerConnection {
    stream: TcpStream,
}

impl RunnerConnection {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
    async fn read<T: serde::de::DeserializeOwned>(&mut self) -> io::Result<T> {
        net_protocol::async_read_local(&mut self.stream).await
    }
    async fn write<T: serde::Serialize>(&mut self, msg: &T) -> io::Result<()> {
        net_protocol::async_write_local(&mut self.stream, msg).await
    }
}

#[derive(Debug)]
pub struct NativeRunnerInfo {
    protocol: ProtocolWitness,
    specification: NativeRunnerSpecification,
}

/// Opens a connection with native test runner on the given listener.
///
/// Validates that the connected native runner communicates with a compatible ABQ protocol.
/// Returns an error if the protocols are incompatible, or the process fails to establish a
/// connection in a suitable time frame.
pub async fn open_native_runner_connection(
    listener: &mut TcpListener,
    timeout: Duration,
    native_runner_died: impl Future<Output = ()>,
) -> Result<(NativeRunnerInfo, RunnerConnection), ProtocolVersionMessageError> {
    let start = Instant::now();
    let (
        NativeRunnerSpawnedMessage {
            protocol_version,
            runner_specification,
        },
        runner_conn,
    ) = tokio::select! {
        _ = tokio::time::sleep(timeout) => {
            tracing::error!(?timeout, elapsed=?start.elapsed(), "timeout");
            return Err(ProtocolVersionError::Timeout.into());
        }

        _  = native_runner_died => {
            return Err(ProtocolVersionError::WorkerQuit.into());
        }

        opt_conn = listener.accept() => {
            let (mut conn, _) = opt_conn?;

            // Messages sent/received from the runner should be done so ASAP. Since we're on a
            // local network, we don't care about packet reduction here.
            conn.set_nodelay(true)?;

            let concrete_spawned_message: RawNativeRunnerSpawnedMessage = net_protocol::async_read(&mut conn).await?;
            let spawned_message = concrete_spawned_message.into();

            (spawned_message, RunnerConnection::new(conn))
        }
    };

    match protocol_version.get_supported_witness() {
        None => Err(ProtocolVersionError::NotCompatible.into()),
        Some(witness) => {
            let runner_info = NativeRunnerInfo {
                protocol: witness,
                specification: runner_specification,
            };
            Ok((runner_info, runner_conn))
        }
    }
}

pub async fn wait_for_manifest(mut runner_conn: RunnerConnection) -> io::Result<ManifestMessage> {
    let manifest: ManifestMessage = runner_conn.read().await?;

    Ok(manifest)
}

#[derive(Debug, Error)]
enum RetrieveManifestError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    ProtocolVersion(#[from] ProtocolVersionError),
    #[error("{0}")]
    OOBError(#[from] OutOfBandError),
}

impl From<ProtocolVersionMessageError> for RetrieveManifestError {
    fn from(e: ProtocolVersionMessageError) -> Self {
        match e {
            ProtocolVersionMessageError::Io(e) => Self::Io(e),
            ProtocolVersionMessageError::Version(e) => Self::ProtocolVersion(e),
        }
    }
}

/// Retrieves the test manifest from native test runner.
#[instrument(level = "trace", skip(additional_env, working_dir))]
async fn retrieve_manifest<'a>(
    cmd: &str,
    args: &[String],
    additional_env: impl IntoIterator<Item = (&'a String, &'a String)>,
    working_dir: &Path,
    protocol_version_timeout: Duration,
) -> Result<ReportedManifest, RetrieveManifestError> {
    // One-shot the native runner. Since we set the manifest generation flag, expect exactly one
    // message to be received, namely the manifest.
    let (runner_info, manifest) = {
        let mut our_listener = TcpListener::bind("127.0.0.1:0").await?;
        let our_addr = our_listener.local_addr()?;

        let mut native_runner = process::Command::new(cmd);
        native_runner.args(args);
        native_runner.env(ABQ_SOCKET, format!("{}", our_addr));
        native_runner.env(ABQ_GENERATE_MANIFEST, "1");
        native_runner.envs(additional_env);
        native_runner.current_dir(working_dir);
        // NB(130): we'd like to surface native runner output today, but tomorrow
        // (with https://github.com/rwx-research/abq/issues/130) this should either
        // be captured or hidden behind a debug flag.
        native_runner.stdout(Stdio::inherit());
        native_runner.stderr(Stdio::inherit());

        let mut native_runner_handle = native_runner.spawn()?;

        let native_runner_died = async {
            let _ = native_runner_handle.wait().await;
        };

        // Wait for and validate the protocol version message, channel must always come first.
        let (runner_info, runner_conn) = open_native_runner_connection(
            &mut our_listener,
            protocol_version_timeout,
            native_runner_died,
        )
        .await?;

        let manifest_message = wait_for_manifest(runner_conn).await?;

        let status = native_runner_handle.wait().await?;
        debug_assert!(status.success());

        let manifest = match manifest_message {
            ManifestMessage::Success(manifest) => manifest.manifest,
            ManifestMessage::Failure(failure) => return Err(failure.error.into()),
        };

        (runner_info, manifest)
    };

    Ok(ReportedManifest {
        manifest,
        native_runner_protocol: runner_info.protocol.get_version(),
        native_runner_specification: Box::new(runner_info.specification),
    })
}

#[derive(Debug, Error)]
pub struct FailureExit(Option<i32>);
impl std::fmt::Display for FailureExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(i) => i.fmt(f),
            None => write!(f, "<unknown exit code>"),
        }
    }
}

#[derive(Debug, Error)]
pub enum GenericRunnerError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    ProtocolVersion(ProtocolVersionError),
    #[error("{0}")]
    NativeRunner(#[from] NativeTestRunnerError),
}

impl From<RetrieveManifestError> for GenericRunnerError {
    fn from(e: RetrieveManifestError) -> Self {
        match e {
            RetrieveManifestError::Io(e) => Self::Io(e),
            RetrieveManifestError::ProtocolVersion(e) => Self::ProtocolVersion(e),
            RetrieveManifestError::OOBError(e) => Self::NativeRunner(e.into()),
        }
    }
}

impl From<ProtocolVersionMessageError> for GenericRunnerError {
    fn from(e: ProtocolVersionMessageError) -> Self {
        match e {
            ProtocolVersionMessageError::Io(e) => Self::Io(e),
            ProtocolVersionMessageError::Version(e) => Self::ProtocolVersion(e),
        }
    }
}

pub type SendTestResults<'a> = &'a dyn Fn(Vec<AssociatedTestResults>) -> BoxFuture<'static, ()>;
pub type SendTestResultsBoxed<'a> =
    Box<dyn Fn(Vec<AssociatedTestResults>) -> BoxFuture<'static, ()>>;

pub type GetNextTests<'a> = &'a dyn Fn() -> BoxFuture<'static, NextWorkBundle>;

pub struct TestRunnerExit {
    pub exit_code: ExitCode,
    /// Captured stdout/stderr after all tests have been run
    pub final_captured_output: CapturedOutput,
}

impl GenericTestRunner {
    pub fn run<ShouldShutdown, SendManifest, GetInitContext>(
        worker_entity: EntityId,
        input: NativeTestRunnerParams,
        working_dir: &Path,
        polling_should_shutdown: ShouldShutdown,
        results_batch_size: u64,
        send_manifest: Option<SendManifest>,
        get_init_context: GetInitContext,
        get_next_test_bundle: GetNextTests,
        send_test_results: SendTestResults,
        debug_native_runner: bool,
    ) -> Result<TestRunnerExit, GenericRunnerError>
    where
        ShouldShutdown: Fn() -> bool,
        // TODO: make both of these async!
        SendManifest: FnMut(ManifestResult),
        GetInitContext: Fn() -> Result<InitContext, RunAlreadyCompleted>,
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(run(
            worker_entity,
            input,
            working_dir,
            polling_should_shutdown,
            results_batch_size,
            send_manifest,
            get_init_context,
            get_next_test_bundle,
            send_test_results,
            debug_native_runner,
        ))
    }
}

type ResultsSender = mpsc::Sender<AssociatedTestResults>;

async fn run<ShouldShutdown, SendManifest, GetInitContext>(
    worker_entity: EntityId,
    input: NativeTestRunnerParams,
    working_dir: &Path,
    _polling_should_shutdown: ShouldShutdown,
    results_batch_size: u64,
    send_manifest: Option<SendManifest>,
    get_init_context: GetInitContext,
    get_next_test_bundle: GetNextTests<'_>,
    send_test_results: SendTestResults<'_>,
    _debug_native_runner: bool,
) -> Result<TestRunnerExit, GenericRunnerError>
where
    ShouldShutdown: Fn() -> bool,
    SendManifest: FnMut(ManifestResult),
    GetInitContext: Fn() -> Result<InitContext, RunAlreadyCompleted>,
{
    let NativeTestRunnerParams {
        cmd,
        args,
        extra_env: additional_env,
    } = input;

    // TODO: get from runner params
    let protocol_version_timeout = Duration::from_secs(60);

    // If we need to retrieve the manifest, do that first.
    if let Some(mut send_manifest) = send_manifest {
        let manifest_or_error = retrieve_manifest(
            &cmd,
            &args,
            &additional_env,
            working_dir,
            protocol_version_timeout,
        )
        .await;

        match manifest_or_error {
            Ok(manifest) => {
                send_manifest(ManifestResult::Manifest(manifest));
            }
            Err(err) => {
                send_manifest(ManifestResult::TestRunnerError {
                    error: err.to_string(),
                });
                return Err(err.into());
            }
        }
    }

    // Now, start the test runner.
    //
    // The interface between us and the runner is described at
    //   https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-Interface-0959f5a9144741d798ac122566a3d887
    //
    // In short: the protocol is
    //
    // Queue |                       | Worker (us) |                              | Native runner
    //       |                       |             | <- recv ProtocolVersion -    |
    //       | <- send Init-Meta -   |             |                              |
    //       | - recv Init-Meta ->   |             |                              |
    //       |                       |             | - send InitMessage    ->     |
    //       |                       |             | <- recv InitSuccessMessage - |
    //       | <- send Next-Test -   |             |                              |
    //       | - recv Next-Test ->   |             |                              |
    //       |                       |             | - send TestCaseMessage ->    |
    //       |                       |             | <- recv TestResult -         |
    //       | <- send Test-Result - |             |                              |
    //       |                       |             |                              |
    //       |          ...          |             |          ...                 |
    //       |                       |             |                              |
    //       | <- send Next-Test -   |             |                              |
    //       | -   recv Done    ->   |             |      <close conn>            |
    // Queue |                       | Worker (us) |                              | Native runner
    let mut our_listener = TcpListener::bind("127.0.0.1:0").await?;
    let our_addr = our_listener.local_addr()?;

    let mut native_runner = process::Command::new(cmd);
    native_runner.args(args);
    native_runner.env(ABQ_SOCKET, format!("{}", our_addr));
    native_runner.envs(additional_env);
    native_runner.current_dir(working_dir);

    // The stdout/stderr will be multiplexed manually below.
    native_runner.stdout(Stdio::piped());
    native_runner.stderr(Stdio::piped());

    // If launching the native runner fails here, it should have failed during manifest
    // generation as well (unless this is a flakey error in the underlying test runner, channel
    // we do not attempt to handle gracefully). Since the failure during manifest generation
    // will have notified the queue, at this point, failures should just result in the worker
    // exiting silently itself.
    let mut native_runner_handle = native_runner.spawn()?;

    // Set up multiplexing of the native runner's stdout/stderr.
    // We want both standard pipes of the child to point to our (the parent's) standard pipes, and
    // to managed buffers we'll extract when an individual test completes.
    let mux_pipes = {
        let child_stdout = native_runner_handle.stdout.take().expect("just spawned");
        let child_stderr = native_runner_handle.stderr.take().expect("just spawned");
        MuxPipes::new(child_stdout, child_stderr)
    };

    let native_runner_died = async {
        let _ = native_runner_handle.wait().await;
    };

    // First, get and validate the protocol version message.
    let (runner_info, mut runner_conn) = open_native_runner_connection(
        &mut our_listener,
        protocol_version_timeout,
        native_runner_died,
    )
    .await?;

    let NativeRunnerInfo {
        protocol,
        specification: runner_spec,
    } = runner_info;

    tracing::info!(integration=?runner_spec.name, version=?runner_spec.version, "Launched native test runner");

    // Wait for the native runner to initialize, and send it the initialization context.
    //
    // If the initialization context informs us that the run is already complete, we can exit
    // immediately. While this case is possible, it is far more likely that the initialization
    // context will be material. As such, we want to spawn the native runner before fetching
    // the initialization context; that way we can pay the price of startup and
    // context-fetching in parallel.
    {
        match get_init_context() {
            Ok(InitContext { init_meta }) => {
                let init_message =
                    net_protocol::runners::InitMessage::new(protocol, init_meta, FastExit(false));
                runner_conn.write(&init_message).await?;
                let _init_success: InitSuccessMessage = runner_conn.read().await?;
            }
            Err(RunAlreadyCompleted {}) => {
                // There is nothing more for the native runner to do, so we tell it to
                // fast-exit and wait for it to terminate.
                let init_message = net_protocol::runners::InitMessage::new(
                    protocol,
                    Default::default(),
                    FastExit(true),
                );
                runner_conn.write(&init_message).await?;
                let exit_status = native_runner_handle.wait().await?;
                let final_captured_output = mux_pipes.finish().await?;
                return Ok(TestRunnerExit {
                    exit_code: exit_status.into(),
                    final_captured_output,
                });
            }
        };
    }

    let results_batch_size = results_batch_size as usize;

    // Assume that the size of the test batches we'll receive from the queue are
    // roughly the same size as the size of the batches of test results we send back.
    //
    // Note that this assumption may change over time, and we may want to tune this figure.
    // While we likely don't gain much from using a channel of unbounded size, it's important to
    // take care that we refill the channel at a frequency that avoids blocking execution on the
    // native test runner.
    let (tests_tx, mut tests_rx) =
        message_buffer::channel(results_batch_size, RefillStrategy::HalfConsumed);

    let fetch_tests_task = {
        let fetch_next_bundle = || async {
            let NextWorkBundle(bundle) = get_next_test_bundle().await;

            let recv_size = bundle.len();

            // NB: we can get rid of this allocation by returning the filter directly, and a word
            // for the length of the list after filtering. The allocation doesn't matter though.
            let filtered_bundle: Vec<_> = bundle
                .into_iter()
                .filter_map(|test| test.into_test())
                .collect();

            // Completed if the bundle contained `EndOfWork` markers, or if there were no tests to
            // begin with!
            let completed = filtered_bundle.len() < recv_size || filtered_bundle.is_empty();

            (filtered_bundle, Completed(completed))
        };

        tests_tx.start(fetch_next_bundle)
    };

    let (results_tx, mut results_rx) = mpsc::channel(results_batch_size * 2);

    let send_results_task = async {
        let mut pending_results = BufferedResults::new(results_batch_size, send_test_results);

        while let Some(test_result) = results_rx.recv().await {
            pending_results.push(test_result).await;
        }

        pending_results.flush().await;
    };

    let run_tests_task = async {
        while let Some(WorkerTest { test_case, work_id }) = tests_rx.recv().await {
            let estimated_start = Instant::now();

            let handled_test = handle_one_test(
                worker_entity,
                &mut runner_conn,
                &mux_pipes,
                work_id,
                test_case,
                &results_tx,
            )
            .await?;

            let estimated_runtime = estimated_start.elapsed();

            if let Err((work_id, native_error)) = handled_test {
                let remaining_tests = tests_rx.flush().await;
                let final_output = mux_pipes.finish().await?;

                handle_native_runner_failure(
                    worker_entity,
                    send_test_results,
                    &native_runner,
                    final_output,
                    native_runner_handle,
                    estimated_runtime,
                    work_id,
                    remaining_tests,
                );

                return Err(GenericRunnerError::from(native_error));
            }
        }

        drop(results_tx);
        drop(runner_conn);
        drop(our_listener);
        let exit_status = native_runner_handle.wait().await?;

        // TODO: write after-all standard output/error to stdout/stderr when we stop piping output
        // through.
        let final_captured_output = mux_pipes.finish().await?;

        Ok(TestRunnerExit {
            exit_code: ExitCode::from(exit_status),
            final_captured_output,
        })
    };

    let ((), run_tests_result, ()) =
        tokio::join!(fetch_tests_task, run_tests_task, send_results_task);

    let test_runner_exit = run_tests_result?;

    Ok(test_runner_exit)
}

#[derive(Debug, Error)]
pub enum NativeTestRunnerError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    OOBError(#[from] OutOfBandError),
}

struct MuxPipes {
    stdout: MuxOutput<ChildStdout, Stdout>,
    stderr: MuxOutput<ChildStderr, Stderr>,
}

impl MuxPipes {
    fn new(child_stdout: ChildStdout, child_stderr: ChildStderr) -> Self {
        let our_stdout: Stdout = tokio::io::stdout();
        let our_stderr: Stderr = tokio::io::stderr();
        Self {
            stdout: mux_output(child_stdout, our_stdout),
            stderr: mux_output(child_stderr, our_stderr),
        }
    }

    fn get_captured(&self) -> (Vec<u8>, Vec<u8>) {
        // NB: if we ever measure this to be compute-expensive, consider making `end_capture` async
        let stdout = self.stdout.side_channel.get_captured();
        let stderr = self.stderr.side_channel.get_captured();

        (stdout, stderr)
    }

    async fn finish(self) -> io::Result<CapturedOutput> {
        let MuxOutput {
            copied_all_output: copied_stdout,
            side_channel: side_stdout,
            ..
        } = self.stdout;
        let MuxOutput {
            copied_all_output: copied_stderr,
            side_channel: side_stderr,
            ..
        } = self.stderr;
        copied_stdout.await.unwrap()?;
        copied_stderr.await.unwrap()?;
        let stdout = side_stdout
            .finish()
            .expect("channel reference must be unique at this point");
        let stderr = side_stderr
            .finish()
            .expect("channel reference must be unique at this point");
        Ok(CapturedOutput { stderr, stdout })
    }
}

async fn handle_one_test(
    worker_entity: EntityId,
    runner_conn: &mut RunnerConnection,
    mux_pipes: &MuxPipes,
    work_id: WorkId,
    test_case: TestCase,
    results_chan: &ResultsSender,
) -> Result<Result<(), (WorkId, NativeTestRunnerError)>, GenericRunnerError> {
    let test_case_message = TestCaseMessage::new(test_case);

    let opt_test_results = send_and_wait_for_test_results(
        worker_entity,
        work_id,
        runner_conn,
        mux_pipes,
        test_case_message,
    )
    .await;

    match opt_test_results {
        Ok(test_results) => {
            let send_to_chan_result = results_chan.send(test_results).await;

            if let Err(se) = send_to_chan_result {
                let work_id = se.0.work_id;
                tracing::error!(?work_id, "results channel closed prematurely");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "results channel closed prematurely, likely do to a previous failure to send test results across a network"
                ).into());
            }

            Ok(Ok(()))
        }
        Err((work_id, err)) => Ok(Err((work_id, err.into()))),
    }
}

async fn send_and_wait_for_test_results(
    worker_entity: EntityId,
    work_id: WorkId,
    runner_conn: &mut RunnerConnection,
    mux_pipes: &MuxPipes,
    test_case_message: TestCaseMessage,
) -> Result<AssociatedTestResults, (WorkId, io::Error)> {
    macro_rules! bail {
        ($e:expr) => {
            match $e {
                Ok(r) => r,
                Err(e) => return Err((work_id, e)),
            }
        };
    }

    // Grab the "before-any-test" output.
    let (before_test_stdout, before_test_stderr) = mux_pipes.get_captured();
    let before_any_test = CapturedOutput {
        stderr: before_test_stderr,
        stdout: before_test_stdout,
    };

    // Prime the "after-all-tests" output.
    // Today, this is empty by default, since in general we associate output outside of a test to
    // the "before" output of the next test to run.
    // However, when we receive incremental test results, output between the last test and the
    // `Done` message can be considered "after-all-tests" output.
    let mut after_test_captures = None;

    bail!(runner_conn.write(&test_case_message).await);
    let raw_msg: RawTestResultMessage = bail!(runner_conn.read().await);

    use net_protocol::runners::{
        IncrementalTestResultStep, RawIncrementalTestResultMessage, TestResultSet,
    };

    // stop capturing stdout after we receive a test result message, since it necessarily
    // corresponds to at least one test result notification
    let (mut result_stdout, mut result_stderr) = mux_pipes.get_captured();

    let results = match raw_msg.into_test_results(worker_entity) {
        TestResultSet::All(mut results) => {
            attach_pipe_output_to_test_results(&mut results, result_stdout, result_stderr);
            results
        }
        TestResultSet::Incremental(mut step) => {
            // We need to poll until we get a marker that all test results are done.
            let mut results = Vec::with_capacity(4);
            loop {
                use IncrementalTestResultStep::*;
                match step {
                    One(mut res) => {
                        // Add the stdout/stderr for the last incremental result.
                        attach_pipe_output_to_test_result(&mut res, result_stdout, result_stderr);
                        results.push(res);

                        // Wait for the next incremental result.
                        let raw_increment: RawIncrementalTestResultMessage =
                            bail!(runner_conn.read().await);

                        // Get the captured output for the test result that just completed, in
                        // `raw_increment`.
                        (result_stdout, result_stderr) = mux_pipes.get_captured();

                        step = raw_increment.into_step(worker_entity);
                    }
                    Done(opt_res) => {
                        if let Some(mut result) = opt_res {
                            attach_pipe_output_to_test_result(
                                &mut result,
                                result_stdout,
                                result_stderr,
                            );
                            results.push(result);
                        } else {
                            after_test_captures = Some(CapturedOutput {
                                stderr: result_stderr,
                                stdout: result_stdout,
                            });
                        }
                        break;
                    }
                }
            }
            results
        }
    };

    Ok(AssociatedTestResults {
        work_id,
        results,
        before_any_test,
        after_all_tests: after_test_captures,
    })
}

fn attach_pipe_output_to_test_results(
    test_results: &mut [TestResult],
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) {
    // Happier path: exactly one test result, we can hand over the output uniquely.
    // TODO: can we pass stdout/stderr to `TestResult` as borrowed for the purposes of sending?
    match test_results {
        [tr] => attach_pipe_output_to_test_result(tr, stdout, stderr),
        _ => {
            // NB: when a manifest test returns multiple test results, we currently cannot
            // distinguish what stdout belongs to what test!
            // Consider protocol-level support for this, with aid from native runners.
            for tr in test_results {
                attach_pipe_output_to_test_result(tr, stdout.clone(), stderr.clone());
            }
        }
    }
}

fn attach_pipe_output_to_test_result(
    test_result: &mut TestResult,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) {
    test_result.stdout = Some(stdout);
    test_result.stderr = Some(stderr);
}

const INDENT: &str = "    ";

#[allow(clippy::format_push_string)] // write! can fail, push can't
#[allow(unused)] // NB(130): will once again become relevant with https://github.com/rwx-research/abq/issues/130
fn format_failed_fd(fd: Option<impl Read>, writer: &mut String, channel: &str) {
    let read_result = fd.map(|mut fd| {
        let mut out_buf = Vec::new();
        fd.read_to_end(&mut out_buf)
            .map(|_| String::from_utf8_lossy(&out_buf).to_string())
    });

    if let Some(Ok(out)) = read_result {
        if !out.is_empty() {
            writer.push_str(&format!(
                "\n\nHere's the standard {channel} of the command:"
            ));
            for line in out.lines() {
                writer.push_str(&format!("\n{INDENT}{line}"));
            }
        } else {
            writer.push_str(&format!("\n\nThe command had no standard {channel}."));
        }
    } else {
        writer.push_str(&format!(
            "\n\nUnfortunately, we couldn't read the full standard {channel} of the command.",
        ));
    }
}

#[allow(clippy::format_push_string)] // write! can fail, push can't
fn handle_native_runner_failure<I>(
    worker_entity: EntityId,
    send_test_result: SendTestResults,
    native_runner: &Command,
    mut final_output: CapturedOutput,
    mut native_runner_handle: process::Child,
    estimated_time_to_failure: Duration,
    failed_on: WorkId,
    remaining_work: I,
) where
    I: IntoIterator<Item = WorkerTest>,
{
    // Our connection with the native test runner failed, for some
    // reason. Consider this a fatal error, and report internal errors
    // back to the queue for the remaining test in the batch.
    let args: Vec<String> = std::iter::once(native_runner.as_std().get_program())
        .chain(native_runner.as_std().get_args())
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    tracing::error!(?args, "underlying native test runner failed");

    let remaining_work = Some(failed_on)
        .into_iter()
        .chain(remaining_work.into_iter().map(|test| test.work_id));

    let formatted_cmd = args.join(" ");

    // Let's try to figure out why the native test runner failed, and print a nice error message
    // enumerating as much of the problem as we're aware of.
    // Since this is an unexpected error with the native test runner, we want to provide the end
    // user with as much information as possible for a report or reproduction.
    let mut error_message = format!(
        indoc!(
            r#"
            -- Unexpected Test Runner Failure --

            The test command

            {}{}
            "#
        ),
        INDENT, formatted_cmd
    );
    error_message.push('\n');
    match native_runner_handle.try_wait() {
        Ok(Some(exit_status)) => {
            tracing::warn!(?exit_status, "native test runner exited before completion");

            match exit_status.code() {
                Some(code) => {
                    error_message.push_str(&format!(
                        "exited unexpectedly with code `{}` before completing all abq test requests.",
                        code,
                    ));
                }
                None => {
                    error_message.push_str("\
                        exited unexpectedly without an exit code before completing all abq test requests.\n\
                        It was likely terminated by a signal.");
                }
            }
        }
        Ok(None) | Err(_) => {
            error_message.push_str(
                "stopped communicating with its abq worker before completing all test requests.",
            );
        }
    }

    error_message.push_str("\n\n");
    error_message.push_str(&format!(
        indoc!(
            r#"
            Please see the standard output/error for worker

            {}{:?}

            to see the native test runner failure.
            "#
        ),
        INDENT, worker_entity
    ));

    tracing::warn!(?error_message, "native test runner failure");

    let mut final_results = vec![];
    for work_id in remaining_work {
        let error_result = TestResult::new(
            worker_entity,
            TestResultSpec {
                status: Status::PrivateNativeRunnerError,
                id: format!("internal-error-{}", uuid::Uuid::new_v4()),
                display_name: "<internal test runner error>".to_string(),
                output: Some(error_message.clone()),
                runtime: TestRuntime::Milliseconds(estimated_time_to_failure.as_millis() as _),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        );

        final_results.push(AssociatedTestResults {
            work_id,
            results: vec![error_result],
            before_any_test: std::mem::replace(&mut final_output, CapturedOutput::empty()),
            after_all_tests: None,
        });
    }

    send_test_result(final_results);
}

/// Executes a native test runner in an end-to-end fashion from the perspective of an ABQ worker.
/// Returns the manifest and all test results.
pub fn execute_wrapped_runner(
    native_runner_params: NativeTestRunnerParams,
    working_dir: PathBuf,
) -> Result<(ReportedManifest, Vec<Vec<TestResult>>), GenericRunnerError> {
    let mut manifest_message = None;

    // Currently, an atomic mutex is used here because `send_manifest`, `get_next_test`, and the
    // main thread all need access to manifest's memory location. We can get away with unsafe code
    // here because we know that the manifest must come in before `get_next_test` will be called,
    // and moreover, `send_manifest` will never be called again. But to avoid bugs, we don't do
    // that for now.
    let flat_manifest = Arc::new(Mutex::new(None));
    let mut opt_error_cell = None;

    let test_results = Arc::new(Mutex::new(vec![]));

    let send_manifest = {
        let flat_manifest = Arc::clone(&flat_manifest);
        let manifest_message = &mut manifest_message;
        let opt_error_cell = &mut opt_error_cell;
        move |manifest_result: ManifestResult| match manifest_result {
            ManifestResult::Manifest(real_manifest) => {
                let mut flat_manifest = flat_manifest.lock().unwrap();

                if manifest_message.is_some() || flat_manifest.is_some() {
                    panic!("Manifest has already been defined, but is being sent again");
                }

                *manifest_message = Some(real_manifest.clone());
                *flat_manifest = Some(real_manifest.manifest.flatten());
            }
            ManifestResult::TestRunnerError { error } => {
                *opt_error_cell = Some(error);
            }
        }
    };
    let get_init_context = || {
        Ok(InitContext {
            init_meta: Default::default(),
        })
    };

    let test_case_index = AtomicUsize::new(0);

    let get_next_test: GetNextTests = {
        let manifest = Arc::clone(&flat_manifest);
        &move || {
            loop {
                let manifest_and_data = manifest.lock().unwrap();
                let next_test = match &(*manifest_and_data) {
                    Some((manifest, _)) => manifest.get(test_case_index.load(atomic::ORDERING)),
                    None => {
                        // still waiting for the manifest, spin
                        continue;
                    }
                };
                let next = match next_test {
                    Some(test_case) => {
                        test_case_index.fetch_add(1, atomic::ORDERING);

                        NextWork::Work(WorkerTest {
                            test_case: test_case.clone(),
                            work_id: WorkId(Default::default()),
                        })
                    }
                    None => NextWork::EndOfWork,
                };
                return async { NextWorkBundle(vec![next]) }.boxed();
            }
        }
    };
    let send_test_result: SendTestResults = {
        let test_results = test_results.clone();
        &move |results| {
            let test_results = test_results.clone();
            Box::pin(async move {
                test_results
                    .lock()
                    .unwrap()
                    .extend(results.into_iter().map(|tr| tr.results))
            })
        }
    };

    let _opt_exit_error = GenericTestRunner::run(
        EntityId::new(),
        native_runner_params,
        &working_dir,
        || false,
        5,
        Some(send_manifest),
        get_init_context,
        get_next_test,
        send_test_result,
        false,
    );

    if let Some(error) = opt_error_cell {
        return Err(GenericRunnerError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            error,
        )));
    }

    let test_results = test_results.lock();

    Ok((
        manifest_message.expect("manifest never received!"),
        test_results.as_deref().unwrap().clone(),
    ))
}

#[cfg(test)]
mod test_validate_protocol_version_message {
    use std::time::Duration;

    use abq_with_protocol_version::with_protocol_version;
    use tokio::{
        net::{TcpListener, TcpStream},
        time::sleep,
    };

    use abq_utils::net_protocol::{
        self,
        runners::{
            AbqProtocolVersion, Manifest, NativeRunnerSpecification, ProtocolWitness,
            RawNativeRunnerSpawnedMessage,
        },
    };

    use super::{open_native_runner_connection, ProtocolVersionError, ProtocolVersionMessageError};

    fn legal_spawned_message(proto: ProtocolWitness) -> RawNativeRunnerSpawnedMessage {
        let protocol_version = proto.get_version();
        let runner_specification = NativeRunnerSpecification {
            name: "test".to_string(),
            version: "0.0.0".to_string(),
            test_framework: Some("rspec".to_owned()),
            test_framework_version: Some("3.12.0".to_owned()),
            language: Some("ruby".to_owned()),
            language_version: Some("3.1.2p20".to_owned()),
            host: Some(
                "ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned(),
            ),
        };
        RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification)
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn recv_and_validate_protocol_version_message() {
        let mut listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();

        let child = async {
            let mut stream = TcpStream::connect(socket_addr).await.unwrap();
            let spawned_message = legal_spawned_message(proto);
            net_protocol::async_write_local(&mut stream, &spawned_message)
                .await
                .unwrap();
        };

        let parent = open_native_runner_connection(
            &mut listener,
            Duration::from_secs(1),
            sleep(Duration::MAX),
        );

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_ok());
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn protocol_version_message_incompatible() {
        let mut listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let child = async {
            let mut stream = TcpStream::connect(socket_addr).await.unwrap();
            let protocol_version = AbqProtocolVersion {
                major: 999123123,
                minor: 12312342,
            };
            let runner_specification = NativeRunnerSpecification {
                name: "test".to_string(),
                version: "0.0.0".to_string(),
                test_framework: None,
                test_framework_version: None,
                language: None,
                language_version: None,
                host: None,
            };
            let spawned_message =
                RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification);
            net_protocol::async_write_local(&mut stream, &spawned_message)
                .await
                .unwrap();
        };

        let parent = open_native_runner_connection(
            &mut listener,
            Duration::from_secs(1),
            sleep(Duration::MAX),
        );

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ProtocolVersionMessageError::Version(ProtocolVersionError::NotCompatible)
        ));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn protocol_version_message_recv_wrong_message() {
        let mut listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let child = async {
            let mut stream = TcpStream::connect(socket_addr).await.unwrap();
            let message = Manifest::new(vec![], Default::default());
            net_protocol::async_write_local(&mut stream, &message)
                .await
                .unwrap();
        };

        let parent = open_native_runner_connection(
            &mut listener,
            Duration::from_secs(1),
            sleep(Duration::MAX),
        );

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::Io(_)));
    }

    #[tokio::test]
    async fn protocol_version_message_tunnel_dropped() {
        let mut listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let child = async {
            let stream = TcpStream::connect(socket_addr).await.unwrap();
            drop(stream);
        };

        let parent = open_native_runner_connection(
            &mut listener,
            Duration::from_secs(1),
            sleep(Duration::MAX),
        );

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::Io(_)));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn protocol_version_message_tunnel_timeout() {
        let mut listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();

        let timeout = Duration::from_millis(0);
        let child = async {
            sleep(Duration::from_millis(100)).await;
            let mut stream = TcpStream::connect(socket_addr).await.unwrap();
            let spawned_message = legal_spawned_message(proto);
            net_protocol::async_write_local(&mut stream, &spawned_message)
                .await
                .unwrap();
        };

        let parent = open_native_runner_connection(&mut listener, timeout, sleep(Duration::MAX));

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ProtocolVersionMessageError::Version(ProtocolVersionError::Timeout)
        ));
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn protocol_version_message_tunnel_connection_dies() {
        let mut listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();

        let timeout = Duration::from_millis(100);
        let child = async {
            sleep(Duration::from_millis(100)).await;
            let mut stream = TcpStream::connect(socket_addr).await.unwrap();
            let spawned_message = legal_spawned_message(proto);
            net_protocol::async_write_local(&mut stream, &spawned_message)
                .await
                .unwrap();
        };

        let parent = open_native_runner_connection(&mut listener, timeout, sleep(Duration::ZERO));

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ProtocolVersionMessageError::Version(ProtocolVersionError::WorkerQuit)
        ));
    }
}

#[cfg(test)]
#[cfg(feature = "test-abq-jest")]
mod test_abq_jest {
    use crate::{execute_wrapped_runner, GenericTestRunner, SendTestResults};
    use abq_utils::net_protocol::entity::EntityId;
    use abq_utils::net_protocol::queue::RunAlreadyCompleted;
    use abq_utils::net_protocol::runners::{AbqProtocolVersion, Status, TestCase, TestResultSpec};
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        ManifestResult, NativeTestRunnerParams, NextWork, NextWorkBundle, ReportedManifest, WorkId,
        WorkerTest,
    };
    use abq_with_protocol_version::with_protocol_version;
    use futures::FutureExt;

    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    fn npm_jest_project_path() -> PathBuf {
        PathBuf::from(std::env::var("ABQ_WORKSPACE_DIR").unwrap())
            .join("testdata/jest/npm-jest-project")
    }

    fn npm_jest_failing_project_path() -> PathBuf {
        PathBuf::from(std::env::var("ABQ_WORKSPACE_DIR").unwrap())
            .join("testdata/jest/npm-jest-project-with-failures")
    }

    fn write_leading_markers(s: &[u8]) -> String {
        String::from_utf8_lossy(s)
            .lines()
            .into_iter()
            .map(|s| format!("|{s}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn get_manifest() {
        let input = NativeTestRunnerParams {
            cmd: "npm".to_string(),
            args: vec!["test".to_string()],
            extra_env: Default::default(),
        };

        let mut manifest = None;
        let test_results = Arc::new(Mutex::new(vec![]));

        let send_manifest = |real_manifest| manifest = Some(real_manifest);
        let get_init_context = || {
            Ok(InitContext {
                init_meta: Default::default(),
            })
        };
        let get_next_test = &|| async { NextWorkBundle(vec![NextWork::EndOfWork]) }.boxed();
        let send_test_result: SendTestResults = {
            let test_results = test_results.clone();
            &move |results| {
                let test_results = test_results.clone();
                Box::pin(async move {
                    test_results
                        .lock()
                        .unwrap()
                        .push(results.into_iter().map(|tr| tr.results))
                })
            }
        };

        GenericTestRunner::run(
            EntityId::new(),
            input,
            &npm_jest_project_path(),
            || false,
            5,
            Some(send_manifest),
            get_init_context,
            get_next_test,
            send_test_result,
            false,
        )
        .unwrap();

        assert!(test_results.lock().unwrap().is_empty());

        let ReportedManifest {
            mut manifest,
            native_runner_protocol,
            native_runner_specification,
        } = match manifest.unwrap() {
            ManifestResult::Manifest(man) => man,
            ManifestResult::TestRunnerError { .. } => unreachable!(),
        };

        assert_eq!(native_runner_protocol, AbqProtocolVersion::V0_2);
        assert_eq!(native_runner_specification.name, "jest-abq");

        manifest.sort();

        insta::assert_json_snapshot!(manifest);
    }

    #[test]
    fn get_manifest_and_run_tests() {
        let working_dir = npm_jest_project_path();
        let input = NativeTestRunnerParams {
            cmd: "npm".to_string(),
            args: vec!["test".to_string()],
            extra_env: Default::default(),
        };

        let (_, test_results) = execute_wrapped_runner(input, working_dir).unwrap();

        let mut test_results: Vec<TestResultSpec> = test_results
            .into_iter()
            .flatten()
            .map(|result| result.into_spec())
            .collect();
        test_results.sort_by_key(|r| r.id.clone());

        assert_eq!(test_results.len(), 2, "{:#?}", test_results);

        assert_eq!(test_results[0].status, Status::Success);
        assert!(
            test_results[0].id.ends_with("mona + lisa"),
            "{:?}",
            &test_results
        );

        assert_eq!(test_results[1].status, Status::Success);
        assert!(
            test_results[1].id.ends_with("three names"),
            "{:?}",
            &test_results
        );
    }

    #[test]
    fn get_manifest_and_run_tests_with_stdout() {
        let working_dir = npm_jest_failing_project_path();
        let input = NativeTestRunnerParams {
            cmd: "npm".to_string(),
            args: vec!["test".to_string()],
            extra_env: vec![("TERM".to_string(), "dumb".to_string())]
                .into_iter()
                .collect(),
        };

        let (_, test_results) = execute_wrapped_runner(input, working_dir).unwrap();

        let mut test_results: Vec<TestResultSpec> = test_results
            .into_iter()
            .flatten()
            .map(|result| result.into_spec())
            .collect();
        test_results.sort_by_key(|r| r.id.clone());

        assert_eq!(test_results.len(), 2, "{:#?}", test_results);

        {
            assert!(matches!(test_results[0].status, Status::Failure { .. }));
            assert!(test_results[0].id.ends_with("1 + 2"));
            assert!(
                matches!(&test_results[0].stderr, Some(s) if s.is_empty()),
                "{:?}",
                &test_results[0].stderr
            );

            let stdout = test_results[0].stdout.as_deref().unwrap();
            let stdout = write_leading_markers(stdout);

            insta::assert_snapshot!(stdout, @r###"
            |  console.log
            |    hello from a second jest test
            |
            |      at Object.log (add2.test.js:4:11)
            |
            "###);
        }

        {
            assert!(matches!(test_results[1].status, Status::Failure { .. }));
            assert!(test_results[1].id.ends_with("mona + lisa"));
            assert!(
                matches!(&test_results[0].stderr, Some(s) if s.is_empty()),
                "{:?}",
                String::from_utf8_lossy(test_results[1].stderr.as_ref().unwrap())
            );

            let stdout = test_results[1].stdout.as_deref().unwrap();
            let stdout = write_leading_markers(stdout);

            insta::assert_snapshot!(stdout, @r###"
            |  console.log
            |    hello from a first jest test
            |
            |      at Object.log (add.test.js:4:11)
            |
            "###);
        }
    }

    #[test]
    #[with_protocol_version]
    fn quick_exit_if_init_context_says_run_is_complete() {
        let input = NativeTestRunnerParams {
            cmd: "npm".to_string(),
            args: vec!["test".to_string()],
            extra_env: Default::default(),
        };

        let get_init_context = || Err(RunAlreadyCompleted {});
        let get_next_test = &move || {
            async move {
                NextWorkBundle(vec![NextWork::Work(WorkerTest {
                    test_case: TestCase::new(proto, "unreachable", Default::default()),
                    work_id: WorkId::new(),
                })])
            }
            .boxed()
        };
        let send_test_result: SendTestResults = &|_| Box::pin(async {});

        let runner_result = GenericTestRunner::run::<_, fn(ManifestResult), _>(
            EntityId::new(),
            input,
            &npm_jest_project_path(),
            || false,
            5,
            None,
            get_init_context,
            get_next_test,
            send_test_result,
            false,
        );

        assert!(runner_result.is_ok());
    }
}

#[cfg(test)]
mod test_invalid_command {
    use crate::{GenericRunnerError, GenericTestRunner, SendTestResults};
    use abq_utils::net_protocol::entity::EntityId;
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        ManifestResult, NativeTestRunnerParams, NextWork, NextWorkBundle,
    };
    use futures::FutureExt;

    #[test]
    fn invalid_command_yields_error() {
        let input = NativeTestRunnerParams {
            cmd: "__zzz_not_a_command__".to_string(),
            args: vec![],
            extra_env: Default::default(),
        };

        let mut manifest_result = None;

        let send_manifest = |result| manifest_result = Some(result);
        let get_init_context = || {
            Ok(InitContext {
                init_meta: Default::default(),
            })
        };
        let get_next_test = &|| async { NextWorkBundle(vec![NextWork::EndOfWork]) }.boxed();
        let send_test_result: SendTestResults = &|_| Box::pin(async {});

        let runner_result = GenericTestRunner::run(
            EntityId::new(),
            input,
            &std::env::current_dir().unwrap(),
            || false,
            5,
            Some(send_manifest),
            get_init_context,
            get_next_test,
            send_test_result,
            false,
        );

        let manifest_result = manifest_result.unwrap();

        assert!(matches!(
            manifest_result,
            ManifestResult::TestRunnerError { .. }
        ));
        assert!(matches!(runner_result, Err(GenericRunnerError::Io(..))));
    }
}
