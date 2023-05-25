use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use std::process::Stdio;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use abq_utils::capture_output::{
    CaptureChildOutputStrategy, ChildOutputHandler, ChildOutputStrategyBox, ProcessOutput,
};
use abq_utils::error::{here, ErrorLocation, LocatedError, ResultLocation};
use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::error::FetchTestsError;
use abq_utils::oneshot_notify::{self, OneshotRx};
use abq_utils::results_handler::{ResultsHandler, StaticResultsHandler};
use abq_utils::timeout_future::TimeoutFuture;
use async_trait::async_trait;
use buffered_results::BufferedResults;
use message_buffer::RefillStrategy;

use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::process;

use abq_utils::net_protocol::entity::RunnerMeta;
use abq_utils::net_protocol::queue::{self, AssociatedTestResults, RunAlreadyCompleted, TestSpec};
use abq_utils::net_protocol::runners::{
    CapturedOutput, FastExit, InitSuccessMessage, ManifestMessage, MetadataMap,
    NativeRunnerSpawnedMessage, NativeRunnerSpecification, OutOfBandError, ProtocolWitness,
    RawNativeRunnerSpawnedMessage, RawTestResultMessage, Status, StdioOutput, TestCase,
    TestCaseMessage, TestResult, TestResultSpec, TestRunnerExit, TestRuntime,
    ABQ_GENERATE_MANIFEST, ABQ_SOCKET,
};
use abq_utils::net_protocol::work_server::InitContext;
use abq_utils::net_protocol::workers::{
    Eow, ManifestResult, NativeTestRunnerParams, NextWorkBundle, ReportedManifest, WorkId,
    WorkerTest, INIT_RUN_NUMBER,
};
use abq_utils::{atomic, log_assert_stderr, net_protocol};
use futures::future::BoxFuture;
use futures::FutureExt;
use indoc::{formatdoc, indoc};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::message_buffer::RecvMsg;

mod buffered_results;
mod message_buffer;

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
) -> Result<(NativeRunnerInfo, RunnerConnection), GenericRunnerErrorKind> {
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

macro_rules! try_setup {
    ($err:expr) => {
        $err.located(here!()).map_err(|e| GenericRunnerError {
            error: e,
            output: StdioOutput::empty(),
        })?
    };
}

pub async fn wait_for_manifest(runner_conn: &mut RunnerConnection) -> io::Result<ManifestMessage> {
    let manifest: ManifestMessage = runner_conn.read().await?;

    Ok(manifest)
}

/// Retrieves the test manifest from native test runner.
#[instrument(level = "trace", skip(native_runner))]
async fn retrieve_manifest<'a>(
    native_runner: &mut NativeRunnerHandle<'a>,
) -> Result<(ReportedManifest, ProcessOutput), GenericRunnerError> {
    // One-shot the native runner. Since we set the manifest generation flag, expect exactly one
    // message to be received, namely the manifest.
    let (manifest, stdio_output) = {
        let manifest_result = retrieve_manifest_help(native_runner).await;

        let final_stdio_output = try_setup!(native_runner.child_output.finish().await);

        match manifest_result {
            Ok(ManifestMessage::Success(manifest)) => (manifest.manifest, final_stdio_output),
            Ok(ManifestMessage::Failure(failure)) => {
                let error = NativeTestRunnerError::from(failure.error).located(here!());
                return Err(GenericRunnerError {
                    error,
                    output: final_stdio_output.into(),
                });
            }
            Err(error) => {
                return Err(GenericRunnerError {
                    error,
                    output: final_stdio_output.into(),
                });
            }
        }
    };

    let manifest = ReportedManifest {
        manifest,
        native_runner_protocol: native_runner.runner_info.protocol.get_version(),
        native_runner_specification: Box::new(native_runner.runner_info.specification.clone()),
    };
    Ok((manifest, stdio_output.combined))
}

// fetch manifest and confirm native runner quit
async fn retrieve_manifest_help(
    native_runner: &mut NativeRunnerHandle<'_>,
) -> Result<ManifestMessage, LocatedError> {
    let manifest_message = wait_for_manifest(&mut native_runner.state.conn)
        .await
        .located(here!())?;

    let status = native_runner.child.wait().await.located(here!())?;
    debug_assert!(status.success());

    Ok(manifest_message)
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
pub enum GenericRunnerErrorKind {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    ProtocolVersion(#[from] ProtocolVersionError),
    #[error("{0}")]
    NativeRunner(#[from] NativeTestRunnerError),
}

#[derive(Debug, Error)]
pub struct GenericRunnerError {
    pub error: LocatedError,
    pub output: StdioOutput,
}

impl std::fmt::Display for GenericRunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl GenericRunnerError {
    pub fn no_captures(kind: LocatedError) -> Self {
        Self {
            error: kind,
            output: StdioOutput::empty(),
        }
    }
}

/// Notifies the queue that this runner finished running all assigned tests for a given run_id.
/// This is only called if the runner actually executed tests, and after all such tests were
/// executed.
/// In cases where the runner exits before all assigned tests were completed (due to unrecoverable fault),
/// or the runner exited early due to a finished test run, this is never called.
pub type NotifyMaterialTestsAllRun = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Asynchronously fetch a bundle of tests.
#[async_trait]
pub trait TestsFetcher {
    async fn get_next_tests(&mut self) -> Result<NextWorkBundle, FetchTestsError>;
}

pub type GetNextTests = Box<dyn TestsFetcher + Send>;

#[async_trait]
pub trait ManifestSender {
    async fn send_manifest(self: Box<Self>, manifest_result: ManifestResult);
}

pub type SendManifest = Box<dyn ManifestSender + Send + Sync>;

pub type InitContextResult = Result<InitContext, RunAlreadyCompleted>;

#[async_trait]
pub trait InitContextFetcher {
    async fn get_init_context(self: Box<Self>) -> io::Result<InitContextResult>;
}

pub type GetInitContext = Box<dyn InitContextFetcher + Send>;

#[async_trait]
pub trait CancelNotifier: Send {
    async fn cancel(self: Box<Self>);
}

pub type NotifyCancellation = Box<dyn CancelNotifier>;

pub type Outcome = Result<TestRunnerExit, GenericRunnerError>;

pub fn run_sync(
    runner_meta: RunnerMeta,
    input: NativeTestRunnerParams,
    protocol_version_timeout: Duration,
    test_timeout: Duration,
    working_dir: PathBuf,
    shutdown_immediately: OneshotRx,
    results_batch_size: u64,
    send_manifest: Option<SendManifest>,
    get_init_context: GetInitContext,
    get_next_test_bundle: GetNextTests,
    results_handler: ResultsHandler,
    notify_all_tests_run: NotifyMaterialTestsAllRun,
    notify_cancellation: NotifyCancellation,
    child_output_strategy: ChildOutputStrategyBox,
    debug_native_runner: bool,
) -> Result<TestRunnerExit, GenericRunnerError> {
    let rt = try_setup!(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build());

    rt.block_on(run_async(
        runner_meta,
        input,
        protocol_version_timeout,
        test_timeout,
        working_dir,
        shutdown_immediately,
        results_batch_size,
        send_manifest,
        get_init_context,
        get_next_test_bundle,
        results_handler,
        notify_all_tests_run,
        notify_cancellation,
        child_output_strategy,
        debug_native_runner,
    ))
}

#[derive(Clone, Copy)]
struct NativeRunnerArgs<'a> {
    program: &'a str,
    args: &'a [String],
    env: &'a HashMap<String, String>,
    working_dir: &'a Path,
}

struct NativeRunnerState {
    child: process::Child,
    child_output: ChildOutputHandler,
    previous_child_output: Option<CapturedOutput>,

    // Needed to keep the port alive.
    _tcp_listener: TcpListener,
    conn: RunnerConnection,
    runner_info: NativeRunnerInfo,
    runner_meta: RunnerMeta,
    for_manifest_generation: bool,
}

impl NativeRunnerState {
    async fn kill(&mut self) -> io::Result<()> {
        self.child.kill().await?;
        self.child_output.close();
        Ok(())
    }
}

/// Representation of a native runner between one or more test suite runs.
struct NativeRunnerHandle<'a> {
    protocol_version_timeout: Duration,
    args: NativeRunnerArgs<'a>,
    /// The current test suite run number.
    run_number: u32,
    /// State of the native runner for the current [run_number].
    state: NativeRunnerState,
    /// How to deal with the output pipes of our native runner child.
    child_output_strategy: ChildOutputStrategyBox,
}

// Allow access to a current native runner's state directly through the handle.
impl<'a> std::ops::Deref for NativeRunnerHandle<'a> {
    type Target = NativeRunnerState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl<'a> std::ops::DerefMut for NativeRunnerHandle<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<'a> NativeRunnerHandle<'a> {
    /// Create a new handle to manage one or more native runner instances across one or more
    /// runs of a test suite.
    async fn new(
        args: NativeRunnerArgs<'a>,
        should_generate_manifest: bool,
        protocol_version_timeout: Duration,
        runner_meta: RunnerMeta,

        child_output_strategy: ChildOutputStrategyBox,
    ) -> Result<NativeRunnerHandle<'a>, GenericRunnerError> {
        let run_number = INIT_RUN_NUMBER;
        let native_runner_state = Self::new_native_runner(
            args,
            should_generate_manifest,
            false, // is not a retry at this point
            protocol_version_timeout,
            runner_meta,
            &child_output_strategy,
            None,
        )
        .await?;
        Ok(Self {
            protocol_version_timeout,
            args,
            run_number,
            state: native_runner_state,
            child_output_strategy,
        })
    }

    // Start a test runner.
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
    async fn new_native_runner(
        args: NativeRunnerArgs<'a>,
        should_generate_manifest: bool,
        is_retry: bool,
        protocol_version_timeout: Duration,
        runner_meta: RunnerMeta,
        child_output_strategy: &ChildOutputStrategyBox,
        previous_child_output: Option<CapturedOutput>,
    ) -> Result<NativeRunnerState, GenericRunnerError> {
        let mut listener = try_setup!(TcpListener::bind("127.0.0.1:0").await);
        let our_addr = try_setup!(listener.local_addr());

        let NativeRunnerArgs {
            program,
            args,
            env,
            working_dir,
        } = args;

        let mut native_runner = process::Command::new(program);
        native_runner.args(args);
        native_runner.env(ABQ_SOCKET, format!("{}", our_addr));
        if should_generate_manifest {
            native_runner.env(ABQ_GENERATE_MANIFEST, "1");
        }
        native_runner.envs(env);
        native_runner.current_dir(working_dir);

        // The stdout/stderr will be captured manually below.
        native_runner.stdout(Stdio::piped());
        native_runner.stderr(Stdio::piped());

        // Make sure that the native runner is SIGKILLed if the handle is dropped.
        // In the happy path we should never reach this, but upon cancellation, we not have access
        // to the runner handle. In that case, we must make sure the runner does not hang around.
        native_runner.kill_on_drop(true);

        // If launching the native runner fails here, it should have failed during manifest
        // generation as well (unless this is a flakey error in the underlying test runner, channel
        // we do not attempt to handle gracefully). Since the failure during manifest generation
        // will have notified the queue, at this point, failures should just result in the worker
        // exiting silently itself.
        let mut child = try_setup!(native_runner.spawn());

        let child_output = child_output_strategy.create(
            child.stdout.take().expect("just spawned"),
            child.stderr.take().expect("just spawned"),
            is_retry,
        );

        // First, get and validate the protocol version message.
        let opt_open_connection_err =
            open_native_runner_connection(&mut listener, protocol_version_timeout)
                .await
                .located(here!());

        let (runner_info, runner_conn) = match opt_open_connection_err {
            Ok(r) => r,
            Err(error) => {
                let output = Self::handle_native_runner_connection_error(
                    listener,
                    child,
                    child_output,
                    protocol_version_timeout,
                )
                .await;
                return Err(GenericRunnerError { error, output });
            }
        };

        Ok(NativeRunnerState {
            child,
            child_output,
            conn: runner_conn,
            _tcp_listener: listener,
            runner_info,
            runner_meta,
            for_manifest_generation: should_generate_manifest,
            previous_child_output,
        })
    }

    /// Wait for a native runner to exit (or forcefully terminate it) after it fails to connect to
    /// ABQ, and return any final output that could be captured for it.
    async fn handle_native_runner_connection_error(
        listener: TcpListener,
        mut child: process::Child,
        mut child_output: ChildOutputHandler,
        kill_after_timeout: Duration,
    ) -> StdioOutput {
        // Make sure to drop the port the native runner was supposed to connect to.
        // If the native runner then tries to connect, they should fail.
        drop(listener);

        // The child may decide to hang around even after the communication port is closed,
        // so time it out if that happens.
        tokio::select! {
            ct = child_output.finish()  =>
            {
                ct.map(|ct| ct.into()).unwrap_or_default()
            }
            () = tokio::time::sleep(kill_after_timeout) => {
                let _ = child.kill().await.located(here!());
                Default::default()
            }
        }
    }

    async fn send_init_message(&mut self, init_context: &InitContext) -> Result<(), LocatedError> {
        let InitContext { init_meta } = init_context;
        let init_message = net_protocol::runners::InitMessage::new(
            self.state.runner_info.protocol,
            // TODO: can we get rid of clone here?
            init_meta.clone(),
            FastExit(false),
        );

        self.state
            .conn
            .write(&init_message)
            .await
            .located(here!())?;

        let _init_success: InitSuccessMessage = self.state.conn.read().await.located(here!())?;

        Ok(())
    }

    async fn send_fast_exit_message(&mut self) -> Result<ExitCode, LocatedError> {
        let init_message = net_protocol::runners::InitMessage::new(
            self.state.runner_info.protocol,
            Default::default(),
            FastExit(true),
        );
        self.state
            .conn
            .write(&init_message)
            .await
            .located(here!())?;

        self.state.conn.stream.shutdown().await.located(here!())?;
        let exit_status = self.state.child.wait().await.located(here!())?;
        Ok(exit_status.into())
    }

    /// If the currently-configured native runner was created for manifest generation, re-spawn it;
    /// otherwise, nothing is done.
    async fn reconcile_for_start_of_run(&mut self) -> Result<(), GenericRunnerError> {
        if !self.state.for_manifest_generation {
            return Ok(());
        }

        let runner_meta = self.runner_meta;

        self.state = Self::new_native_runner(
            self.args,
            false, // don't generate manifest
            false, // this is start-of-run, not a retry
            self.protocol_version_timeout,
            runner_meta,
            &self.child_output_strategy,
            None,
        )
        .await?;

        Ok(())
    }

    /// If the run number exceeds the currently-configured native runner's run number, allocate a
    /// new native runner for the new suite run.
    async fn reconcile_run_number(
        &mut self,
        new_run_number: u32,
        init_context: &InitContext,
    ) -> Result<(), LocatedError> {
        if new_run_number == self.run_number {
            return Ok(());
        }

        debug_assert!(
            new_run_number >= self.run_number,
            "run number must monotonically increase"
        );

        // We have hit the end of what we'll communicate to the current native runner for this test
        // suite run, so shut it down and collect any remaining output.
        self.state.conn.stream.shutdown().await.located(here!())?;
        // TODO: record exit status, captures and pass it up when whole run completes.
        let _exit_status = self.state.child.wait().await.located(here!())?;
        let runner_meta = self.runner_meta;

        let captured_output = self.state.child_output.finish().await.located(here!())?;
        match &mut self.previous_child_output {
            Some(previous_output) => previous_output.append(captured_output),
            None => self.previous_child_output = Some(captured_output),
        }

        // Prime the new runner.
        let should_generate_manifest = false;
        let is_retry = true;
        let previous_child_output = self.previous_child_output.take();

        self.state = Self::new_native_runner(
            self.args,
            should_generate_manifest,
            is_retry,
            self.protocol_version_timeout,
            runner_meta,
            &self.child_output_strategy,
            previous_child_output,
        )
        .await
        .map_err(|e| e.error)?;
        self.run_number = new_run_number;

        // Send the initialization message.
        self.send_init_message(init_context)
            .await
            .located(here!())?;

        Ok(())
    }

    fn get_native_runner_info(self) -> queue::NativeRunnerInfo {
        queue::NativeRunnerInfo {
            protocol_version: self.state.runner_info.protocol.get_version(),
            specification: self.state.runner_info.specification,
        }
    }
}

/// Each native test runner spawned by a worker has the environment variable `ABQ_WORKER` set to a integer.
const ABQ_WORKER: &str = "ABQ_WORKER";

pub const DEFAULT_PROTOCOL_VERSION_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_RUNNER_TEST_TIMEOUT: Duration = Duration::from_secs(60 * 60);

pub async fn run_async(
    runner_meta: RunnerMeta,
    input: NativeTestRunnerParams,
    protocol_version_timeout: Duration,
    test_timeout: Duration,
    working_dir: PathBuf,
    shutdown_immediately: OneshotRx,
    results_batch_size: u64,
    send_manifest: Option<SendManifest>,
    get_init_context: GetInitContext,
    get_next_test_bundle: GetNextTests,
    results_handler: ResultsHandler,
    notify_all_tests_run: NotifyMaterialTestsAllRun,
    notify_cancellation: NotifyCancellation,
    child_output_strategy: ChildOutputStrategyBox,
    _debug_native_runner: bool,
) -> Result<TestRunnerExit, GenericRunnerError> {
    let NativeTestRunnerParams {
        cmd,
        args,
        extra_env: mut additional_env,
    } = input;

    additional_env.insert(
        ABQ_WORKER.to_owned(),
        runner_meta.runner.worker().to_string(),
    );

    let native_runner_args = NativeRunnerArgs {
        program: &cmd,
        args: &args,
        env: &additional_env,
        working_dir: &working_dir,
    };

    let build_runner_and_run_to_completion = async move {
        let opt_native_runner_handle = NativeRunnerHandle::new(
            native_runner_args,
            send_manifest.is_some(),
            protocol_version_timeout,
            runner_meta,
            child_output_strategy,
        )
        .await;

        run_help(
            send_manifest,
            opt_native_runner_handle,
            get_init_context,
            results_batch_size,
            get_next_test_bundle,
            results_handler,
            notify_all_tests_run,
            runner_meta,
            test_timeout,
        )
        .await
    };

    tokio::select! {
        result = build_runner_and_run_to_completion => {
            result
        }
        shutdown_result = shutdown_immediately => {
            log_assert_stderr!(shutdown_result.is_ok(), "somehow, shutdown-immediate resolved with an error prior to the native runner dying. This means the parent lost ownership of the runner prior to shutdown.");

            notify_cancellation.cancel().await;

            // TODO: is there a reasonable way we can capture the output of the native runner after
            // cancellation here?
            Ok(TestRunnerExit {
                exit_code: ExitCode::CANCELLED,
                native_runner_info: None,
                manifest_generation_output: None,
                final_stdio_output: Default::default(),
                process_output: Default::default(),
            })
        }
    }
}

async fn run_help(
    send_manifest: Option<SendManifest>,
    opt_native_runner_handle: Result<NativeRunnerHandle<'_>, GenericRunnerError>,
    get_init_context: GetInitContext,
    results_batch_size: u64,
    get_next_test_bundle: GetNextTests,
    results_handler: ResultsHandler,
    notify_all_tests_run: NotifyMaterialTestsAllRun,
    runner_meta: RunnerMeta,
    test_timeout: Duration,
) -> Result<TestRunnerExit, GenericRunnerError> {
    // If we need to retrieve the manifest, do that first.
    // If the native runner is erroring and we are generating the manigest,
    // the error will be attached to the generated manifest; otherwise, bail out.
    let mut native_runner_handle;
    let mut manifest_generation_output = None;

    if let Some(send_manifest) = send_manifest {
        let manifest_or_error = match opt_native_runner_handle {
            Ok(mut handle) => {
                // Package the native runner handle into the result, so that we can initialize it
                // prior to running all tests below.
                let retrieved = retrieve_manifest(&mut handle).await;
                retrieved.map(|result| (result, handle))
            }
            Err(err) => Err(err),
        };

        match manifest_or_error {
            Ok(((manifest, stdio_output), handle)) => {
                native_runner_handle = handle;
                manifest_generation_output = Some(stdio_output);
                send_manifest
                    .send_manifest(ManifestResult::Manifest(manifest))
                    .await;
            }
            Err(err) => {
                send_manifest
                    .send_manifest(ManifestResult::TestRunnerError {
                        error: err.error.to_string(),
                        output: err.output.clone(),
                    })
                    .await;
                return Err(err);
            }
        }
    } else {
        // No manifest to generate; populate the native runner handle or bail if needed.
        native_runner_handle = opt_native_runner_handle?;
    }

    // If we generated a manifest, now restart the native runner before we begin executing tests.
    native_runner_handle.reconcile_for_start_of_run().await?;

    let opt_err = execute_all_tests(
        &mut native_runner_handle,
        get_init_context,
        results_batch_size,
        get_next_test_bundle,
        results_handler,
        notify_all_tests_run,
        runner_meta,
        test_timeout,
    )
    .await;

    let previous_output = native_runner_handle.state.previous_child_output.take();
    let current_output = native_runner_handle
        .state
        .child_output
        .finish()
        .await
        .unwrap();
    let captured_output = match previous_output {
        Some(mut previous_output) => {
            previous_output.append(current_output);
            previous_output
        }
        None => current_output,
    };

    let CapturedOutput {
        stderr,
        stdout,
        combined,
    } = captured_output;

    match opt_err {
        Ok(exit) => Ok(TestRunnerExit {
            exit_code: exit,
            native_runner_info: Some(native_runner_handle.get_native_runner_info()),
            manifest_generation_output,
            final_stdio_output: StdioOutput { stderr, stdout },
            process_output: combined,
        }),
        Err(err) => Err(GenericRunnerError {
            error: err,
            output: StdioOutput { stderr, stdout },
        }),
    }
}

/// A message sent to the results channel. Sometimes we need to force a flush, even if we
/// haven't hit the test results buffer size.
enum ResultsMsg {
    Results(AssociatedTestResults),
    ForceFlush,
}

type ResultsChanRx = mpsc::Sender<ResultsMsg>;

async fn try_send_result_to_channel(
    work_id: Option<WorkId>,
    results_chan: &ResultsChanRx,
    msg: ResultsMsg,
) -> io::Result<()> {
    let send_to_chan_result = results_chan.send(msg).await;

    if send_to_chan_result.is_err() {
        tracing::error!(?work_id, "results channel closed prematurely");
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "results channel closed prematurely, likely due to a previous failure to send test results across a network"
        ));
    }

    Ok(())
}

async fn execute_all_tests<'a>(
    native_runner_handle: &mut NativeRunnerHandle<'a>,
    get_init_context: GetInitContext,
    results_batch_size: u64,
    test_fetcher: GetNextTests,
    results_handler: ResultsHandler,
    notify_all_tests_run: NotifyMaterialTestsAllRun,
    runner_meta: RunnerMeta,
    test_timeout: Duration,
) -> Result<ExitCode, LocatedError> {
    let NativeRunnerInfo {
        protocol: _,
        specification: runner_spec,
    } = &native_runner_handle.runner_info;

    tracing::info!(integration=?runner_spec.name, version=?runner_spec.version, "Launched native test runner");

    // Wait for the native runner to initialize, and send it the initialization context.
    //
    // If the initialization context informs us that the run is already complete, we can exit
    // immediately. While this case is possible, it is far more likely that the initialization
    // context will be material. As such, we want to spawn the native runner before fetching
    // the initialization context; that way we can pay the price of startup and
    // context-fetching in parallel.
    let init_context: InitContext;
    {
        match get_init_context.get_init_context().await.located(here!())? {
            Ok(the_init_context) => {
                init_context = the_init_context;
                native_runner_handle
                    .send_init_message(&init_context)
                    .await?;
            }
            Err(RunAlreadyCompleted { cancelled }) => {
                // There is nothing more for the native runner to do, so we tell it to
                // fast-exit and wait for it to terminate.
                let mut exit_code = native_runner_handle.send_fast_exit_message().await?;
                if cancelled {
                    exit_code = exit_code.max(ExitCode::CANCELLED);
                }
                return Ok(exit_code);
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

    let fetch_tests_task = tests_tx.start(NextBundleFetcher { test_fetcher });

    let (results_tx, mut results_rx) = mpsc::channel(results_batch_size * 2);

    let send_results_task = async {
        let mut pending_results = BufferedResults::new(results_batch_size, results_handler);

        use ResultsMsg::*;

        while let Some(msg) = results_rx.recv().await {
            match msg {
                Results(test_result) => pending_results.push(test_result).await,
                ForceFlush => pending_results.flush().await,
            }
        }

        pending_results.flush().await;
    };

    let run_tests_task = async {
        while let Some(msg) = tests_rx.recv().await {
            match msg {
                RecvMsg::Item(WorkerTest {
                    spec: TestSpec { test_case, work_id },
                    run_number,
                }) => {
                    native_runner_handle
                        .reconcile_run_number(run_number, &init_context)
                        .await
                        .located(here!())?;

                    let estimated_start = Instant::now();

                    let handled_test = handle_one_test(
                        runner_meta,
                        native_runner_handle,
                        work_id,
                        run_number,
                        test_case,
                        &results_tx,
                        test_timeout,
                    )
                    .await
                    .located(here!())?;

                    let estimated_runtime = estimated_start.elapsed();

                    if let Err((work_id, native_error)) = handled_test {
                        let remaining_tests = tests_rx.flush().await;

                        // Take the output to send over the wire by ref; this is because we'll
                        // steal the captured output when handling the error below for display
                        // in the returned overall error.
                        let _ = native_runner_handle.child.wait().await;
                        let final_output = native_runner_handle.child_output.get_captured_ref();

                        handle_native_runner_failure(
                            runner_meta,
                            &results_tx,
                            native_runner_handle.args,
                            &native_error,
                            final_output,
                            estimated_runtime,
                            work_id,
                            run_number,
                            remaining_tests,
                        )
                        .await
                        .located(here!())?;

                        // Exit with a failure notification so the queue knows not to wait for us.
                        notify_all_tests_run().await;

                        return Err(GenericRunnerErrorKind::from(native_error).located(here!()));
                    }
                }
                RecvMsg::FlushProcessed => {
                    try_send_result_to_channel(None, &results_tx, ResultsMsg::ForceFlush)
                        .await
                        .located(here!())?;
                }
            }
        }

        drop(results_tx);
        let ((), exit_status) = tokio::join!(notify_all_tests_run(), async {
            native_runner_handle
                .conn
                .stream
                .shutdown()
                .await
                .located(here!())?;
            native_runner_handle.child.wait().await.located(here!())
        });

        let exit_status = exit_status?;

        Ok(ExitCode::from(exit_status))
    };

    tracing::info!("starting execution of all tests");
    let (fetch_tests_result, run_tests_result, ()) =
        tokio::join!(fetch_tests_task, run_tests_task, send_results_task);

    fetch_tests_result.located(here!())?;
    let test_runner_exit = run_tests_result?;

    Ok(test_runner_exit)
}

struct NextBundleFetcher {
    test_fetcher: GetNextTests,
}

#[async_trait]
impl message_buffer::FetchMessages for NextBundleFetcher {
    type T = WorkerTest;
    type Iter = std::vec::IntoIter<Self::T>;

    async fn fetch(&mut self) -> Result<(Self::Iter, Eow), FetchTestsError> {
        let NextWorkBundle { work, eow } = self.test_fetcher.get_next_tests().await?;

        Ok((work.into_iter(), eow))
    }
}

#[derive(Debug, Error)]
pub enum NativeTestRunnerError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    OOBError(#[from] OutOfBandError),
    #[error("timed out running a test after {0} seconds")]
    TimedOut(u64),
}

type RunOneTestResult = Result<(), (WorkId, NativeTestRunnerError)>;

async fn handle_one_test(
    runner_meta: RunnerMeta,
    native_runner: &mut NativeRunnerState,
    work_id: WorkId,
    run_number: u32,
    test_case: TestCase,
    results_chan: &ResultsChanRx,
    test_timeout: Duration,
) -> Result<RunOneTestResult, GenericRunnerErrorKind> {
    let handle_test_task = handle_one_test_help(
        runner_meta,
        native_runner,
        work_id,
        run_number,
        test_case,
        results_chan,
    );

    match TimeoutFuture::new(handle_test_task, test_timeout)
        .wait()
        .await
    {
        Some(result) => result,
        None => {
            let native_runner_error = NativeTestRunnerError::TimedOut(test_timeout.as_secs());
            native_runner.kill().await?;
            Ok(Err((work_id, native_runner_error)))
        }
    }
}

async fn handle_one_test_help(
    runner_meta: RunnerMeta,
    native_runner: &mut NativeRunnerState,
    work_id: WorkId,
    run_number: u32,
    test_case: TestCase,
    results_chan: &ResultsChanRx,
) -> Result<RunOneTestResult, GenericRunnerErrorKind> {
    let test_case_message = TestCaseMessage::new(test_case);

    let opt_test_results = send_and_wait_for_test_results(
        runner_meta,
        native_runner,
        work_id,
        run_number,
        test_case_message,
    )
    .await;

    match opt_test_results {
        Ok(test_results) => {
            try_send_result_to_channel(
                Some(work_id),
                results_chan,
                ResultsMsg::Results(test_results),
            )
            .await?;

            Ok(Ok(()))
        }
        Err((work_id, err)) => Ok(Err((work_id, err.into()))),
    }
}

async fn send_and_wait_for_test_results(
    runner_meta: RunnerMeta,
    native_runner: &mut NativeRunnerState,
    work_id: WorkId,
    run_number: u32,
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
    let before_any_test = native_runner.child_output.get_captured();

    // Prime the "after-all-tests" output.
    // Today, this is empty by default, since in general we associate output outside of a test to
    // the "before" output of the next test to run.
    // However, when we receive incremental test results, output between the last test and the
    // `Done` message can be considered "after-all-tests" output.
    let mut after_test_captures = None;

    bail!(native_runner.conn.write(&test_case_message).await);
    let raw_msg: RawTestResultMessage = bail!(native_runner.conn.read().await);

    use net_protocol::runners::{
        IncrementalTestResultStep, RawIncrementalTestResultMessage, TestResultSet,
    };

    // stop capturing stdout after we receive a test result message, since it necessarily
    // corresponds to at least one test result notification
    let mut captured = native_runner.child_output.get_captured();

    let results = match raw_msg.into_test_results(runner_meta) {
        TestResultSet::All(mut results) => {
            attach_pipe_output_to_test_results(&mut results, captured);
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
                        attach_pipe_output_to_test_result(&mut res, captured);
                        results.push(res);

                        // Wait for the next incremental result.
                        let raw_increment: RawIncrementalTestResultMessage =
                            bail!(native_runner.conn.read().await);

                        // Get the captured output for the test result that just completed, in
                        // `raw_increment`.
                        captured = native_runner.child_output.get_captured();

                        step = raw_increment.into_step(runner_meta);
                    }
                    Done(opt_res) => {
                        if let Some(mut result) = opt_res {
                            attach_pipe_output_to_test_result(&mut result, captured);
                            results.push(result);
                        } else {
                            after_test_captures = Some(captured);
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
        run_number,
        results,
        before_any_test,
        after_all_tests: after_test_captures,
    })
}

fn attach_pipe_output_to_test_results(test_results: &mut [TestResult], captured: StdioOutput) {
    // Happier path: exactly one test result, we can hand over the output uniquely.
    // TODO: can we pass stdout/stderr to `TestResult` as borrowed for the purposes of sending?
    match test_results {
        [tr] => attach_pipe_output_to_test_result(tr, captured),
        _ => {
            // NB: when a manifest test returns multiple test results, we currently cannot
            // distinguish what stdout belongs to what test!
            // Consider protocol-level support for this, with aid from native runners.
            for tr in test_results {
                attach_pipe_output_to_test_result(tr, captured.clone());
            }
        }
    }
}

fn attach_pipe_output_to_test_result(test_result: &mut TestResult, captured: StdioOutput) {
    let StdioOutput { stderr, stdout, .. } = captured;
    test_result.stdout = Some(stdout);
    test_result.stderr = Some(stderr);
}

const INDENT: &str = "    ";

#[allow(clippy::format_push_string)] // write! can fail, push can't
async fn handle_native_runner_failure<'a, I>(
    runner_meta: RunnerMeta,
    results_chan: &ResultsChanRx,
    native_runner_args: NativeRunnerArgs<'a>,
    error: &NativeTestRunnerError,
    final_output: StdioOutput,
    estimated_time_to_failure: Duration,
    failed_on: WorkId,
    run_number: u32,
    remaining_work: I,
) -> io::Result<()>
where
    I: IntoIterator<Item = WorkerTest>,
{
    // Our connection with the native test runner failed, for some
    // reason. Consider this a fatal error, and report internal errors
    // back to the queue for the remaining test in the batch.
    let args: Vec<String> = std::iter::once(native_runner_args.program.to_string())
        .chain(native_runner_args.args.iter().cloned())
        .collect();

    tracing::error!(?args, "underlying native test runner failed");

    let remaining_work = Some(failed_on)
        .into_iter()
        .chain(remaining_work.into_iter().map(|test| test.spec.work_id));

    let formatted_cmd = args.join(" ");

    // Let's try to figure out why the native test runner failed, and print a nice error message
    // enumerating as much of the problem as we're aware of.
    // Since this is an unexpected error with the native test runner, we want to provide the end
    // user with as much information as possible for a report or reproduction.
    let mut error_message = match error {
        NativeTestRunnerError::Io(_) | NativeTestRunnerError::OOBError(_) => {
            formatdoc! {
                r#"
                -- Unexpected Test Runner Failure --

                The test command

                {}{}

                stopped communicating with its abq worker before completing all test requests.
                "#,
                INDENT, formatted_cmd
            }
        }
        NativeTestRunnerError::TimedOut(seconds) => {
            formatdoc! {
                r#"
                -- Test Timeout --

                The test command

                {}{}

                timed out while running a test after {} seconds.
                "#,
                INDENT, formatted_cmd, seconds
            }
        }
    };

    error_message.push('\n');
    error_message.push_str(&format!(
        indoc!(
            r#"
            Here's the standard output/error we found for the failing command.

            Stdout:

            {}

            Stderr:

            {}

            Please see {} for more details.
            "#
        ),
        String::from_utf8_lossy(&final_output.stdout),
        String::from_utf8_lossy(&final_output.stderr),
        runner_meta.runner
    ));

    tracing::warn!(?error_message, "native test runner failure");

    for work_id in remaining_work {
        let error_result = TestResult::new(
            runner_meta,
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
        let error_result = AssociatedTestResults {
            work_id,
            run_number,
            results: vec![error_result],
            before_any_test: final_output.clone(),
            after_all_tests: None,
        };

        try_send_result_to_channel(
            Some(work_id),
            results_chan,
            ResultsMsg::Results(error_result),
        )
        .await?;
    }

    Ok(())
}

/// Test harness: executes a native test runner in an end-to-end fashion from the perspective of an ABQ worker.
/// Returns the manifest and all test results.
pub fn execute_wrapped_runner(
    native_runner_params: NativeTestRunnerParams,
    working_dir: PathBuf,
) -> Result<(ReportedManifest, Vec<Vec<TestResult>>), GenericRunnerError> {
    let manifest_message = Arc::new(Mutex::new(None));

    // Currently, an atomic mutex is used here because `send_manifest`, `get_next_test`, and the
    // main thread all need access to manifest's memory location. We can get away with unsafe code
    // here because we know that the manifest must come in before `get_next_test` will be called,
    // and moreover, `send_manifest` will never be called again. But to avoid bugs, we don't do
    // that for now.
    let flat_manifest = Arc::new(Mutex::new(None));
    let opt_error_cell = Arc::new(Mutex::new(None));

    let test_results = Arc::new(Mutex::new(vec![]));

    #[allow(clippy::type_complexity)]
    struct SendManifest {
        flat_manifest: Arc<Mutex<Option<(Vec<TestSpec>, MetadataMap)>>>,
        manifest_message: Arc<Mutex<Option<ReportedManifest>>>,
        opt_error_cell: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl ManifestSender for SendManifest {
        async fn send_manifest(self: Box<Self>, manifest: ManifestResult) {
            let Self {
                flat_manifest,
                manifest_message,
                opt_error_cell,
            } = *self;
            match manifest {
                ManifestResult::Manifest(real_manifest) => {
                    let mut flat_manifest = flat_manifest.lock();
                    let mut manifest_message = manifest_message.lock();

                    if manifest_message.is_some() || flat_manifest.is_some() {
                        panic!("Manifest has already been defined, but is being sent again");
                    }

                    *manifest_message = Some(real_manifest.clone());
                    *flat_manifest = Some(real_manifest.manifest.flatten());
                }
                ManifestResult::TestRunnerError { error, output: _ } => {
                    *opt_error_cell.lock() = Some(error);
                }
            }
        }
    }

    let send_manifest = SendManifest {
        flat_manifest: flat_manifest.clone(),
        manifest_message: manifest_message.clone(),
        opt_error_cell: opt_error_cell.clone(),
    };

    struct GetInitContext;

    #[async_trait]
    impl InitContextFetcher for GetInitContext {
        async fn get_init_context(self: Box<Self>) -> io::Result<InitContextResult> {
            Ok(Ok(InitContext {
                init_meta: Default::default(),
            }))
        }
    }

    let get_init_context = GetInitContext;

    struct Fetcher {
        #[allow(clippy::type_complexity)]
        manifest: Arc<Mutex<Option<(Vec<TestSpec>, MetadataMap)>>>,
        test_case_index: AtomicUsize,
    }

    #[async_trait]
    impl TestsFetcher for Fetcher {
        async fn get_next_tests(&mut self) -> Result<NextWorkBundle, FetchTestsError> {
            loop {
                let manifest_and_data = self.manifest.lock();
                let next_test = match &(*manifest_and_data) {
                    Some((manifest, _)) => {
                        manifest.get(self.test_case_index.load(atomic::ORDERING))
                    }
                    None => {
                        // still waiting for the manifest, spin
                        continue;
                    }
                };
                match next_test {
                    Some(test_spec) => {
                        self.test_case_index.fetch_add(1, atomic::ORDERING);

                        let test = WorkerTest {
                            spec: test_spec.clone(),
                            run_number: INIT_RUN_NUMBER,
                        };
                        return Ok(NextWorkBundle::new([test], Eow(false)));
                    }
                    None => return Ok(NextWorkBundle::new([], Eow(true))),
                }
            }
        }
    }

    let test_case_index = AtomicUsize::new(0);

    let get_next_test: GetNextTests = {
        let manifest = Arc::clone(&flat_manifest);
        Box::new(Fetcher {
            manifest,
            test_case_index,
        })
    };

    let results_handler = Box::new(StaticResultsHandler::new(test_results.clone()));

    let (_shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();

    let child_output_strategy = CaptureChildOutputStrategy::new(true);

    let _opt_exit_error = run_sync(
        RunnerMeta::fake(),
        native_runner_params,
        DEFAULT_PROTOCOL_VERSION_TIMEOUT,
        DEFAULT_RUNNER_TEST_TIMEOUT,
        working_dir,
        shutdown_rx,
        5,
        Some(Box::new(send_manifest)),
        Box::new(get_init_context),
        get_next_test,
        results_handler,
        Box::new(|| async {}.boxed()),
        noop_notify_cancellation(),
        Box::new(child_output_strategy),
        false,
    );

    if let Some(error) = Arc::try_unwrap(opt_error_cell).unwrap().into_inner() {
        return Err(GenericRunnerError {
            error: io::Error::new(io::ErrorKind::InvalidInput, error).located(here!()),
            output: StdioOutput::empty(),
        });
    }

    let test_results = test_results
        .lock()
        .iter()
        .map(|t| t.results.clone())
        .collect();

    Ok((
        Arc::try_unwrap(manifest_message)
            .unwrap()
            .into_inner()
            .expect("manifest never received!"),
        test_results,
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

    use crate::GenericRunnerErrorKind;

    use super::{open_native_runner_connection, ProtocolVersionError};

    fn legal_spawned_message(proto: ProtocolWitness) -> RawNativeRunnerSpawnedMessage {
        let protocol_version = proto.get_version();
        let runner_specification = NativeRunnerSpecification {
            name: "test".to_string(),
            version: "0.0.0".to_string(),
            test_framework: "rspec".to_owned(),
            test_framework_version: "3.12.0".to_owned(),
            language: "ruby".to_owned(),
            language_version: "3.1.2p20".to_owned(),
            host: "ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned(),
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

        let parent = open_native_runner_connection(&mut listener, Duration::from_secs(1));

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
                test_framework: "".into(),
                test_framework_version: "".into(),
                language: "".into(),
                language_version: "".into(),
                host: "".into(),
            };
            let spawned_message =
                RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification);
            net_protocol::async_write_local(&mut stream, &spawned_message)
                .await
                .unwrap();
        };

        let parent = open_native_runner_connection(&mut listener, Duration::from_secs(1));

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            GenericRunnerErrorKind::ProtocolVersion(ProtocolVersionError::NotCompatible)
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

        let parent = open_native_runner_connection(&mut listener, Duration::from_secs(1));

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, GenericRunnerErrorKind::Io(..)));
    }

    #[tokio::test]
    async fn protocol_version_message_tunnel_dropped() {
        let mut listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let child = async {
            let stream = TcpStream::connect(socket_addr).await.unwrap();
            drop(stream);
        };

        let parent = open_native_runner_connection(&mut listener, Duration::from_secs(1));

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, GenericRunnerErrorKind::Io(_)));
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

        let parent = open_native_runner_connection(&mut listener, timeout);

        let (_, result) = futures::join!(child, parent);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            GenericRunnerErrorKind::ProtocolVersion(ProtocolVersionError::Timeout)
        ));
    }
}

pub struct ImmediateTests {
    tests: Vec<NextWorkBundle>,
}

impl ImmediateTests {
    pub fn new(tests: Vec<NextWorkBundle>) -> Self {
        Self { tests }
    }
}

#[async_trait]
impl TestsFetcher for ImmediateTests {
    async fn get_next_tests(&mut self) -> Result<NextWorkBundle, FetchTestsError> {
        Ok(self.tests.remove(0))
    }
}

#[cfg(test)]
use std::sync::atomic::AtomicBool;

#[cfg(test)]
fn notify_all_tests_run() -> (Arc<AtomicBool>, NotifyMaterialTestsAllRun) {
    let all_test_run = Arc::new(AtomicBool::new(false));
    let notify_all_tests_run = {
        let all_run = all_test_run.clone();
        move || {
            async move {
                all_run.store(true, atomic::ORDERING);
            }
            .boxed()
        }
    };

    (all_test_run, Box::new(notify_all_tests_run))
}

// used for testing only
pub struct StaticManifestCollector {
    manifest: Arc<Mutex<Option<ManifestResult>>>,
}

impl StaticManifestCollector {
    pub fn new(manifest: Arc<Mutex<Option<ManifestResult>>>) -> Self {
        Self { manifest }
    }
}

#[async_trait]
impl ManifestSender for StaticManifestCollector {
    async fn send_manifest(self: Box<Self>, result: ManifestResult) {
        *self.manifest.lock() = Some(result);
    }
}

pub struct StaticGetInitContext {
    context: InitContextResult,
}

impl StaticGetInitContext {
    pub fn new(context: InitContextResult) -> Self {
        Self { context }
    }
}

#[async_trait]
impl InitContextFetcher for StaticGetInitContext {
    async fn get_init_context(self: Box<Self>) -> io::Result<InitContextResult> {
        Ok(self.context)
    }
}

pub fn noop_notify_cancellation() -> NotifyCancellation {
    struct Noop {}
    #[async_trait]
    impl CancelNotifier for Noop {
        async fn cancel(self: Box<Self>) {}
    }
    Box::new(Noop {})
}

#[cfg(test)]
#[cfg(feature = "test-abq-jest")]
mod test_abq_jest {
    use crate::{
        execute_wrapped_runner, noop_notify_cancellation, notify_all_tests_run, run_sync,
        ImmediateTests, StaticGetInitContext, StaticManifestCollector,
        DEFAULT_PROTOCOL_VERSION_TIMEOUT, DEFAULT_RUNNER_TEST_TIMEOUT,
    };
    use abq_utils::capture_output::CaptureChildOutputStrategy;
    use abq_utils::net_protocol::entity::RunnerMeta;
    use abq_utils::net_protocol::queue::{RunAlreadyCompleted, TestSpec};
    use abq_utils::net_protocol::runners::{AbqProtocolVersion, Status, TestCase, TestResultSpec};
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        Eow, ManifestResult, NativeTestRunnerParams, NextWorkBundle, ReportedManifest, WorkId,
        WorkerTest, INIT_RUN_NUMBER,
    };
    use abq_utils::results_handler::{NoopResultsHandler, StaticResultsHandler};
    use abq_utils::{atomic, oneshot_notify};
    use abq_with_protocol_version::with_protocol_version;

    use parking_lot::Mutex;
    use std::path::PathBuf;
    use std::sync::Arc;

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

        let manifest = Arc::new(Mutex::new(None));
        let test_results = Arc::new(Mutex::new(vec![]));

        let send_manifest = StaticManifestCollector {
            manifest: manifest.clone(),
        };
        let get_init_context = StaticGetInitContext {
            context: Ok(InitContext {
                init_meta: Default::default(),
            }),
        };
        let get_next_test = Box::new(ImmediateTests {
            tests: vec![NextWorkBundle::new([], Eow(true))],
        });
        let results_handler = Box::new(StaticResultsHandler::new(test_results.clone()));
        let (all_tests_run, notify_all_tests_run) = notify_all_tests_run();

        let (_shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();

        let child_output_strategy = CaptureChildOutputStrategy::new(true);

        run_sync(
            RunnerMeta::fake(),
            input,
            DEFAULT_PROTOCOL_VERSION_TIMEOUT,
            DEFAULT_RUNNER_TEST_TIMEOUT,
            npm_jest_project_path(),
            shutdown_rx,
            5,
            Some(Box::new(send_manifest)),
            Box::new(get_init_context),
            get_next_test,
            results_handler,
            notify_all_tests_run,
            noop_notify_cancellation(),
            Box::new(child_output_strategy),
            false,
        )
        .unwrap();

        assert!(test_results.lock().is_empty());

        let ReportedManifest {
            mut manifest,
            native_runner_protocol,
            native_runner_specification,
        } = match Arc::try_unwrap(manifest).unwrap().into_inner().unwrap() {
            ManifestResult::Manifest(man) => man,
            ManifestResult::TestRunnerError { .. } => unreachable!(),
        };

        assert!(all_tests_run.load(atomic::ORDERING));

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
        assert_eq!(test_results[0].id, "add.test.js#0:0", "{:?}", &test_results);

        assert_eq!(test_results[1].status, Status::Success);
        assert_eq!(
            test_results[1].id, "names.test.js#0:0",
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
            assert_eq!(test_results[0].id, "add.test.js#0:0");
            assert!(
                matches!(&test_results[0].stderr, Some(s) if s.is_empty()),
                "{:?}",
                &test_results[0].stderr
            );

            let stdout = test_results[0].stdout.as_deref().unwrap();
            let stdout = write_leading_markers(stdout);

            insta::assert_snapshot!(stdout, @r###"
            |  console.log
            |    hello from a first jest test
            |
            |      at Object.log (add.test.js:4:11)
            |
            "###);
        }

        {
            assert!(matches!(test_results[1].status, Status::Failure { .. }));
            assert_eq!(test_results[1].id, "add2.test.js#0:0");
            assert!(
                matches!(&test_results[0].stderr, Some(s) if s.is_empty()),
                "{:?}",
                String::from_utf8_lossy(test_results[1].stderr.as_ref().unwrap())
            );

            let stdout = test_results[1].stdout.as_deref().unwrap();
            let stdout = write_leading_markers(stdout);

            insta::assert_snapshot!(stdout, @r###"
            |  console.log
            |    hello from a second jest test
            |
            |      at Object.log (add2.test.js:4:11)
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

        let get_init_context = StaticGetInitContext {
            context: Err(RunAlreadyCompleted { cancelled: false }),
        };
        let get_next_test = ImmediateTests {
            tests: vec![NextWorkBundle::new(
                [WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "unreachable", Default::default()),
                        work_id: WorkId::new(),
                    },
                    run_number: INIT_RUN_NUMBER,
                }],
                Eow(false),
            )],
        };
        let results_handler = Box::new(NoopResultsHandler);

        let (all_tests_run, notify_all_tests_run) = notify_all_tests_run();

        let (_shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();

        let child_output_strategy = CaptureChildOutputStrategy::new(true);

        let runner_result = run_sync(
            RunnerMeta::fake(),
            input,
            DEFAULT_PROTOCOL_VERSION_TIMEOUT,
            DEFAULT_RUNNER_TEST_TIMEOUT,
            npm_jest_project_path(),
            shutdown_rx,
            5,
            None,
            Box::new(get_init_context),
            Box::new(get_next_test),
            results_handler,
            notify_all_tests_run,
            noop_notify_cancellation(),
            Box::new(child_output_strategy),
            false,
        );

        assert!(runner_result.is_ok());

        assert!(
            !all_tests_run.load(atomic::ORDERING),
            "fast exit should not notify completion"
        );
    }
}

#[cfg(test)]
mod test_invalid_command {
    use std::sync::Arc;

    use crate::{
        noop_notify_cancellation, notify_all_tests_run, run_async, run_sync, GenericRunnerError,
        ImmediateTests, StaticGetInitContext, StaticManifestCollector,
        DEFAULT_PROTOCOL_VERSION_TIMEOUT, DEFAULT_RUNNER_TEST_TIMEOUT,
    };
    use abq_utils::capture_output::CaptureChildOutputStrategy;
    use abq_utils::exit::ExitCode;
    use abq_utils::net_protocol::entity::RunnerMeta;
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        Eow, ManifestResult, NativeTestRunnerParams, NextWorkBundle,
    };
    use abq_utils::results_handler::NoopResultsHandler;
    use abq_utils::{atomic, oneshot_notify};
    use parking_lot::Mutex;

    #[test]
    fn invalid_command_yields_error() {
        let input = NativeTestRunnerParams {
            cmd: "__zzz_not_a_command__".to_string(),
            args: vec![],
            extra_env: Default::default(),
        };

        let manifest_result = Arc::new(Mutex::new(None));

        let send_manifest = StaticManifestCollector {
            manifest: manifest_result.clone(),
        };
        let get_init_context = StaticGetInitContext {
            context: Ok(InitContext {
                init_meta: Default::default(),
            }),
        };
        let get_next_test = ImmediateTests {
            tests: vec![NextWorkBundle::new([], Eow(false))],
        };
        let results_handler = Box::new(NoopResultsHandler);

        let (all_tests_run, notify_all_tests_run) = notify_all_tests_run();

        let (_shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();

        let child_output_strategy = CaptureChildOutputStrategy::new(true);

        let runner_result = run_sync(
            RunnerMeta::fake(),
            input,
            DEFAULT_PROTOCOL_VERSION_TIMEOUT,
            DEFAULT_RUNNER_TEST_TIMEOUT,
            std::env::current_dir().unwrap(),
            shutdown_rx,
            5,
            Some(Box::new(send_manifest)),
            Box::new(get_init_context),
            Box::new(get_next_test),
            results_handler,
            notify_all_tests_run,
            noop_notify_cancellation(),
            Box::new(child_output_strategy),
            false,
        );

        let manifest_result = Arc::try_unwrap(manifest_result)
            .unwrap()
            .into_inner()
            .unwrap();

        assert!(matches!(
            manifest_result,
            ManifestResult::TestRunnerError { .. }
        ));
        assert!(matches!(runner_result, Err(GenericRunnerError { .. })));
        assert!(
            !all_tests_run.load(atomic::ORDERING),
            "invalid command should not notify completion"
        );
    }

    #[tokio::test]
    #[ntest::timeout(1000)] // 1 second
    async fn shutdown_stops_runner_immediately() {
        let input = NativeTestRunnerParams {
            cmd: "sleep".to_string(),
            args: vec!["1000".to_string()],
            extra_env: Default::default(),
        };

        let get_init_context = StaticGetInitContext {
            context: Ok(InitContext {
                init_meta: Default::default(),
            }),
        };
        let get_next_test = ImmediateTests {
            tests: vec![NextWorkBundle::new([], Eow(false))],
        };
        let results_handler = Box::new(NoopResultsHandler);

        let (all_tests_run, notify_all_tests_run) = notify_all_tests_run();

        let (shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();

        let child_output_strategy = CaptureChildOutputStrategy::new(true);

        let run_task = run_async(
            RunnerMeta::fake(),
            input,
            DEFAULT_PROTOCOL_VERSION_TIMEOUT,
            DEFAULT_RUNNER_TEST_TIMEOUT,
            std::env::current_dir().unwrap(),
            shutdown_rx,
            1,
            None,
            Box::new(get_init_context),
            Box::new(get_next_test),
            results_handler,
            notify_all_tests_run,
            noop_notify_cancellation(),
            Box::new(child_output_strategy),
            false,
        );

        let wait_and_kill_task = async move {
            // Give the runner enough time to start up.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            shutdown_tx.notify().unwrap();
        };

        let (result, ()) = tokio::join!(run_task, wait_and_kill_task);
        let test_runner_exit = result.unwrap();
        assert_eq!(test_runner_exit.exit_code, ExitCode::CANCELLED);

        assert!(!all_tests_run.load(atomic::ORDERING));
    }
}

#[cfg(test)]
mod test_init_native_runner {
    use std::{process::Stdio, time::Duration};

    use abq_test_utils::write_to_temp;
    use abq_utils::capture_output::{CaptureChildOutputStrategy, ChildOutputStrategy};
    use tokio::{net::TcpListener, process};

    use crate::NativeRunnerHandle;

    #[tokio::test]
    async fn handle_failure_of_process_that_exits_cleanly() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut child = process::Command::new("echo")
            .arg("foo")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = CaptureChildOutputStrategy::new(false).create(
            child.stdout.take().unwrap(),
            child.stderr.take().unwrap(),
            false,
        );

        let output = NativeRunnerHandle::handle_native_runner_connection_error(
            listener,
            child,
            output,
            Duration::MAX,
        )
        .await;

        assert!(output.stderr.is_empty());
        assert_eq!(&output.stdout, b"foo\n");
    }

    #[tokio::test]
    async fn handle_failure_of_process_that_hangs() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut child = process::Command::new("yes")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = CaptureChildOutputStrategy::new(false).create(
            child.stdout.take().unwrap(),
            child.stderr.take().unwrap(),
            false,
        );

        let output = NativeRunnerHandle::handle_native_runner_connection_error(
            listener,
            child,
            output,
            Duration::ZERO,
        )
        .await;

        assert!(output.stderr.is_empty());
    }

    #[tokio::test]
    async fn handle_failure_of_process_that_breaks_only_when_port_dropped() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let js_script = write_to_temp(&indoc::formatdoc! {
            "
            const net = require('net');

            async function main() {{
              await new Promise(resolve => {{
                const sock = new net.Socket();
                sock.connect({{
                  host: '127.0.0.1',
                  port: {port},
                }});
                sock.on('connect', () => {{
                  console.log('SUCCESSFULLY CONNECTED');
                  resolve();
                }});
                sock.on('error', () => {{
                  console.log('FAILED TO CONNECT');
                  resolve();
                }});
              }});
            }}

            main();
            ",
            port=port,
        });

        let mut child = process::Command::new("node")
            .arg(js_script.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let output = CaptureChildOutputStrategy::new(false).create(
            child.stdout.take().unwrap(),
            child.stderr.take().unwrap(),
            false,
        );

        let output = NativeRunnerHandle::handle_native_runner_connection_error(
            listener,
            child,
            output,
            Duration::MAX,
        )
        .await;

        assert!(output.stderr.is_empty());
        assert_eq!(&output.stdout, b"FAILED TO CONNECT\n");
    }
}
