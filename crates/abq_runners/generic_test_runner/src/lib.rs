use std::io::{self, Read};
use std::net::TcpStream;
use std::path::{Path, PathBuf};

use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{net::TcpListener, process};

use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::queue::{AssociatedTestResult, RunAlreadyCompleted};
use abq_utils::net_protocol::runners::{
    AbqProtocolVersionMessage, InitSuccessMessage, ManifestMessage, ManifestResult, Status,
    TestCase, TestCaseMessage, TestResult, TestResultMessage, ABQ_GENERATE_MANIFEST, ABQ_SOCKET,
    ACTIVE_PROTOCOL_VERSION_MAJOR, ACTIVE_PROTOCOL_VERSION_MINOR,
};
use abq_utils::net_protocol::work_server::InitContext;
use abq_utils::net_protocol::workers::{
    NativeTestRunnerParams, NextWork, NextWorkBundle, RunId, WorkContext, WorkId,
};
use abq_utils::{flatten_manifest, net_protocol};
use indoc::indoc;
use thiserror::Error;
use tracing::instrument;

mod message_buffer;
use futures::future::BoxFuture;

pub struct GenericTestRunner;

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

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

type RunnerConnection = TcpStream;

/// Opens a connection with native test runner on the given listener.
///
/// Validates that the connected native runner communicates with a compatible ABQ protocol.
/// Returns an error if the protocols are incompatible, or the process fails to establish a
/// connection in a suitable time frame.
pub fn open_native_runner_connection(
    listener: &mut TcpListener,
    timeout: Duration,
    mut is_native_runner_alive: impl FnMut() -> bool,
) -> Result<RunnerConnection, ProtocolVersionMessageError> {
    use ProtocolVersionMessageError::*;

    listener.set_nonblocking(true).map_err(Io)?;

    let start = Instant::now();
    let (
        AbqProtocolVersionMessage {
            r#type: _,
            major,
            minor,
        },
        runner_conn,
    ) = loop {
        if start.elapsed() >= timeout {
            tracing::error!(?timeout, elapsed=?start.elapsed(), "timeout");
            return Err(ProtocolVersionError::Timeout.into());
        }

        match listener.accept() {
            Ok((mut conn, _)) => {
                let version_message: AbqProtocolVersionMessage =
                    net_protocol::read(&mut conn).map_err(Io)?;

                break (version_message, conn);
            }
            Err(err) => match err.kind() {
                io::ErrorKind::WouldBlock => {
                    if !is_native_runner_alive() {
                        return Err(ProtocolVersionError::WorkerQuit.into());
                    }
                    std::thread::sleep(POLL_WAIT_TIME);
                    continue;
                }
                _ => return Err(Io(err)),
            },
        }
    };

    if major != ACTIVE_PROTOCOL_VERSION_MAJOR || minor != ACTIVE_PROTOCOL_VERSION_MINOR {
        Err(ProtocolVersionError::NotCompatible.into())
    } else {
        Ok(runner_conn)
    }
}

pub fn wait_for_manifest(mut runner_conn: RunnerConnection) -> io::Result<ManifestMessage> {
    let manifest: ManifestMessage = net_protocol::read(&mut runner_conn)?;

    Ok(manifest)
}

#[derive(Debug, Error)]
enum RetrieveManifestError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    ProtocolVersion(#[from] ProtocolVersionError),
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
fn retrieve_manifest<'a>(
    cmd: &str,
    args: &[String],
    additional_env: impl IntoIterator<Item = (&'a String, &'a String)>,
    working_dir: &Path,
    protocol_version_timeout: Duration,
) -> Result<ManifestMessage, RetrieveManifestError> {
    // One-shot the native runner. Since we set the manifest generation flag, expect exactly one
    // message to be received, namely the manifest.
    let manifest = {
        let mut our_listener = TcpListener::bind("127.0.0.1:0")?;
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
        native_runner.stdout(process::Stdio::inherit());
        native_runner.stderr(process::Stdio::inherit());

        let mut native_runner_handle = native_runner.spawn()?;

        let is_native_runner_alive = || matches!(native_runner_handle.try_wait(), Ok(None));

        // Wait for and validate the protocol version message, channel must always come first.
        let runner_conn = open_native_runner_connection(
            &mut our_listener,
            protocol_version_timeout,
            is_native_runner_alive,
        )?;

        let manifest = wait_for_manifest(runner_conn)?;

        let status = native_runner_handle.wait()?;
        debug_assert!(status.success());

        manifest
    };

    Ok(manifest)
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

pub type SendTestResults<'a> = &'a dyn Fn(Vec<AssociatedTestResult>) -> BoxFuture<'static, ()>;

impl GenericTestRunner {
    pub fn run<ShouldShutdown, SendManifest, GetInitContext, GetNextWorkBundle>(
        worker_entity: EntityId,
        input: NativeTestRunnerParams,
        working_dir: &Path,
        _polling_should_shutdown: ShouldShutdown,
        results_batch_size: u64,
        send_manifest: Option<SendManifest>,
        get_init_context: GetInitContext,
        mut get_next_test_bundle: GetNextWorkBundle,
        send_test_results: SendTestResults,
        _debug_native_runner: bool,
    ) -> Result<(), GenericRunnerError>
    where
        ShouldShutdown: Fn() -> bool,
        SendManifest: FnMut(ManifestResult),
        GetInitContext: Fn() -> Result<InitContext, RunAlreadyCompleted>,
        GetNextWorkBundle: FnMut() -> NextWorkBundle + std::marker::Send + 'static,
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
            );

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
        let mut our_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let our_addr = our_listener.local_addr().unwrap();

        let mut native_runner = process::Command::new(cmd);
        native_runner.args(args);
        native_runner.env(ABQ_SOCKET, format!("{}", our_addr));
        native_runner.envs(additional_env);
        native_runner.current_dir(working_dir);
        // NB(130): we'd like to surface native runner output today, but tomorrow
        // (with https://github.com/rwx-research/abq/issues/130) this should either
        // be captured or hidden behind the `debug_native_runner` flag.
        native_runner.stdout(process::Stdio::inherit());
        native_runner.stderr(process::Stdio::inherit());

        // If launching the native runner fails here, it should have failed during manifest
        // generation as well (unless this is a flakey error in the underlying test runner, channel
        // we do not attempt to handle gracefully). Since the failure during manifest generation
        // will have notified the queue, at this point, failures should just result in the worker
        // exiting silently itself.
        let mut native_runner_handle = native_runner.spawn()?;
        let is_native_runner_alive = || matches!(native_runner_handle.try_wait(), Ok(None));

        // First, get and validate the protocol version message.
        let mut runner_conn = open_native_runner_connection(
            &mut our_listener,
            protocol_version_timeout,
            is_native_runner_alive,
        )?;

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
                    let init_message = net_protocol::runners::InitMessage {
                        init_meta,
                        fast_exit: false,
                    };
                    net_protocol::write(&mut runner_conn, init_message)?;
                    let InitSuccessMessage {} = net_protocol::read(&mut runner_conn)?;
                }
                Err(RunAlreadyCompleted {}) => {
                    // There is nothing more for the native runner to do, so we tell it to
                    // fast-exit and wait for it to terminate.
                    let init_message = net_protocol::runners::InitMessage {
                        init_meta: Default::default(),
                        fast_exit: true,
                    };
                    net_protocol::write(&mut runner_conn, init_message)?;
                    native_runner_handle.wait()?;
                    return Ok(());
                }
            };
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let results_batch_size = results_batch_size as usize;
        let mut pending_test_results = Vec::with_capacity(results_batch_size);

        // We establish one connection with the native runner and repeatedly send tests until we're
        // done.
        'test_all: loop {
            let NextWorkBundle(test_bundle) = get_next_test_bundle();
            let mut test_iter = test_bundle.into_iter();
            while let Some(next_test) = test_iter.next() {
                match next_test {
                    NextWork::EndOfWork => {
                        drop(runner_conn);
                        break 'test_all;
                    }
                    NextWork::Work {
                        test_case,
                        context: _,
                        run_id: _,
                        work_id,
                    } => {
                        let estimated_start = Instant::now();

                        let handled_test = handle_one_test(
                            &mut runner_conn,
                            work_id,
                            test_case,
                            &mut pending_test_results,
                        )?;

                        let estimated_runtime = estimated_start.elapsed();

                        if let Err((work_id, native_error)) = handled_test {
                            handle_native_runner_failure(
                                worker_entity,
                                send_test_results,
                                &native_runner,
                                native_runner_handle,
                                estimated_runtime,
                                work_id,
                                test_iter,
                            );
                            return Err(native_error.into());
                        }

                        if pending_test_results.len() >= results_batch_size as _ {
                            let results = std::mem::take(&mut pending_test_results);
                            pending_test_results.reserve(results_batch_size as _);
                            rt.block_on(send_test_results(results));
                        }
                    }
                }
            }
        }

        if !pending_test_results.is_empty() {
            rt.block_on(send_test_results(pending_test_results));
        }

        drop(our_listener);
        native_runner_handle.wait()?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum NativeTestRunnerError {
    #[error("{0}")]
    Io(#[from] io::Error),
}

fn handle_one_test(
    runner_conn: &mut RunnerConnection,
    work_id: WorkId,
    test_case: TestCase,
    pending_test_results: &mut Vec<AssociatedTestResult>,
) -> Result<Result<(), (WorkId, NativeTestRunnerError)>, GenericRunnerError> {
    let test_case_message = TestCaseMessage { test_case };

    let opt_test_result_cycle: Result<TestResultMessage, _> =
        net_protocol::write(runner_conn, test_case_message)
            .and_then(|_| net_protocol::read(runner_conn));

    match opt_test_result_cycle {
        Ok(TestResultMessage { test_result }) => {
            pending_test_results.push((work_id, test_result));

            Ok(Ok(()))
        }
        Err(err) => Ok(Err((work_id, err.into()))),
    }
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
    mut native_runner_handle: process::Child,
    estimated_time_to_failure: Duration,
    failed_on: WorkId,
    remaining_work: I,
) where
    I: IntoIterator<Item = NextWork>,
{
    // Our connection with the native test runner failed, for some
    // reason. Consider this a fatal error, and report internal errors
    // back to the queue for the remaining test in the batch.
    let args: Vec<String> = std::iter::once(native_runner.get_program())
        .chain(native_runner.get_args())
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    tracing::error!(?args, "underlying native test runner failed");

    let remaining_work = Some(failed_on)
        .into_iter()
        .chain(remaining_work.into_iter().filter_map(|w| match w {
            NextWork::Work { work_id, .. } => Some(work_id),
            NextWork::EndOfWork => None,
        }));

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
        let error_result = TestResult {
            status: Status::PrivateNativeRunnerError,
            id: format!("internal-error-{}", uuid::Uuid::new_v4()),
            display_name: "<internal test runner error>".to_string(),
            output: Some(error_message.clone()),
            runtime: estimated_time_to_failure.as_millis() as _,
            meta: Default::default(),
        };

        final_results.push((work_id, error_result));
    }

    send_test_result(final_results);
}

/// Executes a native test runner in an end-to-end fashion from the perspective of an ABQ worker.
/// Returns the manifest and all test results.
pub fn execute_wrapped_runner(
    native_runner_params: NativeTestRunnerParams,
    working_dir: PathBuf,
) -> Result<(ManifestMessage, Vec<TestResult>), GenericRunnerError> {
    let mut test_case_index = 0;

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
                *flat_manifest = Some(flatten_manifest(real_manifest.manifest));
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

    let get_next_test = {
        let manifest = Arc::clone(&flat_manifest);
        let working_dir = working_dir.clone();
        move || {
            loop {
                let manifest_and_data = manifest.lock().unwrap();
                let next_test = match &(*manifest_and_data) {
                    Some((manifest, _)) => manifest.get(test_case_index),
                    None => {
                        // still waiting for the manifest, spin
                        continue;
                    }
                };
                let next = match next_test {
                    Some(test_case) => {
                        test_case_index += 1;

                        NextWork::Work {
                            test_case: test_case.clone(),
                            context: WorkContext {
                                working_dir: working_dir.clone(),
                            },
                            run_id: RunId::unique(),
                            work_id: WorkId(Default::default()),
                        }
                    }
                    None => NextWork::EndOfWork,
                };
                return NextWorkBundle(vec![next]);
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
                    .extend(results.into_iter().map(|(_id, result)| result))
            })
        }
    };

    GenericTestRunner::run(
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
    )?;

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
    use std::{
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    use abq_utils::net_protocol::{
        self,
        runners::{
            AbqProtocolVersionMessage, AbqProtocolVersionTag, ACTIVE_PROTOCOL_VERSION_MAJOR,
            ACTIVE_PROTOCOL_VERSION_MINOR,
        },
    };

    use super::{open_native_runner_connection, ProtocolVersionError, ProtocolVersionMessageError};

    #[test]
    fn recv_and_validate_protocol_version_message() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let version_message = AbqProtocolVersionMessage {
                r#type: AbqProtocolVersionTag::AbqProtocolVersion,
                major: ACTIVE_PROTOCOL_VERSION_MAJOR,
                minor: ACTIVE_PROTOCOL_VERSION_MINOR,
            };
            net_protocol::write(&mut stream, version_message).unwrap();
        })
        .join()
        .unwrap();

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1), || true);

        assert!(result.is_ok());
    }

    #[test]
    fn protocol_version_message_incompatible() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let version_message = AbqProtocolVersionMessage {
                r#type: AbqProtocolVersionTag::AbqProtocolVersion,
                major: 999123123,
                minor: 12312342,
            };
            net_protocol::write(&mut stream, version_message).unwrap();
        })
        .join()
        .unwrap();

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1), || true);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ProtocolVersionMessageError::Version(ProtocolVersionError::NotCompatible)
        ));
    }

    #[test]
    fn protocol_version_message_recv_wrong_message() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let message = net_protocol::runners::Manifest {
                members: vec![],
                init_meta: Default::default(),
            };
            net_protocol::write(&mut stream, message).unwrap();
        })
        .join()
        .unwrap();

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1), || true);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::Io(_)));
    }

    #[test]
    fn protocol_version_message_tunnel_dropped() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let stream = TcpStream::connect(socket_addr).unwrap();
            drop(stream);
        })
        .join()
        .unwrap();

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1), || true);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::Io(_)));
    }

    #[test]
    fn protocol_version_message_tunnel_timeout() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();

        let timeout = Duration::from_millis(0);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let version_message = AbqProtocolVersionMessage {
                r#type: AbqProtocolVersionTag::AbqProtocolVersion,
                major: ACTIVE_PROTOCOL_VERSION_MAJOR,
                minor: ACTIVE_PROTOCOL_VERSION_MINOR,
            };
            net_protocol::write(&mut stream, version_message).unwrap();
        })
        .join()
        .unwrap();

        let result = open_native_runner_connection(&mut listener, timeout, || true);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ProtocolVersionMessageError::Version(ProtocolVersionError::Timeout)
        ));
    }

    #[test]
    fn protocol_version_message_tunnel_connection_dies() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();

        let timeout = Duration::from_millis(100);
        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let version_message = AbqProtocolVersionMessage {
                r#type: AbqProtocolVersionTag::AbqProtocolVersion,
                major: ACTIVE_PROTOCOL_VERSION_MAJOR,
                minor: ACTIVE_PROTOCOL_VERSION_MINOR,
            };
            net_protocol::write(&mut stream, version_message).unwrap();
        });

        let result = open_native_runner_connection(&mut listener, timeout, || false);

        // join blocks so we have to call it after we get a result
        thread.join().unwrap();

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
    use abq_utils::net_protocol::runners::{
        ManifestMessage, ManifestResult, Status, TestCase, TestOrGroup,
    };
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        NativeTestRunnerParams, NextWork, NextWorkBundle, RunId, WorkContext, WorkId,
    };

    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    fn npm_jest_project_path() -> PathBuf {
        PathBuf::from(std::env::var("ABQ_WORKSPACE_DIR").unwrap())
            .join("testdata/jest/npm-jest-project")
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
        let get_next_test = || NextWorkBundle(vec![NextWork::EndOfWork]);
        let send_test_result: SendTestResults = {
            let test_results = test_results.clone();
            &move |results| {
                let test_results = test_results.clone();
                Box::pin(async move {
                    test_results
                        .lock()
                        .unwrap()
                        .push(results.into_iter().map(|(_, result)| result))
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

        let ManifestMessage { mut manifest } = match manifest.unwrap() {
            ManifestResult::Manifest(man) => man,
            ManifestResult::TestRunnerError { .. } => unreachable!(),
        };

        manifest.members.sort_by_key(|member| match member {
            TestOrGroup::Test(test) => test.id.clone(),
            TestOrGroup::Group(group) => group.name.clone(),
        });

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

        let (_, mut test_results) = execute_wrapped_runner(input, working_dir).unwrap();

        test_results.sort_by_key(|r| r.id.clone());

        assert_eq!(test_results.len(), 2, "{:#?}", test_results);

        assert_eq!(test_results[0].status, Status::Success);
        assert!(test_results[0].id.ends_with("add.test.js"));

        assert_eq!(test_results[1].status, Status::Success);
        assert!(test_results[1].id.ends_with("names.test.js"));
    }

    #[test]
    fn quick_exit_if_init_context_says_run_is_complete() {
        let input = NativeTestRunnerParams {
            cmd: "npm".to_string(),
            args: vec!["test".to_string()],
            extra_env: Default::default(),
        };

        let get_init_context = || Err(RunAlreadyCompleted {});
        let get_next_test = || {
            NextWorkBundle(vec![NextWork::Work {
                test_case: TestCase {
                    id: "unreachable".to_string(),
                    meta: Default::default(),
                },
                context: WorkContext {
                    working_dir: std::env::current_dir().unwrap(),
                },
                run_id: RunId::unique(),
                work_id: WorkId("unreachable".to_string()),
            }])
        };
        let send_test_result: SendTestResults = &|_| Box::pin(async {});

        let runner_result = GenericTestRunner::run::<_, fn(ManifestResult), _, _>(
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
mod test {
    use crate::{GenericRunnerError, GenericTestRunner, SendTestResults};
    use abq_utils::net_protocol::entity::EntityId;
    use abq_utils::net_protocol::runners::ManifestResult;
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{NativeTestRunnerParams, NextWork, NextWorkBundle};

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
        let get_next_test = || NextWorkBundle(vec![NextWork::EndOfWork]);
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
