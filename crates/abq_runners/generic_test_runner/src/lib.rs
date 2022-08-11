use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{net::TcpListener, process};

use abq_utils::net_protocol::runners::{
    AbqProtocolVersionMessage, ManifestMessage, TestCaseMessage, TestResult, TestResultMessage,
    ABQ_GENERATE_MANIFEST, ABQ_SOCKET, ACTIVE_PROTOCOL_VERSION_MAJOR,
    ACTIVE_PROTOCOL_VERSION_MINOR,
};
use abq_utils::net_protocol::workers::{
    InvocationId, NativeTestRunnerParams, NextWork, WorkContext, WorkId,
};
use abq_utils::{flatten_manifest, net_protocol};
use thiserror::Error;
use tracing::instrument;

pub struct GenericTestRunner;

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

#[derive(Debug, Error)]
pub enum ProtocolVersionError {
    #[error("Timeout while waiting for protocol version")]
    Timeout,
    #[error("Incompatible native runner protocol version")]
    NotCompatible,
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
            return Err(ProtocolVersionError::Timeout.into());
        }

        match listener.accept() {
            Ok((mut conn, _)) => {
                conn.set_nonblocking(false).map_err(Io)?;
                listener.set_nonblocking(false).map_err(Io)?;

                let version_message: AbqProtocolVersionMessage =
                    net_protocol::read(&mut conn).map_err(Io)?;

                break (version_message, conn);
            }
            Err(err) => match err.kind() {
                io::ErrorKind::WouldBlock => {
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
        native_runner.stdout(process::Stdio::null());
        native_runner.stderr(process::Stdio::null());
        let mut native_runner_handle = native_runner.spawn()?;

        // Wait for and validate the protocol version message, which must always come first.
        let runner_conn =
            open_native_runner_connection(&mut our_listener, protocol_version_timeout)?;

        let manifest = wait_for_manifest(runner_conn)?;

        let status = native_runner_handle.wait()?;
        debug_assert!(status.success());

        manifest
    };

    Ok(manifest)
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("{0}")]
    Io(io::Error),
    #[error("{0}")]
    ProtocolVersion(ProtocolVersionError),
}

impl From<RetrieveManifestError> for RunnerError {
    fn from(e: RetrieveManifestError) -> Self {
        match e {
            RetrieveManifestError::Io(e) => Self::Io(e),
            RetrieveManifestError::ProtocolVersion(e) => Self::ProtocolVersion(e),
        }
    }
}

impl From<ProtocolVersionMessageError> for RunnerError {
    fn from(e: ProtocolVersionMessageError) -> Self {
        match e {
            ProtocolVersionMessageError::Io(e) => Self::Io(e),
            ProtocolVersionMessageError::Version(e) => Self::ProtocolVersion(e),
        }
    }
}

impl GenericTestRunner {
    pub fn run<ShouldShutdown, SendManifest, GetNextWork, SendTestResult>(
        input: NativeTestRunnerParams,
        working_dir: &Path,
        _polling_should_shutdown: ShouldShutdown,
        send_manifest: Option<SendManifest>,
        mut get_next_test: GetNextWork,
        mut send_test_result: SendTestResult,
    ) -> Result<(), RunnerError>
    where
        ShouldShutdown: Fn() -> bool,
        SendManifest: FnMut(ManifestMessage),
        GetNextWork: FnMut() -> NextWork + std::marker::Send + 'static,
        SendTestResult: FnMut(WorkId, TestResult),
    {
        let NativeTestRunnerParams {
            cmd,
            args,
            extra_env: additional_env,
        } = input;

        // TODO: get from runner params
        let protocol_version_timeout = Duration::from_secs(5);

        // If we need to retrieve the manifest, do that first.
        if let Some(mut send_manifest) = send_manifest {
            // TODO: error handling
            let manifest = retrieve_manifest(
                &cmd,
                &args,
                &additional_env,
                working_dir,
                protocol_version_timeout,
            )?;

            send_manifest(manifest);
        }

        // Now, start the test runner.
        //
        // The interface between us and the runner is described at
        //   https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-Interface-0959f5a9144741d798ac122566a3d887
        //
        // In short: the protocol is
        //
        // Queue |                       | Worker (us) |                           | Native runner
        //       | <- send Next-Test -   |             |                           |
        //       | - recv Next-Test ->   |             |                           |
        //       |                       |             | - send TestCaseMessage -> |
        //       |                       |             | <- recv TestResult -      |
        //       | <- send Test-Result - |             |                           |
        //       |                       |             |                           |
        //       |          ...          |             |          ...              |
        //       |                       |             |                           |
        //       | <- send Next-Test -   |             |                           |
        //       | -   recv Done    ->   |             |      <close conn>         |
        // Queue |                       | Worker (us) |                           | Native runner
        let mut our_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let our_addr = our_listener.local_addr().unwrap();

        let mut native_runner = process::Command::new(cmd);
        native_runner.args(args);
        native_runner.env(ABQ_SOCKET, format!("{}", our_addr));
        native_runner.envs(additional_env);
        native_runner.current_dir(working_dir);
        native_runner.stdout(process::Stdio::null());
        native_runner.stderr(process::Stdio::null());
        let mut native_runner_handle = native_runner.spawn().unwrap();

        // First, get and validate the protocol version message.
        let mut runner_conn =
            open_native_runner_connection(&mut our_listener, protocol_version_timeout)?;

        // We establish one connection with the native runner and repeatedly send tests until we're
        // done.
        loop {
            match get_next_test() {
                NextWork::EndOfWork => {
                    drop(runner_conn);
                    break;
                }
                NextWork::Work {
                    test_case,
                    context: _,
                    invocation_id: _,
                    work_id,
                } => {
                    let test_case_message = TestCaseMessage { test_case };

                    // TODO: errors
                    net_protocol::write(&mut runner_conn, test_case_message).unwrap();
                    let test_result_message: TestResultMessage =
                        net_protocol::read(&mut runner_conn).unwrap();

                    send_test_result(work_id, test_result_message.test_result);
                }
            };
        }

        drop(our_listener);
        native_runner_handle.wait().unwrap();

        Ok(())
    }
}

/// Executes a native test runner in an end-to-end fashion from the perspective of an ABQ worker.
/// Returns the manifest and all test results.
pub fn execute_wrapped_runner(
    native_runner_params: NativeTestRunnerParams,
    working_dir: PathBuf,
) -> Result<(ManifestMessage, Vec<TestResult>), RunnerError> {
    let mut test_case_index = 0;

    let mut manifest_message = None;

    // Currently, an atomic mutex is used here because `send_manifest`, `get_next_test`, and the
    // main thread all need access to manifest's memory location. We can get away with unsafe code
    // here because we know that the manifest must come in before `get_next_test` will be called,
    // and moreover, `send_manifest` will never be called again. But to avoid bugs, we don't do
    // that for now.
    let flat_manifest = Arc::new(Mutex::new(None));

    let mut test_results = vec![];

    let send_manifest = {
        let flat_manifest = Arc::clone(&flat_manifest);
        let manifest_message = &mut manifest_message;
        move |real_manifest: ManifestMessage| {
            let mut flat_manifest = flat_manifest.lock().unwrap();

            if manifest_message.is_some() || flat_manifest.is_some() {
                panic!("Manifest has already been defined, but is being sent again");
            }

            *manifest_message = Some(real_manifest.clone());
            *flat_manifest = Some(flatten_manifest(real_manifest.manifest));
        }
    };

    let get_next_test = {
        let manifest = Arc::clone(&flat_manifest);
        let working_dir = working_dir.clone();
        move || {
            loop {
                let manifest = manifest.lock().unwrap();
                let next_test = match &(*manifest) {
                    Some(manifest) => manifest.get(test_case_index),
                    None => {
                        // still waiting for the manifest, spin
                        continue;
                    }
                };
                return match next_test {
                    Some(test_case) => {
                        test_case_index += 1;

                        NextWork::Work {
                            test_case: test_case.clone(),
                            context: WorkContext {
                                working_dir: working_dir.clone(),
                            },
                            invocation_id: InvocationId::new(),
                            work_id: WorkId(Default::default()),
                        }
                    }
                    None => NextWork::EndOfWork,
                };
            }
        }
    };
    let send_test_result = |_, test_result| test_results.push(test_result);

    GenericTestRunner::run(
        native_runner_params,
        &working_dir,
        || false,
        Some(send_manifest),
        get_next_test,
        send_test_result,
    )?;

    Ok((
        manifest_message.expect("manifest never received!"),
        test_results,
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

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1));

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

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1));

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
            let message = net_protocol::runners::Manifest { members: vec![] };
            net_protocol::write(&mut stream, message).unwrap();
        })
        .join()
        .unwrap();

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1));

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

        let result = open_native_runner_connection(&mut listener, Duration::from_secs(1));

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

        let result = open_native_runner_connection(&mut listener, timeout);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ProtocolVersionMessageError::Version(ProtocolVersionError::Timeout)
        ));
    }
}

#[cfg(test)]
#[cfg(feature = "test-abq-jest")]
mod test_abq_jest {
    use crate::{execute_wrapped_runner, GenericTestRunner};
    use abq_utils::net_protocol::runners::{ManifestMessage, Status, TestOrGroup};
    use abq_utils::net_protocol::workers::{NativeTestRunnerParams, NextWork};

    use std::path::PathBuf;

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
        let mut test_results = vec![];

        let send_manifest = |real_manifest| manifest = Some(real_manifest);
        let get_next_test = || NextWork::EndOfWork;
        let send_test_result = |_, test_result| test_results.push(test_result);

        GenericTestRunner::run(
            input,
            &npm_jest_project_path(),
            || false,
            Some(send_manifest),
            get_next_test,
            send_test_result,
        )
        .unwrap();

        assert!(test_results.is_empty());

        let ManifestMessage { mut manifest } = manifest.unwrap();

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
}
