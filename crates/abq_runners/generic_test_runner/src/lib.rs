use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{net::TcpListener, process};

use abq_utils::net_protocol::runners::{
    AbqProtocolVersionMessage, ManifestMessage, TestCaseMessage, TestResult, TestResultMessage,
    ABQ_GENERATE_MANIFEST, ABQ_SOCKET,
};
use abq_utils::net_protocol::workers::{
    InvocationId, NativeTestRunnerParams, NextWork, WorkContext, WorkId,
};
use abq_utils::{flatten_manifest, net_protocol};
use tracing::instrument;

pub struct GenericTestRunner;

static TARGET_PROTOCOL_VERSION_MAJOR: u64 = 0;
static TARGET_PROTOCOL_VERSION_MINOR: u64 = 1;

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

#[derive(Debug)]
enum ProtocolVersionMessageError {
    IoError(io::Error),
    Timeout,
    NotCompatible,
}

#[allow(unused)]
fn wait_and_validate_protocol_version_message(
    listener: &mut TcpListener,
    timeout: Duration,
) -> Result<(), ProtocolVersionMessageError> {
    use ProtocolVersionMessageError::*;

    listener.set_nonblocking(true).map_err(IoError)?;

    let start = Instant::now();
    let AbqProtocolVersionMessage {
        r#type: _,
        major,
        minor,
    } = loop {
        if start.elapsed() >= timeout {
            return Err(Timeout);
        }

        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_nonblocking(false).map_err(IoError)?;
                listener.set_nonblocking(false).map_err(IoError)?;
                let version_message: AbqProtocolVersionMessage =
                    net_protocol::read(&mut stream).map_err(IoError)?;
                break version_message;
            }
            Err(err) => match err.kind() {
                io::ErrorKind::WouldBlock => {
                    std::thread::sleep(POLL_WAIT_TIME);
                    continue;
                }
                _ => return Err(IoError(err)),
            },
        }
    };

    if major != TARGET_PROTOCOL_VERSION_MAJOR || minor != TARGET_PROTOCOL_VERSION_MINOR {
        Err(NotCompatible)
    } else {
        Ok(())
    }
}

pub fn wait_for_manifest(listener: TcpListener) -> io::Result<ManifestMessage> {
    let (mut stream, _) = listener.accept()?;
    let manifest: ManifestMessage = net_protocol::read(&mut stream)?;

    Ok(manifest)
}

/// Retrieves the test manifest from native test runner.
#[instrument(level = "trace", skip(additional_env, working_dir))]
fn retrieve_manifest<'a>(
    cmd: &str,
    args: &[String],
    additional_env: impl IntoIterator<Item = (&'a String, &'a String)>,
    working_dir: &Path,
) -> io::Result<ManifestMessage> {
    // One-shot the native runner. Since we set the manifest generation flag, expect exactly one
    // message to be received, namely the manifest.
    let manifest = {
        let our_listener = TcpListener::bind("127.0.0.1:0")?;
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

        let manifest = wait_for_manifest(our_listener)?;

        let status = native_runner_handle.wait()?;
        debug_assert!(status.success());

        manifest
    };

    Ok(manifest)
}

impl GenericTestRunner {
    pub fn run<ShouldShutdown, SendManifest, GetNextWork, SendTestResult>(
        input: NativeTestRunnerParams,
        working_dir: &Path,
        _polling_should_shutdown: ShouldShutdown,
        send_manifest: Option<SendManifest>,
        mut get_next_test: GetNextWork,
        mut send_test_result: SendTestResult,
    ) where
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

        // If we need to retrieve the manifest, do that first.
        if let Some(mut send_manifest) = send_manifest {
            // TODO: error handling
            let manifest = retrieve_manifest(&cmd, &args, &additional_env, working_dir).unwrap();
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
        let our_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let our_addr = our_listener.local_addr().unwrap();

        let mut native_runner = process::Command::new(cmd);
        native_runner.args(args);
        native_runner.env(ABQ_SOCKET, format!("{}", our_addr));
        native_runner.envs(additional_env);
        native_runner.current_dir(working_dir);
        native_runner.stdout(process::Stdio::null());
        native_runner.stderr(process::Stdio::null());
        let mut native_runner_handle = native_runner.spawn().unwrap();

        // We establish one connection with the native runner and repeatedly send tests until we're
        // done.
        let (mut conn, _) = our_listener.accept().unwrap();
        loop {
            match get_next_test() {
                NextWork::EndOfWork => {
                    drop(conn);
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
                    net_protocol::write(&mut conn, test_case_message).unwrap();
                    let test_result_message: TestResultMessage =
                        net_protocol::read(&mut conn).unwrap();

                    send_test_result(work_id, test_result_message.test_result);
                }
            };
        }

        drop(our_listener);
        native_runner_handle.wait().unwrap();
    }
}

/// Executes a native test runner in an end-to-end fashion from the perspective of an ABQ worker.
/// Returns the manifest and all test results.
pub fn execute_wrapped_runner(
    native_runner_params: NativeTestRunnerParams,
    working_dir: PathBuf,
) -> (ManifestMessage, Vec<TestResult>) {
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
    );

    (
        manifest_message.expect("manifest never received!"),
        test_results,
    )
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
        runners::{AbqProtocolVersionMessage, AbqProtocolVersionTag},
    };

    use super::{
        wait_and_validate_protocol_version_message, ProtocolVersionMessageError,
        TARGET_PROTOCOL_VERSION_MAJOR, TARGET_PROTOCOL_VERSION_MINOR,
    };

    #[test]
    fn recv_and_validate_protocol_version_message() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let () = thread::spawn(move || {
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let version_message = AbqProtocolVersionMessage {
                r#type: AbqProtocolVersionTag::AbqProtocolVersion,
                major: TARGET_PROTOCOL_VERSION_MAJOR,
                minor: TARGET_PROTOCOL_VERSION_MINOR,
            };
            net_protocol::write(&mut stream, version_message).unwrap();
        })
        .join()
        .unwrap();

        let result =
            wait_and_validate_protocol_version_message(&mut listener, Duration::from_secs(1));

        assert!(result.is_ok());
        let ok = result.unwrap();
        assert_eq!(ok, ());
    }

    #[test]
    fn protocol_version_message_incompatible() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let () = thread::spawn(move || {
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

        let result =
            wait_and_validate_protocol_version_message(&mut listener, Duration::from_secs(1));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::NotCompatible));
    }

    #[test]
    fn protocol_version_message_recv_wrong_message() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let () = thread::spawn(move || {
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let message = net_protocol::runners::Manifest { members: vec![] };
            net_protocol::write(&mut stream, message).unwrap();
        })
        .join()
        .unwrap();

        let result =
            wait_and_validate_protocol_version_message(&mut listener, Duration::from_secs(1));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::IoError(_)));
    }

    #[test]
    fn protocol_version_message_tunnel_dropped() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let () = thread::spawn(move || {
            let stream = TcpStream::connect(socket_addr).unwrap();
            drop(stream);
        })
        .join()
        .unwrap();

        let result =
            wait_and_validate_protocol_version_message(&mut listener, Duration::from_secs(1));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::IoError(_)));
    }

    #[test]
    fn protocol_version_message_tunnel_timeout() {
        let mut listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let socket_addr = listener.local_addr().unwrap();

        let timeout = Duration::from_millis(0);
        let () = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            let mut stream = TcpStream::connect(socket_addr).unwrap();
            let version_message = AbqProtocolVersionMessage {
                r#type: AbqProtocolVersionTag::AbqProtocolVersion,
                major: TARGET_PROTOCOL_VERSION_MAJOR,
                minor: TARGET_PROTOCOL_VERSION_MINOR,
            };
            net_protocol::write(&mut stream, version_message).unwrap();
        })
        .join()
        .unwrap();

        let result = wait_and_validate_protocol_version_message(&mut listener, timeout);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProtocolVersionMessageError::Timeout));
    }
}

#[cfg(test)]
#[cfg(feature = "test-abq-jest")]
mod test_abq_jest {
    use crate::{execute_wrapped_runner, GenericTestRunner};
    use abq_utils::net_protocol::runners::{ManifestMessage, Status};
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
        );

        assert!(test_results.is_empty());
        let ManifestMessage { manifest } = manifest.unwrap();

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

        let (_, mut test_results) = execute_wrapped_runner(input, working_dir);

        test_results.sort_by_key(|r| r.id.clone());

        assert_eq!(test_results.len(), 2, "{:#?}", test_results);

        assert_eq!(test_results[0].status, Status::Success);
        assert!(test_results[0].id.ends_with("add.test.js"));

        assert_eq!(test_results[1].status, Status::Success);
        assert!(test_results[1].id.ends_with("names.test.js"));
    }
}
