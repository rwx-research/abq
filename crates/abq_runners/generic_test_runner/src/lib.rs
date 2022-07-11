use std::io;
use std::path::Path;
use std::{net::TcpListener, process};

use abq_utils::net_protocol;
use abq_utils::net_protocol::runners::{
    ManifestMessage, TestCaseMessage, TestResult, TestResultMessage,
};
use abq_utils::net_protocol::workers::{NativeTestRunnerParams, NextWork, WorkId};

static ABQ_SOCKET: &str = "ABQ_SOCKET";
static ABQ_GENERATE_MANIFEST: &str = "ABQ_GENERATE_MANIFEST";

pub struct GenericTestRunner;

/// Retrieves the test manifest from native test runner.
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
        let mut native_runner_handle = native_runner.spawn()?;

        let (mut stream, _) = our_listener.accept()?;
        let manifest: ManifestMessage = net_protocol::read(&mut stream)?;

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

#[cfg(test)]
#[cfg(feature = "test-abq-jest")]
mod test_abq_jest {
    use crate::GenericTestRunner;
    use abq_utils::flatten_manifest;
    use abq_utils::net_protocol::runners::{ManifestMessage, Status, TestCase};
    use abq_utils::net_protocol::workers::{
        InvocationId, NativeTestRunnerParams, NextWork, WorkContext, WorkId,
    };

    use std::path::{Path, PathBuf};
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
        let mut test_ids = flatten_manifest(manifest);
        test_ids.sort_by_key(|r| r.id.clone());

        assert_eq!(test_ids.len(), 2);
        assert!(test_ids[0].id.ends_with("add.test.js"));
        assert!(test_ids[1].id.ends_with("names.test.js"));
    }

    fn faux_work(test_case: TestCase, working_dir: &Path) -> NextWork {
        NextWork::Work {
            test_case,
            context: WorkContext {
                working_dir: working_dir.to_path_buf(),
            },
            invocation_id: InvocationId::new(),
            work_id: WorkId(Default::default()),
        }
    }

    #[test]
    fn get_manifest_and_run_tests() {
        let working_dir = npm_jest_project_path();
        let input = NativeTestRunnerParams {
            cmd: "npm".to_string(),
            args: vec!["test".to_string()],
            extra_env: Default::default(),
        };

        let mut index = 0;
        let manifest = Arc::new(Mutex::new(None));
        let manifest2 = Arc::clone(&manifest);
        let manifest3 = Arc::clone(&manifest);

        let mut test_results = vec![];

        let send_manifest = |real_manifest: ManifestMessage| {
            *manifest.lock().unwrap() = Some(flatten_manifest(real_manifest.manifest))
        };
        let get_next_test = {
            let working_dir = working_dir.clone();
            move || {
                loop {
                    let manifest = manifest2.lock().unwrap();
                    let next_test = match &(*manifest) {
                        Some(manifest) => manifest.get(index),
                        None => {
                            // still waiting for the manifest, spin
                            continue;
                        }
                    };
                    return match next_test {
                        Some(test_case) => {
                            index += 1;
                            faux_work(test_case.clone(), &working_dir)
                        }
                        None => NextWork::EndOfWork,
                    };
                }
            }
        };
        let send_test_result = |_, test_result| test_results.push(test_result);

        GenericTestRunner::run(
            input,
            &working_dir,
            || false,
            Some(send_manifest),
            get_next_test,
            send_test_result,
        );

        let mut guard = manifest3.lock().unwrap();
        let test_ids = guard.as_mut().unwrap();
        test_ids.sort_by_key(|r| r.id.clone());

        assert_eq!(test_ids.len(), 2);
        assert!(test_ids[0].id.ends_with("add.test.js"));
        assert!(test_ids[1].id.ends_with("names.test.js"));

        test_results.sort_by_key(|r| r.id.clone());

        assert_eq!(test_results.len(), 2, "{:#?}", test_results);

        assert_eq!(test_results[0].status, Status::Success);
        assert!(test_results[0].id.ends_with("add.test.js"));

        assert_eq!(test_results[1].status, Status::Success);
        assert!(test_results[1].id.ends_with("names.test.js"));
    }
}
