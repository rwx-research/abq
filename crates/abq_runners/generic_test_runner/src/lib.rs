use std::{
    collections::HashMap, io::Read, net::TcpListener, path::PathBuf, process, sync::mpsc, thread,
};

use abq_utils::net_protocol;
use abq_utils::net_protocol::runners::{Action, Manifest, TestResult};
use abq_utils::net_protocol::workers::NextWork;
use serde_derive::{Deserialize, Serialize};

pub struct WorkInput {
    pub cmd: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub working_dir: PathBuf,
    pub generate_manifest: bool,
}

pub struct GenericTestRunner;

#[derive(Serialize, Deserialize)]
struct TestIds {
    test_ids: Vec<String>,
}

enum NativeWorkerMsg {
    Manifest(Manifest),
    TestResult(TestResult),
    EndOfTests,
}

impl GenericTestRunner {
    pub fn run<SendManifest, GetNextWork, SendTestResult>(
        input: WorkInput,
        mut send_manifest: SendManifest,
        get_next_test: GetNextWork,
        mut send_test_result: SendTestResult,
    ) where
        SendManifest: FnMut(Manifest),
        GetNextWork: Fn() -> NextWork + std::marker::Send + 'static,
        SendTestResult: FnMut(TestResult),
    {
        let our_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let our_addr = our_listener.local_addr().unwrap();

        let WorkInput {
            cmd,
            args,
            env: _,
            working_dir,
            generate_manifest,
        } = input;

        let (msg_tx, msg_rx) = mpsc::channel();

        // Server proxying work in the queue to work the native runner needs to do.
        let work_server_handle = thread::spawn(move || {
            #[allow(clippy::never_loop)]
            for (conn_no, stream) in our_listener.incoming().enumerate() {
                let mut stream = stream.unwrap();

                let msg = if conn_no == 0 && generate_manifest {
                    // We need to generate the test manifest
                    let mut msg_buf = String::new();
                    stream.read_to_string(&mut msg_buf).unwrap();
                    let TestIds { test_ids } = serde_json::from_slice(msg_buf.as_bytes()).unwrap();

                    let manifest = Manifest {
                        actions: test_ids.into_iter().map(Action::TestId).collect(),
                    };
                    NativeWorkerMsg::Manifest(manifest)
                } else {
                    match get_next_test() {
                        NextWork::EndOfWork => NativeWorkerMsg::EndOfTests,
                        NextWork::Work {
                            action,
                            context: _,
                            invocation_id: _,
                            work_id: _,
                        } => match action {
                            Action::TestId(test_id) => {
                                net_protocol::write(&mut stream, test_id).unwrap();
                                let results = net_protocol::read(&mut stream).unwrap();
                                NativeWorkerMsg::TestResult(results)
                            }
                            _ => {
                                unreachable!("Invalid action for generic test runner: {:?}", action)
                            }
                        },
                    }
                };

                msg_tx.send(msg).unwrap();

                // TODO: right now we just send the manifest and then exit
                msg_tx.send(NativeWorkerMsg::EndOfTests).unwrap();
                break;
            }
        });

        let mut native_runner = process::Command::new(cmd);
        native_runner.args(args);
        native_runner.env("ABQ_SOCKET", format!("{}", our_addr));
        if generate_manifest {
            native_runner.env("ABQ_GENERATE_MANIFEST", "1");
        }

        let mut native_runner_handle = native_runner.current_dir(working_dir).spawn().unwrap();

        loop {
            match msg_rx.recv().unwrap() {
                NativeWorkerMsg::Manifest(manifest) => send_manifest(manifest),
                NativeWorkerMsg::TestResult(test_result) => send_test_result(test_result),
                NativeWorkerMsg::EndOfTests => break,
            }
        }

        native_runner_handle.wait().unwrap();
        work_server_handle.join().unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    #[allow(unused)]
    fn npm_jest_project_path() -> PathBuf {
        PathBuf::from(std::env::var("ABQ_WORKSPACE_DIR").unwrap())
            .join("testdata/jest/npm-jest-project")
    }

    #[test]
    #[cfg(feature = "test-abq-jest")]
    fn get_manifest_from_jest() {
        use crate::{GenericTestRunner, WorkInput};
        use abq_utils::net_protocol::{
            runners::{Action, Manifest},
            workers::NextWork,
        };

        let input = WorkInput {
            cmd: "npm".to_string(),
            args: vec!["test".to_string()],
            env: Default::default(),
            working_dir: npm_jest_project_path(),
            generate_manifest: true,
        };

        let mut manifest = None;
        let mut test_results = vec![];

        let send_manifest = |real_manifest| manifest = Some(real_manifest);
        let get_next_test = || NextWork::EndOfWork;
        let send_test_result = |test_result| test_results.push(test_result);

        GenericTestRunner::run(input, send_manifest, get_next_test, send_test_result);

        assert!(test_results.is_empty());
        let Manifest { actions } = manifest.unwrap();

        let mut test_ids = actions
            .into_iter()
            .map(|action| match action {
                Action::TestId(id) => id,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();

        test_ids.sort();
        assert_eq!(test_ids.len(), 2);
        assert!(test_ids[0].ends_with("add.test.js"));
        assert!(test_ids[1].ends_with("names.test.js"));
    }
}
