//! Very coarse shim over `cargo` for running in ABQ.
//! Eventually this should call cargo as a library directly, or fork cargo.

use std::{
    net::{SocketAddr, TcpStream},
    process,
    string::FromUtf8Error,
    time::Instant,
};

use abq_utils::net_protocol::{
    self,
    runners::{
        Manifest, ManifestMessage, Status, Test, TestCase, TestCaseMessage, TestOrGroup,
        TestResult, TestResultMessage, ABQ_GENERATE_MANIFEST, ABQ_SOCKET,
    },
};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().peekable();
    // abq_cargo
    args.next();
    // test
    args.next();

    let abq_socket: SocketAddr = std::env::var(ABQ_SOCKET)?.parse()?;
    let generate_manifest = match std::env::var(ABQ_GENERATE_MANIFEST).as_deref() {
        Ok("0") | Err(_) => false,
        Ok(_) => true,
    };

    if generate_manifest {
        let mut cmd = process::Command::new("cargo");
        cmd.arg("test");
        cmd.args(args);
        cmd.args(["--", "--list", "--format", "terse"]);
        let stdout = cmd.output()?.stdout;
        let tests = stdout
            .split(|&c| c == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                let test_name = String::from_utf8(line.to_vec())?;
                let test = TestOrGroup::Test(Test {
                    id: test_name,
                    tags: Default::default(),
                    meta: Default::default(),
                });
                Ok(test)
            })
            .collect::<Result<Vec<TestOrGroup>, FromUtf8Error>>()?;

        let manifest = ManifestMessage {
            manifest: Manifest { members: tests },
        };

        let mut worker_conn = TcpStream::connect(abq_socket)?;
        net_protocol::write(&mut worker_conn, manifest)?;
    } else {
        let mut worker_conn = TcpStream::connect(abq_socket)?;
        let args: Vec<_> = args.collect();

        #[allow(clippy::while_let_loop)]
        loop {
            match net_protocol::read(&mut worker_conn) {
                Ok(TestCaseMessage {
                    test_case: TestCase { id, meta },
                }) => {
                    let mut cmd = process::Command::new("cargo");
                    cmd.arg("test");
                    cmd.args(args.iter());
                    cmd.arg(&id);

                    let start = Instant::now();
                    let output_result = cmd.output();
                    let duration = start.elapsed();

                    let (status, output) = match output_result {
                        Ok(output) => {
                            let (status, buffer) = if output.status.success() {
                                (Status::Success, output.stdout)
                            } else {
                                (Status::Failure, output.stderr)
                            };
                            let output = String::from_utf8_lossy(&buffer).to_string();
                            (status, output)
                        }
                        Err(err) => (Status::Error, err.to_string()),
                    };

                    let test_result = TestResultMessage {
                        test_result: TestResult {
                            status,
                            id: id.clone(),
                            display_name: id,
                            output: Some(output),
                            runtime: duration.as_millis() as _,
                            meta,
                        },
                    };

                    net_protocol::write(&mut worker_conn, &test_result)?;
                }
                Err(_) => {
                    // Worker probably broke the connection because tests are done, exit
                    // TODO: more error handling
                    break;
                }
            }
        }
    }

    Ok(())
}