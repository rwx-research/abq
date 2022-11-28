//! Very coarse shim over `cargo` for running in ABQ.
//! Eventually this should call cargo as a library directly, or fork cargo.

use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpStream},
    process::{self, Stdio},
    string::FromUtf8Error,
    time::Instant,
};

use abq_utils::net_protocol::{
    self,
    runners::{
        AbqProtocolVersion, InitMessage, InitSuccessMessage, Manifest, ManifestMessage, Status,
        Test, TestCase, TestCaseMessage, TestOrGroup, TestResult, TestResultMessage,
        ABQ_GENERATE_MANIFEST, ABQ_SOCKET, ACTIVE_PROTOCOL_VERSION_MAJOR,
        ACTIVE_PROTOCOL_VERSION_MINOR,
    },
};

fn protocol_version() -> AbqProtocolVersion {
    AbqProtocolVersion {
        major: ACTIVE_PROTOCOL_VERSION_MAJOR,
        minor: ACTIVE_PROTOCOL_VERSION_MINOR,
    }
}

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

    let mut worker_conn = TcpStream::connect(abq_socket)?;

    // Always send our protocol version up-front.
    net_protocol::write(&mut worker_conn, protocol_version())?;

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
                let mut test_name = String::from_utf8(line.to_vec())?;

                // cargo outputs "test_name: test"
                let test_suffix = ": test";
                debug_assert!(test_name.ends_with(test_suffix));
                let test_name_len = test_name.len() - test_suffix.len();
                test_name.truncate(test_name_len);

                let test = TestOrGroup::Test(Test {
                    id: test_name,
                    tags: Default::default(),
                    meta: Default::default(),
                });
                Ok(test)
            })
            .collect::<Result<Vec<TestOrGroup>, FromUtf8Error>>()?;

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: tests,
                init_meta: Default::default(),
            },
        };

        net_protocol::write(&mut worker_conn, manifest)?;
    } else {
        let _init_message: InitMessage = net_protocol::read(&mut worker_conn)?;
        net_protocol::write(&mut worker_conn, InitSuccessMessage {})?;

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
                    cmd.stdout(Stdio::piped());
                    cmd.stderr(Stdio::piped());

                    let start = Instant::now();
                    let mut child = cmd.spawn().unwrap();

                    // Cargo writes both test output and error to stdout
                    let mut output = String::new();
                    let stdout = child.stdout.take().unwrap();
                    for line in BufReader::new(stdout).lines() {
                        output.push_str(&line.unwrap());
                    }

                    let result = child.wait();
                    let duration = start.elapsed();

                    let (status, output) = match result {
                        Ok(status) => {
                            let status = if status.success() {
                                Status::Success
                            } else {
                                // cargo writes test failures to stdout
                                Status::Failure
                            };
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
