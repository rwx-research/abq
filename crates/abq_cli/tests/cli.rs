#![cfg(test)]

#[cfg(feature = "test-abq-jest")]
use std::process::{ExitStatus, Output};
use std::{
    ffi::OsStr,
    io::{BufRead, BufReader},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

#[cfg(feature = "test-abq-jest")]
use abq_utils::net_protocol::workers::InvocationId;

const WORKSPACE: &str = std::env!("ABQ_WORKSPACE_DIR");

fn abq_binary() -> PathBuf {
    if cfg!(debug_assertions) {
        PathBuf::from(WORKSPACE).join("target/debug/abq")
    } else {
        PathBuf::from(WORKSPACE).join("target/release/abq")
    }
}

#[cfg(feature = "test-abq-jest")]
fn testdata_project(subpath: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(WORKSPACE).join("testdata").join(subpath)
}

#[cfg(feature = "test-abq-jest")]
struct CmdOutput {
    stdout: String,
    stderr: String,
    exit_status: ExitStatus,
}

#[cfg(feature = "test-abq-jest")]
fn run_abq<S>(args: impl IntoIterator<Item = S>) -> CmdOutput
where
    S: AsRef<OsStr>,
{
    run_abq_in(args, &std::env::current_dir().unwrap())
}

#[cfg(feature = "test-abq-jest")]
fn run_abq_in<S>(args: impl IntoIterator<Item = S>, working_dir: &Path) -> CmdOutput
where
    S: AsRef<OsStr>,
{
    let Output {
        status,
        stdout,
        stderr,
    } = spawn_abq_in(args, working_dir)
        .wait_with_output()
        .expect("abq cli failed on waiting");

    CmdOutput {
        stdout: String::from_utf8(stdout).expect("abq stdout should be utf-8"),
        stderr: String::from_utf8(stderr).expect("abq stderr should be utf-8"),
        exit_status: status,
    }
}

fn spawn_abq<S>(args: impl IntoIterator<Item = S>) -> Child
where
    S: AsRef<OsStr>,
{
    spawn_abq_in(args, &std::env::current_dir().unwrap())
}

fn spawn_abq_in<S>(args: impl IntoIterator<Item = S>, working_dir: &Path) -> Child
where
    S: AsRef<OsStr>,
{
    Command::new(abq_binary())
        .args(args)
        .current_dir(working_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("abq cli failed to spawn")
}

fn find_free_port() -> u16 {
    // TERRIBLE HACK to find a free port: bind and take that port number, assuming the OS
    // will increment later binds past the attached port, and there won't be a TOCTOU race.
    // Should be fine for testing.
    TcpListener::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn port_active(port: u16) -> bool {
    TcpStream::connect(("0.0.0.0", port)).is_ok()
}

#[test]
#[cfg(feature = "test-abq-jest")]
fn yarn_jest_auto_workers_without_failure() {
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_in(
        ["test", "--reporter", "dot", "--", "yarn", "jest"],
        &testdata_project("jest/npm-jest-project"),
    );

    assert!(exit_status.success());
    assert_eq!(stdout, "..");
    assert!(stderr.is_empty());
}

#[test]
fn queue_open_servers_on_specified_ports() {
    let server_port = find_free_port();
    let worker_port = find_free_port();
    let negotiator_port = find_free_port();

    let mut queue_proc = spawn_abq([
        "start",
        "--bind",
        "0.0.0.0",
        "--port",
        &server_port.to_string(),
        "--work-port",
        &worker_port.to_string(),
        "--negotiator-port",
        &negotiator_port.to_string(),
    ]);

    let queue_stdout = queue_proc.stdout.as_mut().unwrap();
    let mut queue_reader = BufReader::new(queue_stdout).lines();
    // Spin until we know the queue is UP
    loop {
        if let Some(line) = queue_reader.next() {
            let line = line.expect("line is not a string");
            if line.contains("Run the following to start workers") {
                break;
            }
        }
    }

    assert!(port_active(server_port));
    assert!(port_active(worker_port));
    assert!(port_active(negotiator_port));

    // Must kill the queue because it sits around forever, waiting for new requests.
    queue_proc.kill().expect("queue already dead");
}

#[test]
#[cfg(feature = "test-abq-jest")]
fn yarn_jest_separate_queue_workers_test_without_failure() {
    let server_port = find_free_port();
    let worker_port = find_free_port();
    let negotiator_port = find_free_port();

    let queue_addr = format!("0.0.0.0:{server_port}");
    let test_id = InvocationId::new().to_string();

    let npm_jest_project_path = testdata_project("jest/npm-jest-project");

    let mut queue_proc = spawn_abq([
        "start",
        "--bind",
        "0.0.0.0",
        "--port",
        &server_port.to_string(),
        "--work-port",
        &worker_port.to_string(),
        "--negotiator-port",
        &negotiator_port.to_string(),
    ]);

    let queue_stdout = queue_proc.stdout.as_mut().unwrap();
    let mut queue_reader = BufReader::new(queue_stdout).lines();
    // Spin until we know the queue is UP
    loop {
        if let Some(line) = queue_reader.next() {
            let line = line.expect("line is not a string");
            if line.contains("Run the following to start workers") {
                break;
            }
        }
    }

    let mut worker_proc = spawn_abq([
        "work",
        "--queue-addr",
        &queue_addr,
        "--working-dir",
        &npm_jest_project_path.display().to_string(),
        &test_id,
    ]);

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq([
        "test",
        "--reporter",
        "dot",
        "--queue-addr",
        &queue_addr,
        "--test-id",
        &test_id,
        "--",
        "yarn",
        "jest",
    ]);

    assert!(exit_status.success());

    let mut lines = stdout.lines();
    assert!(lines.next().unwrap().contains("Starting test run"));
    assert_eq!(lines.next().unwrap(), "..");

    assert!(stderr.is_empty());

    let worker_exit_status = worker_proc.wait().unwrap();
    assert!(worker_exit_status.success());

    // Must kill the queue because it sits around forever, waiting for new requests.
    queue_proc.kill().expect("queue already dead");
}

#[test]
#[cfg(feature = "test-abq-jest")]
fn yarn_jest_auto_workers_with_failing_tests() {
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_in(
        ["test", "--reporter", "dot", "--", "yarn", "jest"],
        &testdata_project("jest/npm-jest-project-with-failures"),
    );

    let code = exit_status.code().expect("process killed");
    assert_eq!(code, 1);

    let mut stdout_lines = stdout.lines();
    assert_eq!(stdout_lines.next().unwrap(), "F");

    assert!(stderr.is_empty());
}
