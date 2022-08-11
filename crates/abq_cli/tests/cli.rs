#![cfg(test)]
#![cfg(feature = "test-abq-jest")]

use std::{
    ffi::OsStr,
    io::{BufRead, BufReader},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
};

use abq_utils::net_protocol::workers::InvocationId;

const WORKSPACE: &str = std::env!("ABQ_WORKSPACE_DIR");

fn abq_binary() -> PathBuf {
    if cfg!(debug_assertions) {
        PathBuf::from(WORKSPACE).join("target/debug/abq")
    } else {
        PathBuf::from(WORKSPACE).join("target/release/abq")
    }
}

fn npm_jest_project_path() -> PathBuf {
    PathBuf::from(WORKSPACE).join("testdata/jest/npm-jest-project")
}

struct CmdOutput {
    stdout: String,
    stderr: String,
    exit_status: ExitStatus,
}

fn run_abq<S>(args: impl IntoIterator<Item = S>) -> CmdOutput
where
    S: AsRef<OsStr>,
{
    run_abq_in(args, &std::env::current_dir().unwrap())
}

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

#[test]
fn yarn_jest_auto_workers_without_failure() {
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_in(
        ["test", "--reporter", "dot", "--", "yarn", "jest"],
        &npm_jest_project_path(),
    );

    assert!(exit_status.success());
    assert_eq!(stdout, "..");
    assert!(stderr.is_empty());
}

#[test]
fn yarn_jest_separate_queue_workers_test_without_failure() {
    let port = find_free_port();
    let queue_addr = format!("0.0.0.0:{port}");
    let test_id = InvocationId::new().to_string();

    let npm_jest_project_path = npm_jest_project_path();

    let mut queue_proc = spawn_abq(["start", "--bind", &queue_addr]);

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
