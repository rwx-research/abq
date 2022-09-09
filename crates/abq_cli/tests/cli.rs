#![cfg(test)]

use std::process::{ExitStatus, Output};
use std::{
    ffi::OsStr,
    io::{BufRead, BufReader},
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
    use std::net::TcpListener;
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
    use std::net::TcpStream;
    TcpStream::connect(("0.0.0.0", port)).is_ok()
}

const TEST_AUTH_KEY: &str = "abqs_ckoUjQN4ufq1MiUeaNllztfyqjCtuz";

/// Configuration options for how clients and servers in an abq run communicate.
/// Namely: whether they use an authentication header or not, and eventually, whether they use TLS
/// or not.
struct CSConfigOptions {
    use_auth_token: bool,
    tls: bool,
}

impl CSConfigOptions {
    fn extend_args<'a>(&self, args: &[&'a str]) -> Vec<&'a str> {
        let mut args = args.to_vec();
        if self.use_auth_token {
            args.extend(["--token", TEST_AUTH_KEY]);
        }
        if self.tls {
            args.extend(["--tls"]);
        }
        args
    }
}

/// Runs a given test with all network configuration options abq exposes.
/// That is, multiplex over whether an auth token is required.
macro_rules! test_all_network_config_options {
    (
        $(#[$cfg:meta])*
        $test_name:ident
        $run:expr
    ) => { paste::paste! {
        #[test]
        $(#[$cfg])*
        fn [<$test_name _no_auth_no_tls>]() {
            $run(CSConfigOptions {
                use_auth_token: false,
                tls: false,
            })
        }

        #[test]
        $(#[$cfg])*
        fn [<$test_name _no_auth_with_tls>]() {
            $run(CSConfigOptions {
                use_auth_token: false,
                tls: true,
            })
        }

        #[test]
        $(#[$cfg])*
        fn [<$test_name _with_auth_no_tls>]() {
            $run(CSConfigOptions {
                use_auth_token: true,
                tls: false,
            })
        }

        #[test]
        $(#[$cfg])*
        fn [<$test_name _with_auth_with_tls>]() {
            $run(CSConfigOptions {
                use_auth_token: true,
                tls: true,
            })
        }
    }};
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_without_failure |conf: CSConfigOptions| {

        // abq test --reporter dot (--token ...)? -- yarn jest
        let args = &["test", "--reporter", "dot"];
        let mut args = conf.extend_args(args);
        args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq_in(
            args,
            &testdata_project("jest/npm-jest-project"),
        );

        assert!(exit_status.success());
        assert_eq!(stdout, "..");
        assert!(stderr.is_empty(), "{:?}", stderr);
    }
}

test_all_network_config_options! {
    queue_open_servers_on_specified_ports |conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(conf.extend_args(&[
            "start",
            "--bind",
            "0.0.0.0",
            "--port",
            &server_port.to_string(),
            "--work-port",
            &worker_port.to_string(),
            "--negotiator-port",
            &negotiator_port.to_string(),
        ]));

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
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_workers_test_without_failure |conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let test_id = InvocationId::new().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(conf.extend_args(&[
            "start",
            "--bind",
            "0.0.0.0",
            "--port",
            &server_port.to_string(),
            "--work-port",
            &worker_port.to_string(),
            "--negotiator-port",
            &negotiator_port.to_string(),
        ]));

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

        // abq work --queue-addr ... --working-dir ... (--token ...)? <test_id>
        let worker_args = &[
            "work",
            "--queue-addr",
            &queue_addr,
            "--working-dir",
            &npm_jest_project_path.display().to_string(),
        ];
        let mut worker_args = conf.extend_args(worker_args);
        worker_args.extend([test_id.as_str()]);
        let mut worker_proc = spawn_abq(worker_args);

        // abq test --reporter dot --queue-addr ... --test-id ... (--token ...)? -- yarn jest
        let test_args = &[
            "test",
            "--reporter",
            "dot",
            "--queue-addr",
            &queue_addr,
            "--test-id",
            &test_id,
        ];
        let mut test_args = conf.extend_args(test_args);
        test_args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(test_args);

        assert!(exit_status.success(), "{:?}", (stdout, stderr));

        let mut lines = stdout.lines();
        assert!(lines.next().unwrap().contains("Starting test run"));
        assert_eq!(lines.next().unwrap(), "..");

        assert!(stderr.is_empty());

        let worker_exit_status = worker_proc.wait().unwrap();
        assert!(worker_exit_status.success());

        // Must kill the queue because it sits around forever, waiting for new requests.
        queue_proc.kill().expect("queue already dead");
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_with_failing_tests |conf: CSConfigOptions| {
        // abq test --reporter dot (--token ...)? -- yarn jest
        let test_args = &[
            "test",
            "--reporter",
            "dot",
        ];
        let mut test_args = conf.extend_args(test_args);
        test_args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq_in(
            test_args,
            &testdata_project("jest/npm-jest-project-with-failures"),
        );

        let code = exit_status.code().expect("process killed");
        assert_eq!(code, 1);

        let mut stdout_lines = stdout.lines();
        assert_eq!(stdout_lines.next().unwrap(), "F");

        assert!(stderr.is_empty(), "{:?}", stderr);
    }
}

test_all_network_config_options! {
    healthcheck_queue_success |conf: CSConfigOptions| {
        let server_port = find_free_port();
        let work_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let work_scheduler_addr = format!("0.0.0.0:{work_port}");
        let negotiator_addr = format!("0.0.0.0:{negotiator_port}");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(conf.extend_args(&[
            "start",
            "--bind",
            "0.0.0.0",
            "--port",
            &server_port.to_string(),
            "--work-port",
            &work_port.to_string(),
            "--negotiator-port",
            &negotiator_port.to_string(),
        ]));

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

        // Check health
        // abq health --queue ... --work-scheduler ... --negotiator ... (--token ...)?
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(conf.extend_args(&[
            "health",
            "--queue",
            &queue_addr,
            "--work-scheduler",
            &work_scheduler_addr,
            "--negotiator",
            &negotiator_addr,
        ]));

        assert!(exit_status.success(), "{}\n{}", stdout, stderr);
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());

        queue_proc.kill().expect("queue already dead");
    }
}

test_all_network_config_options! {
    healthcheck_queue_failure |conf: CSConfigOptions| {
        let server_port = find_free_port();
        let work_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let work_scheduler_addr = format!("0.0.0.0:{work_port}");
        let negotiator_addr = format!("0.0.0.0:{negotiator_port}");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(conf.extend_args(&[
            "start",
            "--bind",
            "0.0.0.0",
            "--port",
            &server_port.to_string(),
            "--work-port",
            &work_port.to_string(),
            "--negotiator-port",
            &negotiator_port.to_string(),
        ]));

        queue_proc.kill().expect("queue already dead");

        // Check health
        // abq health --queue ... --work-scheduler ... --negotiator ... (--token ...)?
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(conf.extend_args(&[
            "health",
            "--queue",
            &queue_addr,
            "--work-scheduler",
            &work_scheduler_addr,
            "--negotiator",
            &negotiator_addr,
        ]));

        assert_eq!(exit_status.code().unwrap(), 1);
        assert_eq!(
            stdout,
            format!(
                r#"Queue at {queue_addr}: unhealthy
Work scheduler at {work_scheduler_addr}: unhealthy
Negotiator at {negotiator_addr}: unhealthy
"#
            )
        );
        assert!(stderr.is_empty());
    }
}
