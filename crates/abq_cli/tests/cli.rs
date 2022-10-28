#![cfg(test)]

use abq_utils::auth::{AdminToken, UserToken};
use serial_test::serial;
use std::process::{ExitStatus, Output};
use std::{
    ffi::OsStr,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use abq_utils::net_protocol::workers::RunId;

const WORKSPACE: &str = std::env!("ABQ_WORKSPACE_DIR");

const TLS_CERT: &str = std::concat!(
    std::env!("ABQ_WORKSPACE_DIR"),
    "crates/abq_utils/data/cert/server.crt"
);
const TLS_KEY: &str = std::concat!(
    std::env!("ABQ_WORKSPACE_DIR"),
    "crates/abq_utils/data/cert/server.key"
);

fn var_flag_set(var: &str) -> bool {
    match std::env::var(var) {
        Ok(s) => {
            let s = s.trim();
            !s.is_empty() && s != "0" && s != "false"
        }
        Err(_) => false,
    }
}

/// Set ABQ_DEBUG_CLI_TEST=1 to write log output to files locally.
fn debug_log_for_local_run() -> bool {
    var_flag_set("ABQ_DEBUG_CLI_TESTS")
}

fn debug_log_for_ci() -> bool {
    var_flag_set("ABQ_DEBUG_CLI_TESTS_FOR_CI")
}

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

fn run_abq<S>(name: &str, args: impl IntoIterator<Item = S>) -> CmdOutput
where
    S: AsRef<OsStr>,
{
    run_abq_in(name, args, &std::env::current_dir().unwrap(), false)
}

fn run_abq_forcing_capture<S>(name: &str, args: impl IntoIterator<Item = S>) -> CmdOutput
where
    S: AsRef<OsStr>,
{
    run_abq_in(name, args, &std::env::current_dir().unwrap(), true)
}

fn run_abq_in<S>(
    name: &str,
    args: impl IntoIterator<Item = S>,
    working_dir: &Path,
    // If set to `false`, won't capture stderr when ABQ_DEBUG_CLI_TESTS_FOR_CI is set.
    always_capture_stderr: bool,
) -> CmdOutput
where
    S: AsRef<OsStr>,
{
    let Output {
        status,
        stdout,
        stderr,
    } = spawn_abq_in(name, args, working_dir, always_capture_stderr)
        .wait_with_output()
        .expect("abq cli failed on waiting");

    CmdOutput {
        stdout: String::from_utf8(stdout).expect("abq stdout should be utf-8"),
        stderr: String::from_utf8(stderr).expect("abq stderr should be utf-8"),
        exit_status: status,
    }
}

fn spawn_abq<S>(name: &str, args: impl IntoIterator<Item = S>) -> Child
where
    S: AsRef<OsStr>,
{
    spawn_abq_in(name, args, &std::env::current_dir().unwrap(), false)
}

fn spawn_abq_in<S>(
    name: &str,
    args: impl IntoIterator<Item = S>,
    working_dir: &Path,
    // If set to `false`, won't capture stderr when ABQ_DEBUG_CLI_TESTS_FOR_CI is set.
    always_capture_stderr: bool,
) -> Child
where
    S: AsRef<OsStr>,
{
    let mut cmd = Command::new(abq_binary());

    cmd.args(args);
    cmd.current_dir(working_dir);
    cmd.stdout(Stdio::piped());
    if debug_log_for_ci() && !always_capture_stderr {
        cmd.stderr(Stdio::inherit());
    } else {
        cmd.stderr(Stdio::piped());
    }

    if debug_log_for_local_run() {
        cmd.env("ABQ_LOGFILE", format!("{name}.debug"));
        cmd.env("ABQ_LOG", "abq=debug");
    }
    if debug_log_for_ci() && !always_capture_stderr {
        cmd.env("ABQ_LOGCI_WITH_PREFIX", name);
        cmd.env("ABQ_LOG", "abq=debug");
    }

    cmd.spawn().expect("abq cli failed to spawn")
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

const TEST_USER_AUTH_TOKEN: &str = "abqs_ckoUjQN4ufq1MiUeaNllztfyqjCtuz";
const TEST_ADMIN_AUTH_TOKEN: &str = "abqadmin_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF";

/// Configuration options for how clients and servers in an abq run communicate.
/// Namely: whether they use an authentication header or not, and eventually, whether they use TLS
/// or not.
struct CSConfigOptions {
    use_auth_token: bool,
    tls: bool,
}

impl CSConfigOptions {
    fn extend_args_for_start<'a>(&self, args: &[&'a str]) -> Vec<&'a str> {
        let mut args = args.to_vec();
        if self.use_auth_token {
            args.extend(["--user-token", TEST_USER_AUTH_TOKEN]);
            args.extend(["--admin-token", TEST_ADMIN_AUTH_TOKEN]);
        }
        if self.tls {
            args.extend(["--tls-cert", TLS_CERT]);
            args.extend(["--tls-key", TLS_KEY]);
        }
        args
    }

    fn extend_args_for_client<'a>(&self, args: &[&'a str]) -> Vec<&'a str> {
        let mut args = args.to_vec();
        if self.use_auth_token {
            args.extend(["--token", TEST_USER_AUTH_TOKEN]);
        }
        if self.tls {
            args.extend(["--tls-cert", TLS_CERT]);
        }
        args
    }

    #[cfg(feature = "test-abq-jest")]
    fn extend_args_for_in_band_client<'a>(&self, args: &[&'a str]) -> Vec<&'a str> {
        let mut args = args.to_vec();
        if self.use_auth_token {
            args.extend(["--token", TEST_USER_AUTH_TOKEN]);
        }
        if self.tls {
            args.extend(["--tls-cert", TLS_CERT]);
            args.extend(["--tls-key", TLS_KEY]);
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
        #[serial]
        $(#[$cfg])*
        #[allow(clippy::redundant_closure_call)]
        fn [<$test_name _no_auth_no_tls>]() {
            $run(&format!("{}_no_auth_no_tls", stringify!($test_name)), CSConfigOptions {
                use_auth_token: false,
                tls: false,
            })
        }

        #[test]
        #[serial]
        $(#[$cfg])*
        #[allow(clippy::redundant_closure_call)]
        fn [<$test_name _no_auth_with_tls>]() {
            $run(&format!("{}_no_auth_with_tls", stringify!($test_name)), CSConfigOptions {
                use_auth_token: false,
                tls: true,
            })
        }

        #[test]
        #[serial]
        $(#[$cfg])*
        #[allow(clippy::redundant_closure_call)]
        fn [<$test_name _with_auth_no_tls>]() {
            $run(&format!("{}_with_auth_no_tls", stringify!($test_name)), CSConfigOptions {
                use_auth_token: true,
                tls: false,
            })
        }

        #[test]
        #[serial]
        $(#[$cfg])*
        #[allow(clippy::redundant_closure_call)]
        fn [<$test_name _with_auth_with_tls>]() {
            $run(&format!("{}_with_auth_with_tls", stringify!($test_name)), CSConfigOptions {
                use_auth_token: true,
                tls: true,
            })
        }
    }};
}

fn term_queue(mut queue_proc: Child) {
    queue_proc.kill().unwrap();
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_without_failure (|name, conf: CSConfigOptions| {
        // abq test --reporter dot (--token ...)? -- yarn jest
        let args = &["test", "--reporter", "dot"];
        let mut args = conf.extend_args_for_in_band_client(args);
        args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq_in(
            name,
            args,
            &testdata_project("jest/npm-jest-project"),
            false,
        );

        assert!(exit_status.success());
        assert_eq!(stdout, "..");
        assert!(stderr.is_empty(), "{:?}", stderr);
    })
}

test_all_network_config_options! {
    queue_open_servers_on_specified_ports (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(&[
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

        term_queue(queue_proc)
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_workers_test_without_failure (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(&[
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

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = &[
            "work",
            "--queue-addr",
            &queue_addr,
            "--working-dir",
            &npm_jest_project_path.display().to_string(),
            "--run-id",
            &run_id
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let mut worker_proc = spawn_abq(&(name.to_string() + "_worker"), worker_args);

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = &[
            "test",
            "--reporter",
            "dot",
            "--queue-addr",
            &queue_addr,
            "--run-id",
            &run_id,
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test"), test_args);

        assert!(exit_status.success(), "{:?}", (stdout, stderr));

        let mut lines = stdout.lines();
        assert!(lines.next().unwrap().contains("Starting test run"));
        assert_eq!(lines.next().unwrap(), "..");

        assert!(stderr.is_empty());

        let worker_exit_status = worker_proc.wait().unwrap();
        assert!(worker_exit_status.success());

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_cancel_run_before_workers (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(&[
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

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = &[
            "test",
            "--reporter",
            "dot",
            "--queue-addr",
            &queue_addr,
            "--run-id",
            &run_id,
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend(["--", "yarn", "jest"]);
        let mut supervisor = spawn_abq(&(name.to_string() + "_test"), test_args);

        let supervisor_stdout = supervisor.stdout.as_mut().unwrap();
        let mut supervisor_reader = BufReader::new(supervisor_stdout).lines();
        // Spin until we know the supervisor is UP
        loop {
            if let Some(line) = supervisor_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Starting test run") {
                    break;
                }
            }
        }

        use nix::sys::signal;
        use nix::unistd::Pid;

        // SIGTERM the supervisor.
        signal::kill(Pid::from_raw(supervisor.id() as _), signal::Signal::SIGTERM).unwrap();

        let supervisor_exit = supervisor.wait().unwrap();
        assert!(!supervisor_exit.success());

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = &[
            "work",
            "--queue-addr",
            &queue_addr,
            "--working-dir",
            &npm_jest_project_path.display().to_string(),
            "--run-id",
            &run_id
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_worker"), worker_args);

        // The worker should exit with a failure as well.
        assert!(!exit_status.success(), "{:?}", (stdout, stderr));

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_with_failing_tests (|name, conf: CSConfigOptions| {
        // abq test --reporter dot (--token ...)? -- yarn jest
        let test_args = &[
            "test",
            "--reporter",
            "dot",
        ];
        let mut test_args = conf.extend_args_for_in_band_client(test_args);
        test_args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq_in(
            name,
            test_args,
            &testdata_project("jest/npm-jest-project-with-failures"),
            false
        );

        let code = exit_status.code().expect("process killed");
        assert_eq!(code, 1);

        let mut stdout_lines = stdout.lines();
        assert_eq!(stdout_lines.next().unwrap(), "F");

        assert!(stderr.is_empty(), "{:?}", stderr);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_workers_with_failing_tests (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project-with-failures");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(&[
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

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = &[
            "work",
            "--queue-addr",
            &queue_addr,
            "--working-dir",
            &npm_jest_project_path.display().to_string(),
            "--run-id",
            &run_id
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let mut worker_proc = spawn_abq(&(name.to_string() + "_workers"), worker_args);

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = &[
            "test",
            "--reporter",
            "dot",
            "--queue-addr",
            &queue_addr,
            "--run-id",
            &run_id,
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test"), test_args);

        // The `abq test` process should exit with a failure
        assert!(!exit_status.success(), "{:?}", (stdout, stderr));

        let mut lines = stdout.lines();
        assert!(lines.next().unwrap().contains("Starting test run"));
        assert_eq!(lines.next().unwrap(), "F");

        assert!(stderr.is_empty());

        // The `abq work` process should also exit with a failure, corresponding to having
        // witnessed a test failure in the run.
        let worker_exit_status = worker_proc.wait().unwrap();
        assert!(!worker_exit_status.success());

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    healthcheck_queue_success (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let work_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let work_scheduler_addr = format!("0.0.0.0:{work_port}");
        let negotiator_addr = format!("0.0.0.0:{negotiator_port}");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(&[
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
        } = run_abq(&(name.to_string() + "_health"), conf.extend_args_for_client(&[
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

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    healthcheck_queue_failure (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let work_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let work_scheduler_addr = format!("0.0.0.0:{work_port}");
        let negotiator_addr = format!("0.0.0.0:{negotiator_port}");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(&[
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
        } = run_abq(&(name.to_string() + "_health"), conf.extend_args_for_client(&[
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
    })
}

#[test]
#[serial]
fn work_no_queue_addr_or_api_key() {
    // Spawn worker without a queue addr or api key
    // abq work --working-dir . run-id
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_forcing_capture(
        "work_no_queue_addr_or_api_key",
        ["work", "--run-id", "run-id"],
    );

    assert_eq!(exit_status.code().unwrap(), 2);
    assert!(stdout.is_empty());
    insta::assert_snapshot!(stderr);
}

#[test]
#[serial]
fn work_user_token_without_admin_token() {
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_forcing_capture(
        "work_no_queue_addr_or_api_key",
        [
            "start",
            &format!("--user-token={}", UserToken::new_random()),
        ],
    );

    assert_eq!(exit_status.code().unwrap(), 2);
    assert!(stdout.is_empty());
    insta::assert_snapshot!(stderr);
}

#[test]
#[serial]
fn work_admin_token_without_user_token() {
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_forcing_capture(
        "work_no_queue_addr_or_api_key",
        [
            "start",
            &format!("--admin-token={}", AdminToken::new_random()),
        ],
    );

    assert_eq!(exit_status.code().unwrap(), 2);
    assert!(stdout.is_empty());
    insta::assert_snapshot!(stderr);
}

#[test]
#[serial]
fn test_with_invalid_command() {
    let name = "test_with_invalid_command";
    let conf = CSConfigOptions {
        use_auth_token: false,
        tls: false,
    };

    let server_port = find_free_port();
    let worker_port = find_free_port();
    let negotiator_port = find_free_port();

    let queue_addr = format!("0.0.0.0:{server_port}");
    let run_id = RunId::unique().to_string();

    // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
    let mut queue_proc = spawn_abq(
        name,
        conf.extend_args_for_start(&[
            "start",
            "--bind",
            "0.0.0.0",
            "--port",
            &server_port.to_string(),
            "--work-port",
            &worker_port.to_string(),
            "--negotiator-port",
            &negotiator_port.to_string(),
        ]),
    );

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

    // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
    let worker_args = &[
        "work",
        "--queue-addr",
        &queue_addr,
        "--working-dir",
        ".",
        "--run-id",
        &run_id,
    ];
    let worker_args = conf.extend_args_for_client(worker_args);
    let mut worker_proc = spawn_abq(&(name.to_string() + "_workers"), worker_args);

    // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? \
    //   -- __zzz_not_a_command__
    let test_args = &[
        "test",
        "--reporter",
        "dot",
        "--queue-addr",
        &queue_addr,
        "--run-id",
        &run_id,
    ];
    let mut test_args = conf.extend_args_for_client(test_args);
    test_args.extend(["--", "__zzz_not_a_command__"]);

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_forcing_capture(&(name.to_string() + "_test"), test_args);

    // The `abq test` process should exit with a failure
    assert!(!exit_status.success(), "{:?}", (stdout, stderr));

    insta::assert_snapshot!(stderr);

    // The `abq work` process should also exit with a failure, corresponding to having
    // witnessed a test failure in the run.
    let worker_exit_status = worker_proc.wait().unwrap();
    assert!(!worker_exit_status.success());

    term_queue(queue_proc);
}
