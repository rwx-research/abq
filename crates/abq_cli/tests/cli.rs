#![cfg(test)]

use abq_test_utils::{artifacts_dir, WORKSPACE};
use abq_utils::auth::{AdminToken, UserToken};
use abq_utils::net_protocol::runners::{
    NativeRunnerSpecification, ProtocolWitness, RawNativeRunnerSpawnedMessage,
};
use serial_test::serial;
use std::process::{ExitStatus, Output};
use std::{
    ffi::OsStr,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use abq_utils::net_protocol::workers::RunId;

const TLS_CERT: &str = std::concat!(std::env!("ABQ_WORKSPACE_DIR"), "testdata/certs/server.crt");
const TLS_KEY: &str = std::concat!(std::env!("ABQ_WORKSPACE_DIR"), "testdata/certs/server.key");

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
    artifacts_dir().join("abq")
}

#[cfg(feature = "test-abq-jest")]
fn testdata_project(subpath: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(WORKSPACE).join("testdata").join(subpath)
}

fn native_runner_simulation_bin() -> String {
    artifacts_dir()
        .join("abqtest_native_runner_simulation")
        .display()
        .to_string()
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

    cmd.spawn()
        .unwrap_or_else(|_| panic!("{} cli failed to spawn", abq_binary().display()))
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

macro_rules! s {
    ($s:expr) => {
        $s.to_string()
    };
}

/// Configuration options for how clients and servers in an abq run communicate.
/// Namely: whether they use an authentication header or not, and eventually, whether they use TLS
/// or not.
struct CSConfigOptions {
    use_auth_token: bool,
    tls: bool,
}

impl CSConfigOptions {
    fn extend_args_for_start(&self, mut args: Vec<String>) -> Vec<String> {
        if self.use_auth_token {
            args.extend([format!("--user-token={TEST_USER_AUTH_TOKEN}")]);
            args.extend([format!("--admin-token={TEST_ADMIN_AUTH_TOKEN}")]);
        }
        if self.tls {
            args.extend([format!("--tls-cert={TLS_CERT}")]);
            args.extend([format!("--tls-key={TLS_KEY}")]);
        }
        args
    }

    fn extend_args_for_client(&self, mut args: Vec<String>) -> Vec<String> {
        if self.use_auth_token {
            args.extend([format!("--token={TEST_USER_AUTH_TOKEN}")]);
        }
        if self.tls {
            args.extend([format!("--tls-cert={TLS_CERT}")]);
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
        let args = &["test", "--reporter", "dot", "-n", "cpu-cores", "--color=never"];
        let mut args = conf.extend_args_for_in_band_client(args);
        args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr: _,
            exit_status,
        } = run_abq_in(
            name,
            args,
            &testdata_project("jest/npm-jest-project"),
            false,
        );

        assert!(exit_status.success());
        assert!(stdout.contains("2 tests, 0 failures"), "STDOUT:\n{}", stdout);
        // TODO(130): add back once https://github.com/rwx-research/abq/issues/130 lands; right now
        // this prints more than needed because we don't capture worker stdout/stderr
        // assert_eq!(stdout, "..");
        // assert!(stderr.is_empty(), "{:?}", stderr);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_without_failure_worker_0 (|name, conf: CSConfigOptions| {
        // abq test --worker 0 --reporter dot (--token ...)? -- yarn jest
        let args = &["test", "--worker", "0", "--reporter", "dot", "-n", "cpu-cores", "--color=never"];
        let mut args = conf.extend_args_for_in_band_client(args);
        args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr: _,
            exit_status,
        } = run_abq_in(
            name,
            args,
            &testdata_project("jest/npm-jest-project"),
            false,
        );

        assert!(exit_status.success());
        assert!(stdout.contains("2 tests, 0 failures"), "STDOUT:\n{}", stdout);
        // TODO(130): add back once https://github.com/rwx-research/abq/issues/130 lands; right now
        // this prints more than needed because we don't capture worker stdout/stderr
        // assert_eq!(stdout, "..");
        // assert!(stderr.is_empty(), "{:?}", stderr);
    })
}

test_all_network_config_options! {
    queue_open_servers_on_specified_ports (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
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
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        let working_dir = npm_jest_project_path.display();

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = vec![
            format!("work"),
            format!("--queue-addr={queue_addr}"),
            format!("--working-dir={working_dir}"),
            format!("--run-id={run_id}"),
            format!("--num=cpu-cores"),
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let mut worker_proc = spawn_abq(&(name.to_string() + "_worker"), worker_args);

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = vec![
            format!("test"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--color=never"),
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend([s!("--"), s!("yarn"), s!("jest")]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test"), test_args);

        assert!(exit_status.success(), "{:?}", (stdout, stderr));

        let mut lines = stdout.lines();
        assert!(lines.next().unwrap().contains("DEPRECATION NOTICE: not specifying `--worker` will default to `--worker 0` in a future version of ABQ"));
        assert!(lines.next().unwrap().contains("Started test run"));
        assert_eq!(lines.next().unwrap(), "..");
        assert!(stdout.contains("2 tests, 0 failures"), "STDOUT:\n{}", stdout);

        assert!(stderr.is_empty());

        let worker_exit_status = worker_proc.wait().unwrap();
        assert!(worker_exit_status.success());

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_numbered_workers_test_without_failure (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        let working_dir = npm_jest_project_path.display();

        // abq test --worker N --reporter dot --queue-addr ... --working-dir ... --run-id ... (--token ...)? -- yarn jest
        let test_args = |worker: usize| {
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--working-dir={working_dir}"),
                format!("--run-id={run_id}"),
                format!("--num=cpu-cores"),
                format!("--color=never"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), s!("yarn"), s!("jest")]);
            args
        };

        // --worker 1
        let mut test1_proc = spawn_abq(&(name.to_string() + "_test1"), test_args(1));

        // --worker 0
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test0"), test_args(0));

        assert!(exit_status.success(), "{:?}", (stdout, stderr));

        let mut lines = stdout.lines();
        assert!(lines.next().unwrap().contains("Started test run"));
        assert_eq!(lines.next().unwrap(), "..");
        assert!(stdout.contains("2 tests, 0 failures"), "STDOUT:\n{}", stdout);

        assert!(stderr.is_empty());

        let test1_exit_status = test1_proc.wait().unwrap();
        assert!(test1_exit_status.success());

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
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = vec![
            format!("test"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend([s!("--"), s!("yarn"), s!("jest")]);
        let mut supervisor = spawn_abq(&(name.to_string() + "_test"), test_args);

        let supervisor_stdout = supervisor.stdout.as_mut().unwrap();
        let mut supervisor_reader = BufReader::new(supervisor_stdout).lines();
        // Spin until we know the supervisor is UP
        loop {
            if let Some(line) = supervisor_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Started test run") {
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

        let working_dir = npm_jest_project_path.display();

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = vec![
            format!("work"),
            format!("--queue-addr={queue_addr}"),
            format!("--working-dir={working_dir}"),
            format!("--run-id={run_id}"),
            format!("--num=cpu-cores"),
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_worker"), worker_args);

        // The worker should exit with a failure as well.
        assert!(!exit_status.success(), "EXIT:\n{:?}\nSTDOUT:\n{}\nSTDERR:\n{}", exit_status, stdout, stderr);

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_cancel_run_before_workers_numbered (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        let working_dir = npm_jest_project_path.display();

        // abq test --worker N --reporter dot --queue-addr ... --working-dir ... --run-id ... (--token ...)? -- yarn jest
        let test_args = |worker: usize| {
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--working-dir={working_dir}"),
                format!("--run-id={run_id}"),
                format!("--num=cpu-cores"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), s!("yarn"), s!("jest")]);
            args
        };

        let mut supervisor = spawn_abq(&(name.to_string() + "_test0"), test_args(0));

        let supervisor_stdout = supervisor.stdout.as_mut().unwrap();
        let mut supervisor_reader = BufReader::new(supervisor_stdout).lines();
        // Spin until we know the supervisor is UP
        loop {
            if let Some(line) = supervisor_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Started test run") {
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

        // --worker 1
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test1"), test_args(1));

        // The worker should exit with a failure as well.
        assert!(!exit_status.success(), "EXIT:\n{:?}\nSTDOUT:\n{}\nSTDERR:\n{}", exit_status, stdout, stderr);

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_timeout_run (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = vec![
            format!("test"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--inactivity-timeout-seconds=0"),
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend([s!("--"), s!("yarn"), s!("jest")]);
        let CmdOutput { stdout, stderr, exit_status } = run_abq_in(
            &(name.to_string() + "_test"),
            test_args,
            &testdata_project("jest/npm-jest-project"),
            true,
        );
        assert!(!exit_status.success());
        assert!(stderr.contains("--- ERROR ---\nTimed out"), "STDOUT:\n{}STDERR:\n{}", stdout, stderr);

        let working_dir = npm_jest_project_path.display();

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = vec![
            format!("work"),
            format!("--queue-addr={queue_addr}"),
            format!("--working-dir={working_dir}"),
            format!("--run-id={run_id}"),
            format!("--num=cpu-cores"),
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_worker"), worker_args);

        // The worker should exit with a failure as well.
        assert!(!exit_status.success(), "EXIT:\n{:?}\nSTDOUT:\n{}\nSTDERR:\n{}", exit_status, stdout, stderr);

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_timeout_run_test_timeout_seconds_alias (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = vec![
            format!("test"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--test-timeout-seconds=0"),
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend([s!("--"), s!("yarn"), s!("jest")]);
        let CmdOutput { stdout, stderr, exit_status } = run_abq_in(
            &(name.to_string() + "_test"),
            test_args,
            &testdata_project("jest/npm-jest-project"),
            true,
        );
        assert!(!exit_status.success());
        assert!(stderr.contains("--- ERROR ---\nTimed out"), "STDOUT:\n{}STDERR:\n{}", stdout, stderr);

        let working_dir = npm_jest_project_path.display();

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = vec![
            format!("work"),
            format!("--queue-addr={queue_addr}"),
            format!("--working-dir={working_dir}"),
            format!("--run-id={run_id}"),
            format!("--num=cpu-cores"),
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_worker"), worker_args);

        // The worker should exit with a failure as well.
        assert!(!exit_status.success(), "EXIT:\n{:?}\nSTDOUT:\n{}\nSTDERR:\n{}", exit_status, stdout, stderr);

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_timeout_run_workers_numbered (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        let working_dir = npm_jest_project_path.display();

        // abq test --worker N --reporter dot --queue-addr ... --working-dir ... --run-id ... (--token ...)? -- yarn jest
        let test_args = |worker: usize| {
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--working-dir={working_dir}"),
                format!("--run-id={run_id}"),
                format!("--num=cpu-cores"),
                format!("--inactivity-timeout-seconds=0"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), s!("yarn"), s!("jest")]);
            args
        };

        let CmdOutput { stdout, stderr, exit_status } = run_abq_in(
            &(name.to_string() + "_test0"),
            test_args(0),
            &testdata_project("jest/npm-jest-project"),
            true,
        );
        assert!(!exit_status.success());
        assert!(stderr.contains("--- ERROR ---\nTimed out"), "STDOUT:\n{}STDERR:\n{}", stdout, stderr);

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test1"), test_args(1));

        // The worker should exit with a failure as well.
        assert!(!exit_status.success(), "EXIT:\n{:?}\nSTDOUT:\n{}\nSTDERR:\n{}", exit_status, stdout, stderr);

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
            "-n",
            "cpu-cores",
            "--color=never",
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
        assert_eq!(code, 1, "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");

        let stdout_lines = stdout.lines();
        // TODO(130): add back once https://github.com/rwx-research/abq/issues/130 lands; right now
        // this prints more than needed because we don't capture worker stdout/stderr
        // assert_eq!(stdout_lines.next().unwrap(), "F");
        // assert!(stderr.is_empty(), "{:?}", stderr);
        assert!(stdout_lines.into_iter().any(|line| line.contains('F')));
        assert!(stdout.contains("2 tests, 2 failures"), "STDOUT:\n{}", stdout);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_with_failing_tests_worker0 (|name, conf: CSConfigOptions| {
        // abq test --reporter dot (--token ...)? -- yarn jest
        let test_args = &[
            "test",
            "--reporter",
            "dot",
            "-n",
            "cpu-cores",
            "--color=never",
            "--worker=0",
        ];
        let mut test_args = conf.extend_args_for_in_band_client(test_args);
        test_args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr: _,
            exit_status,
        } = run_abq_in(
            name,
            test_args,
            &testdata_project("jest/npm-jest-project-with-failures"),
            false
        );

        let code = exit_status.code().expect("process killed");
        assert_eq!(code, 1);

        let stdout_lines = stdout.lines();
        // TODO(130): add back once https://github.com/rwx-research/abq/issues/130 lands; right now
        // this prints more than needed because we don't capture worker stdout/stderr
        // assert_eq!(stdout_lines.next().unwrap(), "F");
        // assert!(stderr.is_empty(), "{:?}", stderr);
        assert!(stdout_lines.into_iter().any(|line| line.contains('F')));
        assert!(stdout.contains("2 tests, 2 failures"), "STDOUT:\n{}", stdout);
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
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        let working_dir = npm_jest_project_path.display();

        // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
        let worker_args = vec![
            format!("work"),
            format!("--queue-addr={queue_addr}"),
            format!("--working-dir={working_dir}"),
            format!("--run-id={run_id}"),
            format!("--num=cpu-cores"),
        ];
        let worker_args = conf.extend_args_for_client(worker_args);
        let mut worker_proc = spawn_abq(&(name.to_string() + "_workers"), worker_args);

        // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? -- yarn jest
        let test_args = vec![
            format!("test"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--color=never"),
        ];
        let mut test_args = conf.extend_args_for_client(test_args);
        test_args.extend([s!("--"), s!("yarn"), s!("jest")]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test"), test_args);

        // The `abq test` process should exit with a failure
        assert!(!exit_status.success(), "{:?}", (stdout, stderr));

        let mut lines = stdout.lines();
        assert!(lines.next().unwrap().contains("DEPRECATION NOTICE: not specifying `--worker` will default to `--worker 0` in a future version of ABQ"));
        assert!(lines.next().unwrap().contains("Started test run"));
        assert_eq!(lines.next().unwrap(), "FF");
        assert!(stdout.contains("2 tests, 2 failures"), "STDOUT:\n{}", stdout);

        assert!(stderr.is_empty());

        // The `abq work` process should also exit with a failure, corresponding to having
        // witnessed a test failure in the run.
        let worker_exit_status = worker_proc.wait().unwrap();
        assert!(!worker_exit_status.success());

        term_queue(queue_proc);
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_numbered_workers_with_failing_tests (|name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project-with-failures");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start") {
                    break;
                }
            }
        }

        let working_dir = npm_jest_project_path.display();

        // abq test --worker N --reporter dot --queue-addr ... --working-dir ... --run-id ... (--token ...)? -- yarn jest
        let test_args = |worker: usize| {
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--working-dir={working_dir}"),
                format!("--run-id={run_id}"),
                format!("--num=cpu-cores"),
                format!("--color=never"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), s!("yarn"), s!("jest")]);
            args
        };
        let mut test1_proc = spawn_abq(&(name.to_string() + "_test1"), test_args(1));

        // --worker 0
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_test0"), test_args(0));

        // The `abq test` process should exit with a failure
        assert!(!exit_status.success(), "{:?}", (stdout, stderr));

        let mut lines = stdout.lines();
        assert!(lines.next().unwrap().contains("Started test run"));
        assert_eq!(lines.next().unwrap(), "FF");
        assert!(stdout.contains("2 tests, 2 failures"), "STDOUT:\n{}", stdout);

        assert!(stderr.is_empty());

        // The `abq work` process should also exit with a failure, corresponding to having
        // witnessed a test failure in the run.
        let worker_exit_status = test1_proc.wait().unwrap();
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
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={work_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        let mut queue_reader = BufReader::new(queue_stdout).lines();
        // Spin until we know the queue is UP
        loop {
            if let Some(line) = queue_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Run the following to start one or more workers") {
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
        } = run_abq(&(name.to_string() + "_health"), conf.extend_args_for_client(vec![
            format!("health"),
            format!("--queue={queue_addr}"),
            format!("--work-scheduler={work_scheduler_addr}"),
            format!("--negotiator={negotiator_addr}"),
        ]));

        assert!(exit_status.success(), "{}\n{}", stdout, stderr);
        assert!(!stdout.contains("UNHEALTHY"));
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
        let mut queue_proc = spawn_abq(name, conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={work_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]));

        queue_proc.kill().expect("queue already dead");

        // Check health
        // abq health --queue ... --work-scheduler ... --negotiator ... (--token ...)?
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq(&(name.to_string() + "_health"), conf.extend_args_for_client(vec![
            format!("health"),
            format!("--queue={queue_addr}"),
            format!("--work-scheduler={work_scheduler_addr}"),
            format!("--negotiator={negotiator_addr}"),
        ]));

        assert_eq!(exit_status.code().unwrap(), 1, "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert_eq!(
            stdout,
            format!(
                r#"Queue at {queue_addr}: UNHEALTHY
Work scheduler at {work_scheduler_addr}: UNHEALTHY
Negotiator at {negotiator_addr}: UNHEALTHY
"#
            )
        );
        assert!(stderr.is_empty());
    })
}

#[test]
#[serial]
fn work_no_queue_addr_or_access_token() {
    // Spawn worker without a queue addr or access token
    // abq work --working-dir . run-id
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_forcing_capture(
        "work_no_queue_addr_or_access_token",
        ["work", "--run-id", "run-id", "--num", "cpu-cores"],
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
        "work_no_queue_addr_or_access_token",
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
        "work_no_queue_addr_or_access_token",
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
fn invalid_abq_option_before_test_command() {
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = run_abq_forcing_capture(
        "invalid_abq_option_before_test_command",
        [
            "test",
            "--zzz-not-an-abq-option",
            "--",
            "bundle",
            "exec",
            "rspec",
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
        conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind"),
            format!("0.0.0.0"),
            format!("--port"),
            format!("{server_port}"),
            format!("--work-port"),
            format!("{worker_port}"),
            format!("--negotiator-port"),
            format!("{negotiator_port}"),
        ]),
    );

    let queue_stdout = queue_proc.stdout.as_mut().unwrap();
    let mut queue_reader = BufReader::new(queue_stdout).lines();
    // Spin until we know the queue is UP
    loop {
        if let Some(line) = queue_reader.next() {
            let line = line.expect("line is not a string");
            if line.contains("Run the following to start") {
                break;
            }
        }
    }

    // abq work --queue-addr ... --working-dir ... --run-id ... (--token ...)?
    let worker_args = vec![
        format!("work"),
        format!("--queue-addr"),
        format!("{queue_addr}"),
        format!("--working-dir"),
        format!("."),
        format!("--run-id"),
        format!("{run_id}"),
    ];
    let worker_args = conf.extend_args_for_client(worker_args);
    let mut worker_proc = spawn_abq(&(name.to_string() + "_workers"), worker_args);

    // abq test --reporter dot --queue-addr ... --run-id ... (--token ...)? \
    //   -- __zzz_not_a_command__
    let test_args = vec![
        format!("test"),
        format!("--reporter"),
        format!("dot"),
        format!("--queue-addr"),
        format!("{queue_addr}"),
        format!("--run-id"),
        format!("{run_id}"),
    ];
    let mut test_args = conf.extend_args_for_client(test_args);
    test_args.extend([s!("--"), s!("__zzz_not_a_command__")]);

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

fn legal_spawned_message(proto: ProtocolWitness) -> RawNativeRunnerSpawnedMessage {
    let protocol_version = proto.get_version();
    let runner_specification = NativeRunnerSpecification {
        name: "test".to_string(),
        version: "0.0.0".to_string(),
        test_framework: Some("rspec".to_owned()),
        test_framework_version: Some("3.12.0".to_owned()),
        language: Some("ruby".to_owned()),
        language_version: Some("3.1.2p20".to_owned()),
        host: Some("ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned()),
    };
    RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification)
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    huge_manifest_from_native_runner (|name, conf: CSConfigOptions| {
        use abq_utils::net_protocol::runners::{ManifestMessage, Manifest, MetadataMap, AbqProtocolVersion, InitSuccessMessage};
        use abq_native_runner_simulation::{pack, pack_msgs, Msg::*};

        let proto = AbqProtocolVersion::V0_2.get_supported_witness().unwrap();

        let mut meta = MetadataMap::default();
        let value = "y".repeat(1000);
        for i in 0..1_000 {
            meta.insert(i.to_string(), value.clone().into());
        }
        let huge_manifest = ManifestMessage::new(Manifest::new(vec![], meta));

        assert!(
            abq_utils::net_protocol::validate_max_message_size(
                serde_json::to_vec(&huge_manifest).unwrap().len() as _
            ).is_err()
        );

        let simulation = [
            Connect,
            //
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            //
            // Write the manifest if we need to.
            // Otherwise we should get no requests for tests.
            IfGenerateManifest {
                then_do: vec![OpaqueWrite(pack(&huge_manifest))],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                ],
            },
            //
            // Finish
            Exit(0),
        ];

        let simulation_msg = pack_msgs(simulation);
        let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let simfile_path = simfile.to_path_buf();
        std::fs::write(&simfile_path, simulation_msg).unwrap();

        let simulator = native_runner_simulation_bin();
        let simfile_path = simfile_path.display().to_string();

        // abq test --reporter dot (--token ...)? -- simulator
        let args = &["test", "--reporter", "dot", "-n", "1"];
        let mut args = conf.extend_args_for_in_band_client(args);
        args.extend(["--", simulator.as_str(), simfile_path.as_str()]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq_in(
            name,
            args,
            &std::env::current_dir().unwrap(),
            false,
        );

        assert!(exit_status.success());
        assert!(stdout.contains("0 tests, 0 failures"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
    })
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    print_manifest_generation_output (|name, conf: CSConfigOptions| {
        use abq_utils::net_protocol::runners::{ManifestMessage, Manifest, MetadataMap, AbqProtocolVersion, InitSuccessMessage};
        use abq_native_runner_simulation::{pack, pack_msgs, Msg::*};

        let proto = AbqProtocolVersion::V0_2.get_supported_witness().unwrap();

        let mut meta = MetadataMap::default();
        let value = "y".repeat(10);
        for i in 0..10 {
            meta.insert(i.to_string(), value.clone().into());
        }
        let manifest = ManifestMessage::new(Manifest::new(vec![], meta));

        let simulation = [
            Connect,
            Stdout(b"init stdout".to_vec()),
            Stderr(b"init stderr".to_vec()),
            //
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            //
            // Write the manifest if we need to.
            // Otherwise we should get no requests for tests.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
                    Stdout(b"hello from manifest stdout".to_vec()),
                    Stderr(b"hello from manifest stderr".to_vec()),
                ],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                ],
            },
            //
            // Finish
            Exit(0),
        ];

        let simulation_msg = pack_msgs(simulation);
        let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let simfile_path = simfile.to_path_buf();
        std::fs::write(&simfile_path, simulation_msg).unwrap();

        let simulator = native_runner_simulation_bin();
        let simfile_path = simfile_path.display().to_string();

        // abq test --reporter dot (--token ...)? -- simulator
        let args = &["test", "--reporter", "dot", "-n", "1"];
        let mut args = conf.extend_args_for_in_band_client(args);
        args.extend(["--", simulator.as_str(), simfile_path.as_str()]);
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = run_abq_in(
            name,
            args,
            &std::env::current_dir().unwrap(),
            false,
        );

        assert!(exit_status.success(), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("0 tests, 0 failures"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("MANIFEST GENERATION ---"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("init stdouthello from manifest stdout"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("init stderrhello from manifest stderr"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
    })
}
