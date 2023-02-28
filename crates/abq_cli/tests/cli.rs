#![cfg(test)]
// For convenience in tests.
#![allow(clippy::useless_format)]

use abq_native_runner_simulation::{pack, pack_msgs, pack_msgs_to_disk, Msg::*};
use abq_test_utils::{artifacts_dir, sanitize_output, WORKSPACE};
use abq_utils::auth::{AdminToken, UserToken};
use abq_utils::net_protocol::runners::{
    AbqProtocolVersion, InitSuccessMessage, Manifest, ManifestMessage, Status, Test, TestOrGroup,
};
use abq_utils::net_protocol::runners::{
    NativeRunnerSpecification, ProtocolWitness, RawNativeRunnerSpawnedMessage,
};
use abq_with_protocol_version::with_protocol_version;
use serde_json as json;
use serial_test::serial;
use std::fs::File;
use std::process::{ExitStatus, Output};
use std::time::Duration;
use std::{
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

struct Abq {
    name: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    always_capture_stderr: bool,
    working_dir: Option<PathBuf>,
}

impl Abq {
    fn new(name: impl Into<String>) -> Self {
        Abq {
            name: name.into(),
            args: Default::default(),
            env: Default::default(),
            always_capture_stderr: false,
            working_dir: None,
        }
    }

    fn args<S>(mut self, args: impl IntoIterator<Item = S>) -> Self
    where
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    fn env<K, V>(mut self, env: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.env
            .extend(env.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// If set to `false`, won't capture stderr when ABQ_DEBUG_CLI_TESTS_FOR_CI is set.
    fn always_capture_stderr(mut self, always_capture_stderr: bool) -> Self {
        self.always_capture_stderr = always_capture_stderr;
        self
    }

    fn working_dir(mut self, working_dir: impl Into<PathBuf>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    fn run(self) -> CmdOutput {
        let Output {
            status,
            stdout,
            stderr,
        } = self
            .spawn()
            .wait_with_output()
            .expect("abq cli failed on waiting");

        CmdOutput {
            stdout: String::from_utf8(stdout).expect("abq stdout should be utf-8"),
            stderr: String::from_utf8(stderr).expect("abq stderr should be utf-8"),
            exit_status: status,
        }
    }

    fn spawn(self) -> Child {
        let Self {
            name,
            args,
            env,
            always_capture_stderr,
            working_dir,
        } = self;
        let working_dir = working_dir.unwrap_or_else(|| std::env::current_dir().unwrap());

        let mut cmd = Command::new(abq_binary());

        cmd.args(args);
        cmd.current_dir(working_dir);
        cmd.stdout(Stdio::piped());
        if debug_log_for_ci() && !always_capture_stderr {
            cmd.stderr(Stdio::inherit());
        } else {
            cmd.stderr(Stdio::piped());
        }

        cmd.envs(env);

        if debug_log_for_local_run() && name.contains("_queue") {
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

// By default rustc doesn't infer a monomorphic type for our `run` functions in
// `test_all_network_config_options`, so this forces that.
fn _force_closure_type_inference<'a, F: Fn(&'a str, CSConfigOptions)>(f: F) -> F {
    f
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
            let _test_name: &'static str = stringify!($test_name);
            let run = _force_closure_type_inference($run);
            run(&format!("{}_no_auth_no_tls", _test_name), CSConfigOptions {
                use_auth_token: false,
                tls: false,
            })
        }

        #[test]
        #[serial]
        $(#[$cfg])*
        #[allow(clippy::redundant_closure_call)]
        fn [<$test_name _no_auth_with_tls>]() {
            let _test_name: &'static str = stringify!($test_name);
            let run = _force_closure_type_inference($run);
            run(&format!("{}_no_auth_with_tls", _test_name), CSConfigOptions {
                use_auth_token: false,
                tls: true,
            })
        }

        #[test]
        #[serial]
        $(#[$cfg])*
        #[allow(clippy::redundant_closure_call)]
        fn [<$test_name _with_auth_no_tls>]() {
            let _test_name: &'static str = stringify!($test_name);
            let run = _force_closure_type_inference($run);
            run(&format!("{}_with_auth_no_tls", _test_name), CSConfigOptions {
                use_auth_token: true,
                tls: false,
            })
        }

        #[test]
        #[serial]
        $(#[$cfg])*
        #[allow(clippy::redundant_closure_call)]
        fn [<$test_name _with_auth_with_tls>]() {
            let _test_name: &'static str = stringify!($test_name);
            let run = _force_closure_type_inference($run);
            run(&format!("{}_with_auth_with_tls", _test_name), CSConfigOptions {
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
    yarn_jest_auto_workers_without_failure |name, conf: CSConfigOptions| {
        // abq test --reporter dot (--token ...)? -- yarn jest
        let args = &["test", "--reporter", "dot", "-n", "cpu-cores", "--color=never"];
        let mut args = conf.extend_args_for_in_band_client(args);
        args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr: _,
            exit_status,
        } = Abq::new(name)
            .args(args)
            .working_dir(&testdata_project("jest/npm-jest-project"))
            .run();

        assert!(exit_status.success());
        assert!(stdout.contains("2 tests, 0 failures"), "STDOUT:\n{}", stdout);
        // TODO(130): add back once https://github.com/rwx-research/abq/issues/130 lands; right now
        // this prints more than needed because we don't capture worker stdout/stderr
        // assert_eq!(stdout, "..");
        // assert!(stderr.is_empty(), "{:?}", stderr);
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_without_failure_worker_0 |name, conf: CSConfigOptions| {
        // abq test --worker 0 --reporter dot (--token ...)? -- yarn jest
        let args = &["test", "--worker", "0", "--reporter", "dot", "-n", "cpu-cores", "--color=never"];
        let mut args = conf.extend_args_for_in_band_client(args);
        args.extend(["--", "yarn", "jest"]);
        let CmdOutput {
            stdout,
            stderr: _,
            exit_status,
        } = Abq::new(name)
            .args(args)
            .working_dir(&testdata_project("jest/npm-jest-project"))
            .run();

        assert!(exit_status.success());
        assert!(stdout.contains("2 tests, 0 failures"), "STDOUT:\n{}", stdout);
        // TODO(130): add back once https://github.com/rwx-research/abq/issues/130 lands; right now
        // this prints more than needed because we don't capture worker stdout/stderr
        // assert_eq!(stdout, "..");
        // assert!(stderr.is_empty(), "{:?}", stderr);
    }
}

test_all_network_config_options! {
    queue_open_servers_on_specified_ports |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = Abq::new(name)
            .args(conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={worker_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

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
    }
}

fn assert_sum_of_re<'a>(
    outputs: impl IntoIterator<Item = &'a str>,
    re: regex::Regex,
    expected: usize,
) {
    let mut total_run = 0;
    for output in outputs {
        let tests: usize = re
            .captures(output)
            .and_then(|m| m.get(1))
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or_else(|| panic!("{output}"));
        total_run += tests;
    }
    assert_eq!(total_run, expected);
}

fn assert_sum_of_run_tests<'a>(outputs: impl IntoIterator<Item = &'a str>, expected: usize) {
    let re_run = regex::Regex::new(r"(\d+) tests, \d+ failures").unwrap();
    assert_sum_of_re(outputs, re_run, expected);
}

fn assert_sum_of_run_test_failures<'a>(
    outputs: impl IntoIterator<Item = &'a str>,
    expected: usize,
) {
    let re_run = regex::Regex::new(r"\d+ tests, (\d+) failures").unwrap();
    assert_sum_of_re(outputs, re_run, expected);
}

fn assert_sum_of_run_test_retries<'a>(outputs: impl IntoIterator<Item = &'a str>, expected: usize) {
    let re = regex::Regex::new(r"\d+ tests, \d+ failures, (\d+) retried").unwrap();
    let mut total_run = 0;
    for output in outputs {
        let tests: usize = re
            .captures(output)
            .and_then(|m| m.get(1))
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(0);
        total_run += tests;
    }
    assert_eq!(total_run, expected);
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_numbered_workers_test_without_failure |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = Abq::new(name)
            .args(
                conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={worker_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

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
        let test1_proc = Abq::new(name.to_string() + "_test1").args(test_args(1)).spawn();

        // --worker 0
        let worker0_output = Abq::new(name.to_string() + "_test0").args(test_args(0)).run();
        let worker1_output = test1_proc.wait_with_output().unwrap();

        let worker1_stdout = String::from_utf8_lossy(&worker1_output.stdout);

        assert!(worker0_output.exit_status.success(), "{:?}", (worker0_output.stdout, worker0_output.stderr));
        assert!(worker1_output.status.success(), "{:?}", (worker1_output.stdout, worker1_output.stderr));

        assert!(worker0_output.stdout.contains("0 failures"), "STDOUT:\n{}", worker0_output.stdout);
        assert!(worker1_stdout.contains("0 failures"), "STDOUT:\n{worker1_stdout}");

        assert_sum_of_run_tests([&worker0_output.stdout, worker1_stdout.as_ref()], 2);

        term_queue(queue_proc);
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    #[ignore = "TODO(1.3-timeout): enforce timeout on the worker side"]
    yarn_jest_timeout_run_workers |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = Abq::new(name)
            .args(conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={worker_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

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

        let CmdOutput { stdout, stderr, exit_status } =
            Abq::new(name.to_string() + "_test0")
            .args(test_args(0))
            .working_dir(&testdata_project("jest/npm-jest-project"))
            .always_capture_stderr(true)
            .run();
        assert!(!exit_status.success());
        assert!(stderr.contains("--- ERROR ---\nThe test run timed out"), "STDOUT:\n{}STDERR:\n{}", stdout, stderr);

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_test1").args(test_args(1)).run();

        // The worker should exit with a failure as well.
        assert!(!exit_status.success(), "EXIT:\n{:?}\nSTDOUT:\n{}\nSTDERR:\n{}", exit_status, stdout, stderr);

        term_queue(queue_proc);
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_auto_workers_with_failing_tests |name, conf: CSConfigOptions| {
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
        } = Abq::new(name)
            .args(test_args)
            .working_dir(&testdata_project("jest/npm-jest-project-with-failures"))
            .run();

        let code = exit_status.code().expect("process killed");
        assert_eq!(code, 1);

        let stdout_lines = stdout.lines();
        // TODO(130): add back once https://github.com/rwx-research/abq/issues/130 lands; right now
        // this prints more than needed because we don't capture worker stdout/stderr
        // assert_eq!(stdout_lines.next().unwrap(), "F");
        // assert!(stderr.is_empty(), "{:?}", stderr);
        assert!(stdout_lines.into_iter().any(|line| line.contains('F')));
        assert!(stdout.contains("2 tests, 2 failures"), "STDOUT:\n{}", stdout);
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_workers_with_failing_tests |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project-with-failures");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = Abq::new(name)
            .args(conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={worker_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

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
        let test1_proc = Abq::new(name.to_string() + "_test1").args(test_args(1)).spawn();

        // --worker 0
        let worker0_output = Abq::new(name.to_string() + "_test0").args(test_args(0)).run();
        let worker1_output = test1_proc.wait_with_output().unwrap();

        let worker1_stdout = String::from_utf8_lossy(&worker1_output.stdout);

        assert_sum_of_run_tests([&worker0_output.stdout, worker1_stdout.as_ref()], 2);
        assert_sum_of_run_test_failures([&worker0_output.stdout, worker1_stdout.as_ref()], 2);

        // At least one of the workers should fail with a non-zero code.
        assert!(!worker0_output.exit_status.success() || !worker1_output.status.success());

        term_queue(queue_proc);
    }
}

test_all_network_config_options! {
    healthcheck_queue_success |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let work_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let work_scheduler_addr = format!("0.0.0.0:{work_port}");
        let negotiator_addr = format!("0.0.0.0:{negotiator_port}");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = Abq::new(name)
            .args(conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={work_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

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
        } = Abq::new(name.to_string() + "_health")
            .args(conf.extend_args_for_client(vec![
                format!("health"),
                format!("--queue={queue_addr}"),
                format!("--work-scheduler={work_scheduler_addr}"),
                format!("--negotiator={negotiator_addr}"),
            ]))
            .run();

        assert!(exit_status.success(), "{}\n{}", stdout, stderr);
        assert!(!stdout.contains("UNHEALTHY"));
        assert!(stderr.is_empty());

        term_queue(queue_proc);
    }
}

test_all_network_config_options! {
    healthcheck_queue_failure |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let work_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let work_scheduler_addr = format!("0.0.0.0:{work_port}");
        let negotiator_addr = format!("0.0.0.0:{negotiator_port}");

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = Abq::new(name)
            .args(conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={work_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

        queue_proc.kill().expect("queue already dead");

        // Check health
        // abq health --queue ... --work-scheduler ... --negotiator ... (--token ...)?
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_health")
            .args(conf.extend_args_for_client(vec![
                format!("health"),
                format!("--queue={queue_addr}"),
                format!("--work-scheduler={work_scheduler_addr}"),
                format!("--negotiator={negotiator_addr}"),
            ]))
            .run();

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
    }
}

#[test]
#[serial]
fn work_user_token_without_admin_token() {
    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new("work_no_queue_addr_or_access_token")
        .args([
            "start",
            &format!("--user-token={}", UserToken::new_random()),
        ])
        .always_capture_stderr(true)
        .run();

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
    } = Abq::new("work_no_queue_addr_or_access_token")
        .args([
            "start",
            &format!("--admin-token={}", AdminToken::new_random()),
        ])
        .always_capture_stderr(true)
        .run();

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
    } = Abq::new("invalid_abq_option_before_test_command")
        .args([
            "test",
            "--zzz-not-an-abq-option",
            "--",
            "bundle",
            "exec",
            "rspec",
        ])
        .always_capture_stderr(true)
        .run();

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
    let mut queue_proc = Abq::new(name)
        .args(conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind"),
            format!("0.0.0.0"),
            format!("--port"),
            format!("{server_port}"),
            format!("--work-port"),
            format!("{worker_port}"),
            format!("--negotiator-port"),
            format!("{negotiator_port}"),
        ]))
        .spawn();

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

    let test_args = |worker: usize| {
        let args = vec![
            format!("test"),
            format!("--worker={worker}"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("-n=1"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), s!("__zzz_not_a_command__")]);
        args
    };

    let mut worker1 = Abq::new(name.to_string() + "_worker1")
        .args(test_args(1))
        .spawn();

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new(name.to_string() + "_worker0")
        .args(test_args(0))
        .always_capture_stderr(true)
        .run();

    // The worker 0 process should exit with a failure
    assert!(!exit_status.success(), "{:?}", (stdout, stderr));

    let stderr = sanitize_output(&stderr);

    insta::assert_snapshot!(stderr);

    // The worker 1 process should also exit with a failure, corresponding to having
    // witnessed a test failure in the run.
    let worker_exit_status = worker1.wait().unwrap();
    assert!(!worker_exit_status.success());

    term_queue(queue_proc);
}

fn legal_spawned_message(proto: ProtocolWitness) -> RawNativeRunnerSpawnedMessage {
    let protocol_version = proto.get_version();
    let runner_specification = NativeRunnerSpecification {
        name: "test".to_string(),
        version: "0.0.0".to_string(),
        test_framework: "rspec".to_owned(),
        test_framework_version: "3.12.0".to_owned(),
        language: "ruby".to_owned(),
        language_version: "3.1.2p20".to_owned(),
        host: "ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned(),
    };
    RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification)
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    huge_manifest_from_native_runner |name, conf: CSConfigOptions| {
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
        } = Abq::new(name).args(args).run();

        assert!(exit_status.success());
        assert!(stdout.contains("0 tests, 0 failures"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    print_manifest_generation_output |name, conf: CSConfigOptions| {
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
        } = Abq::new( name).args( args).run();

        assert!(exit_status.success(), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("0 tests, 0 failures"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("MANIFEST GENERATION ---"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("init stdouthello from manifest stdout"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("init stderrhello from manifest stderr"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
    }
}

fn verify_and_sanitize_state(state: &mut json::Map<String, json::Value>) {
    {
        let exe_cell = state
            .get_mut("abq_executable")
            .expect("abq_executable missing");
        let exe_path = Path::new(exe_cell.as_str().unwrap());
        assert_eq!(exe_path, abq_binary());
        *exe_cell = json::json!("<replaced abq.exe>");
    }

    {
        let version_cell = state.get_mut("abq_version").expect("abq_version missing");
        assert_eq!(version_cell.as_str().unwrap(), abq_utils::VERSION);
        *version_cell = json::json!("<replaced ABQ version>");
    }
}

#[test]
fn write_statefile_for_worker() {
    let statefile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let statefile = statefile.to_path_buf();

    let server_port = find_free_port();
    let worker_port = find_free_port();
    let negotiator_port = find_free_port();

    let queue_addr = format!("0.0.0.0:{server_port}");

    let mut queue_proc = Abq::new("write_statefile_for_worker_queue")
        .args([
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ])
        .spawn();

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

    let test_args = |worker: usize| {
        vec![
            format!("test"),
            format!("--worker={worker}"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--working-dir=."),
            format!("--run-id=my-test-run-id"),
            format!("--num=cpu-cores"),
            format!("--color=never"),
            format!("--"),
            format!("__zzz_not_a_command__"),
        ]
    };

    let mut worker0 = Abq::new("write_statefile_for_worker0")
        .args(test_args(0))
        .spawn();

    let _worker1 = Abq::new("write_statefile_for_worker0")
        .args(test_args(1))
        .env([("ABQ_STATE_FILE", statefile.display().to_string())])
        .run();

    worker0.kill().unwrap();
    queue_proc.kill().unwrap();

    let statefile = File::open(&statefile).unwrap();
    let mut state = serde_json::from_reader(&statefile).unwrap();

    verify_and_sanitize_state(&mut state);

    insta::assert_json_snapshot!(state, @r###"
    {
      "abq_executable": "<replaced abq.exe>",
      "abq_version": "<replaced ABQ version>",
      "run_id": "my-test-run-id"
    }
    "###);
}

test_all_network_config_options! {
    native_runner_fails_while_executing_test |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");

        let mut queue_proc = Abq::new(name)
            .args(conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={worker_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

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

        // Create a simulation test run to launch a worker with.
        use abq_utils::net_protocol::runners::{ManifestMessage, Manifest, AbqProtocolVersion, InitSuccessMessage, TestOrGroup, Test};
        use abq_native_runner_simulation::{pack, pack_msgs, Msg::*};

        let proto = AbqProtocolVersion::V0_2.get_supported_witness().unwrap();

        let test = TestOrGroup::test(Test::new(proto, "test1", vec![], Default::default()));
        let manifest = ManifestMessage::new(Manifest::new(vec![test], Default::default()));

        let simulation = [
            Connect,
            //
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            //
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
                    Stdout(b"hello from manifest stdout".to_vec()),
                    Stderr(b"hello from manifest stderr".to_vec()),
                    // Finish
                    Exit(0),
                ],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    // Read first test, then bail
                    OpaqueRead,
                    Stdout(b"I failed catastrophically".to_vec()),
                    Stderr(b"For a reason explainable only by a backtrace".to_vec()),
                    Exit(1),
                ],
            },
        ];

        let simulation_msg = pack_msgs(simulation);
        let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let simfile_path = simfile.to_path_buf();
        std::fs::write(&simfile_path, simulation_msg).unwrap();

        let test_args = |worker: usize| {
            let simulator = native_runner_simulation_bin();
            let simfile_path = simfile_path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id=test-run-id"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        // Run the test suite with an in-band worker.
        let CmdOutput {
            stdout,
            stderr: _,
            exit_status,
        } = Abq::new(name.to_string() + "_worker0")
            .args(test_args(0))
            .run();

        assert_eq!(exit_status.code().unwrap(), 101, "should exit with ABQ error");

        let stdout = sanitize_output(&stdout);

        // Make sure the failing message is reflected in captured worker stdout/stderr.

        assert!(stdout.contains(
r#"
--- [worker 0] AFTER completion ---
----- STDOUT
I failed catastrophically
----- STDERR
For a reason explainable only by a backtrace
"#.trim()), "STDOUT:\n{stdout}");

        term_queue(queue_proc);
    }
}

#[test]
#[with_protocol_version]
#[serial]
fn retries_smoke() {
    // Smoke test for retries, that runs a number of tests on a number of workers and makes sure
    // nothing blows up.
    let name = "retries_smoke";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let attempts = 4;
    let num_tests = 64;
    let num_workers = 6;

    let server_port = find_free_port();
    let worker_port = find_free_port();
    let negotiator_port = find_free_port();

    let queue_addr = format!("0.0.0.0:{server_port}");

    let mut queue_proc = Abq::new(name)
        .args(conf.extend_args_for_start(vec![
            format!("start"),
            format!("--bind=0.0.0.0"),
            format!("--port={server_port}"),
            format!("--work-port={worker_port}"),
            format!("--negotiator-port={negotiator_port}"),
        ]))
        .spawn();

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

    let mut manifest = vec![];

    for t in 1..=num_tests {
        manifest.push(TestOrGroup::test(Test::new(
            proto,
            t.to_string(),
            [],
            Default::default(),
        )));
    }

    let proto = AbqProtocolVersion::V0_2.get_supported_witness().unwrap();

    let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));

    let simulation = [
        Connect,
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Write the manifest if we need to.
        // Otherwise handle the one test.
        IfGenerateManifest {
            then_do: vec![OpaqueWrite(pack(&manifest))],
            else_do: {
                let mut run_tests = vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                ];

                for _ in 0..num_tests {
                    // If the socket is alive (i.e. we have a test to run), pull it and give back a
                    // faux result.
                    // Otherwise assume we ran out of tests on our node and exit.
                    run_tests.push(IfAliveReadAndWriteFake(Status::Failure {
                        exception: None,
                        backtrace: None,
                    }));
                }
                run_tests
            },
        },
        //
        // Finish
        Exit(0),
    ];

    let simulation_msg = pack_msgs(simulation);
    let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let simfile_path = simfile.to_path_buf();
    std::fs::write(&simfile_path, simulation_msg).unwrap();

    let test_args = |worker: usize| {
        let simulator = native_runner_simulation_bin();
        let simfile_path = simfile_path.display().to_string();
        let args = vec![
            format!("test"),
            format!("--worker={worker}"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id=test-run-id"),
            format!("--retries={attempts}"),
            format!("-n=1"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let workers: Vec<_> = (0..num_workers)
        .map(|i| {
            Abq::new(format!("{name}_worker{i}"))
                .args(test_args(i))
                .spawn()
        })
        .collect();

    let mut at_least_one_is_failing = false;
    let mut stdouts = vec![];
    for worker in workers {
        let Output { status, stdout, .. } = worker.wait_with_output().unwrap();
        stdouts.push(String::from_utf8_lossy(&stdout).to_string());
        at_least_one_is_failing = at_least_one_is_failing || status.code().unwrap() == 1;
    }

    assert!(at_least_one_is_failing);
    assert_sum_of_run_tests(stdouts.iter().map(|s| s.as_str()), 64);
    assert_sum_of_run_test_failures(stdouts.iter().map(|s| s.as_str()), 64);
    assert_sum_of_run_test_retries(stdouts.iter().map(|s| s.as_str()), 64);

    term_queue(queue_proc);
}

test_all_network_config_options! {
    cancellation_of_active_run_for_workers |name, conf: CSConfigOptions| {
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");
        let run_id = RunId::unique().to_string();

        // abq start --bind ... --port ... --work-port ... --negotiator-port ... (--token ...)?
        let mut queue_proc = Abq::new(name.to_string() + "_queue")
            .args(conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={worker_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .spawn();

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

        let proto = AbqProtocolVersion::V0_2.get_supported_witness().unwrap();

        // Build a simulation that consists of one test, which hangs.
        let manifest = vec![TestOrGroup::test(Test::new(
            proto,
            "test1".to_string(),
            [],
            Default::default(),
        ))];
        let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));

        let simulation = |hang: bool| [
            Connect,
            //
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            //
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![OpaqueWrite(pack(&manifest))],
                else_do: if hang {
                    vec![
                        //
                        // Read init context message + write ACK
                        OpaqueRead,
                        OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                        //
                        // Sleep forever
                        Sleep(Duration::from_secs(600)),
                    ]
                } else {
                    // On the second launch, we should fast-exit.
                    vec![
                        //
                        // Read init context message + write ACK
                        Exit(1)
                    ]
                }
            },
            //
            // Finish
            Exit(0),
        ];

        let simulation = [pack_msgs_to_disk(simulation(true)), pack_msgs_to_disk(simulation(false))];

        // abq test --worker N --reporter dot --queue-addr ... --working-dir ... --run-id ... (--token ...)? -- yarn jest
        let test_args = |worker: usize| {
            let simulator = native_runner_simulation_bin();
            let simulation = &simulation[worker];
            let simfile_path = simulation.path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("--num=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        let mut worker0 = Abq::new(name.to_string() + "_test0").args(test_args(0)).spawn();

        let worker0_stderr = std::mem::take(&mut worker0.stderr).unwrap();
        let mut worker0_reader = BufReader::new(worker0_stderr).lines();
        // Spin until we know the worker0 is UP
        loop {
            if let Some(line) = worker0_reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("Generic test runner for") {
                    break;
                }
            }
        }

        use nix::sys::signal;
        use nix::unistd::Pid;

        // SIGTERM the worker0.
        signal::kill(Pid::from_raw(worker0.id() as _), signal::Signal::SIGTERM).unwrap();

        let worker0_exit = worker0.wait().unwrap();
        assert!(!worker0_exit.success());
        assert_eq!(worker0_exit.code(), Some(1));

        // A second worker should exit immediately with cancellation as well.
        // --worker 1
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_test1").args(test_args(1)).run();

        assert_eq!(exit_status.code().unwrap(), 1, "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");

        term_queue(queue_proc);
    }
}
