#![cfg(test)]
// For convenience in tests.
#![allow(clippy::useless_format)]

use abq_hosted::AccessToken;
use abq_native_runner_simulation::{pack, pack_msgs, pack_msgs_to_disk, Msg::*};
use abq_test_utils::{artifacts_dir, s, sanitize_output, WORKSPACE};
use abq_utils::auth::{AdminToken, UserToken};
use abq_utils::net_protocol::runners::{
    AbqProtocolVersion, InitSuccessMessage, Manifest, ManifestMessage, RawTestResultMessage,
    Status, Test, TestOrGroup,
};
use abq_utils::net_protocol::runners::{
    NativeRunnerSpecification, ProtocolWitness, RawNativeRunnerSpawnedMessage,
};
use abq_with_protocol_version::with_protocol_version;
use indoc::formatdoc;
use regex::Regex;
use serde_json as json;
use serial_test::serial;
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::process::{ChildStderr, ChildStdout, ExitStatus, Output};
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};
use tempfile::{NamedTempFile, TempDir};

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
    inherit: bool,
}

struct AbqProc(Option<Child>);

impl Drop for AbqProc {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
        }
    }
}

impl Deref for AbqProc {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl DerefMut for AbqProc {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}

impl AbqProc {
    fn wait_with_output(mut self) -> std::io::Result<Output> {
        self.0.take().unwrap().wait_with_output()
    }
}

impl Abq {
    fn new(name: impl Into<String>) -> Self {
        Abq {
            name: name.into(),
            args: Default::default(),
            env: Default::default(),
            always_capture_stderr: false,
            working_dir: None,
            inherit: false,
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

    // For debugging, pipe stdout/stderr to parent.
    #[allow(unused)]
    fn inherit(mut self) -> Self {
        self.inherit = true;
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

    fn spawn(self) -> AbqProc {
        let Self {
            name,
            args,
            env,
            always_capture_stderr,
            working_dir,
            inherit,
        } = self;
        let working_dir = working_dir.unwrap_or_else(|| std::env::current_dir().unwrap());

        let mut cmd = Command::new(abq_binary());

        cmd.args(args);
        cmd.current_dir(working_dir);

        if inherit {
            cmd.stdout(Stdio::inherit());
            cmd.stderr(Stdio::inherit());
        } else {
            cmd.stdout(Stdio::piped());
            if debug_log_for_ci() && !always_capture_stderr {
                cmd.stderr(Stdio::inherit());
            } else {
                cmd.stderr(Stdio::piped());
            }
        }

        cmd.envs(env);

        if debug_log_for_local_run() {
            cmd.env("ABQ_LOGFILE", format!("{name}.debug"));
            cmd.env("ABQ_LOG", "abq=debug");
        }
        if debug_log_for_ci() && !always_capture_stderr {
            cmd.env("ABQ_LOGCI_WITH_PREFIX", name);
            cmd.env("ABQ_LOG", "abq=debug");
        }

        let child = cmd
            .spawn()
            .unwrap_or_else(|_| panic!("{} cli failed to spawn", abq_binary().display()));
        AbqProc(Some(child))
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

/// Configuration options for how clients and servers in an abq run communicate.
/// Namely: whether they use an authentication header or not, and eventually, whether they use TLS
/// or not.
#[derive(Clone, Copy)]
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

fn term(mut queue_proc: AbqProc) {
    queue_proc.kill().unwrap();
}

fn wait_for_live_queue(queue_stdout: &mut ChildStdout) {
    let mut queue_reader = BufReader::new(queue_stdout).lines();
    // Spin until we know the queue is UP
    loop {
        if let Some(line) = queue_reader.next() {
            let line = line.expect("line is not a string");
            if line.contains("Run the following to invoke a test run") {
                break;
            }
        }
    }
}

fn wait_for_live_worker(worker_stderr: &mut ChildStderr) {
    let mut worker_reader = BufReader::new(worker_stderr).lines();
    // Spin until we know the worker0 is UP
    eprintln!("HI");
    loop {
        if let Some(line) = worker_reader.next() {
            let line = line.expect("line is not a string");
            eprintln!("{}", line);
            if line.contains("Generic test runner for") {
                break;
            }
        }
    }
}

// Waits for the debug line "starting execution of all tests" in the worker.
fn wait_for_worker_executing(worker_stderr: &mut ChildStderr) {
    let mut worker_reader = BufReader::new(worker_stderr).lines();
    // Spin until we know the worker0 is UP
    loop {
        if let Some(line) = worker_reader.next() {
            let line = line.expect("line is not a string");
            if line.contains("starting execution of all tests") {
                break;
            }
        }
    }
}

macro_rules! setup_queue {
    ($name:expr, $conf:expr) => {
        setup_queue!($name, $conf, env: Vec::<(String, String)>::new())
    };
    ($name:expr, $conf:expr, env:$env:expr) => {{
        let server_port = find_free_port();
        let worker_port = find_free_port();
        let negotiator_port = find_free_port();

        let queue_addr = format!("0.0.0.0:{server_port}");

        let mut queue_proc = Abq::new($name)
            .args($conf.extend_args_for_start(vec![
                format!("start"),
                format!("--bind=0.0.0.0"),
                format!("--port={server_port}"),
                format!("--work-port={worker_port}"),
                format!("--negotiator-port={negotiator_port}"),
            ]))
            .env($env)
            .spawn();

        let queue_stdout = queue_proc.stdout.as_mut().unwrap();
        wait_for_live_queue(queue_stdout);

        (queue_proc, queue_addr)
    }};
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
        wait_for_live_queue(queue_stdout);

        assert!(port_active(server_port));
        assert!(port_active(worker_port));
        assert!(port_active(negotiator_port));

        term(queue_proc)
    }
}

fn assert_sum_of_re<'a>(
    outputs: impl IntoIterator<Item = &'a str>,
    re: regex::Regex,
    expected: usize,
) {
    let mut total_run = 0;
    for output in outputs {
        total_run += get_num_of_re(&re, output);
    }
    assert_eq!(total_run, expected);
}

fn get_num_of_re(re: &Regex, s: &str) -> usize {
    re.captures(s)
        .and_then(|m| m.get(1))
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or_else(|| panic!("{s}"))
}

fn re_run() -> Regex {
    regex::Regex::new(r"(\d+) tests, \d+ failures").unwrap()
}

fn re_failed() -> Regex {
    regex::Regex::new(r"\d+ tests, (\d+) failures").unwrap()
}

fn assert_sum_of_run_tests<'a>(outputs: impl IntoIterator<Item = &'a str>, expected: usize) {
    assert_sum_of_re(outputs, re_run(), expected);
}

fn assert_sum_of_run_test_failures<'a>(
    outputs: impl IntoIterator<Item = &'a str>,
    expected: usize,
) {
    assert_sum_of_re(outputs, re_failed(), expected);
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
        let (queue_proc, queue_addr) = setup_queue!(name, conf);

        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

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

        // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
        let report_args = {
            let args = vec![
                format!("report"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("--color=never"),
            ];
            conf.extend_args_for_client(args)
        };

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_report")
            .args(report_args)
            .always_capture_stderr(true)
            .run();

        assert!(exit_status.success(), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("2 tests, 0 failures"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");

        term(queue_proc);
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_timeout_run_workers |name, conf: CSConfigOptions| {
        let (queue_proc, queue_addr) = setup_queue!(name, conf);

        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project");

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
                //format!("--num=cpu-cores"),
                format!("--inactivity-timeout-seconds=0"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), s!("yarn"), s!("jest")]);
            args
        };

        let CmdOutput { stdout, stderr, exit_status } =
            Abq::new(name.to_string() + "_worker0")
            .args(test_args(0))
            .working_dir(&testdata_project("jest/npm-jest-project"))
            .run();
        assert!(!exit_status.success());
        assert!(stdout.contains("-- Test Timeout --"), "STDOUT:\n{}STDERR:\n{}", stdout, stderr);

        term(queue_proc);
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
        assert!(stdout_lines.into_iter().any(|line| line.contains('F')));
        assert!(stdout.contains("2 tests, 2 failures"), "STDOUT:\n{}", stdout);
    }
}

test_all_network_config_options! {
    #[cfg(feature = "test-abq-jest")]
    yarn_jest_separate_queue_workers_with_failing_tests |name, conf: CSConfigOptions| {
        let (queue_proc, queue_addr) = setup_queue!(name, conf);

        let run_id = RunId::unique().to_string();

        let npm_jest_project_path = testdata_project("jest/npm-jest-project-with-failures");
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

        // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
        let report_args = {
            let args = vec![
                format!("report"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("--color=never"),
            ];
            conf.extend_args_for_client(args)
        };

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_report")
            .args(report_args)
            .always_capture_stderr(true)
            .run();

        assert!(!exit_status.success(), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("2 tests, 2 failures"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");

        term(queue_proc);
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
        wait_for_live_queue(queue_stdout);

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

        term(queue_proc);
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

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

    let run_id = RunId::unique().to_string();

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

    // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
    // ABQ report should error with a message that the test suite failed to run.
    let report_args = {
        let args = vec![
            format!("report"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--color=never"),
        ];
        conf.extend_args_for_client(args)
    };

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new(name.to_string() + "_report")
        .args(report_args)
        .always_capture_stderr(true)
        .run();

    assert!(
        !exit_status.success(),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );
    assert!(
        stderr.contains("failed to fetch test results"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(queue_proc);
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

        assert!(abq_utils::net_protocol::is_large_message(&huge_manifest).unwrap());

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
        assert!(stdout.contains("--- MANIFEST GENERATION ---"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("init stdout"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("hello from manifest stdout"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("init stderr"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("hello from manifest stderr"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
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
    let name = "write_statefile_for_worker";
    let conf = CSConfigOptions {
        use_auth_token: false,
        tls: false,
    };

    let statefile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let statefile = statefile.to_path_buf();

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
    term(queue_proc);

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
        let (queue_proc, queue_addr) = setup_queue!(name, conf);

        let run_id = "test-run-id";

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
                format!("--run-id={run_id}"),
                format!("--reporter=dot"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        // Run the test suite with an in-band worker.
        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_worker0")
            .args(test_args(0))
            .run();

        assert_eq!(exit_status.code().unwrap(), 101, "should exit with ABQ error");

        let stdout = sanitize_output(&stdout);

        // Make sure the failing message is reflected in captured worker stdout/stderr.

        assert!(stdout.contains(
r#"
----------------------------- [worker 0, runner X] -----------------------------
I failed catastrophically
For a reason explainable only by a backtrace
"#.trim()), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");

        // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
        let report_args = {
            let args = vec![
                format!("report"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("--color=never"),
            ];
            conf.extend_args_for_client(args)
        };

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_report")
            .args(report_args)
            .always_capture_stderr(true)
            .run();

        assert!(!exit_status.success(), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");
        assert!(stdout.contains("1 tests, 1 failures"), "STDOUT:\n{stdout}\nSTDERR:\n{stderr}");

        term(queue_proc);
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

    let retries = 4;
    let num_tests = 64;
    let num_workers = 6;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
            format!("--retries={retries}"),
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

    // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
    let report_args = {
        let args = vec![
            format!("report"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id=test-run-id"),
            format!("--color=never"),
        ];
        conf.extend_args_for_client(args)
    };

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new(name.to_string() + "_report")
        .args(report_args)
        .always_capture_stderr(true)
        .run();

    assert!(
        !exit_status.success(),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );
    assert!(
        stdout.contains("64 tests, 64 failures, 64 retried"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(queue_proc);
}

#[test]
#[with_protocol_version]
#[serial]
fn test_grouping_without_failures() {
    // Smoke test abq-test that output is correct for multiple runners when there are no failures and no retries.
    let name = "test_grouping_without_failures";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let retries = 0;
    let num_tests = 64;
    let num_runners = 6;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
                    run_tests.push(IfAliveReadAndWriteFake(Status::Success));
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
            format!("--retries={retries}"),
            format!("-n={num_runners}"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let worker = Abq::new(format!("{name}_worker0"))
        .args(test_args(0))
        .always_capture_stderr(true)
        .spawn();

    let Output {
        status,
        stdout,
        stderr,
    } = worker.wait_with_output().unwrap();
    assert_eq!(0, status.code().unwrap());

    let stdout = String::from_utf8_lossy(&stdout).to_string();
    let stderr = String::from_utf8_lossy(&stderr).to_string();
    let stdouts = vec![&stdout];
    assert_sum_of_run_tests(stdouts.iter().map(|s| s.as_str()), 64);
    assert_sum_of_run_test_failures(stdouts.iter().map(|s| s.as_str()), 0);
    assert_sum_of_run_test_retries(stdouts.iter().map(|s| s.as_str()), 0);

    let stdout = sanitize_output(&stdout);
    assert!(
        stdout.contains("64 tests, 0 failures"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_retry_count =
        regex::Regex::new(r"\nRetries:\n\n\s+runner N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        !re_retry_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_failure_count =
        regex::Regex::new(r"\nFailures:\n\n\s+runner N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        !re_failure_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(queue_proc);
}

#[test]
#[with_protocol_version]
#[serial]
fn test_grouping_with_failures_without_retries() {
    // Smoke test abq-test that output is correct for multiple runners when there are failures but no retries.
    let name = "test_grouping_with_failures_without_retries";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let retries = 0;
    let num_tests = 64;
    let num_runners = 6;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
            format!("--retries={retries}"),
            format!("-n={num_runners}"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let worker = Abq::new(format!("{name}_worker0"))
        .args(test_args(0))
        .always_capture_stderr(true)
        .spawn();

    let Output {
        status,
        stdout,
        stderr,
    } = worker.wait_with_output().unwrap();
    assert_eq!(1, status.code().unwrap());

    let stdout = String::from_utf8_lossy(&stdout).to_string();
    let stderr = String::from_utf8_lossy(&stderr).to_string();
    let stdouts = vec![&stdout];
    assert_sum_of_run_tests(stdouts.iter().map(|s| s.as_str()), 64);
    assert_sum_of_run_test_failures(stdouts.iter().map(|s| s.as_str()), 64);
    assert_sum_of_run_test_retries(stdouts.iter().map(|s| s.as_str()), 0);

    let stdout = sanitize_output(&stdout);
    assert!(
        stdout.contains("64 tests, 64 failures"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_retry_count =
        regex::Regex::new(r"\nRetries:\n\n\s+runner N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        !re_retry_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_failure_count =
        regex::Regex::new(r"\nFailures:\n\n\s+runner N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        re_failure_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(queue_proc);
}

#[test]
#[with_protocol_version]
#[serial]
fn test_grouping_failures_retries() {
    // Smoke test abq-test that output is correct for multiple runners when there are failures and retries.
    let name = "test_grouping_failures_retries";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let retries = 4;
    let num_tests = 64;
    let num_runners = 6;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
            format!("--retries={retries}"),
            format!("-n={num_runners}"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let worker = Abq::new(format!("{name}_worker0"))
        .args(test_args(0))
        .always_capture_stderr(true)
        .spawn();

    let Output {
        status,
        stdout,
        stderr,
    } = worker.wait_with_output().unwrap();
    assert_eq!(1, status.code().unwrap());

    let stdout = String::from_utf8_lossy(&stdout).to_string();
    let stderr = String::from_utf8_lossy(&stderr).to_string();
    let stdouts = vec![&stdout];
    assert_sum_of_run_tests(stdouts.iter().map(|s| s.as_str()), 64);
    assert_sum_of_run_test_failures(stdouts.iter().map(|s| s.as_str()), 64);
    assert_sum_of_run_test_retries(stdouts.iter().map(|s| s.as_str()), 64);

    let stdout = sanitize_output(&stdout);
    assert!(
        stdout.contains("64 tests, 64 failures, 64 retried"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_retry_count =
        regex::Regex::new(r"\nRetries:\n\n\s+runner N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        re_retry_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_failure_count =
        regex::Regex::new(r"\nFailures:\n\n\s+runner N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        re_failure_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(queue_proc);
}

#[test]
#[with_protocol_version]
#[serial]
fn report_grouping_failures_retries() {
    // Smoke test abq-report that output is correct for multiple workers (single runners) when there are failures and retries.
    let name = "report_grouping_failures_retries";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let retries = 4;
    let num_tests = 64;
    let num_workers = 6;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
            format!("--retries={retries}"),
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

    // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
    let report_args = {
        let args = vec![
            format!("report"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id=test-run-id"),
            format!("--color=never"),
        ];
        conf.extend_args_for_client(args)
    };

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new(name.to_string() + "_report")
        .args(report_args)
        .always_capture_stderr(true)
        .run();

    let stdout = sanitize_output(&stdout);
    assert!(
        !exit_status.success(),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );
    assert!(
        stdout.contains("64 tests, 64 failures, 64 retried"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_retry_count =
        regex::Regex::new(r"\nRetries:\n\n\s+worker N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        re_retry_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    let re_failure_count =
        regex::Regex::new(r"\nFailures:\n\n\s+worker N:\n\s+\d+   a/b/x.file\n").unwrap();
    assert!(
        re_failure_count.is_match(&stdout),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(queue_proc);
}

#[test]
#[with_protocol_version]
#[serial]
fn retries_displays_retry_banners() {
    let name = "retries_displays_retry_banners";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let retries = 4;
    let num_tests = 64;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
            format!("--retries={retries}"),
            format!("-n=1"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let CmdOutput {
        stdout,
        stderr: _,
        exit_status,
    } = Abq::new(format!("{name}_worker0")).args(test_args(0)).run();

    assert!(!exit_status.success());

    assert_sum_of_run_tests([stdout.as_str()], 64);
    assert_sum_of_run_test_failures([stdout.as_str()], 64);
    assert_sum_of_run_test_retries([stdout.as_str()], 64);

    insta::assert_snapshot!(sanitize_output(&stdout));

    term(queue_proc);
}

#[test]
#[with_protocol_version]
#[serial]
fn retries_with_multiple_runners_on_one_worker_displays_retry_banner_at_end() {
    let name = "retries_displays_retry_banners";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let retries = 4;
    let num_tests = 2;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
            format!("--retries={retries}"),
            format!("-n=2"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let CmdOutput {
        stdout,
        stderr: _,
        exit_status,
    } = Abq::new(format!("{name}_worker0")).args(test_args(0)).run();

    assert!(!exit_status.success());

    assert_sum_of_run_tests([stdout.as_str()], 2);
    assert_sum_of_run_test_failures([stdout.as_str()], 2);
    assert_sum_of_run_test_retries([stdout.as_str()], 2);

    // We should see at least `retries` RETRY headers.
    // We may see more if there multiple runners on the worker are participating in the retries.
    let mut start_search = 0;
    for n in 1..=retries {
        let found = stdout[start_search..].find("ABQ RETRY").unwrap_or_else(|| {
            panic!("Retry header for {n}th attempt not found.\nSTDOUT:\n{stdout}")
        });
        start_search = found + 1;
    }

    term(queue_proc);
}

test_all_network_config_options! {
    cancellation_of_active_run_for_workers |name, conf: CSConfigOptions| {
        let run_id = RunId::unique().to_string();

        let (queue_proc, queue_addr) = setup_queue!(name, conf);

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

        let mut worker0 = Abq::new(name.to_string() + "_test0").args(test_args(0)).always_capture_stderr(true).spawn();

        let mut worker0_stderr = std::mem::take(&mut worker0.stderr).unwrap();
        wait_for_live_worker(&mut worker0_stderr);

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

        // ABQ report should error because the test run was cancelled.
        // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
        let report_args = {
            let args = vec![
                format!("report"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("--color=never"),
            ];
            conf.extend_args_for_client(args)
        };

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_report")
            .args(report_args)
            .always_capture_stderr(true)
            .run();

        assert!(
            !exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
        assert!(
            stderr.contains("failed to fetch test results because the run was cancelled before all test results were received"),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );

        term(queue_proc);
    }
}

#[test]
#[with_protocol_version]
#[serial]
fn out_of_process_retries_smoke() {
    let name = "out_of_process_retries_smoke";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let retries = 4;
    let num_tests = 64;
    let num_workers = 6;

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

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
            format!("-n=1"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let mut run_on_each_worker = vec![];
    let mut failed_on_each_worker = vec![];

    for attempt in 0..retries {
        let workers: Vec<_> = (0..num_workers)
            .map(|i| {
                Abq::new(format!("{name}_worker{i}_attempt{attempt}"))
                    .args(test_args(i))
                    .spawn()
            })
            .collect();

        let mut stdouts = vec![];
        let mut run_on_each = vec![];
        let mut failed_on_each = vec![];
        let mut some_is_failure = false;
        for worker in workers {
            let Output {
                status,
                stdout,
                stderr: _,
            } = worker.wait_with_output().unwrap();
            let stdout = String::from_utf8_lossy(&stdout).to_string();
            some_is_failure = some_is_failure || !status.success();
            run_on_each.push(get_num_of_re(&re_run(), &stdout));
            failed_on_each.push(get_num_of_re(&re_failed(), &stdout));
            stdouts.push(stdout);
        }

        assert!(some_is_failure);

        assert_sum_of_run_tests(stdouts.iter().map(|s| s.as_str()), 64);
        assert_sum_of_run_test_failures(stdouts.iter().map(|s| s.as_str()), 64);

        if attempt == 0 {
            run_on_each_worker = run_on_each;
            failed_on_each_worker = failed_on_each;
        } else {
            assert_eq!(run_on_each, run_on_each_worker);
            assert_eq!(failed_on_each, failed_on_each_worker);
        }
    }

    // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
    let report_args = {
        let args = vec![
            format!("report"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id=test-run-id"),
            format!("--color=never"),
        ];
        conf.extend_args_for_client(args)
    };

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new(name.to_string() + "_report")
        .args(report_args)
        .always_capture_stderr(true)
        .run();

    assert!(
        !exit_status.success(),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );
    assert!(
        stdout.contains("64 tests, 64 failures, 64 retried"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(queue_proc);
}


use mockito::{Matcher, Server};

fn test_access_token() -> AccessToken {
    AccessToken::from_str("abqapi_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap()
}

fn test_auth_token() -> UserToken {
    UserToken::from_str("abqs_MD2QPKH2VZU2krvOa2mN54Q4qwzNxF").unwrap()
}

fn test_mock_cert() -> String {
    "\
    -----BEGIN CERTIFICATE-----\
    FAKEFAKEFAKEFAKEFAKEFAKEFAKE\
    -----END CERTIFICATE-----\
    "
    .to_string()
}

#[test]
#[with_protocol_version]
#[serial]
fn personal_access_token_does_not_mutate_remote_queue() {
    let name = "personal_access_token_does_not_mutate_remote_queue";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

    let mut manifest = vec![];

    manifest.push(TestOrGroup::test(Test::new(
        proto,
        "some_test",
        [],
        Default::default(),
    )));

    let proto = AbqProtocolVersion::V0_2.get_supported_witness().unwrap();

    let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));

    {
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

                    // If the socket is alive (i.e. we have a test to run), pull it and give back a
                    // faux result.
                    // Otherwise assume we ran out of tests on our node and exit.
                    run_tests.push(IfAliveReadAndWriteFake(Status::Failure {
                        exception: None,
                        backtrace: None,
                    }));
                    run_tests
                },
            },
            //
            // Finish
            Exit(0),
        ];

        let packed = pack_msgs_to_disk(simulation);

        let test_args = {
            let simulator = native_runner_simulation_bin();
            let simfile_path = packed.path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker=1"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id=test-run-id"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        let CmdOutput { exit_status, .. } = Abq::new(format!("{name}_initial"))
            .args(test_args)
            .run();

        // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
        let report_args = {
            let args = vec![
                format!("report"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id=test-run-id"),
                format!("--color=never"),
            ];
            conf.extend_args_for_client(args)
        };

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_report")
            .args(report_args)
            .always_capture_stderr(true)
            .run();

        assert!(
            !exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
        assert!(
            stdout.contains("1 tests, 1 failures"),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    let mut server = Server::new();
    let in_run_id = RunId("test-run-id".to_string());
    let access_token = test_auth_token();
    let _m = server.mock("GET", "/queue")
        .match_header(
            "Authorization",
            format!("Bearer {}", test_access_token()).as_str(),
        )
        .match_header("User-Agent", format!("abq/{}", abq_utils::VERSION).as_str())
        .match_query(Matcher::AnyOf(vec![Matcher::UrlEncoded(
            "run_id".to_string(),
            in_run_id.to_string(),
        )]))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(
            r#"{{"queue_url":"abqs://{}?run_id={}\u0026token={}","tls_public_certificate":"{}","rwx_access_token_kind":"personal_access_token"}}"#,
            queue_addr,
            in_run_id,
            TEST_USER_AUTH_TOKEN,
            TLS_CERT,
        ))
        .create();

    // simulation 2
    {
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

                    // If the socket is alive (i.e. we have a test to run), pull it and give back a
                    // faux result.
                    // Otherwise assume we ran out of tests on our node and exit.
                    run_tests.push(IfAliveReadAndWriteFake(Status::Success));
                    run_tests
                },
            },
            //
            // Finish
            Exit(0),
        ];

        let packed = pack_msgs_to_disk(simulation);

        let test_args = {
            let simulator = native_runner_simulation_bin();
            let simfile_path = packed.path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker=1"),
                format!("--run-id=test-run-id"),
                format!("--access-token={access_token}"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        let CmdOutput { exit_status, stdout, stderr, .. } = Abq::new(format!("{name}_initial"))
            .args(test_args)
            .env([("ABQ_API", server.url())])
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
        assert!(
            stdout.contains("1 tests, 0 failures"),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );

        // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
        let report_args = {
            let args = vec![
                format!("report"),
                format!("--reporter=dot"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id=test-run-id"),
                format!("--color=never"),
            ];
            conf.extend_args_for_client(args)
        };

        let CmdOutput {
            stdout,
            stderr,
            exit_status,
        } = Abq::new(name.to_string() + "_report")
            .args(report_args)
            .always_capture_stderr(true)
            .run();

        assert!(
            !exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
        assert!(
            stdout.contains("1 test, 1 failure"),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    term(queue_proc);
}

#[test]
#[serial]
fn report_while_run_in_progress_is_error() {
    let name = "report_while_run_in_progress_is_error";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

    let run_id = "test-run-id";
    let proto = ProtocolWitness::TEST;

    let manifest = vec![TestOrGroup::test(Test::new(
        proto,
        "test1".to_string(),
        [],
        Default::default(),
    ))];

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
            else_do: vec![
                //
                // Read init context message + write ACK
                OpaqueRead,
                OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                //
                // Sleep forever
                Sleep(Duration::from_secs(600)),
            ],
        },
        //
        // Finish
        Exit(0),
    ];

    let packed = pack_msgs_to_disk(simulation);
    let simfile_path = packed.path;

    let test_args = |worker: usize| {
        let simulator = native_runner_simulation_bin();
        let simfile_path = simfile_path.display().to_string();
        let args = vec![
            format!("test"),
            format!("--worker={worker}"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("-n=1"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    let mut worker0 = Abq::new(format!("{name}_worker0"))
        .args(test_args(0))
        .env([("ABQ_LOG", "abq=debug")])
        .always_capture_stderr(true)
        .spawn();

    wait_for_live_worker(worker0.stderr.as_mut().unwrap());
    wait_for_worker_executing(worker0.stderr.as_mut().unwrap());

    // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
    let report_args = {
        let args = vec![
            format!("report"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--color=never"),
        ];
        conf.extend_args_for_client(args)
    };

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new(name.to_string() + "_report")
        .args(report_args)
        .always_capture_stderr(true)
        .run();

    assert!(
        !exit_status.success(),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );
    assert!(
        stderr.contains("failed to fetch test results because the following runners are still active: worker 0, runner 1"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(worker0);
    term(queue_proc);
}

#[test]
#[serial]
fn report_while_run_is_out_of_process_retried_is_error() {
    let name = "report_while_run_is_out_of_process_retried_is_error";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

    let run_id = "test-run-id";
    let proto = ProtocolWitness::TEST;

    let manifest = vec![TestOrGroup::test(Test::new(
        proto,
        "test1".to_string(),
        [],
        Default::default(),
    ))];

    let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));

    // attempt 1 - complete, albeit with a failure.
    {
        let simulation = [
            Connect,
            OpaqueWrite(pack(legal_spawned_message(proto))),
            IfGenerateManifest {
                then_do: vec![OpaqueWrite(pack(&manifest))],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    //
                    // Send result
                    OpaqueRead,
                    OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
                ],
            },
            Exit(0),
        ];

        let packed = pack_msgs_to_disk(simulation);
        let simfile_path = packed.path;

        let test_args = |worker: usize| {
            let simulator = native_runner_simulation_bin();
            let simfile_path = simfile_path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        let CmdOutput {
            exit_status,
            stdout,
            ..
        } = Abq::new(format!("{name}_worker0")).args(test_args(0)).run();
        assert!(exit_status.success());
        assert_sum_of_run_tests([stdout.as_str()], 1);
        assert_sum_of_run_test_failures([stdout.as_str()], 0);
    }

    // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
    let report_args = || {
        let args = vec![
            format!("report"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--color=never"),
        ];
        conf.extend_args_for_client(args)
    };

    // First ABQ report should give back the completed results
    {
        let CmdOutput {
            exit_status,
            stdout,
            stderr: _,
        } = Abq::new(name.to_string() + "_report")
            .args(report_args())
            .run();

        assert!(exit_status.success());
        assert_sum_of_run_tests([stdout.as_str()], 1);
        assert_sum_of_run_test_failures([stdout.as_str()], 0);
    }

    // Start an out-of-process retry that doesn't yield.
    // ABQ report should error out.
    let (mut worker0, _packed_file) = {
        let simulation = [
            Connect,
            OpaqueWrite(pack(legal_spawned_message(proto))),
            IfGenerateManifest {
                then_do: vec![OpaqueWrite(pack(&manifest))],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    //
                    OpaqueRead,
                    Sleep(Duration::from_secs(600)),
                ],
            },
            Exit(0),
        ];

        let packed = pack_msgs_to_disk(simulation);
        let simfile_path = packed.path.clone();

        let test_args = |worker: usize| {
            let simulator = native_runner_simulation_bin();
            let simfile_path = simfile_path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker={worker}"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        let worker0 = Abq::new(format!("{name}_worker0_retry"))
            .args(test_args(0))
            .env([("ABQ_LOG", "abq=debug")])
            .always_capture_stderr(true)
            .spawn();
        (worker0, packed)
    };

    wait_for_live_worker(worker0.stderr.as_mut().unwrap());
    wait_for_worker_executing(worker0.stderr.as_mut().unwrap());

    let CmdOutput {
        stdout,
        stderr,
        exit_status,
    } = Abq::new(name.to_string() + "_report")
        .args(report_args())
        .always_capture_stderr(true)
        .run();

    assert!(
        !exit_status.success(),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );
    assert!(
        stderr.contains("failed to fetch test results because the following runners are still active: worker 0, runner 1"),
        "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    );

    term(worker0);
    term(queue_proc);
}

macro_rules! test_signal_runner_stops_immediately {
    ($($test_name:ident, $signal:ident)*) => {$(
    #[test]
    #[ntest::timeout(1000)] // 1 second
    #[serial]
    fn $test_name() {
        let name = stringify!($test_name);
        let conf = CSConfigOptions {
            use_auth_token: false,
            tls: false,
        };

        let mut test_proc = Abq::new(name)
            .args({
                let mut args = conf.extend_args_for_client(vec![s!("test")]);
                args.extend([s!("--"), s!("yes"), s!("WORKERLIVE")]);
                args
            })
            .spawn();

        let stdout = test_proc.stdout.as_mut().unwrap();
        let mut reader = BufReader::new(stdout).lines();
        // Spin until we see WORKERLIVE
        loop {
            if let Some(line) = reader.next() {
                let line = line.expect("line is not a string");
                if line.contains("WORKERLIVE") {
                    break;
                }
            }
        }

        use nix::sys::signal;
        use nix::unistd::Pid;

        // Stop the worker.
        signal::kill(Pid::from_raw(test_proc.id() as _), signal::Signal::$signal).unwrap();

        let test_exit = test_proc.wait().unwrap();
        assert!(!test_exit.success());
        assert_eq!(test_exit.code(), Some(1));
    }
    )*}
}

test_signal_runner_stops_immediately! {
    test_signal_runner_stops_immediately_sigterm, SIGTERM
    test_signal_runner_stops_immediately_sigint, SIGINT
    test_signal_runner_stops_immediately_sigquit, SIGQUIT
}

#[test]
#[serial]
fn native_runner_fault_followed_by_success_results_in_report_success() {
    let name = "native_runner_fault_followed_by_success_results_in_report_success";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let (queue_proc, queue_addr) = setup_queue!(name, conf);

    let run_id = "test-run-id";

    use abq_native_runner_simulation::{pack, Msg::*};
    use abq_utils::net_protocol::runners::{
        InitSuccessMessage, Manifest, ManifestMessage, Test, TestOrGroup,
    };

    let proto = ProtocolWitness::TEST;

    let test = TestOrGroup::test(Test::new(proto, "test1", vec![], Default::default()));
    let manifest = ManifestMessage::new(Manifest::new(vec![test], Default::default()));

    // abq test ...
    let test_args = |simfile_path: &Path| {
        let simulator = native_runner_simulation_bin();
        let simfile_path = simfile_path.display().to_string();
        let args = vec![
            format!("test"),
            format!("--worker=0"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("-n=1"),
        ];
        let mut args = conf.extend_args_for_client(args);
        args.extend([s!("--"), simulator, simfile_path]);
        args
    };

    // abq report --reporter dot --queue-addr ... --run-id ... (--token ...)?
    let report_args = || {
        let args = vec![
            format!("report"),
            format!("--reporter=dot"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
            format!("--color=never"),
        ];
        conf.extend_args_for_client(args)
    };

    // attempt 1 - complete with a failure, forcing a synthetic error to be produced.
    {
        let simulation = [
            Connect,
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
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
                    Exit(1),
                ],
            },
        ];

        let packed = pack_msgs_to_disk(simulation);

        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt1"))
            .args(test_args(&packed.path))
            .run();

        assert_eq!(
            exit_status.code().unwrap(),
            101,
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );

        // ABQ report should exit with an internal error too.

        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_report_attempt1"))
            .args(report_args())
            .run();

        assert_eq!(
            exit_status.code().unwrap(),
            101,
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
        assert_sum_of_run_tests([stdout.as_str()], 1);
        assert_sum_of_run_test_failures([stdout.as_str()], 1);
    }

    // attempt 2 - complete with a success, any reference to the synthetic result should be
    // dropped.
    {
        let simulation = [
            Connect,
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
                    // Finish
                    Exit(0),
                ],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    //
                    // Send result
                    OpaqueRead,
                    OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
                ],
            },
            Exit(0),
        ];

        let packed = pack_msgs_to_disk(simulation);

        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt2"))
            .args(test_args(&packed.path))
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );

        // ABQ report should succeed this time.

        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_report_attempt2"))
            .args(report_args())
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
        assert_sum_of_run_tests([stdout.as_str()], 1);
    }

    term(queue_proc);
}

fn write_to_temp(content: &str) -> NamedTempFile {
    use std::io::Write;
    let mut fi = NamedTempFile::new().unwrap();
    fi.write_all(content.as_bytes()).unwrap();
    fi
}

fn heuristic_wait_for_written_path(path: &Path) {
    while !path.exists() {
        thread::sleep(Duration::from_millis(10));
    }
    thread::sleep(Duration::from_millis(10));
}

fn heuristic_wait_for_offloaded_file(path: &Path) {
    while std::fs::metadata(path).unwrap().len() != 0 {
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[serial]
fn custom_remote_persistence() {
    let name = "custom_remote_persistence";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let custom_persisted_path = tempfile::tempdir().unwrap().into_path();
    let custom_script = write_to_temp(&formatdoc! {
        "
        const fs = require(`fs`);

        const dir = `{persist_dir}`;

        const action = process.argv[2];
        const kind = process.argv[3];
        const runId = process.argv[4];
        const readFrom = process.argv[5];
        const writeTo = `${{dir}}/${{action}}-${{kind}}-${{runId}}`;

        // If we're loading a run state, fail - each run should be fresh.
        if (kind === 'run_state' && action === 'load') process.exit(1);

        const data = fs.readFileSync(readFrom, `utf8`);
        fs.writeFileSync(writeTo, data);
        ",
        persist_dir = custom_persisted_path.display(),
    });
    let custom_command = format!("node,{}", custom_script.path().to_str().unwrap());

    let (queue_proc, queue_addr) = setup_queue!(name, conf, env:[
        ("ABQ_REMOTE_PERSISTENCE_STRATEGY", "custom"),
        ("ABQ_REMOTE_PERSISTENCE_COMMAND", &custom_command),
    ]);

    let run_id = "test-run-id";

    use abq_native_runner_simulation::{pack, Msg::*};
    use abq_utils::net_protocol::runners::{
        InitSuccessMessage, Manifest, ManifestMessage, Test, TestOrGroup,
    };

    let proto = ProtocolWitness::TEST;

    let test = TestOrGroup::test(Test::new(proto, "test1", vec![], Default::default()));
    let manifest = ManifestMessage::new(Manifest::new(vec![test], Default::default()));

    // Run `abq test`
    {
        let simulation = [
            Connect,
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
                    // Finish
                    Exit(0),
                ],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    // Read first test, write okay
                    OpaqueRead,
                    OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
                ],
            },
            Exit(0),
        ];

        let packed = pack_msgs_to_disk(simulation);

        // abq test ...
        let test_args = {
            let simulator = native_runner_simulation_bin();
            let simfile_path = packed.path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker=0"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        };

        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt1"))
            .args(test_args)
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    {
        let custom_persisted_manifest_path =
            custom_persisted_path.join("store-manifest-test-run-id");
        heuristic_wait_for_written_path(&custom_persisted_manifest_path);

        let manifest = std::fs::read_to_string(&custom_persisted_manifest_path)
            .unwrap_or_else(|_| panic!("Nothing at {:?}", custom_persisted_manifest_path));

        let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        insta::assert_json_snapshot!(manifest, {
            ".items[0].spec.work_id" => "[redacted]",
        }, @r###"
        {
          "assigned_entities": [
            {
              "Runner": [
                0,
                1
              ]
            }
          ],
          "items": [
            {
              "run_number": 1,
              "spec": {
                "test_case": {
                  "id": "test1",
                  "meta": {}
                },
                "work_id": "[redacted]"
              }
            }
          ]
        }
        "###);
    }

    {
        use std::{fs::File, io::BufReader};

        let custom_persisted_results_path = custom_persisted_path.join("store-results-test-run-id");
        heuristic_wait_for_written_path(&custom_persisted_results_path);

        let lines = BufReader::new(File::open(&custom_persisted_results_path).unwrap()).lines();
        let mut lines: Vec<_> = lines.map(|line| line.unwrap()).collect();
        lines.sort();

        let result_lines: Vec<_> = lines
            .into_iter()
            .map(|line| serde_json::from_str::<serde_json::Value>(&line).unwrap())
            .collect();

        insta::assert_json_snapshot!(result_lines, {
            "[0].Results[0].work_id" => "[redacted]",
            "[0].Results[0].results[0].result.timestamp" => "[redacted]",
        }, @r###"
        [
          {
            "Results": [
              {
                "after_all_tests": null,
                "before_any_test": {
                  "stderr": [],
                  "stdout": []
                },
                "results": [
                  {
                    "result": {
                      "display_name": "zzz-faux",
                      "finished_at": "1994-11-05T13:17:30Z",
                      "id": "zzz-faux",
                      "lineage": [
                        "TopLevel",
                        "SubModule",
                        "Test"
                      ],
                      "location": {
                        "column": 15,
                        "file": "a/b/x.file",
                        "line": 10
                      },
                      "meta": {},
                      "output": "my test output",
                      "runtime": {
                        "Nanoseconds": 0
                      },
                      "started_at": "1994-11-05T13:15:30Z",
                      "status": "Success",
                      "stderr": [],
                      "stdout": [],
                      "timestamp": "[redacted]"
                    },
                    "source": {
                      "has_stdout_reporters": false,
                      "is_singleton": true,
                      "runner": [
                        0,
                        1
                      ]
                    }
                  }
                ],
                "run_number": 1,
                "work_id": "[redacted]"
              }
            ]
          },
          {
            "Summary": {
              "manifest_size_nonce": 1,
              "native_runner_info": {
                "protocol_version": {
                  "major": 0,
                  "minor": 2,
                  "type": "abq_protocol_version"
                },
                "specification": {
                  "host": "ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]",
                  "language": "ruby",
                  "language_version": "3.1.2p20",
                  "name": "test",
                  "test_framework": "rspec",
                  "test_framework_version": "3.12.0",
                  "type": "abq_native_runner_specification",
                  "version": "0.0.0"
                }
              }
            }
          }
        ]
        "###);
    }

    term(queue_proc);
}

struct CopyToDirPersister {
    tempdir: TempDir,
    _script: NamedTempFile,
    command: String,
}

impl CopyToDirPersister {
    fn new() -> Self {
        let custom_persisted_path = tempfile::tempdir().unwrap();
        let custom_script = write_to_temp(&formatdoc! {
            "
            function run() {{
                const fs = require(`fs`);

                const dir = `{persist_dir}`;

                const action = process.argv[2];

                const kind = process.argv[3];
                const runId = process.argv[4];

                const theirs = process.argv[5];
                const mine = `${{dir}}/${{kind}}-${{runId}}`;

                if (kind === 'run_state' && action === 'load') {{
                    if (!fs.existsSync(mine)) {{
                        console.error(`Not found`);
                        process.exit(1);
                    }}
                }}

                if (action === 'store') fs.copyFileSync(theirs, mine);
                else if (action === 'load') fs.copyFileSync(mine, theirs);
                else throw new Error(`Unknown action: ${{action}}`);
            }}

            run()
            ",
            persist_dir = custom_persisted_path.path().display(),
        });
        let custom_command = format!("node,{}", custom_script.path().to_str().unwrap());

        Self {
            tempdir: custom_persisted_path,
            _script: custom_script,
            command: custom_command,
        }
    }

    fn dir(&self) -> &Path {
        self.tempdir.path()
    }
}

#[test]
#[serial]
fn manifest_loaded_from_remote_persistence() {
    let name = "manifest_loaded_from_remote_persistence";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let custom_persister = CopyToDirPersister::new();

    let local_manifests_dir = tempfile::tempdir().unwrap().into_path();

    let (queue_proc, queue_addr) = setup_queue!(name, conf, env:[
        ("ABQ_REMOTE_PERSISTENCE_STRATEGY", "custom"),
        ("ABQ_REMOTE_PERSISTENCE_COMMAND", &custom_persister.command),
        ("ABQ_PERSISTED_MANIFESTS_DIR", local_manifests_dir.to_str().unwrap()),
    ]);

    let run_id = "test-run-id";

    use abq_native_runner_simulation::{pack, Msg::*};
    use abq_utils::net_protocol::runners::{
        InitSuccessMessage, Manifest, ManifestMessage, Test, TestOrGroup,
    };

    let proto = ProtocolWitness::TEST;

    let test = TestOrGroup::test(Test::new(proto, "test1", vec![], Default::default()));
    let manifest = ManifestMessage::new(Manifest::new(vec![test], Default::default()));

    let packed;
    let test_args = {
        let simulation = [
            Connect,
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
                    // Finish
                    Exit(0),
                ],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    // Read first test, write okay
                    OpaqueRead,
                    OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
                ],
            },
            Exit(0),
        ];

        packed = pack_msgs_to_disk(simulation);

        // abq test ...
        move || {
            let simulator = native_runner_simulation_bin();
            let simfile_path = packed.path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker=0"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        }
    };

    // Run `abq test` once. We should succeed.
    {
        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt1"))
            .args(test_args())
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    let local_manifest_path = local_manifests_dir.join("test-run-id.manifest.json");
    let custom_persister_manifest_path = custom_persister.dir().join("manifest-test-run-id");

    // There should now be a manifest in the local persisted directory. Delete it.
    {
        heuristic_wait_for_written_path(&local_manifest_path);
        heuristic_wait_for_written_path(&custom_persister_manifest_path);
        std::fs::remove_file(&local_manifest_path).unwrap();
    }

    // Run `abq test` again. It should succeed, with the manifest loaded from the remote.
    {
        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt2"))
            .args(test_args())
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    // There should again be a manifest in the local persisted directory.
    {
        heuristic_wait_for_written_path(&local_manifest_path);
        heuristic_wait_for_written_path(&custom_persister_manifest_path);
    }

    term(queue_proc);
}

#[test]
#[serial]
fn manifest_offloaded_to_remote_persistence_and_restored() {
    let name = "manifest_offloaded_to_remote_persistence_and_restored";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let custom_persister = CopyToDirPersister::new();

    let local_manifests_dir = tempfile::tempdir().unwrap().into_path();

    let (queue_proc, queue_addr) = setup_queue!(name, conf, env:[
        ("ABQ_REMOTE_PERSISTENCE_STRATEGY", "custom"),
        ("ABQ_REMOTE_PERSISTENCE_COMMAND", &custom_persister.command),
        ("ABQ_PERSISTED_MANIFESTS_DIR", local_manifests_dir.to_str().unwrap()),
        ("ABQ_OFFLOAD_STALE_FILE_THRESHOLD_HOURS", "0"), // allow immediate offloading of manifests
        ("ABQ_OFFLOAD_MANIFESTS_CRON", "* * * * * *"), // allow immediate offloading of manifests
    ]);

    let run_id = "test-run-id";

    use abq_native_runner_simulation::{pack, Msg::*};
    use abq_utils::net_protocol::runners::{
        InitSuccessMessage, Manifest, ManifestMessage, Test, TestOrGroup,
    };

    let proto = ProtocolWitness::TEST;

    let test = TestOrGroup::test(Test::new(proto, "test1", vec![], Default::default()));
    let manifest = ManifestMessage::new(Manifest::new(vec![test], Default::default()));

    let packed;
    let test_args = {
        let simulation = [
            Connect,
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
                    // Finish
                    Exit(0),
                ],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    // Read first test, write okay
                    OpaqueRead,
                    OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
                ],
            },
            Exit(0),
        ];

        packed = pack_msgs_to_disk(simulation);

        // abq test ...
        move || {
            let simulator = native_runner_simulation_bin();
            let simfile_path = packed.path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker=0"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        }
    };

    // Run `abq test` once. We should succeed.
    {
        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt1"))
            .args(test_args())
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    let local_manifest_path = local_manifests_dir.join("test-run-id.manifest.json");
    let custom_persister_manifest_path = custom_persister.dir().join("manifest-test-run-id");

    std::thread::sleep(Duration::from_secs(1));

    // The manifest should now be offloaded from the local persistence path.
    {
        heuristic_wait_for_written_path(&custom_persister_manifest_path);
        heuristic_wait_for_offloaded_file(&local_manifest_path);
    }

    // Run `abq test` again. It should succeed, with the manifest loaded from the remote.
    {
        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt2"))
            .args(test_args())
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    term(queue_proc);
}

#[test]
#[serial]
fn results_offloaded_to_remote_persistence_and_restored() {
    let name = "results_offloaded_to_remote_persistence_and_restored";
    let conf = CSConfigOptions {
        use_auth_token: true,
        tls: true,
    };

    let custom_persister = CopyToDirPersister::new();

    let local_results_dir = tempfile::tempdir().unwrap().into_path();

    let (queue_proc, queue_addr) = setup_queue!(name, conf, env:[
        ("ABQ_REMOTE_PERSISTENCE_STRATEGY", "custom"),
        ("ABQ_REMOTE_PERSISTENCE_COMMAND", &custom_persister.command),
        ("ABQ_PERSISTED_RESULTS_DIR", local_results_dir.to_str().unwrap()),
        ("ABQ_OFFLOAD_STALE_FILE_THRESHOLD_HOURS", "0"), // allow immediate offloading of results
        ("ABQ_OFFLOAD_RESULTS_CRON", "* * * * * *"), // allow immediate offloading of results
    ]);

    let run_id = "test-run-id";

    use abq_native_runner_simulation::{pack, Msg::*};
    use abq_utils::net_protocol::runners::{
        InitSuccessMessage, Manifest, ManifestMessage, Test, TestOrGroup,
    };

    let proto = ProtocolWitness::TEST;

    let test = TestOrGroup::test(Test::new(proto, "test1", vec![], Default::default()));
    let manifest = ManifestMessage::new(Manifest::new(vec![test], Default::default()));

    let packed;
    let test_args = {
        let simulation = [
            Connect,
            // Write spawn message
            OpaqueWrite(pack(legal_spawned_message(proto))),
            // Write the manifest if we need to.
            // Otherwise handle the one test.
            IfGenerateManifest {
                then_do: vec![
                    OpaqueWrite(pack(&manifest)),
                    // Finish
                    Exit(0),
                ],
                else_do: vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                    // Read first test, write okay
                    OpaqueRead,
                    OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
                ],
            },
            Exit(0),
        ];

        packed = pack_msgs_to_disk(simulation);

        let queue_addr = queue_addr.clone();

        // abq test ...
        move || {
            let simulator = native_runner_simulation_bin();
            let simfile_path = packed.path.display().to_string();
            let args = vec![
                format!("test"),
                format!("--worker=0"),
                format!("--queue-addr={queue_addr}"),
                format!("--run-id={run_id}"),
                format!("-n=1"),
            ];
            let mut args = conf.extend_args_for_client(args);
            args.extend([s!("--"), simulator, simfile_path]);
            args
        }
    };

    let report_args = move || {
        let args = vec![
            format!("report"),
            format!("--queue-addr={queue_addr}"),
            format!("--run-id={run_id}"),
        ];
        conf.extend_args_for_client(args)
    };

    // Run `abq test` once. We should succeed.
    {
        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_worker0_attempt1"))
            .args(test_args())
            .run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
    }

    let local_results_path = local_results_dir.join("test-run-id.results.jsonl");
    let custom_persister_results_path = custom_persister.dir().join("results-test-run-id");

    std::thread::sleep(Duration::from_secs(1));

    // The results should now be offloaded from the local persistence path.
    {
        heuristic_wait_for_written_path(&custom_persister_results_path);
        heuristic_wait_for_offloaded_file(&local_results_path);
    }

    // Run `abq report`. It should succeed, with the results loaded from the remote.
    {
        let CmdOutput {
            exit_status,
            stdout,
            stderr,
        } = Abq::new(format!("{name}_report")).args(report_args()).run();

        assert!(
            exit_status.success(),
            "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        );
        assert_sum_of_run_tests([stdout.as_str()], 1);
    }

    term(queue_proc);
}

#[test]
#[serial]
fn persisted_runs_between_queue_instances() {
    let custom_persister = CopyToDirPersister::new();

    let local_results_dir = tempfile::tempdir().unwrap().into_path();
    let local_manifests_dir = tempfile::tempdir().unwrap().into_path();

    let run_cycle = |n| {
        let name = &format!("results_offloaded_to_remote_persistence_and_restored_{n}");
        let conf = CSConfigOptions {
            use_auth_token: true,
            tls: true,
        };

        let (queue_proc, queue_addr) = setup_queue!(name, conf, env:[
            ("ABQ_REMOTE_PERSISTENCE_STRATEGY", "custom"),
            ("ABQ_REMOTE_PERSISTENCE_COMMAND", &custom_persister.command),
            ("ABQ_PERSISTED_RESULTS_DIR", local_results_dir.to_str().unwrap()),
            ("ABQ_PERSISTED_MANIFESTS_DIR", local_manifests_dir.to_str().unwrap()),
            // allow immediate offloading
            ("ABQ_OFFLOAD_STALE_FILE_THRESHOLD_HOURS", "0"),
            ("ABQ_OFFLOAD_RESULTS_CRON", "* * * * * *"),
            ("ABQ_OFFLOAD_MANIFESTS_CRON", "* * * * * *"),
        ]);

        let run_id = "test-run-id";

        use abq_native_runner_simulation::{pack, Msg::*};
        use abq_utils::net_protocol::runners::{
            InitSuccessMessage, Manifest, ManifestMessage, Test, TestOrGroup,
        };

        let proto = ProtocolWitness::TEST;

        let test = TestOrGroup::test(Test::new(proto, "test1", vec![], Default::default()));
        let manifest = ManifestMessage::new(Manifest::new(vec![test], Default::default()));

        let packed;
        let test_args = {
            let simulation = [
                Connect,
                // Write spawn message
                OpaqueWrite(pack(legal_spawned_message(proto))),
                // Write the manifest if we need to.
                // Otherwise handle the one test.
                IfGenerateManifest {
                    then_do: vec![
                        OpaqueWrite(pack(&manifest)),
                        // Finish
                        Exit(0),
                    ],
                    else_do: vec![
                        //
                        // Read init context message + write ACK
                        OpaqueRead,
                        OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                        // Read first test, write okay
                        OpaqueRead,
                        OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
                    ],
                },
                Exit(0),
            ];

            packed = pack_msgs_to_disk(simulation);

            let queue_addr = queue_addr.clone();

            // abq test ...
            move || {
                let simulator = native_runner_simulation_bin();
                let simfile_path = packed.path.display().to_string();
                let args = vec![
                    format!("test"),
                    format!("--worker=0"),
                    format!("--queue-addr={queue_addr}"),
                    format!("--run-id={run_id}"),
                    format!("-n=1"),
                ];
                let mut args = conf.extend_args_for_client(args);
                args.extend([s!("--"), simulator, simfile_path]);
                args
            }
        };

        // Run `abq test` once. We should succeed.
        {
            let CmdOutput {
                exit_status,
                stdout,
                stderr,
            } = Abq::new(format!("{name}_worker0_attempt1"))
                .args(test_args())
                .run();

            assert!(
                exit_status.success(),
                "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            );
        }

        let custom_persister_results_path = custom_persister.dir().join("results-test-run-id");
        let custom_persister_manifest_path = custom_persister.dir().join("manifest-test-run-id");
        let custom_persister_run_state_path = custom_persister.dir().join("run_state-test-run-id");

        std::thread::sleep(Duration::from_millis(500));

        // The results, manifest, and run state should now be offloaded from the local persistence path.
        {
            heuristic_wait_for_written_path(&custom_persister_results_path);
            heuristic_wait_for_written_path(&custom_persister_manifest_path);
            heuristic_wait_for_written_path(&custom_persister_run_state_path);
        }

        // Run `abq report`. It should succeed.
        {
            let CmdOutput {
                exit_status,
                stdout,
                stderr,
            } = Abq::new(format!("{name}_report"))
                .args(conf.extend_args_for_client(vec![
                    format!("report"),
                    format!("--queue-addr={queue_addr}"),
                    format!("--run-id={run_id}"),
                ]))
                .run();

            assert!(
                exit_status.success(),
                "STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            );
            assert_sum_of_run_tests([stdout.as_str()], 1);
            if n > 1 {
                assert_sum_of_run_test_retries([stdout.as_str()], (n - 1) as _);
            }
        }

        term(queue_proc);

        let size_of_results = std::fs::metadata(&custom_persister_results_path)
            .unwrap()
            .len();
        let size_of_manifest = std::fs::metadata(&custom_persister_manifest_path)
            .unwrap()
            .len();
        let size_of_run_state = std::fs::metadata(&custom_persister_run_state_path)
            .unwrap()
            .len();
        (size_of_results, size_of_manifest, size_of_run_state)
    };

    // Run the test once.
    let (init_results_size, init_manifest_size, init_run_state_size) = run_cycle(1);
    // Run the test again, on a new queue. Only the results should increase.
    let (next_results_size, next_manifest_size, next_run_state_size) = run_cycle(2);

    assert!(
        init_results_size < next_results_size,
        "init: {init_results_size} vs next: {next_results_size}"
    );
    assert_eq!(init_manifest_size, next_manifest_size);
    assert_eq!(init_run_state_size, next_run_state_size);
}
