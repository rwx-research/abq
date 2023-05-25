use std::{
    io,
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use abq_utils::{
    auth::{
        build_strategies, Admin, AdminToken, ClientAuthStrategy, ServerAuthStrategy, User,
        UserToken,
    },
    capture_output::StdioOutput,
    net_async,
    net_opt::{ClientOptions, ServerOptions},
    net_protocol::{
        queue::{AssociatedTestResults, TestSpec},
        runners::{ProtocolWitness, Status, TestCase, TestId, TestResult},
        workers::{GroupId, WorkId},
    },
    time::EpochMillis,
    tls::{ClientTlsStrategy, ServerTlsStrategy},
};
use tempfile::NamedTempFile;

pub mod color_writer;

pub const WORKSPACE: &str = env!("ABQ_WORKSPACE_DIR");

pub fn artifacts_dir() -> PathBuf {
    let path = if cfg!(all(target_arch = "x86_64", target_env = "musl")) {
        // GHA is using a musl target
        if cfg!(debug_assertions) {
            "target/x86_64-unknown-linux-musl/release-unstable"
        } else {
            "target/x86_64-unknown-linux-musl/release"
        }
    } else if cfg!(debug_assertions) {
        "target/debug"
    } else {
        "target/release"
    };
    PathBuf::from(WORKSPACE).join(path)
}

/// Only relevant with [traced_test][tracing_test::traced_test].
#[track_caller]
pub fn assert_scoped_log(scope: &str, log: &str) {
    assert_scoped_logs(scope, |logs| logs.iter().any(|l| l.contains(log)));
}

/// Only relevant with [traced_test][tracing_test::traced_test].
#[track_caller]
pub fn assert_scoped_logs(scope: &str, f: impl Fn(&[&str]) -> bool) {
    match tracing_test::internal::logs_assert(scope, |logs| {
        if f(logs) {
            Ok(())
        } else {
            Err("logs not found".to_owned())
        }
    }) {
        Ok(()) => (),
        Err(e) => panic!("{e}"),
    }
}

pub fn one_nonzero() -> NonZeroU64 {
    1.try_into().unwrap()
}

pub fn one_nonzero_usize() -> NonZeroUsize {
    1.try_into().unwrap()
}

pub async fn accept_handshake(
    listener: &dyn net_async::ServerListener,
) -> io::Result<(Box<dyn net_async::ServerStream>, SocketAddr)> {
    let (unverified, addr) = listener.accept().await?;
    let stream = listener.handshake_ctx().handshake(unverified).await?;
    Ok((stream, addr))
}

pub fn sanitize_output(s: &str) -> String {
    let re_paths = regex::Regex::new(r"at crates.*").unwrap();
    let re_simulation_path = regex::Regex::new(r".*abqtest_native_runner_simulation.*").unwrap();
    let re_finished_seconds = regex::Regex::new(r"Finished in .*").unwrap();
    let re_generic_runner = regex::Regex::new(r"Generic test runner for .*").unwrap();
    let re_ran_on_runner = regex::Regex::new(r", runner \d+").unwrap();
    let re_summary_worker_runner_label =
        regex::Regex::new(r"(?m)^(\s+)(worker|runner) \d+:$").unwrap();

    let s = re_paths.replace_all(s, "at <stripped path>");
    let s = re_simulation_path.replace(&s, "<simulation cmd>");
    let s =
        re_finished_seconds.replace(&s, "Finished in XX seconds (XX seconds spent in test code)");
    let s = re_generic_runner.replace(&s, "Generic test runner started on <stripped>");
    let s = re_ran_on_runner.replace_all(&s, ", runner X");
    let s = re_summary_worker_runner_label.replace_all(&s, "${1}${2} N:");

    s.into_owned()
}

pub fn wid(id: usize) -> WorkId {
    WorkId([id as u8; 16])
}

pub fn gid(id: usize) -> GroupId {
    GroupId([id as u8; 16])
}

pub fn test(id: usize) -> TestId {
    format!("test{id}")
}

// only used in tests
pub fn spec(id: usize) -> TestSpec {
    TestSpec {
        test_case: TestCase::new(ProtocolWitness::TEST, test(id), Default::default()),
        work_id: wid(id),
        group_id: gid(id), // arbitrary
    }
}

pub struct TestResultBuilder {
    result: TestResult,
}

impl TestResultBuilder {
    pub fn new(test_id: impl Into<TestId>, status: Status) -> Self {
        let mut result = TestResult::fake();
        let test_id = test_id.into();
        result.result.id = test_id.clone();
        result.result.display_name = test_id;
        result.result.status = status;
        Self { result }
    }

    pub fn output(mut self, output: impl ToString) -> Self {
        self.result.result.output = Some(output.to_string());
        self
    }

    pub fn timestamp(mut self, timestamp: EpochMillis) -> Self {
        self.result.result.timestamp = timestamp;
        self
    }

    pub fn build(self) -> TestResult {
        self.result
    }
}

impl From<TestResultBuilder> for TestResult {
    fn from(tb: TestResultBuilder) -> Self {
        tb.build()
    }
}

pub fn with_focus(test_spec: impl Into<TestSpec>, focus: impl Into<TestId>) -> TestSpec {
    let mut test_spec: TestSpec = test_spec.into();
    test_spec.test_case.add_test_focus(focus.into());
    test_spec
}

pub struct AssociatedTestResultsBuilder {
    results: AssociatedTestResults,
}

impl AssociatedTestResultsBuilder {
    pub fn new<T: Into<TestResult>>(
        work_id: WorkId,
        run_number: u32,
        results: impl IntoIterator<Item = T>,
    ) -> Self {
        let results = AssociatedTestResults {
            work_id,
            run_number,
            results: results.into_iter().map(Into::into).collect(),
            before_any_test: StdioOutput::empty(),
            after_all_tests: None,
        };
        Self { results }
    }

    pub fn before_any_test(mut self, before_any_test: StdioOutput) -> Self {
        self.results.before_any_test = before_any_test;
        self
    }

    pub fn after_all_tests(mut self, after_all_tests: StdioOutput) -> Self {
        self.results.after_all_tests = Some(after_all_tests);
        self
    }

    pub fn build(self) -> AssociatedTestResults {
        self.results
    }
}

pub fn build_random_strategies() -> (
    ServerAuthStrategy,
    ClientAuthStrategy<User>,
    ClientAuthStrategy<Admin>,
) {
    build_strategies(UserToken::new_random(), AdminToken::new_random())
}

pub fn build_primitive_opts() -> (ServerOptions, ClientOptions<User>) {
    let server_opts =
        ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls());
    let client_opts =
        ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());
    (server_opts, client_opts)
}

pub async fn build_fake_server_client() -> (
    Box<dyn net_async::ServerListener>,
    Box<dyn net_async::ConfiguredClient>,
) {
    let (server_opts, client_opts) = build_primitive_opts();
    let server = server_opts.bind_async("0.0.0.0:0").await.unwrap();

    let client = client_opts.build_async().unwrap();

    (server, client)
}

pub async fn build_fake_connection() -> (
    Box<dyn net_async::ServerListener>,
    Box<dyn net_async::ServerStream>,
    Box<dyn net_async::ClientStream>,
) {
    let (server, client) = build_fake_server_client().await;
    let server_addr = server.local_addr().unwrap();

    let (client_res, server_res) =
        tokio::join!(client.connect(server_addr), accept_handshake(&*server));
    let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());
    (server, server_conn, client_conn)
}

pub fn write_to_temp(content: &str) -> NamedTempFile {
    use std::io::Write;
    let mut fi = NamedTempFile::new().unwrap();
    fi.write_all(content.as_bytes()).unwrap();
    fi
}

#[macro_export]
macro_rules! s {
    ($s:expr) => {
        $s.to_string()
    };
}
