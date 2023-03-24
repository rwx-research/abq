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
    net_async,
    net_opt::{ClientOptions, ServerOptions},
    net_protocol::{
        queue::TestSpec,
        runners::{ProtocolWitness, TestCase, TestId},
        workers::WorkId,
    },
    tls::{ClientTlsStrategy, ServerTlsStrategy},
};

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

    let s = re_paths.replace_all(s, "at <stripped path>");
    let s = re_simulation_path.replace(&s, "<simulation cmd>");
    let s =
        re_finished_seconds.replace(&s, "Finished in XX seconds (XX seconds spent in test code)");
    let s = re_generic_runner.replace(&s, "Generic test runner started on <stripped>");
    let s = re_ran_on_runner.replace_all(&s, ", runner X");

    s.into_owned()
}

pub fn wid(id: usize) -> WorkId {
    WorkId([id as u8; 16])
}

pub fn test(id: usize) -> TestId {
    format!("test{id}")
}

pub fn spec(id: usize) -> TestSpec {
    TestSpec {
        test_case: TestCase::new(ProtocolWitness::TEST, test(id), Default::default()),
        work_id: wid(id),
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

#[macro_export]
macro_rules! s {
    ($s:expr) => {
        $s.to_string()
    };
}
