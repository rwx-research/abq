use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use abq_hosted::AccessToken;
use abq_utils::{
    auth::{AdminToken, UserToken},
    net_protocol::workers::RunId,
};

use clap::{ArgGroup, Parser, Subcommand};

use crate::reporting::{ColorPreference, ReporterKind};

#[derive(Clone)]
pub enum NumWorkers {
    CpuCores,
    Fixed(NonZeroUsize),
}

impl std::str::FromStr for NumWorkers {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "cpu-cores" {
            Ok(Self::CpuCores)
        } else {
            let fixed = s
                .parse()
                .map_err(|_| r#"num must be a positive integer, or "cpu-cores""#)?;
            Ok(Self::Fixed(fixed))
        }
    }
}

/// Always be queueing
///
/// The abq cli
#[derive(Parser)]
#[clap(version = abq_utils::VERSION)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]

pub enum Command {
    /// Starts the "abq" ephemeral queue.
    Start {
        /// Host IP address to bind the queue to.
        /// When not specified, the unspecified address 0.0.0.0 is chosen.
        #[clap(long, required = false, default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
        bind: IpAddr,

        /// Port to bind the queue server to.
        /// When not specified, an arbitrary port is chosen.
        #[clap(long, required = false, default_value_t = 0)]
        port: u16,

        /// Port to bind the queue's worker server to.
        ///
        /// When possible, you should avoid configuring this, as it is used for abq-internal
        /// communication only. However, you may need to specify it if you are limited in the ports
        /// you can expose.
        #[clap(long, required = false, default_value_t = 0)]
        work_port: u16,

        /// Port to bind the queue's negotiation server to.
        ///
        /// When possible, you should avoid configuring this, as it is used for abq-internal
        /// communication only. However, you may need to specify it if you are limited in the ports
        /// you can expose.
        #[clap(long, required = false, default_value_t = 0)]
        negotiator_port: u16,

        /// Host IP address to advertise the queue as running on.
        /// When not specified, falls back on `bin`; otherwise, the unspecified address 0.0.0.0 is
        /// used.
        #[clap(long, required = false)]
        public_ip: Option<IpAddr>,

        /// A token against which messages to the queue will be authorized.
        /// When provided, the same token must be provided to runs of the `work` and `test`
        /// commands.
        /// When not provided, the queue will start without assuming enforcing authorization.
        ///
        /// If provided, must also provide `--admin-token`.
        ///
        /// Need a token? Use `abq token new`!
        #[clap(long, requires("admin_token"))]
        user_token: Option<UserToken>,

        /// A token against which admin messages to the queue will be authorized.
        /// When not provided, the queue will start without assuming enforcing authorization.
        ///
        /// If provided, must also provide `--user-token`.
        ///
        /// Need a token? Use `abq token new`!
        #[clap(long, requires("user_token"))]
        admin_token: Option<AdminToken>,

        /// If the queue should accept messages only with TLS, the path of the TLS certificate to
        /// use.
        ///
        /// If provided, must also provide `--tls-key`.
        #[clap(long, requires("tls_key"))]
        tls_cert: Option<PathBuf>,

        /// If the queue should accept messages only with TLS, the path of the TLS private key to
        /// use.
        ///
        /// If provided, must also provide `--tls-cert`.
        #[clap(long, requires("tls_cert"))]
        tls_key: Option<PathBuf>,
    },
    /// Starts an instance of an ABQ test suite run, or connects a worker to a test suite run.
    ///
    /// WORKERS:
    ///
    ///   The executable given to `abq test` must implement the ABQ protocol.
    ///
    /// EXAMPLES:
    ///
    ///   # Run a test suite with a single worker, implicitly named worker 0.
    ///   abq test -- yarn jest -t "onboard flow"
    ///
    ///   # Run a test suite with a single worker
    ///   abq test --worker 0 -- bundle exec rspec
    ///
    ///   # Run a test suite with two workers
    ///   abq test --worker 0 --run-id my-test -- bundle exec rspec &
    ///   abq test --worker 1 --run-id my-test -- bundle exec rspec
    #[clap(verbatim_doc_comment)]
    #[command(group(
        ArgGroup::new("execution") // don't allow both queue_addr and access_token params
            .multiple(false)
            .args(["access_token", "queue_addr"]),
        )
    )]
    #[command(group(
        ArgGroup::new("server-key-exclusion") // don't allow server-side cert key if running in non-local mode
            .multiple(false)
            .args(["access_token", "queue_addr"])
            .conflicts_with("tls_key"),
        )
    )]
    Test {
        /// The number of the test worker connecting for a test suite run.
        ///
        /// There may not be duplicate worker numberings in an ABQ test suite run.
        #[clap(long, required = false, default_value_t = 0)]
        worker: u32,

        /// How many times to retry failed tests.
        ///
        /// A test will be attempted up to `1 + retries` times. If it fails in all of those
        /// attempts, it will be reported as failed.
        #[clap(long, required = false, default_value_t = 0)]
        retries: u32,

        /// Working directory of the worker. Defaults to the current directory.
        #[clap(long, required = false)]
        working_dir: Option<PathBuf>,

        /// Run ID for workers to connect to. If not specified, workers are started in-process.
        /// In CI environments, this can be inferred from CI environment variables.
        #[clap(long, required = false, env("ABQ_RUN_ID"))]
        run_id: Option<RunId>,

        /// The access token to use when fetching queue config information from the ABQ API.
        ///
        /// Cannot be used with --queue-addr (implies: not using the ABQ API).
        #[clap(long, required = false, env("RWX_ACCESS_TOKEN"))]
        access_token: Option<AccessToken>,

        /// Address of the queue where the test command will be sent.
        ///
        /// Requires that abq workers be started as separate processes connected to the queue.
        ///
        /// Cannot be used with access_token (will fetch address from ABQ API).
        #[clap(long, required = false)]
        queue_addr: Option<SocketAddr>,

        /// Number of runners to start on the worker.
        ///
        /// Set to "cpu-cores" to use the number of available (physical) CPUs cores - 1.
        #[clap(long, short = 'n', required = false, default_value = "1")]
        num: NumWorkers,

        /// Token to authorize messages sent to the queue with.
        /// Usually, this should be the same token that `abq start` initialized with.
        ///
        /// If --access-token is specified, the token will be ignored and the token fetched from the ABQ API will be used.
        #[clap(long, required = false)]
        token: Option<UserToken>,

        /// If message should only be sent with TLS, the path of the TLS cert to
        /// anticipate from the communicating queue.
        ///
        /// When set, only queues configured with this TLS cert should be provided via `--queue-addr`.
        ///
        /// If --access-token is specified, the tls flag will be ignored and the setting fetched from the ABQ API will be used.
        #[clap(long)]
        tls_cert: Option<PathBuf>,

        /// If running in local mode, and if messages should only be sent with TLS,
        /// the path of the TLS cert to anticipate from the communicating queue.
        ///
        /// Cannot be used with either `--access-token` or `--queue-addr`, both of which anticipate
        /// running in non-local modes. If provided, must provide `--tls-cert` as well.
        #[clap(long, requires("tls_cert"))]
        tls_key: Option<PathBuf>,

        /// Test result reporter to use for a test run. Options are:{n}
        ///- default: pass-through native runner when using a single runner, otherwise "dot"{n}
        ///- dot: prints a dot for each test{n}
        ///- line: prints a line for each test{n}
        ///- progress: an interactive progress output{n}
        ///- junit-xml[=path/to/results.xml]: outputs a junit-compatible xml file to specified path. Defaults to ./abq-test-results.xml{n}
        ///- rwx-v1-json[=path/to/results.json]: outputs a rwx-v1-compatible json file to specified path. Defaults to ./abq-test-results.json
        #[clap(long, default_value = "default")]
        reporter: Vec<ReporterKind>,

        /// How many tests to send to a worker a time.
        #[clap(long, default_value = "7")]
        batch_size: NonZeroU64,

        /// Whether to report tests with colors.
        ///
        /// When set to `auto`, will try to emit colors unless the output channel is detected
        /// not to be a TTY, if (on Windows) the console isn't available, if NO_COLOR is set, if
        /// TERM is set to `dumb`, amongst other heuristics.
        #[clap(long, default_value = "auto")]
        color: ColorPreference,

        /// A broad measure of inactivity timeout seconds, after which a test run is cancelled.
        ///
        /// The inactivity timeout is applied in the following cases:
        ///
        /// - If a test takes longer than the timeout seconds to complete
        ///
        /// When setting the inactivity, it is recommended to over-estimate
        /// based on historical test runtimes you have observed for abq.
        ///
        /// Hitting a timeout is typically indicative of a failure in a test suite's setup or
        /// configuration.
        #[clap(long, default_value_t = abq_workers::DEFAULT_RUNNER_TEST_TIMEOUT.as_secs())]
        inactivity_timeout_seconds: u64,

        /// Arguments to the test executable.
        #[clap(required = true, num_args = 1.., allow_hyphen_values = true, last = true)]
        args: Vec<String>,
    },
    /// Fetches all test results for an ABQ test run and formats them with the given reporters.
    ///
    /// `abq report` may be only be invoked after all workers for a given test suite run have
    /// completed. If there are outstanding workers for a given test suite run, `abq report` will
    /// error out without reporting results.
    ///
    /// If a worker for a given test suite run is retried at the same time that `abq report` is
    /// invoked, `abq report` may report test results, but may not include all results from the
    /// retried worker.
    #[clap(verbatim_doc_comment)]
    #[command(group(
        ArgGroup::new("execution") // don't allow both queue_addr and access_token params
            .multiple(false)
            .args(["access_token", "queue_addr"]),
        )
    )]
    #[command(group(
        ArgGroup::new("server-key-exclusion") // don't allow server-side cert key if running in non-local mode
            .multiple(false)
            .args(["access_token", "queue_addr"])
            .conflicts_with("tls_key"),
        )
    )]
    Report {
        /// Run ID of the test suite for which test results should be fetched.
        /// In CI environments, this can be inferred from CI environment variables.
        #[clap(long, required = false, env("ABQ_RUN_ID"))]
        run_id: Option<RunId>,

        /// Test result reporter to use for a test run. Options are:{n}
        ///- dot: prints a dot for each test{n}
        ///- line: prints a line for each test{n}
        ///- progress: an interactive progress output{n}
        ///- junit-xml[=path/to/results.xml]: outputs a junit-compatible xml file to specified path. Defaults to ./abq-test-results.xml{n}
        ///- rwx-v1-json[=path/to/results.json]: outputs a rwx-v1-compatible json file to specified path. Defaults to ./abq-test-results.json
        #[clap(long, default_value = "dot")]
        reporter: Vec<ReporterKind>,

        /// Whether to report tests with colors.
        ///
        /// When set to `auto`, will try to emit colors unless the output channel is detected
        /// not to be a TTY, if (on Windows) the console isn't available, if NO_COLOR is set, if
        /// TERM is set to `dumb`, amongst other heuristics.
        #[clap(long, default_value = "auto")]
        color: ColorPreference,

        /// The access token to use when fetching queue config information from the ABQ API.
        ///
        /// Cannot be used with --queue-addr (implies: not using the ABQ API).
        #[clap(long, required = false, env("RWX_ACCESS_TOKEN"))]
        access_token: Option<AccessToken>,

        /// The maximum number of seconds to wait for test results to be available.
        ///
        /// If there are active workers, `abq report` will exit immediately with an error. However,
        /// if all workers are complete, test results may still be pending before delivery to `abq
        /// report`. The command will wait up to `--timeout-seconds` before exiting with an error.
        #[clap(long, default_value = "300")]
        timeout_seconds: u64,

        /// Address of the queue where the test command will be sent.
        ///
        /// Requires that abq workers be started as separate processes connected to the queue.
        ///
        /// Cannot be used with access_token (will fetch address from ABQ API).
        #[clap(long, required = false)]
        queue_addr: Option<SocketAddr>,

        /// Token to authorize messages sent to the queue with.
        /// Usually, this should be the same token that `abq start` initialized with.
        ///
        /// If --access-token is specified, the token will be ignored and the token fetched from the ABQ API will be used.
        #[clap(long, required = false)]
        token: Option<UserToken>,

        /// If message should only be sent with TLS, the path of the TLS cert to
        /// anticipate from the communicating queue.
        ///
        /// When set, only queues configured with this TLS cert should be provided via `--queue-addr`.
        ///
        /// If --access-token is specified, the tls flag will be ignored and the setting fetched from the ABQ API will be used.
        #[clap(long)]
        tls_cert: Option<PathBuf>,
    },
    /// Checks the health of an abq instance.
    ///
    /// Exits with 0 if all provided services to check are health; exits with 1 otherwise.
    Health {
        /// The addresses of one or more queue servers to check.
        #[clap(required = false, long)]
        queue: Vec<SocketAddr>,

        /// The addresses of one or more work-scheduling servers to check.
        #[clap(required = false, long)]
        work_scheduler: Vec<SocketAddr>,

        /// The addresses of one or more negotiator servers to check.
        #[clap(required = false, long)]
        negotiator: Vec<SocketAddr>,

        /// Token to authorize messages sent to the services.
        /// Usually, this should be the same token that `abq start` initialized with.
        #[clap(long, required = false)]
        token: Option<UserToken>,

        /// If message should only be sent with TLS, the path of the TLS cert to
        /// anticipate from the communicating queue.
        ///
        /// When set, only queues configured with this TLS cert should be provided via `--queue-addr`.
        #[clap(long)]
        tls_cert: Option<PathBuf>,
    },
    /// Utilities related to auth tokens.
    #[clap(subcommand)]
    Token(Token),
}

#[derive(Subcommand)]
pub enum Token {
    /// Generate a new auth token for a queue to authenticate against, and for workers and `abq test` to authenticate with.
    ///
    /// This only generates a well-formed token; you must still pass it when instantiating
    /// `abq start`, `abq work`, or `abq test` for it to be used.
    New,
}
