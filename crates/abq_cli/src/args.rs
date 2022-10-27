use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use abq_utils::{api::ApiKey, auth::ClientToken, net_opt::Tls, net_protocol::workers::RunId};
use clap::{ArgAction, ArgGroup, Parser, Subcommand};

use crate::reporting::{ColorPreference, ReporterKind};

pub(crate) fn default_num_workers() -> NonZeroUsize {
    let cpus = num_cpus::get_physical();
    NonZeroUsize::new(cpus).expect("No CPUs detected on this machine")
}

pub(crate) fn default_num_workers_for_test() -> NonZeroUsize {
    let cpus_without_one = num_cpus::get_physical() - 1;
    NonZeroUsize::new(std::cmp::Ord::max(cpus_without_one, 1))
        .expect("No CPUs detected on this machine")
}

const VERSION: &str = include_str!(concat!(env!("OUT_DIR"), "/abq_version.txt"));

/// Always be queueing
///
/// The abq cli
#[derive(Parser)]
#[clap(version = VERSION)]
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
        /// Need a token? Use `abq token new`!
        #[clap(long)]
        token: Option<ClientToken>,

        /// Whether to accept messages only with TLS; false by default.
        /// When set, workers and test clients must also be set to send messages only with TLS.
        #[clap(long, action = ArgAction::SetTrue, required = false)]
        tls: Tls,
    },
    /// Starts a pool of abq workers in a working directory.
    ///
    /// You should use this to start workers on a remote machine that you'd like to connect to an
    /// instance of `abq start`.
    #[command(group(
        ArgGroup::new("execution") // don't allow both queue_addr and num_workers params
            .required(true)
            .multiple(false)
            .args(["api_key", "queue_addr"]),
        ))]
    Work {
        /// Working directory of the workers. Defaults to the current directory.
        #[clap(long, required = true, env("PWD"))]
        working_dir: PathBuf,

        /// The ID of the test run to pull work for.
        /// In CI environments, this can be inferred from CI environment variables. Otherwise it is required.
        #[clap(long, required = false, env("ABQ_RUN_ID"))]
        run_id: Option<RunId>,

        /// The API key to use when fetching queue config information from the ABQ API.
        ///
        /// Cannot be used with queue_addr (implies: not using the ABQ API).
        #[clap(long, required = false, env("ABQ_API_KEY"))]
        api_key: Option<ApiKey>,

        /// Address of the queue to work from.
        ///
        /// Cannot be used with api_key (will fetch address from ABQ API).
        #[clap(long, required = false)]
        queue_addr: Option<SocketAddr>,

        /// Number of workers to start. Must be >= 1. Defaults to the number of available (physical)
        /// CPUs
        #[clap(long, short = 'n', required = false, default_value_t = default_num_workers())]
        num: NonZeroUsize,

        /// Token to authorize messages sent to the queue with.
        /// Usually, this should be the same token that `abq start` initialized with.
        ///
        /// If --api-key is specified, the token will be ignored and the token fetched from the ABQ API will be used.
        #[clap(long, required = false)]
        token: Option<ClientToken>,

        /// Whether to send messages only with TLS; false by default.
        /// When set, only queues configured with TLS as well should be provided via
        /// `--queue-addr`.
        ///
        /// If --api-key is specified, the tls flag will be ignored and the setting fetched from the ABQ API will be used.
        #[clap(long, action = ArgAction::SetTrue, required = false)]
        tls: Tls,
    },
    /// Starts an instance of `abq test`.
    ///
    /// Examples:
    ///
    ///   abq test -- yarn jest -t "onboard flow"
    ///   abq test -- cargo test
    ///
    /// The given executable must be available on abq workers fulfilling this test request, and must resolve to an executable that implements the ABQ protocol.
    ///
    #[clap(verbatim_doc_comment)]
    #[command(group(
        ArgGroup::new("execution") // don't allow both queue_addr and num_workers params
            .multiple(false)
            .args(["api_key", "queue_addr", "num_workers"]),
        ))]
    Test {
        /// Run ID for workers to connect to. If not specified, workers are started in-process.
        /// In CI environments, this can be inferred from CI environment variables.
        #[clap(long, required = false, env("ABQ_RUN_ID"))]
        run_id: Option<RunId>,

        /// The API key to use when fetching queue config information from the ABQ API.
        ///
        /// Cannot be used with queue_addr (implies: not using the ABQ API) or num_workers (implies: run locally).
        #[clap(long, required = false, env("ABQ_API_KEY"))]
        api_key: Option<ApiKey>,

        /// Address of the queue where the test command will be sent.
        ///
        /// Requires that abq workers be started as seperate processes connected to the queue.
        ///
        /// Cannot be used with api_key (will fetch address from ABQ API) or num_workers (implies: run locally).
        #[clap(long, required = false)]
        queue_addr: Option<SocketAddr>,

        /// Specifices the number of workers to start when running in standalone. Must be >= 1. Defaults to the number of available (physical)
        /// CPUs - 1.
        ///
        /// Cannot be used with api_key (will fetch address from ABQ API) or queue_addr (implies: not using the ABQ API)
        #[clap(long, short = 'n', required = false, default_value_t = default_num_workers_for_test())]
        num_workers: NonZeroUsize,

        /// Token to authorize messages sent to the queue with.
        /// Usually, this should be the same token that `abq start` initialized with.
        ///
        /// If --api-key is specified, the token will be ignored and the token fetched from the ABQ API will be used.
        #[clap(long, required = false)]
        token: Option<ClientToken>,

        /// Whether to send messages only with TLS; false by default.
        /// When set, only queues configured with TLS as well should be provided via
        /// `--queue-addr`.
        ///
        /// If --api-key is specified, the tls flag will be ignored and the setting fetched from the ABQ API will be used.
        #[clap(long, action = ArgAction::SetTrue, required = false)]
        tls: Tls,

        /// Test result reporter to use for a test run.
        #[clap(long, default_value = "line")]
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

        /// The maximum number of seconds to wait for a test result to be reported back.
        ///
        /// Note that this applies to both the first test result reported back, and all subsequent
        /// test results. When setting a timeout, it is recommended to significantly over-estimate
        /// based on historical runtimes you have observed for abq.
        ///
        /// Hitting a timeout is typically indicative of a failure in ABQ workers.
        ///
        /// By default, the timeout is unbound.
        #[clap(long, default_value_t = abq_queue::invoke::DEFAULT_CLIENT_POLL_TIMEOUT.as_secs().try_into().unwrap())]
        result_timeout_seconds: NonZeroU64,

        /// Arguments to the test executable.
        #[clap(required = true, num_args=1.., allow_hyphen_values = true)]
        args: Vec<String>,
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
        token: Option<ClientToken>,

        /// Whether the services being checked are configured with TLS.
        #[clap(long, action=ArgAction::SetTrue, required = false)]
        tls: Tls,
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
