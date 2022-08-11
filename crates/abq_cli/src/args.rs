use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::PathBuf,
};

use abq_utils::net_protocol::workers::InvocationId;
use clap::{Parser, Subcommand};

use crate::reporting::ReporterKind;

pub(crate) fn unspecified_socket_addr() -> SocketAddr {
    // Can't be a constant due to https://github.com/rust-lang/rust/issues/67390
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
}

pub(crate) fn default_num_workers() -> NonZeroUsize {
    let cpus = num_cpus::get();
    NonZeroUsize::new(cpus).expect("No CPUs detected on this machine")
}

/// Always be queueing
///
/// The abq cli
#[derive(Parser)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Starts the "abq" ephemeral queue.
    Start {
        /// Host/port IP address to bind the queue to.
        /// When not specified, an arbitrary open port on the unspecified address 0.0.0.0 is
        /// chosen.
        #[clap(long, required = false, default_value_t = unspecified_socket_addr())]
        bind: SocketAddr,

        /// Host IP address to advertise the queue as running on.
        /// When not specified, falls back on `bin`; otherwise, the unspecified address 0.0.0.0 is
        /// used.
        #[clap(long, required = false)]
        public_ip: Option<IpAddr>,
    },
    /// Starts a pool of abq workers in a working directory.
    ///
    /// You should use this to start workers on a remote machine that you'd like to connect to an
    /// instance of `abq start`.
    Work {
        /// Address of the queue to connect to.
        #[clap(long, required = true)]
        queue_addr: SocketAddr,

        /// Working directory of the workers.
        #[clap(long, required = true)]
        working_dir: PathBuf,

        /// The ID of the test run to pull work for.
        test_run: InvocationId,

        /// Number of workers to start. Must be >= 4. Defaults to the number of available (logical)
        /// CPUs - 1.
        #[clap(long, short = 'n', required = false, default_value_t = default_num_workers())]
        num: NonZeroUsize,
    },
    /// Starts an instance of `abq test`. Examples:
    ///
    ///   abq test -- yarn jest -t "onboard flow"
    ///   abq test -- cargo test
    ///
    /// The given executable must be available on abq workers fulfilling this test request,
    /// and must resolve to an executable that implements the ABQ protocol.
    Test {
        /// Test ID for workers to connect to. If not specified, workers are started in-process.
        #[clap(long, required = false)]
        test_id: Option<InvocationId>,

        // TODO: have folks specify queue-addr, and determine the queue and negotiator based on that
        /// Address of the queue to send the test request to.
        ///
        /// Must be specified if and only if `queue_addr` is also specified.
        #[clap(long, required = false)]
        queue_addr: Option<SocketAddr>,

        /// Address workers should use to connect to the queue.
        ///
        /// Must be specified if and only if `queue_addr` is also specified.
        #[clap(long, required = false)]
        negotiator_addr: Option<SocketAddr>,

        /// Test result reporter to use for a test run.
        #[clap(long, default_value = "line")]
        reporter: Vec<ReporterKind>,

        /// Arguments to the test executable.
        #[clap(required = true, multiple_values = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}
