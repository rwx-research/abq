use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use abq_utils::net_protocol::workers::InvocationId;
use clap::{Parser, Subcommand};

use crate::reporting::ReporterKind;

pub(crate) fn unspecified_socket_addr() -> SocketAddr {
    // Can't be a constant due to https://github.com/rust-lang/rust/issues/67390
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
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
    },
    /// Starts an instance of `abq test`. Examples:
    ///
    ///   abq test -- yarn jest -t "onboard flow"
    ///   abq test -- cargo test
    ///
    /// The given executable must be available on abq workers fulfilling this test request,
    /// and must resolve to an executable that implements the ABQ protocol.
    Test {
        /// Create a set of workers in-process when running tests, rather than delegating to external
        /// workers.
        #[clap(long)]
        auto_workers: bool,

        /// Test result reporter to use for a test run.
        #[clap(long, default_value = "line")]
        reporter: Vec<ReporterKind>,

        /// Arguments to the test executable.
        #[clap(required = true, multiple_values = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}
