use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use abq_utils::net_protocol::workers::InvocationId;
use clap::{Parser, Subcommand};

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

    /// Create a set of workers in-process when running tests, rather than delegating to external
    /// workers. (Only relevant for commands that run tests)
    #[clap(long)]
    pub auto_workers: bool,
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
    /// Runs commands related to Rust's `cargo` toolchain.
    #[clap(subcommand)]
    Cargo(CargoCmd),
    /// Runs `yarn jest`.
    Jest {
        /// Wrapper to run `abq-jest` in. Common ones include `yarn` and `npm`.
        /// A wrapper of "" will run `jest` standalone.
        #[clap(long, default_value = "yarn")]
        wrapper: String,

        /// Extra arguments to pass to `jest`.
        #[clap(long, multiple_values = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

#[derive(Subcommand)]
pub enum CargoCmd {
    /// Runs `cargo test` for the project in the current directory.
    Test {
        /// Extra arguments to pass to `cargo test`.
        #[clap(long, multiple_values = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}
