use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};

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
    Start {},
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
    },
    /// Echoes one or more strings.
    /// If the abq queue is not running, a short-lived one is used.
    #[clap(arg_required_else_help = true)]
    Echo {
        /// Strings to echo.
        #[clap(required = true)]
        strings: Vec<String>,
    },
    /// Runs commands related to Rust's `cargo` toolchain.
    #[clap(subcommand)]
    Cargo(CargoCmd),
    Jest,
}

#[derive(Subcommand)]
pub enum CargoCmd {
    /// Runs `cargo test` for the project in the current directory.
    Test,
}
