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
    Start,
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
}

#[derive(Subcommand)]
pub enum CargoCmd {
    /// Runs `cargo test` for the project in the current directory.
    Test,
}
