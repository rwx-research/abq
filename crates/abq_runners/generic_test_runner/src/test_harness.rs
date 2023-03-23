//! Binary that can serve as a testing harness of ABQ workers for native runner implementations
//! wishing to check that their communication protocol is what a ABQ worker expects.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};

use abq_generic_test_runner::{
    execute_wrapped_runner, open_native_runner_connection, wait_for_manifest,
};
use abq_utils::net_protocol::workers::NativeTestRunnerParams;

/// Test harness simulating a ABQ worker, for use by native runner implementations.
#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Exercises a native runner in an end-to-end fashion, requesting a manifest and running all
    /// tests.
    ///
    /// When successful, test results are printed to stdout as JSON, with an exit code of zero.
    /// A non-zero exit code indicates some failure. In the future we'll try to print a nice
    /// diagnostic message in such cases.
    E2e {
        /// File to write the JSON test output to.
        #[clap(long, required = true)]
        results: PathBuf,

        /// File to write the manifest output to.
        #[clap(long, required = true)]
        manifest: PathBuf,

        /// Working directory the native runner should run in.
        /// When not specified, uses cwd this executable is running in.
        #[clap(long, required = false)]
        working_dir: Option<PathBuf>,

        /// Arguments to the test executable.
        #[clap(required = true, num_args = 1.., allow_hyphen_values = true, last = true)]
        args: Vec<String>,
    },

    /// Starts a server waiting for a manifest from a native test runner.
    ///
    /// When the manifest is received, it is printed to stdout as JSON and this process exits with
    /// zero.
    /// A non-zero exit code indicates some failure, for which a message will be available on
    /// stderr.
    Manifest {
        /// The address the server should listen on. This must be provided by the executor, and
        /// should be the address the native runner sends the manifest to.
        #[clap(long, required = true)]
        server_addr: SocketAddr,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::E2e {
            working_dir,
            results,
            manifest,
            mut args,
        } => {
            let working_dir = match working_dir {
                Some(working_dir) => working_dir,
                None => std::env::current_dir()?,
            };

            let cmd = args.remove(0);

            let native_runner_params = NativeTestRunnerParams {
                cmd,
                args,
                // TODO: support extra env on command line
                extra_env: Default::default(),
            };

            let (manifest_data, test_results) =
                execute_wrapped_runner(native_runner_params, working_dir)?;

            {
                let results_fs = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(results)?;

                serde_json::to_writer(results_fs, &test_results)?;
            }

            {
                let manifest_fs = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(manifest)?;

                serde_json::to_writer(manifest_fs, &manifest_data)?;
            }

            Ok(())
        }
        Command::Manifest { server_addr } => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async {
                let mut server = tokio::net::TcpListener::bind(server_addr).await?;

                let (_runner_info, mut runner_conn) =
                    open_native_runner_connection(&mut server, Duration::from_secs(10)).await?;
                let manifest = wait_for_manifest(&mut runner_conn).await?;

                serde_json::to_writer(std::io::stdout(), &manifest)?;

                Ok(())
            })
        }
    }
}
