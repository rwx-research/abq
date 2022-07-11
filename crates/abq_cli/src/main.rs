mod args;
mod instance;
mod workers;

use abq_output::format_result;
use abq_queue::invoke::invoke_work;
use abq_utils::net_protocol::workers::{InvocationId, NativeTestRunnerParams, RunnerKind};
use clap::Parser;

use args::{CargoCmd, Cli, Command};

use tracing_subscriber::{fmt, EnvFilter};

fn main() {
    let tracing_fmt = fmt()
        .with_span_events(fmt::format::FmtSpan::ACTIVE)
        .with_env_filter(EnvFilter::from_env("ABQ_LOG"))
        .finish();
    tracing::subscriber::set_global_default(tracing_fmt).unwrap();

    let args = Cli::parse();

    match args.command {
        Command::Start { bind } => instance::start_abq_forever(bind),
        Command::Work {
            working_dir,
            queue_addr,
            test_run,
        } => workers::start_workers(working_dir, queue_addr, test_run),
        Command::Echo { strings: _ } => {
            todo!();
            // let abq = instance::find_abq();
            // let collector = plugin::echo::collector(strings);
            // run_work(abq, collector);
        }
        Command::Cargo(CargoCmd::Test) => {
            todo!();
            // let abq = instance::find_abq();
            // let collector = plugin::cargo::collector();
            // run_work(abq, collector);
        }
        Command::Jest => {
            let abq = instance::find_abq();
            let on_result = |_, test_result| {
                println!("{}", format_result(test_result));
            };
            let jest_runner = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                cmd: "yarn".to_string(),
                args: vec!["jest".to_string()],
                extra_env: Default::default(),
            });
            let invocation_id = InvocationId::new();
            println!("Starting test run with ID {}", invocation_id);
            invoke_work(abq.server_addr(), invocation_id, jest_runner, on_result);
        }
    }
}
