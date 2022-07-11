mod args;
mod instance;
mod workers;

use abq_output::format_result;
use abq_queue::invoke::invoke_work;
use abq_utils::net_protocol::workers::{NativeTestRunnerParams, RunnerKind};
use clap::Parser;

use args::{CargoCmd, Cli, Command};

fn main() {
    env_logger::Builder::from_env("ABQ_LOG").init();

    let args = Cli::parse();

    match args.command {
        Command::Start { bind } => instance::start_abq_forever(bind),
        Command::Work {
            working_dir,
            queue_addr,
        } => workers::start_workers(working_dir, queue_addr),
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
            invoke_work(abq.server_addr(), jest_runner, on_result);
        }
    }
}
