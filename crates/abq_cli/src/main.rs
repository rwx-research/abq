mod args;
mod instance;
mod workers;

use abq_output::format_result;
use abq_queue::invoke::invoke_work;
use clap::Parser;

use args::{CargoCmd, Cli, Command};

fn main() {
    env_logger::Builder::from_env("ABQ_LOG").init();

    let args = Cli::parse();

    match args.command {
        Command::Start { bind } => instance::start_abq_forever(bind),
        Command::Work {
            kind,
            working_dir,
            queue_addr,
        } => workers::start_workers(kind, working_dir, queue_addr),
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
            invoke_work(abq.server_addr(), on_result);
        }
    }
}
