mod args;
mod collect;
mod instance;
mod plugin;

use clap::Parser;

use collect::run_work;

use args::{CargoCmd, Cli, Command};

fn main() {
    env_logger::Builder::from_env("ABQ_LOG").init();

    let args = Cli::parse();

    match args.command {
        Command::Start => instance::start_abq_forever(),
        Command::Echo { strings } => {
            let abq = instance::find_abq();
            let collector = plugin::echo::collector(strings);
            run_work(abq, collector);
        }
        Command::Cargo(CargoCmd::Test) => {
            let abq = instance::find_abq();
            let collector = plugin::cargo::collector();
            run_work(abq, collector);
        }
        Command::Jest => {
            let abq = instance::find_abq();
            let collector = plugin::jest::collector();
            run_work(abq, collector);
        }
    }
}
