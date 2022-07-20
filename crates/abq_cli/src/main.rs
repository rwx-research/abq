mod args;
mod instance;
mod workers;

use std::thread;

use abq_output::format_result;
use abq_queue::invoke::invoke_work;
use abq_utils::net_protocol::workers::{InvocationId, NativeTestRunnerParams, RunnerKind};
use clap::Parser;

use args::{CargoCmd, Cli, Command};

use tracing_subscriber::{fmt, EnvFilter};

fn main() -> anyhow::Result<()> {
    let tracing_fmt = fmt()
        .with_span_events(fmt::format::FmtSpan::ACTIVE)
        .with_env_filter(EnvFilter::from_env("ABQ_LOG"))
        .finish();
    tracing::subscriber::set_global_default(tracing_fmt).unwrap();

    let Cli {
        command,
        auto_workers,
    } = Cli::parse();

    match command {
        Command::Start { bind } => instance::start_abq_forever(bind),
        Command::Work {
            working_dir,
            queue_addr,
            test_run,
        } => workers::start_workers(working_dir, queue_addr, test_run),
        Command::Cargo(CargoCmd::Test { mut args }) => {
            let cmd = "cargo".to_string();
            args.insert(0, "test".to_string());
            let cargo_runner = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                cmd,
                args,
                extra_env: Default::default(),
            });
            run_tests(cargo_runner, auto_workers)
        }
        Command::Jest { wrapper, mut args } => {
            let (cmd, args) = match wrapper.as_str() {
                "" => ("jest".to_string(), args),
                _ => {
                    args.insert(0, "jest".to_string());
                    (wrapper, args)
                }
            };

            let jest_runner = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                cmd,
                args,
                extra_env: Default::default(),
            });
            run_tests(jest_runner, auto_workers)
        }
    }
}

fn run_tests(runner: RunnerKind, auto_workers: bool) -> anyhow::Result<()> {
    let abq = instance::get_abq();
    let on_result = |_, test_result| {
        println!("{}", format_result(test_result));
    };
    let invocation_id = InvocationId::new();

    let opt_workers_handle = if auto_workers {
        let working_dir = std::env::current_dir().expect("no working directory");
        let negotiator_addr = abq.negotiator_addr();
        let workers_handle = thread::spawn(move || {
            workers::start_workers(working_dir, negotiator_addr, invocation_id)
        });
        Some(workers_handle)
    } else {
        println!("Starting test run with ID {}", invocation_id);
        None
    };

    invoke_work(abq.server_addr(), invocation_id, runner, on_result);

    if let Some(workers_handle) = opt_workers_handle {
        let _ = workers_handle.join();
    }

    Ok(())
}
