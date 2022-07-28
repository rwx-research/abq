mod args;
mod instance;
mod reporting;
mod workers;

use std::thread;

use abq_queue::invoke::invoke_work;
use abq_utils::net_protocol::workers::{InvocationId, NativeTestRunnerParams, RunnerKind};
use clap::Parser;

use args::{Cli, Command};

use reporting::{ReporterKind, Reporters};
use tracing_subscriber::{fmt, EnvFilter};

fn main() -> anyhow::Result<()> {
    let tracing_fmt = fmt()
        .with_span_events(fmt::format::FmtSpan::ACTIVE)
        .with_env_filter(EnvFilter::from_env("ABQ_LOG"))
        .finish();
    tracing::subscriber::set_global_default(tracing_fmt).unwrap();

    let Cli { command } = Cli::parse();

    match command {
        Command::Start { bind } => instance::start_abq_forever(bind),
        Command::Work {
            working_dir,
            queue_addr,
            test_run,
        } => workers::start_workers_forever(working_dir, queue_addr, test_run),
        Command::Test {
            args,
            auto_workers,
            reporter: reporters,
        } => {
            let runner_params = validate_abq_test_args(args)?;
            let runner = RunnerKind::GenericNativeTestRunner(runner_params);
            run_tests(runner, auto_workers, reporters)
        }
    }
}

fn validate_abq_test_args(mut args: Vec<String>) -> Result<NativeTestRunnerParams, clap::Error> {
    use clap::{CommandFactory, ErrorKind};
    if args.is_empty() {
        let mut cmd = Cli::command();
        return Err(cmd.error(
            ErrorKind::InvalidValue,
            "`abq test` is missing an executable to run!",
        ));
    }
    let cmd = args.remove(0);
    Ok(NativeTestRunnerParams {
        cmd,
        args,
        // TODO: populate this
        extra_env: Default::default(),
    })
}

fn run_tests(
    runner: RunnerKind,
    auto_workers: bool,
    reporters: Vec<ReporterKind>,
) -> anyhow::Result<()> {
    let abq = instance::get_abq();

    let test_suite_name = "suite"; // TODO: determine this correctly
    let mut reporters = Reporters::new(reporters, test_suite_name);

    let on_result = {
        // Safety: rustc wants the `collector` to be live for the lifetime of the program because
        // `work_results_thread` might escape. But, we know that `work_results_thread` won't
        // escape; it vanishes at the end of this function, before `collector` is dropped.
        let reporters: &'static mut Reporters = unsafe { std::mem::transmute(&mut reporters) };

        move |_, test_result| {
            // TODO: is there a reasonable way to surface the error?
            let _opt_error = reporters.push_result(&test_result);
        }
    };
    let invocation_id = InvocationId::new();

    let work_results_thread = thread::spawn({
        let server_addr = abq.server_addr();
        move || invoke_work(server_addr, invocation_id, runner, on_result)
    });

    let opt_workers = if auto_workers {
        let working_dir = std::env::current_dir().expect("no working directory");
        let negotiator_addr = abq.negotiator_addr();
        let workers = workers::start_workers(working_dir, negotiator_addr, invocation_id)?;
        Some(workers)
    } else {
        println!("Starting test run with ID {}", invocation_id);
        None
    };

    if let Some(mut workers) = opt_workers {
        workers.shutdown();
    }

    work_results_thread
        .join()
        .expect("results thread should always be free");

    // TODO: is there a reasonable way to surface the errors?
    let _opt_error = reporters.finish();

    Ok(())
}

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::workers::NativeTestRunnerParams;
    use clap::ErrorKind;

    use super::validate_abq_test_args;

    #[test]
    fn validate_test_args_empty() {
        let result = validate_abq_test_args(vec![]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidValue);
        assert!(err.to_string().contains("missing an executable to run"));
    }

    #[test]
    fn validate_one_test_arg() {
        let result = validate_abq_test_args(vec!["abq-test".to_string()]);
        assert!(result.is_ok());
        let NativeTestRunnerParams {
            cmd,
            args,
            extra_env,
        } = result.unwrap();

        assert_eq!(cmd, "abq-test");
        assert!(args.is_empty());
        assert!(extra_env.is_empty());
    }

    #[test]
    fn validate_multiple_args() {
        let result = validate_abq_test_args(vec![
            "abq-test".to_string(),
            "--filter".to_string(),
            "onboarding".to_string(),
        ]);
        assert!(result.is_ok());
        let NativeTestRunnerParams {
            cmd,
            args,
            extra_env,
        } = result.unwrap();

        assert_eq!(cmd, "abq-test");
        assert_eq!(args, vec!["--filter", "onboarding"]);
        assert!(extra_env.is_empty());
    }
}
