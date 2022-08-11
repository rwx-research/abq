mod args;
mod instance;
mod reporting;
mod workers;

use std::{net::SocketAddr, thread};

use abq_queue::invoke::invoke_work;
use abq_utils::net_protocol::workers::{InvocationId, NativeTestRunnerParams, RunnerKind};
use abq_workers::negotiate::QueueNegotiatorHandle;
use clap::Parser;

use args::{default_num_workers, Cli, Command};

use instance::AbqInstance;
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
        Command::Start { bind, public_ip } => instance::start_abq_forever(bind, public_ip),
        Command::Work {
            working_dir,
            queue_addr,
            test_run,
            num,
        } => {
            let queue_negotiator = QueueNegotiatorHandle::ask_queue(queue_addr)?;
            workers::start_workers_forever(num, working_dir, queue_negotiator, test_run)
        }
        Command::Test {
            args,
            test_id,
            queue_addr,
            reporter: reporters,
        } => {
            let runner_params = validate_abq_test_args(args)?;
            let abq = find_or_create_abq(queue_addr)?;
            let runner = RunnerKind::GenericNativeTestRunner(runner_params);
            run_tests(runner, abq, test_id, reporters)
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

fn find_or_create_abq(opt_queue_addr: Option<SocketAddr>) -> anyhow::Result<AbqInstance> {
    match opt_queue_addr {
        Some(queue_addr) => {
            let instance = AbqInstance::from_remote(queue_addr)?;
            Ok(instance)
        }
        None => Ok(AbqInstance::new_ephemeral()),
    }
}

fn run_tests(
    runner: RunnerKind,
    abq: AbqInstance,
    opt_test_id: Option<InvocationId>,
    reporters: Vec<ReporterKind>,
) -> anyhow::Result<()> {
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

    let start_in_process_workers = opt_test_id.is_none();
    let test_id = opt_test_id.unwrap_or_else(InvocationId::new);

    let work_results_thread = thread::spawn({
        let server_addr = abq.server_addr();
        move || invoke_work(server_addr, test_id, runner, on_result)
    });

    let opt_workers = if start_in_process_workers {
        let working_dir = std::env::current_dir().expect("no working directory");
        let workers = workers::start_workers(
            default_num_workers(),
            working_dir,
            abq.negotiator_handle(),
            test_id,
        )?;
        Some(workers)
    } else {
        println!("Starting test run with ID {}", test_id);
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
