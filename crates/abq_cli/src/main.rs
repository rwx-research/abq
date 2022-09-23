mod args;
mod health;
mod instance;
mod reporting;
mod workers;

use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    num::NonZeroU64,
    thread::{self, JoinHandle},
};

use abq_queue::invoke::Client;
use abq_utils::{
    auth::{AuthToken, ClientAuthStrategy, ServerAuthStrategy},
    net_opt::{ClientOptions, ServerOptions, Tls},
    net_protocol::{
        entity::EntityId,
        runners::TestResult,
        workers::{NativeTestRunnerParams, RunId, RunnerKind, WorkId},
    },
};
use abq_workers::negotiate::QueueNegotiatorHandle;
use clap::Parser;

use args::{default_num_workers, Cli, Command};

use instance::AbqInstance;
use reporting::{ColorPreference, ExitCode, ReporterKind, SuiteReporters};
use tracing::metadata::LevelFilter;
use tracing_subscriber::{fmt, layer, prelude::*, EnvFilter, Registry};

use crate::{args::Token, health::HealthCheckKind};

fn main() -> anyhow::Result<()> {
    let exit_code = abq_main()?;
    std::process::exit(exit_code.get());
}

struct TracingGuards {
    _file_appender_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

#[must_use]
fn setup_tracing() -> TracingGuards {
    // Trace to standard error with ABQ_LOG set. If unset, trace and log nothing.
    let env_filter = EnvFilter::builder()
        .with_env_var("ABQ_LOG")
        .with_default_directive(LevelFilter::OFF.into())
        .from_env()
        .unwrap();

    let stderr_layer = fmt::Layer::default()
        .with_writer(std::io::stderr)
        .with_span_events(fmt::format::FmtSpan::ACTIVE)
        .with_filter(env_filter);

    // Trace to a rotating log file under ABQ_FILE_LOG_DIR, if it's set.
    let (file_log_layer, file_appender_guard) = if let Ok(dir) = std::env::var("ABQ_FILE_LOG_DIR") {
        let file_appender = tracing_appender::rolling::hourly(dir, "abq.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        (
            fmt::Layer::default()
                .with_writer(non_blocking)
                .with_ansi(false)
                .boxed(),
            Some(guard),
        )
    } else {
        (layer::Identity::new().boxed(), None)
    };

    Registry::default()
        .with(stderr_layer)
        .with(file_log_layer)
        .init();

    TracingGuards {
        _file_appender_guard: file_appender_guard,
    }
}

fn abq_main() -> anyhow::Result<ExitCode> {
    use clap::{CommandFactory, ErrorKind};

    let _tracing_guards = setup_tracing();

    let Cli { command } = Cli::parse();

    let entity = EntityId::new();

    tracing::debug!(?entity, "new abq client");

    match command {
        Command::Start {
            bind: bind_ip,
            public_ip,
            port: server_port,
            work_port,
            negotiator_port,
            token,
            tls,
        } => instance::start_abq_forever(
            public_ip,
            bind_ip,
            server_port,
            work_port,
            negotiator_port,
            ServerOptions::new(token.into(), tls),
        ),
        Command::Work {
            working_dir,
            queue_addr,
            run_id,
            num,
            token,
            tls,
        } => {
            let client_opts = ClientOptions::new(ClientAuthStrategy::from(token), tls);
            let queue_negotiator =
                QueueNegotiatorHandle::ask_queue(entity, queue_addr, client_opts)?;
            workers::start_workers_forever(num, working_dir, queue_negotiator, client_opts, run_id)
        }
        Command::Test {
            args,
            run_id,
            queue_addr,
            reporter: reporters,
            token,
            tls,
            color,
            batch_size,
        } => {
            let (server_auth, client_auth) = (token.into(), token.into());

            let runner_params = validate_abq_test_args(args)?;
            let abq = find_or_create_abq(entity, queue_addr, server_auth, client_auth, tls)?;
            let runner = RunnerKind::GenericNativeTestRunner(runner_params);
            run_tests(entity, runner, abq, run_id, reporters, color, batch_size)
        }
        Command::Health {
            queue,
            work_scheduler,
            negotiator,
            token,
            tls,
        } => {
            let mut to_check = (queue.into_iter().map(HealthCheckKind::Queue))
                .chain(
                    work_scheduler
                        .into_iter()
                        .map(HealthCheckKind::WorkScheduler),
                )
                .chain(negotiator.into_iter().map(HealthCheckKind::Negotiator))
                .peekable();
            if to_check.peek().is_none() {
                let mut cmd = Cli::command();
                Err(cmd.error(
                    ErrorKind::TooFewValues,
                    "no services provided to healthcheck!",
                ))?;
            }

            let client_auth = token.into();
            let client_options = ClientOptions::new(client_auth, tls);

            let mut all_healthy = true;
            for service in to_check {
                if !service.is_healthy(client_options) {
                    all_healthy = false;
                    println!("{service}: unhealthy");
                }
            }
            let exit = if all_healthy { 0 } else { 1 };
            Ok(reporting::ExitCode::new(exit))
        }
        Command::Token(Token::New) => {
            let token = AuthToken::new_random();
            println!("{token}");
            Ok(ExitCode::new(0))
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
        extra_env: HashMap::from([
            // TODO: This is a hack to get chalk.js to add color codes
            // We probably should instead supply a PTY to the child process
            (String::from("FORCE_COLOR"), String::from("1")),
        ]),
    })
}

fn find_or_create_abq(
    entity: EntityId,
    opt_queue_addr: Option<SocketAddr>,
    server_auth: ServerAuthStrategy,
    client_auth: ClientAuthStrategy,
    tls: Tls,
) -> anyhow::Result<AbqInstance> {
    match opt_queue_addr {
        Some(queue_addr) => {
            let instance = AbqInstance::from_remote(entity, queue_addr, client_auth, tls)?;
            Ok(instance)
        }
        None => Ok(AbqInstance::new_ephemeral(server_auth, client_auth, tls)),
    }
}

fn run_tests(
    entity: EntityId,
    runner: RunnerKind,
    abq: AbqInstance,
    opt_test_id: Option<RunId>,
    reporters: Vec<ReporterKind>,
    color_choice: ColorPreference,
    batch_size: NonZeroU64,
) -> anyhow::Result<ExitCode> {
    let test_suite_name = "suite"; // TODO: determine this correctly
    let mut reporters = SuiteReporters::new(reporters, color_choice, test_suite_name);

    let on_result = {
        // Safety: rustc wants the `collector` to be live for the lifetime of the program because
        // `work_results_thread` might escape. But, we know that `work_results_thread` won't
        // escape; it vanishes at the end of this function, before `collector` is dropped.
        let reporters: &'static mut SuiteReporters = unsafe { std::mem::transmute(&mut reporters) };

        move |_, test_result| {
            // TODO: is there a reasonable way to surface the error?
            let _opt_error = reporters.push_result(&test_result);
        }
    };

    let start_in_process_workers = opt_test_id.is_none();
    let test_id = opt_test_id.unwrap_or_else(RunId::new);

    let work_results_thread = start_test_result_reporter(
        entity,
        abq.server_addr(),
        abq.client_options(),
        test_id,
        runner,
        batch_size,
        on_result,
    );

    let opt_workers = if start_in_process_workers {
        let working_dir = std::env::current_dir().expect("no working directory");
        let workers = workers::start_workers(
            default_num_workers(),
            working_dir,
            abq.negotiator_handle(),
            abq.client_options(),
            test_id,
        )?;
        Some(workers)
    } else {
        println!("Starting test run with ID {}", test_id);
        None
    };

    if let Some(mut workers) = opt_workers {
        // The exit code will be determined by the test result status.
        let _exit_code = workers.shutdown();
    }

    work_results_thread.join().unwrap()?;

    // TODO: is there a reasonable way to surface the errors?
    let (suite_result, _errors) = reporters.finish();

    Ok(suite_result.suggested_exit_code)
}

fn start_test_result_reporter(
    entity: EntityId,
    abq_server_addr: SocketAddr,
    client_opts: ClientOptions,
    test_id: RunId,
    runner: RunnerKind,
    batch_size: NonZeroU64,
    on_result: impl FnMut(WorkId, TestResult) + Send + 'static,
) -> JoinHandle<Result<(), io::Error>> {
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async move {
            let abq_test_client = Client::invoke_work(
                entity,
                abq_server_addr,
                client_opts,
                test_id,
                runner,
                batch_size,
            )
            .await?;
            abq_test_client.stream_results(on_result).await
        })
    })
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
            extra_env: _,
        } = result.unwrap();

        assert_eq!(cmd, "abq-test");
        assert!(args.is_empty());
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
            extra_env: _,
        } = result.unwrap();

        assert_eq!(cmd, "abq-test");
        assert_eq!(args, vec!["--filter", "onboarding"]);
    }
}
