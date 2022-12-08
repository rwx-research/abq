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
    num::NonZeroUsize,
    path::PathBuf,
    thread::{self, JoinHandle},
    time::Duration,
};

use abq_hosted::AccessToken;
use abq_output::format_duration;
use abq_queue::invoke::{self, Client, CompletedSummary, InvocationError, TestResultError};
use abq_utils::{
    auth::{ClientAuthStrategy, ServerAuthStrategy, User, UserToken},
    net_opt::{ClientOptions, ServerOptions},
    net_protocol::{
        entity::EntityId,
        health::Health,
        runners::{TestResult, TestRuntime},
        workers::{NativeTestRunnerParams, RunId, RunnerKind, WorkId},
    },
    tls::{ClientTlsStrategy, ServerTlsStrategy},
};
use abq_workers::negotiate::QueueNegotiatorHandle;

use args::{
    Cli, Command,
    NumWorkers::{CpuCores, Fixed},
};
use clap::Parser;

use instance::AbqInstance;
use reporting::{ColorPreference, ExitCode, ReporterKind, SuiteReporters};
use signal_hook::{consts::TERM_SIGNALS, iterator::Signals};
use tracing::{metadata::LevelFilter, Subscriber};
use tracing_subscriber::{fmt, prelude::*, registry, EnvFilter, Registry};

use crate::{args::Token, health::HealthCheckKind};

fn main() -> anyhow::Result<()> {
    let exit_code = abq_main()?;
    std::process::exit(exit_code.get());
}

struct TracingGuards {
    _file_appender_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

struct PrefixedCiEventFormat<T: fmt::time::FormatTime> {
    format: fmt::format::Format<fmt::format::Full, T>,
    prefix: String,
}

struct ConfigFromApi {
    queue_addr: SocketAddr,
    token: UserToken,
    tls_public_certificate: Option<Vec<u8>>,
}

struct RunIdEnvironment {
    abq_run_id: Result<String, std::env::VarError>,
    ci: Result<String, std::env::VarError>,
    buildkite_build_id: Result<String, std::env::VarError>,
    circle_workflow_id: Result<String, std::env::VarError>,
    github_run_id: Result<String, std::env::VarError>,
    github_run_attempt: Result<String, std::env::VarError>,
}

impl RunIdEnvironment {
    fn from_env() -> Self {
        Self {
            abq_run_id: std::env::var("ABQ_RUN_ID"),
            ci: std::env::var("CI"),
            buildkite_build_id: std::env::var("BUILDKITE_BUILD_ID"),
            circle_workflow_id: std::env::var("CIRCLE_WORKFLOW_ID"),
            github_run_id: std::env::var("GITHUB_RUN_ID"),
            github_run_attempt: std::env::var("GITHUB_RUN_ATTEMPT"),
        }
    }
}

impl<S, N, T> fmt::format::FormatEvent<S, N> for PrefixedCiEventFormat<T>
where
    S: Subscriber + for<'a> registry::LookupSpan<'a>,
    N: for<'writer> fmt::FormatFields<'writer> + 'static,
    T: fmt::time::FormatTime,
{
    fn format_event(
        &self,
        ctx: &fmt::FmtContext<'_, S, N>,
        mut writer: fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        writer.write_str(&self.prefix)?;
        self.format.format_event(ctx, writer, event)
    }
}

#[must_use]
fn setup_tracing() -> TracingGuards {
    // Trace to standard error with ABQ_LOG set. If unset, trace and log nothing.
    let env_filter = EnvFilter::builder()
        .with_env_var("ABQ_LOG")
        .with_default_directive(LevelFilter::OFF.into())
        .from_env()
        .unwrap();

    if let Ok(dir) = std::env::var("ABQ_LOGTO") {
        // Trace to a rotating log file under ABQ_LOGTO, if it's set.
        let file_appender = tracing_appender::rolling::hourly(dir, "abq.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        let rolling_file_layer = fmt::Layer::default()
            .with_writer(non_blocking)
            .with_ansi(false)
            .with_filter(env_filter);

        Registry::default().with(rolling_file_layer).init();

        TracingGuards {
            _file_appender_guard: Some(guard),
        }
    } else if let Ok(file) = std::env::var("ABQ_LOGFILE") {
        // Trace to a static log file at ABQ_LOGFILE, if it's set
        let _ = std::fs::remove_file(&file);
        let file_appender = tracing_appender::rolling::never(".", file);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        let file_layer = fmt::Layer::default()
            .with_writer(non_blocking)
            .with_ansi(false)
            .with_filter(env_filter);

        Registry::default().with(file_layer).init();

        TracingGuards {
            _file_appender_guard: Some(guard),
        }
    } else if let Ok(prefix) = std::env::var("ABQ_LOGCI_WITH_PREFIX") {
        // Log to stderr, with all events having a given prefix. For use in CI where we just want
        // to dump all logs to a single stderr stream and filter by prefix later.
        let format = PrefixedCiEventFormat {
            format: fmt::format::Format::default().with_ansi(false),
            prefix,
        };

        let stderr_layer = fmt::Layer::default()
            .with_span_events(fmt::format::FmtSpan::ACTIVE)
            .event_format(format)
            .with_writer(std::io::stderr)
            .with_filter(env_filter);

        Registry::default().with(stderr_layer).init();

        TracingGuards {
            _file_appender_guard: None,
        }
    } else {
        let with_ansi_colors = match std::env::var("ABQ_LOG_COLORS").as_deref() {
            Ok(s) => !matches!(s.trim(), "0"),
            Err(_) => {
                // color by default if unset
                true
            }
        };

        // Otherwise, log to stderr as appropriate.
        let stderr_layer = fmt::Layer::default()
            .with_writer(std::io::stderr)
            .with_span_events(fmt::format::FmtSpan::ACTIVE)
            .with_ansi(with_ansi_colors)
            .with_filter(env_filter);

        Registry::default().with(stderr_layer).init();

        TracingGuards {
            _file_appender_guard: None,
        }
    }
}

fn get_inferred_run_id(run_id_environment: RunIdEnvironment) -> Option<RunId> {
    let RunIdEnvironment {
        abq_run_id,
        ci,
        buildkite_build_id,
        circle_workflow_id,
        github_run_id,
        github_run_attempt,
    } = run_id_environment;

    if abq_run_id.is_ok() || ci.unwrap_or_else(|_| String::from("false")) == *"false" {
        return None;
    }
    // note: if you change this, change it in setup-abq, too
    // https://github.com/rwx-research/setup-abq/blob/8c5044343fceef53b4efea83d64062f006bf6758/src/index.ts#L23-L28
    let github_actions_run_id = github_run_id.map({
        |run_id| {
            format!(
                "{}-{}",
                run_id,
                github_run_attempt.unwrap_or_else(|_| String::from("1"))
            )
        }
    });
    let run_id_result = buildkite_build_id
        .or(circle_workflow_id)
        .or(github_actions_run_id);
    run_id_result.ok().map(RunId)
}

fn abq_main() -> anyhow::Result<ExitCode> {
    use clap::{error::ErrorKind, CommandFactory};

    let _tracing_guards = setup_tracing();

    let inferred_run_id = get_inferred_run_id(RunIdEnvironment::from_env());

    // todo: drop me once we've upgraded all of our scripts
    if let (Ok(access_token), Err(_)) = (
        std::env::var("ABQ_API_KEY"),
        std::env::var("RWX_ACCESS_TOKEN"),
    ) {
        std::env::set_var("RWX_ACCESS_TOKEN", access_token)
    };

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
            user_token,
            admin_token,
            tls_cert,
            tls_key,
        } => {
            let server_auth = match (user_token, admin_token) {
                (Some(user), Some(admin)) => {
                    ServerAuthStrategy::from_set(user, admin)
                }
                (None, None) => {
                    ServerAuthStrategy::no_auth()
                }
                _ => unreachable!("Mutual dependency of tokens should have been caught by clap during arg parsing!"),
            };

            let tls_cert = read_opt_path_bytes(tls_cert)?;
            let tls_key = read_opt_path_bytes(tls_key)?;
            let server_tls = match (tls_cert, tls_key) {
                (Some(cert), Some(key)) => {
                    ServerTlsStrategy::from_cert(&cert, &key)?
                }
                (None, None) => {
                    ServerTlsStrategy::no_tls()
                }
                _ => unreachable!("Mutual dependency of TLS config should have been caught by clap during arg parsing!"),
            };

            instance::start_abq_forever(
                public_ip,
                bind_ip,
                server_port,
                work_port,
                negotiator_port,
                ServerOptions::new(server_auth, server_tls),
            )
        }
        Command::Work {
            working_dir,
            run_id,
            access_token,
            queue_addr,
            num,
            token,
            tls_cert,
        } => {
            let mut cmd = Cli::command();
            let cmd = cmd.find_subcommand_mut("work").unwrap();

            let run_id = run_id.or(inferred_run_id).unwrap_or_else(|| {
                let error = cmd.error(
                    ErrorKind::MissingRequiredArgument,
                    "The following required arguments were not provided: --run-id <RUN_ID>",
                );
                clap::Error::exit(&error);
            });

            let tls_cert = read_opt_path_bytes(tls_cert)?;

            let ResolvedConfig {
                token,
                queue_addr,
                tls_cert,
            } = resolve_config(token, queue_addr, tls_cert, access_token, &run_id)?;

            let queue_addr = queue_addr.unwrap_or_else(|| {
                let error = cmd.error(
                    ErrorKind::MissingRequiredArgument,
                    "One of --access-token <ACCESS_TOKEN> or --queue-addr <QUEUE_ADDR> must be provided.",
                );
                clap::Error::exit(&error);
            });

            let client_tls = match tls_cert {
                Some(cert) => ClientTlsStrategy::from_cert(&cert)?,
                None => ClientTlsStrategy::no_tls(),
            };

            let client_opts = ClientOptions::new(ClientAuthStrategy::from(token), client_tls);
            let queue_negotiator =
                QueueNegotiatorHandle::ask_queue(entity, queue_addr, client_opts.clone())?;
            let num_workers = match num {
                CpuCores => NonZeroUsize::new(num_cpus::get_physical()).unwrap(),
                Fixed(num) => num,
            };

            workers::start_workers_forever(
                num_workers,
                working_dir,
                queue_negotiator,
                client_opts,
                run_id,
            )
        }
        Command::Test {
            args,
            run_id,
            access_token,
            queue_addr,
            num_workers,
            reporter: reporters,
            token,
            tls_cert,
            tls_key,
            color,
            batch_size,
            result_timeout_seconds,
        } => {
            let external_run_id = run_id.or(inferred_run_id);
            let run_id = external_run_id.unwrap_or_else(RunId::unique);

            // Workers are run in-band only if a queue for `abq test` is not going to be
            // provided from an external source.
            let start_in_process_workers =
                num_workers.is_some() || (access_token.is_none() && queue_addr.is_none());

            let tls_cert = read_opt_path_bytes(tls_cert)?;
            let tls_key = read_opt_path_bytes(tls_key)?;

            let ResolvedConfig {
                queue_addr: resolved_queue_addr,
                token: resolved_token,
                tls_cert: resolved_tls,
            } = resolve_config(token, queue_addr, tls_cert, access_token, &run_id)?;

            let client_auth = resolved_token.into();

            let queue_addr_or_opt_tls_key = resolved_queue_addr.ok_or(tls_key);

            let runner_params = validate_abq_test_args(args)?;
            let abq = find_or_create_abq(
                entity,
                queue_addr_or_opt_tls_key,
                resolved_token,
                client_auth,
                resolved_tls,
            )?;
            let results_timeout = Duration::from_secs(result_timeout_seconds.get());

            let actual_num_workers = match num_workers {
                Some(CpuCores) => {
                    NonZeroUsize::new(std::cmp::Ord::max(num_cpus::get_physical() - 1, 1)).unwrap()
                }
                Some(Fixed(num)) => num,
                None => NonZeroUsize::new(1).unwrap(),
            };

            run_tests(
                entity,
                runner_params,
                abq,
                run_id,
                reporters,
                color,
                batch_size,
                results_timeout,
                actual_num_workers,
                start_in_process_workers,
            )
        }
        Command::Health {
            queue,
            work_scheduler,
            negotiator,
            token,
            tls_cert,
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

            let tls_cert = read_opt_path_bytes(tls_cert)?;

            let client_tls = match tls_cert {
                Some(cert) => ClientTlsStrategy::from_cert(&cert)?,
                None => ClientTlsStrategy::no_tls(),
            };

            let client_auth = token.into();
            let client_options = ClientOptions::new(client_auth, client_tls);

            let mut all_healthy = true;
            for service in to_check {
                match service.get_health(client_options.clone()) {
                    Some(Health {
                        healthy: true,
                        version,
                    }) => {
                        println!("{service}: HEALTHY ({version})");
                    }
                    Some(Health {
                        healthy: false,
                        version: _,
                    })
                    | None => {
                        all_healthy = false;
                        println!("{service}: UNHEALTHY");
                    }
                }
            }
            let exit = if all_healthy { 0 } else { 1 };
            Ok(reporting::ExitCode::new(exit))
        }
        Command::Token(Token::New) => {
            let token = UserToken::new_random();
            println!("{token}");
            Ok(ExitCode::new(0))
        }
    }
}

fn read_opt_path_bytes(p: Option<PathBuf>) -> anyhow::Result<Option<Vec<u8>>> {
    match p {
        Some(p) => Ok(Some(read_path_bytes(p)?)),
        None => Ok(None),
    }
}

fn read_path_bytes(p: PathBuf) -> anyhow::Result<Vec<u8>> {
    let bytes = std::fs::read(p)?;
    Ok(bytes)
}

struct ResolvedConfig {
    queue_addr: Option<SocketAddr>,
    token: Option<UserToken>,
    tls_cert: Option<Vec<u8>>,
}

fn resolve_config(
    token_from_cli: Option<UserToken>,
    queue_addr_from_cli: Option<SocketAddr>,
    tls_cert_from_cli: Option<Vec<u8>>,
    access_token: Option<AccessToken>,
    run_id: &RunId,
) -> anyhow::Result<ResolvedConfig> {
    let (queue_addr_from_api, token_from_api, tls_from_api) = match access_token {
        Some(access_token) => {
            let config = get_config_from_api(access_token, run_id)?;
            (
                Some(config.queue_addr),
                Some(config.token),
                config.tls_public_certificate,
            )
        }
        None => (None, None, None),
    };

    let token = token_from_api.or(token_from_cli);
    let queue_addr = queue_addr_from_api.or(queue_addr_from_cli);
    let tls_cert = tls_from_api.or(tls_cert_from_cli);

    Ok(ResolvedConfig {
        queue_addr,
        token,
        tls_cert,
    })
}

fn get_config_from_api(access_token: AccessToken, run_id: &RunId) -> anyhow::Result<ConfigFromApi> {
    use abq_hosted::{HostedQueueConfig, DEFAULT_RWX_ABQ_API_URL};

    let api_url = std::env::var("ABQ_API").unwrap_or_else(|_| DEFAULT_RWX_ABQ_API_URL.to_string());

    let HostedQueueConfig {
        addr,
        run_id: _,
        auth_token,
        tls_public_certificate,
    } = HostedQueueConfig::from_api(api_url, access_token, run_id)?;

    Ok(ConfigFromApi {
        queue_addr: addr,
        token: auth_token,
        tls_public_certificate,
    })
}

fn validate_abq_test_args(mut args: Vec<String>) -> Result<NativeTestRunnerParams, clap::Error> {
    use clap::{error::ErrorKind, CommandFactory};
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
    queue_addr_or_opt_tls: Result<SocketAddr, Option<Vec<u8>>>,
    opt_user_token: Option<UserToken>,
    client_auth: ClientAuthStrategy<User>,
    client_tls_cert: Option<Vec<u8>>,
) -> anyhow::Result<AbqInstance> {
    let client_tls = match &client_tls_cert {
        Some(cert) => ClientTlsStrategy::from_cert(cert)?,
        None => ClientTlsStrategy::no_tls(),
    };

    match queue_addr_or_opt_tls {
        Ok(queue_addr) => {
            let instance = AbqInstance::from_remote(entity, queue_addr, client_auth, client_tls)?;
            Ok(instance)
        }
        Err(opt_tls_key) => {
            let server_tls = match (client_tls_cert, opt_tls_key) {
                (Some(cert), Some(key)) => ServerTlsStrategy::from_cert(&cert, &key)?,
                (None, None) => ServerTlsStrategy::no_tls(),
                _ => unreachable!(
                    "any other configuration would have been caught during arg parsing"
                ),
            };

            Ok(AbqInstance::new_ephemeral(
                opt_user_token,
                client_auth,
                server_tls,
                client_tls,
            ))
        }
    }
}

fn run_tests(
    entity: EntityId,
    runner_params: NativeTestRunnerParams,
    abq: AbqInstance,
    run_id: RunId,
    reporters: Vec<ReporterKind>,
    color_choice: ColorPreference,
    batch_size: NonZeroU64,
    results_timeout: Duration,
    num_workers: NonZeroUsize,
    start_in_process_workers: bool,
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

    let runner = RunnerKind::GenericNativeTestRunner(runner_params.clone());

    let work_results_thread = start_test_result_reporter(
        entity,
        abq.server_addr(),
        abq.client_options().clone(),
        run_id.clone(),
        runner,
        batch_size,
        results_timeout,
        on_result,
    )?;

    let opt_workers = if start_in_process_workers {
        let working_dir = std::env::current_dir().expect("no working directory");
        let workers = workers::start_workers(
            num_workers,
            working_dir,
            abq.negotiator_handle(),
            abq.client_options().clone(),
            run_id,
        )?;
        Some(workers)
    } else {
        println!("Starting test run with ID {}", run_id);
        None
    };

    let opt_invoked_error = work_results_thread.join().unwrap();

    if let Some(mut workers) = opt_workers {
        // The exit code will be determined by the test result status.
        let _exit_code = workers.shutdown();
    }

    if let Err(invoke_error) = opt_invoked_error {
        return Err(elaborate_invocation_error(invoke_error, runner_params));
    }

    let (suite_result, errors) = reporters.finish();

    for error in errors {
        eprintln!("{error}");
    }

    print!("\n\n");
    suite_result.write_short_summary_lines(&mut io::stdout())?;

    Ok(suite_result.suggested_exit_code())
}

fn elaborate_invocation_error(
    error: InvocationError,
    runner_params: NativeTestRunnerParams,
) -> anyhow::Error {
    match error {
        InvocationError::Io(_)
        | InvocationError::DuplicateRun(_)
        | InvocationError::DuplicateCompletedRun(_) => {
            // The default error message provided is good here.
            error.into()
        }
        InvocationError::TestResultError(error) => match error {
            TestResultError::Io(error) => error.into(),
            TestResultError::TestCommandError(opaque_error) => {
                let NativeTestRunnerParams {
                    cmd,
                    args,
                    extra_env: _,
                } = runner_params;

                let mut cmd = vec![cmd];
                cmd.extend(args);
                let cmd = cmd.join(" ");

                let msg = format!(
                    indoc::indoc!(
                        r#"
                        The command

                            {}

                        failed to be run by all ABQ workers associating to this test run.

                        Here's a message we found concerning the failure:

                        {}

                        HELP: Test commands run by ABQ must have support for the ABQ protocol."#
                    ),
                    cmd, opaque_error,
                );
                anyhow::Error::msg(msg)
            }
            TestResultError::TimedOut(after) => {
                let mut s = Vec::new();
                format_duration(&mut s, TestRuntime::Milliseconds(after.as_millis() as _))
                    .expect("formatting duration to vec is infallible");
                let timeout_s = String::from_utf8_lossy(&s);

                let msg = format!(
                    indoc::indoc!(
                        r#"
                        The test run timed out after {}.

                        This likely indicates a problem in your test suite, or in an ABQ worker setup.
                        Please check the logs of your ABQ workers.
                        "#
                    ),
                    timeout_s
                );
                anyhow::Error::msg(msg)
            }
            TestResultError::Cancelled => anyhow::Error::msg("Test run cancelled!"),
        },
    }
}

/// Starts a test result reporter on a new thread.
/// This should not block the main thread, only return a handle to the reporter.
///
/// NOTE: this function takes control of the process's signal handlers! It may not be
/// composed with other functions that expect control or access to signal handlers.
fn start_test_result_reporter(
    entity: EntityId,
    abq_server_addr: SocketAddr,
    client_opts: ClientOptions<User>,
    test_id: RunId,
    runner: RunnerKind,
    batch_size: NonZeroU64,
    results_timeout: Duration,
    on_result: impl FnMut(WorkId, TestResult) + Send + 'static,
) -> io::Result<JoinHandle<Result<CompletedSummary, InvocationError>>> {
    let (run_cancellation_tx, run_cancellation_rx) = invoke::run_cancellation_pair();

    let mut term_signals = Signals::new(TERM_SIGNALS)?;
    let term_signals_handle = term_signals.handle();
    let kill_run_thread = thread::spawn(move || {
        if term_signals.into_iter().next().is_some() {
            // If this fails, we definitely want a panic.
            run_cancellation_tx.blocking_send().unwrap();
        }
    });

    let test_results_thread = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let result = runtime.block_on(async move {
            let abq_test_client = Client::invoke_work(
                entity,
                abq_server_addr,
                client_opts,
                test_id,
                runner,
                batch_size,
                results_timeout,
                run_cancellation_rx,
            )
            .await?;

            let completed_summary = abq_test_client.stream_results(on_result).await?;
            Ok(completed_summary)
        });

        // No matter how we exited, now close the thread responsible for capturing termination
        // signals.
        term_signals_handle.close();
        kill_run_thread.join().unwrap();

        result
    });

    Ok(test_results_thread)
}

#[cfg(test)]
mod test {
    use std::{env::VarError, str::FromStr};

    use abq_utils::net_protocol::workers::{NativeTestRunnerParams, RunId};
    use clap::error::ErrorKind;

    use super::{get_inferred_run_id, validate_abq_test_args, RunIdEnvironment};

    impl Default for RunIdEnvironment {
        fn default() -> RunIdEnvironment {
            RunIdEnvironment {
                abq_run_id: Err(VarError::NotPresent),
                ci: Err(VarError::NotPresent),
                buildkite_build_id: Err(VarError::NotPresent),
                circle_workflow_id: Err(VarError::NotPresent),
                github_run_id: Err(VarError::NotPresent),
                github_run_attempt: Err(VarError::NotPresent),
            }
        }
    }

    #[test]
    fn validate_test_args_empty() {
        let result = validate_abq_test_args(vec![]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidValue);
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

    #[test]
    fn get_inferred_run_id_github_actions() {
        let run_id = get_inferred_run_id(RunIdEnvironment {
            ci: Ok(String::from("true")),
            github_run_id: Ok(String::from("github-id")),
            github_run_attempt: Ok(String::from("2")),
            ..Default::default()
        });

        assert_eq!(run_id.unwrap(), RunId::from_str("github-id-2").unwrap());
    }

    #[test]
    fn get_inferred_run_id_github_actions_no_attempt_envvar() {
        let run_id = get_inferred_run_id(RunIdEnvironment {
            ci: Ok(String::from("true")),
            github_run_id: Ok(String::from("github-id")),
            ..Default::default()
        });

        assert_eq!(run_id.unwrap(), RunId::from_str("github-id-1").unwrap());
    }

    #[test]
    fn get_inferred_run_id_circleci() {
        let run_id = get_inferred_run_id(RunIdEnvironment {
            ci: Ok(String::from("true")),
            circle_workflow_id: Ok(String::from("circleci-id")),
            ..Default::default()
        });

        assert_eq!(run_id.unwrap(), RunId::from_str("circleci-id").unwrap());
    }

    #[test]
    fn get_inferred_run_id_buildkite() {
        let run_id = get_inferred_run_id(RunIdEnvironment {
            ci: Ok(String::from("true")),
            buildkite_build_id: Ok(String::from("buildkite-id")),
            ..Default::default()
        });

        assert_eq!(run_id.unwrap(), RunId::from_str("buildkite-id").unwrap());
    }

    #[test]
    fn get_inferred_run_id_ci_false() {
        let run_id = get_inferred_run_id(RunIdEnvironment {
            ci: Ok(String::from("false")),
            buildkite_build_id: Ok(String::from("buildkite-id")),
            ..Default::default()
        });

        assert_eq!(run_id, None);
    }

    #[test]
    fn get_inferred_run_id_ci_absent() {
        let run_id = get_inferred_run_id(RunIdEnvironment {
            buildkite_build_id: Ok(String::from("buildkite-id")),
            ..Default::default()
        });

        assert_eq!(run_id, None);
    }

    #[test]
    fn get_inferred_run_id_already_set() {
        let run_id = get_inferred_run_id(RunIdEnvironment {
            abq_run_id: Ok(String::from("abq-id")),
            ci: Ok(String::from("true")),
            buildkite_build_id: Ok(String::from("buildkite-id")),
            ..Default::default()
        });

        assert_eq!(run_id, None);
    }
}
