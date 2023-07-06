mod abq_config;
mod args;
mod health;
mod instance;
mod report;
mod reporting;
mod workers;

use std::io;
use std::str::FromStr;
use std::{
    collections::HashMap, net::SocketAddr, num::NonZeroUsize, path::PathBuf, time::Duration,
};

use abq_hosted::AccessToken;
use abq_hosted::AccessTokenKind;
use abq_utils::{
    auth::{ClientAuthStrategy, ServerAuthStrategy, User, UserToken},
    exit::ExitCode,
    net_opt::{ClientOptions, ServerOptions},
    net_protocol::{
        entity::{Entity, WorkerTag},
        health::Health,
        meta::DeprecationRecord,
        workers::{NativeTestRunnerParams, RunId, RunnerKind},
    },
    tls::{ClientTlsStrategy, ServerTlsStrategy},
};

use args::{
    Cli, Command,
    NumRunners::{CpuCores, Fixed},
    Report,
};
use clap::Parser;

use instance::AbqInstance;
use tracing::{metadata::LevelFilter, Subscriber};
use tracing_subscriber::{fmt, prelude::*, registry, EnvFilter, Registry};
use workers::ExecutionMode;

use crate::{
    args::Token,
    health::HealthCheckKind,
    instance::{
        local_persistence::LocalPersistenceConfig,
        remote_persistence::{OffloadToRemoteConfig, RemotePersistenceConfig},
    },
    reporting::StdoutPreferences,
    workers::TestRunMetadata,
};

fn main() -> anyhow::Result<()> {
    let exit_code = abq_main()?;
    std::process::exit(exit_code.get());
}

#[must_use]
struct TracingGuards {
    _file_appender_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

struct PrefixedCiEventFormat<T: fmt::time::FormatTime> {
    format: fmt::format::Format<fmt::format::Full, T>,
    prefix: String,
}

enum ConfigFromApi {
    Success(SuccessConfigFromApi),
    Unsupported(UnsupportedConfigFromApi),
}

struct SuccessConfigFromApi {
    queue_addr: SocketAddr,
    token: UserToken,
    tls_public_certificate: Option<Vec<u8>>,
    rwx_access_token_kind: AccessTokenKind,
}
struct UnsupportedConfigFromApi {
    usage_error: String,
    rwx_access_token_kind: AccessTokenKind,
}

struct RunIdEnvironment {
    abq_run_id: Result<String, std::env::VarError>,
    ci: Result<String, std::env::VarError>,
    buildkite_build_id: Result<String, std::env::VarError>,
    circle_workflow_id: Result<String, std::env::VarError>,
    github_run_id: Result<String, std::env::VarError>,
}

impl RunIdEnvironment {
    fn from_env() -> Self {
        Self {
            abq_run_id: std::env::var("ABQ_RUN_ID"),
            ci: std::env::var("CI"),
            buildkite_build_id: std::env::var("BUILDKITE_BUILD_ID"),
            circle_workflow_id: std::env::var("CIRCLE_WORKFLOW_ID"),
            github_run_id: std::env::var("GITHUB_RUN_ID"),
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

fn setup_tracing() -> anyhow::Result<TracingGuards> {
    // Trace to standard error with ABQ_LOG set. If unset, trace and log nothing.
    let env_filter = EnvFilter::builder()
        .with_env_var("ABQ_LOG")
        .with_default_directive(LevelFilter::OFF.into())
        .from_env()?;

    let guards = if let Ok(dir) = std::env::var("ABQ_LOGTO") {
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
    } else if std::env::var("ABQ_LOG_JSON").as_deref() == Ok("1") {
        // Log JSON to stdout
        let stdout_layer = fmt::Layer::default()
            .json()
            .with_writer(std::io::stdout)
            .with_span_events(fmt::format::FmtSpan::ACTIVE)
            .with_filter(env_filter);

        Registry::default().with(stdout_layer).init();

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
    };

    Ok(guards)
}

fn get_inferred_run_id(run_id_environment: RunIdEnvironment) -> Option<RunId> {
    let RunIdEnvironment {
        abq_run_id,
        ci,
        buildkite_build_id,
        circle_workflow_id,
        github_run_id,
    } = run_id_environment;

    if abq_run_id.is_ok() || ci.unwrap_or_else(|_| String::from("false")) == *"false" {
        return None;
    }
    // note: if you change this, change it in setup-abq, too
    // https://github.com/rwx-research/setup-abq/blob/8c5044343fceef53b4efea83d64062f006bf6758/src/index.ts#L23-L28
    let run_id_result = buildkite_build_id.or(circle_workflow_id).or(github_run_id);
    run_id_result.ok().map(RunId)
}

#[tokio::main(flavor = "multi_thread")]
async fn abq_main() -> anyhow::Result<ExitCode> {
    use clap::{error::ErrorKind, CommandFactory};

    let _tracing_guards = setup_tracing()?;

    let inferred_run_id = get_inferred_run_id(RunIdEnvironment::from_env());

    let Cli { command } = Cli::parse();

    match command {
        Command::Login { access_token } => {
            let abq_config = match access_token {
                Some(token) => abq_config::AbqConfig {
                    rwx_access_token: token,
                },
                None => {
                    let mut input = String::new();
                    println!("Generate a Personal Access Token at https://account.rwx.com/_/personal_access_tokens");
                    println!("\n");
                    println!("Enter your RWX Personal Access Token:");

                    io::stdin()
                        .read_line(&mut input)
                        .expect("Failed to read line");

                    abq_config::AbqConfig {
                        rwx_access_token: AccessToken::from_str(input.trim())?,
                    }
                }
            };

            if let Some(config_path) = get_abq_config_filepath() {
                abq_config::write_abq_config(abq_config, Ok(config_path.clone()))?;
                println!("\n");
                println!(
                    "Your access token is now stored at: {}",
                    config_path.display()
                );
                return Ok(ExitCode::SUCCESS);
            }

            let mut cmd = Cli::command();
            Err(cmd.error(ErrorKind::InvalidValue, "Failed to locate ABQ config file."))?
        }
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
            persisted_results_dir,
            persisted_manifests_dir,
            remote_persistence_strategy,
            remote_persistence_command,
            remote_persistence_s3_bucket,
            remote_persistence_s3_key_prefix,
            offload_manifests_cron,
            offload_results_cron,
            offload_stale_file_threshold_hours,
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

            let local_persistence_config =
                LocalPersistenceConfig::new(persisted_manifests_dir, persisted_results_dir);

            let remote_persistence_config = RemotePersistenceConfig::new(
                remote_persistence_strategy,
                remote_persistence_command,
                remote_persistence_s3_bucket,
                remote_persistence_s3_key_prefix,
            );

            let offload_to_remote_config = {
                let offload_state_file_threshold_seconds =
                    (offload_stale_file_threshold_hours as u64) * 60 * 60;

                OffloadToRemoteConfig {
                    offload_manifests_cron,
                    offload_results_cron,
                    stale_duration: Duration::from_secs(offload_state_file_threshold_seconds),
                }
            };

            let code = instance::start_abq_forever(
                public_ip,
                bind_ip,
                server_port,
                work_port,
                negotiator_port,
                ServerOptions::new(server_auth, server_tls),
                local_persistence_config,
                remote_persistence_config,
                offload_to_remote_config,
            )
            .await?;
            Ok(code)
        }
        Command::Test {
            worker,
            retries,
            args,
            working_dir,
            run_id,
            access_token,
            queue_addr,
            num,
            reporter: reporters,
            token,
            tls_cert,
            tls_key,
            color,
            batch_size,
            startup_timeout_seconds,
            test_strategy,
            inactivity_timeout_seconds,
        } => {
            let deprecations = DeprecationRecord::default();
            let stdout_preferences = StdoutPreferences::new(color);

            let external_run_id = run_id.or(inferred_run_id);
            let explicit_run_id_provided = external_run_id.is_some();
            let run_id = external_run_id.unwrap_or_else(RunId::unique);

            let working_dir =
                working_dir.unwrap_or_else(|| std::env::current_dir().expect("no current dir"));

            let tls_cert = read_opt_path_bytes(tls_cert)?;
            let tls_key = read_opt_path_bytes(tls_key)?;

            let access_token = access_token.or_else(|| {
                let config = abq_config::read_abq_config(get_abq_config_filepath())?;
                Some(config.rwx_access_token)
            });

            let ResolvedConfig {
                token: resolved_token,
                tls_cert: resolved_tls,
                rwx_access_token_kind,
                queue_location,
            } = resolve_config(
                token,
                queue_addr,
                tls_cert,
                tls_key,
                &access_token,
                &run_id,
                explicit_run_id_provided,
            )
            .await?;

            validate_queue_location("test", &queue_location, explicit_run_id_provided)?;

            let client_auth = resolved_token.into();

            let runner_params = validate_abq_test_args(args)?;

            let entity = Entity::local_client();
            let abq = find_or_create_abq(
                entity,
                run_id.clone(),
                queue_location.clone(),
                resolved_token,
                client_auth,
                resolved_tls,
                deprecations,
            )
            .await?;
            let tests_timeout = Duration::from_secs(inactivity_timeout_seconds);

            let num_runners = match num {
                CpuCores => {
                    NonZeroUsize::new(std::cmp::Ord::max(num_cpus::get_physical() - 1, 1)).unwrap()
                }
                Fixed(num) => num,
            };

            let execution_mode = match rwx_access_token_kind.as_ref() {
                Some(AccessTokenKind::Personal) => ExecutionMode::Readonly,
                _ => ExecutionMode::WriteNormal,
            };

            let runner = RunnerKind::GenericNativeTestRunner(runner_params);

            let max_run_number = 1 + retries;

            let startup_timeout = Duration::from_secs(startup_timeout_seconds);

            let test_run_metadata = TestRunMetadata {
                api_url: get_hosted_api_base_url(),
                access_token,
                run_id: run_id.clone(),
                record_telemetry: queue_location.is_remote(),
            };

            workers::start_workers_standalone(
                run_id,
                WorkerTag::new(worker as _),
                num_runners,
                max_run_number,
                runner,
                working_dir,
                reporters,
                stdout_preferences,
                batch_size,
                test_strategy,
                tests_timeout,
                abq.negotiator_handle(),
                abq.client_options().clone(),
                startup_timeout,
                execution_mode,
                test_run_metadata,
            )
            .await
        }
        Command::Report {
            run_id,
            reporter,
            color,
            access_token,
            timeout_seconds,
            queue_addr,
            token,
            tls_cert,
            subcommand: None,
        } => {
            let deprecations = DeprecationRecord::default();
            let stdout_preferences = StdoutPreferences::new(color);

            let explicit_run_id_provided = run_id.is_some();
            let run_id = run_id.or(inferred_run_id).ok_or_else (|| {
                let mut cmd = Cli::command();
                cmd.error(
                    ErrorKind::InvalidValue,
                    "`abq report` was not given a run-id and could not infer one. Consider setting `--run-id` or the `ABQ_RUN_ID` environment variable.",
                )
            })?;

            let tls_cert = read_opt_path_bytes(tls_cert)?;

            let access_token = access_token.or_else(|| {
                let config = abq_config::read_abq_config(get_abq_config_filepath())?;
                Some(config.rwx_access_token)
            });

            let ResolvedConfig {
                token: resolved_token,
                tls_cert: resolved_tls,
                rwx_access_token_kind: _resolved_rwx_access_token_kind,
                queue_location,
            } = resolve_config(
                token,
                queue_addr,
                tls_cert,
                None,
                &access_token,
                &run_id,
                explicit_run_id_provided,
            )
            .await?;

            let client_auth = resolved_token.into();

            validate_queue_location("report", &queue_location, explicit_run_id_provided)?;

            let entity = Entity::local_client();
            let abq = find_or_create_abq(
                entity,
                run_id.clone(),
                queue_location,
                resolved_token,
                client_auth,
                resolved_tls,
                deprecations,
            )
            .await?;

            let code = report::report_results(
                abq,
                entity,
                run_id,
                reporter,
                stdout_preferences,
                Duration::from_secs(timeout_seconds),
            )
            .await?;
            Ok(code)
        }
        Command::Report {
            run_id,
            reporter,
            color,
            access_token,
            timeout_seconds,
            queue_addr,
            token,
            tls_cert,
            subcommand: Some(listTests),
        } => {
            println!("todo: print the tests!");

            Ok(ExitCode::new(1))
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
            Ok(ExitCode::new(exit))
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
    token: Option<UserToken>,
    tls_cert: Option<Vec<u8>>,
    rwx_access_token_kind: Option<AccessTokenKind>,
    queue_location: QueueLocation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueueLocation {
    Remote(SocketAddr),
    Ephemeral { opt_tls_key: Option<Vec<u8>> },
    Unsupported(String),
}

impl QueueLocation {
    fn is_remote(&self) -> bool {
        matches!(self, QueueLocation::Remote(_))
    }
}

struct QueueLocationConfig {
    access_token_kind: Option<AccessTokenKind>,
    run_id_provided: bool,
    queue_addr: Option<SocketAddr>,
    usage_error_from_api: Option<String>,
    tls_key: Option<Vec<u8>>,
}

fn determine_queue_location(config: QueueLocationConfig) -> QueueLocation {
    match config.access_token_kind {
        Some(AccessTokenKind::Personal) => {
            match (
                config.run_id_provided,
                config.usage_error_from_api,
                config.queue_addr,
            ) {
                (false, _, _) => QueueLocation::Ephemeral {
                    opt_tls_key: config.tls_key,
                },
                (_, Some(usage_error), _) => QueueLocation::Unsupported(usage_error),
                (_, None, Some(queue_addr)) => QueueLocation::Remote(queue_addr),
                (_, None, None) => QueueLocation::Ephemeral {
                    opt_tls_key: config.tls_key,
                },
            }
        }
        _ => match config.queue_addr {
            Some(queue_addr) => QueueLocation::Remote(queue_addr),
            None => QueueLocation::Ephemeral {
                opt_tls_key: config.tls_key,
            },
        },
    }
}

async fn resolve_config(
    token_from_cli: Option<UserToken>,
    queue_addr_from_cli: Option<SocketAddr>,
    tls_cert_from_cli: Option<Vec<u8>>,
    tls_key: Option<Vec<u8>>,
    access_token: &Option<AccessToken>,
    run_id: &RunId,
    explicit_run_id_provided: bool,
) -> anyhow::Result<ResolvedConfig> {
    let (
        queue_addr_from_api,
        token_from_api,
        tls_from_api,
        usage_error_from_api,
        rwx_access_token_kind,
    ) = match access_token.as_ref() {
        Some(access_token) => match get_config_from_api(access_token, run_id).await? {
            ConfigFromApi::Success(config) => (
                Some(config.queue_addr),
                Some(config.token),
                config.tls_public_certificate,
                None,
                Some(config.rwx_access_token_kind),
            ),
            ConfigFromApi::Unsupported(config) => (
                None,
                None,
                None,
                Some(config.usage_error),
                Some(config.rwx_access_token_kind),
            ),
        },
        None => (None, None, None, None, None),
    };

    let token = token_from_api.or(token_from_cli);
    let queue_addr = queue_addr_from_api.or(queue_addr_from_cli);
    let tls_cert = tls_from_api.or(tls_cert_from_cli);

    let queue_location_config = QueueLocationConfig {
        access_token_kind: rwx_access_token_kind,
        run_id_provided: explicit_run_id_provided,
        queue_addr,
        usage_error_from_api,
        tls_key,
    };
    let queue_location = determine_queue_location(queue_location_config);

    Ok(ResolvedConfig {
        token,
        tls_cert,
        rwx_access_token_kind,
        queue_location,
    })
}

fn get_abq_config_filepath() -> Option<PathBuf> {
    let config_file_mode = match std::env::var("ABQ_CONFIG_FILE") {
        Ok(path) => {
            if path.is_empty() {
                abq_config::AbqConfigFileMode::Ignore
            } else {
                abq_config::AbqConfigFileMode::Override(path)
            }
        }
        Err(_) => abq_config::AbqConfigFileMode::Conventional,
    };
    abq_config::abq_config_filepath(config_file_mode)
}

fn get_hosted_api_base_url() -> String {
    std::env::var("ABQ_API").unwrap_or_else(|_| abq_hosted::DEFAULT_RWX_ABQ_API_URL.to_string())
}

async fn get_config_from_api(
    access_token: &AccessToken,
    run_id: &RunId,
) -> anyhow::Result<ConfigFromApi> {
    use abq_hosted::HostedQueueConfig;

    let api_url = get_hosted_api_base_url();
    let hosted_queue_config = HostedQueueConfig::from_api(api_url, access_token, run_id).await?;

    match hosted_queue_config {
        HostedQueueConfig::Success(config) => Ok(ConfigFromApi::Success(SuccessConfigFromApi {
            queue_addr: config.addr,
            token: config.auth_token,
            tls_public_certificate: config.tls_public_certificate,
            rwx_access_token_kind: config.rwx_access_token_kind,
        })),
        HostedQueueConfig::Unsupported(config) => {
            Ok(ConfigFromApi::Unsupported(UnsupportedConfigFromApi {
                usage_error: config.usage_error,
                rwx_access_token_kind: config.rwx_access_token_kind,
            }))
        }
    }
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
        extra_env: HashMap::from([
            // TODO: This is a hack to get chalk.js to add color codes
            // We probably should instead supply a PTY to the child process
            (String::from("FORCE_COLOR"), String::from("1")),
        ]),
    })
}

fn validate_queue_location(
    command: &str,
    queue_location: &QueueLocation,
    explicit_run_id_provided: bool,
) -> Result<(), clap::Error> {
    use clap::{error::ErrorKind, CommandFactory};
    if let QueueLocation::Unsupported(error_message) = &queue_location {
        let mut cmd = Cli::command();
        Err(cmd.error(
            ErrorKind::InvalidValue,
            format!(
                "ABQ was unable to find a queue to run against. {}",
                error_message
            ),
        ))?;
    }

    if explicit_run_id_provided && !queue_location.is_remote() {
        let mut cmd = Cli::command();
        Err(cmd.error(
            ErrorKind::InvalidValue,
            indoc::formatdoc!("
            `abq {}` was provided a run id, but we've detected an ephemeral queue.

            If you intended to run against a remote queue, please provide an access token by passing `--access-token`, setting `RWX_ACCESS_TOKEN`, or running `abq login`.
            If you intended to run against an ephemeral queue, please remove the run id argument.
            ", command)
        ))?;
    }

    Result::Ok(())
}

async fn find_or_create_abq(
    entity: Entity,
    run_id: RunId,
    queue_location: QueueLocation,
    opt_user_token: Option<UserToken>,
    client_auth: ClientAuthStrategy<User>,
    client_tls_cert: Option<Vec<u8>>,
    deprecations: DeprecationRecord,
) -> anyhow::Result<AbqInstance> {
    let client_tls = match &client_tls_cert {
        Some(cert) => ClientTlsStrategy::from_cert(cert)?,
        None => ClientTlsStrategy::no_tls(),
    };

    match queue_location {
        QueueLocation::Remote(queue_addr) => {
            let instance = AbqInstance::from_remote(
                entity,
                run_id,
                queue_addr,
                client_auth,
                client_tls,
                deprecations,
            )?;
            Ok(instance)
        }
        QueueLocation::Ephemeral { opt_tls_key } => {
            let server_tls = match (client_tls_cert, opt_tls_key) {
                (Some(cert), Some(key)) => ServerTlsStrategy::from_cert(&cert, &key)?,
                (None, None) => ServerTlsStrategy::no_tls(),
                _ => unreachable!(
                    "any other configuration would have been caught during arg parsing"
                ),
            };
            Ok(
                AbqInstance::new_ephemeral(opt_user_token, client_auth, server_tls, client_tls)
                    .await,
            )
        }
        QueueLocation::Unsupported(_) => {
            unreachable!("any other configuration would have been caught during arg parsing")
        }
    }
}

#[cfg(test)]
mod test {
    use std::{env::VarError, str::FromStr};

    use abq_hosted::AccessTokenKind;
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
            ..Default::default()
        });

        assert_eq!(run_id.unwrap(), RunId::from_str("github-id").unwrap());
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

    #[test]
    fn determine_queue_location_pat_no_run_id_provided_uses_ephemeral() {
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: Some(AccessTokenKind::Personal),
            run_id_provided: false,
            queue_addr: None,
            usage_error_from_api: None,
            tls_key: None,
        });

        assert_eq!(
            queue_location,
            super::QueueLocation::Ephemeral { opt_tls_key: None }
        );
    }

    #[test]
    fn determine_queue_location_pat_nonexistent_run_id_unsupported() {
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: Some(AccessTokenKind::Personal),
            run_id_provided: true,
            queue_addr: None,
            usage_error_from_api: Some("nonexistent-run-id".to_string()),
            tls_key: None,
        });

        assert_eq!(
            queue_location,
            super::QueueLocation::Unsupported("nonexistent-run-id".to_string())
        );
    }

    #[test]
    fn determine_queue_location_pat_existing_run_id_remote() {
        let queue_addr = std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        );
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: Some(AccessTokenKind::Personal),
            run_id_provided: true,
            queue_addr: Some(queue_addr),
            usage_error_from_api: None,
            tls_key: None,
        });

        assert_eq!(queue_location, super::QueueLocation::Remote(queue_addr))
    }

    #[test]
    fn determine_queue_location_pat_existing_run_id_no_queue_addr_ephemeral() {
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: Some(AccessTokenKind::Personal),
            run_id_provided: true,
            queue_addr: None,
            usage_error_from_api: None,
            tls_key: None,
        });

        assert_eq!(
            queue_location,
            super::QueueLocation::Ephemeral { opt_tls_key: None }
        );
    }

    #[test]
    fn determine_queue_location_org_existing_queue_addr_remote() {
        let queue_addr = std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        );
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: Some(AccessTokenKind::Organization),
            run_id_provided: true,
            queue_addr: Some(queue_addr),
            usage_error_from_api: None,
            tls_key: None,
        });

        assert_eq!(queue_location, super::QueueLocation::Remote(queue_addr))
    }

    #[test]
    fn determine_queue_location_org_no_queue_addr_ephemeral() {
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: Some(AccessTokenKind::Organization),
            run_id_provided: true,
            queue_addr: None,
            usage_error_from_api: None,
            tls_key: None,
        });

        assert_eq!(
            queue_location,
            super::QueueLocation::Ephemeral { opt_tls_key: None }
        );
    }

    #[test]
    fn determine_queue_location_noauthtoken_existing_no_queue_addr_ephemeral() {
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: None,
            run_id_provided: true,
            queue_addr: None,
            usage_error_from_api: None,
            tls_key: None,
        });

        assert_eq!(
            queue_location,
            super::QueueLocation::Ephemeral { opt_tls_key: None }
        );
    }

    #[test]
    fn determine_queue_location_noauthtoken_existing_queue_addr_remote() {
        let queue_addr = std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        );
        let queue_location = super::determine_queue_location(super::QueueLocationConfig {
            access_token_kind: None,
            run_id_provided: true,
            queue_addr: Some(queue_addr),
            usage_error_from_api: None,
            tls_key: None,
        });

        assert_eq!(queue_location, super::QueueLocation::Remote(queue_addr))
    }
}
