mod abq_config;
mod args;
mod health;
mod instance;
mod report;
mod reporting;
mod statefile;
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
    NumWorkers::{CpuCores, Fixed},
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

struct ConfigFromApi {
    queue_addr: SocketAddr,
    token: UserToken,
    tls_public_certificate: Option<Vec<u8>>,
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
        Command::Login {} => {
            let mut input = String::new();
            println!("Generate a Personal Access Token at https://account.rwx.com/_/personal_access_tokens");
            println!("\n\n");
            println!("Enter your RWX Personal Access Token:");

            io::stdin()
                .read_line(&mut input)
                .expect("Failed to read line");

            input = input.trim().to_string();

            let new_config = abq_config::AbqConfig {
                rwx_access_token: AccessToken::from_str(&input)?,
            };
            let abq_filepath = abq_config::write_abq_config(new_config)?;

            println!(
                "Your access token is now stored at {}",
                abq_filepath.display()
            );
            Ok(ExitCode::SUCCESS)
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
                let config = abq_config::read_abq_config().ok()?;
                Some(config.rwx_access_token)
            });

            let ResolvedConfig {
                token: resolved_token,
                tls_cert: resolved_tls,
                rwx_access_token_kind,
                queue_location,
            } = resolve_config(token, queue_addr, tls_cert, tls_key, &access_token, &run_id)
                .await?;

            if explicit_run_id_provided && !queue_location.is_remote() {
                let mut cmd = Cli::command();
                Err(cmd.error(
                    ErrorKind::MissingRequiredArgument,
                    "`abq test` was not given an access token and could not infer one. Consider passing `--access-token`, setting `RWX_ACCESS_TOKEN`, or running `abq login`.",
                ))?;
            }

            let client_auth = resolved_token.into();

            let runner_params = validate_abq_test_args(args)?;

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

            statefile::optional_write_worker_statefile(&run_id)?;

            let runner = RunnerKind::GenericNativeTestRunner(runner_params);

            let max_run_number = 1 + retries;

            let startup_timeout = Duration::from_secs(startup_timeout_seconds);

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
                tests_timeout,
                abq.negotiator_handle(),
                abq.client_options().clone(),
                startup_timeout,
                execution_mode,
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

            let ResolvedConfig {
                token: resolved_token,
                tls_cert: resolved_tls,
                rwx_access_token_kind: _resolved_rwx_access_token_kind,
                queue_location,
            } = resolve_config(token, queue_addr, tls_cert, None, &access_token, &run_id).await?;

            let client_auth = resolved_token.into();

            if explicit_run_id_provided && !queue_location.is_remote() {
                let mut cmd = Cli::command();
                Err(cmd.error(
                    ErrorKind::MissingRequiredArgument,
                    "`abq report` was not given an access token and could not infer one. Consider passing `--access-token`, setting `RWX_ACCESS_TOKEN`, or running `abq login`.",
                ))?;
            }

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

enum QueueLocation {
    Remote(SocketAddr),
    Ephemeral { opt_tls_key: Option<Vec<u8>> },
}

impl QueueLocation {
    fn is_remote(&self) -> bool {
        match self {
            QueueLocation::Remote(_) => true,
            QueueLocation::Ephemeral { .. } => false,
        }
    }
}

async fn resolve_config(
    token_from_cli: Option<UserToken>,
    queue_addr_from_cli: Option<SocketAddr>,
    tls_cert_from_cli: Option<Vec<u8>>,
    tls_key: Option<Vec<u8>>,
    access_token: &Option<AccessToken>,
    run_id: &RunId,
) -> anyhow::Result<ResolvedConfig> {
    let (queue_addr_from_api, token_from_api, tls_from_api, rwx_access_token_kind) =
        match access_token.as_ref() {
            Some(access_token) => {
                let config = get_config_from_api(access_token, run_id).await?;
                (
                    Some(config.queue_addr),
                    Some(config.token),
                    config.tls_public_certificate,
                    Some(config.rwx_access_token_kind),
                )
            }
            None => (None, None, None, None),
        };

    let token = token_from_api.or(token_from_cli);
    let queue_addr = queue_addr_from_api.or(queue_addr_from_cli);
    let tls_cert = tls_from_api.or(tls_cert_from_cli);
    let queue_location = match queue_addr {
        Some(queue_addr) => QueueLocation::Remote(queue_addr),
        None => QueueLocation::Ephemeral {
            opt_tls_key: tls_key,
        },
    };

    Ok(ResolvedConfig {
        token,
        tls_cert,
        rwx_access_token_kind,
        queue_location,
    })
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

    let HostedQueueConfig {
        addr,
        run_id: _,
        auth_token,
        tls_public_certificate,
        rwx_access_token_kind,
    } = HostedQueueConfig::from_api(api_url, access_token, run_id).await?;

    Ok(ConfigFromApi {
        queue_addr: addr,
        token: auth_token,
        tls_public_certificate,
        rwx_access_token_kind,
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
        extra_env: HashMap::from([
            // TODO: This is a hack to get chalk.js to add color codes
            // We probably should instead supply a PTY to the child process
            (String::from("FORCE_COLOR"), String::from("1")),
        ]),
    })
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
    }
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
}
