use abq_queue::persistence;
use abq_queue::persistence::remote::NoopPersister;
use abq_queue::queue::{Abq, QueueConfig};
use abq_queue::RunTimeoutStrategy;
use abq_utils::auth::{AdminToken, ServerAuthStrategy, UserToken};
use abq_utils::exit::ExitCode;
use abq_utils::net_opt::ServerOptions;
use abq_utils::net_protocol::entity::Entity;
use abq_utils::net_protocol::meta::DeprecationRecord;
use abq_utils::net_protocol::publicize_addr;
use abq_utils::net_protocol::workers::RunId;
use abq_utils::tls::{ClientTlsStrategy, ServerTlsStrategy};
use abq_workers::negotiate::{QueueNegotiatorHandle, QueueNegotiatorHandleError};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::iterator::Signals;
use std::net::{IpAddr, SocketAddr};
use std::thread;
use tempfile::TempDir;

use thiserror::Error;
use tokio::select;

use self::remote_persistence::RemotePersistenceConfig;

pub mod remote_persistence;

type ClientOptions = abq_utils::net_opt::ClientOptions<abq_utils::auth::User>;
type ClientAuthStrategy = abq_utils::auth::ClientAuthStrategy<abq_utils::auth::User>;

/// Max number of file descriptors to keep open in the results-persistence LRU cache at any given
/// time.
const RESULTS_PERSISTENCE_LRU_CAPACITY: usize = 25;

/// Starts an [Abq] instance in the current process forever.
pub async fn start_abq_forever(
    public_ip: Option<IpAddr>,
    bind_ip: IpAddr,
    server_port: u16,
    work_port: u16,
    negotiator_port: u16,
    server_options: ServerOptions,
    remote_persistence_config: RemotePersistenceConfig,
) -> Result<ExitCode, clap::Error> {
    // Public IP defaults to the binding IP.
    let public_ip = public_ip.unwrap_or(bind_ip);

    let remote_persistence = remote_persistence_config.resolve().await?;

    let manifests_path = tempfile::tempdir().expect("unable to create a temporary file");
    let persist_manifest = persistence::manifest::FilesystemPersistor::new_shared(
        manifests_path.path(),
        remote_persistence,
    );

    let results_path = tempfile::tempdir().expect("unable to create a temporary file");
    let persist_results = persistence::results::FilesystemPersistor::new_shared(
        results_path.path(),
        RESULTS_PERSISTENCE_LRU_CAPACITY,
    );

    let run_timeout_strategy = RunTimeoutStrategy::RUN_BASED;

    let queue_config = QueueConfig {
        public_ip,
        bind_ip,
        server_port,
        work_port,
        negotiator_port,
        server_options,
        persist_manifest,
        persist_results,
        run_timeout_strategy,
    };
    let mut abq = Abq::start(queue_config).await;

    tracing::debug!("Queue active at {}", abq.server_addr());

    println!(
        "Persisting manifests at {}",
        manifests_path.path().display()
    );
    println!("Persisting results at {}", results_path.path().display());

    println!("Run the following to invoke a test run:");
    println!(
        "\tabq test --queue-addr={} --run-id <a-unique-run-id> -- <your test args here>",
        publicize_addr(abq.server_addr(), public_ip),
    );

    // Register signal handlers, so we know to shutdown (or kill) the queue if
    // we get a termination signal.
    let (stop_queue_tx, mut stop_queue_rx) = tokio::sync::mpsc::channel(2);
    let mut term_signals = Signals::new(TERM_SIGNALS).unwrap();
    let term_signals_handle = term_signals.handle();
    let listen_for_signals_thread = thread::spawn(move || {
        if term_signals.into_iter().next().is_some() {
            // If this fails, we definitely want a panic.
            stop_queue_tx.blocking_send(()).unwrap();
        }
    });

    // Make sure the queue retires on one term signal, and dies on two term signals.
    let mut received_shutdown = false;

    loop {
        select! {
            _ = stop_queue_rx.recv() => {
                if received_shutdown {
                    assert!(abq.is_retired());
                    tracing::debug!("second shutdown signal; killing queue");
                    // If we already received a shutdown signal, immediately
                    // exit and kill the queue.
                    break;
                }
                // Otherwise, move the queue into retirement and wake only
                // to check if it's fully been drained.
                received_shutdown = true;
                abq.retire();
                tracing::debug!("first shutdown signal; retiring queue");
            }
        }
    }

    tracing::debug!("shutting down queue");
    term_signals_handle.close();
    listen_for_signals_thread.join().unwrap();

    tracing::debug!("shut down queue");
    abq.shutdown().await.unwrap();

    Ok(ExitCode::SUCCESS)
}

pub(crate) struct AbqInstance {
    locator: AbqLocator,
    client_options: ClientOptions,
}

enum AbqLocator {
    Remote {
        queue_negotiator: QueueNegotiatorHandle,
        server_addr: SocketAddr,
    },
    Local(Abq, EphemeralAbqGuards),
}

struct EphemeralAbqGuards {
    _manifests_path: TempDir,
    _results_path: TempDir,
}

#[derive(Debug, Error)]
pub(crate) enum AbqInstanceError {
    QueueNegotiatorHandleError(#[from] QueueNegotiatorHandleError),
}

impl std::fmt::Display for AbqInstanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use QueueNegotiatorHandleError::*;
        match self {
            Self::QueueNegotiatorHandleError(e @ IncompatibleVersion { .. }) => {
                writeln!(f, "{e}.")?;
                writeln!(f, "If you are using `setup-abq`, please ensure that `abq` is invoked with the same run-id as `setup-abq`.")
            }
            Self::QueueNegotiatorHandleError(e) => e.fmt(f),
        }
    }
}

impl AbqInstance {
    pub fn negotiator_handle(&self) -> QueueNegotiatorHandle {
        match &self.locator {
            AbqLocator::Remote {
                queue_negotiator, ..
            } => *queue_negotiator,
            AbqLocator::Local(abq, _) => abq.get_negotiator_handle(),
        }
    }

    pub fn server_addr(&self) -> SocketAddr {
        match &self.locator {
            AbqLocator::Remote { server_addr, .. } => *server_addr,
            AbqLocator::Local(abq, _) => abq.server_addr(),
        }
    }

    pub async fn new_ephemeral(
        opt_user_token: Option<UserToken>,
        client_auth: ClientAuthStrategy,
        server_tls: ServerTlsStrategy,
        client_tls: ClientTlsStrategy,
    ) -> Self {
        tracing::debug!("Creating an ephemeral queue");

        let server_auth = match opt_user_token {
            Some(user_token) => {
                // Ephemeral instances are managed in-process, so there will be no external admin
                // communicating - as such we can create a fresh admin token.
                ServerAuthStrategy::from_set(user_token, AdminToken::new_random())
            }
            None => ServerAuthStrategy::no_auth(),
        };

        let manifests_path = tempfile::tempdir().expect("unable to create a temporary file");
        let persist_manifest = persistence::manifest::FilesystemPersistor::new_shared(
            manifests_path.path(),
            NoopPersister,
        );

        let results_path = tempfile::tempdir().expect("unable to create a temporary file");
        let persist_results =
            persistence::results::FilesystemPersistor::new_shared(results_path.path(), 10);

        let mut config = QueueConfig::new(persist_manifest, persist_results);
        config.server_options = ServerOptions::new(server_auth, server_tls);

        let queue = Abq::start(config).await;
        let guards = EphemeralAbqGuards {
            _manifests_path: manifests_path,
            _results_path: results_path,
        };

        AbqInstance {
            locator: AbqLocator::Local(queue, guards),
            client_options: ClientOptions::new(client_auth, client_tls),
        }
    }

    pub fn from_remote(
        entity: Entity,
        run_id: RunId,
        queue_addr: SocketAddr,
        auth: ClientAuthStrategy,
        client_tls: ClientTlsStrategy,
        deprecations: DeprecationRecord,
    ) -> Result<Self, AbqInstanceError> {
        tracing::debug!("Creating instance from remote {}", queue_addr);

        let client_options = ClientOptions::new(auth, client_tls);

        // TODO: if we get an error here, there is a reasonable chance it's because the provided
        // client auth is invalid; we should provide a nice error message in such cases.
        let queue_negotiator = QueueNegotiatorHandle::ask_queue(
            entity,
            run_id,
            queue_addr,
            client_options.clone(),
            deprecations,
        )?;

        let abq = AbqLocator::Remote {
            queue_negotiator,
            server_addr: queue_addr,
        };

        Ok(AbqInstance {
            locator: abq,
            client_options,
        })
    }

    pub fn client_options(&self) -> &ClientOptions {
        &self.client_options
    }

    pub fn client_options_owned(self) -> ClientOptions {
        self.client_options
    }
}
