use abq_queue::timeout::RunTimeoutStrategy;
use abq_utils::auth::{AdminToken, ServerAuthStrategy, UserToken};
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

use abq_queue::queue::{Abq, QueueConfig};

use thiserror::Error;
use tokio::{runtime::Builder, select};

type ClientOptions = abq_utils::net_opt::ClientOptions<abq_utils::auth::User>;
type ClientAuthStrategy = abq_utils::auth::ClientAuthStrategy<abq_utils::auth::User>;

/// Starts an [Abq] instance in the current process forever.
pub fn start_abq_forever(
    public_ip: Option<IpAddr>,
    bind_ip: IpAddr,
    server_port: u16,
    work_port: u16,
    negotiator_port: u16,
    server_options: ServerOptions,
) -> ! {
    // Public IP defaults to the binding IP.
    let public_ip = public_ip.unwrap_or(bind_ip);

    let queue_config = QueueConfig {
        public_ip,
        bind_ip,
        server_port,
        work_port,
        negotiator_port,
        server_options,
        timeout_strategy: RunTimeoutStrategy::RUN_BASED,
    };
    let mut abq = Abq::start(queue_config);

    tracing::debug!("Queue active at {}", abq.server_addr());

    println!("Run the following to invoke a test run:");
    println!(
        "\tabq test --queue-addr={} --run-id <a-unique-run-id> -- <your test args here>",
        publicize_addr(abq.server_addr(), public_ip),
    );
    println!("Run the following to start one or more workers for that test run:");
    println!(
        "\tabq work --queue-addr={} --run-id <the-same-run-id>",
        publicize_addr(abq.server_addr(), public_ip)
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
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let mut received_shutdown = false;

        loop {
            select! {
                _ = stop_queue_rx.recv() => {
                    if received_shutdown {
                        assert!(abq.is_retired());
                        tracing::debug!("second shutdown signal; killing queue");
                        // If we already received a shutdown signal, immediately
                        // exit and kill the queue.
                        return;
                    }
                    // Otherwise, move the queue into retirement and wake only
                    // to check if it's fully been drained.
                    received_shutdown = true;
                    abq.retire();
                    tracing::debug!("first shutdown signal; retiring queue");
                }
            }
        }
    });

    tracing::debug!("shutting down queue");
    term_signals_handle.close();
    listen_for_signals_thread.join().unwrap();

    tracing::debug!("shutting down queue");
    abq.shutdown().unwrap();

    std::process::exit(0);
}

pub(crate) struct AbqInstance {
    locator: AbqLocator,
    client_options: ClientOptions,
}

enum AbqLocator {
    Remote {
        queue_negotiator: QueueNegotiatorHandle,
    },
    Local(Abq),
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
            AbqLocator::Local(abq) => abq.get_negotiator_handle(),
        }
    }

    pub fn new_ephemeral(
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

        let queue = Abq::start(QueueConfig {
            server_options: ServerOptions::new(server_auth, server_tls),
            ..Default::default()
        });

        AbqInstance {
            locator: AbqLocator::Local(queue),
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

        let abq = AbqLocator::Remote { queue_negotiator };

        Ok(AbqInstance {
            locator: abq,
            client_options,
        })
    }

    pub fn client_options(&self) -> &ClientOptions {
        &self.client_options
    }
}
