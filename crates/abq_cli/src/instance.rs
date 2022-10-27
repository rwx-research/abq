use abq_utils::auth::{AdminToken, ServerAuthStrategy, UserToken};
use abq_utils::net_opt::{ServerOptions, Tls};
use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::publicize_addr;
use abq_workers::negotiate::{QueueNegotiatorHandle, QueueNegotiatorHandleError};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use std::net::{IpAddr, SocketAddr};

use abq_queue::queue::{Abq, QueueConfig};

use thiserror::Error;

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
    abq_queue::queue::init();

    // Public IP defaults to the binding IP.
    let public_ip = public_ip.unwrap_or(bind_ip);

    let queue_config = QueueConfig {
        public_ip,
        bind_ip,
        server_port,
        work_port,
        negotiator_port,
        server_options,
    };
    let mut abq = Abq::start(queue_config);

    tracing::debug!("Queue active at {}", abq.server_addr());

    println!("Run the following to start workers and attach to the queue:");
    println!(
        "\tabq work --queue-addr={}",
        publicize_addr(abq.server_addr(), public_ip)
    );
    println!("Run the following to invoke a test run:");
    println!(
        "\tabq test --queue-addr={} -- <your test args here>",
        publicize_addr(abq.server_addr(), public_ip),
    );

    // Make sure the queue shuts down and the socket is unliked when the process dies.
    let mut term_signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
    for _ in term_signals.forever() {
        #[allow(unused_must_use)]
        {
            abq.shutdown();
            tracing::debug!("Queue shutdown");
            std::process::exit(0);
        }
    }

    std::process::exit(101);
}

pub(crate) struct AbqInstance {
    locator: AbqLocator,
    client_options: ClientOptions,
}

enum AbqLocator {
    Remote {
        server_addr: SocketAddr,
        queue_negotiator: QueueNegotiatorHandle,
    },
    Local(Abq),
}

#[derive(Debug, Error)]
pub(crate) enum AbqInstanceError {
    #[error("{0}")]
    QueueNegotiatorHandleError(#[from] QueueNegotiatorHandleError),
}

impl AbqInstance {
    pub fn server_addr(&self) -> SocketAddr {
        match &self.locator {
            AbqLocator::Remote { server_addr, .. } => *server_addr,
            AbqLocator::Local(abq) => abq.server_addr(),
        }
    }

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
        tls: Tls,
    ) -> Self {
        tracing::debug!("Creating an ephemeral queue");

        abq_queue::queue::init();

        let server_auth = match opt_user_token {
            Some(user_token) => {
                // Ephemeral instances are managed in-process, so there will be no external admin
                // communicating - as such we can create a fresh admin token.
                ServerAuthStrategy::from_set(user_token, AdminToken::new_random())
            }
            None => ServerAuthStrategy::no_auth(),
        };

        let queue = Abq::start(QueueConfig {
            server_options: ServerOptions::new(server_auth, tls),
            ..Default::default()
        });

        AbqInstance {
            locator: AbqLocator::Local(queue),
            client_options: ClientOptions::new(client_auth, tls),
        }
    }

    pub fn from_remote(
        entity: EntityId,
        queue_addr: SocketAddr,
        auth: ClientAuthStrategy,
        tls: Tls,
    ) -> Result<Self, AbqInstanceError> {
        tracing::debug!("Creating instance from remote {}", queue_addr);

        let client_options = ClientOptions::new(auth, tls);

        // TODO: if we get an error here, there is a reasonable chance it's because the provided
        // client auth is invalid; we should provide a nice error message in such cases.
        let queue_negotiator =
            QueueNegotiatorHandle::ask_queue(entity, queue_addr, client_options)?;

        let abq = AbqLocator::Remote {
            server_addr: queue_addr,
            queue_negotiator,
        };

        Ok(AbqInstance {
            locator: abq,
            client_options,
        })
    }

    pub fn client_options(&self) -> ClientOptions {
        self.client_options
    }
}
