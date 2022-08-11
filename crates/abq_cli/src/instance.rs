use abq_utils::net_protocol::publicize_addr;
use abq_workers::negotiate::{QueueNegotiatorHandle, QueueNegotiatorHandleError};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use std::net::{IpAddr, SocketAddr};

use abq_queue::queue::Abq;

use crate::args::unspecified_socket_addr;

use thiserror::Error;

/// Starts an [Abq] instance in the current process forever.
pub fn start_abq_forever(bind_addr: SocketAddr, public_ip: Option<IpAddr>) -> ! {
    abq_queue::queue::init();

    let public_ip = public_ip.unwrap_or_else(|| bind_addr.ip());

    let mut abq = Abq::start(bind_addr, public_ip);

    tracing::debug!("Queue active at {}", abq.server_addr());

    println!("Run the following to start workers and attach to the queue:");
    println!(
        "\tabq work --queue-addr={}",
        publicize_addr(abq.get_negotiator_handle().get_address(), public_ip)
    );
    println!("Run the following to invoke a test run:");
    println!(
        "\tabq test --queue-addr={} --negotiator-addr={} -- <your test args here>",
        publicize_addr(abq.server_addr(), public_ip),
        publicize_addr(abq.get_negotiator_handle().get_address(), public_ip)
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

pub(crate) enum AbqInstance {
    Remote {
        server_addr: SocketAddr,
        queue_negotiator: QueueNegotiatorHandle,
    },
    #[allow(unused)]
    Global(SocketAddr),
    Local(Abq),
}

#[derive(Debug, Error)]
pub(crate) enum AbqInstanceError {
    #[error("{0}")]
    QueueNegotiatorHandleError(#[from] QueueNegotiatorHandleError),
}

impl AbqInstance {
    pub fn server_addr(&self) -> SocketAddr {
        match self {
            AbqInstance::Remote { server_addr, .. } => *server_addr,
            AbqInstance::Global(addr) => *addr,
            AbqInstance::Local(abq) => abq.server_addr(),
        }
    }

    pub fn negotiator_handle(&self) -> QueueNegotiatorHandle {
        match self {
            AbqInstance::Remote {
                queue_negotiator, ..
            } => *queue_negotiator,
            AbqInstance::Global(_addr) => todo!(),
            AbqInstance::Local(abq) => abq.get_negotiator_handle(),
        }
    }

    pub fn new_ephemeral() -> Self {
        tracing::debug!("Creating an ephemeral queue");

        abq_queue::queue::init();

        let queue = Abq::start(unspecified_socket_addr(), unspecified_socket_addr().ip());
        AbqInstance::Local(queue)
    }

    pub fn from_remote(queue_addr: SocketAddr) -> Result<Self, AbqInstanceError> {
        tracing::debug!("Creating instance from remote {}", queue_addr);

        let queue_negotiator = QueueNegotiatorHandle::ask_queue(queue_addr)?;
        Ok(AbqInstance::Remote {
            server_addr: queue_addr,
            queue_negotiator,
        })
    }
}
