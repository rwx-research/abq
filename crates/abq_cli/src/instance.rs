use std::net::{IpAddr, SocketAddr};
use std::path::Path;

use abq_utils::net_protocol::publicize_addr;
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

use abq_queue::queue::Abq;

use crate::args::unspecified_socket_addr;

fn state_file() -> &'static Path {
    Path::new("/tmp/abq.state")
}

fn state_write_addr(addr: SocketAddr) {
    std::fs::write(state_file(), addr.to_string()).unwrap();
}

fn state_read_addr() -> SocketAddr {
    let contents = std::fs::read_to_string(state_file()).unwrap();
    contents.parse().unwrap()
}

/// Starts an [Abq] instance in the current process forever.
pub fn start_abq_forever(bind_addr: SocketAddr, public_ip: Option<IpAddr>) -> ! {
    abq_queue::queue::init();

    let public_ip = public_ip.unwrap_or_else(|| bind_addr.ip());

    if state_file().exists() {
        eprintln!(
            "ABQ's state file is present at {}. Is there another abq process running?",
            state_file().display(),
        );
        std::process::exit(1);
    }

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

    state_write_addr(abq.server_addr());

    // Make sure the queue shuts down and the socket is unliked when the process dies.
    let mut term_signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
    for _ in term_signals.forever() {
        #[allow(unused_must_use)]
        {
            std::fs::remove_file(state_file());
            abq.shutdown();
            tracing::debug!("Queue shutdown");
            std::process::exit(0);
        }
    }

    std::process::exit(101);
}

pub enum AbqInstance {
    Remote {
        server_addr: SocketAddr,
        negotiator_addr: SocketAddr,
    },
    Global(SocketAddr),
    Local(Abq),
}

impl AbqInstance {
    pub fn server_addr(&self) -> SocketAddr {
        match self {
            AbqInstance::Remote { server_addr, .. } => *server_addr,
            AbqInstance::Global(addr) => *addr,
            AbqInstance::Local(abq) => abq.server_addr(),
        }
    }

    pub fn negotiator_addr(&self) -> SocketAddr {
        match self {
            AbqInstance::Remote {
                negotiator_addr, ..
            } => *negotiator_addr,
            AbqInstance::Global(_addr) => todo!(),
            AbqInstance::Local(abq) => abq.get_negotiator_handle().get_address(),
        }
    }
}

/// Gets an instnace ABQ (and possibly workers).
///
/// If there is a global ABQ instance available, that is used. Otherwise, a local queue is created.
pub fn get_abq(queue_addr: Option<(SocketAddr, SocketAddr)>) -> AbqInstance {
    if let Some((server_addr, negotiator_addr)) = queue_addr {
        AbqInstance::Remote {
            server_addr,
            negotiator_addr,
        }
    } else if state_file().exists() {
        tracing::debug!("using ephemeral queue");
        AbqInstance::Global(state_read_addr())
    } else {
        tracing::debug!("using temporary queue");
        // Create a temporary ABQ instance
        abq_queue::queue::init();

        let queue = Abq::start(unspecified_socket_addr(), unspecified_socket_addr().ip());
        AbqInstance::Local(queue)
    }
}
