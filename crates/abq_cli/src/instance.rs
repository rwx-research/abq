use std::net::SocketAddr;
use std::path::Path;

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

#[allow(unused)] // TODO: remove when CLI plugins are added back
fn state_read_addr() -> SocketAddr {
    let contents = std::fs::read_to_string(state_file()).unwrap();
    contents.parse().unwrap()
}

/// Starts an [Abq] instance in the current process forever.
pub fn start_abq_forever(bind_addr: SocketAddr) -> ! {
    abq_queue::queue::init();

    if state_file().exists() {
        eprintln!(
            "ABQ's state file is present at {}. Is there another abq process running?",
            state_file().display(),
        );
        std::process::exit(1);
    }

    let mut abq = Abq::start(bind_addr);

    tracing::debug!("Queue active at {}", abq.server_addr());

    println!("Run the following to start workers and attach to the queue:");
    println!(
        "\tabq work --queue-addr={}",
        abq.get_negotiator_handle().get_address()
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
    Ephemeral(SocketAddr),
    Temp(Abq),
}

impl AbqInstance {
    #[allow(unused)] // TODO: remove when CLI plugins are added back
    pub fn server_addr(&self) -> SocketAddr {
        match self {
            AbqInstance::Ephemeral(addr) => *addr,
            AbqInstance::Temp(abq) => abq.server_addr(),
        }
    }
}

#[allow(unused)] // TODO: remove when CLI plugins are added back
pub fn find_abq() -> AbqInstance {
    if state_file().exists() {
        tracing::debug!("using ephemeral queue");
        AbqInstance::Ephemeral(state_read_addr())
    } else {
        tracing::debug!("using temporary queue");
        // Create a temporary ABQ instance
        abq_queue::queue::init();

        let queue = Abq::start(unspecified_socket_addr());
        AbqInstance::Temp(queue)
    }
}
