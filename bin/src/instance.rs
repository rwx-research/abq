use std::path::{Path, PathBuf};
use std::thread;

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use tempfile::TempDir;

use libabq::queue::Abq;

fn abq_socket() -> &'static Path {
    Path::new("/tmp/abq.socket")
}

/// Starts an [Abq] instance in the current process forever.
pub fn start_abq_forever() {
    libabq::queue::init();

    let socket = abq_socket();
    if socket.exists() {
        eprintln!(
            "The socket at {} is already taken. Is there another abq process running?",
            socket.display()
        );
        std::process::exit(1);
    }

    // Make sure the socket is unliked when the process dies.
    let mut term_signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
    thread::spawn(move || {
        for _ in term_signals.forever() {
            #[allow(unused_must_use)]
            {
                std::fs::remove_file(socket);
                std::process::exit(0);
            }
        }
    });

    let mut abq = Abq::start(socket.to_path_buf());
    abq.wait_forever();
}

fn temp_socket() -> (TempDir, PathBuf) {
    let socket_dir = tempfile::TempDir::new().unwrap();
    let socket_path = socket_dir.path().join("abq.socket");
    (socket_dir, socket_path)
}

pub enum AbqInstance {
    Ephemeral(&'static Path),
    Temp(Abq, TempDir),
}

impl AbqInstance {
    pub fn socket(&self) -> &Path {
        match self {
            AbqInstance::Ephemeral(path) => path,
            AbqInstance::Temp(abq, _) => abq.socket(),
        }
    }
}

pub fn find_abq() -> AbqInstance {
    if abq_socket().exists() {
        log::debug!("using ephemeral queue");
        AbqInstance::Ephemeral(abq_socket())
    } else {
        log::debug!("using temporary queue");
        // Create a temporary ABQ instance
        libabq::queue::init();

        let (sock_dir, socket) = temp_socket();
        let queue = Abq::start(socket);
        AbqInstance::Temp(queue, sock_dir)
    }
}
