use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use abq_utils::net_protocol::workers::InvocationId;
use abq_workers::negotiate::{QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator};
use abq_workers::workers::WorkerContext;
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

pub fn start_workers(
    working_dir: PathBuf,
    queue_negotiator_addr: SocketAddr,
    invocation_id: InvocationId,
) -> ! {
    abq_workers::workers::init();

    let context = WorkerContext::AlwaysWorkIn { working_dir };

    // TODO: make this configurable
    let workers_config = WorkersConfig {
        num_workers: NonZeroUsize::new(4).unwrap(),
        worker_context: context,
        work_timeout: Duration::from_secs(30),
        work_retries: 2,
    };

    let queue_negotiator = QueueNegotiatorHandle::from_raw_address(queue_negotiator_addr).unwrap();

    tracing::debug!(
        "Workers attaching to queue negotiator {}",
        queue_negotiator.get_address()
    );

    let mut worker_pool = WorkersNegotiator::negotiate_and_start_pool(
        workers_config,
        queue_negotiator,
        invocation_id,
    )
    .unwrap();

    tracing::debug!("Workers attached");

    // Make sure the queue shuts down and the socket is unliked when the process dies.
    let mut term_signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();
    for _ in term_signals.forever() {
        #[allow(unused_must_use)]
        {
            worker_pool.shutdown();
            tracing::debug!("Workers shutdown");
            std::process::exit(0);
        }
    }

    std::process::exit(102);
}
