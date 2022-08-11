use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use abq_utils::net_protocol::workers::InvocationId;
use abq_workers::negotiate::{QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator};
use abq_workers::workers::{WorkerContext, WorkerPool};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

pub fn start_workers(
    num_workers: NonZeroUsize,
    working_dir: PathBuf,
    queue_negotiator_addr: SocketAddr,
    invocation_id: InvocationId,
) -> anyhow::Result<WorkerPool> {
    abq_workers::workers::init();

    let context = WorkerContext::AlwaysWorkIn { working_dir };

    // TODO: make this configurable
    let workers_config = WorkersConfig {
        num_workers,
        worker_context: context,
        work_timeout: Duration::from_secs(30),
        work_retries: 2,
    };

    let queue_negotiator = QueueNegotiatorHandle::from_raw_address(queue_negotiator_addr)?;

    tracing::debug!(
        "Workers attaching to queue negotiator {}",
        queue_negotiator.get_address()
    );

    let worker_pool = WorkersNegotiator::negotiate_and_start_pool(
        workers_config,
        queue_negotiator,
        invocation_id,
    )?;

    tracing::debug!("Workers attached");

    Ok(worker_pool)
}

pub fn start_workers_forever(
    num_workers: NonZeroUsize,
    working_dir: PathBuf,
    queue_negotiator_addr: SocketAddr,
    invocation_id: InvocationId,
) -> ! {
    let mut worker_pool = start_workers(
        num_workers,
        working_dir,
        queue_negotiator_addr,
        invocation_id,
    )
    .unwrap();

    const POLL_WAIT_TIME: Duration = Duration::from_millis(10);
    let mut term_signals = Signals::new(&[SIGINT, SIGTERM]).unwrap();

    // Shut down the pool when
    //   - all its workers are done
    //   - we get a signal to shutdown
    loop {
        thread::sleep(POLL_WAIT_TIME);

        let should_shutdown =
            term_signals.pending().next().is_some() || !worker_pool.workers_alive();

        if should_shutdown {
            worker_pool.shutdown();
            tracing::debug!("Workers shutdown");
            std::process::exit(0);
        }
    }
}
