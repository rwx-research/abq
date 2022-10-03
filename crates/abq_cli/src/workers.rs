use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use abq_utils::net_opt::ClientOptions;
use abq_utils::net_protocol::workers::RunId;
use abq_workers::negotiate::{
    NegotiatedWorkers, QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator,
};
use abq_workers::workers::{WorkerContext, WorkersExit};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

pub fn start_workers(
    num_workers: NonZeroUsize,
    working_dir: PathBuf,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
    run_id: RunId,
) -> anyhow::Result<NegotiatedWorkers> {
    abq_workers::workers::init();

    let context = WorkerContext::AlwaysWorkIn { working_dir };

    // TODO: make this configurable
    let workers_config = WorkersConfig {
        num_workers,
        worker_context: context,
        work_timeout: Duration::from_secs(30),
        work_retries: 2,
    };

    tracing::debug!(
        "Workers attaching to queue negotiator {}",
        queue_negotiator.get_address()
    );

    let worker_pool = WorkersNegotiator::negotiate_and_start_pool(
        workers_config,
        queue_negotiator,
        client_opts,
        run_id,
    )?;

    tracing::debug!("Workers attached");

    Ok(worker_pool)
}

pub fn start_workers_forever(
    num_workers: NonZeroUsize,
    working_dir: PathBuf,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
    run_id: RunId,
) -> ! {
    let mut worker_pool = start_workers(
        num_workers,
        working_dir,
        queue_negotiator,
        client_opts,
        run_id,
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
            let exit_status = worker_pool.shutdown();
            tracing::debug!("Workers shutdown");

            // We want to exit with an appropriate code if the workers were determined to have run
            // any test that failed. This way, in distributed contexts, failure of test runs induces
            // failures of workers, and it is enough to restart all unsuccessful processes to
            // re-initialize an ABQ run.
            let exit_code = match exit_status {
                WorkersExit::Success => 0,
                WorkersExit::Failure => 1,
                WorkersExit::Error => 101,
            };
            std::process::exit(exit_code);
        }
    }
}
