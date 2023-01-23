use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::runners::CapturedOutput;
use abq_utils::net_protocol::workers::RunId;
use abq_workers::negotiate::{
    NegotiatedWorkers, QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator,
};
use abq_workers::workers::{WorkerContext, WorkersExit, WorkersExitStatus};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::iterator::Signals;

type ClientOptions = abq_utils::net_opt::ClientOptions<abq_utils::auth::User>;

pub fn start_workers(
    num_workers: NonZeroUsize,
    working_dir: PathBuf,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
    run_id: RunId,
    supervisor_in_band: bool,
) -> anyhow::Result<NegotiatedWorkers> {
    let context = WorkerContext::AlwaysWorkIn { working_dir };

    let workers_config = WorkersConfig {
        num_workers,
        worker_context: context,
        supervisor_in_band,
        debug_native_runner: std::env::var_os("ABQ_DEBUG_NATIVE").is_some(),
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

pub fn start_workers_standalone(
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
        false, // no supervisor in-band
    )
    .unwrap();

    let worker_entity = worker_pool.entity();

    const POLL_WAIT_TIME: Duration = Duration::from_millis(10);
    let mut term_signals = Signals::new(TERM_SIGNALS).unwrap();

    // Shut down the pool when
    //   - all its workers are done
    //   - we get a signal to shutdown
    loop {
        thread::sleep(POLL_WAIT_TIME);

        let should_shutdown =
            term_signals.pending().next().is_some() || !worker_pool.workers_alive();

        if should_shutdown {
            let WorkersExit {
                status,
                final_captured_outputs,
            } = worker_pool.shutdown();

            tracing::debug!("Workers shutdown");

            print_final_runner_outputs(worker_entity, num_workers, final_captured_outputs);

            // We want to exit with an appropriate code if the workers were determined to have run
            // any test that failed. This way, in distributed contexts, failure of test runs induces
            // failures of workers, and it is enough to restart all unsuccessful processes to
            // re-initialize an ABQ run.
            let exit_code = match status {
                WorkersExitStatus::Success => ExitCode::SUCCESS,
                WorkersExitStatus::Failure { exit_code } => exit_code,
                WorkersExitStatus::Error { errors } => {
                    for error in errors {
                        eprintln!("{error}");
                    }
                    ExitCode::ABQ_ERROR
                }
            };
            std::process::exit(exit_code.get());
        }
    }
}

pub(crate) fn print_final_runner_outputs(
    worker_entity: EntityId,
    num_workers: NonZeroUsize,
    final_captured_runner_outputs: Vec<(usize, CapturedOutput)>,
) {
    for (runner_idx, runner_out) in final_captured_runner_outputs {
        // If we only had one runner, don't display the runner index.
        let runner_index = if num_workers.get() == 1 {
            None
        } else {
            Some(runner_idx)
        };
        // NB: there is no reasonable way to surface the error at this point, since we are
        // about to shut down.
        let _opt_err = abq_output::format_final_runner_output(
            &mut std::io::stdout(),
            worker_entity,
            runner_index,
            &runner_out,
        );
    }
}
