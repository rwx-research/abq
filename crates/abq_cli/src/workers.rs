use std::num::NonZeroUsize;
use std::path::PathBuf;

use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::{RunnerMeta, WorkerTag};
use abq_utils::net_protocol::runners::CapturedOutput;
use abq_utils::net_protocol::workers::{RunId, RunnerKind};
use abq_utils::results_handler::SharedResultsHandler;
use abq_workers::negotiate::{
    NegotiatedWorkers, QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator,
};
use abq_workers::workers::{WorkerContext, WorkersExit, WorkersExitStatus};

use futures::stream::StreamExt;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook_tokio::Signals;

type ClientOptions = abq_utils::net_opt::ClientOptions<abq_utils::auth::User>;

pub fn start_workers(
    run_id: RunId,
    tag: WorkerTag,
    num_workers: NonZeroUsize,
    runner_kind: RunnerKind,
    working_dir: PathBuf,
    local_results_handler: SharedResultsHandler,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
    results_batch_size: u64,
    supervisor_in_band: bool,
) -> anyhow::Result<NegotiatedWorkers> {
    let context = WorkerContext::AlwaysWorkIn { working_dir };

    let workers_config = WorkersConfig {
        tag,
        num_workers,
        runner_kind,
        local_results_handler,
        worker_context: context,
        supervisor_in_band,
        debug_native_runner: std::env::var_os("ABQ_DEBUG_NATIVE").is_some(),
        results_batch_size_hint: results_batch_size,
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
    run_id: RunId,
    tag: WorkerTag,
    num_workers: NonZeroUsize,
    runner_kind: RunnerKind,
    working_dir: PathBuf,
    local_results_handler: SharedResultsHandler,
    results_batch_size: u64,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
) -> ! {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(start_standalone_help(
        run_id,
        tag,
        num_workers,
        runner_kind,
        working_dir,
        local_results_handler,
        results_batch_size,
        queue_negotiator,
        client_opts,
    ))
}

async fn start_standalone_help(
    run_id: RunId,
    tag: WorkerTag,
    num_workers: NonZeroUsize,
    runner_kind: RunnerKind,
    working_dir: PathBuf,
    local_results_handler: SharedResultsHandler,
    results_batch_size: u64,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
) -> ! {
    let context = WorkerContext::AlwaysWorkIn { working_dir };

    let workers_config = WorkersConfig {
        tag,
        num_workers,
        runner_kind,
        local_results_handler,
        worker_context: context,
        supervisor_in_band: false,
        debug_native_runner: std::env::var_os("ABQ_DEBUG_NATIVE").is_some(),
        results_batch_size_hint: results_batch_size,
    };

    tracing::debug!(
        "Workers attaching to queue negotiator {}",
        queue_negotiator.get_address()
    );

    let mut worker_pool = WorkersNegotiator::negotiate_and_start_pool_on_executor(
        workers_config,
        queue_negotiator,
        client_opts,
        run_id,
    )
    .await
    .unwrap();

    tracing::debug!("Workers attached");

    let mut term_signals = Signals::new(TERM_SIGNALS).unwrap();

    // Shut down the pool when
    //   - all its workers are done
    //   - we get a signal to shutdown
    loop {
        tokio::select! {
            () = worker_pool.wait() => {
                do_shutdown(worker_pool);
            }
            _ = term_signals.next() => {
                do_shutdown(worker_pool);
            }
        }
    }
}

fn do_shutdown(mut worker_pool: NegotiatedWorkers) -> ! {
    let WorkersExit {
        status,
        manifest_generation_output,
        final_captured_outputs,
    } = worker_pool.shutdown();

    tracing::debug!("Workers shutdown");

    if let Some((runner, manifest_output)) = manifest_generation_output {
        print_manifest_generation_output(runner, manifest_output);
    }
    print_final_runner_outputs(final_captured_outputs);

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

pub(crate) fn print_manifest_generation_output(
    runner: RunnerMeta,
    manifest_output: CapturedOutput,
) {
    // NB: there is no reasonable way to surface the error at this point, since we are
    // about to shut down.
    let _opt_err = abq_reporting::output::format_manifest_generation_output(
        &mut std::io::stdout(),
        runner,
        &manifest_output,
    );
}

pub(crate) fn print_final_runner_outputs(
    final_captured_runner_outputs: Vec<(RunnerMeta, CapturedOutput)>,
) {
    for (runner, runner_out) in final_captured_runner_outputs {
        // NB: there is no reasonable way to surface the error at this point, since we are
        // about to shut down.
        let _opt_err = abq_reporting::output::format_final_runner_output(
            &mut std::io::stdout(),
            runner,
            &runner_out,
        );
    }
}
