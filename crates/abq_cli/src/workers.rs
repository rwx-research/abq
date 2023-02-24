use std::num::NonZeroUsize;
use std::path::PathBuf;

use abq_reporting::{CompletedSummary, Reporter};
use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::{RunnerMeta, WorkerTag};
use abq_utils::net_protocol::runners::CapturedOutput;
use abq_utils::net_protocol::workers::{RunId, RunnerKind};
use abq_utils::results_handler::SharedResultsHandler;
use abq_workers::negotiate::{
    NegotiatedWorkers, QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator,
};
use abq_workers::workers::{WorkerContext, WorkersExit, WorkersExitStatus};

mod reporting;
mod retry_manifest_tracker;
mod summary;

use futures::stream::StreamExt;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook_tokio::Signals;

use crate::reporting::{ReporterKind, StdoutPreferences};
use crate::workers::reporting::create_reporting_task;

use self::reporting::{build_reporters, ReportingTaskHandle};

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
    reporter_kinds: Vec<ReporterKind>,
    stdout_preferences: StdoutPreferences,
    results_batch_size: u64,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
) -> ! {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let test_suite_name = "suite"; // TODO: determine this correctly
    let reporters = build_reporters(reporter_kinds, stdout_preferences, test_suite_name);

    rt.block_on(start_standalone_help(
        run_id,
        tag,
        num_workers,
        runner_kind,
        working_dir,
        reporters,
        stdout_preferences,
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
    reporters: Vec<Box<dyn Reporter>>,
    stdout_preferences: StdoutPreferences,
    results_batch_size: u64,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
) -> ! {
    let context = WorkerContext::AlwaysWorkIn { working_dir };

    let (reporting_proxy, reporting_handle) = create_reporting_task(reporters);

    let workers_config = WorkersConfig {
        tag,
        num_workers,
        runner_kind,
        local_results_handler: Box::new(reporting_proxy),
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
                do_shutdown(worker_pool, reporting_handle, stdout_preferences).await;
            }
            _ = term_signals.next() => {
                // Shutdown immediately
                do_forceful_shutdown(worker_pool);
            }
        }
    }
}

async fn do_shutdown(
    mut worker_pool: NegotiatedWorkers,
    reporting_handle: ReportingTaskHandle,
    stdout_preferences: StdoutPreferences,
) -> ! {
    let WorkersExit {
        status,
        native_runner_info,
        manifest_generation_output,
        final_captured_outputs,
    } = worker_pool.shutdown();

    tracing::debug!("Workers shutdown");

    let finalized_reporters = reporting_handle.join().await;

    if let Some((runner, manifest_output)) = manifest_generation_output {
        print_manifest_generation_output(runner, manifest_output);
    }
    print_final_runner_outputs(final_captured_outputs);

    let completed_summary = CompletedSummary { native_runner_info };

    let (suite_result, errors) = finalized_reporters.finish(&completed_summary);

    for error in errors {
        eprintln!("{error}");
    }

    print!("\n\n");
    suite_result
        .write_short_summary_lines(&mut stdout_preferences.stdout_stream())
        .unwrap();

    // If the workers didn't fault, exit with whatever status the test suite run is at; otherwise,
    // indicate the worker fault.
    let exit_code = match status {
        WorkersExitStatus::Success => suite_result.suggested_exit_code(),
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

fn do_forceful_shutdown(mut worker_pool: NegotiatedWorkers) -> ! {
    worker_pool.shutdown();

    std::process::exit(ExitCode::CANCELLED.get());
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
