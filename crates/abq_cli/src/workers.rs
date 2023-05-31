use std::io::Write;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use abq_reporting::output::ShortSummaryGrouping;
use abq_reporting::CompletedSummary;
use abq_utils::capture_output::ProcessOutput;
use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::{WorkerRunner, WorkerTag};
use abq_utils::net_protocol::queue::InvokeWork;
use abq_utils::net_protocol::workers::{RunId, RunnerKind};
use abq_utils::whitespace::is_blank;
use abq_workers::negotiate::{
    NegotiatedWorkers, QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator,
};
use abq_workers::workers::{WorkerContext, WorkersExit, WorkersExitStatus};

mod reporting;

use futures::stream::StreamExt;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook_tokio::Signals;

use crate::reporting::{build_reporters, ReporterKind, StdoutPreferences};
use crate::workers::reporting::create_reporting_task;

use self::reporting::ReportingTaskHandle;

type ClientOptions = abq_utils::net_opt::ClientOptions<abq_utils::auth::User>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    WriteNormal,
    Readonly,
}
pub struct TestRunMetadata {
    pub api_url: String,
    pub access_token: abq_hosted::AccessToken,
    pub run_id: RunId,
}

pub async fn start_workers_standalone(
    run_id: RunId,
    tag: WorkerTag,
    num_workers: NonZeroUsize,
    max_run_number: u32,
    runner_kind: RunnerKind,
    working_dir: PathBuf,
    reporter_kinds: Vec<ReporterKind>,
    stdout_preferences: StdoutPreferences,
    batch_size: NonZeroU64,
    test_timeout: Duration,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
    startup_timeout: Duration,
    execution_mode: ExecutionMode,
    test_run_metadata: Option<TestRunMetadata>,
) -> ! {
    let test_suite_name = "suite"; // TODO: determine this correctly
    let has_stdout_reporters = reporter_kinds
        .iter()
        .any(|r| r.outputs_to_stdout(num_workers));
    let reporters = build_reporters(
        reporter_kinds,
        stdout_preferences,
        test_suite_name,
        num_workers,
    );

    let mut term_signals = Signals::new(TERM_SIGNALS).unwrap();

    let context = WorkerContext::AlwaysWorkIn { working_dir };

    let (reporting_proxy, reporting_handle) = create_reporting_task(reporters);

    let workers_config = WorkersConfig {
        tag,
        num_workers,
        runner_kind,
        local_results_handler: Box::new(reporting_proxy),
        worker_context: context,
        debug_native_runner: std::env::var_os("ABQ_DEBUG_NATIVE").is_some(),
        has_stdout_reporters,
        protocol_version_timeout: startup_timeout,
        test_timeout,
        results_batch_size_hint: batch_size.get(),
        max_run_number,
        should_send_results: execution_mode == ExecutionMode::WriteNormal,
    };

    tracing::debug!(
        "Workers attaching to queue negotiator {}",
        queue_negotiator.get_address()
    );

    let invoke_work = InvokeWork {
        run_id: run_id.clone(),
        batch_size_hint: batch_size,
    };

    let mut worker_pool = WorkersNegotiator::negotiate_and_start_pool(
        workers_config,
        queue_negotiator,
        client_opts,
        invoke_work,
    )
    .await
    .unwrap();

    tracing::debug!("Workers attached");

    // Shut down the pool when
    //   - all its workers are done
    //   - we get a signal to shutdown
    loop {
        tokio::select! {
            () = worker_pool.wait() => {
                do_shutdown(worker_pool, reporting_handle, stdout_preferences, execution_mode, run_id, tag, num_workers, test_run_metadata).await;
            }
            _ = term_signals.next() => {
                do_cancellation_shutdown(worker_pool).await;
            }
        }
    }
}

fn runner_header(runner: &WorkerRunner) -> String {
    format!("\n{:-^80}\n", format!(" [{}] ", runner))
}

async fn do_shutdown(
    mut worker_pool: NegotiatedWorkers,
    reporting_handle: ReportingTaskHandle,
    stdout_preferences: StdoutPreferences,
    execution_mode: ExecutionMode,
    run_id: RunId,
    worker_tag: WorkerTag,
    num_runners: NonZeroUsize,
    test_run_metadata: Option<TestRunMetadata>,
) -> ! {
    let WorkersExit {
        status,
        native_runner_info,
        manifest_generation_output,
        process_outputs,
        ..
    } = worker_pool.shutdown().await;

    tracing::debug!("Workers shutdown");

    let finalized_reporters = reporting_handle.join().await;

    let mut stdout = stdout_preferences.stdout_stream();
    if let Some((_, manifest_output)) = manifest_generation_output {
        // NB: there is no reasonable way to surface the error at this point, since we are
        // about to shut down.
        let _ = print_manifest_generation_output(&mut stdout, manifest_output);
    }

    for (runner_meta, process_output) in process_outputs {
        if runner_meta.pipes_to_parent_stdio() || process_output.is_empty() {
            continue;
        }
        stdout
            .write_all(runner_header(&runner_meta.runner).as_bytes())
            .unwrap();
        stdout.write_all(&process_output).unwrap();
    }

    let native_runner_info = native_runner_info.clone();
    let completed_summary = CompletedSummary { native_runner_info };

    let (suite_result, errors) = finalized_reporters.finish(&completed_summary);

    for error in errors {
        eprintln!("{error}");
    }

    print!("\n\n");
    suite_result
        .write_short_summary_lines(&mut stdout, ShortSummaryGrouping::Runner)
        .unwrap();
    println!("\n");
    if execution_mode == ExecutionMode::WriteNormal {
        println!("Run the following command to replay these tests locally:");
        println!("\n");
        println!(
            "\tabq test --run-id {} --worker {} --num {} -- <your-test-command>",
            run_id,
            worker_tag.index(),
            num_runners,
        );
        println!("\n");
        println!("Specify your Access Token with the RWX_ACCESS_TOKEN env variable, passing --access-token, or running `abq login`.");
    }

    // If the workers didn't fault, exit with whatever status the test suite run is at; otherwise,
    // indicate the worker fault.
    let exit_code = match status {
        WorkersExitStatus::Completed(runner_exit_code) => {
            suite_result.suggested_exit_code().max(runner_exit_code)
        }
        WorkersExitStatus::Error { errors } => {
            for error in errors {
                eprintln!("{error}");
            }
            ExitCode::ABQ_ERROR
        }
    };

    if let Some(test_run_metadata) = test_run_metadata {
        if let Some(native_runner_info) = completed_summary.native_runner_info {
            abq_hosted::record_test_run_metadata(
                test_run_metadata.api_url,
                &test_run_metadata.access_token,
                &test_run_metadata.run_id,
                &native_runner_info.specification,
            )
            .await;
        }
    }

    std::process::exit(exit_code.get());
}

async fn do_cancellation_shutdown(mut worker_pool: NegotiatedWorkers) -> ! {
    worker_pool.cancel().await;

    let _ = worker_pool.shutdown().await;

    tracing::debug!("Workers cancelled");

    std::process::exit(ExitCode::CANCELLED.get());
}

const MANIFEST_GENERATION_HEADER: &str =
    "----------------------------- MANIFEST GENERATION ------------------------------\n";

pub(crate) fn print_manifest_generation_output(
    mut writer: impl Write,
    manifest_output: ProcessOutput,
) -> std::io::Result<()> {
    if is_blank(&manifest_output) {
        return Ok(());
    }

    writer.write_all(MANIFEST_GENERATION_HEADER.as_bytes())?;
    writer.write_all(&manifest_output)?;
    writer.write_all(b"\n")?;
    writer.flush()
}
