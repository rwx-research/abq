use std::io::Write;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use abq_reporting::output::ShortSummaryGrouping;
use abq_reporting::CompletedSummary;
use abq_utils::capture_output::ProcessOutput;
use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::{WorkerRunner, WorkerTag};
use abq_utils::net_protocol::queue::{InvokeWork, TestStrategy};
use abq_utils::net_protocol::workers::{RunId, RunnerKind};
use abq_utils::results_handler::NotifyResults;
use abq_utils::whitespace::is_blank;
use abq_workers::negotiate::{
    NegotiatedWorkers, QueueNegotiatorHandle, WorkersConfig, WorkersNegotiator,
};
use abq_workers::resume::ResumeLog;
use abq_workers::workers::{WorkerContext, WorkersExit, WorkersExitStatus};

mod reporting;
mod spot_interruption;

use futures::stream::StreamExt;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook_tokio::Signals;

use crate::reporting::{build_reporters, ReporterKind, StdoutPreferences};
use crate::workers::reporting::create_reporting_task;
use crate::QueueLocation;

use self::reporting::ReportingTaskHandle;
use self::spot_interruption::SpotInterruption;

type ClientOptions = abq_utils::net_opt::ClientOptions<abq_utils::auth::User>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    WriteNormal,
    Readonly,
}
pub struct TestRunMetadata {
    pub api_url: String,
    pub access_token: Option<abq_hosted::AccessToken>,
    pub run_id: RunId,
    pub record_telemetry: bool,
    pub queue_location: QueueLocation,
}

pub async fn start_workers_standalone(
    run_id: RunId,
    tag: WorkerTag,
    num_runners: NonZeroUsize,
    max_run_number: u32,
    runner_kind: RunnerKind,
    working_dir: PathBuf,
    reporter_kinds: Vec<ReporterKind>,
    stdout_preferences: StdoutPreferences,
    batch_size: u64,
    test_strategy: TestStrategy,
    test_timeout: Duration,
    queue_negotiator: QueueNegotiatorHandle,
    client_opts: ClientOptions,
    startup_timeout: Duration,
    execution_mode: ExecutionMode,
    test_run_metadata: TestRunMetadata,
) -> ! {
    let test_suite_name = "suite"; // TODO: determine this correctly
    let has_stdout_reporters = reporter_kinds
        .iter()
        .any(|r| r.outputs_to_stdout(num_runners));
    let reporters = build_reporters(
        reporter_kinds,
        stdout_preferences,
        test_suite_name,
        num_runners,
    );

    let mut term_signals = Signals::new(TERM_SIGNALS).unwrap();

    let context = WorkerContext::AlwaysWorkIn { working_dir };

    let (reporting_proxy, reporting_handle) = create_reporting_task(reporters);

    // Only local runs can resume after a spot interruption, since their queue lives and dies with
    // this process.
    let spot_interruption = match test_run_metadata.queue_location.is_remote() {
        true => None,
        false => SpotInterruption::from_env(),
    };
    let resume_log = match spot_interruption {
        Some(_) => Some(resume_from_previous_attempt(reporting_proxy.clone()).await),
        None => None,
    };

    tracing::debug!(
        "Workers attaching to queue negotiator {}",
        queue_negotiator.get_address()
    );

    let invoke_work = InvokeWork {
        run_id: run_id.clone(),
        // On-demand runners request one test at a time from the queue.
        batch_size_hint: NonZeroU64::new(batch_size.max(1)).unwrap(),
        test_strategy,
        test_command_hash: runner_kind.command_hash(),
    };

    let stderr_writer = Box::new(stdout_preferences.stderr_stream());

    let mut worker_pool = WorkersNegotiator::negotiate_and_start_pool(
        WorkersConfig {
            tag,
            num_runners,
            runner_kind,
            local_results_handler: Box::new(reporting_proxy),
            worker_context: context,
            debug_native_runner: std::env::var_os("ABQ_DEBUG_NATIVE").is_some(),
            has_stdout_reporters,
            protocol_version_timeout: startup_timeout,
            test_timeout,
            results_batch_size_hint: batch_size,
            max_run_number,
            should_send_results: execution_mode == ExecutionMode::WriteNormal,
            warning_writer: stderr_writer,
            resume_log: resume_log.clone(),
        },
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
                do_shutdown(worker_pool, reporting_handle, stdout_preferences, execution_mode, run_id, tag, num_runners, test_run_metadata).await;
            }
            _ = term_signals.next() => {
                do_cancellation_shutdown(worker_pool).await;
            }
            () = spot_interruption_notice(&spot_interruption) => {
                do_spot_interruption_shutdown(
                    worker_pool,
                    reporting_handle,
                    spot_interruption.as_ref().unwrap(),
                    resume_log.as_ref().unwrap(),
                )
                .await;
            }
        }
    }
}

async fn spot_interruption_notice(spot_interruption: &Option<SpotInterruption>) {
    match spot_interruption {
        Some(spot_interruption) => spot_interruption.notice().await,
        None => std::future::pending().await,
    }
}

/// Starts this attempt where an interrupted previous attempt left off, if there was one: reports
/// the tests it completed, and returns a log that skips them in this attempt.
async fn resume_from_previous_attempt(mut reporting_proxy: impl NotifyResults) -> ResumeLog {
    let completed_tests = match SpotInterruption::previous_attempt() {
        Ok(completed_tests) => completed_tests,
        Err(error) => {
            eprintln!("Could not resume after spot interruption, running all tests: {error}");
            vec![]
        }
    };
    if !completed_tests.is_empty() {
        eprintln!(
            "Resuming after spot interruption: skipping {} tests completed by the interrupted attempt.",
            completed_tests.len()
        );
        let results = completed_tests.iter().map(|t| t.results.clone()).collect();
        reporting_proxy.send_results(results).await;
    }
    ResumeLog::resume_from(completed_tests)
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
    test_run_metadata: TestRunMetadata,
) -> ! {
    let WorkersExit {
        status,
        native_runner_info,
        manifest_generation_output,
        process_outputs,
        ..
    } = worker_pool.shutdown().await;

    tracing::debug!(?status, "Workers shutdown");

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

    if matches!(status, WorkersExitStatus::Completed { .. }) || suite_result.count > 0 {
        print!("\n\n");
        suite_result
            .write_short_summary_lines(&mut stdout, ShortSummaryGrouping::Runner)
            .unwrap();
        println!("\n");
        if execution_mode == ExecutionMode::WriteNormal
            && test_run_metadata.queue_location.is_remote()
        {
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

    if test_run_metadata.record_telemetry {
        if let Some(access_token) = test_run_metadata.access_token {
            if let Some(native_runner_info) = completed_summary.native_runner_info {
                abq_hosted::record_test_run_metadata(
                    test_run_metadata.api_url,
                    &access_token,
                    &test_run_metadata.run_id,
                    &native_runner_info.specification,
                )
                .await;
            }
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

/// Stops the run, records the tests completed so far for the retry Mint is about to start, and
/// writes out the results reported so far.
async fn do_spot_interruption_shutdown(
    mut worker_pool: NegotiatedWorkers,
    reporting_handle: ReportingTaskHandle,
    spot_interruption: &SpotInterruption,
    resume_log: &ResumeLog,
) -> ! {
    eprintln!("Received spot interruption notice; stopping so the run can resume on retry.");

    worker_pool.cancel().await;
    let WorkersExit {
        native_runner_info, ..
    } = worker_pool.shutdown().await;

    // Record the completed tests first; the retry depends on them, and Mint gives us little time.
    if let Err(error) = spot_interruption.write_retry_action(resume_log.completed()) {
        eprintln!("Failed to record completed tests for the spot interruption retry: {error}");
    }

    let (_, errors) = reporting_handle
        .join()
        .await
        .finish(&CompletedSummary { native_runner_info });
    for error in errors {
        eprintln!("{error}");
    }

    std::process::exit(spot_interruption::EXIT_CODE.get());
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
