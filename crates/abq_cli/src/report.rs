//! Implements the report command.

use std::time::Duration;

use abq_reporting::{CompletedSummary, ReportedResult, Reporter};
use abq_utils::{
    exit::ExitCode,
    log_assert_stderr,
    net_protocol::{
        self,
        entity::Entity,
        queue::AssociatedTestResults,
        results::{OpaqueLazyAssociatedTestResults, ResultsLine, Summary},
        workers::{RunId, WorkId},
    },
    retry::async_retry_n,
    timeout_future::TimeoutFuture,
};
use anyhow::{anyhow, bail};
use fnv::FnvHashSet;
use indoc::formatdoc;

use crate::{
    instance::AbqInstance,
    reporting::{build_reporters, summary, ReporterKind, StdoutPreferences},
};

const RECONNECT_ATTEMPTS: usize = 5;
const RECONNECT_DELAY: Duration = Duration::from_secs(3);

const PENDING_RESULTS_DELAY: Duration = Duration::from_secs(2);

pub(crate) async fn report_results(
    abq: AbqInstance,
    entity: Entity,
    run_id: RunId,
    reporter_kinds: Vec<ReporterKind>,
    stdout_preferences: StdoutPreferences,
    results_timeout: Duration,
) -> anyhow::Result<ExitCode> {
    use crate::reporting::ONE;

    let test_suite_name = "suite"; // TODO: determine this correctly
    let mut reporters = build_reporters(reporter_kinds, stdout_preferences, test_suite_name, ONE);
    let mut overall_tracker = summary::SuiteTracker::new();

    let mut seen_work_ids = FnvHashSet::default();

    let all_results: OpaqueLazyAssociatedTestResults =
        wait_for_results(abq, entity, run_id, results_timeout).await?;
    let all_results = all_results.decode().map_err(|e| {
        anyhow!(
            "failed to decode corrupted test results message: {}",
            e.to_string()
        )
    })?;

    let mut run_summary = None;

    for result_line in all_results.into_iter() {
        match result_line {
            ResultsLine::Results(results) => {
                handle_results(
                    results,
                    &mut seen_work_ids,
                    &mut reporters,
                    &mut overall_tracker,
                )?;
            }
            ResultsLine::Summary(summary) => {
                let old_summary = run_summary.replace(summary);
                log_assert_stderr!(
                    old_summary.is_none(),
                    "ABQ sent two summaries for a test run; this is an error."
                )
            }
        }
    }

    reporters
        .iter_mut()
        .for_each(|reporter| reporter.after_all_results());

    let suite_result = overall_tracker.suite_result();

    let Summary {
        manifest_size_nonce,
        native_runner_info,
    } = run_summary.ok_or_else(|| {
        anyhow!("ABQ did not report a run summary while fetching test results; this is an error.")
    })?;

    let seen_work_ids = seen_work_ids.len() as u64;
    if manifest_size_nonce != seen_work_ids {
        bail!(formatdoc! {"
            fatal error - ABQ recorded a test suite with manifest size {}, but only {} results were reported.
            This is an with ABQ itself. Please contact RWX.
            ",
            manifest_size_nonce,
            seen_work_ids
        })
    }

    let completed_summary = &CompletedSummary {
        native_runner_info: Some(native_runner_info),
    };

    for reporter in reporters {
        reporter.finish(completed_summary)?;
    }

    print!("\n\n");
    suite_result
        .write_short_summary_lines(&mut stdout_preferences.stdout_stream())
        .unwrap();

    Ok(suite_result.suggested_exit_code())
}

async fn wait_for_results(
    abq: AbqInstance,
    entity: Entity,
    run_id: RunId,
    results_timeout: Duration,
) -> anyhow::Result<OpaqueLazyAssociatedTestResults> {
    let task = wait_for_results_help(abq, entity, run_id);
    TimeoutFuture::new(task, results_timeout)
        .wait()
        .await
        .ok_or_else(|| {
            anyhow!(
                "timed out waiting for pending test results from completed run after {} seconds",
                results_timeout.as_secs()
            )
        })?
}

async fn wait_for_results_help(
    abq: AbqInstance,
    entity: Entity,
    run_id: RunId,
) -> anyhow::Result<OpaqueLazyAssociatedTestResults> {
    let queue_addr = abq.server_addr();
    let client = abq.client_options_owned().build_async()?;

    let mut attempt = 1;
    loop {
        let client = &client;
        let mut conn = async_retry_n(RECONNECT_ATTEMPTS, RECONNECT_DELAY, |_attempt| {
            client.connect(queue_addr)
        })
        .await
        .map_err(|_| anyhow!("failed to connect to ABQ after {RECONNECT_ATTEMPTS} attempts"))?;

        let request = net_protocol::queue::Request {
            entity,
            message: net_protocol::queue::Message::TestResults(run_id.clone()),
        };
        net_protocol::async_write(&mut conn, &request).await?;

        use net_protocol::queue::TestResultsResponse::*;
        let response = net_protocol::async_read(&mut conn).await?;
        match response {
            Results(results) => return Ok(results),
            Pending => {
                tracing::debug!(
                    attempt,
                    "deferring fetching results do to pending notification"
                );
                tokio::time::sleep(PENDING_RESULTS_DELAY).await;
                attempt += 1;
                continue;
            }
            OutstandingRunners(tags) => {
                let active_runners = tags
                    .into_iter()
                    .map(|t| t.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                bail!("failed to fetch test results because the following runners are still active: {active_runners}")
            }
            Error(reason) => bail!("failed to fetch test results because {reason}"),
        }
    }
}

#[inline]
fn handle_results(
    results: Vec<AssociatedTestResults>,
    seen_work_ids: &mut FnvHashSet<WorkId>,
    reporters: &mut [Box<dyn Reporter>],
    overall_tracker: &mut summary::SuiteTracker,
) -> anyhow::Result<()> {
    for AssociatedTestResults {
        work_id,
        run_number,
        results,
        before_any_test,
        after_all_tests,
    } in results
    {
        seen_work_ids.insert(work_id);

        let mut output_before = Some(before_any_test);
        let mut output_after = after_all_tests;

        let mut results = results.into_iter().peekable();

        while let Some(test_result) = results.next() {
            overall_tracker.account_result(run_number, &test_result);

            let output_before = std::mem::take(&mut output_before);
            let output_after = if results.peek().is_none() {
                // This is the last test result, the output-after is associated
                // with it
                std::mem::take(&mut output_after)
            } else {
                None
            };

            let reported_result = ReportedResult {
                output_before,
                test_result,
                output_after,
            };

            for reporter in reporters.iter_mut() {
                reporter.push_result(run_number, &reported_result)?;
            }
        }
    }
    Ok(())
}
