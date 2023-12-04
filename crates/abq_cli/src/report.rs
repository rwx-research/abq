//! Implements the report command.

use std::{net::SocketAddr, num::NonZeroUsize, time::Duration};

use abq_reporting::{
    output::{format_test_result_summary, ShortSummaryGrouping},
    CompletedSummary, ReportedResult, Reporter,
};
use abq_utils::{
    exit::ExitCode,
    log_assert_stderr, net_async,
    net_protocol::{
        self,
        entity::{Entity, WorkerRunner},
        queue::AssociatedTestResults,
        results::{OpaqueLazyAssociatedTestResults, ResultsLine, Summary},
        runners::{TestResult, TestResultSpec},
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
    reporting::{build_reporters, summary, ReporterKind, StdoutPreferences, SuiteResult},
};

use self::ordered_results::OrderedAssociatedResults;

mod ordered_results;

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
    let reporters = build_reporters(reporter_kinds, stdout_preferences, test_suite_name, ONE);
    let mut stdout = stdout_preferences.stdout_stream();

    let all_results: Vec<ResultsLine> =
        wait_for_results(abq, entity, run_id, results_timeout).await?;

    process_results(&mut stdout, reporters, all_results.into_iter())
}

pub(crate) async fn list_tests(
    abq: AbqInstance,
    entity: Entity,
    run_id: RunId,
    stdout_preferences: StdoutPreferences,
    results_timeout: Duration,
    worker: u32,
    runner: NonZeroUsize,
) -> anyhow::Result<ExitCode> {
    let all_results: Vec<ResultsLine> =
        wait_for_results(abq, entity, run_id, results_timeout).await?;

    print_tests_for_runner(
        &mut stdout_preferences.stdout_stream(),
        all_results.into_iter(),
        WorkerRunner::from((worker, runner.get() as u32)),
    );

    Ok(ExitCode::new(0))
}

fn print_tests_for_runner(
    writer: &mut impl termcolor::WriteColor,
    all_results: impl IntoIterator<Item = ResultsLine>,
    worker_runner: WorkerRunner,
) {
    let associated_test_results = all_results
        .into_iter()
        .filter_map(|results_line: ResultsLine| match results_line {
            ResultsLine::Results(results) => Some(results),
            ResultsLine::Summary(_) => None,
        })
        .flatten();

    let results = associated_test_results
        .filter_map(|associated_test_results| {
            // run number goes up for retries.
            if associated_test_results.run_number == 1 {
                Some(associated_test_results.results)
            } else {
                None
            }
        })
        .flatten();

    let mut results_for_worker: Vec<TestResultSpec> = results
        .filter(|test_result| test_result.source.runner == worker_runner)
        .map(|test_result| test_result.result)
        .collect();

    results_for_worker.sort_by_key(|result| result.timestamp); // future proofing in case results change order

    results_for_worker.iter().for_each(|result| {
        writeln!(writer, "{}", result.id).unwrap();
    });
}

fn process_results(
    writer: &mut impl termcolor::WriteColor,
    mut reporters: Vec<Box<dyn Reporter>>,
    all_results: impl IntoIterator<Item = ResultsLine>,
) -> anyhow::Result<ExitCode> {
    let mut seen_work_ids = FnvHashSet::default();
    let mut overall_tracker = summary::SuiteTracker::new();
    let mut tracked_results: Vec<ReportTestResult> = Vec::new();
    let mut run_summary = None;

    let all_results = all_results.into_iter();
    let (min_size, max_size) = all_results.size_hint();

    let mut ordered_results = OrderedAssociatedResults::with_capacity(max_size.unwrap_or(min_size));

    for result_line in all_results {
        match result_line {
            ResultsLine::Results(results) => {
                ordered_results.extend(results);
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

    for result in ordered_results.into_iter() {
        handle_result(
            result,
            &mut seen_work_ids,
            &mut reporters,
            &mut overall_tracker,
            &mut tracked_results,
        )?;
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

    writer.write_all(b"\n\n")?;

    print_report(writer, &suite_result, &tracked_results)
}

fn print_report(
    writer: &mut impl termcolor::WriteColor,
    suite_result: &SuiteResult,
    report_results: &Vec<ReportTestResult>,
) -> anyhow::Result<ExitCode> {
    let mut formatted_one_failure = false;

    for report_result in report_results {
        use abq_utils::net_protocol::runners::Status::*;

        match report_result.test_result.status {
            Failure { .. } | Error { .. } | PrivateNativeRunnerError | TimedOut => {
                if formatted_one_failure {
                    writeln!(writer)?;
                }
                formatted_one_failure = true;

                format_test_result_summary(
                    writer,
                    report_result.run_number,
                    &report_result.test_result,
                )?;
            }
            Success | Pending | Todo | Skipped => {}
        }
    }

    suite_result.write_short_summary_lines(writer, ShortSummaryGrouping::Worker)?;

    Ok(suite_result.suggested_exit_code())
}

async fn wait_for_results(
    abq: AbqInstance,
    entity: Entity,
    run_id: RunId,
    results_timeout: Duration,
) -> anyhow::Result<Vec<ResultsLine>> {
    let queue_addr = abq.server_addr();
    let client = abq.client_options_owned().build_async()?;

    let task = wait_for_results_help(queue_addr, client, entity, run_id);

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
    queue_addr: SocketAddr,
    client: Box<dyn net_async::ConfiguredClient>,
    entity: Entity,
    run_id: RunId,
) -> anyhow::Result<Vec<ResultsLine>> {
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

        // TODO: as this is a hot loop of just fetching results, reporting would be more
        // interactive if we wrote results into a channel as they came in, with the
        // results processing happening on a separate thread.
        use net_protocol::queue::TestResultsResponse::*;
        let response = net_protocol::async_read(&mut conn).await?;
        match response {
            StreamingResults => {
                let mut stream = net_protocol::async_read_stream(&mut conn).await?;

                let results =
                    OpaqueLazyAssociatedTestResults::read_results_lines(&mut stream).await?;
                let results = results.decode()?;

                return Ok(results);
            }
            Pending => {
                tracing::debug!(
                    attempt,
                    "deferring fetching results do to pending notification"
                );
                tokio::time::sleep(PENDING_RESULTS_DELAY).await;
                attempt += 1;
                continue;
            }
            RunInProgress { active_runners } => {
                if active_runners.is_empty() {
                    bail!("this ABQ run has not assigned all tests in your test suite, but there are no active runners to assign them to. Please either add more runners, or launch a new run.")
                }

                let active_runners = active_runners
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
fn handle_result(
    result: AssociatedTestResults,
    seen_work_ids: &mut FnvHashSet<WorkId>,
    reporters: &mut [Box<dyn Reporter>],
    overall_tracker: &mut summary::SuiteTracker,
    tracked_results: &mut Vec<ReportTestResult>,
) -> anyhow::Result<()> {
    let AssociatedTestResults {
        work_id,
        run_number,
        results,
        before_any_test,
        after_all_tests,
    } = result;
    seen_work_ids.insert(work_id);

    let mut output_before = Some(before_any_test);
    let mut output_after = after_all_tests;

    let mut results = results.into_iter().peekable();

    while let Some(test_result) = results.next() {
        overall_tracker.account_result(run_number, work_id, &test_result);

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

        let ReportedResult { test_result, .. } = reported_result;
        tracked_results.push(ReportTestResult {
            run_number,
            test_result,
        });
    }

    Ok(())
}

#[derive(Debug)]
struct ReportTestResult {
    run_number: u32,
    test_result: TestResult,
}

#[cfg(test)]
mod test {
    use abq_dot_reporter::DotReporter;
    use abq_line_reporter::LineReporter;
    use abq_reporting::Reporter;
    use abq_test_utils::{
        build_fake_server_client, color_writer::SharedTestColorWriter, wid,
        AssociatedTestResultsBuilder, TestResultBuilder,
    };
    use abq_utils::net_protocol::{
        self,
        entity::{Entity, WorkerRunner},
        queue::NativeRunnerInfo,
        results::{OpaqueLazyAssociatedTestResults, ResultsLine, Summary},
        runners::{AbqProtocolVersion, NativeRunnerSpecification, Status},
        workers::{RunId, INIT_RUN_NUMBER},
    };
    use abq_utils::time::EpochMillis;

    use super::{print_tests_for_runner, process_results, wait_for_results_help};

    #[tokio::test]
    async fn fetches_streamed_tests() {
        let (server, client) = build_fake_server_client().await;
        let server_addr = server.local_addr().unwrap();

        let results1 = ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
            wid(1),
            INIT_RUN_NUMBER,
            [TestResultBuilder::new("test1", Status::Success)],
        )
        .build()]);
        let results2 = ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
            wid(2),
            INIT_RUN_NUMBER,
            [TestResultBuilder::new("test1", Status::Success)],
        )
        .build()]);

        let server_task = {
            let (results1, results2) = (&results1, &results2);
            async move {
                use net_protocol::queue;

                let (conn, _) = server.accept().await.unwrap();
                let mut conn = server.handshake_ctx().handshake(conn).await.unwrap();
                let msg = net_protocol::async_read(&mut conn).await.unwrap();
                assert!(matches!(
                    msg,
                    queue::Request {
                        message: queue::Message::TestResults(_),
                        ..
                    }
                ));

                let results_buffer = OpaqueLazyAssociatedTestResults::into_jsonl_buffer(&[
                    serde_json::value::to_raw_value(results1).unwrap(),
                    serde_json::value::to_raw_value(results2).unwrap(),
                ])
                .unwrap();

                let mut results_buffer_slice = &results_buffer[..];

                net_protocol::async_write(&mut conn, &queue::TestResultsResponse::StreamingResults)
                    .await
                    .unwrap();

                net_protocol::async_write_stream(
                    &mut conn,
                    results_buffer.len(),
                    &mut results_buffer_slice,
                )
                .await
                .unwrap();
            }
        };

        let client_task =
            wait_for_results_help(server_addr, client, Entity::local_client(), RunId::unique());

        let ((), results) = tokio::join!(server_task, client_task);

        let results = results.unwrap();
        let expected = [results1, results2];
        assert_eq!(results, expected);
    }

    macro_rules! test_print_results_summary {
        ($test:ident, $reporter:ident, @$expect:literal) => {
        #[test]
        fn $test() {
            let mut buf = SharedTestColorWriter::new(vec![]);
            let reporter = $reporter::new(Box::new(buf.clone()));

            let reporters: Vec<Box<dyn Reporter>> = vec![Box::new(reporter)];

            let all_results = [
                ResultsLine::Summary(Summary {
                    manifest_size_nonce: 7,
                    native_runner_info: NativeRunnerInfo {
                        protocol_version: AbqProtocolVersion::V0_2,
                        specification: NativeRunnerSpecification::fake(),
                    },
                }),
                ResultsLine::Results(vec![
                    AssociatedTestResultsBuilder::new(
                        wid(1),
                        INIT_RUN_NUMBER,
                        [TestResultBuilder::new("test1", Status::Success)],
                    )
                    .build(),
                    AssociatedTestResultsBuilder::new(
                        wid(2),
                        INIT_RUN_NUMBER,
                        [TestResultBuilder::new(
                            "test2",
                            Status::Failure {
                                exception: None,
                                backtrace: None,
                            },
                        )
                        .output("my test2 failed")],
                    )
                    .build(),
                ]),
                ResultsLine::Results(vec![
                    AssociatedTestResultsBuilder::new(
                        wid(3),
                        INIT_RUN_NUMBER,
                        [TestResultBuilder::new("test3", Status::Skipped)],
                    )
                    .build(),
                    AssociatedTestResultsBuilder::new(
                        wid(4),
                        INIT_RUN_NUMBER,
                        [TestResultBuilder::new(
                            "test4",
                            Status::Error {
                                exception: None,
                                backtrace: None,
                            },
                        )
                        .output("my test4 errored")],
                    )
                    .build(),
                ]),
                ResultsLine::Results(vec![
                    AssociatedTestResultsBuilder::new(
                        wid(5),
                        INIT_RUN_NUMBER,
                        [TestResultBuilder::new("test5", Status::TimedOut).output("i timed out")],
                    )
                    .build(),
                    AssociatedTestResultsBuilder::new(
                        wid(6),
                        INIT_RUN_NUMBER,
                        [
                            TestResultBuilder::new("test6", Status::PrivateNativeRunnerError)
                                .output("native runner exception"),
                        ],
                    )
                    .build(),
                ]),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    wid(7),
                    INIT_RUN_NUMBER,
                    [
                        TestResultBuilder::new("test7", Status::Pending).output("i am pending"),
                        TestResultBuilder::new("test8", Status::Pending).output("i was skipped"),
                    ],
                )
                .build()]),
            ];

            process_results(&mut buf, reporters, all_results).unwrap();

            let snapshot = String::from_utf8(buf.get()).unwrap();

            insta::assert_snapshot!(snapshot, @$expect);
        }
        }
    }

    test_print_results_summary!(
        test_print_results_summary_line, LineReporter, @r###"
    test1: <green>ok<reset>
    test2: <red>FAILED<reset>
    test3: <yellow>skipped<reset>
    test4: <red>ERRORED<reset>
    test5: <red>TIMED OUT<reset>
    test6: <red>ERRORED<reset>
    native runner exception
    test7: <yellow>pending<reset>
    test8: <yellow>pending<reset>


    --- test2: <red>FAILED<reset> --- [worker 0]
    my test2 failed
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test4: <red>ERRORED<reset> --- [worker 0]
    my test4 errored
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test5: <red>TIMED OUT<reset> --- [worker 0]
    i timed out
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test6: <red>ERRORED<reset> --- [worker 0]
    native runner exception
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 0.00 seconds<reset> (0.00 seconds spent in test code)
    <bold-green>8 tests<reset>, <bold-red>4 failures<reset>

    Failures:

        worker 0:
            3   a/b/x.file
    "###
    );

    test_print_results_summary!(
        test_print_results_summary_dot, DotReporter, @r###"
    <green>.<reset><red>F<reset><yellow>S<reset><red>E<reset><red>F<reset><red>E<reset>native runner exception
    <yellow>P<reset><yellow>P<reset>


    --- test2: <red>FAILED<reset> --- [worker 0]
    my test2 failed
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test4: <red>ERRORED<reset> --- [worker 0]
    my test4 errored
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test5: <red>TIMED OUT<reset> --- [worker 0]
    i timed out
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test6: <red>ERRORED<reset> --- [worker 0]
    native runner exception
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 0.00 seconds<reset> (0.00 seconds spent in test code)
    <bold-green>8 tests<reset>, <bold-red>4 failures<reset>

    Failures:

        worker 0:
            3   a/b/x.file
    "###
    );

    macro_rules! test_print_results_with_retries {
        ($test:ident, $reporter:ident, @$expect:literal) => {
        #[test]
        fn $test() {
            let mut buf = SharedTestColorWriter::new(vec![]);
            let reporter = $reporter::new(Box::new(buf.clone()));

            let reporters: Vec<Box<dyn Reporter>> = vec![Box::new(reporter)];

            let work1 = wid(1);

            let test1 = "test1";
            let test2 = "test2";

            const SUCCESS: Status = Status::Success;
            const FAILURE: Status = Status::Failure { exception: None, backtrace: None };

            let all_results = [
                ResultsLine::Summary(Summary {
                    manifest_size_nonce: 1,
                    native_runner_info: NativeRunnerInfo {
                        protocol_version: AbqProtocolVersion::V0_2,
                        specification: NativeRunnerSpecification::fake(),
                    },
                }),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    work1,
                    INIT_RUN_NUMBER,
                    [
                        TestResultBuilder::new(test1, FAILURE).output("test 1, failure 1"),
                        TestResultBuilder::new(test2, FAILURE).output("test 2, failure 1"),
                    ],
                )
                .build()]),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    work1,
                    INIT_RUN_NUMBER + 1,
                    [
                        TestResultBuilder::new(test1, FAILURE).output("test 1, failure 2"),
                        TestResultBuilder::new(test2, FAILURE).output("test 2, failure 2"),
                    ],
                )
                .build()]),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    work1,
                    INIT_RUN_NUMBER + 2,
                    [
                        TestResultBuilder::new(test1, FAILURE).output("test 1, failure 3"),
                        TestResultBuilder::new(test2, SUCCESS).output("test 2, success"),
                    ],
                )
                .build()]),
            ];

            process_results(&mut buf, reporters, all_results).unwrap();

            let snapshot = String::from_utf8(buf.get()).unwrap();

            insta::assert_snapshot!(snapshot, @$expect);
        }
        }
    }

    test_print_results_with_retries!(
        test_print_results_with_retries_line, LineReporter, @r###"
    test1: <red>FAILED<reset>
    test2: <red>FAILED<reset>
    test1: <red>FAILED<reset>
    test2: <red>FAILED<reset>
    test1: <red>FAILED<reset>
    test2: <green>ok<reset>


    --- test1: <red>FAILED<reset> --- [worker 0]
    test 1, failure 1
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test2: <red>FAILED<reset> --- [worker 0]
    test 2, failure 1
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test1: <red>FAILED<reset> (attempt 2) --- [worker 0]
    test 1, failure 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test2: <red>FAILED<reset> (attempt 2) --- [worker 0]
    test 2, failure 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test1: <red>FAILED<reset> (attempt 3) --- [worker 0]
    test 1, failure 3
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 0.00 seconds<reset> (0.00 seconds spent in test code)
    <bold-green>2 tests<reset>, <bold-red>1 failures<reset>, <bold-yellow>2 retried<reset>

    Retries:

        worker 0:
            2   a/b/x.file

    Failures:

        worker 0:
            1   a/b/x.file
    "###
    );

    test_print_results_with_retries!(
        test_print_results_with_retries_dot, DotReporter, @r###"
    <red>F<reset><red>F<reset><red>F<reset><red>F<reset><red>F<reset><green>.<reset>


    --- test1: <red>FAILED<reset> --- [worker 0]
    test 1, failure 1
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test2: <red>FAILED<reset> --- [worker 0]
    test 2, failure 1
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test1: <red>FAILED<reset> (attempt 2) --- [worker 0]
    test 1, failure 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test2: <red>FAILED<reset> (attempt 2) --- [worker 0]
    test 2, failure 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test1: <red>FAILED<reset> (attempt 3) --- [worker 0]
    test 1, failure 3
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 0.00 seconds<reset> (0.00 seconds spent in test code)
    <bold-green>2 tests<reset>, <bold-red>1 failures<reset>, <bold-yellow>2 retried<reset>

    Retries:

        worker 0:
            2   a/b/x.file

    Failures:

        worker 0:
            1   a/b/x.file
    "###
    );

    macro_rules! test_print_results_with_retries_out_of_order {
        ($test:ident, $reporter:ident, @$expect:literal) => {
        #[test]
        fn $test() {
            let mut buf = SharedTestColorWriter::new(vec![]);
            let reporter = $reporter::new(Box::new(buf.clone()));

            let reporters: Vec<Box<dyn Reporter>> = vec![Box::new(reporter)];

            let work1 = wid(1);

            let test1 = "test1";
            let test2 = "test2";

            let time1 = EpochMillis::from_millis(1);
            let time2 = EpochMillis::from_millis(2);

            const SUCCESS: Status = Status::Success;
            const FAILURE: Status = Status::Failure { exception: None, backtrace: None };

            let all_results = [
                ResultsLine::Summary(Summary {
                    manifest_size_nonce: 1,
                    native_runner_info: NativeRunnerInfo {
                        protocol_version: AbqProtocolVersion::V0_2,
                        specification: NativeRunnerSpecification::fake(),
                    },
                }),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    work1,
                    INIT_RUN_NUMBER + 2,
                    [
                        TestResultBuilder::new(test1, FAILURE).output("test 1, failure 4").timestamp(time2),
                        TestResultBuilder::new(test2, SUCCESS).output("test 2, success").timestamp(time2),
                    ],
                )
                .build()]),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    work1,
                    INIT_RUN_NUMBER + 1,
                    [
                        TestResultBuilder::new(test1, FAILURE).output("test 1, failure 3").timestamp(time2),
                        TestResultBuilder::new(test2, FAILURE).output("test 2, failure 3").timestamp(time2),
                    ],
                )
                .build()]),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    work1,
                    INIT_RUN_NUMBER,
                    [
                        TestResultBuilder::new(test1, FAILURE).output("test 1, failure 2").timestamp(time2),
                        TestResultBuilder::new(test2, FAILURE).output("test 2, failure 2").timestamp(time2),
                    ],
                )
                .build()]),
                ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                    work1,
                    INIT_RUN_NUMBER,
                    [
                        TestResultBuilder::new(test1, FAILURE).output("test 1, failure 1").timestamp(time1),
                        TestResultBuilder::new(test2, FAILURE).output("test 2, failure 1").timestamp(time1),
                    ],
                )
                .build()]),
            ];

            process_results(&mut buf, reporters, all_results).unwrap();

            let snapshot = String::from_utf8(buf.get()).unwrap();

            insta::assert_snapshot!(snapshot, @$expect);
        }
        }
    }

    test_print_results_with_retries_out_of_order!(
        test_print_results_with_retries_out_of_order_line, LineReporter, @r###"
    test1: <red>FAILED<reset>
    test2: <red>FAILED<reset>
    test1: <red>FAILED<reset>
    test2: <red>FAILED<reset>
    test1: <red>FAILED<reset>
    test2: <red>FAILED<reset>
    test1: <red>FAILED<reset>
    test2: <green>ok<reset>


    --- test1: <red>FAILED<reset> --- [worker 0]
    test 1, failure 1
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test2: <red>FAILED<reset> --- [worker 0]
    test 2, failure 1
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test1: <red>FAILED<reset> --- [worker 0]
    test 1, failure 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test2: <red>FAILED<reset> --- [worker 0]
    test 2, failure 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test1: <red>FAILED<reset> (attempt 2) --- [worker 0]
    test 1, failure 3
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test2: <red>FAILED<reset> (attempt 2) --- [worker 0]
    test 2, failure 3
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout

    --- test1: <red>FAILED<reset> (attempt 3) --- [worker 0]
    test 1, failure 4
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 0.00 seconds<reset> (0.00 seconds spent in test code)
    <bold-green>2 tests<reset>, <bold-red>1 failures<reset>, <bold-yellow>2 retried<reset>

    Retries:

        worker 0:
            2   a/b/x.file

    Failures:

        worker 0:
            1   a/b/x.file
    "###
    );

    #[test]
    fn test_list_tests() {
        let work1 = wid(1);

        let test1 = "test1";
        let test2 = "test2";
        let test3 = "test3";

        const SUCCESS: Status = Status::Success;
        const FAILURE: Status = Status::Failure {
            exception: None,
            backtrace: None,
        };

        let all_results = [
            ResultsLine::Summary(Summary {
                manifest_size_nonce: 1,
                native_runner_info: NativeRunnerInfo {
                    protocol_version: AbqProtocolVersion::V0_2,
                    specification: NativeRunnerSpecification::fake(),
                },
            }),
            ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                work1,
                INIT_RUN_NUMBER,
                [
                    {
                        let mut on_other_runner = TestResultBuilder::new(test1, FAILURE)
                            .output("test 1 filtered out 1")
                            .build();
                        on_other_runner.source.runner = WorkerRunner::from((0, 2));
                        on_other_runner
                    },
                    {
                        let mut on_other_worker = TestResultBuilder::new(test2, FAILURE)
                            .output("test 2 filtered out 2")
                            .build();
                        on_other_worker.source.runner = WorkerRunner::from((1, 1));
                        on_other_worker
                    },
                    TestResultBuilder::new(test3, SUCCESS)
                        .output("test 3 included 1")
                        .build(),
                ],
            )
            .build()]),
            // retries are all included
            ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                work1,
                INIT_RUN_NUMBER + 1,
                [
                    TestResultBuilder::new(test1, FAILURE).output("test 1, failure 2"),
                    TestResultBuilder::new(test2, FAILURE).output("test 2, failure 2"),
                ],
            )
            .build()]),
            ResultsLine::Results(vec![AssociatedTestResultsBuilder::new(
                work1,
                INIT_RUN_NUMBER + 2,
                [
                    TestResultBuilder::new(test1, FAILURE).output("test 1, failure 3"),
                    TestResultBuilder::new(test2, SUCCESS).output("test 2, success"),
                ],
            )
            .build()]),
        ];

        let mut buf = SharedTestColorWriter::new(vec![]);
        print_tests_for_runner(&mut buf, all_results.clone(), WorkerRunner::from((0, 1)));
        let snapshot = String::from_utf8(buf.get()).unwrap();

        insta::assert_snapshot!(
            snapshot,
            @r###"
test3
"###

        );

        let mut buf = SharedTestColorWriter::new(vec![]);
        print_tests_for_runner(&mut buf, all_results.clone(), WorkerRunner::from((0, 2)));
        let snapshot = String::from_utf8(buf.get()).unwrap();

        insta::assert_snapshot!(
            snapshot,
            @r###"
test1
"###

        );

        let mut buf = SharedTestColorWriter::new(vec![]);
        print_tests_for_runner(&mut buf, all_results, WorkerRunner::from((1, 1)));
        let snapshot = String::from_utf8(buf.get()).unwrap();

        insta::assert_snapshot!(
            snapshot,
            @r###"
test2
"###

        );
    }
}
