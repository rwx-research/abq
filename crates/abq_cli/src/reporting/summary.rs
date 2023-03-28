//! Tracks summary of a test suite run.

use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use abq_reporting::output::RunnerSummary;
use abq_utils::{
    exit::ExitCode,
    net_protocol::{
        entity::WorkerRunner,
        runners::{Status, TestId, TestResult, TestRuntime},
        workers::INIT_RUN_NUMBER,
    },
};

use crate::reporting::SuiteResult;

/// Status of a test, possibly across multiple attempts.
#[derive(Debug)]
enum StatusTag {
    /// The test never failed.
    NeverFailed,
    /// The test has always failed, across all attempts.
    AlwaysFailed {
        /// The test failed due to an internal fault.
        internal_error: bool,
    },
    /// The test failed until a given attempt, then didn't fail.
    UltimatelySucceeded,
    /// The test succeeded until a given attempt, then failed.
    /// This should only happen when a worker ran more tests than needed for a retry.
    /// It indicates flakiness, but is accounted as a success.
    UltimatelyFailed,
}

#[derive(Debug)]
struct OverallStatus {
    tag: StatusTag,
    retried: bool,
}

impl OverallStatus {
    const fn new(tag: StatusTag, retried: bool) -> Self {
        Self { tag, retried }
    }
}

#[derive(Debug)]
struct TestStatus {
    overall_status: OverallStatus,
    file_path: Option<String>,
    runner: WorkerRunner,
}

impl TestStatus {
    const fn new(
        overall_status: OverallStatus,
        file_path: Option<String>,
        runner: WorkerRunner,
    ) -> Self {
        Self {
            overall_status,
            file_path,
            runner,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct AggregatedMetrics {
    failed: u64,
    retried: u64,
    exit_code: ExitCode,
}

impl AggregatedMetrics {
    fn new<'a>(iter: impl IntoIterator<Item = &'a OverallStatus>) -> Self {
        let mut exit_code = ExitCode::SUCCESS;
        let mut failed = 0;
        let mut retried = 0;
        use StatusTag::*;
        for status in iter {
            retried += status.retried as u64;
            match status.tag {
                AlwaysFailed { internal_error } => {
                    failed += 1;

                    if internal_error {
                        exit_code = ExitCode::ABQ_ERROR;
                    } else if exit_code == ExitCode::SUCCESS {
                        exit_code = ExitCode::FAILURE;
                    };
                }
                NeverFailed | UltimatelySucceeded | UltimatelyFailed => {
                    // no update
                }
            }
        }
        Self {
            failed,
            retried,
            exit_code,
        }
    }
}

pub struct SuiteTracker {
    start_time: Instant,
    test_time: TestRuntime,
    total_attempts: u64,

    tests: HashMap<TestId, TestStatus>,

    // (total_attempts, aggregated metrics)
    // Need to re-calculate every time the total attempts change.
    cached_aggregated_metric: Cell<Option<(u64, AggregatedMetrics)>>,
}

impl Default for SuiteTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SuiteTracker {
    /// Create a new tracker of a test suite from its manifest.
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            test_time: TestRuntime::ZERO,
            total_attempts: 0,

            tests: HashMap::new(),

            cached_aggregated_metric: Cell::new(None),
        }
    }

    pub fn account_result(&mut self, run_number: u32, test_result: &TestResult) {
        use StatusTag::*;

        self.total_attempts += 1;

        let mut is_new_result = false;
        if !self.tests.contains_key(&test_result.id) {
            self.tests.insert(
                test_result.id.clone(),
                TestStatus::new(
                    OverallStatus::new(NeverFailed, false),
                    test_result.location.as_ref().map(|l| l.file.clone()),
                    test_result.source.runner,
                ),
            );
            is_new_result = true;
        };

        let entry = self.tests.get_mut(&test_result.id).unwrap();

        let is_fail_like = test_result.status.is_fail_like();

        self.test_time += test_result.runtime;

        match entry.overall_status.tag {
            NeverFailed if is_fail_like => {
                if run_number == INIT_RUN_NUMBER {
                    entry.overall_status.tag = AlwaysFailed {
                        internal_error: matches!(
                            test_result.status,
                            Status::PrivateNativeRunnerError
                        ),
                    };
                } else {
                    entry.overall_status.tag = UltimatelyFailed;
                }
            }
            AlwaysFailed { .. } if !is_fail_like => {
                debug_assert!(run_number > INIT_RUN_NUMBER);
                entry.overall_status.tag = UltimatelySucceeded;
            }
            _ => { /* no update */ }
        }

        // If we haven't seen this result before, it must be retried.
        // We use this over a check of the `run_number` to deal with out-of-process retries and
        // report aggregation - if a test suite is out-of-process retried, it may consist of many
        // results with the same INIT_RUN_NUMBER. But in fact, all of those are retries.
        entry.overall_status.retried = !is_new_result;
    }

    fn load_aggregated_metrics(&self) -> AggregatedMetrics {
        match self.cached_aggregated_metric.get() {
            Some((dirty_bit, metrics)) if dirty_bit == self.total_attempts => metrics,
            _ => {
                let metric =
                    AggregatedMetrics::new(self.tests.values().map(|ts| &ts.overall_status));
                self.cached_aggregated_metric
                    .set(Some((self.total_attempts, metric)));
                metric
            }
        }
    }

    pub fn suite_result(&self) -> SuiteResult {
        let Self {
            start_time,
            test_time,
            tests,
            ..
        } = self;

        let count = tests.len() as _;

        let AggregatedMetrics {
            failed: failing,
            exit_code: suggested_exit_code,
            retried,
        } = self.load_aggregated_metrics();

        let mut runner_summaries: BTreeMap<WorkerRunner, RunnerSummary> = BTreeMap::new();

        for test in tests.values() {
            let runner_summary =
                runner_summaries
                    .entry(test.runner)
                    .or_insert_with(|| RunnerSummary {
                        runner: test.runner,
                        failures_per_file: Default::default(),
                        retries_per_file: Default::default(),
                    });

            match test.overall_status.tag {
                StatusTag::AlwaysFailed { internal_error } if !internal_error => {
                    if let Some(file_path) = &test.file_path {
                        runner_summary
                            .failures_per_file
                            .entry(file_path.clone())
                            .and_modify(|counter| *counter += 1)
                            .or_insert(1);
                    }
                }
                _ => {}
            }

            if test.overall_status.retried {
                if let Some(file_path) = &test.file_path {
                    runner_summary
                        .retries_per_file
                        .entry(file_path.clone())
                        .and_modify(|counter| *counter += 1)
                        .or_insert(1);
                }
            }
        }

        SuiteResult {
            suggested_exit_code,
            count,
            count_failed: failing,
            tests_retried: retried,
            wall_time: start_time.elapsed(),
            test_time: *test_time,
            runner_summaries: runner_summaries.into_values().collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, time::Duration};

    use abq_utils::{
        exit::ExitCode,
        net_protocol::{
            entity::RunnerMeta,
            runners::{Status, TestResult, TestResultSpec, TestRuntime},
            workers::{WorkId, INIT_RUN_NUMBER},
        },
    };

    use crate::reporting::SuiteResult;

    use super::{AggregatedMetrics, OverallStatus, StatusTag, SuiteTracker};

    const FAILURE: Status = Status::Failure {
        exception: None,
        backtrace: None,
    };
    const ERROR: Status = Status::Error {
        exception: None,
        backtrace: None,
    };

    #[test]
    fn failed_and_ec_when_no_failures() {
        use StatusTag::*;

        let AggregatedMetrics {
            failed,
            retried,
            exit_code,
        } = AggregatedMetrics::new([
            &OverallStatus::new(NeverFailed, false),
            &OverallStatus::new(UltimatelyFailed, false),
            &OverallStatus::new(UltimatelySucceeded, false),
            &OverallStatus::new(NeverFailed, false),
        ]);

        assert_eq!(failed, 0);
        assert_eq!(retried, 0);
        assert_eq!(exit_code, ExitCode::SUCCESS);
    }

    #[test]
    fn failed_and_ec_when_some_failures() {
        use StatusTag::*;

        let AggregatedMetrics {
            failed,
            retried,
            exit_code,
        } = AggregatedMetrics::new([
            &OverallStatus::new(NeverFailed, false),
            &OverallStatus::new(UltimatelyFailed, false),
            &OverallStatus::new(
                AlwaysFailed {
                    internal_error: false,
                },
                false,
            ),
            &OverallStatus::new(UltimatelySucceeded, false),
            &OverallStatus::new(
                AlwaysFailed {
                    internal_error: false,
                },
                false,
            ),
            &OverallStatus::new(NeverFailed, false),
            &OverallStatus::new(
                AlwaysFailed {
                    internal_error: false,
                },
                false,
            ),
        ]);

        assert_eq!(failed, 3);
        assert_eq!(retried, 0);
        assert_eq!(exit_code, ExitCode::FAILURE);
    }

    #[test]
    fn failed_and_ec_when_some_failures_and_internal_error() {
        use StatusTag::*;

        let AggregatedMetrics {
            failed,
            retried,
            exit_code,
        } = AggregatedMetrics::new([
            &OverallStatus::new(NeverFailed, false),
            &OverallStatus::new(UltimatelyFailed, false),
            &OverallStatus::new(
                AlwaysFailed {
                    internal_error: false,
                },
                false,
            ),
            &OverallStatus::new(UltimatelySucceeded, false),
            &OverallStatus::new(
                AlwaysFailed {
                    internal_error: true,
                },
                false,
            ),
            &OverallStatus::new(NeverFailed, false),
            &OverallStatus::new(
                AlwaysFailed {
                    internal_error: false,
                },
                false,
            ),
        ]);

        assert_eq!(failed, 3);
        assert_eq!(retried, 0);
        assert_eq!(exit_code, ExitCode::ABQ_ERROR);
    }

    type IdToWorkId = HashMap<&'static str, WorkId>;

    fn inject_results(
        results: impl IntoIterator<Item = (&'static str, u32, Status)>,
    ) -> (SuiteTracker, IdToWorkId) {
        let mut id_to_work_id: IdToWorkId = HashMap::new();
        let mut results_to_inject = vec![];

        for (id, run_number, status) in results {
            let work_id = id_to_work_id.entry(id).or_insert_with(WorkId::new);

            let test_result = TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    id: id.to_string(),
                    status,
                    ..TestResultSpec::fake()
                },
            );
            results_to_inject.push((*work_id, run_number, test_result));
        }

        let mut tracker = SuiteTracker::new();

        for (_work_id, run_number, result) in results_to_inject {
            tracker.account_result(run_number, &result);
        }
        (tracker, id_to_work_id)
    }

    macro_rules! test_suite_results {
        ($($test_name:ident, $statuses:expr, $expect_exit:expr, total $count:literal, failed $failed:literal, retries $retries:literal)*) => {$(
            #[test]
            fn $test_name() {
                #[allow(unused)]
                use Status::*;

                let (tracker, _) = inject_results($statuses);

                let SuiteResult {
                    suggested_exit_code,
                    count,
                    count_failed,
                    tests_retried,
                    wall_time: _,
                    test_time: _,
                    runner_summaries: _,
                } = tracker.suite_result();

                assert_eq!(suggested_exit_code, $expect_exit);
                assert_eq!(count, $count);
                assert_eq!(count_failed, $failed);
                assert_eq!(tests_retried, $retries);
            }
        )*};
    }

    test_suite_results! {
        success_if_no_errors, [
            ("id1", INIT_RUN_NUMBER, Success),
            ("id2", INIT_RUN_NUMBER, Pending),
            ("id3", INIT_RUN_NUMBER, Skipped),
        ], ExitCode::SUCCESS, total 3, failed 0, retries 0

        fail_if_success_then_error, [
            ("id1", INIT_RUN_NUMBER, Success),
            ("id2", INIT_RUN_NUMBER, ERROR)
        ], ExitCode::FAILURE, total 2, failed 1, retries 0

        fail_if_error_then_success, [
            ("id1", INIT_RUN_NUMBER, ERROR),
            ("id2", INIT_RUN_NUMBER, Success)
        ], ExitCode::FAILURE, total 2, failed 1, retries 0

        fail_if_success_then_failure, [
            ("id1", INIT_RUN_NUMBER, Success),
            ("id2", INIT_RUN_NUMBER, FAILURE)
        ], ExitCode::FAILURE, total 2, failed 1, retries 0

        fail_if_failure_then_success, [
            ("id1", INIT_RUN_NUMBER, Failure { exception: None, backtrace: None }),
            ("id2", INIT_RUN_NUMBER, Success)
        ], ExitCode::FAILURE, total 2, failed 1, retries 0

        error_if_success_then_internal_error, [
            ("id1", INIT_RUN_NUMBER, Success),
            ("id2", INIT_RUN_NUMBER, PrivateNativeRunnerError)
        ], ExitCode::ABQ_ERROR, total 2, failed 1, retries 0

        error_if_internal_error_then_success, [
            ("id1", INIT_RUN_NUMBER, PrivateNativeRunnerError),
            ("id2", INIT_RUN_NUMBER, Success),
        ], ExitCode::ABQ_ERROR, total 2, failed 1, retries 0

        error_if_error_then_internal_error, [
            ("id1", INIT_RUN_NUMBER, ERROR),
            ("id2", INIT_RUN_NUMBER, PrivateNativeRunnerError),
        ], ExitCode::ABQ_ERROR, total 2, failed 2, retries 0

        error_if_internal_error_then_error, [
            ("id1", INIT_RUN_NUMBER, PrivateNativeRunnerError),
            ("id2", INIT_RUN_NUMBER, ERROR),
        ], ExitCode::ABQ_ERROR, total 2, failed 2, retries 0

        error_if_failure_then_internal_error, [
            ("id1", INIT_RUN_NUMBER, FAILURE),
            ("id2", INIT_RUN_NUMBER, PrivateNativeRunnerError),
        ], ExitCode::ABQ_ERROR, total 2, failed 2, retries 0

        error_if_internal_error_then_failure, [
            ("id1", INIT_RUN_NUMBER, PrivateNativeRunnerError),
            ("id2", INIT_RUN_NUMBER, FAILURE),
        ], ExitCode::ABQ_ERROR, total 2, failed 2, retries 0

        retries_count_tests_not_attempts, [
            ("id1", INIT_RUN_NUMBER, FAILURE),
            ("id1", INIT_RUN_NUMBER + 1, FAILURE),
            ("id1", INIT_RUN_NUMBER + 2, FAILURE),
            ("id1", INIT_RUN_NUMBER + 3, FAILURE),
            ("id1", INIT_RUN_NUMBER + 4, FAILURE),
        ], ExitCode::FAILURE, total 1, failed 1, retries 1

        no_failures_after_retries, [
            ("id1", INIT_RUN_NUMBER, Success),
            ("id2", INIT_RUN_NUMBER, FAILURE),
            ("id3", INIT_RUN_NUMBER, FAILURE),
            ("id4", INIT_RUN_NUMBER, FAILURE),
            ("id5", INIT_RUN_NUMBER, FAILURE),
            ("id6", INIT_RUN_NUMBER, Success),
            //
            ("id2", INIT_RUN_NUMBER + 1, Success),
            ("id3", INIT_RUN_NUMBER + 1, Success),
            ("id4", INIT_RUN_NUMBER + 1, Success),
            ("id5", INIT_RUN_NUMBER + 1, Success),
        ], ExitCode::SUCCESS, total 6, failed 0, retries 4

        failures_after_retries, [
            ("id1", INIT_RUN_NUMBER, Success),
            ("id2", INIT_RUN_NUMBER, FAILURE),
            ("id3", INIT_RUN_NUMBER, FAILURE),
            ("id4", INIT_RUN_NUMBER, FAILURE),
            ("id5", INIT_RUN_NUMBER, FAILURE),
            ("id6", INIT_RUN_NUMBER, Success),
            //
            ("id2", INIT_RUN_NUMBER + 1, Success),
            ("id3", INIT_RUN_NUMBER + 1, FAILURE),
            ("id4", INIT_RUN_NUMBER + 1, Success),
            ("id5", INIT_RUN_NUMBER + 1, FAILURE),
            //
            ("id5", INIT_RUN_NUMBER + 2, FAILURE),
        ], ExitCode::FAILURE, total 6, failed 2, retries 4
    }

    #[test]
    fn update_suite_result_on_change() {
        let (mut tracker, _to_wid) = inject_results([
            ("id1", INIT_RUN_NUMBER, Status::Success),
            ("id2", INIT_RUN_NUMBER, FAILURE),
            ("id3", INIT_RUN_NUMBER, ERROR),
        ]);

        {
            let SuiteResult {
                suggested_exit_code,
                count,
                count_failed,
                tests_retried,
                ..
            } = tracker.suite_result();
            assert_eq!(suggested_exit_code, ExitCode::FAILURE);
            assert_eq!(count, 3);
            assert_eq!(count_failed, 2);
            assert_eq!(tests_retried, 0);
        }

        tracker.account_result(
            INIT_RUN_NUMBER + 1,
            &TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    id: "id2".to_owned(),
                    status: Status::Success,
                    ..TestResultSpec::fake()
                },
            ),
        );
        tracker.account_result(
            INIT_RUN_NUMBER + 1,
            &TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    id: "id3".to_owned(),
                    status: Status::Success,
                    ..TestResultSpec::fake()
                },
            ),
        );

        {
            let SuiteResult {
                suggested_exit_code,
                count,
                count_failed,
                tests_retried,
                ..
            } = tracker.suite_result();
            assert_eq!(suggested_exit_code, ExitCode::SUCCESS);
            assert_eq!(count, 3);
            assert_eq!(count_failed, 0);
            assert_eq!(tests_retried, 2);
        }
    }

    #[test]
    fn account_test_runtime() {
        let mut tracker = SuiteTracker::new();
        for i in 0..10 {
            let test_result = TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    id: format!("test{i}"),
                    status: Status::Success,
                    runtime: TestRuntime::Nanoseconds(2_000_000_000),
                    ..TestResultSpec::fake()
                },
            );
            tracker.account_result(INIT_RUN_NUMBER, &test_result);
        }

        let expected = Duration::from_secs(20);

        assert_eq!(tracker.test_time.duration(), expected);

        let summary = tracker.suite_result();

        assert_eq!(summary.test_time.duration(), expected);
    }
}
