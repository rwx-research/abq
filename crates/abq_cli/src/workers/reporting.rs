use abq_reporting::{CompletedSummary, Reporter, ReportingError};
use abq_utils::{
    log_assert,
    net_protocol::{client::ReportedResult, queue::AssociatedTestResults},
    results_handler::{NotifyResults, SharedNotifyResults, SharedResultsHandler},
};
use async_trait::async_trait;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::reporting::{reporter_from_kind, ReporterKind, StdoutPreferences, SuiteResult};

use super::summary;

struct ReportingTask {
    rx_results: mpsc::UnboundedReceiver<Vec<AssociatedTestResults>>,
    reporters: Vec<Box<dyn Reporter>>,
    overall_tracker: summary::SuiteTracker,
}

pub(crate) struct FinalizedReporters {
    suite_result: SuiteResult,
    reporters: Vec<Box<dyn Reporter>>,
}

impl ReportingTask {
    fn new(
        rx_results: mpsc::UnboundedReceiver<Vec<AssociatedTestResults>>,
        reporters: Vec<Box<dyn Reporter>>,
    ) -> Self {
        Self {
            reporters,
            rx_results,
            overall_tracker: Default::default(),
        }
    }

    async fn work_forever(mut self) -> FinalizedReporters {
        while let Some(results) = self.rx_results.recv().await {
            results
                .into_iter()
                .for_each(|results| self.handle_associated_results(results));
        }

        self.reporters
            .iter_mut()
            .for_each(|reporter| reporter.after_all_results());

        FinalizedReporters {
            suite_result: self.overall_tracker.suite_result(),
            reporters: self.reporters,
        }
    }

    fn handle_associated_results(&mut self, results: AssociatedTestResults) {
        let AssociatedTestResults {
            work_id: _,
            run_number,
            results,
            before_any_test,
            after_all_tests,
        } = results;

        assert!(
            !results.is_empty(),
            "ABQ protocol must never ship empty list of test results"
        );

        let mut output_before = Some(before_any_test);
        let mut output_after = after_all_tests;

        let mut results = results.into_iter().peekable();

        while let Some(test_result) = results.next() {
            self.overall_tracker
                .account_result(run_number, &test_result);

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

            for reporter in self.reporters.iter_mut() {
                // TODO: is there a reasonable way to surface the error?
                let _opt_error = reporter.push_result(run_number, &reported_result);
            }
        }
    }
}

impl FinalizedReporters {
    pub fn finish(self, summary: &CompletedSummary) -> (SuiteResult, Vec<ReportingError>) {
        let Self {
            suite_result,
            reporters,
        } = self;

        let errors: Vec<_> = reporters
            .into_iter()
            .filter_map(|reporter| reporter.finish(summary).err())
            .collect();

        (suite_result, errors)
    }
}

pub(crate) struct ReportingTaskHandle {
    handle: JoinHandle<FinalizedReporters>,
}

impl ReportingTaskHandle {
    pub async fn join(self) -> FinalizedReporters {
        self.handle
            .await
            .expect("reporting task must not be dropped before its handle is")
    }
}

#[derive(Clone)]
pub struct ReportingProxy {
    tx_results: mpsc::UnboundedSender<Vec<AssociatedTestResults>>,
}

#[async_trait]
impl NotifyResults for ReportingProxy {
    async fn send_results(&mut self, results: Vec<AssociatedTestResults>) {
        let res = self.tx_results.send(results);
        log_assert!(
            res.is_ok(),
            "reporting receiver died before all test results consumed"
        );
    }
}

impl SharedNotifyResults for ReportingProxy {
    fn boxed_clone(&self) -> SharedResultsHandler {
        Box::new(self.clone())
    }
}

pub fn build_reporters(
    reporter_kinds: Vec<ReporterKind>,
    stdout_preferences: StdoutPreferences,
    test_suite_name: &str,
) -> Vec<Box<dyn Reporter>> {
    reporter_kinds
        .into_iter()
        .map(|kind| reporter_from_kind(kind, stdout_preferences, test_suite_name))
        .collect()
}

/// Spawns a task to handle reporting of results.
/// Returns a [ReportingProxy] that can be used to communicate with the reporting task.
pub(crate) fn create_reporting_task(
    reporters: Vec<Box<dyn Reporter>>,
) -> (ReportingProxy, ReportingTaskHandle) {
    // TODO make the channel bounded, though in practice this is unlikely to ever materially
    // block since processing results is CPU bound.
    let (tx_results, rx_results) = mpsc::unbounded_channel();

    let reporting_task = ReportingTask::new(rx_results, reporters);
    let task_handle = tokio::spawn(reporting_task.work_forever());

    let proxy = ReportingProxy { tx_results };
    let handle = ReportingTaskHandle {
        handle: task_handle,
    };

    (proxy, handle)
}
