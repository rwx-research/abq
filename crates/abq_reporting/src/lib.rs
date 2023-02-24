use thiserror::Error;

use abq_utils::net_protocol::{client::ReportedResult, queue::NativeRunnerInfo};

pub mod colors;
pub mod output;

#[must_use]
#[derive(Debug)]
pub struct CompletedSummary {
    /// The native test runner that was in use for this test run.
    /// Can be [None] if the returned worker never ran any tests.
    pub native_runner_info: Option<NativeRunnerInfo>,
}

#[derive(Debug, Error)]
pub enum ReportingError {
    #[error("failed to format a test result in the reporting format")]
    FailedToFormat,
    #[error("failed to write a report to an output buffer")]
    FailedToWrite,
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// A [`Reporter`] defines a way to emit abq test results.
///
/// A reporter is allowed to be side-effectful.
pub trait Reporter: Send {
    /// Consume the next test result.
    fn push_result(
        &mut self,
        run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError>;

    fn tick(&mut self);

    /// Runs after the last call to [Self::push_result] and [Self::tick], but before [Self::finish].
    fn after_all_results(&mut self);

    /// Consume the reporter, and perform any needed finalization steps.
    ///
    /// This method is only called when all test results for a run have been consumed.
    fn finish(self: Box<Self>, summary: &CompletedSummary) -> Result<(), ReportingError>;
}
