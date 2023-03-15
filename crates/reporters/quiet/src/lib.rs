use abq_reporting::{CompletedSummary, ReportedResult, Reporter, ReportingError};

/// Ignores all results.
#[derive(Default, Debug)]
pub struct QuietReporter {}

impl QuietReporter {
    pub fn new() -> Self {
        Self {}
    }
}

impl Reporter for QuietReporter {
    #![allow(unused)]

    fn push_result(
        &mut self,
        run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError> {
        Ok(())
    }

    fn tick(&mut self) {}

    fn after_all_results(&mut self) {}

    fn finish(self: Box<Self>, _summary: &CompletedSummary) -> Result<(), ReportingError> {
        Ok(())
    }
}
