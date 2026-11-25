//! Resuming a run from where an interrupted attempt of it left off.
//!
//! Completion is tracked per manifest entry rather than per test result, because a manifest entry
//! (for example a whole test file) may produce results with different IDs than its own.

use std::{collections::HashSet, sync::Arc};

use abq_utils::net_protocol::{
    queue::AssociatedTestResults,
    runners::{Manifest, TestId},
};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

/// A manifest entry that finished running, with the results it produced.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompletedTest {
    /// ID of the manifest entry, as the native runner reported it in its manifest.
    pub test_id: TestId,
    pub results: AssociatedTestResults,
}

/// Records the manifest entries completed in this attempt, and skips the entries a previous
/// attempt completed when the manifest is generated.
#[derive(Clone, Default)]
pub struct ResumeLog(Arc<Mutex<ResumeLogInner>>);

#[derive(Default)]
struct ResumeLogInner {
    skip: HashSet<TestId>,
    completed: Vec<CompletedTest>,
}

impl ResumeLog {
    /// Starts a log that resumes from the tests a previous attempt completed.
    /// Those tests are kept in the log, so an interruption of this attempt records them again.
    pub fn resume_from(previous: Vec<CompletedTest>) -> Self {
        let skip = previous.iter().map(|t| t.test_id.clone()).collect();
        Self(Arc::new(Mutex::new(ResumeLogInner {
            skip,
            completed: previous,
        })))
    }

    pub(crate) fn record(&self, test_id: TestId, results: AssociatedTestResults) {
        self.0
            .lock()
            .completed
            .push(CompletedTest { test_id, results });
    }

    /// Drops the tests a previous attempt completed from a newly generated manifest.
    pub(crate) fn filter_manifest(&self, manifest: &mut Manifest) {
        let inner = self.0.lock();
        if !inner.skip.is_empty() {
            manifest.retain_tests(|id| !inner.skip.contains(id));
        }
    }

    /// All tests completed so far, including those resumed from a previous attempt.
    pub fn completed(&self) -> Vec<CompletedTest> {
        self.0.lock().completed.clone()
    }
}
