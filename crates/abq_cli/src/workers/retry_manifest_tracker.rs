// NB(ayaz): i think i want to rip this out and move it one level lower, to happen in the
// individual workers. We probably will need a mutex over writing retry entries and fetching the
// retry manifest for each worker, and moreover there is no reason all workers need to synchronize
// on one tracker..

use std::collections::HashMap;

use abq_utils::{
    log_assert,
    net_protocol::{
        queue::TestSpec,
        runners::TestId,
        workers::{WorkId, INIT_RUN_NUMBER},
    },
};

#[derive(Default)]
pub(super) struct RetryManifestTracker {
    // Since we need to preserve insertion order of the manifest, keep a hashmap for faster
    // indexing, but an ordered list for the underlying buffer.
    entry_indices: HashMap<WorkId, usize>,
    entries: Vec<TestEntry>,
}

enum Status {
    /// The test has had a non-failing attempt.
    /// Also used to represent the initial state, before the first test suite run attempt result.
    HasNonFailingAttempt,
    /// The test failed on its first attempt, all subsequent attempts up to and including the given
    /// last test run attempt number.
    AlwaysFailedOn { last: u32 },
}

struct TestEntry {
    /// The specification of the test, as it would appear in a flat manifest used for execution.
    spec: TestSpec,
    /// Status of the test entry.
    status: Status,
}

impl RetryManifestTracker {
    /// Insert an item from the manifest, in the order it appeared to be seen from the manifest by
    /// this process.
    ///
    /// Calling [Self::account_failure] before the corresponding manifest item is accounted for is
    /// a bug.
    #[allow(unused)] // for now
    pub fn account_ordered_manifest_entry(&mut self, spec: TestSpec) {
        log_assert!(
            !spec.test_case.has_focus(),
            "manifest entries should have no focus when first constructed"
        );

        let work_id = spec.work_id;
        let entry = TestEntry {
            spec,
            status: Status::HasNonFailingAttempt,
        };

        let index = self.entries.len();
        let old = self.entry_indices.insert(work_id, index);
        log_assert!(old.is_none(), "manifest may not consist of any duplicates");

        self.entries.push(entry);
    }

    #[allow(unused)] // for now
    pub fn account_failure(&mut self, work_id: WorkId, run_number: u32, test_id: TestId) {
        let entry_index = self
            .entry_indices
            .get(&work_id)
            .expect("illegal state - received failure must correspond to manifest entry");
        let entry = &mut self.entries[*entry_index];

        let set_failed = match entry.status {
            Status::HasNonFailingAttempt => {
                // If there were runs after the initial run, where an attempt of this test did not
                // fail, the overall status is that the test did not fail. In that case, don't
                // change the test's status.
                run_number == INIT_RUN_NUMBER
            }
            Status::AlwaysFailedOn { last } => {
                // The test only continued to fail if it failed on the last attempt.
                last + 1 == run_number
            }
        };

        if set_failed {
            entry.status = Status::AlwaysFailedOn { last: run_number };
            entry.spec.test_case.add_test_focus(test_id);
        }
    }

    /// Assemble a subset of the manifest consisting only of entries that failed for the given
    /// test suite run attempt number.
    /// The retrieved manifest is in the same order as the manifest this tracker was constructed
    /// with.
    #[allow(unused)] // for now
    pub fn failing_subset(&mut self, run_number: u32) -> Vec<TestSpec> {
        self.entries
            .iter()
            .filter_map(|entry| match entry.status {
                Status::HasNonFailingAttempt => None,
                Status::AlwaysFailedOn { last } => {
                    debug_assert!(entry.spec.test_case.has_focus());
                    if last == run_number {
                        Some(entry.spec.clone())
                    } else {
                        None
                    }
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod test {
    use abq_run_n_times::n_times;
    use abq_utils::net_protocol::{
        queue::TestSpec,
        runners::TestCase,
        workers::{WorkId, INIT_RUN_NUMBER},
    };
    use abq_with_protocol_version::with_protocol_version;
    use rand::distributions::{Alphanumeric, DistString};

    use super::RetryManifestTracker;

    #[test]
    #[with_protocol_version]
    fn empty_failing_subset_when_no_failures() {
        let mut tracker = RetryManifestTracker::default();

        tracker.account_ordered_manifest_entry(TestSpec {
            test_case: TestCase::new(proto, "test1", Default::default()),
            work_id: WorkId::new(),
        });
        tracker.account_ordered_manifest_entry(TestSpec {
            test_case: TestCase::new(proto, "test2", Default::default()),
            work_id: WorkId::new(),
        });

        assert!(tracker.failing_subset(INIT_RUN_NUMBER).is_empty());
    }

    #[test]
    #[with_protocol_version]
    fn failing_subset_consists_of_accounted_failures() {
        let id1 = WorkId::new();
        let id2 = WorkId::new();
        let id3 = WorkId::new();

        let spec1 = TestSpec {
            test_case: TestCase::new(proto, "test1", Default::default()),
            work_id: id1,
        };
        let spec2 = TestSpec {
            test_case: TestCase::new(proto, "test2", Default::default()),
            work_id: id2,
        };
        let spec3 = TestSpec {
            test_case: TestCase::new(proto, "test3", Default::default()),
            work_id: id3,
        };

        let mut tracker = RetryManifestTracker::default();

        tracker.account_ordered_manifest_entry(spec1.clone());
        tracker.account_ordered_manifest_entry(spec2);
        tracker.account_ordered_manifest_entry(spec3.clone());

        tracker.account_failure(id3, INIT_RUN_NUMBER, "test3".to_string());
        tracker.account_failure(id1, INIT_RUN_NUMBER, "test1".to_string());

        let failing_manifest = tracker.failing_subset(INIT_RUN_NUMBER);
        let expected = vec![
            {
                let mut spec1 = spec1;
                spec1.test_case.add_test_focus("test1".to_string());
                spec1
            },
            {
                let mut spec3 = spec3;
                spec3.test_case.add_test_focus("test3".to_string());
                spec3
            },
        ];
        assert_eq!(failing_manifest, expected);
    }

    #[test]
    #[with_protocol_version]
    fn failing_subset_for_subsequent_attempt_consists_of_accounted_failures_only_for_subsequent_attempt(
    ) {
        let id1 = WorkId::new();
        let id2 = WorkId::new();
        let id3 = WorkId::new();
        let id4 = WorkId::new();
        let id5 = WorkId::new();

        let spec1 = TestSpec {
            test_case: TestCase::new(proto, "test1", Default::default()),
            work_id: id1,
        };
        let spec2 = TestSpec {
            test_case: TestCase::new(proto, "test2", Default::default()),
            work_id: id2,
        };
        let spec3 = TestSpec {
            test_case: TestCase::new(proto, "test3", Default::default()),
            work_id: id3,
        };
        let spec4 = TestSpec {
            test_case: TestCase::new(proto, "test4", Default::default()),
            work_id: id4,
        };
        let spec5 = TestSpec {
            test_case: TestCase::new(proto, "test5", Default::default()),
            work_id: id5,
        };

        let mut tracker = RetryManifestTracker::default();

        tracker.account_ordered_manifest_entry(spec1.clone());
        tracker.account_ordered_manifest_entry(spec2);
        tracker.account_ordered_manifest_entry(spec3.clone());
        tracker.account_ordered_manifest_entry(spec4);
        tracker.account_ordered_manifest_entry(spec5.clone());

        tracker.account_failure(id5, INIT_RUN_NUMBER, "test5".to_string());
        tracker.account_failure(id3, INIT_RUN_NUMBER, "test3".to_string());
        tracker.account_failure(id1, INIT_RUN_NUMBER, "test1".to_string());

        // Add failures on the second run
        tracker.account_failure(id5, INIT_RUN_NUMBER + 1, "test5".to_string());
        tracker.account_failure(id1, INIT_RUN_NUMBER + 1, "test1".to_string());

        let failing_manifest = tracker.failing_subset(INIT_RUN_NUMBER + 1);
        let expected = vec![
            {
                let mut spec1 = spec1;
                spec1.test_case.add_test_focus("test1".to_string());
                spec1
            },
            {
                let mut spec5 = spec5;
                spec5.test_case.add_test_focus("test5".to_string());
                spec5
            },
        ];

        assert_eq!(failing_manifest, expected);
    }

    #[test]
    #[with_protocol_version]
    #[n_times(100)]
    fn preserve_order_in_failing_subset() {
        const N: usize = 250;
        let mut manifest = Vec::with_capacity(N);
        for _ in 0..N {
            let id = WorkId::new();
            let spec = TestSpec {
                test_case: TestCase::new(
                    proto,
                    Alphanumeric.sample_string(&mut rand::thread_rng(), 8),
                    Default::default(),
                ),
                work_id: id,
            };
            manifest.push(spec);
        }

        let mut tracker = RetryManifestTracker::default();

        for spec in manifest.iter() {
            tracker.account_ordered_manifest_entry(spec.clone());
        }

        let mut expected_failing_ids = Vec::with_capacity(N);
        for spec in manifest {
            if rand::random() {
                tracker.account_failure(spec.work_id, INIT_RUN_NUMBER, spec.test_case.id().clone());
                expected_failing_ids.push(spec.work_id);
            }
        }

        let failing_manifest = tracker.failing_subset(INIT_RUN_NUMBER);
        let failing_ids: Vec<_> = failing_manifest.into_iter().map(|e| e.work_id).collect();
        assert_eq!(failing_ids, expected_failing_ids);
    }
}
