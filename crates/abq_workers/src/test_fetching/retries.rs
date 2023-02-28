#![allow(unused)] // for now

use std::collections::HashMap;

use abq_utils::{
    log_assert,
    net_protocol::{
        queue::{AssociatedTestResults, TestSpec},
        runners::TestId,
        workers::{NextWork, WorkId, WorkerTest, INIT_RUN_NUMBER},
    },
};

struct RetryManifestTracker {
    // Since we need to preserve insertion order of the manifest, keep a hashmap for faster
    // indexing, but an ordered list for the underlying buffer.
    entry_indices: HashMap<WorkId, usize>,
    entries: Vec<TestEntry>,

    max_run_number: u32,
    current_run_number: u32,

    assembled_state: AssembledState,
}

#[derive(Debug)]
enum Status {
    /// The test has had a non-failing attempt.
    /// Also used to represent the initial state, before the first test suite run attempt result.
    HasNonFailingAttempt,
    /// The test failed on its first attempt, all subsequent attempts up to and including the given
    /// last test run attempt number.
    AlwaysFailedOn { last: u32 },
}

#[derive(Debug)]
struct TestEntry {
    /// The specification of the test, as it would appear in a flat manifest used for execution.
    spec: TestSpec,
    /// Status of the test entry.
    status: Status,
}

/// The assembled retry manifest can be in one of the following states.
enum AssembledState {
    /// The retry manifest has not started populating any entries for the retry manifest.
    NotStarted,
    /// The retry manifest started hydrating entries of the manifest, but has not wholly
    /// hydrated it yet. It is waiting for an end-of-test marker to delimit the size of the manifest.
    WaitingForEndOfHydration { last_seen_work_name: WorkId },
    /// The retry manifest has been fully hydrated with entries of the manifest.
    /// We are now tracking whether the current attempt's last result has been seen.
    /// A retry manifest for an attempts can only be fetched once the results for all tests in an
    /// attempt have been seen.
    Hydrated {
        last_work_name: WorkId,
        processed_last_work_result: bool,
    },
}

impl AssembledState {
    fn note_seen_manifest_item(&mut self, work_id: WorkId) {
        use AssembledState::*;
        let next_state = match self {
            NotStarted | WaitingForEndOfHydration { .. } => WaitingForEndOfHydration {
                last_seen_work_name: work_id,
            },
            Hydrated { .. } => {
                unreachable!("cannot see new manifest items after hydration is complete")
            }
        };
        *self = next_state;
    }

    fn note_end_of_manifest(&mut self) {
        let next_state = match self {
            AssembledState::NotStarted => unreachable!(
                "empty manifests should never be reachable here, as they should exit early"
            ),
            AssembledState::WaitingForEndOfHydration {
                last_seen_work_name,
            } => AssembledState::Hydrated {
                last_work_name: *last_seen_work_name,
                processed_last_work_result: false,
            },
            AssembledState::Hydrated { .. } => {
                unreachable!("cannot reach end of manifest again after hydration was completed")
            }
        };
        *self = next_state;
    }

    fn note_accounted_result(&mut self, work_id: WorkId) {
        match self {
            AssembledState::NotStarted => unreachable!(
                "should never start accounting results before any manifest entry was hydrated"
            ),
            AssembledState::WaitingForEndOfHydration { .. } => {
                // do nothing, we are incrementally filling results in we're populating the
                // manifest.
            }
            AssembledState::Hydrated {
                last_work_name,
                processed_last_work_result,
            } => {
                let just_accounted_last_work_result = work_id == *last_work_name;
                debug_assert!(
                    !(just_accounted_last_work_result && *processed_last_work_result),
                    "must not see last work result without resetting for a next attempt"
                );
                *processed_last_work_result = just_accounted_last_work_result;
            }
        }
    }
}

impl RetryManifestTracker {
    pub fn new(max_run_number: u32) -> Self {
        Self {
            entry_indices: Default::default(),
            entries: Default::default(),
            max_run_number,
            current_run_number: INIT_RUN_NUMBER,
            assembled_state: AssembledState::NotStarted,
        }
    }

    /// Hydrates the retry manifest tracker with a slice of items from the manifest. The slice
    /// order is preserved, and is expected to be the order provided by the queue.
    ///
    /// Calling [Self::account_results] before the corresponding manifest entries are accounted is
    /// a bug.
    pub fn hydrate_ordered_manifest_slice(&mut self, slice: impl IntoIterator<Item = NextWork>) {
        for item in slice {
            match item {
                NextWork::Work(WorkerTest { spec, run_number }) => {
                    debug_assert_eq!(self.current_run_number, run_number);
                    self.hydrate_ordered_manifest_item(spec);
                }
                NextWork::EndOfWork => {
                    self.assembled_state.note_end_of_manifest();
                }
            }
        }
    }

    #[inline]
    fn hydrate_ordered_manifest_item(&mut self, spec: TestSpec) {
        debug_assert!(
            !spec.test_case.has_focus(),
            "manifest entries should have no focus when first constructed"
        );

        self.assembled_state.note_seen_manifest_item(spec.work_id);

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

    pub fn account_results<'a>(
        &mut self,
        results: impl IntoIterator<Item = &'a AssociatedTestResults>,
    ) {
        for AssociatedTestResults {
            work_id,
            run_number,
            results,
            ..
        } in results
        {
            debug_assert_eq!(*run_number, self.current_run_number);
            for result in results.iter() {
                if result.status.is_fail_like() {
                    self.account_failure(*work_id, *run_number, result.id.clone());
                }
            }
            self.assembled_state.note_accounted_result(*work_id);
        }
    }

    #[inline]
    fn account_failure(&mut self, work_id: WorkId, run_number: u32, test_id: TestId) {
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

    /// Assemble a subset of the manifest consisting only of entries that failed for the current
    /// test suite run attempt number.
    /// The retrieved manifest is in the same order as the manifest this tracker was constructed
    /// with.
    ///
    /// Returns `None` if the last test result for the current run has not yet been seen, in which
    /// case the assembly must yield until the last result is known.
    ///
    /// If all attempts have been exhausted, a list of [NextWork::EndOfWork] markers is returned.
    pub fn try_assemble_retry_manifest(&mut self) -> Option<Vec<NextWork>> {
        debug_assert!(self.current_run_number <= self.max_run_number);

        if self.current_run_number == self.max_run_number {
            return Some(vec![NextWork::EndOfWork]);
        }

        match &mut self.assembled_state {
            AssembledState::NotStarted | AssembledState::WaitingForEndOfHydration { .. } => None,
            AssembledState::Hydrated {
                last_work_name: _,
                processed_last_work_result,
            } => {
                if !*processed_last_work_result {
                    return None;
                }

                let current_run_number = self.current_run_number;
                let next_run_number = current_run_number + 1;
                self.current_run_number = next_run_number;

                *processed_last_work_result = false;

                let failing_subset: Vec<_> = self
                    .failing_subset(current_run_number)
                    .map(|spec| {
                        NextWork::Work(WorkerTest {
                            spec,
                            run_number: next_run_number,
                        })
                    })
                    .collect();

                Some(failing_subset)
            }
        }
    }

    fn failing_subset(&self, run_number: u32) -> impl Iterator<Item = TestSpec> + '_ {
        self.entries
            .iter()
            .filter_map(move |entry| match entry.status {
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
    }
}

#[cfg(test)]
mod test {
    use abq_run_n_times::n_times;
    use abq_utils::net_protocol::{
        queue::{AssociatedTestResults, TestSpec},
        runners::{CapturedOutput, Status, Test, TestCase, TestId, TestResult},
        workers::{NextWork, WorkId, WorkerTest, INIT_RUN_NUMBER},
    };
    use abq_with_protocol_version::with_protocol_version;
    use rand::distributions::{Alphanumeric, DistString};

    use super::RetryManifestTracker;

    #[test]
    #[with_protocol_version]
    fn empty_failing_subset_when_no_failures() {
        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 1);

        tracker.hydrate_ordered_manifest_item(TestSpec {
            test_case: TestCase::new(proto, "test1", Default::default()),
            work_id: WorkId::new(),
        });
        tracker.hydrate_ordered_manifest_item(TestSpec {
            test_case: TestCase::new(proto, "test2", Default::default()),
            work_id: WorkId::new(),
        });

        assert!(tracker.failing_subset(INIT_RUN_NUMBER).next().is_none());
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

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 1);

        tracker.hydrate_ordered_manifest_item(spec1.clone());
        tracker.hydrate_ordered_manifest_item(spec2);
        tracker.hydrate_ordered_manifest_item(spec3.clone());

        tracker.account_failure(id3, INIT_RUN_NUMBER, "test3".to_string());
        tracker.account_failure(id1, INIT_RUN_NUMBER, "test1".to_string());

        let failing_manifest = tracker.failing_subset(INIT_RUN_NUMBER).collect::<Vec<_>>();
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

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 1);

        tracker.hydrate_ordered_manifest_item(spec1.clone());
        tracker.hydrate_ordered_manifest_item(spec2);
        tracker.hydrate_ordered_manifest_item(spec3.clone());
        tracker.hydrate_ordered_manifest_item(spec4);
        tracker.hydrate_ordered_manifest_item(spec5.clone());

        tracker.account_failure(id5, INIT_RUN_NUMBER, "test5".to_string());
        tracker.account_failure(id3, INIT_RUN_NUMBER, "test3".to_string());
        tracker.account_failure(id1, INIT_RUN_NUMBER, "test1".to_string());

        // Add failures on the second run
        tracker.account_failure(id5, INIT_RUN_NUMBER + 1, "test5".to_string());
        tracker.account_failure(id1, INIT_RUN_NUMBER + 1, "test1".to_string());

        let failing_manifest: Vec<_> = tracker.failing_subset(INIT_RUN_NUMBER + 1).collect();
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

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 1);

        for spec in manifest.iter() {
            tracker.hydrate_ordered_manifest_item(spec.clone());
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

    #[test]
    fn fetching_retry_manifest_before_hydration_starts_returns_nothing() {
        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 1);
        assert_eq!(tracker.try_assemble_retry_manifest(), None);
    }

    const SUCCESS: Status = Status::Success;
    const FAILURE: Status = Status::Failure {
        exception: None,
        backtrace: None,
    };

    fn result(test_id: impl Into<TestId>, status: Status) -> TestResult {
        let mut result = TestResult::fake();
        result.result.id = test_id.into();
        result.result.status = status;
        result
    }

    fn with_focus(test_spec: impl Into<TestSpec>, focus: impl Into<TestId>) -> TestSpec {
        let mut test_spec = test_spec.into();
        test_spec.test_case.add_test_focus(focus.into());
        test_spec
    }

    fn associated_results(
        work_id: WorkId,
        run_number: u32,
        results: impl IntoIterator<Item = TestResult>,
    ) -> AssociatedTestResults {
        AssociatedTestResults {
            work_id,
            run_number,
            results: results.into_iter().collect(),
            before_any_test: CapturedOutput::empty(),
            after_all_tests: None,
        }
    }

    #[test]
    #[with_protocol_version]
    fn fetching_retry_manifest_before_hydration_completes_returns_nothing() {
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

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 1);

        tracker.hydrate_ordered_manifest_slice([
            NextWork::Work(WorkerTest::new(spec1.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec2.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec3.clone(), INIT_RUN_NUMBER)),
        ]);

        tracker.account_results([
            &associated_results(id1, INIT_RUN_NUMBER, [result("test1", FAILURE)]),
            &associated_results(id2, INIT_RUN_NUMBER, [result("test2", FAILURE)]),
            &associated_results(id3, INIT_RUN_NUMBER, [result("test3", FAILURE)]),
        ]);

        assert!(tracker.try_assemble_retry_manifest().is_none());
    }

    #[test]
    #[with_protocol_version]
    fn fetching_retry_manifest_when_hydrating_but_before_final_result_returns_nothing() {
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

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 1);

        tracker.hydrate_ordered_manifest_slice([
            NextWork::Work(WorkerTest::new(spec1.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec2.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec3.clone(), INIT_RUN_NUMBER)),
            NextWork::EndOfWork,
        ]);

        tracker.account_results([
            &associated_results(id1, INIT_RUN_NUMBER, [result("test1", FAILURE)]),
            &associated_results(id2, INIT_RUN_NUMBER, [result("test2", FAILURE)]),
        ]);

        assert!(tracker.try_assemble_retry_manifest().is_none());
    }

    #[test]
    #[with_protocol_version]
    fn fetching_retry_manifest_after_hydrated_and_last_result() {
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

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 2);

        tracker.hydrate_ordered_manifest_slice([
            NextWork::Work(WorkerTest::new(spec1.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec2.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec3.clone(), INIT_RUN_NUMBER)),
            NextWork::EndOfWork,
        ]);

        tracker.account_results([
            &associated_results(id1, INIT_RUN_NUMBER, [result("test1", FAILURE)]),
            &associated_results(id2, INIT_RUN_NUMBER, [result("test2", FAILURE)]),
            &associated_results(id3, INIT_RUN_NUMBER, [result("test3", FAILURE)]),
        ]);

        let manifest = tracker.try_assemble_retry_manifest();
        assert!(manifest.is_some());

        let manifest = manifest.unwrap();
        assert_eq!(manifest.len(), 3);

        assert_eq!(
            manifest[0],
            NextWork::Work(WorkerTest::new(
                with_focus(spec1.clone(), "test1"),
                INIT_RUN_NUMBER + 1
            ))
        );
        assert_eq!(
            manifest[1],
            NextWork::Work(WorkerTest::new(
                with_focus(spec2.clone(), "test2"),
                INIT_RUN_NUMBER + 1
            ))
        );
        assert_eq!(
            manifest[2],
            NextWork::Work(WorkerTest::new(
                with_focus(spec3.clone(), "test3"),
                INIT_RUN_NUMBER + 1
            ))
        );

        assert_eq!(tracker.current_run_number, INIT_RUN_NUMBER + 1);

        // Getting the retry manifest again should return nothing, since we should now be waiting
        // for the next state.
        assert!(tracker.try_assemble_retry_manifest().is_none());
    }

    #[test]
    #[with_protocol_version]
    fn fetching_retry_manifest_after_hydrated_and_last_result_is_not_failure() {
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

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 2);

        tracker.hydrate_ordered_manifest_slice([
            NextWork::Work(WorkerTest::new(spec1.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec2.clone(), INIT_RUN_NUMBER)),
            NextWork::Work(WorkerTest::new(spec3.clone(), INIT_RUN_NUMBER)),
            NextWork::EndOfWork,
        ]);

        tracker.account_results([
            &associated_results(id1, INIT_RUN_NUMBER, [result("test1", FAILURE)]),
            &associated_results(id2, INIT_RUN_NUMBER, [result("test2", SUCCESS)]),
            &associated_results(id3, INIT_RUN_NUMBER, [result("test3", SUCCESS)]),
        ]);

        let manifest = tracker.try_assemble_retry_manifest();
        assert!(manifest.is_some());

        let manifest = manifest.unwrap();
        assert_eq!(manifest.len(), 1);

        assert_eq!(
            manifest[0],
            NextWork::Work(WorkerTest::new(
                with_focus(spec1.clone(), "test1"),
                INIT_RUN_NUMBER + 1
            ))
        );

        assert_eq!(tracker.current_run_number, INIT_RUN_NUMBER + 1);

        // Getting the retry manifest again should return nothing, since we should now be waiting
        // for the next state.
        assert_eq!(tracker.try_assemble_retry_manifest(), None);
    }

    #[test]
    #[with_protocol_version]
    fn fetching_retry_manifest_is_empty_after_all_retries_exhausted() {
        let id1 = WorkId::new();
        let id2 = WorkId::new();
        let id3 = WorkId::new();

        let spec1 = TestSpec {
            test_case: TestCase::new(proto, "test1", Default::default()),
            work_id: id1,
        };

        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER + 2);

        tracker.hydrate_ordered_manifest_slice([
            NextWork::Work(WorkerTest::new(spec1.clone(), INIT_RUN_NUMBER)),
            NextWork::EndOfWork,
        ]);

        // First retry
        {
            tracker.account_results([&associated_results(
                id1,
                INIT_RUN_NUMBER,
                [result("test1", FAILURE)],
            )]);

            let manifest = tracker.try_assemble_retry_manifest();
            assert!(manifest.is_some());

            let manifest = manifest.unwrap();
            assert_eq!(manifest.len(), 1);

            assert_eq!(
                manifest[0],
                NextWork::Work(WorkerTest::new(
                    with_focus(spec1.clone(), "test1"),
                    INIT_RUN_NUMBER + 1
                ))
            );

            assert_eq!(tracker.current_run_number, INIT_RUN_NUMBER + 1);
            assert!(
                tracker.try_assemble_retry_manifest().is_none(),
                "should be waiting for next attempt results"
            );
        }

        // Second retry
        {
            tracker.account_results([&associated_results(
                id1,
                INIT_RUN_NUMBER + 1,
                [result("test1", FAILURE)],
            )]);

            let manifest = tracker.try_assemble_retry_manifest();
            assert!(manifest.is_some());

            let manifest = manifest.unwrap();
            assert_eq!(manifest.len(), 1);

            assert_eq!(
                manifest[0],
                NextWork::Work(WorkerTest::new(
                    with_focus(spec1.clone(), "test1"),
                    INIT_RUN_NUMBER + 2
                ))
            );

            assert_eq!(tracker.current_run_number, INIT_RUN_NUMBER + 2);
        }

        // Third retry - should be done
        {
            tracker.account_results([&associated_results(
                id1,
                INIT_RUN_NUMBER + 2,
                [result("test1", FAILURE)],
            )]);

            for _ in 0..2 {
                // No matter how many times we request the retry manifest now, we should get
                // end-of-work.
                let manifest = tracker.try_assemble_retry_manifest();
                assert!(manifest.is_some());

                let manifest = manifest.unwrap();
                assert_eq!(manifest.len(), 1);

                assert_eq!(manifest[0], NextWork::EndOfWork);

                assert_eq!(tracker.current_run_number, INIT_RUN_NUMBER + 2);
            }
        }
    }

    #[test]
    #[with_protocol_version]
    fn fetching_retry_manifest_with_no_retry_attempts() {
        let mut tracker = RetryManifestTracker::new(INIT_RUN_NUMBER);
        for _ in 0..2 {
            // No matter how many times we request the retry manifest, we should get end-of-work.
            let manifest = tracker.try_assemble_retry_manifest();
            assert!(manifest.is_some());

            let manifest = manifest.unwrap();
            assert_eq!(manifest.len(), 1);

            assert_eq!(manifest[0], NextWork::EndOfWork);

            assert_eq!(tracker.current_run_number, INIT_RUN_NUMBER);
        }
    }
}
