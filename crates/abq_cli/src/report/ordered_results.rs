use std::collections::BinaryHeap;

use abq_utils::{
    net_protocol::{queue::AssociatedTestResults, workers::WorkId},
    time::EpochMillis,
};

/// Orders reported [AssociatedTestResults] chronologically.
/// Needed because results are not guaranteed to be reported in the same order that they were
/// evaluated (e.g. a test's retry may be reported before the initial attempt).
pub struct OrderedAssociatedResults {
    results: BinaryHeap<AssociatedTestResultsWrapper>,
}

impl OrderedAssociatedResults {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            results: BinaryHeap::with_capacity(capacity),
        }
    }

    pub fn extend<I>(&mut self, results: I)
    where
        I: IntoIterator<Item = AssociatedTestResults>,
        I::IntoIter: ExactSizeIterator,
    {
        let results = results.into_iter();
        self.results.reserve(results.len());
        self.results
            .extend(results.map(AssociatedTestResultsWrapper));
    }

    pub fn into_iter(self) -> impl Iterator<Item = AssociatedTestResults> {
        self.results.into_sorted_vec().into_iter().map(|w| w.0)
    }
}

type Key = (WorkId, Option<EpochMillis>, u32);

#[repr(transparent)]
struct AssociatedTestResultsWrapper(AssociatedTestResults);

impl AssociatedTestResultsWrapper {
    // Order by test name, then timestamp, then run number.
    #[inline]
    fn key_triple(&self) -> Key {
        (
            self.0.work_id,
            self.0.results.first().map(|r| r.timestamp),
            self.0.run_number,
        )
    }
}

impl PartialEq for AssociatedTestResultsWrapper {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.key_triple() == other.key_triple()
    }
}

impl Eq for AssociatedTestResultsWrapper {}

impl PartialOrd for AssociatedTestResultsWrapper {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AssociatedTestResultsWrapper {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key_triple().cmp(&other.key_triple())
    }
}

#[cfg(test)]
mod test {
    use abq_test_utils::{wid, AssociatedTestResultsBuilder, TestResultBuilder};
    use abq_utils::{
        net_protocol::{runners::Status, workers::INIT_RUN_NUMBER},
        time::EpochMillis,
    };

    use super::OrderedAssociatedResults;

    #[test]
    fn correct_sort() {
        let expected_sort = || {
            vec![
                AssociatedTestResultsBuilder::new(
                    wid(1),
                    INIT_RUN_NUMBER,
                    [TestResultBuilder::new("t1", Status::Success)
                        .timestamp(EpochMillis::from_millis(1))],
                )
                .build(),
                AssociatedTestResultsBuilder::new(
                    wid(1),
                    INIT_RUN_NUMBER + 1,
                    [TestResultBuilder::new("t1", Status::Success)
                        .timestamp(EpochMillis::from_millis(1))],
                )
                .build(),
                //
                AssociatedTestResultsBuilder::new(
                    wid(1),
                    INIT_RUN_NUMBER,
                    [TestResultBuilder::new("t1", Status::Success)
                        .timestamp(EpochMillis::from_millis(2))],
                )
                .build(),
                AssociatedTestResultsBuilder::new(
                    wid(1),
                    INIT_RUN_NUMBER + 1,
                    [TestResultBuilder::new("t1", Status::Success)
                        .timestamp(EpochMillis::from_millis(2))],
                )
                .build(),
                AssociatedTestResultsBuilder::new(
                    wid(1),
                    INIT_RUN_NUMBER + 2,
                    [TestResultBuilder::new("t1", Status::Success)
                        .timestamp(EpochMillis::from_millis(2))],
                )
                .build(),
                //
                AssociatedTestResultsBuilder::new(
                    wid(2),
                    INIT_RUN_NUMBER,
                    [TestResultBuilder::new("t2", Status::Success)
                        .timestamp(EpochMillis::from_millis(1))],
                )
                .build(),
                AssociatedTestResultsBuilder::new(
                    wid(2),
                    INIT_RUN_NUMBER + 1,
                    [TestResultBuilder::new("t2", Status::Success)
                        .timestamp(EpochMillis::from_millis(1))],
                )
                .build(),
                //
                AssociatedTestResultsBuilder::new(
                    wid(2),
                    INIT_RUN_NUMBER,
                    [TestResultBuilder::new("t2", Status::Success)
                        .timestamp(EpochMillis::from_millis(2))],
                )
                .build(),
                AssociatedTestResultsBuilder::new(
                    wid(2),
                    INIT_RUN_NUMBER + 1,
                    [TestResultBuilder::new("t2", Status::Success)
                        .timestamp(EpochMillis::from_millis(2))],
                )
                .build(),
                AssociatedTestResultsBuilder::new(
                    wid(2),
                    INIT_RUN_NUMBER + 2,
                    [TestResultBuilder::new("t2", Status::Success)
                        .timestamp(EpochMillis::from_millis(2))],
                )
                .build(),
            ]
        };

        use rand::seq::SliceRandom;
        use rand::thread_rng;

        for _ in 0..1000 {
            let mut results = expected_sort();
            results.shuffle(&mut thread_rng());

            let mut orderer = OrderedAssociatedResults::with_capacity(results.len());
            orderer.extend(results);
            let ordered = orderer.into_iter().collect::<Vec<_>>();

            let expected = expected_sort();
            assert_eq!(ordered, expected);
        }
    }
}
