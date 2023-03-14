//! Persistence of test results for test runs.

mod fs;
mod in_memory;

pub use fs::FilesystemPersistor;
pub use in_memory::InMemoryPersistor;

use std::sync::{atomic::AtomicU64, Arc};

use abq_utils::{
    atomic,
    error::LocatedError,
    net_protocol::{
        queue::AssociatedTestResults,
        results::{OpaqueLazyAssociatedTestResults, ResultsLine, Summary},
        workers::RunId,
    },
};
use async_trait::async_trait;

type ArcResult<T> = std::result::Result<T, Arc<LocatedError>>;

#[async_trait]
trait PersistResults: Send + Sync {
    /// Dumps a summary line.
    async fn dump(&self, run_id: &RunId, results: ResultsLine) -> ArcResult<()>;

    /// Load a set of test results as [OpaqueLazyAssociatedTestResults].
    async fn get_results(&self, run_id: &RunId) -> ArcResult<OpaqueLazyAssociatedTestResults>;

    fn boxed_clone(&self) -> Box<dyn PersistResults>;
}

pub struct SharedPersistResults(Box<dyn PersistResults>);

impl Clone for SharedPersistResults {
    fn clone(&self) -> Self {
        Self(self.0.boxed_clone())
    }
}

/// A cell that tracks the progress of persisting test results for a given run.
/// The cell is also the interface for actually executing persistence and retrieval operations.
///
/// Results persistence tracks how many persistence operations are in progress. Retrieval of
/// results is permitted only when there are no active persistence operations observed at the
/// moment.
///
/// Persistence and retrieval of results ensures linearizability, but does not guarantee a total
/// and sequential order. In particular, retrieval of results will always retrieve all results
/// whose persistence was enqueued prior to the issue of retrieval. However, during the processing
/// of retrieval, additional results' persistence may be enqueued; it is not well-defined whether
/// those new results will be retrieved or not.
///
/// It is intended that results for a run are attempted to be retrieved only when there are no
/// active workers seen for a run. The hypothesis is that in the happy case, at this point all
/// pending results are either in the process of being persisted, or already persisted, and
/// retrieval can happen shortly or immediately. This intention does not carry for out-of-process
/// retries, but the addition of those results may only be supplementary.
#[derive(Clone, Debug)]
pub struct ResultsPersistedCell(Arc<CellInner>);

#[derive(Debug)]
struct CellInner {
    run_id: RunId,
    processing: AtomicU64,
}

impl ResultsPersistedCell {
    pub fn new(run_id: RunId) -> Self {
        Self(Arc::new(CellInner {
            run_id,
            processing: AtomicU64::new(0),
        }))
    }

    #[inline]
    pub fn run_id(&self) -> &RunId {
        &self.0.run_id
    }

    /// Creates a persistence job, recording the pending set of results to persist.
    #[inline]
    pub fn build_persist_results_plan<'a>(
        &'a self,
        persistence: &'a SharedPersistResults,
        results: Vec<AssociatedTestResults>,
    ) -> PersistencePlan<'a> {
        self.0.processing.fetch_add(1, atomic::ORDERING);

        PersistencePlan {
            persist_results: &*persistence.0,
            cell: &self.0,
            line: ResultsLine::Results(results),
        }
    }

    /// Creates a persistence job, recording the pending summary to persist.
    #[inline]
    pub fn build_persist_summary_plan<'a>(
        &'a self,
        persistence: &'a SharedPersistResults,
        summary: Summary,
    ) -> PersistencePlan<'a> {
        self.0.processing.fetch_add(1, atomic::ORDERING);

        PersistencePlan {
            persist_results: &*persistence.0,
            cell: &self.0,
            line: ResultsLine::Summary(summary),
        }
    }

    /// Attempts to retrieve a set of test results.
    /// If there are persistence jobs pending, returns [None].
    pub async fn retrieve(
        &self,
        persistence: &SharedPersistResults,
    ) -> Option<ArcResult<OpaqueLazyAssociatedTestResults>> {
        if self.0.processing.load(atomic::ORDERING) != 0 {
            return None;
        }
        Some(persistence.0.get_results(&self.0.run_id).await)
    }
}

pub struct PersistencePlan<'a> {
    persist_results: &'a dyn PersistResults,
    cell: &'a CellInner,
    line: ResultsLine,
}

impl<'a> PersistencePlan<'a> {
    pub async fn execute(self) -> ArcResult<()> {
        let result = self
            .persist_results
            .dump(&self.cell.run_id, self.line)
            .await;
        self.cell.processing.fetch_sub(1, atomic::ORDERING);
        result
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use abq_run_n_times::n_times;
    use abq_test_utils::wid;
    use abq_utils::{
        atomic,
        net_protocol::{
            queue::AssociatedTestResults, results::ResultsLine, runners::TestResult, workers::RunId,
        },
    };

    use super::{fs::FilesystemPersistor, ResultsPersistedCell};

    #[tokio::test]
    async fn retrieve_is_none_while_pending() {
        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1);

        let cell = ResultsPersistedCell::new(RunId::unique());
        cell.0.processing.fetch_add(1, atomic::ORDERING);

        assert!(cell.retrieve(&persistence).await.is_none());
    }

    #[tokio::test]
    async fn retrieve_is_some_when_no_pending() {
        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1);

        let cell = ResultsPersistedCell::new(RunId::unique());

        let retrieved = cell.retrieve(&persistence).await.unwrap().unwrap();
        let results = retrieved.decode().unwrap();
        assert!(results.is_empty());
    }

    #[n_times(10_000)]
    #[tokio::test]
    async fn retrieve_is_linearized() {
        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1);

        let cell = ResultsPersistedCell::new(RunId::unique());

        let results1 = vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ];
        let results2 = vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ];

        let plan = cell.build_persist_results_plan(&persistence, results1.clone());
        plan.execute().await.unwrap();

        // Due to our listed constraints, retrieval may finish before persistence of results2 does.
        // That's okay. But the retrieved must definitely include at least results1.
        let retrieve_task = {
            async {
                loop {
                    match cell.retrieve(&persistence).await {
                        None => tokio::time::sleep(Duration::from_micros(1)).await,
                        Some(results) => break results,
                    }
                }
            }
        };
        let persist_task = async {
            let plan = cell.build_persist_results_plan(&persistence, results2.clone());
            plan.execute().await.unwrap();
        };
        let ((), retrieve_result) = tokio::join!(persist_task, retrieve_task);

        let retrieved = retrieve_result.unwrap();
        let results = retrieved.decode().unwrap();

        use ResultsLine::Results;
        match results.len() {
            1 => assert_eq!(results, vec![Results(results1)]),
            2 => assert_eq!(results, vec![Results(results1), Results(results2)]),
            _ => unreachable!("{results:?}"),
        }
    }
}
