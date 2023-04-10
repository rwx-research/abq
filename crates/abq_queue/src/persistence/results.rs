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
pub trait PersistResults: Send + Sync {
    /// Dumps a summary line.
    async fn dump(&self, run_id: &RunId, results: ResultsLine) -> ArcResult<()>;

    /// Dumps the persisted results to a remote, if any is configured.
    async fn dump_to_remote(&self, run_id: &RunId) -> ArcResult<()>;

    /// Load a set of test results as [OpaqueLazyAssociatedTestResults].
    async fn get_results(&self, run_id: &RunId) -> ArcResult<OpaqueLazyAssociatedTestResults>;

    fn boxed_clone(&self) -> Box<dyn PersistResults>;
}

pub struct SharedPersistResults(Box<dyn PersistResults>);

impl SharedPersistResults {
    pub fn new<T: PersistResults + 'static>(inner: T) -> Self {
        Self(Box::new(inner))
    }
}

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

/// Whether the results persistence is eligible for persistence to the remote, if any.
///
/// This is a hint as to whether or not the remote can be hit. The remote will only actually be
/// persisted to if there are no additional pending local persistence operations after a local
/// persistence operation completes.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum EligibleForRemoteDump {
    Yes,
    No,
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
        eligible_for_remote_dump: EligibleForRemoteDump,
    ) -> PersistencePlan<'a> {
        self.0.processing.fetch_add(1, atomic::ORDERING);

        PersistencePlan {
            persist_results: &*persistence.0,
            cell: &self.0,
            line: ResultsLine::Results(results),
            eligible_for_remote_dump,
        }
    }

    /// Creates a persistence job, recording the pending summary to persist.
    #[inline]
    pub fn build_persist_summary_plan<'a>(
        &'a self,
        persistence: &'a SharedPersistResults,
        summary: Summary,
        eligible_for_remote_dump: EligibleForRemoteDump,
    ) -> PersistencePlan<'a> {
        self.0.processing.fetch_add(1, atomic::ORDERING);

        PersistencePlan {
            persist_results: &*persistence.0,
            cell: &self.0,
            line: ResultsLine::Summary(summary),
            eligible_for_remote_dump,
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
    eligible_for_remote_dump: EligibleForRemoteDump,
}

impl<'a> PersistencePlan<'a> {
    pub async fn execute(self) -> ArcResult<()> {
        let result = self
            .persist_results
            .dump(&self.cell.run_id, self.line)
            .await;
        let additional_persistence_tasks = self.cell.processing.fetch_sub(1, atomic::ORDERING);

        if additional_persistence_tasks == 1
            && self.eligible_for_remote_dump == EligibleForRemoteDump::Yes
        {
            // The last local persistence task was just completed by us; we can now dump to the
            // remote.
            self.persist_results
                .dump_to_remote(&self.cell.run_id)
                .await?;
        }

        result
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{atomic::AtomicBool, Arc},
        time::Duration,
    };

    use abq_run_n_times::n_times;
    use abq_test_utils::wid;
    use abq_utils::{
        atomic,
        net_protocol::{
            queue::AssociatedTestResults, results::ResultsLine, runners::TestResult, workers::RunId,
        },
    };

    use crate::persistence::{
        remote::{self, fake_error, fake_unreachable, PersistenceKind},
        results::EligibleForRemoteDump,
    };

    use super::{fs::FilesystemPersistor, ResultsPersistedCell};

    #[tokio::test]
    async fn retrieve_is_none_while_pending() {
        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1, remote::NoopPersister);

        let cell = ResultsPersistedCell::new(RunId::unique());
        cell.0.processing.fetch_add(1, atomic::ORDERING);

        assert!(cell.retrieve(&persistence).await.is_none());
    }

    #[tokio::test]
    async fn retrieve_is_some_when_no_pending() {
        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(
            tempdir.path(),
            1,
            remote::FakePersister::new(fake_unreachable, |_, _, _| async {
                // Load nothing new into the file
                Ok(())
            }),
        );

        let cell = ResultsPersistedCell::new(RunId::unique());

        let retrieved = cell.retrieve(&persistence).await.unwrap().unwrap();
        let results = retrieved.decode().unwrap();
        assert!(results.is_empty());
    }

    #[n_times(1_000)]
    #[tokio::test]
    async fn retrieve_is_linearized() {
        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1, remote::NoopPersister);

        let cell = ResultsPersistedCell::new(RunId::unique());

        let results1 = vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ];
        let results2 = vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ];

        let plan = cell.build_persist_results_plan(
            &persistence,
            results1.clone(),
            EligibleForRemoteDump::No,
        );
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
            let plan = cell.build_persist_results_plan(
                &persistence,
                results2.clone(),
                EligibleForRemoteDump::No,
            );
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

    #[tokio::test]
    async fn execute_not_eligible_for_persistence_is_last() {
        let remote = remote::FakePersister::new(fake_unreachable, fake_error);

        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1, remote);

        let cell = ResultsPersistedCell::new(RunId::unique());

        let results = vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ];

        let plan = cell.build_persist_results_plan(
            &persistence,
            results.clone(),
            EligibleForRemoteDump::No,
        );
        plan.execute().await.unwrap();

        assert!(cell.0.processing.load(atomic::ORDERING) == 0);
    }

    #[tokio::test]
    async fn execute_not_eligible_for_persistence_is_not_last() {
        let remote = remote::FakePersister::new(fake_unreachable, fake_error);

        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1, remote);

        let cell = ResultsPersistedCell::new(RunId::unique());

        cell.0.processing.fetch_add(1, atomic::ORDERING);

        let results = vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ];

        let plan = cell.build_persist_results_plan(
            &persistence,
            results.clone(),
            EligibleForRemoteDump::No,
        );
        plan.execute().await.unwrap();

        assert!(cell.0.processing.load(atomic::ORDERING) == 1);
    }

    #[tokio::test]
    async fn execute_eligible_for_persistence_is_not_last() {
        let remote = remote::FakePersister::new(fake_unreachable, fake_error);

        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1, remote);

        let cell = ResultsPersistedCell::new(RunId::unique());

        cell.0.processing.fetch_add(1, atomic::ORDERING);

        let results = vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ];

        let plan =
            cell.build_persist_results_plan(&persistence, results, EligibleForRemoteDump::Yes);
        plan.execute().await.unwrap();

        assert!(cell.0.processing.load(atomic::ORDERING) == 1);
    }

    #[tokio::test]
    async fn execute_eligible_for_persistence_is_last() {
        let run_id = RunId("test-run-id".to_string());

        let results = vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ];

        let set_remote = Arc::new(AtomicBool::new(false));

        let remote = remote::FakePersister::new(
            {
                let results = ResultsLine::Results(results.clone());
                let set_remote = set_remote.clone();
                move |kind, run_id, path| {
                    let results = results.clone();
                    let set_remote = set_remote.clone();
                    async move {
                        assert_eq!(kind, PersistenceKind::Results);
                        assert_eq!(run_id.0, "test-run-id");
                        let data = tokio::fs::read_to_string(path).await.unwrap();
                        let read: ResultsLine = serde_json::from_str(&data).unwrap();
                        assert_eq!(read, results);

                        set_remote.store(true, atomic::ORDERING);

                        Ok(())
                    }
                }
            },
            fake_error,
        );

        let tempdir = tempfile::tempdir().unwrap();
        let persistence = FilesystemPersistor::new_shared(tempdir.path(), 1, remote);

        let cell = ResultsPersistedCell::new(run_id);

        let plan =
            cell.build_persist_results_plan(&persistence, results, EligibleForRemoteDump::Yes);
        plan.execute().await.unwrap();

        assert!(cell.0.processing.load(atomic::ORDERING) == 0);
        assert!(set_remote.load(atomic::ORDERING));
    }
}
