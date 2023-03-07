use std::{path::PathBuf, sync::Arc};

use abq_utils::{
    error::ResultLocation,
    here,
    net_protocol::{
        results::{OpaqueLazyAssociatedTestResults, ResultsLine},
        workers::RunId,
    },
};
use async_trait::async_trait;
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use super::{ArcResult, PersistResults, SharedPersistResults};

/// A concurrent LRU cache of open file descriptors.
#[derive(Clone)]
struct FdCache(moka::future::Cache<PathBuf, Arc<Mutex<File>>>);

impl FdCache {
    fn new(cap: usize) -> Self {
        Self(moka::future::Cache::new(cap as _))
    }

    async fn get_or_insert(&self, path: PathBuf) -> ArcResult<Arc<Mutex<File>>> {
        let open_path = path.clone();
        let open_fi = async {
            let fi = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(open_path)
                .await
                .located(here!())?;
            Ok(Arc::new(Mutex::new(fi)))
        };
        self.0.try_get_with(path, open_fi).await
    }
}

/// Persists results on a filesystem, encoding/decoding via JSON.
/// Uses an LRU cache to store open file descriptors for quick access.
#[derive(Clone)]
pub struct FilesystemPersistor {
    root: PathBuf,
    fds: FdCache,
}

impl FilesystemPersistor {
    pub fn new(root: impl Into<PathBuf>, lru_capacity: usize) -> Self {
        Self {
            root: root.into(),
            fds: FdCache::new(lru_capacity),
        }
    }

    pub fn new_shared(root: impl Into<PathBuf>, lru_capacity: usize) -> SharedPersistResults {
        SharedPersistResults(Box::new(Self::new(root, lru_capacity)))
    }

    fn get_path(&self, run_id: &RunId) -> PathBuf {
        let run_id = &run_id.0;

        self.root.join(format!("{run_id}.results.jsonl"))
    }

    #[cfg(test)]
    async fn invalidate(&self, run_id: &RunId) {
        let path = self.get_path(run_id);
        self.fds.0.invalidate(&path).await
    }

    #[cfg(test)]
    fn contains(&self, run_id: &RunId) -> bool {
        let path = self.get_path(run_id);
        self.fds.0.contains_key(&path)
    }
}

#[async_trait]
impl PersistResults for FilesystemPersistor {
    async fn dump(&self, run_id: &RunId, results: ResultsLine) -> ArcResult<()> {
        let path = self.get_path(run_id);

        let packed = serde_json::to_vec(&results).located(here!())?;

        let fi = self.fds.get_or_insert(path).await?;
        let mut fi = fi.lock().await;
        fi.write_all(&packed).await.located(here!())?;
        fi.write_all(&[b'\n']).await.located(here!())?;

        Ok(())
    }

    async fn get_results(&self, run_id: &RunId) -> ArcResult<OpaqueLazyAssociatedTestResults> {
        let path = self.get_path(run_id);

        let fi = self.fds.get_or_insert(path).await?;
        let mut fi = fi.lock().await;
        fi.rewind().await.located(here!())?;

        let mut iter = tokio::io::BufReader::new(&mut *fi).lines();
        let mut opaque_jsonl = vec![];
        while let Some(line) = iter.next_line().await.located(here!())? {
            opaque_jsonl.push(serde_json::value::RawValue::from_string(line).located(here!())?);
        }

        Ok(OpaqueLazyAssociatedTestResults::from_raw_json_lines(
            opaque_jsonl,
        ))
    }

    fn boxed_clone(&self) -> Box<dyn PersistResults> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use abq_run_n_times::n_times;
    use abq_test_utils::wid;
    use abq_utils::net_protocol::{
        queue::AssociatedTestResults, results::ResultsLine::Results, runners::TestResult,
        workers::RunId,
    };
    use tokio::task::JoinSet;

    use crate::persistence::results::PersistResults;

    use super::FilesystemPersistor;

    #[test]
    fn get_path() {
        let fs = FilesystemPersistor::new("/tmp", 1);
        let run_id = RunId("run1".to_owned());
        assert_eq!(
            fs.get_path(&run_id),
            PathBuf::from("/tmp/run1.results.jsonl")
        )
    }

    #[tokio::test]
    async fn dump_to_nonexistent_file_is_error() {
        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__", 1);

        let err = fs.dump(&RunId::unique(), Results(vec![])).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn load_from_nonexistent_file_is_error() {
        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__", 1);

        let err = fs.get_results(&RunId::unique()).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn dump_and_load_results() {
        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 1);

        let run_id = RunId::unique();

        let results1 = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);
        let results2 = Results(vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ]);

        fs.dump(&run_id, results1.clone()).await.unwrap();
        fs.dump(&run_id, results2.clone()).await.unwrap();
        let results = fs.get_results(&run_id).await.unwrap().decode().unwrap();

        assert_eq!(results, vec![results1, results2]);
    }

    #[tokio::test]
    async fn dump_and_load_results_multiple_times() {
        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 1);

        let run_id = RunId::unique();

        let results1 = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        fs.dump(&run_id, results1.clone()).await.unwrap();
        let results = fs.get_results(&run_id).await.unwrap().decode().unwrap();

        assert_eq!(results, vec![results1.clone()]);

        let results2 = Results(vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ]);
        fs.dump(&run_id, results2.clone()).await.unwrap();
        let results = fs.get_results(&run_id).await.unwrap().decode().unwrap();

        assert_eq!(results, vec![results1, results2]);
    }

    #[tokio::test]
    #[n_times(1000)]
    async fn dump_and_load_results_after_eviction() {
        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 1);

        let run_id = RunId::unique();

        let results1 = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        fs.dump(&run_id, results1.clone()).await.unwrap();

        // Evict the active run from the cache
        fs.invalidate(&run_id).await;
        assert!(
            !fs.contains(&run_id),
            "active file not evicted from the cache!"
        );

        let results = fs.get_results(&run_id).await.unwrap().decode().unwrap();

        assert_eq!(results, vec![results1]);
    }

    #[tokio::test]
    #[n_times(1000)]
    async fn dump_and_load_results_multiple_concurrent() {
        const RUNS: usize = 10;

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), RUNS);

        let mut join_set = JoinSet::new();
        let mut expected_for_run = Vec::with_capacity(RUNS);

        for _ in 0..RUNS {
            let run_id = RunId::unique();

            let results1 = Results(vec![
                AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
                AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
            ]);
            let results2 = Results(vec![
                AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
                AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
            ]);

            let task = {
                let fs = fs.clone();
                let run_id = run_id.clone();
                let (results1, results2) = (results1.clone(), results2.clone());
                async move {
                    fs.dump(&run_id, results1).await.unwrap();
                    fs.dump(&run_id, results2).await.unwrap();
                }
            };

            join_set.spawn(task);
            expected_for_run.push((run_id, vec![results1, results2]));
        }

        while let Some(result) = join_set.join_next().await {
            result.unwrap();
        }

        for (run_id, expected) in expected_for_run {
            let actual = fs.get_results(&run_id).await.unwrap().decode().unwrap();
            assert_eq!(actual, expected);
        }
    }
}
