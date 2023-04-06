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
    io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::Mutex,
};

use crate::persistence::remote::{PersistenceKind, RemotePersister};

use super::{ArcResult, PersistResults, SharedPersistResults};

/// A concurrent LRU cache of open file descriptors.
//
// TODO: reading and writing to results files asynchronously may be an expensive task,
// given that results file may be quite large. Would it be better to use [std::fs::File]
// and spawn off a blocking tasks instead?
//
//     https://docs.rs/tokio/latest/tokio/fs/index.html#usage
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
    remote: RemotePersister,
}

impl FilesystemPersistor {
    pub fn new(
        root: impl Into<PathBuf>,
        lru_capacity: usize,
        remote: impl Into<RemotePersister>,
    ) -> Self {
        Self {
            root: root.into(),
            fds: FdCache::new(lru_capacity),
            remote: remote.into(),
        }
    }

    pub fn new_shared(
        root: impl Into<PathBuf>,
        lru_capacity: usize,
        remote: impl Into<RemotePersister>,
    ) -> SharedPersistResults {
        SharedPersistResults(Box::new(Self::new(root, lru_capacity, remote)))
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

        fi.seek(SeekFrom::End(0)).await.located(here!())?;
        fi.write_all(&packed).await.located(here!())?;
        fi.write_all(&[b'\n']).await.located(here!())?;
        fi.flush().await.located(here!())?;

        Ok(())
    }

    async fn dump_to_remote(&self, run_id: &RunId) -> ArcResult<()> {
        let path = self.get_path(run_id);
        let fi = self.fds.get_or_insert(path.clone()).await?;

        // Take an exclusive lock so that new results don't come in while we flush to the remote.
        let fi = fi.lock().await;

        // While we have this opportunity, also instruct the OS to totally sync all data to disk
        // rather than keeping it possibly in memory.
        fi.sync_all().await.located(here!())?;

        self.remote
            .store_from_disk(PersistenceKind::Results, run_id, &path)
            .await?;

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
    use abq_utils::{
        error::ResultLocation,
        net_protocol::{
            queue::AssociatedTestResults,
            results::ResultsLine::{self, Results},
            runners::TestResult,
            workers::RunId,
        },
    };
    use tokio::{runtime, task::JoinSet};

    use crate::persistence::{
        remote::{self, PersistenceKind},
        results::PersistResults,
    };

    use super::FilesystemPersistor;

    #[test]
    fn get_path() {
        let fs = FilesystemPersistor::new("/tmp", 1, remote::NoopPersister);
        let run_id = RunId("run1".to_owned());
        assert_eq!(
            fs.get_path(&run_id),
            PathBuf::from("/tmp/run1.results.jsonl")
        )
    }

    #[tokio::test]
    async fn dump_to_nonexistent_file_is_error() {
        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__", 1, remote::NoopPersister);

        let err = fs.dump(&RunId::unique(), Results(vec![])).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn load_from_nonexistent_file_is_error() {
        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__", 1, remote::NoopPersister);

        let err = fs.get_results(&RunId::unique()).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn dump_and_load_results() {
        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 1, remote::NoopPersister);

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
        let fs = FilesystemPersistor::new(tempdir.path(), 1, remote::NoopPersister);

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

    #[n_times(1000)]
    #[tokio::test]
    async fn dump_and_load_results_after_eviction() {
        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 1, remote::NoopPersister);

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

    #[n_times(1000)]
    #[tokio::test]
    async fn dump_and_load_results_multiple_concurrent() {
        const RUNS: usize = 10;

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), RUNS, remote::NoopPersister);

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

    #[tokio::test]
    async fn dump_to_remote() {
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote = remote::FakePersister::new(
            {
                let results = results.clone();
                move |kind, run_id, path| {
                    assert_eq!(kind, PersistenceKind::Results);
                    assert_eq!(run_id.0, "test-run-id");
                    assert_eq!(path.file_name().unwrap(), "test-run-id.results.jsonl");
                    let data = std::fs::read_to_string(path).unwrap();
                    let read: ResultsLine = serde_json::from_str(&data).unwrap();
                    assert_eq!(read, results);
                    Ok(())
                }
            },
            |_, _, _| unreachable!(),
        );

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.dump(&run_id, results).await.unwrap();

        fs.dump_to_remote(&run_id).await.unwrap();
    }

    #[tokio::test]
    async fn dump_to_remote_err() {
        let run_id = RunId("test-run-id".to_string());

        let remote = remote::FakePersister::new(
            |_, _, _| Err("i failed").located(abq_utils::here!()),
            |_, _, _| unreachable!(),
        );

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        let err = fs.dump_to_remote(&run_id).await.unwrap_err();

        assert!(err.to_string().contains("i failed"));
    }

    #[tokio::test]
    async fn dump_to_remote_run_id_does_not_exist() {
        let run_id = RunId("test-run-id".to_string());

        // The remote should see the results, but the results will be empty.
        let remote = remote::FakePersister::new(
            move |kind, run_id, path| {
                assert_eq!(kind, PersistenceKind::Results);
                assert_eq!(run_id.0, "test-run-id");
                assert_eq!(path.file_name().unwrap(), "test-run-id.results.jsonl");
                let data = std::fs::read_to_string(path).unwrap();
                assert!(data.is_empty());
                Ok(())
            },
            |_, _, _| unreachable!(),
        );

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.dump_to_remote(&run_id).await.unwrap();
    }

    #[n_times(100)]
    #[test]
    fn dump_to_remote_while_another_results_line_comes_in() {
        // Race dumping to the remote and having another results line come in.
        // Who wins is arbitrary - that's okay since we only want to enforce linearizability.
        // However, we must make sure that the remote does not dump the results in a partial state
        // are another results line writes to it.

        let run_id = RunId("test-run-id".to_string());

        let results1 = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);
        let results2 = Results(vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ]);

        let remote = remote::FakePersister::new(
            {
                let (results1, results2) = (results1.clone(), results2.clone());
                move |kind, run_id, path| {
                    assert_eq!(kind, PersistenceKind::Results);
                    assert_eq!(run_id.0, "test-run-id");
                    assert_eq!(path.file_name().unwrap(), "test-run-id.results.jsonl");

                    use std::io::BufRead;

                    let mut fi = std::fs::File::open(path).unwrap();
                    let lines = std::io::BufReader::new(&mut fi).lines();

                    let data: Vec<ResultsLine> = lines
                        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
                        .collect();

                    if data.len() == 1 {
                        assert_eq!(data[0], results1);
                    } else if data.len() == 2 {
                        assert_eq!(data[0], results1);
                        assert_eq!(data[1], results2);
                    } else {
                        panic!("unexpected number of lines: {:?}", data);
                    }

                    Ok(())
                }
            },
            |_, _, _| unreachable!(),
        );

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        let rt = || {
            runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
        };

        // Dump the first results line.
        {
            let fs = fs.clone();
            let run_id = run_id.clone();
            rt().block_on(async move {
                fs.dump(&run_id, results1).await.unwrap();
            });
        }

        // Start the two tasks on separate threads, since the remote persister check is blocking.
        let dump_remote_task = {
            let fs = fs.clone();
            let run_id = run_id.clone();
            move || rt().block_on(async move { fs.dump_to_remote(&run_id).await.unwrap() })
        };

        let dump_results_task =
            move || rt().block_on(async move { fs.dump(&run_id, results2).await.unwrap() });

        let dump_remote_thread;
        let dump_results_thread;
        if i % 2 == 0 {
            dump_remote_thread = std::thread::spawn(dump_remote_task);
            dump_results_thread = std::thread::spawn(dump_results_task);
        } else {
            dump_results_thread = std::thread::spawn(dump_results_task);
            dump_remote_thread = std::thread::spawn(dump_remote_task);
        }

        dump_remote_thread.join().unwrap();
        dump_results_thread.join().unwrap();
    }
}
