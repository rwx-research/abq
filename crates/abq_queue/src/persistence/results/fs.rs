use std::{path::PathBuf, sync::Arc};

use abq_utils::{
    error::{OpaqueResult, ResultLocation},
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

        let opaque_jsonl = match read_results_lines(&mut fi).await {
            Ok(results) if !results.is_empty() => results,
            _ => {
                // Slow path: the results are missing in the local cache, or corrupted.
                // Load them in now and retry.
                self.remote
                    .load_to_disk(PersistenceKind::Results, run_id, &self.get_path(run_id))
                    .await?;

                fi.rewind().await.located(here!())?;
                read_results_lines(&mut fi).await?
            }
        };

        Ok(OpaqueLazyAssociatedTestResults::from_raw_json_lines(
            opaque_jsonl,
        ))
    }

    fn boxed_clone(&self) -> Box<dyn PersistResults> {
        Box::new(self.clone())
    }
}

async fn read_results_lines(fi: &mut File) -> OpaqueResult<Vec<Box<serde_json::value::RawValue>>> {
    let mut iter = tokio::io::BufReader::new(fi).lines();
    let mut opaque_jsonl = vec![];
    while let Some(line) = iter.next_line().await.located(here!())? {
        opaque_jsonl.push(serde_json::value::RawValue::from_string(line).located(here!())?);
    }
    Ok(opaque_jsonl)
}

#[cfg(test)]
mod test {
    use std::{
        path::PathBuf,
        sync::{atomic::AtomicUsize, Arc},
    };

    use abq_run_n_times::n_times;
    use abq_test_utils::wid;
    use abq_utils::{
        atomic,
        error::{ErrorLocation, ResultLocation},
        here,
        net_protocol::{
            queue::AssociatedTestResults,
            results::ResultsLine::{self, Results},
            runners::TestResult,
            workers::RunId,
        },
    };
    use tokio::task::JoinSet;

    use crate::persistence::{
        remote::{self, fake_unreachable, PersistenceKind},
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
                    let results = results.clone();
                    async move {
                        assert_eq!(kind, PersistenceKind::Results);
                        assert_eq!(run_id.0, "test-run-id");
                        assert_eq!(path.file_name().unwrap(), "test-run-id.results.jsonl");
                        let data = tokio::fs::read_to_string(path).await.unwrap();
                        let read: ResultsLine = serde_json::from_str(&data).unwrap();
                        assert_eq!(read, results);
                        Ok(())
                    }
                }
            },
            fake_unreachable,
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
            |_, _, _| async { Err("i failed").located(abq_utils::here!()) },
            fake_unreachable,
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
            move |kind, run_id, path| async move {
                assert_eq!(kind, PersistenceKind::Results);
                assert_eq!(run_id.0, "test-run-id");
                assert_eq!(path.file_name().unwrap(), "test-run-id.results.jsonl");
                let data = tokio::fs::read_to_string(path).await.unwrap();
                assert!(data.is_empty());
                Ok(())
            },
            fake_unreachable,
        );

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.dump_to_remote(&run_id).await.unwrap();
    }

    #[n_times(100)]
    #[tokio::test]
    async fn dump_to_remote_while_another_results_line_comes_in() {
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
                    let (results1, results2) = (results1.clone(), results2.clone());

                    async move {
                        assert_eq!(kind, PersistenceKind::Results);
                        assert_eq!(run_id.0, "test-run-id");
                        assert_eq!(path.file_name().unwrap(), "test-run-id.results.jsonl");

                        use tokio::io::AsyncBufReadExt;

                        let mut fi = tokio::fs::File::open(path).await.unwrap();
                        let mut lines = tokio::io::BufReader::new(&mut fi).lines();

                        let mut data: Vec<ResultsLine> = vec![];
                        while let Some(line) = lines.next_line().await.unwrap() {
                            data.push(serde_json::from_str(&line).unwrap());
                        }

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
                }
            },
            fake_unreachable,
        );

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        // Dump the first results line.
        {
            let fs = fs.clone();
            fs.dump(&run_id, results1).await.unwrap();
        }

        // Start the two tasks on separate threads, since the remote persister check is blocking.
        let dump_remote_task = {
            let fs = fs.clone();
            let run_id = run_id.clone();
            async move { fs.dump_to_remote(&run_id).await }
        };

        let dump_results_task = { fs.dump(&run_id, results2) };

        let dump_remote_result;
        let dump_results_result;
        if i % 2 == 0 {
            (dump_remote_result, dump_results_result) =
                tokio::join!(dump_remote_task, dump_results_task);
        } else {
            (dump_results_result, dump_remote_result) =
                tokio::join!(dump_remote_task, dump_results_task);
        }
        dump_remote_result.unwrap();
        dump_results_result.unwrap();
    }

    #[tokio::test]
    async fn missing_get_results_fetches_from_remote() {
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote = {
            let results = results.clone();
            remote::FakePersister::new(fake_unreachable, move |_, _, path| {
                let results = results.clone();
                async move {
                    tokio::fs::write(path, serde_json::to_vec(&results).unwrap())
                        .await
                        .unwrap();
                    Ok(())
                }
            })
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        let actual_results = fs.get_results(&run_id).await.unwrap();
        let actual_results = actual_results.decode().unwrap();
        assert_eq!(actual_results, vec![results]);
    }

    #[tokio::test]
    async fn missing_get_results_errors_if_remote_errors() {
        let run_id = RunId("test-run-id".to_string());

        let remote = remote::FakePersister::new(fake_unreachable, |_, _, _| async {
            Err("i failed".located(here!()))
        });

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        let result = fs.get_results(&run_id).await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("i failed"));
    }

    #[tokio::test]
    async fn get_results_prefer_local_to_remote() {
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote = { remote::FakePersister::new(fake_unreachable, fake_unreachable) };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.dump(&run_id, results.clone()).await.unwrap();

        let actual_results = fs.get_results(&run_id).await.unwrap();
        let actual_results = actual_results.decode().unwrap();
        assert_eq!(actual_results, vec![results]);
    }

    #[n_times(100)]
    #[tokio::test]
    async fn race_getting_multiple_results_from_remote() {
        // When we need to fetch results from the remote, they should only be loaded in once if the
        // local cache is hot.
        const N: usize = 10;

        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote_loads = Arc::new(AtomicUsize::new(0));

        let remote = {
            let results = results.clone();
            let remote_loads = remote_loads.clone();
            remote::FakePersister::new(fake_unreachable, move |_, _, path| {
                let results = results.clone();
                let remote_loads = remote_loads.clone();
                async move {
                    tokio::fs::write(path, serde_json::to_vec(&results).unwrap())
                        .await
                        .unwrap();
                    remote_loads.fetch_add(1, atomic::ORDERING);
                    Ok(())
                }
            })
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        let mut join_set = tokio::task::JoinSet::new();

        for _ in 0..N {
            let run_id = run_id.clone();
            let results = results.clone();
            let fs = fs.clone();
            join_set.spawn(async move {
                let actual_results = fs.get_results(&run_id).await.unwrap();
                let actual_results = actual_results.decode().unwrap();
                assert_eq!(actual_results, vec![results]);
            });
        }

        while let Some(join_handle) = join_set.join_next().await {
            join_handle.unwrap();
        }

        assert_eq!(remote_loads.load(atomic::ORDERING), 1);
    }
}
