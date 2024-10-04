use std::{
    collections::HashMap,
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use abq_utils::{
    error::{ErrorLocation, OpaqueResult, ResultLocation},
    here, illegal_state, log_assert,
    net_protocol::{results::ResultsLine, workers::RunId},
};
use async_trait::async_trait;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::{Mutex, RwLock},
};

use crate::persistence::{
    remote::{PersistenceKind, RemotePersister},
    results::ResultsStream,
    OffloadConfig, OffloadSummary,
};

use super::{PersistResults, Result, SharedPersistResults, WithResultsStream};

enum FdState {
    Open(File),
    Offloaded,
}

type FdMap = HashMap<RunId, Mutex<FdState>>;

struct FdCacheInner {
    root: PathBuf,
    cache: RwLock<FdMap>,
    remote: RemotePersister,
}

/// Persists results on a filesystem, encoding/decoding via JSON.
//
// TODO: reading and writing to results files asynchronously may be an expensive task,
// given that results file may be quite large. Would it be better to use [std::fs::File]
// and spawn off a blocking tasks instead?
//
//     https://docs.rs/tokio/latest/tokio/fs/index.html#usage
#[derive(Clone)]
#[repr(transparent)]
pub struct FilesystemPersistor(Arc<FdCacheInner>);

#[async_trait]
trait WithFile<T> {
    async fn run(self, fi: &mut File) -> Result<T>;
}

impl FilesystemPersistor {
    pub fn new(root: impl Into<PathBuf>, cap: usize, remote: impl Into<RemotePersister>) -> Self {
        Self(Arc::new(FdCacheInner {
            root: root.into(),
            cache: RwLock::new(HashMap::with_capacity(cap)),
            remote: remote.into(),
        }))
    }

    pub fn new_shared(
        root: impl Into<PathBuf>,
        capacity: usize,
        remote: impl Into<RemotePersister>,
    ) -> SharedPersistResults {
        SharedPersistResults(Box::new(Self::new(root, capacity, remote)))
    }

    fn get_path(&self, run_id: &RunId) -> PathBuf {
        let run_id = &run_id.0;

        self.0.root.join(format!("{run_id}.results.jsonl"))
    }

    fn run_id_of_path(&self, path: &Path) -> Result<RunId> {
        let file_name = path
            .file_name()
            .ok_or_else(|| format!("path {path:?} has no file name"))
            .located(here!())?;

        let file_name = file_name
            .to_str()
            .ok_or_else(|| format!("{path:?} is not valid UTF-8"))
            .located(here!())?;

        let run_id = file_name
            .strip_suffix(".results.jsonl")
            .ok_or_else(|| format!("file name {path:?} does not end with '.results.jsonl'"))
            .located(here!())?;

        Ok(RunId(run_id.to_owned()))
    }

    #[cfg(test)] // for now
    async fn insert_offloaded<'a>(&self, run_id: RunId) -> Result<()> {
        let mut cache = self.0.cache.write().await;
        if cache.contains_key(&run_id) {
            return Err(format!("run ID {run_id:?} already inserted in cache").located(here!()));
        }
        cache.insert(run_id, Mutex::new(FdState::Offloaded));
        Ok(())
    }

    async fn open_file(&self, run_id: &RunId) -> Result<File> {
        let path = self.get_path(run_id);
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .await
            .located(here!())
    }

    async fn with_file_fast_path<T, F: WithFile<T>>(
        &self,
        run_id: &RunId,
        f: F,
    ) -> Result<ControlFlow<Result<T>, F>> {
        // Fast path: try to read or load in an open file descriptor from the cache.
        let cache = self.0.cache.read().await;
        let state = match cache.get(run_id) {
            Some(state) => state,
            None => return Ok(ControlFlow::Continue(f)),
        };

        let mut state = state.lock().await;
        let result = match &mut *state {
            FdState::Open(fi) => f.run(&mut *fi).await,
            FdState::Offloaded => {
                // Load the file from the remote, then update the fd state.
                let path = self.get_path(run_id);
                self.0
                    .remote
                    .load_to_disk(PersistenceKind::Results, run_id, &path)
                    .await?;

                tracing::info!(?run_id, "Loaded test results from remote.");

                let mut fi = self.open_file(run_id).await?;

                // Compute the result, then update the fd state.
                let result = f.run(&mut fi).await;

                *state = FdState::Open(fi);

                result
            }
        };
        Ok(ControlFlow::Break(result))
    }

    async fn with_file<T>(&self, run_id: &RunId, f: impl WithFile<T>) -> Result<T> {
        let f = match self.with_file_fast_path(run_id, f).await? {
            ControlFlow::Break(result) => return result,
            ControlFlow::Continue(f) => f,
        };

        {
            // Slow path: we may need to insert a new file descriptor into the cache.
            let mut cache = self.0.cache.write().await;
            match cache.get(run_id) {
                None => {
                    // Try to load the file from the remote, then update the fd state.
                    //
                    // TODO: this blocks reads for a possibly-long time; consider instead loading
                    // the results into a tempfile, and then atomically renaming when holding a
                    // lock over the cache.
                    let path = self.get_path(run_id);
                    let remote_load_result = self
                        .0
                        .remote
                        .load_to_disk(PersistenceKind::Results, run_id, &path)
                        .await;

                    let fi = self.open_file(run_id).await?;

                    if remote_load_result.is_ok() {
                        tracing::info!(?run_id, "Created local results file from remote data.");
                    } else {
                        tracing::info!(?run_id, "Created fresh local results file.");
                    }

                    cache.insert(run_id.clone(), Mutex::new(FdState::Open(fi)));
                }
                Some(_) => {
                    // We hit a TOCTOU race; that's fine, release our write lock and go into a reader.
                }
            }
        }

        match self.with_file_fast_path(run_id, f).await? {
            ControlFlow::Break(result) => result,
            ControlFlow::Continue(_) => {
                illegal_state!(
                    "file descriptor missing after insertion into cache",
                    ?run_id
                );
                Err(
                    format!("file descriptor missing after insertion into cache for {run_id:?}")
                        .located(here!()),
                )
            }
        }
    }

    /// Offloads to the remote all non-empty results files that are older than
    /// the configured threshold.
    pub async fn run_offload_job(&self, offload_config: OffloadConfig) -> Result<OffloadSummary> {
        let time_now = SystemTime::now();
        let mut results_files = tokio::fs::read_dir(&self.0.root).await.located(here!())?;
        let mut offloaded_run_ids = vec![];

        while let Some(results_file) = results_files.next_entry().await.located(here!())? {
            let offload_result = self
                .wrapped_offload_one_results_file(&results_file, time_now, offload_config)
                .await;

            match offload_result {
                Ok(opt_offloaded_run_id) => offloaded_run_ids.extend(opt_offloaded_run_id),
                Err(e) => {
                    tracing::error!(
                        ?e,
                        path=?results_file.path(),
                        "Error offloading results file."
                    );
                }
            }
        }

        Ok(OffloadSummary { offloaded_run_ids })
    }

    async fn wrapped_offload_one_results_file(
        &self,
        results_file: &tokio::fs::DirEntry,
        time_now: SystemTime,
        offload_config: OffloadConfig,
    ) -> Result<Option<RunId>> {
        let metadata = results_file.metadata().await.located(here!())?;
        log_assert!(!metadata.is_dir(), path=?results_file.path(), "results file is a directory");

        if offload_config.file_eligible_for_offload(&time_now, &metadata)? {
            let path = results_file.path();
            let run_id = self.run_id_of_path(&path)?;
            let did_offload = self
                .perform_offload(&run_id, &path, time_now, offload_config)
                .await?;

            if did_offload {
                return Ok(Some(run_id));
            }
        }

        Ok(None)
    }

    async fn perform_offload(
        &self,
        run_id: &RunId,
        path: &Path,
        time_now: SystemTime,
        offload_config: OffloadConfig,
    ) -> Result<bool> {
        // Take an exclusive lock so that new results don't come in while we flush to the remote.
        let cache = self.0.cache.write().await;
        let fd_state = cache
            .get(run_id)
            .ok_or_else(|| format!("run ID {run_id:?} has results on disk, but not found in cache"))
            .located(here!())?;
        let mut fd_state = fd_state.lock().await;
        let fi = match &*fd_state {
            FdState::Open(fi) => fi,
            FdState::Offloaded => {
                // THEORY: this is only possible if the file was offloaded by another
                // thread. In practice we expect offloading jobs to be serialized, so
                // this is an unexpected state.
                tracing::warn!(
                    ?run_id,
                    "run ID has results on disk, but is not in an open state"
                );

                return Ok(false);
            }
        };

        // We must now check again whether the file is eligible for offload, since it may have been
        // modified since we performed the non-locked check.
        let metadata = fi.metadata().await.located(here!())?;
        if !offload_config.file_eligible_for_offload(&time_now, &metadata)? {
            return Ok(false);
        }

        // While we have this opportunity, also instruct the OS to totally sync all data to disk
        // rather than keeping it possibly in memory.
        fi.sync_all().await.located(here!())?;

        let run_id = self.run_id_of_path(path).located(here!())?;

        self.0
            .remote
            .store_from_disk(PersistenceKind::Results, &run_id, path)
            .await?;

        // Truncate the file to 0 bytes, freeing the disk space.
        // Note that this doesn't free the inode.
        fi.set_len(0).await.located(here!())?;
        fi.sync_all().await.located(here!())?;

        *fd_state = FdState::Offloaded;

        Ok(true)
    }
}

#[async_trait]
impl PersistResults for FilesystemPersistor {
    async fn dump(&self, run_id: &RunId, results: ResultsLine) -> Result<()> {
        struct Dump {
            packed: Vec<u8>,
        }
        #[async_trait]
        impl WithFile<()> for Dump {
            #[inline]
            async fn run(self, fi: &mut File) -> Result<()> {
                fi.seek(SeekFrom::End(0)).await.located(here!())?;

                write_packed_line(fi, self.packed).await?;

                Ok(())
            }
        }

        let packed = serde_json::to_vec(&results).located(here!())?;
        self.with_file(run_id, Dump { packed }).await
    }

    async fn dump_to_remote(&self, run_id: &RunId) -> Result<()> {
        struct DumpToRemote<'a> {
            remote: &'a RemotePersister,
            path: PathBuf,
            run_id: &'a RunId,
        }
        #[async_trait]
        impl<'a> WithFile<()> for DumpToRemote<'a> {
            #[inline]
            async fn run(self, fi: &mut File) -> Result<()> {
                // While we have this opportunity, also instruct the OS to totally sync all data to disk
                // rather than keeping it possibly in memory.
                fi.sync_all().await.located(here!())?;

                self.remote
                    .store_from_disk(PersistenceKind::Results, self.run_id, &self.path)
                    .await?;

                Ok(())
            }
        }

        self.with_file(
            run_id,
            DumpToRemote {
                remote: &self.0.remote,
                path: self.get_path(run_id),
                run_id,
            },
        )
        .await
    }

    async fn with_results_stream(
        &self,
        run_id: &RunId,
        callback: Box<dyn WithResultsStream + Send>,
    ) -> Result<()> {
        struct GetResults {
            callback: Box<dyn WithResultsStream + Send>,
        }

        #[async_trait]
        impl WithFile<()> for GetResults {
            #[inline]
            async fn run(self, fi: &mut File) -> Result<()> {
                fi.rewind().await.located(here!())?;

                let fi_size = fi.metadata().await.located(here!())?.len();

                let results_stream = ResultsStream {
                    stream: Box::new(fi),
                    len: fi_size as _,
                };

                self.callback.with_results_stream(results_stream).await
            }
        }

        self.with_file(run_id, GetResults { callback }).await
    }

    fn boxed_clone(&self) -> Box<dyn PersistResults> {
        Box::new(self.clone())
    }
}

async fn write_packed_line(fi: &mut File, packed: Vec<u8>) -> OpaqueResult<()> {
    fi.write_all(&packed).await.located(here!())?;
    fi.write_all(b"\n").await.located(here!())?;
    fi.flush().await.located(here!())
}

#[cfg(test)]
mod test {
    use std::{
        path::PathBuf,
        sync::{atomic::AtomicUsize, Arc},
        time::Duration,
    };

    use abq_run_n_times::n_times;
    use abq_test_utils::wid;
    use abq_utils::{
        atomic,
        error::{ErrorLocation, OpaqueResult, ResultLocation},
        here,
        net_protocol::{
            queue::AssociatedTestResults,
            results::{
                OpaqueLazyAssociatedTestResults,
                ResultsLine::{self, Results},
            },
            runners::TestResult,
            workers::RunId,
        },
    };
    use futures::FutureExt;
    use tokio::task::JoinSet;

    use crate::persistence::{
        remote::{self, fake_error, OneWriteFakePersister, PersistenceKind},
        results::{
            fs::write_packed_line,
            test_utils::{ResultsLoader, ResultsWrapper},
            PersistResults,
        },
        OffloadConfig, OffloadSummary,
    };

    use super::FilesystemPersistor;

    async fn get_results(
        persistor: &FilesystemPersistor,
        run_id: &RunId,
    ) -> OpaqueResult<OpaqueLazyAssociatedTestResults> {
        let results = ResultsWrapper::default();
        let results_loader = ResultsLoader {
            results: results.clone(),
        };

        persistor
            .with_results_stream(run_id, Box::new(results_loader))
            .await
            .located(here!())?;

        results.get()
    }

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

        let results = ResultsWrapper::default();
        let loader = ResultsLoader { results };
        let err = fs
            .with_results_stream(&RunId::unique(), Box::new(loader))
            .await;
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

        let results = get_results(&fs, &run_id).await.unwrap();

        let results = results.decode().unwrap();

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

        let results = get_results(&fs, &run_id).await.unwrap();
        let results = results.decode().unwrap();

        assert_eq!(results, vec![results1.clone()]);

        let results2 = Results(vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ]);
        fs.dump(&run_id, results2.clone()).await.unwrap();
        let results = get_results(&fs, &run_id).await.unwrap();
        let results = results.decode().unwrap();

        assert_eq!(results, vec![results1, results2]);
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
            let actual = get_results(&fs, &run_id).await.unwrap();
            let actual = actual.decode().unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[n_times(10)]
    #[tokio::test]
    async fn dump_and_load_results_high_count_contention() {
        const RUNS: usize = 10;

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), RUNS, remote::NoopPersister);

        let mut join_set = JoinSet::new();
        let mut expected_for_run = Vec::with_capacity(RUNS);

        let results = (0..10)
            .map(|i| {
                Results(vec![
                    AssociatedTestResults::fake(wid(i * 2), vec![TestResult::fake()]),
                    AssociatedTestResults::fake(wid(i * 2 + 1), vec![TestResult::fake()]),
                ])
            })
            .collect::<Vec<_>>();

        for res in results.iter() {
            for _ in 0..RUNS {
                let run_id = RunId::unique();

                let task = {
                    let fs = fs.clone();
                    let run_id = run_id.clone();
                    let res = res.clone();
                    async move {
                        fs.dump(&run_id, res).await.unwrap();
                    }
                };

                join_set.spawn(task);
                expected_for_run.push((run_id, vec![res.clone()]));
            }
        }

        while let Some(result) = join_set.join_next().await {
            result.unwrap();
        }

        for (run_id, expected) in expected_for_run {
            let actual = get_results(&fs, &run_id).await.unwrap();
            let actual = actual.decode().unwrap();
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

        let remote = remote::FakePersister::builder()
            .on_store_from_disk({
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
                    .boxed()
                }
            })
            .on_load_to_disk(fake_error)
            .build();

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.dump(&run_id, results).await.unwrap();

        fs.dump_to_remote(&run_id).await.unwrap();
    }

    #[tokio::test]
    async fn dump_to_remote_err() {
        let run_id = RunId("test-run-id".to_string());

        let remote = remote::FakePersister::builder()
            .on_store_from_disk(|_, _, _| {
                async { Err("i failed").located(abq_utils::here!()) }.boxed()
            })
            .on_load_to_disk(fake_error)
            .build();

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        let err = fs.dump_to_remote(&run_id).await.unwrap_err();

        assert!(err.to_string().contains("i failed"));
    }

    #[tokio::test]
    async fn dump_to_remote_run_id_does_not_exist() {
        let run_id = RunId("test-run-id".to_string());

        // The remote should see the results, but the results will be empty.
        let remote = remote::FakePersister::builder()
            .on_store_from_disk(move |kind, run_id, path| {
                async move {
                    assert_eq!(kind, PersistenceKind::Results);
                    assert_eq!(run_id.0, "test-run-id");
                    assert_eq!(path.file_name().unwrap(), "test-run-id.results.jsonl");
                    let data = tokio::fs::read_to_string(path).await.unwrap();
                    assert!(data.is_empty());
                    Ok(())
                }
                .boxed()
            })
            .on_load_to_disk(fake_error)
            .build();

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

        let remote = remote::FakePersister::builder()
            .on_store_from_disk({
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
                    .boxed()
                }
            })
            .on_load_to_disk(fake_error)
            .build();

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
    async fn first_get_results_fetches_from_remote() {
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote = {
            let results = results.clone();
            remote::FakePersister::builder()
                .on_load_to_disk(move |_, _, path| {
                    let results = results.clone();
                    async move {
                        tokio::fs::write(path, serde_json::to_vec(&results).unwrap())
                            .await
                            .unwrap();
                        Ok(())
                    }
                    .boxed()
                })
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        let actual_results = get_results(&fs, &run_id).await.unwrap();
        let actual_results = actual_results.decode().unwrap();
        assert_eq!(actual_results, vec![results]);
    }

    #[tokio::test]
    async fn missing_get_results_fetches_from_remote_offloaded() {
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote = {
            let results = results.clone();
            remote::FakePersister::builder()
                .on_load_to_disk(move |_, _, path| {
                    let results = results.clone();
                    async move {
                        tokio::fs::write(path, serde_json::to_vec(&results).unwrap())
                            .await
                            .unwrap();
                        Ok(())
                    }
                    .boxed()
                })
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.insert_offloaded(run_id.clone()).await.unwrap();

        let actual_results = get_results(&fs, &run_id).await.unwrap();
        let actual_results = actual_results.decode().unwrap();
        assert_eq!(actual_results, vec![results]);
    }

    #[tokio::test]
    async fn missing_get_results_errors_if_remote_errors_offloaded() {
        let run_id = RunId("test-run-id".to_string());

        let remote = remote::FakePersister::builder()
            .on_load_to_disk(|_, _, _| async { Err("i failed".located(here!())) }.boxed())
            .build();

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.insert_offloaded(run_id.clone()).await.unwrap();

        let result = get_results(&fs, &run_id).await;
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

        let remote = {
            remote::FakePersister::builder()
                .on_load_to_disk(fake_error)
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.dump(&run_id, results.clone()).await.unwrap();

        let actual_results = get_results(&fs, &run_id).await.unwrap();
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
            remote::FakePersister::builder()
                .on_load_to_disk(move |_, _, path| {
                    let results = results.clone();
                    let remote_loads = remote_loads.clone();
                    async move {
                        tokio::fs::write(path, serde_json::to_vec(&results).unwrap())
                            .await
                            .unwrap();
                        remote_loads.fetch_add(1, atomic::ORDERING);
                        Ok(())
                    }
                    .boxed()
                })
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        // Prime the cache with the run ID offloaded.
        fs.insert_offloaded(run_id.clone()).await.unwrap();

        let mut join_set = tokio::task::JoinSet::new();

        for _ in 0..N {
            let run_id = run_id.clone();
            let results = results.clone();
            let fs = fs.clone();
            join_set.spawn(async move {
                let actual_results = get_results(&fs, &run_id).await.unwrap();
                let actual_results = actual_results.decode().unwrap();
                assert_eq!(actual_results, vec![results]);
            });
        }

        while let Some(join_handle) = join_set.join_next().await {
            join_handle.unwrap();
        }

        assert_eq!(remote_loads.load(atomic::ORDERING), 1);
    }

    #[tokio::test]
    async fn first_write_results_fetches_from_remote() {
        let run_id = RunId("test-run-id".to_string());

        let results1 = Results(vec![AssociatedTestResults::fake(
            wid(1),
            vec![TestResult::fake()],
        )]);
        let results2 = Results(vec![AssociatedTestResults::fake(
            wid(2),
            vec![TestResult::fake()],
        )]);

        let remote = {
            let results = results1.clone();
            remote::FakePersister::builder()
                .on_load_to_disk(move |_, _, path| {
                    let results = results.clone();
                    async move {
                        let mut fi = tokio::fs::File::create(path).await.unwrap();
                        write_packed_line(&mut fi, serde_json::to_vec(&results).unwrap())
                            .await
                            .unwrap();
                        Ok(())
                    }
                    .boxed()
                })
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        // Write locally. We should try a fetch from the remote and append to it.
        fs.dump(&run_id, results2.clone()).await.unwrap();

        let actual_results = get_results(&fs, &run_id).await.unwrap();

        let actual_results = actual_results.decode().unwrap();
        assert_eq!(actual_results, vec![results1, results2]);
    }

    #[tokio::test]
    async fn missing_write_results_fetches_from_remote_offloaded() {
        let run_id = RunId("test-run-id".to_string());

        let results1 = Results(vec![AssociatedTestResults::fake(
            wid(1),
            vec![TestResult::fake()],
        )]);
        let results2 = Results(vec![AssociatedTestResults::fake(
            wid(2),
            vec![TestResult::fake()],
        )]);

        let remote = {
            let results = results1.clone();
            remote::FakePersister::builder()
                .on_load_to_disk(move |_, _, path| {
                    let results = results.clone();
                    async move {
                        let mut fi = tokio::fs::File::create(path).await.unwrap();
                        write_packed_line(&mut fi, serde_json::to_vec(&results).unwrap())
                            .await
                            .unwrap();
                        Ok(())
                    }
                    .boxed()
                })
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.insert_offloaded(run_id.clone()).await.unwrap();

        // Write locally. We should try a fetch from the remote and append to it.
        fs.dump(&run_id, results2.clone()).await.unwrap();

        let actual_results = get_results(&fs, &run_id).await.unwrap();

        let actual_results = actual_results.decode().unwrap();
        assert_eq!(actual_results, vec![results1, results2]);
    }

    #[tokio::test]
    async fn missing_write_results_fetches_from_remote_with_error_is_error_offloaded() {
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote_loads = Arc::new(AtomicUsize::new(0));

        let remote = {
            let remote_loads = remote_loads.clone();
            remote::FakePersister::builder()
                .on_load_to_disk(move |_, _, _| {
                    let remote_loads = remote_loads.clone();
                    async move {
                        remote_loads.fetch_add(1, atomic::ORDERING);
                        Err("i failed".located(here!()))
                    }
                    .boxed()
                })
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.insert_offloaded(run_id.clone()).await.unwrap();

        // Write locally. The remote fetch should fail, so we should assume this is a new results
        // file.
        let err = fs.dump(&run_id, results.clone()).await;
        assert!(err.is_err());
        let err = err.unwrap_err();
        assert!(err.to_string().contains("i failed"));
    }

    #[tokio::test]
    async fn write_results_prefer_local_to_remote() {
        let run_id = RunId("test-run-id".to_string());

        let results1 = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);
        let results2 = Results(vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ]);

        let remote = {
            remote::FakePersister::builder()
                .on_load_to_disk(|_, _, _| async { Err("").located(here!()) }.boxed())
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        fs.dump(&run_id, results1.clone()).await.unwrap();
        fs.dump(&run_id, results2.clone()).await.unwrap();

        let actual_results = get_results(&fs, &run_id).await.unwrap();
        let actual_results = actual_results.decode().unwrap();
        assert_eq!(actual_results, vec![results1, results2]);
    }

    #[n_times(100)]
    #[tokio::test]
    async fn race_writing_results_with_fetch_from_remote() {
        // When we need to fetch results from the remote, they should only be loaded in once if the
        // local cache is hot.
        const N: usize = 10;

        let run_id = RunId("test-run-id".to_string());

        let remote_results = Results(vec![AssociatedTestResults::fake(
            wid(1),
            vec![TestResult::fake()],
        )]);

        let other_results: Vec<_> = (1..=N)
            .map(|i| {
                Results(vec![AssociatedTestResults::fake(
                    wid(i * 10),
                    vec![TestResult::fake()],
                )])
            })
            .collect();

        let remote_loads = Arc::new(AtomicUsize::new(0));

        let remote = {
            let results = remote_results.clone();
            let remote_loads = remote_loads.clone();
            remote::FakePersister::builder()
                .on_load_to_disk(move |_, _, path| {
                    let results = results.clone();
                    let remote_loads = remote_loads.clone();
                    async move {
                        let mut fi = tokio::fs::File::create(path).await.unwrap();
                        write_packed_line(&mut fi, serde_json::to_vec(&results).unwrap())
                            .await
                            .unwrap();

                        remote_loads.fetch_add(1, atomic::ORDERING);
                        Ok(())
                    }
                    .boxed()
                })
                .build()
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote);

        // Prime the cache with the run ID offloaded.
        fs.insert_offloaded(run_id.clone()).await.unwrap();

        let mut join_set = tokio::task::JoinSet::new();

        for results in other_results.iter() {
            let run_id = run_id.clone();
            let fs = fs.clone();
            let results = results.clone();
            join_set.spawn(async move {
                fs.dump(&run_id, results).await.unwrap();
            });
        }

        while let Some(join_handle) = join_set.join_next().await {
            join_handle.unwrap();
        }

        assert_eq!(remote_loads.load(atomic::ORDERING), 1);

        let actual_results = get_results(&fs, &run_id).await.unwrap();
        let mut actual_results = actual_results.decode().unwrap();
        actual_results.sort_by_key(|x| match x {
            Results(x) => x[0].work_id,
            _ => unreachable!(),
        });

        let expected_results = {
            let mut results = vec![remote_results];
            results.extend(other_results);
            results
        };

        assert_eq!(actual_results.len(), expected_results.len());
        assert_eq!(actual_results, expected_results);
    }

    #[tokio::test]
    async fn offload_test_results_file() {
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        let remote = OneWriteFakePersister::default();

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote.clone());

        // Dump the data. Nothing should be synced to the remote yet.
        {
            fs.dump(&run_id, results.clone()).await.unwrap();

            assert!(!remote.has_data());
            assert_eq!(remote.stores(), 0);
            assert_eq!(remote.loads(), 0);
        }

        // Run the offload job. Only the results should be stored.
        {
            let OffloadSummary { offloaded_run_ids } = fs
                .run_offload_job(OffloadConfig::new(Duration::ZERO))
                .await
                .unwrap();

            assert_eq!(offloaded_run_ids, vec![run_id.clone()]);

            assert!(remote.has_data());
            assert_eq!(remote.stores(), 1);
            assert_eq!(remote.loads(), 0);

            let file_size = tokio::fs::metadata(fs.get_path(&run_id))
                .await
                .unwrap()
                .len();
            assert_eq!(file_size, 0);
        }

        // Now when we load the results, we should force a fetch of the remote.
        {
            let tests = get_results(&fs, &run_id).await.unwrap();
            assert_eq!(tests.decode().unwrap(), vec![results]);

            assert!(remote.has_data());
            assert_eq!(remote.stores(), 1);
            assert_eq!(remote.loads(), 1);
        }
    }

    #[n_times(50)]
    #[tokio::test]
    async fn race_dump_new_results_and_offload_job() {
        // We want to race the offload-results job and the introduction of new results to the
        // persistence layers. We should end up in a consistent state, and fetches of the results
        // should succeed, regardless of who wins.
        let run_id = RunId("test-run-id".to_string());

        let results1 = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);
        let results2 = Results(vec![
            AssociatedTestResults::fake(wid(3), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(4), vec![TestResult::fake()]),
        ]);

        // Two cases:
        //   - Offload job wins
        //     - the remote sees one result to load/store
        //   - Write-results job win
        //     - the remote sees two results to load/store

        let remote = OneWriteFakePersister::default();
        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote.clone());

        // Dump the first results line.
        {
            let fs = fs.clone();
            fs.dump(&run_id, results1.clone()).await.unwrap();
        }

        let offload_config = OffloadConfig::new(Duration::ZERO);

        // Run both tasks.
        {
            let offload_task = {
                let fs = fs.clone();
                let run_id = run_id.clone();
                async move {
                    let OffloadSummary { offloaded_run_ids } =
                        fs.run_offload_job(offload_config).await.unwrap();

                    assert_eq!(offloaded_run_ids, &[run_id]);
                }
            };

            let dump_results_task = {
                let fs = fs.clone();
                let run_id = run_id.clone();
                let results2 = results2.clone();
                async move { fs.dump(&run_id, results2).await.unwrap() }
            };

            if i % 2 == 0 {
                tokio::join!(dump_results_task, offload_task);
            } else {
                tokio::join!(offload_task, dump_results_task);
            }
        }

        // Now, loads of the results should succeed with both.
        let results = get_results(&fs, &run_id).await.unwrap();
        let results = results.decode().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], results1);
        assert_eq!(results[1], results2);

        assert_eq!(remote.stores(), 1);
        assert_eq!(remote.loads(), 1);

        let stored_in_remote = remote.get_data().unwrap();
        let stored_in_remote: Vec<ResultsLine> =
            std::io::BufRead::split(stored_in_remote.as_slice(), b'\n')
                .map(|line| serde_json::from_slice(&line.unwrap()).unwrap())
                .collect();

        match stored_in_remote.as_slice() {
            [r1] => {
                assert_eq!(r1, &results1);
            }
            [r1, r2] => {
                assert_eq!(r1, &results1);
                assert_eq!(r2, &results2);
            }
            _ => unreachable!(),
        }
    }

    #[n_times(100)]
    #[tokio::test]
    async fn race_get_results_and_offload_job() {
        // We want to race the offload-results job and the fetching of results to the
        // persistence layers. We should end up in a consistent state, and fetches of the results
        // should succeed, regardless of who wins.
        let run_id = RunId("test-run-id".to_string());

        let results = Results(vec![
            AssociatedTestResults::fake(wid(1), vec![TestResult::fake()]),
            AssociatedTestResults::fake(wid(2), vec![TestResult::fake()]),
        ]);

        // Two cases:
        //   - Offload job wins
        //     - the remote sees one result to load/store, or nothing at all.
        //   - Get-results job win
        //     - the remote sees one results in store, no loads

        let remote = OneWriteFakePersister::default();
        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), 10, remote.clone());

        // Dump the first results line.
        {
            let fs = fs.clone();
            fs.dump(&run_id, results.clone()).await.unwrap();
        }

        let offload_config = OffloadConfig::new(Duration::ZERO);

        // Run both tasks.
        let offload_summary;
        {
            let offload_task = {
                let fs = fs.clone();
                async move { fs.run_offload_job(offload_config).await.unwrap() }
            };

            let fetch_results_task = {
                let fs = fs.clone();
                let run_id = run_id.clone();
                let results = results.clone();
                async move {
                    let real_results = get_results(&fs, &run_id).await.unwrap();
                    let real_results = real_results.decode().unwrap();
                    assert_eq!(real_results, vec![results])
                }
            };

            if i % 2 == 0 {
                (offload_summary, ()) = tokio::join!(offload_task, fetch_results_task);
            } else {
                ((), offload_summary) = tokio::join!(fetch_results_task, offload_task);
            }
        }

        let OffloadSummary { offloaded_run_ids } = offload_summary;
        match remote.stores() {
            // Offload job completed.
            //
            // Just because the offload job completed, does not mean it won first and happened
            // before the read of the results. It could have happened after the read of the results,
            // since we've configured a zero-time offload job. As such, we may see either one load
            // from the remote (if the offload job finished before the read), or no loads at all
            // (if the offload job was after the read).
            1 => {
                assert!(remote.loads() == 1 || remote.loads() == 0);
                let stored_in_remote = remote.get_data().unwrap();
                let stored_in_remote: Vec<ResultsLine> =
                    std::io::BufRead::split(stored_in_remote.as_slice(), b'\n')
                        .map(|line| serde_json::from_slice(&line.unwrap()).unwrap())
                        .collect();

                assert_eq!(stored_in_remote.len(), 1);
                assert_eq!(stored_in_remote[0], results);

                assert_eq!(offloaded_run_ids, &[run_id]);
            }
            // Get-results job won, nothing should be offloaded at all.
            0 => {
                assert_eq!(remote.loads(), 0);
                assert!(offloaded_run_ids.is_empty());
            }
            _ => unreachable!(),
        }
    }
}
