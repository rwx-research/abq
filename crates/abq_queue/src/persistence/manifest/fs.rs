use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

use abq_utils::{
    error::ResultLocation,
    here, log_assert,
    net_protocol::{
        entity::Tag,
        workers::{RunId, WorkerTest},
    },
};
use async_trait::async_trait;
use fs4::FileExt;

use crate::persistence::{
    remote::{PersistenceKind, RemotePersister},
    OffloadConfig, OffloadSummary,
};

use super::{ManifestView, PersistentManifest, Result, SharedPersistManifest};

/// Persists manifests on a filesystem, encoding/decoding to JSON.
#[derive(Clone)]
pub struct FilesystemPersistor {
    root: PathBuf,
    remote: RemotePersister,
}

enum LoadFromDisk {
    Loaded(ManifestView),
    TryLoadFromRemote { locked_file: std::fs::File },
}

type LockedFile = std::fs::File;

impl FilesystemPersistor {
    pub fn new(root: impl Into<PathBuf>, remote: impl Into<RemotePersister>) -> Self {
        Self {
            root: root.into(),
            remote: remote.into(),
        }
    }

    pub fn new_shared(
        root: impl Into<PathBuf>,
        remote: impl Into<RemotePersister>,
    ) -> SharedPersistManifest {
        SharedPersistManifest(Box::new(Self::new(root, remote)))
    }

    fn get_path(&self, run_id: &RunId) -> PathBuf {
        let run_id = &run_id.0;

        self.root.join(format!("{run_id}.manifest.json"))
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
            .strip_suffix(".manifest.json")
            .ok_or_else(|| format!("file name {path:?} does not end with '.results.jsonl'"))
            .located(here!())?;

        Ok(RunId(run_id.to_owned()))
    }

    async fn get_locked_file(&self, run_id: &RunId) -> Result<LockedFile> {
        let path = self.get_path(run_id);
        self.get_locked_file_path(path).await
    }

    async fn get_locked_file_path(&self, path: PathBuf) -> Result<LockedFile> {
        // NB: we use a blocking task here because the fs4 `lock_exclusive` blocks the
        // executor, even when using the async extension.
        //
        // NB: The behavior of `flock` is such that the lock over a file will be released when the
        // file descriptor is dropped.
        tokio::task::spawn_blocking(move || {
            let fi = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)
                .located(here!())?;

            fi.lock_exclusive().located(here!())?;

            Ok(fi)
        })
        .await
        .located(here!())?
    }

    async fn load_manifest_from_disk_or_fallback(&self, run_id: &RunId) -> Result<LoadFromDisk> {
        let locked_file = self.get_locked_file(run_id).await?;

        let load_task = move || {
            match serde_json::from_reader(&locked_file) {
                Ok(view) => Ok(LoadFromDisk::Loaded(view)),
                Err(_) => {
                    // The file is likely empty, or corrupted. We'll try to load from the remote
                    Ok(LoadFromDisk::TryLoadFromRemote { locked_file })
                }
            }
        };

        tokio::task::spawn_blocking(load_task)
            .await
            .located(here!())?
    }

    async fn load_manifest_from_disk(&self, locked_file: LockedFile) -> Result<ManifestView> {
        let load_task = move || {
            let view = serde_json::from_reader(&locked_file).located(here!())?;
            Ok(view)
        };

        tokio::task::spawn_blocking(load_task)
            .await
            .located(here!())?
    }

    async fn load_manifest_into_disk_from_remote(
        &self,
        run_id: &RunId,
        _locked_file_witness: &std::fs::File,
    ) -> Result<()> {
        let path = self.get_path(run_id);

        self.remote
            .load_to_disk(PersistenceKind::Manifest, run_id, &path)
            .await?;

        Ok(())
    }

    async fn load_manifest_view(&self, run_id: &RunId) -> Result<ManifestView> {
        match self.load_manifest_from_disk_or_fallback(run_id).await? {
            LoadFromDisk::Loaded(view) => Ok(view),
            LoadFromDisk::TryLoadFromRemote { locked_file } => {
                self.load_manifest_into_disk_from_remote(run_id, &locked_file)
                    .await?;

                self.load_manifest_from_disk(locked_file).await
            }
        }
    }

    /// Offloads to the remote all non-empty manifest files that are older than
    /// the configured threshold.
    pub async fn run_offload_job(&self, offload_config: OffloadConfig) -> Result<OffloadSummary> {
        let time_now = SystemTime::now();

        let mut manifest_files = tokio::fs::read_dir(&self.root).await.located(here!())?;
        let mut offloaded_run_ids = vec![];

        while let Some(manifest_file) = manifest_files.next_entry().await.located(here!())? {
            let metadata = manifest_file.metadata().await.located(here!())?;
            log_assert!(!metadata.is_dir(), path=?manifest_file.path(), "manifest file is a directory");

            if offload_config.file_eligible_for_offload(&time_now, &metadata)? {
                let path = manifest_file.path();

                let did_offload = self.perform_offload(path, time_now, offload_config).await?;

                if did_offload {
                    let run_id = self.run_id_of_path(&manifest_file.path())?;
                    offloaded_run_ids.push(run_id);
                }
            }
        }

        Ok(OffloadSummary { offloaded_run_ids })
    }

    async fn perform_offload(
        &self,
        path: PathBuf,
        time_now: SystemTime,
        offload_config: OffloadConfig,
    ) -> Result<bool> {
        // Grab an exclusive lock on the file so it isn't changed from under us.
        let locked_file = self.get_locked_file_path(path.clone()).await?;

        let task = move || {
            // We must now check again whether the file is eligible for offload, since it may have been
            // modified since we performed the non-locked check.
            let metadata = locked_file.metadata().located(here!())?;
            if !offload_config.file_eligible_for_offload(&time_now, &metadata)? {
                return Ok(false);
            }

            // The manifest will already have been persisted to the remote when it was first persisted
            // locally, so we can just truncate the file now.

            // Truncate the file to 0 bytes, freeing the disk space.
            // Note that this doesn't free the inode.
            locked_file.set_len(0).located(here!())?;
            locked_file.sync_all().located(here!())?;

            Ok(true)
        };

        let offloaded = tokio::task::spawn_blocking(task).await.located(here!())??;

        Ok(offloaded)
    }
}

#[async_trait]
impl PersistentManifest for FilesystemPersistor {
    async fn dump(&self, run_id: &RunId, view: ManifestView) -> Result<()> {
        // Write the manifest to disk with exclusive access.
        let _locked_file = {
            let mut locked_file = self.get_locked_file(run_id).await?;
            let write_task = move || {
                use std::io::Write;

                serde_json::to_writer(&locked_file, &view).located(here!())?;
                locked_file.flush().located(here!())?;

                Ok(locked_file)
            };

            tokio::task::spawn_blocking(write_task)
                .await
                .located(here!())??
        };

        // Cede control to the remote persister so it can read from the path, while we still have
        // exclusive access to the file.
        {
            let path = self.get_path(run_id);
            self.remote
                .store_from_disk(PersistenceKind::Manifest, run_id, &path)
                .await?;
        }

        Ok(())
    }

    async fn get_partition_for_entity(
        &self,
        run_id: &RunId,
        entity_tag: Tag,
    ) -> Result<Vec<WorkerTest>> {
        let view = self.load_manifest_view(run_id).await?;

        Ok(view.get_partition_for_entity(entity_tag))
    }

    fn boxed_clone(&self) -> Box<dyn PersistentManifest> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use std::{path::PathBuf, time::Duration};

    use abq_run_n_times::n_times;
    use abq_test_utils::spec;
    use abq_utils::{
        error::ErrorLocation,
        here,
        net_protocol::{
            entity::Tag,
            workers::{RunId, WorkerTest, INIT_RUN_NUMBER},
        },
    };

    use crate::persistence::{
        manifest::{ManifestView, PersistentManifest},
        remote::{
            fake_unreachable, FakePersister, NoopPersister, OneWriteFakePersister, PersistenceKind,
        },
        OffloadConfig, OffloadSummary,
    };

    use super::FilesystemPersistor;

    #[test]
    fn get_path() {
        let fs = FilesystemPersistor::new("/tmp", NoopPersister);
        let run_id = RunId("run1".to_owned());
        assert_eq!(
            fs.get_path(&run_id),
            PathBuf::from("/tmp/run1.manifest.json")
        )
    }

    #[tokio::test]
    async fn dump_to_nonexistent_file_is_error() {
        let view = ManifestView {
            items: vec![],
            assigned_entities: vec![],
        };

        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__", NoopPersister);

        let err = fs.dump(&RunId::unique(), view).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn load_from_nonexistent_file_is_error() {
        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__", NoopPersister);

        let err = fs
            .get_partition_for_entity(&RunId::unique(), Tag::runner(1, 1))
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn dump_and_load_manifest() {
        let runner1 = Tag::runner(0, 1);
        let runner2 = Tag::runner(0, 2);

        let test1 = WorkerTest::new(spec(1), INIT_RUN_NUMBER);
        let test2 = WorkerTest::new(spec(2), INIT_RUN_NUMBER);
        let test3 = WorkerTest::new(spec(3), INIT_RUN_NUMBER);
        let test4 = WorkerTest::new(spec(4), INIT_RUN_NUMBER);
        let test5 = WorkerTest::new(spec(5), INIT_RUN_NUMBER);
        let test6 = WorkerTest::new(spec(6), INIT_RUN_NUMBER);

        let items = vec![
            test1.clone(),
            test2.clone(),
            test3.clone(),
            test4.clone(),
            test5.clone(),
            test6.clone(),
        ];
        let assigned_entities = vec![
            runner1, runner1, //
            runner2, runner2, //
            runner1, //
            runner2,
        ];

        let view = ManifestView {
            items,
            assigned_entities,
        };

        let tempdir = tempfile::tempdir().unwrap();
        let fs = FilesystemPersistor::new(tempdir.path(), NoopPersister);

        let run_id = RunId::unique();

        let res = fs.dump(&run_id, view).await;
        assert!(res.is_ok(), "{res:?}");

        let man1 = fs.get_partition_for_entity(&run_id, runner1).await.unwrap();
        assert_eq!(man1, vec![test1.clone(), test2.clone(), test5.clone()]);

        let man2 = fs.get_partition_for_entity(&run_id, runner2).await.unwrap();
        assert_eq!(man2, vec![test3.clone(), test4.clone(), test6.clone()]);

        let man3 = fs
            .get_partition_for_entity(&run_id, Tag::runner(0, 3))
            .await
            .unwrap();
        assert_eq!(man3, vec![]);
    }

    #[tokio::test]
    async fn dump_includes_write_to_remote() {
        let runner1 = Tag::runner(0, 1);
        let runner2 = Tag::runner(0, 2);

        let test1 = WorkerTest::new(spec(1), INIT_RUN_NUMBER);
        let test2 = WorkerTest::new(spec(2), INIT_RUN_NUMBER);

        let items = vec![test1.clone(), test2.clone()];
        let assigned_entities = vec![runner1, runner2];

        let view = ManifestView {
            items,
            assigned_entities,
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();
        let tempdir_path = tempdir.path().to_owned();

        let fs = FilesystemPersistor::new(
            tempdir.path(),
            FakePersister::new(
                {
                    let view = view.clone();
                    move |kind, run_id, path| {
                        let view = view.clone();
                        let tempdir_path = tempdir_path.clone();
                        async move {
                            assert_eq!(kind, PersistenceKind::Manifest);
                            assert_eq!(run_id.0, "run-id");
                            assert_eq!(path, tempdir_path.join("run-id.manifest.json"));
                            let buf = tokio::fs::read_to_string(path).await.unwrap();
                            let loaded_view = serde_json::from_slice(buf.as_bytes()).unwrap();
                            assert_eq!(view, loaded_view);
                            Ok(())
                        }
                    }
                },
                fake_unreachable,
            ),
        );

        let res = fs.dump(&run_id, view).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn failure_to_remote_write_is_error() {
        let view = ManifestView {
            items: vec![],
            assigned_entities: vec![],
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let fs = FilesystemPersistor::new(
            tempdir.path(),
            FakePersister::new(
                |_, _, _| async { Err("i failed".located(here!())) },
                fake_unreachable,
            ),
        );

        let res = fs.dump(&run_id, view).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err.to_string().contains("i failed"));
    }

    #[tokio::test]
    async fn missing_local_loads_from_remote() {
        let runner1 = Tag::runner(0, 1);
        let test1 = WorkerTest::new(spec(1), INIT_RUN_NUMBER);

        let view = ManifestView {
            items: vec![test1.clone()],
            assigned_entities: vec![runner1],
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let fs = FilesystemPersistor::new(
            tempdir.path(),
            FakePersister::new(fake_unreachable, move |_, _, path| {
                let view = view.clone();
                async move {
                    tokio::fs::write(path, serde_json::to_vec(&view).unwrap())
                        .await
                        .unwrap();
                    Ok(())
                }
            }),
        );

        let tests = fs.get_partition_for_entity(&run_id, runner1).await.unwrap();
        assert_eq!(tests, vec![test1]);
    }

    #[tokio::test]
    async fn missing_local_errors_if_remote_errors() {
        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let fs = FilesystemPersistor::new(
            tempdir.path(),
            FakePersister::new(fake_unreachable, |_, _, _| async {
                Err("i failed".located(here!()))
            }),
        );

        let res = fs
            .get_partition_for_entity(&run_id, Tag::runner(0, 1))
            .await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err.to_string().contains("i failed"));
    }

    #[tokio::test]
    async fn prefer_local_over_remote() {
        let runner1 = Tag::runner(0, 1);
        let test1 = WorkerTest::new(spec(1), INIT_RUN_NUMBER);

        let view = ManifestView {
            items: vec![test1.clone()],
            assigned_entities: vec![runner1],
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let fs = FilesystemPersistor::new(
            tempdir.path(),
            FakePersister::new(
                |_, _, _| async { Ok(()) },
                |_, _, path| async move {
                    let fake_view = ManifestView {
                        items: vec![],
                        assigned_entities: vec![],
                    };
                    tokio::fs::write(path, serde_json::to_vec(&fake_view).unwrap())
                        .await
                        .unwrap();
                    Ok(())
                },
            ),
        );

        fs.dump(&run_id, view).await.unwrap();
        let tests = fs.get_partition_for_entity(&run_id, runner1).await.unwrap();
        assert_eq!(tests, vec![test1]);
    }

    #[n_times(100)]
    #[tokio::test]
    async fn race_multiple_reads_with_fetch_from_remote() {
        const N: usize = 10;

        let runners = (1..=N)
            .map(|r| Tag::runner(0, r as u32))
            .collect::<Vec<_>>();
        let tests = (1..=N)
            .map(|t| WorkerTest::new(spec(t), INIT_RUN_NUMBER))
            .collect::<Vec<_>>();

        let view = ManifestView {
            items: tests.clone(),
            assigned_entities: runners.clone(),
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let fs = FilesystemPersistor::new(
            tempdir.path(),
            FakePersister::new(
                |_, _, _| async { Ok(()) },
                move |_, _, path| {
                    let view = view.clone();
                    async move {
                        tokio::fs::write(path, serde_json::to_vec(&view).unwrap())
                            .await
                            .unwrap();
                        Ok(())
                    }
                },
            ),
        );

        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..N {
            let fs = fs.clone();
            let run_id = run_id.clone();

            let runner = runners[i];
            let test = tests[i].clone();

            join_set.spawn(async move {
                let tests = fs.get_partition_for_entity(&run_id, runner).await.unwrap();
                assert_eq!(tests, vec![test]);
            });
        }

        while let Some(result) = join_set.join_next().await {
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn offload_manifest_file() {
        let runner1 = Tag::runner(0, 1);
        let test1 = WorkerTest::new(spec(1), INIT_RUN_NUMBER);

        let view = ManifestView {
            items: vec![test1.clone()],
            assigned_entities: vec![runner1],
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let remote = OneWriteFakePersister::default();

        let fs = FilesystemPersistor::new(tempdir.path(), remote.clone());
        fs.dump(&run_id, view).await.unwrap();

        assert!(remote.stores() == 1);
        assert!(remote.loads() == 0);

        // Run the offloader
        let OffloadSummary { offloaded_run_ids } = fs
            .run_offload_job(OffloadConfig::new(Duration::ZERO))
            .await
            .unwrap();

        let file_size = tokio::fs::metadata(fs.get_path(&run_id))
            .await
            .unwrap()
            .len();
        assert_eq!(file_size, 0);

        assert_eq!(offloaded_run_ids.len(), 1);
        assert_eq!(offloaded_run_ids[0], run_id);

        // Now when we load again, we should force a fetch of the remote.
        let tests = fs.get_partition_for_entity(&run_id, runner1).await.unwrap();
        assert_eq!(tests, vec![test1]);

        assert!(remote.stores() == 1);
        assert!(remote.loads() == 1);
        assert!(remote.has_data());
    }

    #[n_times(1000)]
    #[tokio::test]
    async fn race_dump_new_manifest_and_offload_job() {
        // We want to race the offload-manifests job and the introduction of a new manifest to the
        // persistence layers. We should end up in a consistent state, and fetches of the manifest
        // should succeed, regardless of who wins.

        let runner1 = Tag::runner(0, 1);
        let test1 = WorkerTest::new(spec(1), INIT_RUN_NUMBER);

        let view = ManifestView {
            items: vec![test1.clone()],
            assigned_entities: vec![runner1],
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let remote = OneWriteFakePersister::default();

        let fs = FilesystemPersistor::new(tempdir.path(), remote.clone());
        let dump_task = {
            let fs = fs.clone();
            let run_id = run_id.clone();
            let view = view.clone();
            async move {
                fs.dump(&run_id, view).await.unwrap();
            }
        };

        let offload_task = {
            let fs = fs.clone();
            async move {
                fs.run_offload_job(OffloadConfig::new(Duration::ZERO))
                    .await
                    .unwrap()
            }
        };

        let offloaded_summary;
        if i % 2 == 0 {
            ((), offloaded_summary) = tokio::join!(dump_task, offload_task);
        } else {
            (offloaded_summary, ()) = tokio::join!(offload_task, dump_task);
        }

        // Two cases now:
        //   1. Dump finished first, then was offloaded.
        //   2. Offload finished first, then dump happened and is available locally.
        // Regardless, we should have one store to the remote, and the read of the partition should
        // succeed.

        assert!(remote.stores() == 1);
        assert!(remote.loads() == 0);

        let tests = fs.get_partition_for_entity(&run_id, runner1).await.unwrap();
        assert_eq!(tests, vec![test1]);

        let OffloadSummary { offloaded_run_ids } = offloaded_summary;

        assert!(remote.stores() == 1);
        match remote.loads() {
            1 => {
                // Case 1: offload finished first, so the dump was offloaded.
                assert_eq!(offloaded_run_ids.len(), 1);
                assert_eq!(offloaded_run_ids[0], run_id);
            }
            0 => assert!(offloaded_run_ids.is_empty()),
            _ => unreachable!(),
        }
        assert!(remote.has_data());
    }

    #[n_times(50)]
    #[tokio::test]
    async fn race_get_manifest_and_offload_job() {
        // We want to race the offload-manifests job and the fetching of a manifest to the
        // persistence layers. We should end up in a consistent state, and fetches should
        // succeed, regardless of who wins.

        const N: usize = 10;
        let runners: Vec<_> = (0..N).map(|i| Tag::runner(0, i as u32)).collect();
        let tests: Vec<_> = (0..N)
            .map(|i| WorkerTest::new(spec(i), INIT_RUN_NUMBER))
            .collect();

        let view = ManifestView {
            items: tests.clone(),
            assigned_entities: runners.clone(),
        };

        let run_id = RunId("run-id".to_string());

        let tempdir = tempfile::tempdir().unwrap();

        let remote = OneWriteFakePersister::default();

        let fs = FilesystemPersistor::new(tempdir.path(), remote.clone());
        fs.dump(&run_id, view).await.unwrap();

        let fetch_task = {
            let fs = fs.clone();
            let run_id = run_id.clone();
            let all_tests = tests;
            async move {
                for (i, runner) in runners.into_iter().enumerate() {
                    let tests = fs.get_partition_for_entity(&run_id, runner).await.unwrap();
                    assert_eq!(tests.len(), 1);
                    assert_eq!(tests[0], all_tests[i]);
                }
            }
        };

        let offload_task = {
            let fs = fs.clone();
            async move {
                let OffloadSummary { offloaded_run_ids } = fs
                    .run_offload_job(OffloadConfig::new(Duration::ZERO))
                    .await
                    .unwrap();

                assert!(offloaded_run_ids == [run_id] || offloaded_run_ids.is_empty());
            }
        };

        if i % 2 == 0 {
            tokio::join!(fetch_task, offload_task);
        } else {
            tokio::join!(offload_task, fetch_task);
        }

        // Two cases now:
        //   1. Fetch finished first, then was offloaded.
        //   2. Offload finished first, then fetch happened and is available locally.
        // Regardless, we should have one store to the remote, and the read of the partition should
        // succeed.

        assert!(remote.stores() == 1);
        assert!(remote.loads() == 0 || remote.loads() == 1);
        assert!(remote.has_data());
    }
}
