use std::path::PathBuf;

use abq_utils::{
    error::ResultLocation,
    here,
    net_protocol::{
        entity::Tag,
        workers::{RunId, WorkerTest},
    },
};
use async_trait::async_trait;
use fs4::FileExt;

use crate::persistence::remote::{PersistenceKind, RemotePersister};

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
        SharedPersistManifest(Box::new(Self {
            root: root.into(),
            remote: remote.into(),
        }))
    }

    fn get_path(&self, run_id: &RunId) -> PathBuf {
        let run_id = &run_id.0;

        self.root.join(format!("{run_id}.manifest.json"))
    }

    async fn get_locked_file(&self, run_id: &RunId) -> Result<LockedFile> {
        let path = self.get_path(run_id);

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
}

#[async_trait]
impl PersistentManifest for FilesystemPersistor {
    async fn dump(&self, run_id: &RunId, view: ManifestView) -> Result<()> {
        // Write the manifest to disk with exclusive access.
        {
            let locked_file = self.get_locked_file(run_id).await?;
            let write_task = move || serde_json::to_writer(&locked_file, &view).located(here!());

            tokio::task::spawn_blocking(write_task)
                .await
                .located(here!())??
        };

        // Cede control to the remote persister so it can read from the path, while we still have
        // exlusive access to the file.
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
    use std::path::PathBuf;

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
        remote::{fake_unreachable, FakePersister, NoopPersister, PersistenceKind},
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
}
