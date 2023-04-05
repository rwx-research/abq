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
use tokio::fs;

use crate::persistence::remote::{PersistenceKind, RemotePersister};

use super::{ManifestView, PersistentManifest, Result, SharedPersistManifest};

/// Persists manifests on a filesystem, encoding/decoding to JSON.
#[derive(Clone)]
pub struct FilesystemPersistor {
    root: PathBuf,
    remote: RemotePersister,
}

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
}

#[async_trait]
impl PersistentManifest for FilesystemPersistor {
    async fn dump(&self, run_id: &RunId, view: ManifestView) -> Result<()> {
        let path = self.get_path(run_id);

        let write_task = {
            let path = path.clone();
            move || {
                let fd = std::fs::File::create(path).located(here!())?;
                serde_json::to_writer(fd, &view).located(here!())?;
                Ok(())
            }
        };

        tokio::task::spawn_blocking(write_task)
            .await
            .located(here!())??;

        self.remote
            .store_from_disk(PersistenceKind::Manifest, run_id, &path)
            .await?;

        Ok(())
    }

    async fn get_partition_for_entity(
        &self,
        run_id: &RunId,
        entity_tag: Tag,
    ) -> Result<Vec<WorkerTest>> {
        let path = self.get_path(run_id);

        let packed = fs::read(path).await.located(here!())?;

        let view: ManifestView = serde_json::from_slice(&packed).located(here!())?;

        Ok(view.get_partition_for_entity(entity_tag))
    }

    fn boxed_clone(&self) -> Box<dyn PersistentManifest> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

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
        remote::{FakePersister, NoopPersister, PersistenceKind},
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
        assert!(res.is_ok());

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
                |_, _, _| unreachable!(),
                {
                    let view = view.clone();
                    move |kind, run_id, path| {
                        assert_eq!(kind, PersistenceKind::Manifest);
                        assert_eq!(run_id.0, "run-id");
                        assert_eq!(path, tempdir_path.join("run-id.manifest.json"));
                        let buf = std::fs::read_to_string(path).unwrap();
                        let loaded_view = serde_json::from_slice(&buf.as_bytes()).unwrap();
                        assert_eq!(view, loaded_view);
                        Ok(())
                    }
                },
                |_, _, _| unreachable!(),
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
                |_, _, _| unreachable!(),
                |_, _, _| Err("i failed".located(here!())),
                |_, _, _| unreachable!(),
            ),
        );

        let res = fs.dump(&run_id, view).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err.to_string().contains("i failed"));
    }
}
