use std::path::PathBuf;

use abq_utils::{
    error::ResultLocation,
    here, log_assert,
    net_protocol::{
        entity::{self, Entity, Tag, WorkerRunner},
        workers::{RunId, WorkerTest},
    },
};
use async_trait::async_trait;
use tokio::fs;

use super::{ManifestView, PersistentManifest, Result};

/// Persists manifests on a filesystem, encoding/decoding with [bincode].
pub struct FilesystemPersistor {
    root: PathBuf,
}

impl FilesystemPersistor {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn get_path(&self, run_id: &RunId) -> PathBuf {
        let run_id = &run_id.0;

        // TODO: I (Ayaz) would like to not have to use json here, but currently the metadata map is
        // stored as an opauq JSON blob.
        self.root.join(format!("{run_id}.manifest.json"))
    }
}

#[async_trait]
impl PersistentManifest for FilesystemPersistor {
    async fn dump(&self, run_id: &RunId, view: ManifestView) -> Result<()> {
        let path = self.get_path(run_id);

        let packed = serde_json::to_vec(&view).located(here!())?;

        fs::write(path, packed).await.located(here!())?;

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
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use abq_test_utils::{spec, wid};
    use abq_utils::net_protocol::{
        entity::{Entity, Tag},
        workers::{RunId, WorkerTest, INIT_RUN_NUMBER},
    };

    use crate::persistence::manifest::{ManifestView, PersistentManifest};

    use super::FilesystemPersistor;

    #[test]
    fn get_path() {
        let fs = FilesystemPersistor::new("/tmp");
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

        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__");

        let err = fs.dump(&RunId::unique(), view).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn load_from_nonexistent_file_is_error() {
        let fs = FilesystemPersistor::new("__zzz_this_is_not_a_subdir__");

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
        let fs = FilesystemPersistor::new(tempdir.path());

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
}
