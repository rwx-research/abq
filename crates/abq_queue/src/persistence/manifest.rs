//! Persistence of manifests across test runs.

mod fs;
mod in_memory;

pub use fs::FilesystemPersistor;
pub use in_memory::InMemoryPersistor;

use std::sync::{atomic::AtomicBool, Arc};

use abq_utils::{
    atomic,
    error::LocatedError,
    net_protocol::{
        entity::Tag,
        workers::{RunId, WorkerTest},
    },
};
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ManifestView {
    items: Vec<WorkerTest>,
    assigned_entities: Vec<Tag>,
}

impl ManifestView {
    #[inline]
    pub const fn new(items: Vec<WorkerTest>, assigned_entities: Vec<Tag>) -> Self {
        Self {
            items,
            assigned_entities,
        }
    }

    fn get_partition_for_entity(self, entity_tag: Tag) -> Vec<WorkerTest> {
        let Self {
            mut items,
            assigned_entities,
        } = self;
        let mut end = 0;
        for (i, entity) in assigned_entities.into_iter().enumerate() {
            if entity == entity_tag {
                items.swap(end, i);
                end += 1;
            }
        }
        items.truncate(end);
        items
    }
}

type Result<T> = std::result::Result<T, LocatedError>;

#[async_trait]
pub trait PersistentManifest: Send + Sync {
    async fn dump(&self, run_id: &RunId, view: ManifestView) -> Result<()>;
    async fn get_partition_for_entity(
        &self,
        run_id: &RunId,
        entity_tag: Tag,
    ) -> Result<Vec<WorkerTest>>;
    fn boxed_clone(&self) -> Box<dyn PersistentManifest>;
}

pub struct SharedPersistManifest(Box<dyn PersistentManifest>);

impl Clone for SharedPersistManifest {
    fn clone(&self) -> Self {
        Self(self.0.boxed_clone())
    }
}

impl SharedPersistManifest {
    pub fn borrowed(&self) -> &dyn PersistentManifest {
        &*self.0
    }

    pub async fn get_partition_for_entity(
        &self,
        run_id: &RunId,
        entity_tag: Tag,
    ) -> Result<Vec<WorkerTest>> {
        self.0.get_partition_for_entity(run_id, entity_tag).await
    }
}

/// A cell associated with a manifest-persistence task that demarcates when a manifest has been
/// persisted.
#[derive(Clone, Debug)]
pub struct ManifestPersistedCell(Arc<AtomicBool>);

impl ManifestPersistedCell {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    fn set_persisted(&self) {
        self.0.store(true, atomic::ORDERING);
    }

    /// Atomically determines whether the manifest is persisted under the manifest persistence task.
    ///
    /// Once a manifest is persisted, it is irreversible. As such, a response of `true` from this
    /// functions means it is safe to query [PersistentManifest::get_partition_for_entity] any time
    /// after the response.
    pub fn is_persisted(&self) -> bool {
        self.0.load(atomic::ORDERING)
    }
}

#[derive(Debug)]
pub struct PersistManifestPlan<'a> {
    cell: ManifestPersistedCell,
    run_id: &'a RunId,
    view: ManifestView,
}

pub fn build_persistence_plan(
    run_id: &RunId,
    view: ManifestView,
) -> (ManifestPersistedCell, PersistManifestPlan<'_>) {
    let cell = ManifestPersistedCell::new();
    let plan = PersistManifestPlan {
        cell: cell.clone(),
        run_id,
        view,
    };
    (cell, plan)
}

/// Returns a future that persists a manifest.
pub async fn make_persistence_task(
    persist_manifest: &dyn PersistentManifest,
    plan: PersistManifestPlan<'_>,
) -> Result<()> {
    let PersistManifestPlan { cell, run_id, view } = plan;
    persist_manifest.dump(run_id, view).await?;
    cell.set_persisted();
    Ok(())
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use abq_run_n_times::n_times;
    use abq_test_utils::spec;
    use abq_utils::net_protocol::{
        entity::Tag,
        workers::{RunId, WorkerTest, INIT_RUN_NUMBER},
    };

    use super::{
        build_persistence_plan, in_memory::InMemoryPersistor, make_persistence_task, ManifestView,
    };

    #[n_times(1000)]
    #[tokio::test]
    async fn eventually_persists() {
        let run_id = &RunId::unique();
        let test1 = WorkerTest::new(spec(1), INIT_RUN_NUMBER);
        let test2 = WorkerTest::new(spec(2), INIT_RUN_NUMBER);
        let tag1 = Tag::runner(1, 1);
        let tag2 = Tag::runner(2, 2);
        let view = ManifestView::new(vec![test1.clone(), test2], vec![tag1, tag2]);

        let persist_manifest = InMemoryPersistor::shared();

        let (cell, plan) = build_persistence_plan(run_id, view);

        let task = make_persistence_task(persist_manifest.borrowed(), plan);
        let persister = persist_manifest.borrowed();

        let wait_and_read_task = async move {
            loop {
                if cell.is_persisted() {
                    return persister.get_partition_for_entity(run_id, tag1).await;
                } else {
                    tokio::time::sleep(Duration::from_micros(10)).await;
                }
            }
        };

        let (task_result, partition) = tokio::join!(task, wait_and_read_task);

        assert!(task_result.is_ok(), "{task_result:?}");
        assert!(partition.is_ok(), "{partition:?}");
        assert_eq!(partition.unwrap(), vec![test1]);
    }
}
