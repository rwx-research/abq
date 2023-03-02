//! Persistence of manifests across test runs.

#![allow(unused)] // for now

mod fs;

use abq_utils::{
    error::LocatedError,
    net_protocol::{
        entity::{Entity, Tag},
        workers::{RunId, WorkerTest},
    },
};
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ManifestView {
    items: Vec<WorkerTest>,
    assigned_entities: Vec<Tag>,
}

impl ManifestView {
    pub fn new(items: Vec<WorkerTest>, assigned_entities: Vec<Tag>) -> Self {
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
trait PersistentManifest {
    async fn dump(&self, run_id: &RunId, view: ManifestView) -> Result<()>;
    async fn get_partition_for_entity(
        &self,
        run_id: &RunId,
        entity_tag: Tag,
    ) -> Result<Vec<WorkerTest>>;
}
