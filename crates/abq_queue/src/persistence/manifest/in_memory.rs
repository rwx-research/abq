use std::{collections::HashMap, sync::Arc};

use abq_utils::{
    error::ErrorLocation,
    here,
    net_protocol::{
        entity::Tag,
        workers::{RunId, WorkerTest},
    },
};
use async_trait::async_trait;
use tokio::sync::RwLock;

use super::{ManifestView, PersistentManifest, Result, SharedPersistManifest};

#[derive(Default, Clone)]
pub struct InMemoryPersistor {
    manifests: Arc<RwLock<HashMap<RunId, ManifestView>>>,
}

impl InMemoryPersistor {
    pub fn shared() -> SharedPersistManifest {
        SharedPersistManifest(Box::new(Self::default()))
    }
}

#[async_trait]
impl PersistentManifest for InMemoryPersistor {
    async fn dump(&self, run_id: &RunId, view: ManifestView) -> Result<()> {
        self.manifests.write().await.insert(run_id.clone(), view);

        Ok(())
    }

    async fn get_partition_for_entity(
        &self,
        run_id: &RunId,
        entity_tag: Tag,
    ) -> Result<Vec<WorkerTest>> {
        let view: ManifestView = self
            .manifests
            .read()
            .await
            .get(run_id)
            .ok_or_else(|| "manifest not found for run ID".located(here!()))
            .cloned()?;

        Ok(view.get_partition_for_entity(entity_tag))
    }

    fn boxed_clone(&self) -> Box<dyn PersistentManifest> {
        Box::new(self.clone())
    }
}
