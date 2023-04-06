use std::path::Path;

use abq_utils::{error::OpaqueResult, net_protocol::workers::RunId};
use async_trait::async_trait;

use super::{PersistenceKind, RemotePersistence};

#[derive(Clone)]
pub struct FakePersister<OnStoreFromDisk, OnLoad> {
    on_store_from_disk: OnStoreFromDisk,
    on_load: OnLoad,
}

impl<OnStoreFromDisk, OnLoad> FakePersister<OnStoreFromDisk, OnLoad>
where
    OnStoreFromDisk: Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync,
    OnLoad: Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync,
{
    pub fn new(on_store_from_disk: OnStoreFromDisk, on_load: OnLoad) -> Self {
        Self {
            on_store_from_disk,
            on_load,
        }
    }
}

#[async_trait]
impl<OnStoreFromDisk, OnLoad> RemotePersistence for FakePersister<OnStoreFromDisk, OnLoad>
where
    OnStoreFromDisk:
        Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync + Clone + 'static,
    OnLoad: Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync + Clone + 'static,
{
    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        (self.on_store_from_disk)(kind, run_id, from_local_path)
    }

    async fn load(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        (self.on_load)(kind, run_id, into_local_path)
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}
