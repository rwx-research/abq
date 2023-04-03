use std::path::Path;

use abq_utils::{error::OpaqueResult, net_protocol::workers::RunId};
use async_trait::async_trait;

use super::{PersistenceKind, RemotePersistence};

#[derive(Clone)]
pub struct FakePersister<OnStore, OnLoad> {
    on_store: OnStore,
    on_load: OnLoad,
}

impl<OnStore, OnLoad> FakePersister<OnStore, OnLoad>
where
    OnStore: Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync,
    OnLoad: Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync,
{
    pub fn new(on_store: OnStore, on_load: OnLoad) -> Self {
        Self { on_store, on_load }
    }
}

#[async_trait]
impl<OnStore, OnLoad> RemotePersistence for FakePersister<OnStore, OnLoad>
where
    OnStore: Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync + Clone + 'static,
    OnLoad: Fn(PersistenceKind, &RunId, &Path) -> OpaqueResult<()> + Send + Sync + Clone + 'static,
{
    async fn store(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        (self.on_store)(kind, run_id, from_local_path)
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
