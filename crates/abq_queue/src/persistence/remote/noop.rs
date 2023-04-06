use std::path::Path;

use abq_utils::{
    error::{ErrorLocation, OpaqueResult},
    here,
    net_protocol::workers::RunId,
};
use async_trait::async_trait;

use super::{PersistenceKind, RemotePersistence};

#[derive(Clone, Default)]
pub struct NoopPersister;

impl NoopPersister {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RemotePersistence for NoopPersister {
    async fn store_from_disk(
        &self,
        _kind: PersistenceKind,
        _run_id: &RunId,
        _from_local_path: &Path,
    ) -> OpaqueResult<()> {
        Ok(())
    }

    async fn load(
        &self,
        _kind: PersistenceKind,
        _run_id: &RunId,
        _into_local_path: &Path,
    ) -> OpaqueResult<()> {
        Err("NoopPersister does not support loading.".located(here!()))
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}
