use std::path::Path;

use abq_utils::{
    error::{ErrorLocation, OpaqueResult},
    here,
    net_protocol::workers::RunId,
};
use async_trait::async_trait;

use crate::persistence::run_state::SerializableRunState;

use super::{LoadedRunState, PersistenceKind, RemotePersistence};

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

    async fn load_to_disk(
        &self,
        _kind: PersistenceKind,
        _run_id: &RunId,
        _into_local_path: &Path,
    ) -> OpaqueResult<()> {
        Err("NoopPersister does not support loading.".located(here!()))
    }

    async fn store_run_state(
        &self,
        _run_id: &RunId,
        _state: SerializableRunState,
    ) -> OpaqueResult<()> {
        Ok(())
    }

    async fn try_load_run_state(&self, _run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        Ok(LoadedRunState::NotFound)
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}
