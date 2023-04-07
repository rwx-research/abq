use std::path::Path;
use std::{future::Future, path::PathBuf};

use abq_utils::error::ErrorLocation;
use abq_utils::here;
use abq_utils::{error::OpaqueResult, net_protocol::workers::RunId};
use async_trait::async_trait;

use super::{PersistenceKind, RemotePersistence};

#[derive(Clone)]
pub struct FakePersister<OnStoreFromDisk, OnLoad> {
    on_store_from_disk: OnStoreFromDisk,
    on_load: OnLoad,
}

#[track_caller]
pub async fn unreachable(_x: PersistenceKind, _y: RunId, _z: PathBuf) -> OpaqueResult<()> {
    unreachable!()
}

#[track_caller]
pub async fn error(_x: PersistenceKind, _y: RunId, _z: PathBuf) -> OpaqueResult<()> {
    Err("error".located(here!()))
}

impl<OnStoreFromDisk, OnStoreFromDiskF, OnLoad, OnLoadF> FakePersister<OnStoreFromDisk, OnLoad>
where
    OnStoreFromDisk: Fn(PersistenceKind, RunId, PathBuf) -> OnStoreFromDiskF + Send + Sync,
    OnStoreFromDiskF: Future<Output = OpaqueResult<()>> + Send + Sync,

    OnLoad: Fn(PersistenceKind, RunId, PathBuf) -> OnLoadF + Send + Sync,
    OnLoadF: Future<Output = OpaqueResult<()>> + Send + Sync,
{
    pub fn new(on_store_from_disk: OnStoreFromDisk, on_load: OnLoad) -> Self {
        Self {
            on_store_from_disk,
            on_load,
        }
    }
}

#[async_trait]
impl<OnStoreFromDisk, OnStoreFromDiskF, OnLoad, OnLoadF> RemotePersistence
    for FakePersister<OnStoreFromDisk, OnLoad>
where
    OnStoreFromDisk:
        Fn(PersistenceKind, RunId, PathBuf) -> OnStoreFromDiskF + Send + Sync + Clone + 'static,
    OnStoreFromDiskF: Future<Output = OpaqueResult<()>> + Send + Sync,

    OnLoad: Fn(PersistenceKind, RunId, PathBuf) -> OnLoadF + Send + Sync + Clone + 'static,
    OnLoadF: Future<Output = OpaqueResult<()>> + Send + Sync,
{
    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        (self.on_store_from_disk)(kind, run_id.clone(), from_local_path.to_owned()).await
    }

    async fn load_to_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        (self.on_load)(kind, run_id.clone(), into_local_path.to_owned()).await
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}
