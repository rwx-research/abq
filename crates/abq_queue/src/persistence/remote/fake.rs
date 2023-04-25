use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{future::Future, path::PathBuf};

use abq_utils::atomic;
use abq_utils::error::ErrorLocation;
use abq_utils::here;
use abq_utils::{error::OpaqueResult, net_protocol::workers::RunId};
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;

use crate::persistence::run_state::SerializableRunState;

use super::{LoadedRunState, PersistenceKind, RemotePersistence};

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
    OnStoreFromDiskF: Future<Output = OpaqueResult<()>> + Send,

    OnLoad: Fn(PersistenceKind, RunId, PathBuf) -> OnLoadF + Send + Sync,
    OnLoadF: Future<Output = OpaqueResult<()>> + Send,
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
    OnStoreFromDiskF: Future<Output = OpaqueResult<()>> + Send,

    OnLoad: Fn(PersistenceKind, RunId, PathBuf) -> OnLoadF + Send + Sync + Clone + 'static,
    OnLoadF: Future<Output = OpaqueResult<()>> + Send,
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

    async fn store_run_state(
        &self,
        _run_id: &RunId,
        _run_state: SerializableRunState,
    ) -> OpaqueResult<()> {
        unimplemented!("FakePersister does not support storing run state.")
    }

    async fn try_load_run_state(&self, _run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        unimplemented!("FakePersister does not support loading run state.")
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}

#[derive(Default, Clone)]
pub struct OneWriteFakePersister {
    stored: Arc<Mutex<Option<Vec<u8>>>>,
    stored_to: Arc<AtomicUsize>,
    loaded_from: Arc<AtomicUsize>,
}

impl OneWriteFakePersister {
    pub fn stores(&self) -> usize {
        self.stored_to.load(atomic::ORDERING)
    }

    pub fn loads(&self) -> usize {
        self.loaded_from.load(atomic::ORDERING)
    }

    pub fn has_data(&self) -> bool {
        self.stored.lock().is_some()
    }

    pub fn get_data(&self) -> Option<Vec<u8>> {
        self.stored.lock().clone()
    }
}

#[async_trait]
impl RemotePersistence for OneWriteFakePersister {
    async fn store_from_disk(
        &self,
        _kind: PersistenceKind,
        _run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        self.stored_to.fetch_add(1, atomic::ORDERING);
        let loaded = tokio::fs::read(from_local_path).await.unwrap();
        let mut locked = self.stored.lock();
        assert!(locked.replace(loaded).is_none());
        Ok(())
    }

    async fn load_to_disk(
        &self,
        _kind: PersistenceKind,
        _run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        let data = match self.stored.lock().clone() {
            Some(data) => data,
            None => return Err("no data in storage".located(here!())),
        };
        self.loaded_from.fetch_add(1, atomic::ORDERING);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(into_local_path)
            .await
            .unwrap();
        file.write_all(&data).await.unwrap();
        file.flush().await.unwrap();
        Ok(())
    }

    async fn store_run_state(
        &self,
        _run_id: &RunId,
        _run_state: SerializableRunState,
    ) -> OpaqueResult<()> {
        unimplemented!("FakePersister does not support storing run state.");
    }

    async fn try_load_run_state(&self, _run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        unimplemented!("FakePersister does not support loading run state.");
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}
