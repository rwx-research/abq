use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use abq_utils::atomic;
use abq_utils::error::ErrorLocation;
use abq_utils::here;
use abq_utils::{error::OpaqueResult, net_protocol::workers::RunId};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;

use crate::persistence::run_state::SerializableRunState;

use super::{LoadedRunState, PersistenceKind, RemotePersistence};

type OnStoreFromDisk = Box<
    dyn Fn(PersistenceKind, RunId, PathBuf) -> BoxFuture<'static, OpaqueResult<()>> + Send + Sync,
>;
type OnLoadToDisk = Box<
    dyn Fn(PersistenceKind, RunId, PathBuf) -> BoxFuture<'static, OpaqueResult<()>> + Send + Sync,
>;
type OnStoreRunState =
    Box<dyn Fn(&RunId, SerializableRunState) -> BoxFuture<'static, OpaqueResult<()>> + Send + Sync>;
type OnTryLoadRunState =
    Box<dyn Fn(&RunId) -> BoxFuture<'static, OpaqueResult<LoadedRunState>> + Send + Sync>;

#[derive(Clone)]
pub struct FakePersister {
    on_store_from_disk: Arc<OnStoreFromDisk>,
    on_load_to_disk: Arc<OnLoadToDisk>,
    on_store_run_state: Arc<OnStoreRunState>,
    on_try_load_run_state: Arc<OnTryLoadRunState>,
}

pub async fn unreachable(_x: PersistenceKind, _y: RunId, _z: PathBuf) -> OpaqueResult<()> {
    unreachable!()
}

#[track_caller]
pub fn error(_x: PersistenceKind, _y: RunId, _z: PathBuf) -> BoxFuture<'static, OpaqueResult<()>> {
    async { Err("error".located(here!())) }.boxed()
}

pub struct FakePersisterBuilder {
    on_store_from_disk: OnStoreFromDisk,
    on_load_to_disk: OnLoadToDisk,
    on_store_run_state: OnStoreRunState,
    on_try_load_run_state: OnTryLoadRunState,
}

impl FakePersisterBuilder {
    pub fn build(self) -> FakePersister {
        FakePersister {
            on_store_from_disk: Arc::new(self.on_store_from_disk),
            on_load_to_disk: Arc::new(self.on_load_to_disk),
            on_store_run_state: Arc::new(self.on_store_run_state),
            on_try_load_run_state: Arc::new(self.on_try_load_run_state),
        }
    }

    pub fn on_store_from_disk<F>(mut self, f: F) -> Self
    where
        F: Fn(PersistenceKind, RunId, PathBuf) -> BoxFuture<'static, OpaqueResult<()>>
            + Send
            + Sync
            + 'static,
    {
        self.on_store_from_disk = Box::new(f);
        self
    }

    pub fn on_load_to_disk<F>(mut self, f: F) -> Self
    where
        F: Fn(PersistenceKind, RunId, PathBuf) -> BoxFuture<'static, OpaqueResult<()>>
            + Send
            + Sync
            + 'static,
    {
        self.on_load_to_disk = Box::new(f);
        self
    }

    pub fn on_store_run_state<F>(mut self, f: F) -> Self
    where
        F: Fn(&RunId, SerializableRunState) -> BoxFuture<'static, OpaqueResult<()>>
            + Send
            + Sync
            + 'static,
    {
        self.on_store_run_state = Box::new(f);
        self
    }

    pub fn on_try_load_run_state<F>(mut self, f: F) -> Self
    where
        F: Fn(&RunId) -> BoxFuture<'static, OpaqueResult<LoadedRunState>> + Send + Sync + 'static,
    {
        self.on_try_load_run_state = Box::new(f);
        self
    }
}

impl FakePersister {
    pub fn builder() -> FakePersisterBuilder {
        FakePersisterBuilder {
            on_store_from_disk: Box::new(|_, _, _| async { unimplemented!() }.boxed()),
            on_load_to_disk: Box::new(|_, _, _| async { unimplemented!() }.boxed()),
            on_store_run_state: Box::new(|_, _| async { unimplemented!() }.boxed()),
            on_try_load_run_state: Box::new(|_| async { unimplemented!() }.boxed()),
        }
    }
}

#[async_trait]
impl RemotePersistence for FakePersister {
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
        (self.on_load_to_disk)(kind, run_id.clone(), into_local_path.to_owned()).await
    }

    async fn store_run_state(
        &self,
        run_id: &RunId,
        run_state: SerializableRunState,
    ) -> OpaqueResult<()> {
        (self.on_store_run_state)(run_id, run_state).await
    }

    async fn try_load_run_state(&self, run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        (self.on_try_load_run_state)(run_id).await
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
        Err("FakePersister does not support storing run state.".located(here!()))
    }

    async fn try_load_run_state(&self, _run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        Err("FakePersister does not support loading run state.".located(here!()))
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}
