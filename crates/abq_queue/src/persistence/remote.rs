//! Utilities for remote persistence of [results] and [manifest]s.
//!
//! [results]: super::results
//! [manifest]: super::manifest

use std::{path::Path, sync::Arc};

use abq_utils::{error::OpaqueResult, net_protocol::workers::RunId};
use async_trait::async_trait;

#[cfg(feature = "s3")]
mod s3;

#[cfg(feature = "s3")]
pub use s3::{S3Client, S3Persister};

mod noop;
pub use noop::NoopPersister;

mod custom;
pub use custom::CustomPersister;

#[cfg(test)]
mod fake;
#[cfg(test)]
pub use fake::error as fake_error;
#[cfg(test)]
pub use fake::unreachable as fake_unreachable;
#[cfg(test)]
pub use fake::FakePersister;
#[cfg(test)]
pub use fake::OneWriteFakePersister;

use super::run_state::RunState;
use super::run_state::SerializableRunState;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PersistenceKind {
    Manifest,
    Results,
    RunState,
}

impl PersistenceKind {
    fn kind_str(&self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Results => "results",
            Self::RunState => "run_state",
        }
    }

    fn file_extension(&self) -> &'static str {
        match self {
            Self::Manifest => "json",
            Self::Results => "jsonl",
            Self::RunState => "json",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LoadedRunState {
    Found(RunState),
    NotFound,
    IncompatibleSchemaVersion { found: u32, expected: u32 },
}

#[async_trait]
pub trait RemotePersistence {
    /// Stores a file from the local filesystem to the remote persistence.
    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()>;

    /// Loads a file from the remote persistence to the local filesystem.
    /// The given local path must have all intermediate directories already created.
    async fn load_to_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()>;

    /// Stores a [RunState] to the remote persistence.
    async fn store_run_state(
        &self,
        run_id: &RunId,
        state: SerializableRunState,
    ) -> OpaqueResult<()>;

    /// Tries to load a [RunState] from the remote persistence.
    async fn try_load_run_state(&self, run_id: &RunId) -> OpaqueResult<LoadedRunState>;

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync>;
}

/// A shared wrapper around [RemotePersister].
///
/// Clones are cheap, as they are atomically reference counted.
#[derive(Clone)]
#[repr(transparent)]
pub struct RemotePersister(Arc<Box<dyn RemotePersistence + Send + Sync>>);

impl<T> From<T> for RemotePersister
where
    T: RemotePersistence + Send + Sync + 'static,
{
    fn from(persister: T) -> Self {
        Self::new(persister)
    }
}

impl RemotePersister {
    pub fn new(persister: impl RemotePersistence + Send + Sync + 'static) -> RemotePersister {
        RemotePersister(Arc::new(Box::new(persister)))
    }

    pub async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        self.0.store_from_disk(kind, run_id, from_local_path).await
    }

    pub async fn load_to_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        self.0.load_to_disk(kind, run_id, into_local_path).await
    }

    pub async fn store_run_state(
        &self,
        run_id: &RunId,
        state: SerializableRunState,
    ) -> OpaqueResult<()> {
        self.0.store_run_state(run_id, state).await
    }

    pub async fn try_load_run_state(&self, run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        self.0.try_load_run_state(run_id).await
    }
}
