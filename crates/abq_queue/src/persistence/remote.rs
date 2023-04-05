//! Utilities for remote persistence of [results] and [manifest]s.
//!
//! [results]: super::results
//! [manifest]: super::manifest

use std::path::Path;

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
pub use fake::FakePersister;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PersistenceKind {
    Manifest,
    Results,
}

impl PersistenceKind {
    fn kind_str(&self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Results => "results",
        }
    }

    fn file_extension(&self) -> &'static str {
        match self {
            Self::Manifest => "json",
            Self::Results => "jsonl",
        }
    }
}

#[async_trait]
pub trait RemotePersistence {
    async fn store(&self, kind: PersistenceKind, run_id: &RunId, data: Vec<u8>)
        -> OpaqueResult<()>;

    /// Stores a file from the local filesystem to the remote persistence.
    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()>;

    /// Loads a file from the remote persistence to the local filesystem.
    /// The given local path must have all intermediate directories already created.
    async fn load(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()>;

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync>;
}

#[repr(transparent)]
pub struct RemotePersister(Box<dyn RemotePersistence + Send + Sync>);

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
        RemotePersister(Box::new(persister))
    }

    pub async fn store(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        data: Vec<u8>,
    ) -> OpaqueResult<()> {
        self.0.store(kind, run_id, data).await
    }

    pub async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        self.0.store_from_disk(kind, run_id, from_local_path).await
    }

    pub async fn load(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        self.0.load(kind, run_id, into_local_path).await
    }
}

impl Clone for RemotePersister {
    fn clone(&self) -> Self {
        Self(self.0.boxed_clone())
    }
}
