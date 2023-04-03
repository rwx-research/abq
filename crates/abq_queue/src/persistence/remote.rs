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

pub enum PersistenceKind {
    Manifest,
    Results,
}

#[async_trait]
pub trait RemotePersistence {
    /// Stores a file from the local filesystem to the remote persistence.
    async fn store(
        &self,
        kind: PersistenceKind,
        run_id: RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()>;

    /// Loads a file from the remote persistence to the local filesystem.
    /// The given local path must have all intermediate directories already created.
    async fn load(
        &self,
        kind: PersistenceKind,
        run_id: RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()>;
}
