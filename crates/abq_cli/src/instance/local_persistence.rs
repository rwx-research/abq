use std::{
    io,
    path::{Path, PathBuf},
};

use tempfile::TempDir;

pub const ENV_PERSISTED_MANIFESTS_DIR: &str = "ABQ_PERSISTED_MANIFESTS_DIR";
pub const ENV_PERSISTED_RESULTS_DIR: &str = "ABQ_PERISTED_RESULTS_DIR";

pub struct LocalPersistenceConfig {
    manifests_dir: Option<PathBuf>,
    results_dir: Option<PathBuf>,
}

impl LocalPersistenceConfig {
    pub fn new(manifests_dir: Option<PathBuf>, results_dir: Option<PathBuf>) -> Self {
        Self {
            manifests_dir,
            results_dir,
        }
    }

    pub fn build(self) -> io::Result<LocalPersistence> {
        let Self {
            manifests_dir,
            results_dir,
        } = self;

        Ok(LocalPersistence {
            manifests: PersistedDir::new(manifests_dir)?,
            results: PersistedDir::new(results_dir)?,
        })
    }
}

pub struct LocalPersistence {
    manifests: PersistedDir,
    results: PersistedDir,
}

impl LocalPersistence {
    pub fn manifests_dir(&self) -> &Path {
        self.manifests.path()
    }

    pub fn results_dir(&self) -> &Path {
        self.results.path()
    }
}

enum PersistedDir {
    Configured(PathBuf),
    Temp(TempDir),
}

impl PersistedDir {
    pub fn new(dir: Option<PathBuf>) -> io::Result<Self> {
        match dir {
            Some(dir) => Ok(Self::Configured(dir)),
            None => Ok(Self::Temp(TempDir::new()?)),
        }
    }

    pub fn path(&self) -> &Path {
        match self {
            Self::Configured(dir) => dir,
            Self::Temp(dir) => dir.path(),
        }
    }
}
