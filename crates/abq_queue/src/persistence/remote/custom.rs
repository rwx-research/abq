use std::path::Path;

use abq_utils::{
    error::{OpaqueResult, ResultLocation},
    here,
    net_protocol::workers::RunId,
};
use async_trait::async_trait;

use super::{PersistenceKind, RemotePersistence};

/// A custom [remote file persistor][RemotePersistence], whose implementation is defined by an
/// external process.
///
/// The process the custom persistor is [created][CustomPersister::new] with must be an executable
/// in the PATH, and is called in the following form:
///
/// ```bash
/// <command> <..args> <action=load|store> <kind=manifest|results> <run_id> <path_to_load_or_store>
/// ```
///
/// The process must exit with a zero exit code on success, and a non-zero exit code on failure.
///
/// If the process exits with a non-zero exit code, the error message will be the contents of the
/// process's standard error.
#[derive(Clone)]
pub struct CustomPersister {
    command: String,
    head_args: Vec<String>,
}

enum Action {
    Load,
    Store,
}

impl CustomPersister {
    /// Creates a new custom persister from the given command and head arguments.
    ///
    /// The command must be an executable in the PATH, and confirm to the described [calling
    /// convention][CustomPersister].
    pub fn new(command: impl Into<String>, head_args: impl Into<Vec<String>>) -> Self {
        Self {
            command: command.into(),
            head_args: head_args.into(),
        }
    }

    async fn call(
        &self,
        action: Action,
        kind: PersistenceKind,
        run_id: &RunId,
        path: &Path,
    ) -> OpaqueResult<()> {
        let action = match action {
            Action::Load => "load",
            Action::Store => "store",
        };

        let std::process::Output {
            status,
            stdout: _,
            stderr,
        } = tokio::process::Command::new(&self.command)
            .args(&self.head_args)
            .arg(action)
            .arg(kind.kind_str())
            .arg(run_id.to_string())
            .arg(path)
            .output()
            .await
            .located(here!())?;

        if !status.success() {
            return Err(format!(
                "custom persister exited with non-zero exit code: {}",
                String::from_utf8_lossy(&stderr)
            ))
            .located(here!());
        }

        Ok(())
    }
}

#[async_trait]
impl RemotePersistence for CustomPersister {
    async fn load(&self, kind: PersistenceKind, run_id: &RunId, path: &Path) -> OpaqueResult<()> {
        self.call(Action::Load, kind, run_id, path).await
    }

    /// Stores the bytes to a temporary path, then calls [Self::store_from_disk].
    /// If possible, prefer to use [Self::store_from_disk] directly.
    async fn store(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        data: Vec<u8>,
    ) -> OpaqueResult<()> {
        let tempfile = tokio::task::spawn_blocking(tempfile::NamedTempFile::new)
            .await
            .located(here!())?
            .located(here!())?;

        let (tempfile, path) = tempfile.into_parts();
        let mut tempfile = tokio::fs::File::from_std(tempfile);

        use tokio::io::AsyncWriteExt;
        tempfile.write_all(&data).await.located(here!())?;

        let result = self.store_from_disk(kind, run_id, path.as_ref()).await;

        drop(path);

        result
    }

    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        path: &Path,
    ) -> OpaqueResult<()> {
        self.call(Action::Store, kind, run_id, path).await
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use std::{io::Write, path::Path};

    use abq_utils::net_protocol::workers::RunId;
    use indoc::indoc;
    use tempfile::NamedTempFile;

    use crate::persistence::remote::RemotePersistence;

    fn write_js(content: &str) -> NamedTempFile {
        let mut fi = NamedTempFile::new().unwrap();
        fi.write_all(content.as_bytes()).unwrap();
        fi
    }

    #[tokio::test]
    async fn load_okay() {
        let fi = write_js(indoc!(
            r#"
            if (process.argv[2] !== "load") process.exit(1);
            if (process.argv[3] !== "manifest") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            if (process.argv[5] !== "/tmp/foo") process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        persister
            .load(
                super::PersistenceKind::Manifest,
                &RunId("run-id".to_string()),
                Path::new("/tmp/foo"),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn load_error() {
        let fi = write_js(indoc!(
            r#"
            console.error("I have failed");
            process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let err = persister
            .load(
                super::PersistenceKind::Manifest,
                &RunId("run-id".to_string()),
                Path::new("/tmp/foo"),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("I have failed"));
    }

    #[tokio::test]
    async fn store_okay() {
        let fi = write_js(indoc!(
            r#"
            if (process.argv[2] !== "store") process.exit(1);
            if (process.argv[3] !== "manifest") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            if (process.argv[5] !== "/tmp/foo") process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        persister
            .store_from_disk(
                super::PersistenceKind::Manifest,
                &RunId("run-id".to_string()),
                Path::new("/tmp/foo"),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn store_error() {
        let fi = write_js(indoc!(
            r#"
            console.error("I have failed");
            process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let err = persister
            .store_from_disk(
                super::PersistenceKind::Manifest,
                &RunId("run-id".to_string()),
                Path::new("/tmp/foo"),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("I have failed"));
    }
}
