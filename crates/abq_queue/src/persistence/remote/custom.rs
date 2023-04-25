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
/// <command> <..args> <action=load|store|exists> <kind=manifest|results> <run_id> <path?>
/// ```
///
/// The behavior for each action should be as follows:
/// - `exists`: the process must print "true" to stdout if the data for a run_id exists,
///   or "false" otherwise.
///   Output will be trimmed, but nothing else should be written to stdout.
/// - `load`: the process must load the remote data into `path`.
/// - `store`: the process must store the remote data into `path`.
///
/// `path` is only present if the action is `load` or `store`.
///
/// The process must exit with a zero exit code on success, and a non-zero exit code on failure.
/// If the process exits with a non-zero exit code, the error message will be the contents of the
/// process's standard error.
#[derive(Clone)]
pub struct CustomPersister {
    command: String,
    head_args: Vec<String>,
}

#[derive(Debug)]
enum Action {
    Load,
    Store,
    Exists,
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
        path: Option<&Path>,
    ) -> OpaqueResult<Vec<u8>> {
        tracing::info!(?path, ?action, "calling with");

        let action = match action {
            Action::Load => "load",
            Action::Store => "store",
            Action::Exists => "exists",
        };

        let mut cmd = tokio::process::Command::new(&self.command);
        cmd.args(&self.head_args);
        cmd.arg(action);
        cmd.arg(kind.kind_str());
        cmd.arg(run_id.to_string());
        if let Some(path) = path {
            cmd.arg(path);
        }

        let std::process::Output {
            status,
            stdout,
            stderr,
        } = cmd.output().await.located(here!())?;

        if !status.success() {
            return Err(format!(
                "custom persister exited with non-zero exit code: {}",
                String::from_utf8_lossy(&stderr)
            ))
            .located(here!());
        }

        Ok(stdout)
    }
}

#[async_trait]
impl RemotePersistence for CustomPersister {
    async fn load_to_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        path: &Path,
    ) -> OpaqueResult<()> {
        self.call(Action::Load, kind, run_id, Some(path)).await?;
        Ok(())
    }

    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        path: &Path,
    ) -> OpaqueResult<()> {
        self.call(Action::Store, kind, run_id, Some(path)).await?;
        Ok(())
    }

    async fn has_run_id(&self, run_id: &RunId) -> OpaqueResult<bool> {
        let output = self
            .call(Action::Exists, PersistenceKind::Manifest, run_id, None)
            .await?;
        let output = String::from_utf8_lossy(&output);
        let output = output.trim();
        match output {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!(
                "custom persister returned invalid output: {}",
                output
            ))
            .located(here!()),
        }
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
            .load_to_disk(
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
            .load_to_disk(
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

    #[tokio::test]
    async fn exists_yes() {
        let fi = write_js(indoc!(
            r#"
            if (process.argv[2] !== "exists") process.exit(1);
            if (process.argv[3] !== "manifest") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            console.log("true")
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let exists = persister
            .has_run_id(&RunId("run-id".to_string()))
            .await
            .unwrap();

        assert!(exists);
    }

    #[tokio::test]
    async fn exists_no() {
        let fi = write_js(indoc!(
            r#"
            if (process.argv[2] !== "exists") process.exit(1);
            if (process.argv[3] !== "manifest") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            console.log("false")
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let exists = persister
            .has_run_id(&RunId("run-id".to_string()))
            .await
            .unwrap();

        assert!(!exists);
    }

    #[tokio::test]
    async fn exists_bad_message() {
        let fi = write_js(indoc!(
            r#"
            if (process.argv[2] !== "exists") process.exit(1);
            if (process.argv[3] !== "manifest") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            console.log("maybe")
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let exists_err = persister
            .has_run_id(&RunId("run-id".to_string()))
            .await
            .unwrap_err();

        assert!(exists_err.to_string().contains("invalid output: maybe"));
    }

    #[tokio::test]
    async fn exists_error() {
        let fi = write_js(indoc!(
            r#"
            console.error("I have failed");
            process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let err = persister
            .has_run_id(&RunId("run-id".to_string()))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("I have failed"));
    }
}
