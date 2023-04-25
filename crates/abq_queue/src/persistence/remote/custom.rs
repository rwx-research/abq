use std::{
    io::{Read, Write},
    path::Path,
};

use abq_utils::{
    error::{OpaqueResult, ResultLocation},
    here,
    net_protocol::workers::RunId,
};
use async_trait::async_trait;

use crate::persistence::run_state::{self, SerializableRunState};

use super::{LoadedRunState, PersistenceKind, RemotePersistence};

/// A custom [remote file persistor][RemotePersistence], whose implementation is defined by an
/// external process.
///
/// The process the custom persistor is [created][CustomPersister::new] with must be an executable
/// in the PATH, and is called in the following form:
///
/// ```bash
/// <command> <..args> <action=load|store> <kind=manifest|results|run_state> <run_id> <path?>
/// ```
///
/// The behavior for each action should be as follows:
/// - `load`: the process must load the remote data into `path`.
/// - `store`: the process must store the remote data into `path`.
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
        tracing::info!(?path, ?action, "calling with");

        let action = match action {
            Action::Load => "load",
            Action::Store => "store",
        };

        let mut cmd = tokio::process::Command::new(&self.command);
        cmd.args(&self.head_args);
        cmd.arg(action);
        cmd.arg(kind.kind_str());
        cmd.arg(run_id.to_string());
        cmd.arg(path);

        let std::process::Output {
            status,
            stdout: _,
            stderr,
        } = cmd.output().await.located(here!())?;

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
    async fn load_to_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        path: &Path,
    ) -> OpaqueResult<()> {
        self.call(Action::Load, kind, run_id, path).await?;
        Ok(())
    }

    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        path: &Path,
    ) -> OpaqueResult<()> {
        self.call(Action::Store, kind, run_id, path).await?;
        Ok(())
    }

    async fn store_run_state(
        &self,
        run_id: &RunId,
        run_state: SerializableRunState,
    ) -> OpaqueResult<()> {
        let write_to_temp_task = move || {
            let mut temp_file = tempfile::NamedTempFile::new().located(here!())?;
            run_state.serialize_to(&mut temp_file).located(here!())?;
            temp_file.flush().located(here!())?;
            Ok(temp_file)
        };

        let temp_file = tokio::task::spawn_blocking(write_to_temp_task)
            .await
            .located(here!())??;

        self.call(
            Action::Store,
            PersistenceKind::RunState,
            run_id,
            temp_file.path(),
        )
        .await?;

        Ok(())
    }

    async fn try_load_run_state(&self, run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        let temp_file =
            tokio::task::spawn_blocking(move || tempfile::NamedTempFile::new().located(here!()))
                .await
                .located(here!())??;

        let load_result = self
            .call(
                Action::Load,
                PersistenceKind::RunState,
                run_id,
                temp_file.path(),
            )
            .await;

        if load_result.is_err() {
            return Ok(LoadedRunState::NotFound);
        }

        let result = tokio::task::spawn_blocking(move || {
            let mut temp_file = temp_file.reopen().located(here!())?;
            let mut bytes = vec![];
            temp_file.read_to_end(&mut bytes).located(here!())?;

            match SerializableRunState::deserialize(&bytes) {
                Ok(run_state) => Ok(LoadedRunState::Found(run_state.into_run_state())),
                Err(run_state::ParseError::IncompatibleSchemaVersion { found, expected }) => {
                    Ok(LoadedRunState::IncompatibleSchemaVersion { found, expected })
                }
                Err(e) => Err(e).located(here!()),
            }
        })
        .await
        .located(here!())?;

        result
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use std::{io::Write, path::Path};

    use abq_utils::net_protocol::workers::RunId;
    use indoc::{formatdoc, indoc};
    use tempfile::NamedTempFile;

    use crate::persistence::{
        remote::{LoadedRunState, RemotePersistence},
        run_state::{RunState, SerializableRunState},
    };

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
    async fn store_run_state_okay() {
        let fi = write_js(indoc!(
            r#"
            if (process.argv[2] !== "store") process.exit(1);
            if (process.argv[3] !== "run_state") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        persister
            .store_run_state(
                &RunId("run-id".to_string()),
                SerializableRunState::new(RunState::fake()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn store_run_state_error() {
        let fi = write_js(indoc!(
            r#"
            console.error("I have failed");
            process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let err = persister
            .store_run_state(
                &RunId("run-id".to_string()),
                SerializableRunState::new(RunState::fake()),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("I have failed"));
    }

    #[tokio::test]
    async fn load_run_state_okay() {
        let run_state = SerializableRunState::new(RunState::fake());

        let fi = write_js(&formatdoc!(
            r#"
            if (process.argv[2] !== "load") process.exit(1);
            if (process.argv[3] !== "run_state") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            const target = process.argv[5];
            require('fs').writeFileSync(target, `{json}`);
            "#,
            json = String::from_utf8_lossy(&run_state.serialize().unwrap()),
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let loaded = persister
            .try_load_run_state(&RunId("run-id".to_string()))
            .await
            .unwrap();

        assert_eq!(loaded, LoadedRunState::Found(run_state.into_run_state()));
    }

    #[tokio::test]
    async fn load_run_state_not_found() {
        let fi = write_js(indoc!(
            r#"
            console.error("I have not found it");
            process.exit(1);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let loaded = persister
            .try_load_run_state(&RunId("run-id".to_string()))
            .await
            .unwrap();

        assert_eq!(loaded, LoadedRunState::NotFound);
    }

    #[tokio::test]
    async fn load_run_state_incompatible_versions() {
        let fi = write_js(&indoc!(
            r#"
            if (process.argv[2] !== "load") process.exit(1);
            if (process.argv[3] !== "run_state") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            const target = process.argv[5];
            require('fs').writeFileSync(target, `{"schema_version": 999}`);
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let loaded = persister
            .try_load_run_state(&RunId("run-id".to_string()))
            .await
            .unwrap();

        assert!(matches!(
            loaded,
            LoadedRunState::IncompatibleSchemaVersion {
                found: 999,
                expected: _
            }
        ));
    }

    #[tokio::test]
    async fn load_run_state_corrupted() {
        let fi = write_js(indoc!(
            r#"
            if (process.argv[2] !== "load") process.exit(1);
            if (process.argv[3] !== "run_state") process.exit(1);
            if (process.argv[4] !== "run-id") process.exit(1);
            const target = process.argv[5];
            require('fs').writeFileSync(target, "this is not json");
            "#,
        ));

        let persister = super::CustomPersister::new("node", vec![fi.path().display().to_string()]);

        let loaded = persister
            .try_load_run_state(&RunId("run-id".to_string()))
            .await;

        assert!(loaded.is_err());
    }
}
