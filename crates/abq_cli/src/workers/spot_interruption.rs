//! Resuming local runs on RWX Mint after a spot interruption.
//!
//! When Mint is about to lose a spot instance, it writes the task's spot interruption notice
//! file. ABQ then stops, records the tests completed so far in the `spot-interruption` retry
//! action, and exits with [EXIT_CODE]. Mint retries the task on a non-spot instance with that retry action's env and
//! data, and ABQ hydrates its results with the completed tests rather than running them again.

use std::{
    fs, io,
    path::{Path, PathBuf},
    time::Duration,
};

use abq_utils::exit::ExitCode;
use abq_workers::resume::CompletedTest;
use serde_derive::{Deserialize, Serialize};

/// The exit code telling Mint that ABQ stopped because of a spot interruption.
pub const EXIT_CODE: ExitCode = ExitCode::new(86);

const RETRY_ACTION_KEY: &str = "spot-interruption";
const RETRY_ENV_VAR: &str = "RWX_SPOT_INTERRUPTION_RETRY";
const DATA_FILE: &str = "abq/completed-tests.json";
/// Marks that an ABQ run in this task already resumed. Mint's retry data is read-only, and the
/// temporary directory lives as long as the task.
const RESUMED_MARKER: &str = "abq-resumed-after-spot-interruption";
const NOTICE_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Serialize, Deserialize)]
struct RetryData {
    completed_tests: Vec<CompletedTest>,
}

pub struct SpotInterruption {
    notice_file: PathBuf,
    retry_actions_dir: PathBuf,
}

impl SpotInterruption {
    /// Returns [Some] when running in a Mint task that supports spot interruption retries.
    pub fn from_env() -> Option<Self> {
        let notice_file = std::env::var_os("RWX_SPOT_INTERRUPTION_FILE")?;
        let retry_actions_dir = std::env::var_os("RWX_RETRY_ACTIONS")?;
        Some(Self {
            notice_file: notice_file.into(),
            retry_actions_dir: retry_actions_dir.into(),
        })
    }

    /// Resolves once Mint gives notice that the spot instance is about to be interrupted.
    pub async fn notice(&self) {
        while !self.notice_file.exists() {
            tokio::time::sleep(NOTICE_POLL_INTERVAL).await;
        }
    }

    /// The tests completed by the attempt that was interrupted, if this is its retry.
    /// Only the first call in a task returns them; see [take_retry_data].
    pub fn previous_attempt() -> io::Result<Vec<CompletedTest>> {
        if std::env::var(RETRY_ENV_VAR).as_deref() != Ok("true") {
            return Ok(vec![]);
        }
        let Some(retry_data_dir) = std::env::var_os("RWX_RETRY_DATA") else {
            return Ok(vec![]);
        };
        take_retry_data(
            Path::new(&retry_data_dir),
            &std::env::temp_dir().join(RESUMED_MARKER),
        )
    }

    /// Writes the `spot-interruption` retry action, which carries the completed tests to the retry.
    pub fn write_retry_action(&self, completed_tests: Vec<CompletedTest>) -> io::Result<()> {
        write_retry_action(
            &self.retry_actions_dir.join(RETRY_ACTION_KEY),
            completed_tests,
        )
    }
}

/// Reads the retry data unless a run already resumed from it, so that later ABQ runs in the same
/// task, such as Captain retrying failed tests, run their tests rather than resuming again.
fn take_retry_data(retry_data_dir: &Path, resumed_marker: &Path) -> io::Result<Vec<CompletedTest>> {
    if resumed_marker.exists() {
        return Ok(vec![]);
    }
    let data = match fs::read(retry_data_dir.join(DATA_FILE)) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        data => data?,
    };
    fs::write(resumed_marker, "")?;
    let RetryData { completed_tests } = serde_json::from_slice(&data)?;
    Ok(completed_tests)
}

fn write_retry_action(action_dir: &Path, completed_tests: Vec<CompletedTest>) -> io::Result<()> {
    let env_dir = action_dir.join("env");
    fs::create_dir_all(&env_dir)?;
    fs::write(env_dir.join(RETRY_ENV_VAR), "true")?;

    let data_file = action_dir.join("data").join(DATA_FILE);
    fs::create_dir_all(data_file.parent().unwrap())?;
    let file = io::BufWriter::new(fs::File::create(data_file)?);
    serde_json::to_writer(file, &RetryData { completed_tests })?;
    Ok(())
}

#[cfg(test)]
mod test {
    use abq_test_utils::{AssociatedTestResultsBuilder, TestResultBuilder};
    use abq_utils::net_protocol::{runners::Status, workers::WorkId};
    use abq_workers::resume::CompletedTest;

    use super::{take_retry_data, write_retry_action};

    #[test]
    fn retry_action_round_trips_through_retry_data() {
        let dir = tempfile::tempdir().unwrap();
        let action_dir = dir.path().join("spot-interruption");
        let completed = vec![CompletedTest {
            test_id: "spec/a_spec.rb".to_owned(),
            results: AssociatedTestResultsBuilder::new(
                WorkId::new(),
                1,
                [
                    TestResultBuilder::new("spec/a_spec.rb[1:1]", Status::Success),
                    TestResultBuilder::new(
                        "spec/a_spec.rb[1:2]",
                        Status::Failure {
                            exception: None,
                            backtrace: None,
                        },
                    ),
                ],
            )
            .build(),
        }];

        write_retry_action(&action_dir, completed.clone()).unwrap();

        let env = std::fs::read_to_string(action_dir.join("env/RWX_SPOT_INTERRUPTION_RETRY"));
        assert_eq!(env.unwrap(), "true");

        // Mint extracts the action's data directory as the retry's data directory.
        let resumed_marker = dir.path().join("resumed");
        let read = take_retry_data(&action_dir.join("data"), &resumed_marker).unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].test_id, completed[0].test_id);
        assert_eq!(read[0].results, completed[0].results);

        // Later runs in the same task don't resume again.
        let read_again = take_retry_data(&action_dir.join("data"), &resumed_marker).unwrap();
        assert!(read_again.is_empty());
    }
}
