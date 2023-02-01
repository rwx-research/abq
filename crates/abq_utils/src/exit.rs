use serde_derive::{Deserialize, Serialize};

/// Exit code to issue if an abq entity itself fails.
pub const CODE_ERROR: i32 = 101;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ExitCode(i32);

impl ExitCode {
    pub const SUCCESS: ExitCode = ExitCode(0);
    pub const FAILURE: ExitCode = ExitCode(1);
    pub const CANCELLED: ExitCode = ExitCode(1);
    pub const ABQ_ERROR: ExitCode = ExitCode(CODE_ERROR);

    /// For use when a worker is used in-band with a supervisor,
    /// and the exit code seen by the worker should be overwritten by the supervisor.
    /// This code is used to track in case such logic ever goes wrong.
    pub const WORKER_CEDES_TO_SUPERVISOR: ExitCode = ExitCode(88);

    pub const fn get(&self) -> i32 {
        self.0
    }

    pub const fn new(code: i32) -> Self {
        Self(code)
    }
}

impl From<std::process::ExitStatus> for ExitCode {
    fn from(es: std::process::ExitStatus) -> Self {
        let code = es.code().unwrap_or(CODE_ERROR);
        Self::new(code)
    }
}
