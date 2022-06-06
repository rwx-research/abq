use std::time::Duration;

use abq_runner_protocol::Output;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
/// ID for a particular invocation of the queue, which sends many units of work.
pub struct InvocationId(pub [u8; 16]);

impl InvocationId {
    #[allow(clippy::new_without_default)] // Invocation IDs should be fresh, not defaulted
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }
}

/// ID for a piece of work, reflected both during work queueing and completion.
/// For example, a test may have ID "vanguard:test.rb:test_homepage".
// TODO: maybe we want uuids here and some mappings to test IDs
#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct WorkId(pub String);

/// A unit of work sent to a worker.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg(not(test))]
pub enum WorkerAction {
    Echo(String),
    Exec {
        cmd: String,
        args: Vec<String>,
        working_dir: std::path::PathBuf,
    },
}

/// A unit of work, specialized for testing.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg(test)]
pub enum WorkerAction {
    Echo(String),
    InduceTimeout,
    EchoOnRetry(usize, String),
}

/// The result of a worker's execution of a unit of work.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum WorkerResult {
    Output(Output),
    Timeout(Duration),
    Panic(String),
}
