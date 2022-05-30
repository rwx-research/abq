use std::time::Duration;

use abq_worker_protocol::Output;
use serde_derive::{Deserialize, Serialize};

/// ID for a piece of work, reflected both during work queueing and completion.
/// For example, a test may have ID "vanguard:test.rb:test_homepage".
// TODO: maybe we want uuids here and some mappings to test IDs
#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct WorkId(pub String);

/// A unit of work.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg(not(test))]
pub enum WorkAction {
    Echo(String),
}

/// A unit of work, specialized for testing.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg(test)]
pub enum WorkAction {
    Echo(String),
    InduceTimeout,
    EchoOnRetry(usize, String),
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Shutdown {
    pub timeout: Duration,
    pub opt_expect_n: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum WorkResult {
    Output(Output),
    Timeout(Duration),
    Panic(String),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Message {
    Shutdown(Shutdown),
    Work(WorkId, WorkAction),
    Result(WorkId, WorkResult),
}
