use std::time::Duration;

use serde_derive::{Deserialize, Serialize};

/// ID for a piece of work, reflected both during work queueing and completion.
/// For example, a test may have ID "vanguard:test.rb:test_homepage".
// TODO: maybe we want uuids here and some mappings to test IDs
#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct WorkId(pub String);

#[derive(Serialize, Deserialize)]
pub enum WorkAction {
    Echo(String),
}

#[derive(Serialize, Deserialize)]
pub struct WorkItem {
    pub id: WorkId,
    pub action: WorkAction,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct WorkResult {
    pub id: WorkId,
    pub message: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Shutdown {
    pub timeout: Duration,
    pub opt_expect_n: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Message {
    Shutdown(Shutdown),
    Work(WorkItem),
    Result(WorkResult),
}
