//! Native runner protocol version 0.1
//!
//! https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-IPC-Interface-0959f5a9144741d798ac122566a3d887#9226eb8d416d4ed7ab0fc567eef5b1a2

use serde_derive::{Deserialize, Serialize};

use super::MetadataMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct InitMessage {
    pub init_meta: MetadataMap,
    pub fast_exit: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitSuccessMessage {}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestCaseMessage {
    pub test_case: TestCase,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TestCase {
    pub id: TestId,
    pub meta: MetadataMap,
}

pub type Milliseconds = f64;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
pub enum Status {
    /// An explicit test failure.
    #[serde(rename = "failure")]
    Failure,
    /// An explicit test success.
    #[serde(rename = "success")]
    Success,
    /// An erroring execution of a test.
    #[serde(rename = "error")]
    Error,
    /// A test that is not yet implemented.
    #[serde(rename = "pending")]
    Pending,
    /// A test that was explicitly skipped.
    #[serde(rename = "skipped")]
    Skipped,
}

#[derive(Serialize, Deserialize)]
pub struct TestResultMessage {
    pub test_result: TestResult,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestResult {
    pub status: Status,
    pub id: TestId,
    pub display_name: String,
    pub output: Option<String>,
    pub runtime: Milliseconds,
    pub meta: MetadataMap,
}

pub type TestId = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestMessage {
    pub manifest: Manifest,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum TestOrGroup {
    #[serde(rename = "test")]
    Test(Test),
    #[serde(rename = "group")]
    Group(Group),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Test {
    pub id: TestId,
    pub tags: Vec<String>,
    pub meta: MetadataMap,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub members: Vec<TestOrGroup>,
    pub tags: Vec<String>,
    pub meta: MetadataMap,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Manifest {
    pub members: Vec<TestOrGroup>,
    pub init_meta: MetadataMap,
}
