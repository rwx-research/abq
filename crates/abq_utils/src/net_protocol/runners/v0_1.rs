//! Native runner protocol version 0.1
//!
//! https://www.notion.so/rwx/Native-Runner-Protocol-0-1-b16c33b82f854a799b0aca53891a7f5a

use serde_derive::{Deserialize, Serialize};

use super::{v0_2, AbqProtocolVersion, MetadataMap};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename = "abq_native_runner_spawned")]
pub struct AbqNativeRunnerSpawnedMessage {
    pub protocol_version: AbqProtocolVersion,
    pub runner_specification: AbqNativeRunnerSpecification,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename = "abq_native_runner_specification")]
pub struct AbqNativeRunnerSpecification {
    pub name: String,
    pub version: String,
    pub test_framework: Option<String>,
    pub test_framework_version: Option<String>,
    pub language: Option<String>,
    pub language_version: Option<String>,
    pub host: Option<String>,
}

impl From<v0_2::AbqNativeRunnerSpecification> for AbqNativeRunnerSpecification {
    fn from(spec: v0_2::AbqNativeRunnerSpecification) -> Self {
        let v0_2::AbqNativeRunnerSpecification {
            name,
            version,
            test_framework,
            test_framework_version,
            language,
            language_version,
            host,
        } = spec;
        Self {
            name,
            version,
            test_framework: Some(test_framework),
            test_framework_version: Some(test_framework_version),
            language: Some(language),
            language_version: Some(language_version),
            host: Some(host),
        }
    }
}

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
