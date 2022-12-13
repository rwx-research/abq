//! Native runner protocol version 0.2
//!
//! https://www.notion.so/rwx/Native-Runner-Protocol-0-2-d992ef3b4fde4289b02244c1b89a8cc7

use serde_derive::{Deserialize, Serialize};

use super::{v0_1, AbqProtocolVersion, MetadataMap};

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
    pub test_framework: String,
    pub test_framework_version: String,
    pub language: String,
    pub language_version: String,
    pub host: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Test {
    pub id: TestId,
    pub tags: Vec<String>,
    pub meta: MetadataMap,
}

impl From<v0_1::Test> for Test {
    fn from(t: v0_1::Test) -> Self {
        let v0_1::Test { id, tags, meta } = t;
        Test { id, tags, meta }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub members: Vec<TestOrGroup>,
    pub tags: Vec<String>,
    pub meta: MetadataMap,
}

impl From<v0_1::Group> for Group {
    fn from(g: v0_1::Group) -> Self {
        let v0_1::Group {
            name,
            members,
            tags,
            meta,
        } = g;
        let members = members.into_iter().map(Into::into).collect();
        Group {
            name,
            members,
            tags,
            meta,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum TestOrGroup {
    #[serde(rename = "test")]
    Test(Test),
    #[serde(rename = "group")]
    Group(Group),
}

impl From<v0_1::TestOrGroup> for TestOrGroup {
    fn from(tg: v0_1::TestOrGroup) -> Self {
        use v0_1::TestOrGroup::*;
        match tg {
            Test(t) => Self::Test(t.into()),
            Group(g) => Self::Group(g.into()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Manifest {
    pub members: Vec<TestOrGroup>,
    pub init_meta: MetadataMap,
}

impl From<v0_1::Manifest> for Manifest {
    fn from(man: v0_1::Manifest) -> Self {
        let v0_1::Manifest { members, init_meta } = man;
        let members = members.into_iter().map(Into::into).collect();
        Manifest { members, init_meta }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(tag = "type", rename = "manifest_success")]
pub struct ManifestSuccessMessage {
    pub manifest: Manifest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_errors: Option<Vec<OutOfBandError>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename = "manifest_failure")]
pub struct ManifestFailureMessage {
    pub error: OutOfBandError,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_errors: Option<Vec<OutOfBandError>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ManifestMessage {
    Success(ManifestSuccessMessage),
    Failure(ManifestFailureMessage),
}

impl From<v0_1::ManifestMessage> for ManifestMessage {
    fn from(msg: v0_1::ManifestMessage) -> Self {
        let v0_1::ManifestMessage { manifest } = msg;
        ManifestMessage::Success(ManifestSuccessMessage {
            manifest: manifest.into(),
            other_errors: None,
        })
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Nanoseconds(pub u64);

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Failure {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Error {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "type")]
pub enum Status {
    #[serde(rename = "failure")]
    Failure(Failure),
    #[serde(rename = "error")]
    Error(Error),
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "skipped")]
    Skipped,
    #[serde(rename = "todo")]
    Todo,
    #[serde(rename = "timed_out")]
    TimedOut,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Iso8601(pub String);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestResult {
    pub status: Status,
    pub id: TestId,
    pub display_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    pub runtime: Nanoseconds,
    pub meta: MetadataMap,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Iso8601>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Iso8601>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub past_attempts: Option<Vec<TestResult>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_errors: Option<Vec<OutOfBandError>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Location {
    pub file: String,
    pub line: Option<u64>,
    pub column: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutOfBandError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<MetadataMap>,
}

#[derive(Serialize, Deserialize)]
pub struct TestResultMessage {
    pub test_result: TestResult,
}

pub type TestId = String;
