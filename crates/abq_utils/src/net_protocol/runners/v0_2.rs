//! Native runner protocol version 0.2
//!
//! https://www.notion.so/rwx/Native-Runner-Protocol-0-2-d992ef3b4fde4289b02244c1b89a8cc7

use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use crate::net_protocol::entity::RunnerMeta;

use super::{AbqProtocolVersion, MetadataMap};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename = "abq_native_runner_spawned")]
pub struct AbqNativeRunnerSpawnedMessage {
    pub protocol_version: AbqProtocolVersion,
    pub runner_specification: AbqNativeRunnerSpecification,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub members: Vec<TestOrGroup>,
    pub tags: Vec<String>,
    pub meta: MetadataMap,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum TestOrGroup {
    #[serde(rename = "test")]
    Test(Test),
    #[serde(rename = "group")]
    Group(Group),
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Manifest {
    pub members: Vec<TestOrGroup>,
    pub init_meta: MetadataMap,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub focus: Option<TestFocus>,
}

impl TestCase {
    pub(crate) fn has_focus(&self) -> bool {
        match &self.focus {
            Some(TestFocus { test_ids }) => !test_ids.is_empty(),
            None => false,
        }
    }

    pub fn clear_focus(&mut self) {
        self.focus = None;
    }

    pub fn add_focus(&mut self, test_id: TestId) {
        match &mut self.focus {
            None => {
                self.focus = Some(TestFocus {
                    test_ids: vec![test_id],
                });
            }
            Some(TestFocus { test_ids }) => {
                if !test_ids.contains(&test_id) {
                    test_ids.push(test_id)
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TestFocus {
    test_ids: Vec<TestId>,
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

#[cfg(feature = "expose-native-protocols")]
impl From<super::Status> for Status {
    fn from(s: super::Status) -> Self {
        match s {
            super::Status::Failure {
                exception,
                backtrace,
            } => Self::Failure(Failure {
                exception,
                backtrace,
            }),
            super::Status::Success => Self::Success,
            super::Status::Error {
                exception,
                backtrace,
            } => Self::Error(Error {
                exception,
                backtrace,
            }),
            super::Status::Pending => Self::Pending,
            super::Status::Todo => Self::Todo,
            super::Status::Skipped => Self::Skipped,
            super::Status::TimedOut => Self::TimedOut,
            super::Status::PrivateNativeRunnerError => Self::TimedOut,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Iso8601(pub String);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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

#[cfg(feature = "expose-native-protocols")]
impl From<super::TestResult> for TestResult {
    fn from(r: super::TestResult) -> Self {
        let super::TestResultSpec {
            status,
            id,
            display_name,
            output,
            runtime,
            meta,
            location,
            started_at,
            finished_at,
            lineage,
            other_errors,
            ..
        } = r.result;

        Self {
            status: status.into(),
            id,
            display_name,
            output,
            runtime: Nanoseconds(runtime.duration().as_nanos() as _),
            meta,
            location,
            started_at,
            finished_at,
            lineage,
            past_attempts: None, // TODO
            other_errors,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub file: String,
    pub line: Option<u64>,
    pub column: Option<u64>,
}

impl Location {
    #[cfg(feature = "expose-native-protocols")]
    pub fn fake() -> Self {
        Self {
            file: "a/b/x.file".to_string(),
            line: Some(10),
            column: Some(15),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Error, PartialEq, Eq)]
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

pub type TestId = String;

#[derive(Serialize, Deserialize, Debug)]
pub struct SingleTestResultMessage {
    pub test_result: TestResult,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MultipleTestResultsMessage {
    pub test_results: Vec<TestResult>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum IncrementalTestResultMessage {
    #[serde(rename = "incremental_result")]
    One { one_test_result: TestResult },
    #[serde(rename = "incremental_result_done")]
    Done {
        last_test_result: Option<TestResult>,
    },
}

impl IncrementalTestResultMessage {
    pub fn into_step(self, source: RunnerMeta) -> super::IncrementalTestResultStep {
        use super::IncrementalTestResultStep::*;
        match self {
            Self::One { one_test_result } => One(one_test_result.reify(source)),
            Self::Done { last_test_result } => Done(last_test_result.map(|r| r.reify(source))),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum TestResultMessage {
    Incremental(IncrementalTestResultMessage),
    Single(SingleTestResultMessage),
    Multiple(MultipleTestResultsMessage),
}

impl TestResultMessage {
    pub fn into_test_results(self, source: RunnerMeta) -> super::TestResultSet {
        use super::TestResultSet::*;
        match self {
            Self::Single(msg) => All(vec![msg.test_result.reify(source)]),
            Self::Multiple(msg) => All(msg
                .test_results
                .into_iter()
                .map(|r| r.reify(source))
                .collect()),
            Self::Incremental(msg) => Incremental(msg.into_step(source)),
        }
    }
}

#[cfg(test)]
mod result_message {
    use serde_json;

    use super::{
        IncrementalTestResultMessage, MultipleTestResultsMessage, SingleTestResultMessage, Status,
        TestResult, TestResultMessage,
    };
    use IncrementalTestResultMessage::*;
    use TestResultMessage::*;

    fn fake_test_result() -> TestResult {
        TestResult {
            status: Status::Success,
            id: "fake-id".to_owned(),
            display_name: "fake".to_owned(),
            output: None,
            runtime: super::Nanoseconds(17823),
            meta: Default::default(),
            location: None,
            started_at: None,
            finished_at: None,
            lineage: None,
            past_attempts: None,
            other_errors: None,
        }
    }

    fn fake_result_str() -> String {
        serde_json::to_string(&fake_test_result()).unwrap()
    }

    fn parse(s: &str) -> serde_json::Result<TestResultMessage> {
        serde_json::from_str(s)
    }

    #[test]
    fn parse_incremental_success() {
        assert!(matches!(parse(&format!(
            r#"{{ "type": "incremental_result", "one_test_result": {} }}"#,
            fake_result_str()
        )), Ok(Incremental(One { one_test_result })) if one_test_result == fake_test_result()));

        assert!(matches!(parse(&format!(
            r#"{{ "type": "incremental_result_done", "last_test_result": {} }}"#,
            fake_result_str()
        )), Ok(Incremental(Done { last_test_result: Some(r) })) if r == fake_test_result()));

        assert!(matches!(
            parse(r#"{ "type": "incremental_result_done" }"#),
            Ok(Incremental(Done {
                last_test_result: None
            }))
        ));
    }

    #[test]
    fn parse_incremental_fail() {
        assert!(matches!(
            parse(r#"{ "type": "incremental_result" }"#),
            Err(..)
        ));

        assert!(matches!(parse(r#"{  }"#), Err(..)));
    }

    #[test]
    fn parse_single_success() {
        assert!(matches!(parse(&format!(
            r#"{{ "test_result": {} }}"#,
            fake_result_str()
        )), Ok(Single(SingleTestResultMessage { test_result })) if test_result == fake_test_result()));
    }

    #[test]
    fn parse_single_fail() {
        assert!(matches!(parse(r#"{  }"#), Err(..)));
    }

    #[test]
    fn parse_multiple_success() {
        assert!(matches!(parse(&format!(
            r#"{{ "test_results": [{}] }}"#,
            fake_result_str()
        )), Ok(Multiple(MultipleTestResultsMessage { test_results })) if test_results == vec![fake_test_result()]));
    }

    #[test]
    fn parse_multiple_fail() {
        assert!(matches!(
            parse(&format!(r#"{{ "test_results": {} }}"#, fake_result_str())),
            Err(..)
        ));
        assert!(matches!(parse(r#"{  }"#), Err(..)));
    }
}
