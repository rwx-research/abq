//! The interface to the native runner protocol.
//!
//! Docs: https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-IPC-Interface-0959f5a9144741d798ac122566a3d887#9226eb8d416d4ed7ab0fc567eef5b1a2
//!
//! # Compatibility strategy
//!
//! ABQ needs to support multiple protocol versions at a time.
//! Each protocol version is entirely stored in a sub-module; for example, see [v0_1].
//!
//! Each such protocol version **describes entirely** the interface between a worker and native
//! runner. As such, once data is read into an ABQ worker, it will not escape back into a native
//! runner.
//!
//! This means that we can expose to the rest of ABQ (including ABQ workers) a uniform interface
//! for the data received from native runners, across any protocol. This module provides that
//! interface, and the individual protocol version data structures are not made public.
//!
//! Different protocol versions are serialized into a private uniform sum type. For example, see [PrivTestResult].
//! Importantly, these sum types *must* be untagged, because they are built directly one from one
//! concrete protocol version.
//!
//! The sum type is then normalized to one data structure that the rest of ABQ operates over.

use std::{collections::VecDeque, ops::Deref};

use serde_derive::{Deserialize, Serialize};

pub static ABQ_SOCKET: &str = "ABQ_SOCKET";
pub static ABQ_GENERATE_MANIFEST: &str = "ABQ_GENERATE_MANIFEST";

mod v0_1;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(tag = "type", rename = "abq_protocol_version")]
pub struct AbqProtocolVersion {
    pub major: u64,
    pub minor: u64,
}

pub type MetadataMap = serde_json::Map<String, serde_json::Value>;

impl AbqProtocolVersion {
    pub const V0_1: AbqProtocolVersion = AbqProtocolVersion { major: 0, minor: 1 };

    /// Checks whether the protocol version returned by a native runner is supported by this ABQ.
    /// If so, returns a [ProtocolWitness] that can be used for parsing a specific protocol
    /// version.
    pub fn get_supported_witness(&self) -> Option<ProtocolWitness> {
        use PrivProtocolWitness::*;

        if self == &Self::V0_1 {
            return Some(ProtocolWitness(V0_1));
        }
        None
    }
}

#[derive(Clone, Copy, Debug)]
enum PrivProtocolWitness {
    V0_1,
}
#[derive(Clone, Copy, Debug)]
pub struct ProtocolWitness(PrivProtocolWitness);

impl ProtocolWitness {
    pub const fn get_version(&self) -> AbqProtocolVersion {
        use PrivProtocolWitness::*;
        match self.0 {
            V0_1 => AbqProtocolVersion::V0_1,
        }
    }

    pub fn iter_all() -> impl Iterator<Item = ProtocolWitness> {
        use PrivProtocolWitness::*;

        [Self(V0_1)].into_iter()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NativeRunnerSpecification {
    pub name: String,
    pub version: String,
    pub test_framework: Option<String>,
    pub test_framework_version: Option<String>,
    pub language: Option<String>,
    pub language_version: Option<String>,
    pub host: Option<String>,
}

impl From<v0_1::AbqNativeRunnerSpecification> for NativeRunnerSpecification {
    fn from(spec: v0_1::AbqNativeRunnerSpecification) -> Self {
        let v0_1::AbqNativeRunnerSpecification {
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
            test_framework,
            test_framework_version,
            language,
            language_version,
            host,
        }
    }
}

#[cfg(feature = "expose-native-protocols")]
impl From<NativeRunnerSpecification> for v0_1::AbqNativeRunnerSpecification {
    fn from(spec: NativeRunnerSpecification) -> v0_1::AbqNativeRunnerSpecification {
        let NativeRunnerSpecification {
            name,
            version,
            test_framework,
            test_framework_version,
            language,
            language_version,
            host,
        } = spec;
        v0_1::AbqNativeRunnerSpecification {
            name,
            version,
            test_framework,
            test_framework_version,
            language,
            language_version,
            host,
        }
    }
}

/// Normalized spawn message.
pub struct NativeRunnerSpawnedMessage {
    pub protocol_version: AbqProtocolVersion,
    pub runner_specification: NativeRunnerSpecification,
}

impl From<RawNativeRunnerSpawnedMessage> for NativeRunnerSpawnedMessage {
    fn from(msg: RawNativeRunnerSpawnedMessage) -> Self {
        let v0_1::AbqNativeRunnerSpawnedMessage {
            protocol_version,
            runner_specification,
        } = msg.0;
        Self {
            protocol_version,
            runner_specification: runner_specification.into(),
        }
    }
}

/// Spawn message received from a native runner.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawNativeRunnerSpawnedMessage(PrivAbqNativeRunnerSpawnedMessage);
type PrivAbqNativeRunnerSpawnedMessage = v0_1::AbqNativeRunnerSpawnedMessage;

impl RawNativeRunnerSpawnedMessage {
    #[cfg(feature = "expose-native-protocols")]
    pub fn new(
        protocol: ProtocolWitness,
        protocol_version: AbqProtocolVersion,
        spec: NativeRunnerSpecification,
    ) -> Self {
        use PrivProtocolWitness::*;
        match protocol.0 {
            V0_1 => Self(v0_1::AbqNativeRunnerSpawnedMessage {
                protocol_version,
                runner_specification: spec.into(),
            }),
        }
    }
}

type PrivTestCase = v0_1::TestCase;
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TestCase(PrivTestCase);

impl TestCase {
    pub fn id(&self) -> &TestId {
        &self.0.id
    }

    #[cfg(feature = "expose-native-protocols")]
    pub fn new(protocol: ProtocolWitness, id: impl Into<TestId>, meta: MetadataMap) -> Self {
        use PrivProtocolWitness::*;
        match protocol.0 {
            V0_1 => Self(v0_1::TestCase {
                id: id.into(),
                meta,
            }),
        }
    }
}

impl From<v0_1::TestCase> for TestCase {
    fn from(tc: v0_1::TestCase) -> Self {
        Self(tc)
    }
}

type PrivTestCaseMessage = v0_1::TestCaseMessage;
#[derive(Serialize, Deserialize, Debug)]
pub struct TestCaseMessage(PrivTestCaseMessage);

impl TestCaseMessage {
    pub fn new(test_case: TestCase) -> Self {
        let v0_1_message = v0_1::TestCaseMessage {
            test_case: test_case.0,
        };
        Self(v0_1_message)
    }
}

pub struct TestOrGroup(PrivTestOrGroup);
type PrivTestOrGroup = v0_1::TestOrGroup;

impl TestOrGroup {
    #[cfg(feature = "expose-native-protocols")]
    pub fn test(protocol: ProtocolWitness, test: Test) -> Self {
        use PrivProtocolWitness::*;

        match protocol.0 {
            V0_1 => Self(v0_1::TestOrGroup::Test(test.0)),
        }
    }
}

pub struct Test(PrivTest);
type PrivTest = v0_1::Test;

impl Test {
    #[cfg(feature = "expose-native-protocols")]
    pub fn new(
        protocol: ProtocolWitness,
        id: impl Into<TestId>,
        tags: impl Into<Vec<String>>,
        meta: MetadataMap,
    ) -> Self {
        use PrivProtocolWitness::*;

        match protocol.0 {
            V0_1 => Self(v0_1::Test {
                id: id.into(),
                tags: tags.into(),
                meta,
            }),
        }
    }
}

type PrivManifest = v0_1::Manifest;
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Manifest(PrivManifest);

impl Manifest {
    /// Flattens a manifest into only [TestId]s, preserving the manifest order.
    pub fn flatten(self) -> (Vec<TestCase>, MetadataMap) {
        use v0_1::{Group, Test, TestCase, TestOrGroup};

        let v0_1::Manifest {
            members,
            init_meta: meta,
        } = self.0;

        let mut collected = Vec::with_capacity(members.len());
        let mut queue: VecDeque<_> = members.into_iter().collect();
        while let Some(test_or_group) = queue.pop_front() {
            match test_or_group {
                TestOrGroup::Test(Test { id, meta, .. }) => {
                    collected.push(TestCase { id, meta }.into());
                }
                TestOrGroup::Group(Group { members, .. }) => {
                    for member in members.into_iter().rev() {
                        queue.push_front(member);
                    }
                }
            }
        }
        (collected, meta)
    }

    /// Sorts the manifest into a consistent ordering.
    pub fn sort(&mut self) {
        use v0_1::TestOrGroup;
        self.0.members.sort_by_key(|member| match member {
            TestOrGroup::Test(test) => test.id.clone(),
            TestOrGroup::Group(group) => group.name.clone(),
        });
    }

    #[cfg(feature = "expose-native-protocols")]
    pub fn new(
        protocol: ProtocolWitness,
        members: impl IntoIterator<Item = TestOrGroup>,
        init_meta: MetadataMap,
    ) -> Self {
        use PrivProtocolWitness::*;

        match protocol.0 {
            V0_1 => {
                let members = members.into_iter().map(|tg| tg.0).collect();
                Self(v0_1::Manifest { members, init_meta })
            }
        }
    }
}

impl From<v0_1::Manifest> for Manifest {
    fn from(man: v0_1::Manifest) -> Self {
        Self(man)
    }
}

type PrivManifestMessage = v0_1::ManifestMessage;
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestMessage(PrivManifestMessage);

impl ManifestMessage {
    /// Extract the [manifest][Manifest] from the manifest message.
    pub fn into_manifest(self) -> Manifest {
        self.0.manifest.into()
    }

    #[cfg(feature = "expose-native-protocols")]
    pub fn new(protocol: ProtocolWitness, manifest: Manifest) -> Self {
        use PrivProtocolWitness::*;

        match protocol.0 {
            V0_1 => Self(v0_1::ManifestMessage {
                manifest: manifest.0,
            }),
        }
    }
}

impl From<v0_1::ManifestMessage> for ManifestMessage {
    fn from(msg: v0_1::ManifestMessage) -> Self {
        Self(msg)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum TestRuntime {
    Milliseconds(f64),
}

impl TestRuntime {
    pub const ZERO: TestRuntime = TestRuntime::Milliseconds(0.);
}

impl std::ops::AddAssign for TestRuntime {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (TestRuntime::Milliseconds(lms), TestRuntime::Milliseconds(rms)) => *lms += rms,
        }
    }
}

pub type TestId = String;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
pub enum Status {
    /// An explicit test failure.
    Failure,
    /// An explicit test success.
    Success,
    /// An erroring execution of a test.
    Error,
    /// A test that is not yet implemented.
    Pending,
    /// A test that was explicitly skipped.
    Skipped,
    /// The underlying native test runner failed unexpectedly.
    /// This is a state caught only be abq workers, and should never be reported directly.
    PrivateNativeRunnerError,
}

impl Status {
    /// If this status had to be classified as either a success or failure,
    /// would it be a failure?
    pub fn is_fail_like(&self) -> bool {
        matches!(
            self,
            Status::Failure | Status::Error | Status::PrivateNativeRunnerError
        )
    }
}

impl From<v0_1::Status> for Status {
    fn from(ts: v0_1::Status) -> Self {
        use v0_1::Status as VStatus;
        use Status::*;
        match ts {
            VStatus::Failure => Failure,
            VStatus::Success => Success,
            VStatus::Error => Error,
            VStatus::Pending => Pending,
            VStatus::Skipped => Skipped,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestResultSpec {
    pub status: Status,
    pub id: TestId,
    pub display_name: String,
    pub output: Option<String>,
    pub runtime: TestRuntime,
    pub meta: MetadataMap,
}

/// Test result received from a native runner.
#[derive(Serialize, Deserialize, Debug)]
pub struct NativeTestResult(PrivNativeTestResult);
type PrivNativeTestResult = v0_1::TestResult;

static_assertions::assert_eq_size!(NativeTestResult, [u8; 112]);
impl From<v0_1::TestResult> for NativeTestResult {
    fn from(result: v0_1::TestResult) -> Self {
        Self(result)
    }
}

/// Test result reported to ABQ, normalizing from a native runner.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestResult(PrivReportedTestResult);
type PrivReportedTestResult = TestResultSpec;

impl Deref for TestResult {
    type Target = TestResultSpec;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TestResult {
    pub fn new(spec: TestResultSpec) -> Self {
        Self(spec)
    }

    pub fn into_spec(self) -> TestResultSpec {
        self.0
    }
}

impl From<NativeTestResult> for TestResult {
    fn from(native_result: NativeTestResult) -> Self {
        let v0_1::TestResult {
            status,
            id,
            display_name,
            output,
            runtime,
            meta,
        } = native_result.0;

        Self(TestResultSpec {
            status: status.into(),
            id,
            display_name,
            output,
            runtime: TestRuntime::Milliseconds(runtime),
            meta,
        })
    }
}

type PrivTestResultMessage = v0_1::TestResultMessage;
#[derive(Serialize, Deserialize)]
pub struct TestResultMessage(PrivTestResultMessage);

static_assertions::assert_eq_size!(TestResultMessage, [u8; 112]);
impl TestResultMessage {
    /// Extract the [test result][TestResult] from the message.
    pub fn into_test_result(self) -> TestResult {
        let native_test_result: NativeTestResult = self.0.test_result.into();
        native_test_result.into()
    }
}

pub struct FastExit(pub bool);

type PrivInitMessage = v0_1::InitMessage;
#[derive(Serialize, Deserialize, Debug)]
pub struct InitMessage(PrivInitMessage);

impl InitMessage {
    pub fn new(witness: ProtocolWitness, init_meta: MetadataMap, fast_exit: FastExit) -> Self {
        use PrivProtocolWitness::*;
        match witness.0 {
            V0_1 => Self(v0_1::InitMessage {
                init_meta,
                fast_exit: fast_exit.0,
            }),
        }
    }
}

type PrivInitSuccessMessage = v0_1::InitSuccessMessage;
#[derive(Serialize, Deserialize, Debug)]
pub struct InitSuccessMessage(PrivInitSuccessMessage);
