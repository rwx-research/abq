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

use std::time::Duration;
use std::{collections::VecDeque, ops::Deref};

use serde_derive::{Deserialize, Serialize};

pub static ABQ_SOCKET: &str = "ABQ_SOCKET";
pub static ABQ_GENERATE_MANIFEST: &str = "ABQ_GENERATE_MANIFEST";

mod v0_1;
mod v0_2;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(tag = "type", rename = "abq_protocol_version")]
pub struct AbqProtocolVersion {
    pub major: u64,
    pub minor: u64,
}

pub type MetadataMap = serde_json::Map<String, serde_json::Value>;

impl AbqProtocolVersion {
    pub const V0_1: AbqProtocolVersion = AbqProtocolVersion { major: 0, minor: 1 };
    pub const V0_2: AbqProtocolVersion = AbqProtocolVersion { major: 0, minor: 2 };

    /// Checks whether the protocol version returned by a native runner is supported by this ABQ.
    /// If so, returns a [ProtocolWitness] that can be used for parsing a specific protocol
    /// version.
    pub fn get_supported_witness(&self) -> Option<ProtocolWitness> {
        use PrivProtocolWitness::*;

        if self == &Self::V0_1 {
            return Some(ProtocolWitness(V0_1));
        }
        if self == &Self::V0_2 {
            return Some(ProtocolWitness(V0_2));
        }
        None
    }
}

#[derive(Clone, Copy, Debug)]
enum PrivProtocolWitness {
    V0_1,
    V0_2,
}
#[derive(Clone, Copy, Debug)]
pub struct ProtocolWitness(PrivProtocolWitness);

impl ProtocolWitness {
    pub const fn get_version(&self) -> AbqProtocolVersion {
        use PrivProtocolWitness::*;
        match self.0 {
            V0_1 => AbqProtocolVersion::V0_1,
            V0_2 => AbqProtocolVersion::V0_2,
        }
    }

    pub fn iter_all() -> impl Iterator<Item = ProtocolWitness> {
        use PrivProtocolWitness::*;

        [Self(V0_1), Self(V0_2)].into_iter()
    }
}

// NORMALIZE: runner specification 0.1
pub use v0_1::AbqNativeRunnerSpecification as NativeRunnerSpecification;

#[cfg(feature = "expose-native-protocols")]
impl From<NativeRunnerSpecification> for v0_2::AbqNativeRunnerSpecification {
    fn from(spec: NativeRunnerSpecification) -> v0_2::AbqNativeRunnerSpecification {
        let NativeRunnerSpecification {
            name,
            version,
            test_framework,
            test_framework_version,
            language,
            language_version,
            host,
        } = spec;
        v0_2::AbqNativeRunnerSpecification {
            name,
            version,
            test_framework: test_framework.unwrap(),
            test_framework_version: test_framework_version.unwrap(),
            language: language.unwrap(),
            language_version: language_version.unwrap(),
            host: host.unwrap(),
        }
    }
}

// NORMALIZE: runner specification 0.1
pub use v0_1::AbqNativeRunnerSpawnedMessage as NativeRunnerSpawnedMessage;

impl From<RawNativeRunnerSpawnedMessage> for NativeRunnerSpawnedMessage {
    fn from(msg: RawNativeRunnerSpawnedMessage) -> Self {
        use PrivAbqNativeRunnerSpawnedMessage::*;
        match msg.0 {
            V0_1(msg) => msg,
            V0_2(v0_2::AbqNativeRunnerSpawnedMessage {
                protocol_version,
                runner_specification,
            }) => Self {
                protocol_version,
                runner_specification: runner_specification.into(),
            },
        }
    }
}

/// Spawn message received from a native runner.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawNativeRunnerSpawnedMessage(PrivAbqNativeRunnerSpawnedMessage);
#[derive(Serialize, Deserialize, Debug, Clone, derive_more::From)]
#[serde(untagged)]
enum PrivAbqNativeRunnerSpawnedMessage {
    V0_1(v0_1::AbqNativeRunnerSpawnedMessage),
    V0_2(v0_2::AbqNativeRunnerSpawnedMessage),
}

impl RawNativeRunnerSpawnedMessage {
    #[cfg(feature = "expose-native-protocols")]
    pub fn new(
        protocol: ProtocolWitness,
        protocol_version: AbqProtocolVersion,
        spec: NativeRunnerSpecification,
    ) -> Self {
        use PrivProtocolWitness::*;
        Self(match protocol.0 {
            V0_1 => v0_1::AbqNativeRunnerSpawnedMessage {
                protocol_version,
                runner_specification: spec,
            }
            .into(),
            V0_2 => v0_2::AbqNativeRunnerSpawnedMessage {
                protocol_version,
                runner_specification: spec.into(),
            }
            .into(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, derive_more::From)]
pub struct TestCase(PrivTestCase);
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, derive_more::From)]
#[serde(untagged)]
enum PrivTestCase {
    V0_1(v0_1::TestCase),
    V0_2(v0_2::TestCase),
}

impl TestCase {
    pub fn id(&self) -> &TestId {
        use PrivTestCase::*;
        match &self.0 {
            V0_1(tc) => &tc.id,
            V0_2(tc) => &tc.id,
        }
    }

    #[cfg(feature = "expose-native-protocols")]
    pub fn new(protocol: ProtocolWitness, id: impl Into<TestId>, meta: MetadataMap) -> Self {
        use PrivProtocolWitness::*;
        match protocol.0 {
            V0_1 => Self(
                v0_1::TestCase {
                    id: id.into(),
                    meta,
                }
                .into(),
            ),
            V0_2 => Self(
                v0_2::TestCase {
                    id: id.into(),
                    meta,
                }
                .into(),
            ),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TestCaseMessage(PrivTestCaseMessage);
#[derive(Serialize, Deserialize, Debug, derive_more::From)]
#[serde(untagged)]
enum PrivTestCaseMessage {
    V0_1(v0_1::TestCaseMessage),
    V0_2(v0_2::TestCaseMessage),
}

impl TestCaseMessage {
    pub fn new(test_case: TestCase) -> Self {
        use PrivTestCase::*;
        match test_case.0 {
            V0_1(test_case) => Self((v0_1::TestCaseMessage { test_case }).into()),
            V0_2(test_case) => Self(v0_2::TestCaseMessage { test_case }.into()),
        }
    }
}

pub struct TestOrGroup(PrivTestOrGroup);
#[derive(derive_more::From)]
enum PrivTestOrGroup {
    V0_1(v0_1::TestOrGroup),
    V0_2(v0_2::TestOrGroup),
}

impl TestOrGroup {
    #[cfg(feature = "expose-native-protocols")]
    pub fn test(test: Test) -> Self {
        use PrivTest::*;
        match test.0 {
            V0_1(test) => Self(v0_1::TestOrGroup::Test(test).into()),
            V0_2(test) => Self(v0_2::TestOrGroup::Test(test).into()),
        }
    }
}

pub struct Test(PrivTest);
#[derive(derive_more::From)]
enum PrivTest {
    V0_1(v0_1::Test),
    V0_2(v0_2::Test),
}

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
            V0_1 => Self(
                v0_1::Test {
                    id: id.into(),
                    tags: tags.into(),
                    meta,
                }
                .into(),
            ),
            V0_2 => Self(
                v0_2::Test {
                    id: id.into(),
                    tags: tags.into(),
                    meta,
                }
                .into(),
            ),
        }
    }
}

// TODO 0.2: expose this as flat manifest to the rest of ABQ rather than keeping everything around
// here (for now).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawManifest(PrivManifest);
#[derive(Serialize, Deserialize, Debug, Clone, derive_more::From)]
#[serde(untagged)]
enum PrivManifest {
    V0_1(v0_1::Manifest),
    V0_2(v0_2::Manifest),
}

// NORMALIZE: use 0.2's manifest
pub use v0_2::Manifest;

impl From<RawManifest> for Manifest {
    fn from(man: RawManifest) -> Self {
        use PrivManifest::*;
        match man.0 {
            V0_1(man) => man.into(),
            V0_2(man) => man,
        }
    }
}

impl Manifest {
    /// Flattens a manifest into only [TestId]s, preserving the manifest order.
    pub fn flatten(self) -> (Vec<TestCase>, MetadataMap) {
        use v0_2::{Group, Test, TestCase, TestOrGroup};

        let Manifest { members, init_meta } = self;
        let mut collected = Vec::with_capacity(members.len());
        let mut queue: VecDeque<_> = members.into_iter().collect();
        while let Some(test_or_group) = queue.pop_front() {
            match test_or_group {
                TestOrGroup::Test(Test { id, meta, .. }) => {
                    collected.push(self::TestCase(TestCase { id, meta }.into()));
                }
                TestOrGroup::Group(Group { members, .. }) => {
                    for member in members.into_iter().rev() {
                        queue.push_front(member);
                    }
                }
            }
        }
        (collected, init_meta)
    }

    /// Sorts the manifest into a consistent ordering.
    #[cfg(feature = "expose-native-protocols")]
    pub fn sort(&mut self) {
        use v0_2::TestOrGroup;
        self.members.sort_by_key(|member| match member {
            TestOrGroup::Test(test) => test.id.clone(),
            TestOrGroup::Group(group) => group.name.clone(),
        });
    }

    #[cfg(feature = "expose-native-protocols")]
    pub fn new(members: impl IntoIterator<Item = TestOrGroup>, init_meta: MetadataMap) -> Self {
        let members = members
            .into_iter()
            .map(|tg| match tg.0 {
                PrivTestOrGroup::V0_2(tg) => tg,
                PrivTestOrGroup::V0_1(tg) => tg.into(),
            })
            .collect();
        Self { members, init_meta }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawManifestMessage(PrivManifestMessage);
#[derive(Serialize, Deserialize, Debug, Clone, derive_more::From)]
#[serde(untagged)]
enum PrivManifestMessage {
    V0_1(v0_1::ManifestMessage),
    V0_2(v0_2::ManifestMessage),
}

// Normalize: use 0.2
pub use v0_2::ManifestMessage;

impl From<RawManifestMessage> for ManifestMessage {
    fn from(msg: RawManifestMessage) -> Self {
        use PrivManifestMessage::*;
        match msg.0 {
            V0_1(msg) => msg.into(),
            V0_2(msg) => msg,
        }
    }
}

impl ManifestMessage {
    /// Extract the [manifest][Manifest] from the manifest message.
    pub fn into_manifest(self) -> Manifest {
        use v0_2::ManifestSuccessMessage;
        match self {
            ManifestMessage::Success(ManifestSuccessMessage {
                manifest,
                other_errors: _,
            }) => manifest,
            ManifestMessage::Failure(_) => todo!(),
        }
    }

    #[cfg(feature = "expose-native-protocols")]
    pub fn new(manifest: Manifest) -> Self {
        Self::Success(v0_2::ManifestSuccessMessage {
            manifest,
            other_errors: None,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum TestRuntime {
    Milliseconds(f64), // v0.1
    Nanoseconds(u64),  // v0.2
}

impl TestRuntime {
    pub const ZERO: TestRuntime = TestRuntime::Nanoseconds(0);

    pub fn duration(&self) -> Duration {
        match self {
            TestRuntime::Milliseconds(ms) => Duration::from_millis(*ms as _),
            TestRuntime::Nanoseconds(ns) => Duration::from_nanos(*ns),
        }
    }
}

impl std::ops::AddAssign for TestRuntime {
    fn add_assign(&mut self, rhs: Self) {
        use TestRuntime::*;
        *self = match (*self, rhs) {
            (Milliseconds(lms), Milliseconds(rms)) => Milliseconds(lms + rms),
            (Nanoseconds(lns), Nanoseconds(rns)) => Nanoseconds(lns + rns),
            (Milliseconds(ms), Nanoseconds(ns)) | (Nanoseconds(ns), Milliseconds(ms)) => {
                Nanoseconds(ns + ((ms * 1_000_000.) as u64))
            }
        };
    }
}

pub type TestId = String;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Status {
    /// An explicit test failure.
    Failure {
        #[serde(skip_serializing_if = "Option::is_none")]
        exception: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        backtrace: Option<Vec<String>>,
    },
    /// An explicit test success.
    Success,
    /// An erroring execution of a test.
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        exception: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        backtrace: Option<Vec<String>>,
    },
    /// A test that is not yet implemented.
    /// Pending is similar to Todo in some frameworks.
    Pending,
    /// A test that is not yet implemented.
    /// Todo is similar to Pending in some frameworks.
    Todo,
    /// A test that was explicitly skipped.
    Skipped,
    /// A test that failed due to a timeout.
    TimedOut,
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
            Status::Failure { .. }
                | Status::Error { .. }
                | Status::TimedOut
                | Status::PrivateNativeRunnerError
        )
    }
}

impl From<v0_1::Status> for Status {
    fn from(ts: v0_1::Status) -> Self {
        use v0_1::Status as VStatus;
        use Status::*;
        match ts {
            VStatus::Failure => Failure {
                exception: None,
                backtrace: None,
            },
            VStatus::Success => Success,
            VStatus::Error => Error {
                exception: None,
                backtrace: None,
            },
            VStatus::Pending => Pending,
            VStatus::Skipped => Skipped,
        }
    }
}

impl From<v0_2::Status> for Status {
    fn from(ts: v0_2::Status) -> Self {
        use v0_2::Status as VStatus;
        use Status::*;
        match ts {
            VStatus::Failure(v0_2::Failure {
                exception,
                backtrace,
            }) => Failure {
                exception,
                backtrace,
            },
            VStatus::Success => Success,
            VStatus::Error(v0_2::Error {
                exception,
                backtrace,
            }) => Error {
                exception,
                backtrace,
            },
            VStatus::Pending => Pending,
            VStatus::Skipped => Skipped,
            VStatus::Todo => Todo,
            VStatus::TimedOut => TimedOut,
        }
    }
}

pub use v0_2::Iso8601;
pub use v0_2::Location;
pub use v0_2::OutOfBandError;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestResultSpec {
    pub status: Status,
    pub id: TestId,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    pub runtime: TestRuntime,
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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    pub stdout: Option<String>,
}

impl TestResultSpec {
    #[cfg(feature = "expose-native-protocols")]
    pub fn fake() -> Self {
        Self {
            status: Status::Success,
            id: "zzz-faux".to_string(),
            display_name: "zzz-faux".to_string(),
            output: None,
            runtime: TestRuntime::ZERO,
            meta: Default::default(),
            location: None,
            started_at: None,
            finished_at: None,
            lineage: None,
            past_attempts: None,
            other_errors: None,
            stderr: None,
            stdout: None,
        }
    }
}

impl From<v0_1::TestResult> for TestResultSpec {
    fn from(tr: v0_1::TestResult) -> Self {
        let v0_1::TestResult {
            status,
            id,
            display_name,
            output,
            runtime,
            meta,
        } = tr;

        let status = status.into();
        let runtime = TestRuntime::Milliseconds(runtime as _);

        TestResultSpec {
            status,
            id,
            display_name,
            output,
            runtime,
            meta,
            location: None,
            started_at: None,
            finished_at: None,
            lineage: None,
            past_attempts: None,
            other_errors: None,
            stderr: None,
            stdout: None,
        }
    }
}

impl From<v0_2::TestResult> for TestResultSpec {
    fn from(tr: v0_2::TestResult) -> Self {
        let v0_2::TestResult {
            status,
            id,
            display_name,
            output,
            runtime: v0_2::Nanoseconds(nanos),
            meta,
            location,
            started_at,
            finished_at,
            lineage,
            past_attempts,
            other_errors,
        } = tr;

        let status = status.into();
        let runtime = TestRuntime::Nanoseconds(nanos);
        let past_attempts =
            past_attempts.map(|attempts| attempts.into_iter().map(Into::into).collect());

        TestResultSpec {
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
            past_attempts,
            other_errors,
            stderr: None,
            stdout: None,
        }
    }
}

/// Test result received from a native runner.
#[derive(Serialize, Deserialize, Debug)]
pub struct RawNativeTestResult(PrivNativeTestResult);
#[derive(Serialize, Deserialize, Debug, derive_more::From)]
#[serde(untagged)]
enum PrivNativeTestResult {
    V0_1(v0_1::TestResult),
    V0_2(Box<v0_2::TestResult>),
}

static_assertions::assert_eq_size!(RawNativeTestResult, [u8; 112]);

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

    #[cfg(feature = "expose-native-protocols")]
    pub fn fake() -> Self {
        Self(TestResultSpec::fake())
    }
}

impl From<v0_1::TestResult> for TestResult {
    fn from(tr: v0_1::TestResult) -> Self {
        Self(tr.into())
    }
}
impl From<v0_2::TestResult> for TestResult {
    fn from(tr: v0_2::TestResult) -> Self {
        Self(tr.into())
    }
}
impl From<RawNativeTestResult> for TestResult {
    fn from(native_result: RawNativeTestResult) -> Self {
        use PrivNativeTestResult::*;

        match native_result.0 {
            V0_1(tr) => tr.into(),
            V0_2(tr) => (*tr).into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct RawTestResultMessage(PrivTestResultMessage);
#[derive(Serialize, Deserialize, derive_more::From)]
#[serde(untagged)]
enum PrivTestResultMessage {
    V0_1(v0_1::TestResultMessage),
    V0_2(Box<v0_2::TestResultMessage>),
}

static_assertions::assert_eq_size!(RawTestResultMessage, [u8; 112]);

impl RawTestResultMessage {
    /// Extract the [test result][TestResult] from the message.
    pub fn into_test_result(self) -> TestResult {
        use PrivTestResultMessage::*;
        match self.0 {
            V0_1(msg) => msg.test_result.into(),
            V0_2(msg) => msg.test_result.into(),
        }
    }
}

pub struct FastExit(pub bool);

#[derive(Serialize, Deserialize, Debug)]
pub struct InitMessage(PrivInitMessage);
#[derive(Serialize, Deserialize, Debug, derive_more::From)]
#[serde(untagged)]
enum PrivInitMessage {
    V0_1(v0_1::InitMessage),
    V0_2(v0_2::InitMessage),
}

impl InitMessage {
    pub fn new(witness: ProtocolWitness, init_meta: MetadataMap, fast_exit: FastExit) -> Self {
        use PrivProtocolWitness::*;
        match witness.0 {
            V0_1 => Self(
                v0_1::InitMessage {
                    init_meta,
                    fast_exit: fast_exit.0,
                }
                .into(),
            ),
            V0_2 => Self(
                v0_2::InitMessage {
                    init_meta,
                    fast_exit: fast_exit.0,
                }
                .into(),
            ),
        }
    }
}

type PrivInitSuccessMessage = v0_1::InitSuccessMessage;
#[derive(Serialize, Deserialize, Debug)]
pub struct InitSuccessMessage(PrivInitSuccessMessage);
