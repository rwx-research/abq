//! The native runner protocol is described at
//!
//! https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-IPQ-Interface-0959f5a9144741d798ac122566a3d887#f480d133e2c942719b1a0c0a9e76fb3a

use std::{
    io::{self, Read, Write},
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

pub mod runners;

pub mod health {
    use serde_derive::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    pub struct Health {
        pub healthy: bool,
        pub version: String,
    }

    pub fn healthy() -> Health {
        Health {
            healthy: true,
            version: crate::VERSION.to_string(),
        }
    }
}

pub mod meta {
    use serde_derive::{Deserialize, Serialize};

    /// A record of deprecated ABQ features an entity is using.
    #[derive(Serialize, Deserialize, Debug)]
    #[must_use]
    pub enum Deprecation {}

    /// A record of deprecated ABQ features an entity is using.
    #[derive(Serialize, Deserialize, Default)]
    pub struct DeprecationRecord(Vec<Deprecation>);

    impl DeprecationRecord {
        pub fn push(&mut self, deprecation: Deprecation) {
            self.0.push(deprecation);
        }
        pub fn extract(self) -> Vec<Deprecation> {
            self.0
        }
    }
}

pub mod entity {
    use serde_derive::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
    #[repr(transparent)]
    pub struct RunnerTag(u32);

    impl From<u32> for RunnerTag {
        fn from(runner: u32) -> Self {
            Self(runner)
        }
    }

    impl RunnerTag {
        pub const fn new(runner: u32) -> Self {
            Self(runner)
        }
    }

    #[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
    #[repr(transparent)]
    pub struct WorkerTag(u32);

    impl From<u32> for WorkerTag {
        fn from(runner: u32) -> Self {
            Self(runner)
        }
    }

    impl WorkerTag {
        pub const ZERO: Self = Self(0);
        pub const fn new(workers_number: u32) -> Self {
            Self(workers_number)
        }
        pub fn index(&self) -> u32 {
            self.0
        }
    }

    #[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
    pub struct WorkerRunner(WorkerTag, RunnerTag);

    impl std::fmt::Display for WorkerRunner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let Self(WorkerTag(worker), RunnerTag(runner)) = self;
            write!(f, "worker {worker}, runner {runner}")
        }
    }

    impl From<(u32, u32)> for WorkerRunner {
        fn from(tags: (u32, u32)) -> Self {
            Self::new(tags.0, tags.1)
        }
    }

    impl WorkerRunner {
        pub fn new(worker: impl Into<WorkerTag>, runner: impl Into<RunnerTag>) -> Self {
            Self(worker.into(), runner.into())
        }

        pub fn worker(&self) -> u32 {
            self.0 .0
        }

        pub fn runner(&self) -> u32 {
            self.1 .0
        }
    }

    #[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
    pub struct RunnerMeta {
        pub runner: WorkerRunner,
        /// Is this the only runner in the associated worker pool?
        pub is_singleton: bool,
        /// Does this runner have any reporters that output to stdout?
        pub has_stdout_reporters: bool,
    }

    impl RunnerMeta {
        pub fn new(
            runner: impl Into<WorkerRunner>,
            is_singleton: bool,
            has_stdout_reporters: bool,
        ) -> Self {
            Self {
                runner: runner.into(),
                is_singleton,
                has_stdout_reporters,
            }
        }

        pub fn singleton(worker: impl Into<WorkerTag>) -> Self {
            Self::new(WorkerRunner::new(worker, 1), true, false)
        }

        pub fn fake() -> Self {
            Self::singleton(0)
        }

        pub fn pipes_to_parent_stdio(&self) -> bool {
            self.is_singleton && !self.has_stdout_reporters
        }
    }

    #[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
    pub enum Tag {
        Runner(WorkerRunner),
        /// An anonymous client created through the ABQ interface (e.g. CLI)
        LocalClient,
        /// An external client created outside ABQ interfaces.
        ExternalClient,
    }

    impl Tag {
        pub fn runner(worker: impl Into<WorkerTag>, runner: impl Into<RunnerTag>) -> Self {
            Tag::Runner(WorkerRunner::new(worker, runner))
        }
    }

    impl std::fmt::Display for Tag {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Tag::Runner(worker_runner) => worker_runner.fmt(f),
                Tag::LocalClient => write!(f, "local client"),
                Tag::ExternalClient => write!(f, "external client"),
            }
        }
    }

    pub type EntityId = [u8; 16];

    /// Identifies a unique instance of an entity participating in the ABQ network ecosystem.
    /// The queue, workers, and abq test clients are all entities.
    #[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
    pub struct Entity {
        pub id: EntityId,
        pub tag: Tag,
    }

    impl Entity {
        pub fn new(tag: Tag) -> Self {
            Self {
                id: uuid::Uuid::new_v4().into_bytes(),
                tag,
            }
        }

        pub fn runner(worker: impl Into<WorkerTag>, runner: impl Into<RunnerTag>) -> Self {
            Self::new(Tag::Runner(WorkerRunner::new(worker, runner)))
        }

        pub fn first_runner(worker: impl Into<WorkerTag>) -> Self {
            Self::new(Tag::Runner(WorkerRunner::new(worker, 1)))
        }

        pub fn local_client() -> Self {
            Self::new(Tag::LocalClient)
        }

        pub fn external_client() -> Self {
            Self::new(Tag::ExternalClient)
        }

        pub fn display_id(&self) -> impl std::fmt::Display + '_ {
            uuid::Uuid::from_bytes_ref(&self.id)
        }

        #[cfg(feature = "expose-native-protocols")]
        pub fn fake() -> Self {
            Self {
                id: [7; 16],
                tag: Tag::LocalClient,
            }
        }
    }

    impl std::fmt::Debug for Entity {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{} ({})", self.display_id(), self.tag)
        }
    }
}

pub mod workers {
    use super::{
        queue::TestSpec,
        runners::{AbqProtocolVersion, Manifest, ManifestMessage, NativeRunnerSpecification},
    };
    use crate::capture_output::StdioOutput;
    use serde_derive::{Deserialize, Serialize};
    use std::{
        collections::HashMap,
        fmt::{self, Display},
        str::FromStr,
    };

    /// ID for a particular run of the queue, which sends many units of work.
    /// We allow run IDs to be arbitrary strings, so that users can choose an identifier that suits
    /// their use case best.
    #[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
    pub struct RunId(
        // NOTE: we could be clever here, for example, most identifiers are likely to be
        // 40 bytes or less. You could keep those on the stack and avoid clones everywhere, or have the
        // protocol messages always take `RunId`s by reference.
        //
        // But let's not be clever right now!
        pub String,
    );

    impl RunId {
        #[allow(clippy::new_without_default)] // Run IDs should be fresh, not defaulted
        pub fn unique() -> Self {
            Self(uuid::Uuid::new_v4().to_string())
        }
    }

    impl FromStr for RunId {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self(s.to_string()))
        }
    }

    impl Display for RunId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl fmt::Debug for RunId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// ABQ-internal-ID for a piece of work, reflected both during work queueing and completion.
    ///
    /// Distinct from the test ID reflected for a native test runner, this is used for associating
    /// manifest entries inside ABQ itself.
    #[derive(Serialize, Deserialize, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
    pub struct WorkId(pub [u8; 16]);

    impl WorkId {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            Self(uuid::Uuid::new_v4().into_bytes())
        }
    }

    impl std::fmt::Display for WorkId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", uuid::Uuid::from_bytes_ref(&self.0))
        }
    }
    impl std::fmt::Debug for WorkId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", uuid::Uuid::from_bytes_ref(&self.0))
        }
    }

    /// ABQ-internal-ID for a grouping
    /// In order to do file-based allocation to workers, we need to have a way of
    /// knowing which tests are in which file. We use this group id as a proxy for that.
    /// Eventually, these groupings will be assigned to specific workers
    #[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
    pub struct GroupId(pub [u8; 16]);

    impl GroupId {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            Self(uuid::Uuid::new_v4().into_bytes())
        }
    }

    impl std::fmt::Display for GroupId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", uuid::Uuid::from_bytes_ref(&self.0))
        }
    }
    impl std::fmt::Debug for GroupId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", uuid::Uuid::from_bytes_ref(&self.0))
        }
    }

    /// Runners used only for integration testing.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum TestLikeRunner {
        /// A worker that echos strings given to it.
        Echo,
        /// A worker that echos strings given to it as many test results, given by the separator.
        EchoMany { separator: char },
        /// A worker that echos initialization context.
        EchoInitContext,
        /// A worker that executes commands given to it.
        Exec,
        /// A worker that always times out.
        InduceTimeout,
        /// A worker that fails with "INDUCED FAIL" if given a test with the given name.
        FailOnTestName(String),
        /// A worker that errors before generating a manifest.
        NeverReturnManifest,
        /// A worker that hangs when the test run starts.
        HangOnTestStart,
        /// A worker that errors when a test of a name is received, never returning a result.
        NeverReturnOnTest(String),
        /// A worker that panics in a section of ABQ code.
        Panic,
        /// A worker that fails with "INDUCED FAIL" until the given run attempt number.
        FailUntilAttemptNumber(u32),
        /// Yields the given exit code.
        ExitWith(i32),
        /// A worker that echos a string given to it after a number of retries.
        #[cfg(feature = "test-actions")]
        EchoOnRetry(u8),
    }

    /// Environment variables a native test runner is initialized with.
    // Alias in case we want to change the representation later.
    pub type EnvironmentVariables = HashMap<String, String>;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct NativeTestRunnerParams {
        pub cmd: String,
        pub args: Vec<String>,
        pub extra_env: EnvironmentVariables,
    }

    /// The kind of runner that a worker should start.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum RunnerKind {
        GenericNativeTestRunner(NativeTestRunnerParams),
        TestLikeRunner(TestLikeRunner, Box<ManifestMessage>),
    }

    /// Each native test runner spawned by a worker has the environment variable
    /// `ABQ_RUNNER` set to a integer, counting from 1, that is unique to the runners launched by
    /// the worker.
    const ABQ_RUNNER: &str = "ABQ_RUNNER";

    impl RunnerKind {
        pub fn set_runner_id(&mut self, id: usize) {
            match self {
                RunnerKind::GenericNativeTestRunner(params) => {
                    debug_assert!(!params.extra_env.contains_key(ABQ_RUNNER));
                    params
                        .extra_env
                        .insert(ABQ_RUNNER.to_string(), id.to_string());
                }
                RunnerKind::TestLikeRunner(_, _) => {
                    // Do nothing
                }
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    pub struct WorkerTest {
        pub spec: TestSpec,
        /// The identity of the serial test suite run this test is a part of. Always starts at
        /// [INIT_RUN_NUMBER].
        /// Retried tests are always part of a larger run_number. For example, if test T is
        /// executed three times, it will be done so with run_number 1, 2, and 3.
        pub run_number: u32,
    }

    impl WorkerTest {
        pub fn new(spec: TestSpec, run_number: u32) -> Self {
            Self { spec, run_number }
        }
    }

    /// The initial test suite run number.
    /// Higher run numbers are reserved for retries of test suites.
    pub const INIT_RUN_NUMBER: u32 = 1;

    /// End-of-work
    #[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(transparent)]
    pub struct Eow(pub bool);

    impl std::ops::Not for Eow {
        type Output = bool;

        fn not(self) -> Self::Output {
            !self.0
        }
    }

    /// A bundle of work sent to a worker to be run in sequence.
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct NextWorkBundle {
        pub work: Vec<WorkerTest>,
        /// End of work
        pub eow: Eow,
    }

    impl NextWorkBundle {
        pub fn new(work: impl Into<Vec<WorkerTest>>, end_of_work: Eow) -> Self {
            Self {
                work: work.into(),
                eow: end_of_work,
            }
        }
    }

    /// Manifest reported to from a worker to the queue, including
    /// - test manifest
    /// - native test runner metadata
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct ReportedManifest {
        pub manifest: Manifest,
        pub native_runner_protocol: AbqProtocolVersion,
        // Boxed to decrease total size on the stack
        pub native_runner_specification: Box<NativeRunnerSpecification>,
    }

    static_assertions::assert_eq_size!(ReportedManifest, [u8; 72]);

    /// The result of a worker attempting to retrieve a manifest for a test command.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum ManifestResult {
        /// The manifest was successfully retrieved.
        Manifest(ReportedManifest),
        /// The worker failed to start the underlying test runner.
        TestRunnerError {
            /// Opaque error message from the failing test runner.
            error: String,
            /// Captured stdout/stderr from the failing runner.
            output: StdioOutput,
        },
    }

    static_assertions::assert_eq_size!(ManifestResult, [u8; 80]);
}

pub mod queue {
    use std::{fmt::Display, io, net::SocketAddr, num::NonZeroU64, str::FromStr};

    use serde_derive::{Deserialize, Serialize};

    use super::{
        entity::{Entity, Tag},
        meta::DeprecationRecord,
        results::OpaqueLazyAssociatedTestResults,
        runners::{AbqProtocolVersion, NativeRunnerSpecification, TestCase, TestResult},
        workers::{ManifestResult, RunId, WorkId},
        LARGE_MESSAGE_SIZE,
    };
    use crate::capture_output::StdioOutput;

    /// Information about the queue and its negotiation server.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct NegotiatorInfo {
        pub negotiator_address: SocketAddr,
        /// ABQ version
        pub version: String,
    }

    /// A marker that a test run has already completed.
    #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
    #[serde(tag = "type")]
    pub struct RunAlreadyCompleted {
        pub cancelled: bool,
    }

    // strategies for popping work off the queue
    #[derive(Default, Serialize, Deserialize, Debug, Clone, Copy)]
    pub enum TestStrategy {
        #[default]
        /// Traverse tests in order
        ByTest,

        /// Traverse tests grouped by top level group (which should, in most cases, be by-file, and if not, should still exhibit similar before- and after- work characteristics).
        /// using this strategy ensure each top level group gets distributed to a single worker.
        ///
        /// This should avoid running expensive before-group / after-group work from being run multiple times at the expense
        /// of uneven test distribution
        ByTopLevelGroup,
    }

    const STRATEGY_BY_TEST: &str = "by-test";
    const STRATEGY_BY_FILE: &str = "by-file";

    impl Display for TestStrategy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TestStrategy::ByTest => write!(f, "{}", STRATEGY_BY_TEST),
                TestStrategy::ByTopLevelGroup => write!(f, "{}", STRATEGY_BY_FILE),
            }
        }
    }

    impl FromStr for TestStrategy {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match s {
                STRATEGY_BY_TEST => Ok(Self::ByTest),
                STRATEGY_BY_FILE => Ok(Self::ByTopLevelGroup),
                other => Err(format!(
                    "Can't parse distribution strategy :'{}', must be default or by-group",
                    other
                )),
            }
        }
    }

    /// An ask to run some work by an invoker.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct InvokeWork {
        pub run_id: RunId,
        pub batch_size_hint: NonZeroU64,
        pub test_strategy: TestStrategy,
    }

    /// Specification of a test case to run.
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    pub struct TestSpec {
        /// ABQ-internal identity of this test.
        pub work_id: WorkId,
        /// The test case communicated to a native runner.
        pub test_case: TestCase,
    }

    /// A set of test results associated with an individual unit of work.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub struct AssociatedTestResults {
        /// The run number this test result comes from.
        pub work_id: WorkId,
        pub run_number: u32,
        pub results: Vec<TestResult>,
        pub before_any_test: StdioOutput,
        pub after_all_tests: Option<StdioOutput>,
    }

    impl AssociatedTestResults {
        pub fn has_fail_like_result(&self) -> bool {
            self.results.iter().any(|r| r.status.is_fail_like())
        }

        #[cfg(feature = "expose-native-protocols")]
        pub fn fake(work_id: WorkId, results: Vec<TestResult>) -> Self {
            Self {
                work_id,
                results,
                run_number: 1,
                before_any_test: StdioOutput::empty(),
                after_all_tests: None,
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    pub struct NativeRunnerInfo {
        pub protocol_version: AbqProtocolVersion,
        pub specification: NativeRunnerSpecification,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RunStatus {
        Active,
        /// All items in the manifest have been handed out.
        /// Workers may still be executing locally, for example in-band retries.
        InitialManifestDone {
            /// The number of workers that are still seen as active, for example executing retries, if any.
            num_active_workers: usize,
        },
        Cancelled,
    }

    /// Notification of how many active test runs are currently being processed by the queue.
    #[derive(Serialize, Deserialize)]
    pub struct ActiveTestRunsResponse {
        pub active_runs: u64,
    }

    /// A reason a test run was cancelled.
    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
    pub enum CancelReason {
        /// The test run was cancelled on a worker
        User,
        /// Timed out because no progress was made popping items off the manifest.
        ManifestHadNoProgress,
    }

    /// A request sent to the queue.
    #[derive(Serialize, Deserialize)]
    pub struct Request {
        pub entity: Entity,
        pub message: Message,
    }

    /// An acknowledgement from the queue that it received a manifest message.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckManifest {}

    /// An acknowledgement of receiving a bundle of test result from a worker.
    /// Sent by the queue.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckTestResults {}

    /// An acknowledgement of receiving a notification that a worker ran all tests.
    /// Sent by the queue.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckWorkerRanAllTests {}

    /// An acknowledgement of receiving a test cancellation request from a worker
    /// Sent by the queue.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckTestCancellation {}

    /// A message sent to the queue.
    #[derive(Serialize, Deserialize)]
    pub enum Message {
        HealthCheck,
        /// Asks the queue how many active test runs it has enqueued, returning an [ActiveTestRunsResponse].
        /// This is only useful for entities that manage test runs fed to a queue. Otherwise, the
        /// result provided here is only accurate for a moment in time during the request
        /// lifecycle.
        // TODO: should this be gated behind additional authz?
        ActiveTestRuns,
        /// An ask to return information needed to begin negotiation with the queue.
        NegotiatorInfo {
            run_id: RunId,
            /// Deprecations from an ABQ client.
            deprecations: DeprecationRecord,
        },
        /// An ask to mark an active test run as cancelled.
        CancelRun(RunId),
        /// A work manifest for a given run.
        ManifestResult(RunId, ManifestResult),
        /// The result of some work from the queue.
        WorkerResult(RunId, Vec<AssociatedTestResults>),
        /// Notification from a worker to the queue that it finished running all scheduled tests
        /// for a suite run.
        WorkerRanAllTests(RunId),
        /// A request to stream test results.
        TestResults(RunId),
        /// Query the queue for the state of a run at a particular moment in time.
        RunStatus(RunId),
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum TestResultsResponse {
        /// The test results are available.
        Results {
            /// A slice of test results.
            /// May be split off the full list to avoid exceeding the maximum network message size.
            chunk: OpaqueLazyAssociatedTestResults,
            final_chunk: bool,
        },
        /// Some test results are still being persisted, the request for test results should
        /// re-query in the future.
        Pending,
        /// Test results are not yet available because of the following outstanding runners.
        OutstandingRunners(Vec<Tag>),
        /// The test results are unavailable for the given reason.
        Error(String),
    }

    impl TestResultsResponse {
        const MAX_OVERHEAD_OF_RESPONSE_FOR_RESULTS: usize = 50;

        /// Splits [OpaqueLazyAssociatedTestResults] into network-safe chunks ready for wrapping by
        /// this response type.
        pub fn chunk_results(
            results: OpaqueLazyAssociatedTestResults,
        ) -> io::Result<Vec<OpaqueLazyAssociatedTestResults>> {
            results.into_network_safe_chunks(
                LARGE_MESSAGE_SIZE - Self::MAX_OVERHEAD_OF_RESPONSE_FOR_RESULTS,
            )
        }
    }

    #[cfg(test)]
    mod test {
        use crate::net_protocol::results::OpaqueLazyAssociatedTestResults;

        use super::TestResultsResponse;

        #[test]
        fn max_overhead_of_response_for_results() {
            let results = OpaqueLazyAssociatedTestResults::from_raw_json_lines(vec![
                serde_json::value::to_raw_value(r#"RWXRWX"#).unwrap(),
                serde_json::value::to_raw_value(r#"rwxrwx"#).unwrap(),
            ]);
            let results_len = serde_json::to_vec(&results).unwrap().len();

            for final_chunk in [true, false] {
                let response = TestResultsResponse::Results {
                    chunk: results.clone(),
                    final_chunk,
                };
                let response_len = serde_json::to_vec(&response).unwrap().len();

                let overhead = response_len - results_len;

                assert!(
                    overhead <= TestResultsResponse::MAX_OVERHEAD_OF_RESPONSE_FOR_RESULTS,
                    "{overhead}"
                );
            }
        }
    }
}

pub mod results {
    use std::io;

    use serde_derive::{Deserialize, Serialize};

    use super::{
        queue::{AssociatedTestResults, NativeRunnerInfo},
        write_message_bytes_help,
    };

    /// A line in the results-persistence scheme.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub enum ResultsLine {
        /// A list of results
        Results(Vec<AssociatedTestResults>),
        /// Summary of information related to the test suite.
        /// Exactly one of these will appear the results persistence scheme.
        Summary(Summary),
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    pub struct Summary {
        pub manifest_size_nonce: u64,
        pub native_runner_info: NativeRunnerInfo,
    }

    /// A lazy-loaded representation of [AssociatedTestResults].
    /// The implementor should store the results as JSON lines of JSON-encoded [AssociatedTestResults].
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct OpaqueLazyAssociatedTestResults(Vec<Box<serde_json::value::RawValue>>);

    impl OpaqueLazyAssociatedTestResults {
        pub fn from_raw_json_lines(opaque_lines: Vec<Box<serde_json::value::RawValue>>) -> Self {
            Self(opaque_lines)
        }

        /// Splits the results into chunks that respect the [maximum network message size][MAX_MESSAGE_SIZE].
        /// Assumes COMPRESS_LARGE is turned on, with the possibility of gz-encoded messages.
        pub(super) fn into_network_safe_chunks(
            self,
            max_message_size: usize,
        ) -> io::Result<Vec<Self>> {
            // Preserve the order of test result lines across chunks.
            // This is useful for reproducibility, even though the lines are opaque and can be
            // processed out-of-order.
            // As such, we keep a stack to process, initially populated in the same order as the
            // opaque chunks.
            let mut to_process = vec![self];
            let mut processed = Vec::with_capacity(1);
            while let Some(chunk) = to_process.pop() {
                let (encoded_msg, _) = write_message_bytes_help::<
                    _,
                    true, /* COMPRESS_LARGE on */
                >(&chunk, max_message_size)?;
                if encoded_msg.len() > max_message_size {
                    // Split the chunk in two.
                    let half = chunk.0.len() / 2;
                    let mut left = chunk.0;
                    let right = left.split_off(half);
                    if left.is_empty() || right.is_empty() {
                        // The chunk was a singleton, and unfortunately, we can't split it any
                        // further here.
                        // TODO: we could get further by decoding the chunk into test results and
                        // splitting those. However, I (Ayaz) suspect this will not be a problem in
                        // practice unless someone is using a very large batch size.
                        processed.push(OpaqueLazyAssociatedTestResults(left));
                        processed.push(OpaqueLazyAssociatedTestResults(right));
                    } else {
                        // Push the chunks back on so that the first half (the left one) will be
                        // processed first.
                        to_process.push(OpaqueLazyAssociatedTestResults(right));
                        to_process.push(OpaqueLazyAssociatedTestResults(left));
                    }
                } else {
                    processed.push(chunk);
                }
            }
            Ok(processed)
        }

        pub fn decode(&self) -> serde_json::Result<Vec<ResultsLine>> {
            let mut results = Vec::with_capacity(self.0.len());
            for results_list in self.0.iter() {
                results.push(serde_json::from_str(results_list.get())?);
            }
            Ok(results)
        }
    }

    impl PartialEq for OpaqueLazyAssociatedTestResults {
        fn eq(&self, other: &Self) -> bool {
            if self.0.len() != other.0.len() {
                return false;
            }
            for (l, r) in self.0.iter().zip(other.0.iter()) {
                if l.get() != r.get() {
                    return false;
                }
            }
            true
        }
    }

    #[cfg(test)]
    mod test {
        use super::OpaqueLazyAssociatedTestResults;

        #[test]
        fn opaque_lazy_results_does_not_realloc_value() {
            let opaque = OpaqueLazyAssociatedTestResults::from_raw_json_lines(vec![
                serde_json::value::to_raw_value(r#"hello"#).unwrap(),
            ]);

            let encoded = serde_json::to_string(&opaque).unwrap();
            assert_eq!(encoded, r#"["hello"]"#);
            let decoded: OpaqueLazyAssociatedTestResults = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded.0.len(), 1);
            assert_eq!(decoded.0[0].get(), r#""hello""#);
        }

        #[test]
        fn chunking() {
            let results = OpaqueLazyAssociatedTestResults::from_raw_json_lines(vec![
                // ["RWXRWX"] <- 10 bytes
                serde_json::value::to_raw_value(r#"RWXRWX"#).unwrap(),
                // ["R","R"] <- 9 bytes
                serde_json::value::to_raw_value(r#"R"#).unwrap(),
                serde_json::value::to_raw_value(r#"R"#).unwrap(),
                // ["rwxrwx"] <- 10 bytes
                serde_json::value::to_raw_value(r#"rwxrwx"#).unwrap(),
                // ["r","r"] <- 9 bytes
                serde_json::value::to_raw_value(r#"r"#).unwrap(),
                serde_json::value::to_raw_value(r#"r"#).unwrap(),
            ]);

            let expected_chunks = vec![
                OpaqueLazyAssociatedTestResults::from_raw_json_lines(vec![
                    serde_json::value::to_raw_value(r#"RWXRWX"#).unwrap(),
                ]),
                OpaqueLazyAssociatedTestResults::from_raw_json_lines(vec![
                    serde_json::value::to_raw_value(r#"R"#).unwrap(),
                    serde_json::value::to_raw_value(r#"R"#).unwrap(),
                ]),
                OpaqueLazyAssociatedTestResults::from_raw_json_lines(vec![
                    serde_json::value::to_raw_value(r#"rwxrwx"#).unwrap(),
                ]),
                OpaqueLazyAssociatedTestResults::from_raw_json_lines(vec![
                    serde_json::value::to_raw_value(r#"r"#).unwrap(),
                    serde_json::value::to_raw_value(r#"r"#).unwrap(),
                ]),
            ];

            let chunks = results.into_network_safe_chunks(10).unwrap();
            assert_eq!(chunks.len(), 4, "{chunks:?}");
            assert_eq!(chunks, expected_chunks);
        }
    }
}

pub mod work_server {
    use serde_derive::{Deserialize, Serialize};

    use super::{
        entity::Entity,
        error::RetryManifestError,
        runners::MetadataMap,
        workers::{self, NextWorkBundle, RunId},
    };

    #[derive(Serialize, Deserialize)]
    pub struct Request {
        pub entity: Entity,
        pub message: Message,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum Message {
        HealthCheck,
        /// An ask to get the initialization metadata for a
        InitContext {
            run_id: RunId,
        },

        /// A worker is connecting with the intent to hold a persistent connection
        /// over which it will request next tests to run.
        PersistentWorkerNextTestsConnection(RunId),

        /// An ask to the partition of a manifest assigned to a given entity for the given run.
        RetryManifestPartition {
            run_id: RunId,
            entity: Entity,
        },
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct InitContext {
        pub init_meta: MetadataMap,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum InitContextResponse {
        /// The manifest is yet to be received; try again later
        WaitingForManifest,
        InitContext(InitContext),
        /// The run is already done, the worker can exit.
        RunAlreadyCompleted {
            /// Was the run cancelled?
            cancelled: bool,
        },
    }

    /// An ask to get the next test for a particular run from the queue.
    /// This should be requested from a [persistent connection][WorkServerRequest::PersistentWorkerNextTestsConnection].
    #[derive(Serialize, Deserialize, Debug)]
    #[serde(tag = "type")]
    pub struct NextTestRequest {}

    #[derive(Serialize, Deserialize, Debug)]
    pub enum NextTestResponse {
        /// The set of tests to run next
        Bundle(workers::NextWorkBundle),
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum RetryManifestResponse {
        Manifest(NextWorkBundle),
        /// The manifest has not been fully persisted yet; try again later.
        NotYetPersisted,
        Error(RetryManifestError),
    }
}

pub mod error {
    use serde_derive::{Deserialize, Serialize};
    use thiserror::Error;

    #[derive(Debug, Error, Serialize, Deserialize, PartialEq, Eq)]
    pub enum FetchTestsError {
        #[error("{0}")]
        RetryManifest(#[from] RetryManifestError),
    }

    #[derive(Debug, Error, Serialize, Deserialize, PartialEq, Eq)]
    pub enum RetryManifestError {
        #[error("attempted to retry a test run that does not exist")]
        RunDoesNotExist,
        #[error("manifest has not been received for the given run, so it cannot be retried")]
        ManifestNeverReceived,
        #[error("retry manifest failed to be loaded by ABQ")]
        FailedToLoad,
        #[error("ABQ test suite run cannot be retried because the run was previously cancelled")]
        RunCancelled,
    }
}

pub fn publicize_addr(mut socket_addr: SocketAddr, public_ip: IpAddr) -> SocketAddr {
    socket_addr.set_ip(public_ip);
    socket_addr
}

/// Threshold at which messages are considered "large", and typically compressed.
/// See `COMPRESS_LARGE`.
const LARGE_MESSAGE_SIZE: usize = 1_000_000; // 1MB

pub fn is_large_message(v: &impl serde::Serialize) -> io::Result<bool> {
    Ok(serde_json::to_vec(v)?.len() > LARGE_MESSAGE_SIZE)
}

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_READ: Duration = Duration::from_micros(10);

fn gz_decode(buf: &[u8]) -> io::Result<Vec<u8>> {
    let mut result_buf = Vec::with_capacity(buf.len() * 2);
    flate2::read::GzDecoder::new(buf).read_to_end(&mut result_buf)?;
    Ok(result_buf)
}

fn gz_encode(buf: &[u8]) -> io::Result<Vec<u8>> {
    let result_buf = Vec::with_capacity(buf.len() * 2);
    // Use the default compression level of 6
    let mut encoder = flate2::write::GzEncoder::new(result_buf, flate2::Compression::new(6));
    encoder.write_all(buf)?;
    encoder.finish()
}

/// Reads a message from a stream communicating with abq.
///
/// The provided stream must be non-blocking.
pub fn read<T: serde::de::DeserializeOwned>(reader: &mut impl Read) -> Result<T, std::io::Error> {
    read_help(reader, READ_TIMEOUT)
}

fn read_help<T: serde::de::DeserializeOwned>(
    reader: &mut impl Read,
    timeout: Duration,
) -> Result<T, std::io::Error> {
    // Do not timeout waiting for the first four bytes, since it may well be reasonable that we are
    // waiting on an open connection for a new message.
    let mut msg_size_buf = [0; 4];
    read_exact_help(reader, &mut msg_size_buf, Duration::MAX)?;

    let msg_size = i32::from_be_bytes(msg_size_buf);
    let needs_decompression = msg_size.is_negative();
    let msg_size = msg_size.unsigned_abs();

    let mut msg_buf = vec![0; msg_size as usize];
    read_exact_help(reader, &mut msg_buf, timeout)?;

    if needs_decompression {
        msg_buf = gz_decode(&msg_buf)?;
    }

    let msg = serde_json::from_slice(&msg_buf)?;
    Ok(msg)
}

fn read_exact_help(
    reader: &mut impl Read,
    mut buf: &mut [u8],
    timeout: Duration,
) -> io::Result<()> {
    // Derived from the Rust standard library implementation of `default_read_exact`, found at
    // https://github.com/rust-lang/rust/blob/160b19429523ea44c4c3b7cad4233b2a35f58b8f/library/std/src/io/mod.rs#L449
    // Dual-licensed under MIT and Apache. Thank you, Rust contributors.

    let start = Instant::now();
    while !buf.is_empty() {
        if start.elapsed() >= timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out attempting to read exact message size",
            ));
        }
        match reader.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => std::thread::sleep(POLL_READ),
            Err(e) => return Err(e),
        }
    }
    if !buf.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        ))
    } else {
        Ok(())
    }
}

/// Writes a message to a stream communicating with abq.
pub fn write<T: serde::Serialize>(writer: &mut impl Write, msg: T) -> Result<(), std::io::Error> {
    let (msg_bytes, compressed) = write_message_bytes_help::<_, true>(&msg, LARGE_MESSAGE_SIZE)?;

    let msg_size_buf = {
        let mut msg_size = msg_bytes.len() as i32;
        if compressed {
            msg_size *= -1
        };
        i32::to_be_bytes(msg_size)
    };

    let mut msg_buf = Vec::new();
    msg_buf.extend_from_slice(&msg_size_buf);
    msg_buf.extend_from_slice(&msg_bytes);

    // NB: to be safe, always flush after writing.
    // See `async_write` for motivation.
    writer.write_all(&msg_buf)?;
    writer.flush()?;

    Ok(())
}

/// A cancel-safe async reader of protocol messages.
///
/// In cases where an async read might be a part of a `select` branch, it's pivotal to use an
/// AsyncReader rather than [async_read] directly, to ensure that if the reading future is
/// cancelled, it can be resumed without losing place of where the read stopped.
///
/// If `COMPRESS_LARGE` is true, compression of large messages is enforced:
/// - a negative message size represents a [compressed][gz_encode] message.
///
/// `COMPRESS_LARGE` should only be applied for intra-ABQ-node communication. Compression reduces
/// chatter on the network, and is not relevant for communication between a native runner.
///
/// DoS protections like capping max message sizes are not enforced. Protection against malicious
/// actors that may craft large messages should happen at a higher level, by enabling
/// [TLS][ServerTlsStrategy] and [authentication][ServerAuthStrategy]. TLS and/or authentication security features
/// [are verified before messages are read][handshake].
///
/// [ServerTlsStrategy]: crate::tls::ServerTlsStrategy
/// [ServerAuthStrategy]: crate::auth::ServerAuthStrategy
/// [handshake]: crate::net_async::ServerHandshakeCtx::handshake
pub struct AsyncReader<const COMPRESS_LARGE: bool> {
    size_buf: [u8; 4],
    msg_size: Option<MsgSize>,
    msg_buf: Vec<u8>,
    read: usize,
    timeout: Duration,
    next_expiration: Option<tokio::time::Instant>,
}

/// An async reader with DOS protection enabled.
pub type AsyncReaderDos = AsyncReader<true>;

#[derive(Debug)]
struct MsgSize {
    size: usize,
    needs_decompression: bool,
}

impl Default for AsyncReader<true> {
    fn default() -> Self {
        Self::new(READ_TIMEOUT)
    }
}

impl<const COMPRESS_LARGE: bool> AsyncReader<COMPRESS_LARGE> {
    fn new(timeout: Duration) -> Self {
        Self {
            size_buf: [0; 4],
            msg_size: None,
            msg_buf: Default::default(),
            read: 0,
            timeout,
            next_expiration: None,
        }
    }
}

impl<const COMPRESS_LARGE: bool> AsyncReader<COMPRESS_LARGE> {
    /// Reads the next message from a given reader.
    ///
    /// Cancellation-safe, but the same `reader` must be provided between cancellable calls.
    /// If errors, not resumable.
    pub async fn next<R, T: serde::de::DeserializeOwned>(&mut self, reader: &mut R) -> io::Result<T>
    where
        R: tokio::io::AsyncReadExt + Unpin,
    {
        loop {
            match self.msg_size {
                None => {
                    // Do not timeout reading the message size, since we might just be waiting indefinitely for a
                    // new message to come in.
                    debug_assert_ne!(self.read, 4);

                    let num_bytes_read = reader.read(&mut self.size_buf[self.read..]).await?;
                    if num_bytes_read == 0 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "early eof"));
                    }

                    self.read += num_bytes_read;
                    if self.read == 4 {
                        let (msg_size, needs_decompression) = {
                            let raw_size = i32::from_be_bytes(self.size_buf);
                            (raw_size.unsigned_abs(), raw_size.is_negative())
                        };
                        self.read = 0;
                        self.msg_size = Some(MsgSize {
                            size: msg_size as _,
                            needs_decompression,
                        });
                    }
                }
                Some(MsgSize {
                    size,
                    needs_decompression,
                }) => {
                    // Prime the message buffer with the number of bytes we're looking to read here.
                    if self.read == 0 {
                        self.msg_buf.reserve(size);
                        self.msg_buf.extend(std::iter::repeat(0).take(size));
                    }

                    // Prime the expiration time, since we are now starting to read an incoming
                    // message.
                    if self.next_expiration.is_none() {
                        self.next_expiration = Some(tokio::time::Instant::now() + self.timeout);
                    }

                    let num_bytes_read = tokio::select! {
                        num_bytes_read = reader.read(&mut self.msg_buf[self.read..]) => {
                            num_bytes_read?
                        }
                        _ = tokio::time::sleep_until(self.next_expiration.unwrap()) => {
                            let read = self.read;
                            let msg = format!("timed out waiting to read {size} bytes; read {read}");
                            return Err(io::Error::new(io::ErrorKind::TimedOut, msg));
                        }
                    };

                    self.read += num_bytes_read;
                    if self.read == size {
                        let msg = if needs_decompression {
                            debug_assert!(COMPRESS_LARGE);
                            let msg_bytes = gz_decode(&self.msg_buf)?;
                            serde_json::from_slice(&msg_bytes)?
                        } else {
                            serde_json::from_slice(&self.msg_buf)?
                        };

                        // Clear for the next read
                        self.msg_size = None;
                        self.read = 0;
                        self.msg_buf.clear();
                        self.next_expiration = None;

                        return Ok(msg);
                    }
                }
            }
        }
    }
}

/// Like [read], but async.
///
/// Not cancellation-safe. If you need a cancellation-safe read, use an [AsyncReader].
pub async fn async_read<R, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T, std::io::Error>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    AsyncReader::<true>::new(READ_TIMEOUT).next(reader).await
}

/// Like [async_read], but for communication over a local network (namely, worker<->native
/// runner).
///
/// Not cancellation-safe. If you need a cancellation-safe read, use an [AsyncReader].
///
/// Disables DOS protections.
pub async fn async_read_local<R, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T, std::io::Error>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    AsyncReader::<false>::new(READ_TIMEOUT).next(reader).await
}

/// Like [write], but async.
///
/// Not cancellation-safe. Do not use this in `select!` branches!
///
/// Enables DOS protections.
pub async fn async_write<R, T: serde::Serialize>(
    writer: &mut R,
    msg: &T,
) -> Result<(), std::io::Error>
where
    R: tokio::io::AsyncWriteExt + Unpin,
{
    async_write_help::<R, T, true>(writer, msg).await
}

/// Like [async_write], but for communication over a local network (namely, worker<->native
/// runner).
/// Disables DOS protections.
pub async fn async_write_local<R, T: serde::Serialize>(
    writer: &mut R,
    msg: &T,
) -> Result<(), std::io::Error>
where
    R: tokio::io::AsyncWriteExt + Unpin,
{
    async_write_help::<R, T, false>(writer, msg).await
}

async fn async_write_help<R, T: serde::Serialize, const COMPRESS_LARGE: bool>(
    writer: &mut R,
    msg: &T,
) -> Result<(), std::io::Error>
where
    R: tokio::io::AsyncWriteExt + Unpin,
{
    let (msg_bytes, compressed) =
        write_message_bytes_help::<_, COMPRESS_LARGE>(msg, LARGE_MESSAGE_SIZE)?;

    let msg_size_buf = {
        let mut msg_size = msg_bytes.len() as i32;
        if compressed {
            msg_size *= -1
        };
        i32::to_be_bytes(msg_size)
    };

    let mut msg_buf = Vec::with_capacity(msg_size_buf.len() + msg_bytes.len());
    msg_buf.extend_from_slice(&msg_size_buf);
    msg_buf.extend_from_slice(&msg_bytes);

    // NB: to be safe, always flush after writing. [tokio::io::AsyncWrite::poll_write] makes no
    // guarantee about the behavior after `write_all`, including whether the implementing type must
    // flush any intermediate buffers upon destruction.
    //
    // In fact, our tokio-tls shim does not ensure TLS frames are flushed upon connection
    // destruction:
    //
    //   https://github.com/tokio-rs/tls/blob/master/tokio-rustls/src/client.rs#L138-L139
    writer.write_all(&msg_buf).await?;
    writer.flush().await?;

    Ok(())
}

#[inline]
fn write_message_bytes_help<T: serde::Serialize, const COMPRESS_LARGE: bool>(
    msg: &T,
    max_message_size: usize,
) -> io::Result<(Vec<u8>, bool)> {
    let (msg_bytes, compressed) = {
        let msg_json = serde_json::to_vec(msg)?;
        if COMPRESS_LARGE && msg_json.len() > max_message_size {
            (gz_encode(&msg_json)?, true)
        } else {
            (msg_json, false)
        }
    };
    Ok((msg_bytes, compressed))
}

#[cfg(test)]
mod test {
    use std::{
        io::{self, Write},
        time::Duration,
    };

    use rand::Rng;
    use tokio::io::AsyncWriteExt;

    use crate::net_protocol::{read_help, AsyncReader, LARGE_MESSAGE_SIZE};

    #[test]
    fn error_reads_that_timeout() {
        use std::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (mut server_conn, _) = server.accept().unwrap();
        server_conn.set_nonblocking(true).unwrap();

        let msg_size = 10_u32.to_be_bytes();
        client_conn.write_all(&msg_size).unwrap();

        let read_result: Result<(), _> = read_help(&mut server_conn, Duration::from_secs(0));
        assert!(read_result.is_err());
        let err = read_result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn error_reads_that_timeout_async() {
        use tokio::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server_conn, _) = server.accept().await.unwrap();

        let msg_size = 10_u32.to_be_bytes();
        client_conn.write_all(&msg_size).await.unwrap();

        let read_result: Result<(), _> = AsyncReader::<true>::new(Duration::from_secs(0))
            .next(&mut server_conn)
            .await;
        assert!(read_result.is_err());
        let err = read_result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn async_reader_is_cancellation_safe() {
        use tokio::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server_conn, _) = server.accept().await.unwrap();

        let splits = [
            // Chop up the size message
            &[0u8, 0] as &[u8],
            &[0, 10],
            // Chop up the rest of the message
            &[b'"', b'1', b'1'],
            &[b'1', b'1'],
            &[b'1', b'1', b'1', b'1', b'"'],
        ];

        let mut async_reader = AsyncReader::default();
        let mut splits = splits.iter().peekable();

        while let Some(split) = splits.next() {
            // The reader and server connection are only relevant in the `handle` below until
            // we cancel it, which we explicitly do on every iteration, so we can tell rustc to
            // pretend that these things are effectively static relative to the lifetime of the
            // reader job.
            let async_reader: &'static mut AsyncReader<true> =
                unsafe { std::mem::transmute(&mut async_reader) };
            let server_conn: &'static mut TcpStream =
                unsafe { std::mem::transmute(&mut server_conn) };

            let handle = tokio::spawn(async_reader.next::<_, String>(server_conn));

            client_conn.write_all(split).await.unwrap();

            if splits.peek().is_some() {
                handle.abort();
                let err = handle.await.unwrap_err();
                assert!(err.is_cancelled());
            } else {
                // After we read the last message, we should in fact end up with the message we
                // expect.
                let msg = handle.await.unwrap().unwrap();
                assert_eq!(msg, "11111111");
            }
        }
    }

    #[tokio::test]
    async fn async_reader_is_cancellation_safe_fuzz() {
        // Fuzz-test the AsyncReader to make sure it's cancel-safe.

        use tokio::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server_conn, _) = server.accept().await.unwrap();

        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            // Make ourselves a string message, built in the network protocol
            // <msg_size>"11111...111111"
            let (msg, expected_str) = {
                let raw_msg_size: u32 = rng.gen_range(10..100);
                let mut msg = vec![];
                msg.extend_from_slice(&u32::to_be_bytes(raw_msg_size + 2));
                msg.push(b'"');
                msg.extend(std::iter::repeat(b'1').take(raw_msg_size as usize));
                msg.push(b'"');
                (msg, "1".repeat(raw_msg_size as _))
            };

            // Choose slices of the msg that we'll send one-at-a-time, and make sure that
            // cancelling the async reader after each send doesn't drop the whole message.
            let splits = {
                let msg_len = msg.len();

                let num_splits = rng.gen_range(1..(msg_len / 2)) as usize;
                let mut split_idxs = vec![];
                while split_idxs.len() != num_splits {
                    let split_idx = rng.gen_range(1..msg_len - 1);
                    if split_idxs.contains(&split_idx) {
                        continue;
                    }
                    split_idxs.push(split_idx);
                }

                assert!(!split_idxs.contains(&msg_len));
                split_idxs.push(msg_len);
                split_idxs.sort();

                let mut splits = vec![];
                let mut i = 0;
                for j in split_idxs {
                    splits.push(&msg[i..j as usize]);
                    i = j as usize;
                }
                assert_eq!(splits.iter().map(|l| l.len()).sum::<usize>(), msg_len);
                splits
            };

            let mut async_reader = AsyncReader::default();
            let mut splits = splits.into_iter().peekable();

            while let Some(split) = splits.next() {
                // The reader and server connection are only relevant in the `handle` below until
                // we cancel it, which we explicitly do on every iteration, so we can tell rustc to
                // pretend that these things are effectively static relative to the lifetime of the
                // reader job.
                let async_reader: &'static mut AsyncReader<true> =
                    unsafe { std::mem::transmute(&mut async_reader) };
                let server_conn: &'static mut TcpStream =
                    unsafe { std::mem::transmute(&mut server_conn) };

                let handle = tokio::spawn(async_reader.next::<_, String>(server_conn));

                client_conn.write_all(split).await.unwrap();

                if splits.peek().is_some() {
                    handle.abort();
                    let err = handle.await.unwrap_err();
                    assert!(err.is_cancelled());
                } else {
                    // After we read the last message, we should in fact end up with the message we
                    // expect.
                    let msg = handle.await.unwrap().unwrap();
                    assert_eq!(msg, expected_str);
                }
            }
        }
    }

    #[test]
    fn send_read_compressed_message() {
        use std::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (mut server_conn, _) = server.accept().unwrap();
        server_conn.set_nonblocking(true).unwrap();

        let msg = "y".repeat(LARGE_MESSAGE_SIZE * 2);
        assert!(serde_json::to_vec(&msg).unwrap().len() > LARGE_MESSAGE_SIZE);

        super::write(&mut client_conn, &msg).unwrap();
        let read_msg: String = super::read(&mut server_conn).unwrap();

        assert_eq!(msg, read_msg);
    }

    #[tokio::test]
    async fn send_read_compressed_message_async() {
        use tokio::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server_conn, _) = server.accept().await.unwrap();

        let msg = "y".repeat(LARGE_MESSAGE_SIZE * 2);
        assert!(serde_json::to_vec(&msg).unwrap().len() > LARGE_MESSAGE_SIZE);

        let (write_res, read_res) = tokio::join!(
            super::async_write(&mut client_conn, &msg),
            super::async_read(&mut server_conn),
        );
        write_res.unwrap();
        let read_msg: String = read_res.unwrap();

        assert_eq!(msg, read_msg);
    }

    #[tokio::test]
    async fn send_read_large_local_message_async() {
        use tokio::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server_conn, _) = server.accept().await.unwrap();

        let msg = "y".repeat(LARGE_MESSAGE_SIZE * 2);
        assert!(serde_json::to_vec(&msg).unwrap().len() > LARGE_MESSAGE_SIZE);

        let (write_res, read_res) = tokio::join!(
            super::async_write_local(&mut client_conn, &msg),
            super::async_read_local(&mut server_conn),
        );
        write_res.unwrap();
        let read_msg: String = read_res.unwrap();

        assert_eq!(msg, read_msg);
    }
}
