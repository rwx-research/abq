//! The protocol is described at
//!
//! https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-IPQ-Interface-0959f5a9144741d798ac122566a3d887#f480d133e2c942719b1a0c0a9e76fb3a

use std::{
    io::{self, Read, Write},
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

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

pub mod entity {
    use serde_derive::{Deserialize, Serialize};

    /// Identifies a unique instance of an entity participating in the ABQ network ecosystem.
    /// The queue, workers, and abq test clients are all entities.
    #[derive(Serialize, Deserialize, Clone, Copy)]
    pub struct EntityId(pub [u8; 16]);

    impl EntityId {
        #[allow(clippy::new_without_default)] // Entity IDs should be fresh, not defaulted
        pub fn new() -> Self {
            Self(uuid::Uuid::new_v4().into_bytes())
        }
    }

    impl std::fmt::Debug for EntityId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", uuid::Uuid::from_bytes_ref(&self.0))
        }
    }
}

pub mod runners {
    use serde_derive::{Deserialize, Serialize};

    pub static ABQ_SOCKET: &str = "ABQ_SOCKET";
    pub static ABQ_GENERATE_MANIFEST: &str = "ABQ_GENERATE_MANIFEST";

    pub type MetadataMap = serde_json::Map<String, serde_json::Value>;

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
        pub id: String,
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
        /// The underlying native test runner failed unexpectedly.
        /// This is a state caught only be abq workers, and should never be reported directly.
        PrivateNativeRunnerError,
    }

    impl Status {
        /// If this status had to be classified as either a success or failure,
        /// would it be a failure?
        pub fn is_fail_like(&self) -> bool {
            matches!(self, Status::Failure | Status::Error)
        }
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

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Manifest {
        pub members: Vec<TestOrGroup>,
        pub init_meta: MetadataMap,
    }

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
    }

    pub const ACTIVE_PROTOCOL_VERSION_MAJOR: u64 = 0;
    pub const ACTIVE_PROTOCOL_VERSION_MINOR: u64 = 1;

    pub const ACTIVE_ABQ_PROTOCOL_VERSION: AbqProtocolVersion = AbqProtocolVersion {
        major: ACTIVE_PROTOCOL_VERSION_MAJOR,
        minor: ACTIVE_PROTOCOL_VERSION_MINOR,
    };

    #[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
    #[serde(tag = "type", rename = "abq_protocol_version")]
    pub struct AbqProtocolVersion {
        pub major: u64,
        pub minor: u64,
    }
}

pub mod workers {
    use super::runners::{
        AbqNativeRunnerSpecification, AbqProtocolVersion, Manifest, ManifestMessage, TestCase,
    };
    use serde_derive::{Deserialize, Serialize};
    use std::{
        collections::HashMap,
        fmt::{self, Display},
        path::PathBuf,
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

    /// ID for a piece of work, reflected both during work queueing and completion.
    /// For example, a test may have ID "vanguard:test.rb:test_homepage".
    // TODO: maybe we want uuids here and some mappings to test IDs
    #[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
    pub struct WorkId(pub String);

    /// Runners used only for integration testing.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum TestLikeRunner {
        /// A worker that echos strings given to it.
        Echo,
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
        /// A worker that errors when a test of a name is received, never returning a result.
        NeverReturnOnTest(String),
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
        TestLikeRunner(TestLikeRunner, ManifestMessage),
    }

    /// Context for a unit of work.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    pub struct WorkContext {
        pub working_dir: PathBuf,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct WorkerTest {
        pub test_case: TestCase,
        pub context: WorkContext,
        pub run_id: RunId,
        pub work_id: WorkId,
    }

    /// A unit of work sent to a worker to be run.
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub enum NextWork {
        Work(WorkerTest),
        EndOfWork,
    }

    impl NextWork {
        pub fn into_test(self) -> Option<WorkerTest> {
            match self {
                NextWork::Work(test) => Some(test),
                NextWork::EndOfWork => None,
            }
        }
    }

    /// A bundle of work sent to a worker to be run in sequence.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct NextWorkBundle(pub Vec<NextWork>);

    /// Manifest reported to from a worker to the queue, including
    /// - test manifest
    /// - native test runner metadata
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct ReportedManifest {
        pub manifest: Manifest,
        pub native_runner_protocol: AbqProtocolVersion,
        pub native_runner_specification: AbqNativeRunnerSpecification,
    }

    /// The result of a worker attempting to retrieve a manifest for a test command.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum ManifestResult {
        /// The manifest was successfully retrieved.
        Manifest(ReportedManifest),
        /// The worker failed to start the underlying test runner.
        TestRunnerError {
            /// Opaque error message from the failing test runner.
            error: String,
        },
    }
}

pub mod queue {
    use std::{num::NonZeroU64, time::Duration};

    use serde_derive::{Deserialize, Serialize};

    use super::{
        entity::EntityId,
        runners::{AbqNativeRunnerSpecification, AbqProtocolVersion, TestResult},
        workers::{ManifestResult, RunId, RunnerKind, WorkId},
    };

    /// A marker that a test run has already completed.
    #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
    #[serde(tag = "type")]
    pub struct RunAlreadyCompleted {}

    /// An ask to run some work by an invoker.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct InvokeWork {
        pub run_id: RunId,
        pub runner: RunnerKind,
        pub batch_size_hint: NonZeroU64,
    }

    /// Why an invocation of some work did not succeed.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum InvokeFailureReason {
        /// There is a run of the same ID already in progress, or recently completed.
        DuplicateRunId {
            /// A hint as to whether the run was determined to have recently completed.
            recently_completed: bool,
        },
    }

    /// Response from the queue regarding an ask to [invoke work][InvokeWork].
    #[derive(Serialize, Deserialize, Debug)]
    pub enum InvokeWorkResponse {
        /// The work invocation was acknowledged and is enqueued.
        /// The invoker should move to reading [test result][InvokerTestResult]s.
        Success,
        /// The work invocation failed.
        Failure(InvokeFailureReason),
    }

    /// A test result associated with an individual unit of work.
    pub type AssociatedTestResult = (WorkId, TestResult);

    /// An incremental unit of information about the state of a test suite, or its test result.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum InvokerTestData {
        /// A batch of test results.
        Results(Vec<AssociatedTestResult>),
        /// No more results are known.
        EndOfResults,
        /// The present test run has timed out, and not all test results have been reported in
        /// time.
        /// This is likely due to an error on a worker.
        TimedOut {
            /// How long after the timeout was set that it fired.
            after: Duration,
        },
        /// The given test command is determined to have failed for all workers associated with
        /// this test run, and the test run will not continue.
        TestCommandError {
            /// Opaque error message related to the failure of the test command.
            error: String,
        },
        /// Information about the native test runner being used for the current test suite.
        /// This message is sent exaclty once per test run, and usually received at the start of a
        /// test run.
        /// Receipt of this message by the supervisor intentionally does not block
        /// - the start of workers for a test run, or
        /// - the consumption of test results by the supervisor
        NativeRunnerInfo(NativeRunnerInfo),
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct NativeRunnerInfo {
        pub protocol_version: AbqProtocolVersion,
        pub specification: AbqNativeRunnerSpecification,
    }

    /// A response regarding the final result of a given test run, after all tests in the run are
    /// completed.
    #[derive(Serialize, Deserialize)]
    pub enum TotalRunResult {
        /// The run is still ongoing; the queue will need to be polled again to determine whether
        /// the result was a success.
        Pending,
        /// The result of the run.
        Completed { succeeded: bool },
    }

    /// Notification of how many active test runs are currently being processed by the queue.
    #[derive(Serialize, Deserialize)]
    pub struct ActiveTestRunsResponse {
        pub active_runs: u64,
    }

    /// A request sent to the queue.
    #[derive(Serialize, Deserialize)]
    pub struct Request {
        pub entity: EntityId,
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

    /// **ADMIN RESPONSE**
    /// The queue acknowledges retirement.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckRetirement {}

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
        /// An ask to return the address of the queue negotiator.
        NegotiatorAddr,
        /// An ask to run some work by an invoker.
        InvokeWork(InvokeWork),
        /// An ask to mark an active test run as cancelled.
        CancelRun(RunId),
        /// An invoker of a test run would like to reconnect to the queue for results streaming.
        Reconnect(RunId),
        /// A work manifest for a given run.
        ManifestResult(RunId, ManifestResult),
        /// The result of some work from the queue.
        WorkerResult(RunId, Vec<(WorkId, TestResult)>),
        /// An ask to return information about whether a given test run failed or not.
        /// A worker issues this request before exiting to determine whether they should exit
        /// cleanly, or fail.
        RequestTotalRunResult(RunId),

        /// A worker is connecting with the intent to hold a persistent connection over which test
        /// results will be communicated back.
        PersistentWorkerResultsConnection(RunId),

        /// **ADMIN REQUEST**
        /// Asks the queue to retire, rejecting any new test run requests.
        Retire,
    }
}

pub mod work_server {
    use serde_derive::{Deserialize, Serialize};

    use super::{
        runners::MetadataMap,
        workers::{self, RunId},
    };

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum WorkServerRequest {
        HealthCheck,
        /// An ask to get the initialization metadata for a
        InitContext {
            run_id: RunId,
        },
        /// An ask to get the next test for a particular run for the queue.
        NextTest {
            run_id: RunId,
        },

        /// A worker is connecting with the intent to hold a persistent connection
        /// over which it will request next tests to run.
        PersistentWorkerNextTestsConnection(RunId),
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
        RunAlreadyCompleted,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum NextTestResponse {
        /// The set of tests to run next
        Bundle(workers::NextWorkBundle),
    }
}

pub mod client {
    use serde_derive::{Deserialize, Serialize};

    /// An acknowledgement of receiving a test result from the queue server. Sent by the client.
    #[derive(Serialize, Deserialize)]
    pub struct AckTestData {}

    /// An acknowledgement of receiving a test result from the queue server. Sent by the client.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckTestRunEnded {}
}

pub fn publicize_addr(mut socket_addr: SocketAddr, public_ip: IpAddr) -> SocketAddr {
    socket_addr.set_ip(public_ip);
    socket_addr
}

fn validate_max_message_size(message_size: u32) -> Result<(), std::io::Error> {
    const MAX_MESSAGE_SIZE: u32 = 1_000_000; // 1MB
    if message_size > MAX_MESSAGE_SIZE {
        tracing::warn!("Refusing to read message of size {message_size} bytes");
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("message size {message_size} bytes exceeds the maximum"),
        ));
    }
    Ok(())
}

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_READ: Duration = Duration::from_micros(10);

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

    let msg_size = u32::from_be_bytes(msg_size_buf);
    validate_max_message_size(msg_size)?;

    let mut msg_buf = vec![0; msg_size as usize];
    read_exact_help(reader, &mut msg_buf, timeout)?;

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
    let msg_json = serde_json::to_vec(&msg)?;

    let msg_size = msg_json.len();
    let msg_size_buf = u32::to_be_bytes(msg_size as u32);

    let mut msg_buf = Vec::new();
    msg_buf.extend_from_slice(&msg_size_buf);
    msg_buf.extend_from_slice(&msg_json);
    writer.write_all(&msg_buf)?;
    Ok(())
}

/// Like [read], but async.
pub async fn async_read<R, T: serde::de::DeserializeOwned>(
    reader: &mut R,
) -> Result<T, std::io::Error>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    async_read_help(reader, READ_TIMEOUT).await
}

async fn async_read_help<R, T: serde::de::DeserializeOwned>(
    reader: &mut R,
    timeout: Duration,
) -> Result<T, std::io::Error>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    // Do not timeout reading the message size, since we might just be waiting indefinitely for a
    // new message to come in.
    let mut msg_size_buf = [0; 4];
    reader.read_exact(&mut msg_size_buf).await?;

    let msg_size = u32::from_be_bytes(msg_size_buf);
    validate_max_message_size(msg_size)?;

    let mut msg_buf = vec![0; msg_size as usize];

    tokio::select! {
        read_result = reader.read_exact(&mut msg_buf) => {
            read_result?;
        }
        _ = tokio::time::sleep(timeout) => {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "timed out attempting to read exact message size"));
        }
    }

    let msg = serde_json::from_slice(&msg_buf)?;
    Ok(msg)
}

/// Like [write], but async.
pub async fn async_write<R, T: serde::Serialize>(
    writer: &mut R,
    msg: &T,
) -> Result<(), std::io::Error>
where
    R: tokio::io::AsyncWriteExt + Unpin,
{
    let msg_json = serde_json::to_vec(msg)?;

    let msg_size = msg_json.len();
    let msg_size_buf = u32::to_be_bytes(msg_size as u32);

    let mut msg_buf = Vec::new();
    msg_buf.extend_from_slice(&msg_size_buf);
    msg_buf.extend_from_slice(&msg_json);
    writer.write_all(&msg_buf).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::{
        io::{self, Write},
        time::Duration,
    };

    use tokio::io::AsyncWriteExt;

    use crate::net_protocol::{async_read_help, read_help};

    use super::{async_read, read};

    #[test]
    fn error_reads_that_are_too_large() {
        use std::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (mut server_conn, _) = server.accept().unwrap();
        server_conn.set_nonblocking(true).unwrap();

        let two_mb_msg_size = 2_000_000_u32.to_be_bytes();
        client_conn.write_all(&two_mb_msg_size).unwrap();

        let read_result: Result<(), _> = read(&mut server_conn);
        assert!(read_result.is_err());
        let err = read_result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "message size 2000000 bytes exceeds the maximum"
        );
    }

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
    async fn error_reads_that_are_too_large_async() {
        use tokio::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server_conn, _) = server.accept().await.unwrap();

        let two_mb_msg_size = 2_000_000_u32.to_be_bytes();
        client_conn.write_all(&two_mb_msg_size).await.unwrap();

        let read_result: Result<(), _> = async_read(&mut server_conn).await;
        assert!(read_result.is_err());
        let err = read_result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "message size 2000000 bytes exceeds the maximum"
        );
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

        let read_result: Result<(), _> =
            async_read_help(&mut server_conn, Duration::from_secs(0)).await;
        assert!(read_result.is_err());
        let err = read_result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }
}
