//! The protocol is described at
//!
//! https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-IPQ-Interface-0959f5a9144741d798ac122566a3d887#f480d133e2c942719b1a0c0a9e76fb3a

use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
};

pub mod health {
    pub type HEALTH = String;
    pub static HEALTHY: &str = "HEALTHY";
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
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct InitSuccessMessage {}

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TestCaseMessage {
        pub test_case: TestCase,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
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

    /// The result of a worker attempting to retrieve a manifest for a test command.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum ManifestResult {
        /// The manifest was successfully retrieved.
        Manifest(ManifestMessage),
        /// The worker failed to start the underlying test runner.
        TestRunnerError {
            /// Opaque error message from the failing test runner.
            error: String,
        },
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

    #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
    #[serde(rename_all = "snake_case")]
    pub enum AbqProtocolVersionTag {
        AbqProtocolVersion,
    }

    pub const ACTIVE_PROTOCOL_VERSION_MAJOR: u64 = 0;
    pub const ACTIVE_PROTOCOL_VERSION_MINOR: u64 = 1;

    #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
    pub struct AbqProtocolVersionMessage {
        pub r#type: AbqProtocolVersionTag,
        pub major: u64,
        pub minor: u64,
    }
}

pub mod workers {
    use super::runners::{ManifestMessage, TestCase};
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

    /// Runners mostly used for testing.
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
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkContext {
        pub working_dir: PathBuf,
    }

    /// A unit of work sent to a worker to be run.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum NextWork {
        Work {
            test_case: TestCase,
            context: WorkContext,
            run_id: RunId,
            work_id: WorkId,
        },
        EndOfWork,
    }

    /// A bundle of work sent to a worker to be run in sequence.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct NextWorkBundle(pub Vec<NextWork>);

    /// An acknowledgement from the queue that it received a manifest message.
    #[derive(Serialize, Deserialize)]
    pub struct AckManifest;
}

pub mod queue {
    use std::num::NonZeroU64;

    use serde_derive::{Deserialize, Serialize};

    use super::{
        entity::EntityId,
        runners::{ManifestResult, TestResult},
        workers::{RunId, RunnerKind, WorkId},
    };

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

    /// An incremental test result sent back to an invoker.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum InvokerTestResult {
        /// A batch of test results.
        Results(Vec<AssociatedTestResult>),
        /// No more results are known.
        EndOfResults,
        /// The given test command is determined to have failed for all workers associated with
        /// this test run, and the test run will not continue.
        TestCommandError {
            /// Opaque error message related to the failure of the test command.
            error: String,
        },
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
        WorkerResult(RunId, WorkId, TestResult),
        /// An ask to return information about whether a given test run failed or not.
        /// A worker issues this request before exiting to determine whether they should exit
        /// cleanly, or fail.
        RequestTotalRunResult(RunId),

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
    pub struct AckTestResult {}

    /// An acknowledgement of receiving a test result from the queue server. Sent by the client.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckEndOfTests {}
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

/// Reads a message from a stream communicating with abq.
///
/// Note that [Read::read_exact] is used, and so the stream cannot be non-blocking.
///
/// NOTE: this is susceptible to DoS by sending a message size that is larger than what can be
/// allocated. It's possible we'll be able to remove this as a plausible attack vector after adding
/// TLS + auth.
pub fn read<T: serde::de::DeserializeOwned>(reader: &mut impl Read) -> Result<T, std::io::Error> {
    let mut msg_size_buf = [0; 4];
    reader.read_exact(&mut msg_size_buf)?;
    let msg_size = u32::from_be_bytes(msg_size_buf);
    validate_max_message_size(msg_size)?;

    let mut msg_buf = vec![0; msg_size as usize];
    reader.read_exact(&mut msg_buf)?;

    let msg = serde_json::from_slice(&msg_buf)?;
    Ok(msg)
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
    let mut msg_size_buf = [0; 4];
    reader.read_exact(&mut msg_size_buf).await?;
    let msg_size = u32::from_be_bytes(msg_size_buf);
    validate_max_message_size(msg_size)?;

    let mut msg_buf = vec![0; msg_size as usize];
    reader.read_exact(&mut msg_buf).await?;

    let msg = serde_json::from_slice(&msg_buf)?;
    Ok(msg)
}

/// Like [write], but async.
///
/// NOTE: this is susceptible to DoS by sending a message size that is larger than what can be
/// allocated. It's possible we'll be able to remove this as a plausible attack vector after adding
/// TLS + auth.
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
    use std::io::Write;

    use tokio::io::AsyncWriteExt;

    use super::{async_read, read};

    #[test]
    fn error_reads_that_are_too_large() {
        use std::net::{TcpListener, TcpStream};

        let server = TcpListener::bind("0.0.0.0:0").unwrap();
        let mut client_conn = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (mut server_conn, _) = server.accept().unwrap();

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
}
