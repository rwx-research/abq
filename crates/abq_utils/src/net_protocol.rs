//! The protocol is described at
//!
//! https://www.notion.so/rwx/ABQ-Worker-Native-Test-Runner-IPQ-Interface-0959f5a9144741d798ac122566a3d887#f480d133e2c942719b1a0c0a9e76fb3a

use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
};

pub mod runners {
    use serde_derive::{Deserialize, Serialize};

    pub static ABQ_SOCKET: &str = "ABQ_SOCKET";
    pub static ABQ_GENERATE_MANIFEST: &str = "ABQ_GENERATE_MANIFEST";

    pub type MetadataMap = serde_json::Map<String, serde_json::Value>;

    #[derive(Serialize, Deserialize)]
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
    use std::{collections::HashMap, fmt::Display, path::PathBuf, str::FromStr};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
    /// ID for a particular invocation of the queue, which sends many units of work.
    ///
    // TODO: consider supporting arbitrary strings, to ease the generation of in some contexts
    // (like a CI server that wants to use the build number to identify the test run). Note that we
    // need uniqueness checking either way, which we don't do yet.
    pub struct InvocationId(pub [u8; 16]);

    impl InvocationId {
        #[allow(clippy::new_without_default)] // Invocation IDs should be fresh, not defaulted
        pub fn new() -> Self {
            Self(uuid::Uuid::new_v4().into_bytes())
        }
    }

    impl FromStr for InvocationId {
        type Err = String;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let id = uuid::Uuid::from_str(s)
                .map_err(|_| format!("{} is not a valid invocation ID", s))?;

            Ok(Self(id.into_bytes()))
        }
    }

    impl Display for InvocationId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", uuid::Uuid::from_bytes_ref(&self.0))
        }
    }

    /// ID for a piece of work, reflected both during work queueing and completion.
    /// For example, a test may have ID "vanguard:test.rb:test_homepage".
    // TODO: maybe we want uuids here and some mappings to test IDs
    #[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
    pub struct WorkId(pub String);

    /// Runners mostly used for testing.
    #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
    pub enum TestLikeRunner {
        /// A worker that echos strings given to it.
        Echo,
        /// A worker that executes commands given to it.
        Exec,
        /// A worker that always times out.
        #[cfg(feature = "test-actions")]
        InduceTimeout,
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
    #[derive(Serialize, Deserialize)]
    pub enum NextWork {
        Work {
            test_case: TestCase,
            context: WorkContext,
            invocation_id: InvocationId,
            work_id: WorkId,
        },
        EndOfWork,
    }
}

pub mod queue {
    use serde_derive::{Deserialize, Serialize};

    use super::{
        runners::{ManifestMessage, TestResult},
        workers::{InvocationId, RunnerKind, WorkId},
    };

    /// An ask to run some work by an invoker.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct InvokeWork {
        pub invocation_id: InvocationId,
        pub runner: RunnerKind,
    }

    /// An incremental response to an invoker.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum InvokerResponse {
        /// The result of a requested unit of work.
        Result(WorkId, TestResult),
        /// No more results are known.
        EndOfResults,
    }

    /// A message sent to the queue.
    #[derive(Serialize, Deserialize)]
    pub enum Message {
        /// An ask to return the address of the queue negotiator.
        NegotiatorAddr,
        /// An ask to run some work by an invoker.
        InvokeWork(InvokeWork),
        /// An invoker of a test run would like to reconnect to the queue for results streaming.
        Reconnect(InvocationId),
        /// A work manifest for a given invocation.
        Manifest(InvocationId, ManifestMessage),
        /// The result of some work from the queue.
        WorkerResult(InvocationId, WorkId, TestResult),
    }
}

pub mod client {
    use serde_derive::{Deserialize, Serialize};

    /// An acknowledgement of receiving a test result from the queue server. Sent by the client.
    #[derive(Serialize, Deserialize)]
    pub struct AckTestResult {}
}

pub fn publicize_addr(mut socket_addr: SocketAddr, public_ip: IpAddr) -> SocketAddr {
    socket_addr.set_ip(public_ip);
    socket_addr
}

/// Reads a message from a stream communicating with abq.
///
/// Note that [Read::read_exact] is used, and so the stream cannot be non-blocking.
pub fn read<T: serde::de::DeserializeOwned>(reader: &mut impl Read) -> Result<T, std::io::Error> {
    let mut msg_size_buf = [0; 4];
    reader.read_exact(&mut msg_size_buf)?;
    let msg_size = u32::from_be_bytes(msg_size_buf);

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

    let mut msg_buf = vec![0; msg_size as usize];
    reader.read_exact(&mut msg_buf).await?;

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
