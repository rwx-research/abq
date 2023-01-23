//! The protocol is described at
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

        #[cfg(feature = "expose-native-protocols")]
        pub fn fake() -> Self {
            Self([7; 16])
        }
    }

    impl std::fmt::Debug for EntityId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", uuid::Uuid::from_bytes_ref(&self.0))
        }
    }
}

pub mod workers {
    use super::runners::{
        AbqProtocolVersion, Manifest, ManifestMessage, NativeRunnerSpecification, TestCase,
    };
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

    /// Runners used only for integration testing.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum TestLikeRunner {
        /// A worker that echos strings given to it.
        Echo,
        /// A worker that echos strings given to it as many test results, given by the separator.
        EchoMany { seperator: char },
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

    /// Each native test runner spawned by a worker has the enviornment variable
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

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct WorkerTest {
        pub test_case: TestCase,
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
        },
    }

    static_assertions::assert_eq_size!(ManifestResult, [u8; 72]);
}

pub mod queue {
    use std::{net::SocketAddr, num::NonZeroU64, time::Duration};

    use serde_derive::{Deserialize, Serialize};

    use super::{
        entity::EntityId,
        runners::{AbqProtocolVersion, CapturedOutput, NativeRunnerSpecification, TestResult},
        workers::{ManifestResult, RunId, RunnerKind, WorkId},
    };

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
    pub struct RunAlreadyCompleted {}

    /// An ask to run some work by an invoker.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct InvokeWork {
        pub run_id: RunId,
        pub runner: RunnerKind,
        pub batch_size_hint: NonZeroU64,
        pub test_results_timeout: Duration,
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

    /// A set of test results associated with an individual unit of work.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct AssociatedTestResults {
        pub work_id: WorkId,
        pub results: Vec<TestResult>,
        pub before_any_test: CapturedOutput,
        pub after_all_tests: Option<CapturedOutput>,
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
                before_any_test: CapturedOutput::empty(),
                after_all_tests: None,
            }
        }
    }

    /// An incremental unit of information about the state of a test suite, or its test result.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum InvokerTestData {
        /// A batch of test results.
        Results(Vec<AssociatedTestResults>),
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
        pub specification: NativeRunnerSpecification,
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

    /// An acknowledgement of receiving a test cancellation request from a supervisor
    /// Sent by the queue.
    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type")]
    pub struct AckTestCancellation {}

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
        /// An ask to return information needed to begin negotiation with the queue.
        NegotiatorInfo,
        /// An ask to run some work by an invoker.
        InvokeWork(InvokeWork),
        /// An ask to mark an active test run as cancelled.
        CancelRun(RunId),
        /// An invoker of a test run would like to reconnect to the queue for results streaming.
        Reconnect(RunId),
        /// A work manifest for a given run.
        ManifestResult(RunId, ManifestResult),
        /// The result of some work from the queue.
        WorkerResult(RunId, Vec<AssociatedTestResults>),
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
pub struct AsyncReader {
    size_buf: [u8; 4],
    msg_size: Option<usize>,
    msg_buf: Vec<u8>,
    read: usize,
    timeout: Duration,
    next_expiration: Option<tokio::time::Instant>,
}

impl Default for AsyncReader {
    fn default() -> Self {
        Self::new(READ_TIMEOUT)
    }
}

impl AsyncReader {
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

impl AsyncReader {
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
                        let msg_size = u32::from_be_bytes(self.size_buf);
                        validate_max_message_size(msg_size)?;
                        self.read = 0;
                        self.msg_size = Some(msg_size as _);
                    }
                }
                Some(size) => {
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
                        // Clear for the next read
                        let msg = serde_json::from_slice(&self.msg_buf)?;
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
    AsyncReader::default().next(reader).await
}

/// Like [write], but async.
///
/// Not cancellation-safe. Do not use this in `select!` branches!
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

    let mut msg_buf = Vec::with_capacity(msg_size_buf.len() + msg_json.len());
    msg_buf.extend_from_slice(&msg_size_buf);
    msg_buf.extend_from_slice(&msg_json);

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

#[cfg(test)]
mod test {
    use std::{
        io::{self, Write},
        time::Duration,
    };

    use rand::Rng;
    use tokio::io::AsyncWriteExt;

    use crate::net_protocol::{read_help, AsyncReader};

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

        let read_result: Result<(), _> = AsyncReader::new(Duration::from_secs(0))
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
            let async_reader: &'static mut AsyncReader =
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
                let async_reader: &'static mut AsyncReader =
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
}
