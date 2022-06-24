//! Simple read/write protocol for abq-related messages across networks.
//! The first 4 bytes of any message is the size of the message (in big-endian order).
//! The rest of the message are the contents, which are serde-serialized json.

use std::io::{Read, Write};

pub mod runners {
    use serde_derive::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum Action {
        TestId(TestId),
        // The following are mostly for testing
        Echo(String),
        Exec { cmd: String, args: Vec<String> },
    }

    #[derive(Serialize, Deserialize)]
    pub struct TestResult {
        test_id: TestId,
        success: bool,
        message: String,
    }

    pub type TestId = String;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Manifest {
        pub actions: Vec<Action>,
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
    pub struct Output {
        pub success: bool,
        pub message: String,
    }
}

pub mod workers {
    use super::runners::{Action, Manifest, Output};
    use serde_derive::{Deserialize, Serialize};
    use std::{path::PathBuf, time::Duration};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
    /// ID for a particular invocation of the queue, which sends many units of work.
    pub struct InvocationId(pub [u8; 16]);

    impl InvocationId {
        #[allow(clippy::new_without_default)] // Invocation IDs should be fresh, not defaulted
        pub fn new() -> Self {
            Self(uuid::Uuid::new_v4().into_bytes())
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

    /// The kind of runner that a worker should start.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum RunnerKind {
        GenericTestRunner,
        TestLikeRunner(TestLikeRunner, Manifest),
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
            action: Action,
            context: WorkContext,
            invocation_id: InvocationId,
            work_id: WorkId,
        },
        EndOfWork,
    }

    /// The result of a worker's execution of an action.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    pub enum WorkerResult {
        Output(Output),
        Timeout(Duration),
        Panic(String),
    }
}

pub mod queue {
    use serde_derive::{Deserialize, Serialize};

    use super::{
        runners::Manifest,
        workers::{InvocationId, WorkId, WorkerResult},
    };

    #[derive(Serialize, Deserialize)]
    pub struct Shutdown {}

    /// An ask to run some work by an invoker.
    #[derive(Serialize, Deserialize)]
    pub struct InvokeWork {
        pub invocation_id: InvocationId,
    }

    /// An incremental response to an invoker.
    #[derive(Serialize, Deserialize)]
    pub enum InvokerResponse {
        /// The result of a requested unit of work.
        Result(WorkId, WorkerResult),
        /// No more results are known.
        EndOfResults,
    }

    /// A message sent to the queue.
    #[derive(Serialize, Deserialize)]
    pub enum Message {
        /// An ask to run some work by an invoker.
        InvokeWork(InvokeWork),
        /// A work manifest for a given invocation.
        Manifest(InvocationId, Manifest),
        /// The result of some work from the queue.
        WorkerResult(InvocationId, WorkId, WorkerResult),
        /// An ask to shutdown the queue.
        Shutdown(Shutdown),
    }
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
