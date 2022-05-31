use std::io::{Read, Write};
use std::time::Duration;

use abq_worker_protocol::Output;
use anyhow::Result;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
/// ID for a particular invocation of the queue, which sends many units of work.
pub(crate) struct InvocationId(pub [u8; 16]);

impl InvocationId {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }
}

/// ID for a piece of work, reflected both during work queueing and completion.
/// For example, a test may have ID "vanguard:test.rb:test_homepage".
// TODO: maybe we want uuids here and some mappings to test IDs
#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct WorkId(pub String);

/// A unit of work sent to a worker.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg(not(test))]
pub enum WorkerAction {
    Echo(String),
    Exec {
        cmd: String,
        args: Vec<String>,
        working_dir: std::path::PathBuf,
    },
}

/// A unit of work, specialized for testing.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg(test)]
pub enum WorkerAction {
    Echo(String),
    InduceTimeout,
    EchoOnRetry(usize, String),
}

/// The result of a worker's execution of a unit of work.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum WorkerResult {
    Output(Output),
    Timeout(Duration),
    Panic(String),
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Shutdown {
    pub timeout: Duration,
}

/// An ask to run some work by an invoker.
#[derive(Serialize, Deserialize)]
pub(crate) struct InvokeWork {
    pub invocation_id: InvocationId,
    pub work: Vec<(WorkId, WorkerAction)>,
}

/// An incremental response to an invoker.
#[derive(Serialize, Deserialize)]
pub(crate) enum InvokerResponse {
    /// The result of a requested unit of work.
    Result(WorkId, WorkerResult),
    /// No more results are known.
    EndOfResults,
}

/// A message sent to the queue.
#[derive(Serialize, Deserialize)]
pub(crate) enum Message {
    /// An ask to run some work by an invoker.
    InvokeWork(InvokeWork),
    /// The result of some work from the queue.
    WorkerResult(InvocationId, WorkId, WorkerResult),
    /// An ask to shutdown the queue.
    Shutdown(Shutdown),
}

// The read/write protocol is as follows:
// The first 4 bytes of any message is the size of the message (in big-endian order).
// The rest of the message are the contents, which are serde-serialized json.

/// Reads a message from a stream communicating with abq.
///
/// Note that [Read::read_exact] is used, and so the stream cannot be non-blocking.
pub(crate) fn read<T: serde::de::DeserializeOwned>(reader: &mut impl Read) -> Result<T> {
    let mut msg_size_buf = [0; 4];
    reader.read_exact(&mut msg_size_buf)?;
    let msg_size = u32::from_be_bytes(msg_size_buf);

    let mut msg_buf = vec![0; msg_size as usize];
    reader.read_exact(&mut msg_buf)?;

    let msg = serde_json::from_slice(&msg_buf)?;
    Ok(msg)
}

/// Writes a message to a stream communicating with abq.
pub(crate) fn write<T: serde::Serialize>(writer: &mut impl Write, msg: T) -> Result<()> {
    let msg_json = serde_json::to_vec(&msg)?;

    let msg_size = msg_json.len();
    let msg_size_buf = u32::to_be_bytes(msg_size as u32);

    let mut msg_buf = Vec::new();
    msg_buf.extend_from_slice(&msg_size_buf);
    msg_buf.extend_from_slice(&msg_json);
    writer.write_all(&msg_buf)?;
    Ok(())
}
