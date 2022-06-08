use std::time::Duration;

use serde_derive::{Deserialize, Serialize};

use abq_workers::protocol::{InvocationId, WorkId, WorkUnit, WorkerResult};

#[derive(Serialize, Deserialize)]
pub(crate) struct Shutdown {
    pub timeout: Duration,
}

/// An ask to run some work by an invoker.
#[derive(Serialize, Deserialize)]
pub(crate) struct InvokeWork {
    pub invocation_id: InvocationId,
    pub work: Vec<(WorkId, WorkUnit)>,
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
