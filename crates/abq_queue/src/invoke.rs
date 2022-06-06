//! Invocation of jobs on the queue, and waiting for their results.
//!
//! Invoker <-> responder protocol:
//!
//! 1. [invoke_work] is called with a unique [invocation id][InvocationId] `IId`.
//! 1. The invoker keeps a stream open with the queue until it retrieves all work results.
//! 1. When a result for a unit of work with invocation id `IId` is received by the
//!    [responder][respond], it is sent back to the invoker in a piecemeal fashion.
//! 1. Once all results are communicated back by the responder, an
//!    [end-of-results][InvokerResponse::EndOfResults] message is sent and the responder-side of
//!    the stream is closed.
//! 1. Once it's done reading, the invoker closes its side of the stream.

use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::mpsc;

use crate::protocol::{self, InvokeWork, InvokerResponse, Message};
use abq_workers::protocol::{InvocationId, WorkId, WorkerAction, WorkerResult};

/// Invokes work on an instance of [Abq]. This function blocks, but cedes control to [on_result]
/// when an individual result for a unit of work is received.
pub fn invoke_work<OnResult>(
    abq_socket: &Path,
    work: Vec<(WorkId, WorkerAction)>,
    mut on_result: OnResult,
) where
    OnResult: FnMut(WorkId, WorkerResult),
{
    let mut stream = UnixStream::connect(abq_socket).expect("socket not available");

    let mut results_remaining = work.len();
    let invocation_id = InvocationId::new();
    let invoke_msg = Message::InvokeWork(InvokeWork {
        invocation_id,
        work,
    });

    protocol::write(&mut stream, invoke_msg).unwrap();

    loop {
        match protocol::read(&mut stream).expect("failed to read message") {
            InvokerResponse::Result(work_id, work_result) => {
                results_remaining -= 1;
                on_result(work_id, work_result);
            }
            InvokerResponse::EndOfResults => {
                debug_assert_eq!(results_remaining, 0);
                break;
            }
        }
    }
}

/// Communicates results of work back to an invoker as they are received by the queue.
/// Once all the work submitted is sent back, the connection to the invoker is terminated.
pub(crate) fn respond(
    mut invoker: UnixStream,
    results_rx: mpsc::Receiver<(WorkId, WorkerResult)>,
    mut results_remaining: usize,
) {
    while results_remaining > 0 {
        match results_rx.recv() {
            Ok((work_id, work_result)) => {
                protocol::write(&mut invoker, InvokerResponse::Result(work_id, work_result))
                    .unwrap();
            }
            Err(_) => {
                // TODO: there are many reasonable cases why there might be a receive error,
                // including the invoker process was terminated.
                // This needs to be handled gracefully, and we must tell the parent that it
                // should drop the channel results are being sent over when it happens.
                panic!("Invoker shutdown before all our messages were received")
            }
        }
        results_remaining -= 1;
    }
    protocol::write(&mut invoker, InvokerResponse::EndOfResults).unwrap();
    invoker.shutdown(std::net::Shutdown::Write).unwrap();
}
