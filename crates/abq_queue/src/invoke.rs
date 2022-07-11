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

use std::net::{SocketAddr, TcpStream};

use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, InvokerResponse, Message},
    runners::TestResult,
    workers::{InvocationId, RunnerKind, WorkId},
};

/// Invokes work on an instance of [Abq]. This function blocks, but cedes control to [on_result]
/// when an individual result for a unit of work is received.
pub fn invoke_work<OnResult>(
    abq_server_addr: SocketAddr,
    invocation_id: InvocationId,
    runner: RunnerKind,
    mut on_result: OnResult,
) where
    OnResult: FnMut(WorkId, TestResult),
{
    let mut stream = TcpStream::connect(abq_server_addr).expect("socket not available");

    let invoke_msg = Message::InvokeWork(InvokeWork {
        invocation_id,
        runner,
    });

    net_protocol::write(&mut stream, invoke_msg).unwrap();

    while let InvokerResponse::Result(work_id, work_result) =
        net_protocol::read(&mut stream).expect("failed to read message")
    {
        on_result(work_id, work_result);
    }
}
