//! Run of jobs on the queue, and waiting for their results.
//!
//! Invoker <-> responder protocol:
//!
//! 1. [invoke_work] is called with a unique [run id][RunId] `IId`.
//! 1. The invoker keeps a stream open with the queue until it retrieves all work results.
//! 1. When a result for a unit of work with run id `IId` is received by the
//!    [responder][respond], it is sent back to the invoker in a piecemeal fashion.
//! 1. Once all results are communicated back by the responder, an
//!    [end-of-results][InvokerResponse::EndOfResults] message is sent and the responder-side of
//!    the stream is closed.
//! 1. Once it's done reading, the invoker closes its side of the stream.

use std::{io, net::SocketAddr, num::NonZeroU64};

use abq_utils::{
    net_async,
    net_opt::ClientOptions,
    net_protocol::{
        self,
        entity::EntityId,
        queue::{self, InvokeWork, InvokerTestResult, Message},
        runners::TestResult,
        workers::{RunId, RunnerKind, WorkId},
    },
};

use thiserror::Error;

/// A client of [Abq]. Issues work to [Abq], and listens for test results from it.
pub struct Client {
    pub(crate) entity: EntityId,
    pub(crate) abq_server_addr: SocketAddr,
    pub(crate) client: Box<dyn net_async::ConfiguredClient>,
    /// The test run this client is responsible for.
    pub(crate) run_id: RunId,
    /// The stream to the queue server.
    pub(crate) stream: Box<dyn net_async::ClientStream>,
}

#[derive(Debug, Error)]
pub enum InvocationError {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{0} corresponds to a test run already active on this queue; please provide a different test run ID.")]
    DuplicateRun(RunId),

    #[error("{0} corresponds to a test run already completed on this queue; please provide a different test run ID.")]
    DuplicateCompletedRun(RunId),
}

impl Client {
    /// Invokes work on an instance of [Abq], returning a [Client].
    pub async fn invoke_work(
        entity: EntityId,
        abq_server_addr: SocketAddr,
        client_options: ClientOptions,
        run_id: RunId,
        runner: RunnerKind,
        batch_size_hint: NonZeroU64,
    ) -> Result<Self, InvocationError> {
        let client = client_options.build_async()?;
        let mut stream = client.connect(abq_server_addr).await?;

        tracing::debug!(?entity, ?run_id, "invoking new work");

        let invoke_request = queue::Request {
            entity,
            message: Message::InvokeWork(InvokeWork {
                run_id: run_id.clone(),
                runner,
                batch_size_hint,
            }),
        };

        net_protocol::async_write(&mut stream, &invoke_request).await?;
        let response: queue::InvokeWorkResponse = net_protocol::async_read(&mut stream).await?;

        if let queue::InvokeWorkResponse::Failure(err) = response {
            match err {
                queue::InvokeFailureReason::DuplicateRunId { recently_completed } => {
                    let error_msg = if recently_completed {
                        InvocationError::DuplicateCompletedRun(run_id)
                    } else {
                        InvocationError::DuplicateRun(run_id)
                    };

                    return Err(error_msg);
                }
            }
        }

        Ok(Client {
            entity,
            abq_server_addr,
            client,
            run_id,
            stream,
        })
    }

    /// Attempts to reconnect the client to the abq server.
    pub(crate) async fn reconnect(&mut self) -> Result<(), io::Error> {
        let mut new_stream = self.client.connect(self.abq_server_addr).await?;

        net_protocol::async_write(
            &mut new_stream,
            &queue::Request {
                entity: self.entity,
                message: Message::Reconnect(self.run_id.clone()),
            },
        )
        .await?;

        self.stream = new_stream;
        Ok(())
    }

    /// Yields the next test result, as it streams in.
    /// Returns [None] when there are no more test results.
    pub async fn next(&mut self) -> Option<Result<Vec<(WorkId, TestResult)>, io::Error>> {
        loop {
            match net_protocol::async_read(&mut self.stream).await {
                Ok(InvokerTestResult::Results(results)) => {
                    // Send an acknowledgement of the result to the server. If it fails, attempt to reconnect once.
                    let ack_result = net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::AckTestResult {},
                    )
                    .await;
                    if ack_result.is_err() {
                        if let Err(err) = self.reconnect().await {
                            return Some(Err(err));
                        }
                    }

                    return Some(Ok(results));
                }
                Ok(InvokerTestResult::EndOfResults) => return None,
                Err(err) => {
                    // Attempt to reconnect once. If it's successful, just re-read the next message.
                    // If it's unsuccessful, return the original error.
                    match self.reconnect().await {
                        Ok(()) => continue,
                        Err(_) => return Some(Err(err)),
                    }
                }
            }
        }
    }

    /// Blockingly calls [Self::next] until the are no more test results, or we failed to retrieve
    /// test results from the queue and cannot reconnect. Cedes control to `on_result` when a
    /// result is received.
    pub async fn stream_results(
        mut self,
        mut on_result: impl FnMut(WorkId, TestResult),
    ) -> Result<(), io::Error> {
        while let Some(maybe_results) = self.next().await {
            let results = maybe_results?;
            for (work_id, test_result) in results {
                on_result(work_id, test_result);
            }
        }
        Ok(())
    }
}
