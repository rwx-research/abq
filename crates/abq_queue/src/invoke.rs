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

use std::{io, net::SocketAddr, num::NonZeroU64, time::Duration};

use abq_utils::{
    auth::User,
    net_async,
    net_opt::ClientOptions,
    net_protocol::{
        self,
        client::ReportedResult,
        entity::EntityId,
        queue::{
            self, AssociatedTestResults, InvokeWork, InvokerTestData, Message, NativeRunnerInfo,
        },
        runners::CapturedOutput,
        workers::{RunId, RunnerKind},
        AsyncReaderDos,
    },
};

use thiserror::Error;
use tokio::time::Interval;

/// The default maximum amount of a time a client will wait between consecutive
/// test results streamed from the queue.
/// The current default is 1 hour.
pub const DEFAULT_CLIENT_POLL_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// The time to tick the test result handlers, if we are waiting on results in the meantime.
pub const TICK_TIMEOUT: Duration = Duration::from_millis(500);

/// A client of [Abq]. Issues work to [Abq], and listens for test results from it.
pub struct Client {
    pub(crate) entity: EntityId,
    pub(crate) abq_server_addr: SocketAddr,
    pub(crate) client: Box<dyn net_async::ConfiguredClient>,
    /// The test run this client is responsible for.
    pub(crate) run_id: RunId,
    /// The stream to the queue server.
    pub(crate) stream: Box<dyn net_async::ClientStream>,
    // The maximum amount of a time a client should wait between consecutive test results
    // streamed from the queue.
    pub(crate) poll_timeout: Duration,
    /// Signal to cancel an active test run.
    pub(crate) cancellation_rx: RunCancellationRx,
    pub(crate) async_reader: AsyncReaderDos,
}

#[derive(Debug, Error)]
pub enum InvocationError {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{0} corresponds to a test run already active on this queue; please provide a different test run ID.")]
    DuplicateRun(RunId),

    #[error("{0} corresponds to a test run already completed on this queue; please provide a different test run ID.")]
    DuplicateCompletedRun(RunId),

    #[error("{0}")]
    TestResultError(#[from] TestResultError),
}

#[derive(Debug, Error)]
pub enum TestResultError {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("The given test command failed to be executed by all workers. The recorded error message is:\n{0}")]
    TestCommandError(String, CapturedOutput),

    #[error("The given test command timed out.")]
    TimedOut(Duration),

    #[error("The test run was cancelled.")]
    Cancelled,
}

pub struct RunCancellationTx(tokio::sync::mpsc::Sender<()>);
pub struct RunCancellationRx(tokio::sync::mpsc::Receiver<()>);

impl RunCancellationTx {
    pub fn blocking_send(&self) -> io::Result<()> {
        self.0
            .blocking_send(())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "failed to send cancellation"))
    }

    pub async fn send(&self) -> io::Result<()> {
        self.0
            .send(())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "failed to send cancellation"))
    }
}

impl RunCancellationRx {
    pub async fn recv(&mut self) -> Option<()> {
        self.0.recv().await
    }
}

pub(crate) enum IncrementalTestData {
    Tick,
    Results(Vec<AssociatedTestResults>),
    Finished,
    NativeRunnerInfo(NativeRunnerInfo),
}

pub fn run_cancellation_pair() -> (RunCancellationTx, RunCancellationRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    (RunCancellationTx(tx), RunCancellationRx(rx))
}

#[must_use]
#[derive(Debug)]
pub struct CompletedSummary {
    /// The native test runner that was in use for this test run.
    pub native_runner_info: NativeRunnerInfo,
}

pub trait ResultHandler {
    fn on_result(&mut self, result: ReportedResult);
    fn tick(&mut self);
}

impl Client {
    /// Invokes work on an instance of [Abq], returning a [Client].
    pub async fn invoke_work(
        entity: EntityId,
        abq_server_addr: SocketAddr,
        client_options: ClientOptions<User>,
        run_id: RunId,
        runner: RunnerKind,
        batch_size_hint: NonZeroU64,
        test_results_timeout: Duration,
        cancellation_rx: RunCancellationRx,
        track_exit_code_in_band: bool,
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
                test_results_timeout,
                track_exit_code_in_band,
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
            poll_timeout: test_results_timeout,
            cancellation_rx,
            async_reader: Default::default(),
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

    /// Signals to the abq server that a test run should be cancelled prematurely.
    async fn cancel_active_run(&mut self) -> io::Result<()> {
        let mut conn = self.client.connect(self.abq_server_addr).await?;

        net_protocol::async_write(
            &mut conn,
            &queue::Request {
                entity: self.entity,
                message: Message::CancelRun(self.run_id.clone()),
            },
        )
        .await?;

        let queue::AckTestCancellation {} = net_protocol::async_read(&mut conn).await?;

        Ok(())
    }

    /// Yields the next test result, as it streams in.
    /// Returns [None] when there are no more test results.
    pub(crate) async fn next(
        &mut self,
        tick_interval: &mut Interval,
    ) -> Result<IncrementalTestData, TestResultError> {
        loop {
            let read_result = tokio::select! {
                read = self.async_reader.next(&mut self.stream) => {
                    read
                }
                _ = tick_interval.tick() => {
                    return Ok(IncrementalTestData::Tick);
                }
                _ = tokio::time::sleep(self.poll_timeout) => {
                    tracing::error!(timeout=?self.poll_timeout, entity=?self.entity, run_id=?self.run_id, "timed out waiting for queue message");
                    self.cancel_active_run().await?;
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "Timed out waiting for a message from the queue").into());
                }
                _ = self.cancellation_rx.recv() => {
                    self.cancel_active_run().await?;
                    return Err(TestResultError::Cancelled);
                }
            };

            match read_result {
                Ok(InvokerTestData::Results(results)) => {
                    // Send an acknowledgement of the result to the server. If it fails, attempt to reconnect once.
                    let ack_result = net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::AckTestData {},
                    )
                    .await;
                    if ack_result.is_err() {
                        self.reconnect().await?;
                    }

                    return Ok(IncrementalTestData::Results(results));
                }
                Ok(InvokerTestData::EndOfResults) => {
                    // Send a final ACK of the test results, so that cancellation signals do not
                    // interfere with us here.
                    net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::AckTestRunEnded {},
                    )
                    .await?;

                    return Ok(IncrementalTestData::Finished);
                }
                Ok(InvokerTestData::TestCommandError { error, captured }) => {
                    return Err(TestResultError::TestCommandError(error, captured))
                }
                Ok(InvokerTestData::TimedOut { after }) => {
                    // Send a final ACK of the test results, so that cancellation signals do not
                    // interfere with us here.
                    net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::AckTestRunEnded {},
                    )
                    .await?;

                    return Err(TestResultError::TimedOut(after));
                }
                Ok(InvokerTestData::NativeRunnerInfo(native_runner_info)) => {
                    let ack_result = net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::AckTestData {},
                    )
                    .await;
                    if ack_result.is_err() {
                        self.reconnect().await?;
                    }

                    return Ok(IncrementalTestData::NativeRunnerInfo(native_runner_info));
                }
                Err(err) => {
                    // Attempt to reconnect once. If it's successful, just re-read the next message.
                    // If it's unsuccessful, return the original error.
                    match self.reconnect().await {
                        Ok(()) => continue,
                        Err(_) => return Err(err.into()),
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
        mut handler: impl ResultHandler,
    ) -> Result<CompletedSummary, TestResultError> {
        use IncrementalTestData::*;

        let mut native_runner_info = None;

        // The first tick interval will complete immediately and will prime the handler;
        // all subsequent ticks obey the interval.
        let mut tick_interval = tokio::time::interval(TICK_TIMEOUT);

        loop {
            match self.next(&mut tick_interval).await? {
                Tick => {
                    handler.tick();
                    continue;
                }
                Finished => {
                    break;
                }
                Results(results) => {
                    for AssociatedTestResults {
                        work_id: _,
                        results,
                        before_any_test,
                        after_all_tests,
                    } in results
                    {
                        assert!(
                            !results.is_empty(),
                            "ABQ protocol must never ship empty list of test results"
                        );

                        let mut output_before = Some(before_any_test);
                        let mut output_after = after_all_tests;

                        let mut results = results.into_iter().peekable();

                        while let Some(test_result) = results.next() {
                            let output_before = std::mem::take(&mut output_before);
                            let output_after = if results.peek().is_none() {
                                // This is the last test result, the output-after is associated
                                // with it
                                std::mem::take(&mut output_after)
                            } else {
                                None
                            };

                            handler.on_result(ReportedResult {
                                output_before,
                                test_result,
                                output_after,
                            })
                        }
                    }
                }
                NativeRunnerInfo(runner_info) => {
                    assert!(
                        native_runner_info.is_none(),
                        "illegal run state - native runner info received more than once"
                    );

                    native_runner_info = Some(runner_info);
                }
            }
        }

        let native_runner_info = native_runner_info
            .expect("illegal run state - run completed succesfully but runner info never received");

        Ok(CompletedSummary { native_runner_info })
    }
}
