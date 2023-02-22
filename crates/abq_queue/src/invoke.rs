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
    exit::ExitCode,
    net_async,
    net_opt::ClientOptions,
    net_protocol::{
        self,
        client::ReportedResult,
        entity::Entity,
        queue::{
            self, AssociatedTestResults, InvokeWork, InvokerTestData, Message, NativeRunnerInfo,
            RunStartData, TestSpec,
        },
        runners::CapturedOutput,
        workers::{RunId, RunnerKind, WorkId, INIT_RUN_NUMBER},
        AsyncReaderDos,
    },
};

use thiserror::Error;
use tokio::time::{Instant, Interval};

/// The default maximum amount of a time a client will wait between consecutive
/// test results streamed from the queue.
/// The current default is 1 hour.
pub const DEFAULT_CLIENT_POLL_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// The time to tick the test result handlers, if we are waiting on results in the meantime.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(500);

/// A client of [Abq]. Issues work to [Abq], and listens for test results from it.
pub struct Client {
    entity: Entity,
    abq_server_addr: SocketAddr,
    client: Box<dyn net_async::ConfiguredClient>,
    /// The test run this client is responsible for.
    run_id: RunId,
    /// The stream to the queue server.
    stream: Box<dyn net_async::ClientStream>,
    // The maximum amount of a time a client should wait between consecutive test results
    // streamed from the queue.
    poll_timeout: Duration,
    next_poll_timeout_at: Instant,
    tick_interval: Duration,
    /// Signal to cancel an active test run.
    cancellation_rx: RunCancellationRx,
    async_reader: AsyncReaderDos,
    current_run_attempt: u32,
    track_exit_code_in_band: bool,
    max_run_attempt: u32,
}

#[derive(Debug, Error)]
pub enum Error {
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

pub enum IncrementalTestData {
    Tick,
    Results(Vec<AssociatedTestResults>),
    Finished,
    RunStart(RunStartData),
}

pub fn run_cancellation_pair() -> (RunCancellationTx, RunCancellationRx) {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    (RunCancellationTx(tx), RunCancellationRx(rx))
}

#[must_use]
#[derive(Debug)]
pub struct CompletedRunData {
    /// The native test runner that was in use for this test run.
    pub native_runner_info: NativeRunnerInfo,
}

pub type StreamResult<H> = std::result::Result<(H, CompletedRunData), Error>;

pub trait ResultHandler {
    type InitData;

    fn create(manifest: Vec<TestSpec>, init: Self::InitData) -> Self;
    fn on_result(&mut self, work_id: WorkId, run_number: u32, result: ReportedResult);
    fn get_ordered_retry_manifest(&mut self, run_number: u32) -> Vec<TestSpec>;
    fn get_exit_code(&self) -> ExitCode;
    fn tick(&mut self);
}

impl Client {
    /// Invokes work on an instance of [Abq], returning a [Client].
    pub async fn invoke_work(
        entity: Entity,
        abq_server_addr: SocketAddr,
        client_options: ClientOptions<User>,
        run_id: RunId,
        runner: RunnerKind,
        batch_size_hint: NonZeroU64,
        test_results_timeout: Duration,
        retries: u32,
        tick_interval: Duration,
        cancellation_rx: RunCancellationRx,
        track_exit_code_in_band: bool,
    ) -> Result<Self, Error> {
        let client = client_options.build_async()?;
        let mut stream = client.connect(abq_server_addr).await?;

        tracing::debug!(?entity, ?run_id, "invoking new work");

        let max_run_attempt = INIT_RUN_NUMBER + retries;

        let invoke_request = queue::Request {
            entity,
            message: Message::InvokeWork(InvokeWork {
                run_id: run_id.clone(),
                runner,
                batch_size_hint,
                test_results_timeout,
                max_run_attempt,
                track_exit_code_in_band,
            }),
        };

        net_protocol::async_write(&mut stream, &invoke_request).await?;
        let response: queue::InvokeWorkResponse = net_protocol::async_read(&mut stream).await?;

        if let queue::InvokeWorkResponse::Failure(err) = response {
            match err {
                queue::InvokeFailureReason::DuplicateRunId { recently_completed } => {
                    let error_msg = if recently_completed {
                        Error::DuplicateCompletedRun(run_id)
                    } else {
                        Error::DuplicateRun(run_id)
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
            next_poll_timeout_at: Self::next_poll_timeout_at_from_timeout(test_results_timeout),
            tick_interval,
            current_run_attempt: INIT_RUN_NUMBER,
            max_run_attempt,
            track_exit_code_in_band,
        })
    }

    /// Attempts to reconnect the client to the abq server.
    async fn reconnect(&mut self) -> Result<(), io::Error> {
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

    // We use the timeout to determine a deadline of when when specifically to timeout.
    // We update the deadline whenever we see activity.
    fn next_poll_timeout_at_from_timeout(poll_timeout: Duration) -> Instant {
        Instant::now() + poll_timeout
    }

    /// Yields the next test result, as it streams in.
    /// Returns [None] when there are no more test results.
    async fn next<H: ResultHandler>(
        &mut self,
        tick_interval: &mut Interval,
        handler: &mut Option<H>,
    ) -> Result<IncrementalTestData, Error> {
        loop {
            let read_result = tokio::select! {
                read = self.async_reader.next(&mut self.stream) => {
                    read
                }
                _ = tick_interval.tick() => {
                    return Ok(IncrementalTestData::Tick);
                }
                _ = tokio::time::sleep_until(self.next_poll_timeout_at) => {
                    tracing::error!(timeout=?self.poll_timeout, entity=?self.entity, run_id=?self.run_id, "timed out waiting for queue message");
                    self.cancel_active_run().await?;
                    return Err(TestResultError::TimedOut(self.poll_timeout).into());
                }
                _ = self.cancellation_rx.recv() => {
                    self.cancel_active_run().await?;
                    return Err(TestResultError::Cancelled.into());
                }
            };

            self.next_poll_timeout_at = Self::next_poll_timeout_at_from_timeout(self.poll_timeout);

            match read_result {
                Ok(InvokerTestData::Results(results)) => {
                    // Send an acknowledgement of the result to the server. If it fails, attempt to reconnect once.
                    let ack_result = net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::AckTestResults {},
                    )
                    .await;
                    if ack_result.is_err() {
                        self.reconnect().await?;
                    }

                    return Ok(IncrementalTestData::Results(results));
                }
                Ok(InvokerTestData::EndOfResults) => {
                    let is_last_run_attempt = self.current_run_attempt == self.max_run_attempt;
                    let opt_retry_manifest = match (is_last_run_attempt, handler.as_mut()) {
                        (true, _) | (false, None) => None,
                        (false, Some(handler)) => {
                            let retry_manifest =
                                handler.get_ordered_retry_manifest(self.current_run_attempt);

                            if !retry_manifest.is_empty() {
                                Some(retry_manifest)
                            } else {
                                None
                            }
                        }
                    };

                    match opt_retry_manifest {
                        None => {
                            // Finish up the test run.
                            //
                            // Send a final ACK of the test results, so that cancellation signals do not
                            // interfere with us here.
                            let exit_code = if self.track_exit_code_in_band {
                                let code = handler
                                    .as_ref()
                                    .expect("handler must be known with manifest at this point")
                                    .get_exit_code();
                                Some(code)
                            } else {
                                None
                            };

                            net_protocol::async_write(
                                &mut self.stream,
                                &net_protocol::client::EndOfResultsResponse::AckEnd { exit_code },
                            )
                            .await?;

                            return Ok(IncrementalTestData::Finished);
                        }
                        Some(ordered_manifest) => {
                            let next_run_attempt = self.current_run_attempt + 1;

                            net_protocol::async_write(
                                &mut self.stream,
                                &net_protocol::client::EndOfResultsResponse::AdditionalAttempts {
                                    ordered_manifest,
                                    attempt_number: next_run_attempt,
                                },
                            )
                            .await?;

                            self.current_run_attempt = next_run_attempt;

                            continue;
                        }
                    }
                }
                Ok(InvokerTestData::TestCommandError { error, captured }) => {
                    return Err(TestResultError::TestCommandError(error, captured).into())
                }
                Ok(InvokerTestData::TimedOut { after }) => {
                    // Send a final ACK of the test results, so that cancellation signals do not
                    // interfere with us here.
                    net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::EndOfResultsResponse::AckEnd {
                            exit_code: Some(ExitCode::CANCELLED),
                        },
                    )
                    .await?;

                    return Err(TestResultError::TimedOut(after).into());
                }
                Ok(InvokerTestData::RunStart(start_data)) => {
                    let ack_result = net_protocol::async_write(
                        &mut self.stream,
                        &net_protocol::client::AckRunStart {},
                    )
                    .await;
                    if ack_result.is_err() {
                        self.reconnect().await?;
                    }

                    return Ok(IncrementalTestData::RunStart(start_data));
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
    /// test results from the queue and cannot reconnect. Cedes control to the handler `H` when a
    /// result is received.
    pub async fn stream_results<H: ResultHandler>(
        mut self,
        handler_data: H::InitData,
    ) -> StreamResult<H> {
        use IncrementalTestData::*;

        let mut native_runner_info = None;
        let mut handler: Option<H> = None;
        let mut handler_data = Some(handler_data);

        // The first tick interval will complete immediately and will prime the handler;
        // all subsequent ticks obey the interval.
        let mut tick_interval = tokio::time::interval(self.tick_interval);

        loop {
            match self.next(&mut tick_interval, &mut handler).await? {
                Tick => {
                    if let Some(handler) = handler.as_mut() {
                        handler.tick();
                    }
                    continue;
                }
                Finished => {
                    break;
                }
                Results(results) => {
                    let handler = handler.as_mut().expect("illegal state - results handler not initialized before first result received");
                    for AssociatedTestResults {
                        work_id,
                        run_number,
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

                            handler.on_result(
                                work_id,
                                run_number,
                                ReportedResult {
                                    output_before,
                                    test_result,
                                    output_after,
                                },
                            )
                        }
                    }
                }
                RunStart(RunStartData {
                    native_runner_info: runner_info,
                    manifest,
                }) => {
                    assert!(
                        handler.is_none(),
                        "illegal state - initialized results handler before run start info"
                    );
                    assert!(
                        native_runner_info.is_none(),
                        "illegal state - native runner info received more than once"
                    );

                    native_runner_info = Some(runner_info);

                    let handler_data = std::mem::take(&mut handler_data)
                        .expect("illegal state - handler data can only be used once");
                    handler = Some(H::create(manifest, handler_data));
                }
            }
        }

        let native_runner_info = native_runner_info.expect(
            "illegal run state - run completed successfully but runner info never received",
        );
        let handler = handler.expect("illegal state - handler must be available after completion");

        Ok((handler, CompletedRunData { native_runner_info }))
    }
}
