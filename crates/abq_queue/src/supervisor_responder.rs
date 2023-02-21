//! Queue-side communication to a supervisor.

use abq_utils::{
    net_async,
    net_protocol::{
        self,
        queue::{AssociatedTestResults, InvokerTestData, RunStartData},
    },
};
use futures::{future, TryFutureExt};

#[derive(Debug)]
pub struct BufferedResults {
    buffer: Vec<AssociatedTestResults>,
    max_size: usize,
}

impl BufferedResults {
    /// Default suggested sizing of the buffer. In general you should try to use a more precise
    /// size when possible.
    pub const DEFAULT_SIZE: usize = 10;

    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(max_size),
            max_size,
        }
    }

    /// Adds a new test result to the buffer. If the buffer capacity is exceeded, all test results
    /// to be drained are returned.
    fn extend(
        &mut self,
        results: impl IntoIterator<Item = AssociatedTestResults>,
    ) -> Option<Vec<AssociatedTestResults>> {
        self.buffer.extend(results);
        if self.buffer.len() >= self.max_size {
            return Some(self.buffer.drain(..).collect());
        }
        None
    }

    fn drain_all(&mut self) -> Vec<AssociatedTestResults> {
        std::mem::take(&mut self.buffer)
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// Handler for sending test results back to the abq client that issued a test run.
#[derive(Debug)]
pub enum SupervisorResponder {
    /// We have an open stream upon which to send back the test results.
    DirectStream {
        stream: Box<dyn net_async::ServerStream>,
        buffer: BufferedResults,
    },
    /// There is not yet an open connection we can stream the test results back on.
    /// This state may be reached during the time a client is disconnected.
    Disconnected {
        results: Vec<AssociatedTestResults>,
        pending_start_info: Option<RunStartData>,
        /// Size of the [buffer][BufferedResults] we should create when moving back into connected
        /// mode.
        buffer_size: usize,
    },
}

impl SupervisorResponder {
    pub fn new(stream: Box<dyn net_async::ServerStream>, buffer_size: usize) -> Self {
        Self::DirectStream {
            stream,
            buffer: BufferedResults::new(buffer_size),
        }
    }

    async fn stream_test_result_batch_help(
        stream: &mut Box<dyn net_async::ServerStream>,
        buffer: &mut BufferedResults,
        results: Vec<AssociatedTestResults>,
    ) -> Result<(), Self> {
        use net_protocol::client::AckTestResults;

        let batch_msg = InvokerTestData::Results(results);

        // Send the test results and wait for an ack. If either fails, suppose the client is
        // disconnected.
        let write_result = net_protocol::async_write(stream, &batch_msg).await;
        let client_disconnected = match write_result {
            Err(_) => true,
            Ok(()) => {
                let ack = net_protocol::async_read(stream).await;
                match ack {
                    Err(_) => true,
                    Ok(AckTestResults {}) => false,
                }
            }
        };

        if client_disconnected {
            // Demote ourselves to the disconnected state until reconnection happens.
            let results = match batch_msg {
                InvokerTestData::Results(results) => results,
                _ => unreachable!(),
            };

            // NOTE: we could store old, empty `buffer` here, so we moving back to
            // `DirectStream` doesn't cost an allocation.
            return Err(Self::Disconnected {
                results,
                buffer_size: buffer.max_size,
                pending_start_info: None,
            });
        }

        Ok(())
    }

    pub async fn send_test_results(
        &mut self,
        test_results: impl IntoIterator<Item = AssociatedTestResults>,
    ) {
        match self {
            SupervisorResponder::DirectStream { stream, buffer } => {
                let opt_results_to_drain = buffer.extend(test_results);

                if let Some(results) = opt_results_to_drain {
                    let opt_disconnected =
                        Self::stream_test_result_batch_help(stream, buffer, results).await;
                    if let Err(disconnected) = opt_disconnected {
                        *self = disconnected;
                    }
                }
            }
            SupervisorResponder::Disconnected {
                results,
                buffer_size: _,
                pending_start_info: _,
            } => {
                results.extend(test_results);
            }
        }
    }

    pub async fn flush_results(&mut self) {
        match self {
            SupervisorResponder::DirectStream { stream, buffer } => {
                let remaining_results = buffer.drain_all();

                if !remaining_results.is_empty() {
                    let opt_disconnected =
                        Self::stream_test_result_batch_help(stream, buffer, remaining_results)
                            .await;

                    debug_assert!(
                        buffer.is_empty(),
                        "buffer should remain empty after drainage"
                    );

                    if let Err(disconnected) = opt_disconnected {
                        *self = disconnected;
                    }
                }
            }
            SupervisorResponder::Disconnected { .. } => {
                // nothing we can do
            }
        }
    }

    pub async fn send_run_start_info(&mut self, start_info: RunStartData) {
        match self {
            SupervisorResponder::DirectStream { stream, buffer } => {
                let start_info_message = InvokerTestData::RunStart(start_info);

                let send_recv_result =
                    future::ready(net_protocol::async_write(stream, &start_info_message).await)
                        .and_then(|_| net_protocol::async_read(stream))
                        .await;

                let client_disconnected = match send_recv_result {
                    Ok(net_protocol::client::AckRunStart {}) => false,
                    Err(_) => true,
                };

                if client_disconnected {
                    // Demote ourselves to the disconnected state until reconnection happens.
                    tracing::info!(peer_addr=?stream.peer_addr(), "failed to send runner specification; demoting to supervisor to disconnected state");

                    let start_info = match start_info_message {
                        InvokerTestData::RunStart(info) => info,
                        _ => unreachable!(),
                    };

                    *self = Self::Disconnected {
                        results: buffer.drain_all(),
                        buffer_size: buffer.max_size,
                        pending_start_info: Some(start_info),
                    };
                }
            }
            SupervisorResponder::Disconnected {
                pending_start_info, ..
            } => {
                debug_assert!(pending_start_info.is_none());
                *pending_start_info = Some(start_info);
            }
        }
    }

    /// Updates the responder with a new connection. If there are any pending test results that
    /// failed to be sent from a previous connections, they are streamed to the new connection
    /// before the future returned from this function completes.
    pub async fn update_connection_to(&mut self, new_conn: Box<dyn net_async::ServerStream>) {
        // There's no great way for us to check whether the existing client connection is,
        // in fact, closed. Instead we accept all faithful reconnection requests, and
        // assume that the client is well-behaved (it will not attempt to reconnect unless
        // it is explicitly disconnected).
        //
        // Even if we could detect closed connections at this point, we'd have an TOCTOU race -
        // it may be the case that a stream closes between the time that we check it is closed,
        // and when we issue an error for the reconnection attempt.

        let buffer_size_hint = match self {
            SupervisorResponder::DirectStream { buffer, .. } => buffer.max_size,
            SupervisorResponder::Disconnected { buffer_size, .. } => *buffer_size,
        };

        let old_conn = {
            let mut new_stream = Self::DirectStream {
                stream: new_conn,
                buffer: BufferedResults::new(buffer_size_hint),
            };
            std::mem::swap(self, &mut new_stream);
            new_stream
        };

        match old_conn {
            SupervisorResponder::Disconnected {
                results,
                pending_start_info,
                ..
            } => {
                if let Some(start_info) = pending_start_info {
                    self.send_run_start_info(start_info).await;
                }

                self.send_test_results(results).await;
            }
            SupervisorResponder::DirectStream { .. } => {
                // nothing more to do
            }
        }
    }
}
