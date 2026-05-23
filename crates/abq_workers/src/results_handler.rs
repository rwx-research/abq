//! Interface for notifying on results.

use std::{net::SocketAddr, time::Duration};

use abq_utils::{
    error::ResultLocation,
    here, net_async,
    net_protocol::{self, entity::Entity, queue::AssociatedTestResults, workers::RunId},
    results_handler::{NotifyResults, SharedResultsHandler},
    retry::async_retry_n,
    slow_log::log_if_slow,
};
use async_trait::async_trait;
use tracing::instrument;

use crate::test_fetching;

/// A results handler that dispatches results to the remote queue, and also any local handlers.
pub(crate) struct MultiplexingResultsHandler {
    remote_handler: Option<QueueResultsSender>,
    local_handler: SharedResultsHandler,
    results_retry_tracker: test_fetching::ResultsTracker,
}

impl MultiplexingResultsHandler {
    pub fn new(
        remote_handler: Option<QueueResultsSender>,
        local_handler: SharedResultsHandler,
        results_retry_tracker: test_fetching::ResultsTracker,
    ) -> Self {
        Self {
            remote_handler,
            local_handler,
            results_retry_tracker,
        }
    }
}

#[async_trait]
impl NotifyResults for MultiplexingResultsHandler {
    #[instrument(level="trace", skip_all, fields(results=results.len()))]
    async fn send_results(&mut self, results: Vec<AssociatedTestResults>) {
        self.results_retry_tracker.account_results(results.iter());
        if let Some(remote_handler) = self.remote_handler.as_mut() {
            let ((), ()) = tokio::join!(
                remote_handler.send_results(results.clone()),
                self.local_handler.send_results(results)
            );
        } else {
            self.local_handler.send_results(results).await;
        }
    }
}

/// Send results to the queue.
pub(crate) struct QueueResultsSender {
    /// whoami?
    entity: Entity,
    run_id: RunId,
    client: Box<dyn net_async::ConfiguredClient>,
    queue_results_addr: SocketAddr,
}

impl QueueResultsSender {
    pub fn new(
        client: Box<dyn net_async::ConfiguredClient>,
        queue_results_addr: SocketAddr,
        entity: Entity,
        run_id: RunId,
    ) -> Self {
        Self {
            entity,
            run_id,
            client,
            queue_results_addr,
        }
    }
}

#[async_trait]
impl NotifyResults for QueueResultsSender {
    async fn send_results(&mut self, results: Vec<AssociatedTestResults>) {
        let entity = self.entity;
        let run_id = self.run_id.clone();
        let queue_results_addr = self.queue_results_addr;
        let client = &self.client;

        let request = net_protocol::queue::Request {
            entity,
            message: net_protocol::queue::Message::WorkerResult(self.run_id.clone(), results),
        };

        // Wrap each network phase in `log_if_slow` so that if any one of them hangs (connect,
        // write, or ACK read) we get a warn-level breadcrumb tagged with the runner entity
        // identifying *which* runner is stuck and at *which* phase.
        let mut stream = log_if_slow(
            "QueueResultsSender::connect",
            Duration::from_secs(30),
            async_retry_n(5, Duration::from_secs(3), |attempt| async move {
                if attempt > 1 {
                    tracing::info!(
                        ?entity,
                        "reattempting connection to queue for results {}",
                        attempt
                    );
                }
                client.connect(queue_results_addr).await
            }),
        )
        .await
        .located(here!())
        .unwrap_or_else(|e| {
            panic!(
                "failed to connect after 5 attempts (entity={entity:?} run_id={run_id:?}): {e:?}"
            )
        });

        log_if_slow(
            "QueueResultsSender::write",
            Duration::from_secs(30),
            net_protocol::async_write(&mut stream, &request),
        )
        .await
        .located(here!())
        .unwrap_or_else(|e| panic!("failed to write results after connection (entity={entity:?} run_id={run_id:?}): {e:?}"));

        let net_protocol::queue::AckTestResults {} = log_if_slow(
            "QueueResultsSender::ack_read",
            Duration::from_secs(30),
            net_protocol::async_read(&mut stream),
        )
        .await
        .located(here!())
        .unwrap_or_else(|e| panic!("failed to read results ACK after connection (entity={entity:?} run_id={run_id:?}): {e:?}"));
    }
}
