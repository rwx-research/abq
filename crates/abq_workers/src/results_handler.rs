//! Interface for notifying on results.

use std::{net::SocketAddr, time::Duration};

use abq_utils::{
    error::ResultLocation,
    here, net_async,
    net_protocol::{self, entity::Entity, queue::AssociatedTestResults, workers::RunId},
    results_handler::{NotifyResults, ResultsHandler, SharedResultsHandler},
    retry::async_retry_n,
};
use async_trait::async_trait;
use tracing::instrument;

/// A way to get a new [NotifyResults] value for a unique test run.
pub type ResultsHandlerGenerator<'a> = &'a (dyn Fn(Entity) -> ResultsHandler + Send + Sync);

/// A results handler that dispatches results to the remote queue, and also any local handlers.
pub(crate) struct MultiplexingResultsHandler {
    remote_handler: QueueResultsSender,
    local_handler: SharedResultsHandler,
}

impl MultiplexingResultsHandler {
    pub fn new(remote_handler: QueueResultsSender, local_handler: SharedResultsHandler) -> Self {
        Self {
            remote_handler,
            local_handler,
        }
    }
}

#[async_trait]
impl NotifyResults for MultiplexingResultsHandler {
    #[instrument(level="trace", skip_all, fields(results=results.len()))]
    async fn send_results(&mut self, results: Vec<AssociatedTestResults>) {
        tokio::join!(
            self.remote_handler.send_results(results.clone()),
            self.local_handler.send_results(results)
        );
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
        let request = net_protocol::queue::Request {
            entity: self.entity,
            message: net_protocol::queue::Message::WorkerResult(self.run_id.clone(), results),
        };

        let client = &self.client;
        let queue_results_addr = self.queue_results_addr;

        let mut stream = async_retry_n(5, Duration::from_secs(3), |attempt| async move {
            if attempt > 1 {
                tracing::info!("reattempting connection to queue for results {}", attempt);
            }
            client.connect(queue_results_addr).await
        })
        .await
        .located(here!())
        .expect("failed to connected after 5 attempts");

        net_protocol::async_write(&mut stream, &request)
            .await
            .located(here!())
            .expect("failed to write results after connection");

        let net_protocol::queue::AckTestResults {} = net_protocol::async_read(&mut stream)
            .await
            .located(here!())
            .expect("failed to read results ACK after connection");
    }
}