//! How a runner should communicate certain messages or handle results.

use std::{net::SocketAddr, time::Duration};

use abq_generic_test_runner::GetNextTests;
use abq_utils::{
    net_async,
    net_protocol::{self, entity::Entity, queue, workers::RunId},
    results_handler::{ResultsHandler, SharedResultsHandler},
    retry::async_retry_n,
};
use futures::FutureExt;

use crate::{
    results_handler::{MultiplexingResultsHandler, QueueResultsSender},
    test_fetching,
    workers::NotifyMaterialTestsAllRun,
};

pub struct RunnerStrategy {
    pub get_next_tests: GetNextTests,
    pub results_handler: ResultsHandler,
    pub notify_all_tests_run: NotifyMaterialTestsAllRun,
}

pub trait StrategyGenerator: Sync {
    /// Generate a [RunnerStrategy] given the entity of the runner.
    fn generate(&self, runner_entity: Entity) -> RunnerStrategy;
}

pub struct RunnerStrategyGenerator {
    client: Box<dyn net_async::ConfiguredClient>,
    run_id: RunId,
    queue_results_addr: SocketAddr,
    work_server_addr: SocketAddr,
    local_results_handler: SharedResultsHandler,
    max_run_number: u32,
}

impl RunnerStrategyGenerator {
    pub fn new(
        client: Box<dyn net_async::ConfiguredClient>,
        run_id: RunId,
        queue_results_addr: SocketAddr,
        work_server_addr: SocketAddr,
        local_results_handler: SharedResultsHandler,
        max_run_number: u32,
    ) -> Self {
        Self {
            client,
            run_id,
            queue_results_addr,
            work_server_addr,
            local_results_handler,
            max_run_number,
        }
    }
}

impl StrategyGenerator for RunnerStrategyGenerator {
    fn generate(&self, runner_entity: Entity) -> RunnerStrategy {
        let Self {
            client,
            run_id,
            queue_results_addr,
            work_server_addr,
            local_results_handler,
            max_run_number,
        } = &self;

        let (tests_fetcher, results_retry_tracker) = test_fetching::Fetcher::new(
            runner_entity,
            *work_server_addr,
            client.boxed_clone(),
            run_id.clone(),
            *max_run_number,
        );

        let results_handler: ResultsHandler = {
            let queue_handler = QueueResultsSender::new(
                client.boxed_clone(),
                *queue_results_addr,
                runner_entity,
                run_id.clone(),
            );
            let notifier = MultiplexingResultsHandler::new(
                queue_handler,
                local_results_handler.boxed_clone(),
                results_retry_tracker,
            );

            Box::new(notifier)
        };

        let get_next_tests: GetNextTests = Box::new(tests_fetcher);

        let notify_all_tests_run: NotifyMaterialTestsAllRun = {
            let run_id = run_id.clone();
            let async_client = client.boxed_clone();
            let queue_results_addr = *queue_results_addr;

            Box::new(move || {
                notify_all_tests_run_impl(runner_entity, run_id, async_client, queue_results_addr)
                    .boxed()
            })
        };

        RunnerStrategy {
            get_next_tests,
            results_handler,
            notify_all_tests_run,
        }
    }
}

async fn notify_all_tests_run_impl(
    entity: Entity,
    run_id: RunId,
    client: Box<dyn net_async::ConfiguredClient>,
    queue_addr: SocketAddr,
) {
    let client = &*client;
    let mut stream = async_retry_n(5, Duration::from_secs(3), |attempt| async move {
        if attempt > 1 {
            tracing::info!("reattempting connection to queue server {}", attempt);
        }
        client.connect(queue_addr).await
    })
    .await
    .expect("results server not available");

    let message = queue::Request {
        entity,
        message: queue::Message::WorkerRanAllTests(run_id),
    };

    net_protocol::async_write(&mut stream, &message)
        .await
        .unwrap();
    let queue::AckWorkerRanAllTests {} = net_protocol::async_read(&mut stream).await.unwrap();
}
