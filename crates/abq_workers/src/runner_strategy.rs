//! How a runner should communicate certain messages or handle results.

use std::{io, net::SocketAddr, time::Duration};

use abq_generic_test_runner::{
    CancelNotifier, GetNextTests, InitContextFetcher, ManifestSender, NotifyCancellation,
};
use abq_utils::{
    net_async,
    net_protocol::{
        self,
        entity::Entity,
        queue::{self, RunAlreadyCompleted},
        workers::{ManifestResult, RunId},
    },
    results_handler::{ResultsHandler, SharedResultsHandler, NoopResultsHandler, NotifyResults},
    retry::async_retry_n,
};
use async_trait::async_trait;
use futures::FutureExt;
use tracing::instrument;

use crate::{
    results_handler::{MultiplexingResultsHandler, QueueResultsSender},
    test_fetching,
    workers::{GetInitContext, InitContextResult, NotifyManifest, NotifyMaterialTestsAllRun},
    AssignedRun,
};

pub struct RunnerStrategy {
    /// If [Some], the runner should generate manifest.
    pub notify_manifest: Option<NotifyManifest>,
    pub get_init_context: GetInitContext,
    pub get_next_tests: GetNextTests,
    pub results_handler: ResultsHandler,
    pub notify_all_tests_run: NotifyMaterialTestsAllRun,
    pub notify_cancellation: NotifyCancellation,
}

pub trait StrategyGenerator: Sync {
    /// Generate a [RunnerStrategy] given the entity of the runner.
    fn generate(&self, runner_entity: Entity, should_generate_manifest: bool) -> RunnerStrategy;
}

pub struct RunnerStrategyGenerator {
    client: Box<dyn net_async::ConfiguredClient>,
    run_id: RunId,
    queue_results_addr: SocketAddr,
    work_server_addr: SocketAddr,
    local_results_handler: SharedResultsHandler,
    max_run_number: u32,
    assigned: AssignedRun,
    should_send_results: bool,
}

impl RunnerStrategyGenerator {
    pub fn new(
        client: Box<dyn net_async::ConfiguredClient>,
        run_id: RunId,
        queue_results_addr: SocketAddr,
        work_server_addr: SocketAddr,
        local_results_handler: SharedResultsHandler,
        max_run_number: u32,
        assigned: AssignedRun,
        should_send_results: bool,
    ) -> Self {
        Self {
            client,
            run_id,
            queue_results_addr,
            work_server_addr,
            local_results_handler,
            max_run_number,
            assigned,
            should_send_results,
        }
    }
}

impl StrategyGenerator for RunnerStrategyGenerator {
    fn generate(&self, runner_entity: Entity, should_generate_manifest: bool) -> RunnerStrategy {
        let Self {
            client,
            run_id,
            queue_results_addr,
            work_server_addr,
            local_results_handler,
            max_run_number,
            assigned,
            should_send_results,
        } = &self;

        let sourcing_strategy = match assigned {
            AssignedRun::Fresh { .. } => test_fetching::SourcingStrategy::Fresh,
            AssignedRun::Retry => test_fetching::SourcingStrategy::Retry,
        };

        let (tests_fetcher, results_retry_tracker) = test_fetching::Fetcher::new(
            sourcing_strategy,
            runner_entity,
            *work_server_addr,
            client.boxed_clone(),
            run_id.clone(),
            *max_run_number,
        );

        let get_init_context: GetInitContext = Box::new({
            GetInitContextOnline {
                run_id: run_id.clone(),
                entity: runner_entity,
                client: client.boxed_clone(),
                work_server_addr: *work_server_addr,
            }
        });

        let notify_manifest: Option<NotifyManifest> = if should_generate_manifest {
            Some(Box::new({
                SendManifestOnline {
                    run_id: run_id.clone(),
                    entity: runner_entity,
                    client: client.boxed_clone(),
                    queue_results_addr: *queue_results_addr,
                }
            }))
        } else {
            None
        };

        let results_handler: ResultsHandler = {
            let queue_handler: Box<dyn NotifyResults + Send> = match should_send_results {
                true => Box::new(QueueResultsSender::new(
                    client.boxed_clone(),
                    *queue_results_addr,
                    runner_entity,
                    run_id.clone(),
                )),
                false => Box::new(NoopResultsHandler)
            };
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

        let notify_cancellation = Box::new(OnlineCancelNotifier {
            client: client.boxed_clone(),
            entity: runner_entity,
            run_id: run_id.clone(),
            queue_addr: *queue_results_addr,
        });

        RunnerStrategy {
            get_next_tests,
            results_handler,
            notify_all_tests_run,
            notify_manifest,
            get_init_context,
            notify_cancellation,
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

struct GetInitContextOnline {
    run_id: RunId,
    entity: Entity,
    client: Box<dyn net_async::ConfiguredClient>,
    work_server_addr: SocketAddr,
}

/// Asks the work server for native runner initialization context.
/// Blocks on the result, repeatedly pinging the server until work is available.
#[async_trait]
impl InitContextFetcher for GetInitContextOnline {
    #[instrument(level = "trace", skip_all, fields(run_id=?self.run_id, work_server=?self.work_server_addr))]
    async fn get_init_context(self: Box<Self>) -> io::Result<InitContextResult> {
        use net_protocol::work_server::{InitContextResponse, Message, Request};

        let Self {
            run_id,
            entity,
            client,
            work_server_addr,
        } = *self;

        // The work server may be waiting for the manifest, which the initialization context is blocked on;
        // to avoid pinging the server too often, let's decay on the frequency of our requests.
        let mut decay = Duration::from_millis(10);
        let max_decay = Duration::from_secs(3);
        loop {
            let mut stream = client.connect(work_server_addr).await?;

            let init_context_request = &Request {
                entity,
                message: Message::InitContext {
                    run_id: run_id.clone(),
                },
            };

            net_protocol::async_write(&mut stream, init_context_request).await?;
            match net_protocol::async_read(&mut stream).await? {
                InitContextResponse::WaitingForManifest => {
                    tokio::time::sleep(decay).await;
                    decay *= 2;
                    if decay >= max_decay {
                        tracing::info!("hit max decay limit for requesting initialization context");
                        decay = max_decay;
                    }
                    continue;
                }
                InitContextResponse::InitContext(init_context) => {
                    return Ok(Ok(init_context));
                }
                InitContextResponse::RunAlreadyCompleted { cancelled } => {
                    return Ok(Err(RunAlreadyCompleted { cancelled }));
                }
            }
        }
    }
}

struct SendManifestOnline {
    run_id: RunId,
    entity: Entity,
    client: Box<dyn net_async::ConfiguredClient>,
    queue_results_addr: SocketAddr,
}

/// Asks the work server for native runner initialization context.
/// Blocks on the result, repeatedly pinging the server until work is available.
#[async_trait]
impl ManifestSender for SendManifestOnline {
    #[instrument(level = "trace", skip_all, fields(run_id=?self.run_id, queue_server=?self.queue_results_addr))]
    async fn send_manifest(self: Box<Self>, manifest_result: ManifestResult) {
        let Self {
            run_id,
            entity,
            client,
            queue_results_addr,
        } = *self;

        let message = net_protocol::queue::Request {
            entity,
            message: net_protocol::queue::Message::ManifestResult(run_id.clone(), manifest_result),
        };

        let mut stream = async_retry_n(5, Duration::from_secs(3), |attempt| {
            if attempt > 1 {
                tracing::info!("reattempting connection to queue for manifest {}", attempt);
            }
            client.connect(queue_results_addr)
        })
        .await
        .expect("results server not available");

        net_protocol::async_write(&mut stream, &message)
            .await
            .unwrap();
        let net_protocol::queue::AckManifest {} =
            net_protocol::async_read(&mut stream).await.unwrap();
    }
}

struct OnlineCancelNotifier {
    client: Box<dyn net_async::ConfiguredClient>,
    entity: Entity,
    run_id: RunId,
    queue_addr: SocketAddr,
}

#[async_trait]
impl CancelNotifier for OnlineCancelNotifier {
    async fn cancel(self: Box<Self>) {
        let Self {
            client,
            entity,
            run_id,
            queue_addr,
        } = *self;

        let mut stream = async_retry_n(5, Duration::from_secs(3), |attempt| {
            if attempt > 1 {
                tracing::info!(
                    "reattempting connection to queue for cancellation {}",
                    attempt
                );
            }
            client.connect(queue_addr)
        })
        .await
        .expect("queue server not available");

        let message = net_protocol::queue::Request {
            entity,
            message: net_protocol::queue::Message::CancelRun(run_id),
        };

        net_protocol::async_write(&mut stream, &message)
            .await
            .unwrap();
        let net_protocol::queue::AckTestCancellation {} =
            net_protocol::async_read(&mut stream).await.unwrap();
    }
}
