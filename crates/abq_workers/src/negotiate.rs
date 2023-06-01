//! Module negotiate helps worker pools attach to queues.

use serde_derive::{Deserialize, Serialize};
use std::{
    error::Error,
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::{error, instrument};

use crate::{
    negotiate,
    runner_strategy::RunnerStrategyGenerator,
    workers::{WorkerContext, WorkerPool, WorkerPoolConfig, WorkersExit, WorkersExitStatus},
    AssignedRun, AssignedRunStatus, GetAssignedRun,
};
use abq_utils::{
    auth::User,
    error::{EntityfulError, ErrorEntity, ResultLocation},
    exit::ExitCode,
    here, log_entityful_error, net, net_async,
    net_opt::ClientOptions,
    net_protocol::{
        self,
        entity::{Entity, WorkerTag},
        meta::DeprecationRecord,
        publicize_addr,
        queue::{InvokeWork, NegotiatorInfo},
        workers::{RunId, RunnerKind},
    },
    results_handler::SharedResultsHandler,
    server_shutdown::ShutdownReceiver,
};

// TODO: send negotiation protocol versions

// Handshake is
//
// -------                              --------------
// Queue |     send Wants-To-Attach     | Workers Negotiator
//       | <--------------------------- |
// Queue |    recv Execution-Context    | Workers Negotiator
//       | ---------------------------> |
//       |                              |
//       | (negotiator closes, workers) |
//       |  (query the queue directly)  |
//       |                              |
//       |                              | ----------
//       |        send Next-Test        |          | loop
// Queue | <--------------------------- | Worker   |
//       |        recv Next-Test        |          |
// Queue | ---------------------------> | Worker   |
//       |        send Test-Result      |          |
// Queue | <--------------------------- | Worker   |
// -------                              ------------
//
// When the queue shuts down or sends `EndOfTests`, the workers shutdown.

#[derive(Serialize, Deserialize)]
struct ExecutionContext {
    /// Where workers should receive messages from.
    work_server_addr: SocketAddr,
    /// Where workers should send results to.
    queue_results_addr: SocketAddr,

    assigned: AssignedRun,
}

#[allow(clippy::large_enum_variant)] // I believe we can drop this after we upgrade to rust 1.65.0
#[derive(Serialize, Deserialize)]
enum MessageFromQueueNegotiator {
    /// The run a worker set is negotiating for has already completed, and the set should
    /// immediately exit.
    RunAlreadyCompleted {
        exit_code: ExitCode,
    },
    /// The context a worker set should execute a run with.
    ExecutionContext(ExecutionContext),
    RunUnknown,
    FatalError(String),
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub entity: Entity,
    pub message: MessageToQueueNegotiator,
}

#[derive(Serialize, Deserialize)]
pub enum MessageToQueueNegotiator {
    HealthCheck,
    WantsToAttach { invoke_data: InvokeWork },
}

pub struct WorkersConfig {
    pub tag: WorkerTag,
    pub num_workers: NonZeroUsize,
    pub runner_kind: RunnerKind,
    /// How should test results be handled, locally?
    /// The handler will be shared between all workers created on the pool.
    pub local_results_handler: SharedResultsHandler,
    /// Context under which workers should operate.
    pub worker_context: WorkerContext,
    pub debug_native_runner: bool,
    pub has_stdout_reporters: bool,
    pub protocol_version_timeout: Duration,
    pub test_timeout: Duration,
    /// Hint for how many test results should be sent back in a batch.
    pub results_batch_size_hint: u64,
    /// Max number of test suite run attempts
    pub max_run_number: u32,
    pub should_send_results: bool,
}

#[derive(Debug, Error)]
pub enum WorkersNegotiateError {
    #[error("could not connect to queue")]
    CouldNotConnect,
    #[error("illegal message received from queue")]
    BadQueueMessage,
    #[error(
        "ABQ failed to create a run: {0}. This is a fatal error in ABQ. Please report it to RWX."
    )]
    FatalErrorCreatingRun(String),
    #[error("{0}")]
    Io(#[from] io::Error),
}

/// The worker pool side of the negotiation.
pub struct WorkersNegotiator(Box<dyn net::ClientStream>, WorkerContext);

pub enum NegotiatedWorkers {
    /// No more workers were created, because there is no more work to be done.
    Redundant { exit_code: ExitCode },
    /// A pool of workers were created.
    Pool(WorkerPool),
}

impl NegotiatedWorkers {
    pub async fn shutdown(&mut self) -> WorkersExit {
        match self {
            &mut NegotiatedWorkers::Redundant { exit_code } => WorkersExit {
                status: WorkersExitStatus::Completed(exit_code),
                manifest_generation_output: None,
                final_stdio_outputs: Default::default(),
                process_outputs: Default::default(),
                native_runner_info: None,
            },
            NegotiatedWorkers::Pool(pool) => pool.shutdown().await,
        }
    }

    pub async fn cancel(&mut self) {
        match self {
            NegotiatedWorkers::Redundant { .. } => {}
            NegotiatedWorkers::Pool(pool) => pool.cancel().await,
        }
    }

    /// Returns a future that resolves when all workers are completed and ready to shut down.
    pub async fn wait(&mut self) {
        match self {
            NegotiatedWorkers::Redundant { .. } => {}
            NegotiatedWorkers::Pool(pool) => pool.wait().await,
        }
    }

    pub fn workers_alive(&self) -> bool {
        match self {
            NegotiatedWorkers::Redundant { .. } => false,
            NegotiatedWorkers::Pool(pool) => pool.workers_alive(),
        }
    }
}

impl WorkersNegotiator {
    #[instrument(level = "trace", skip(workers_config, queue_negotiator_handle, client_options), fields(
        num_workers = workers_config.num_workers
    ))]
    pub async fn negotiate_and_start_pool(
        workers_config: WorkersConfig,
        queue_negotiator_handle: QueueNegotiatorHandle,
        client_options: ClientOptions<User>,
        invoke_data: InvokeWork,
    ) -> Result<NegotiatedWorkers, WorkersNegotiateError> {
        let first_runner_entity = Entity::runner(workers_config.tag, 1);
        let async_client = client_options.build_async()?;

        let run_id = invoke_data.run_id.clone();

        let execution_decision = wait_for_execution_context(
            &*async_client,
            first_runner_entity,
            queue_negotiator_handle.0,
            invoke_data,
        )
        .await?;

        let ExecutionContext {
            work_server_addr,
            queue_results_addr,
            assigned,
        } = match execution_decision {
            Ok(ctx) => ctx,
            Err(redundnant_workers) => return Ok(redundnant_workers),
        };

        tracing::debug!(
            "Received execution message. New work addr: {:?}, results addr: {:?}",
            work_server_addr,
            queue_results_addr
        );

        let WorkersConfig {
            tag,
            num_workers,
            runner_kind,
            local_results_handler,
            worker_context,
            debug_native_runner,
            has_stdout_reporters,
            results_batch_size_hint,
            max_run_number,
            protocol_version_timeout,
            test_timeout,
            should_send_results,
        } = workers_config;

        let some_runner_should_generate_manifest = match assigned {
            AssignedRun::Fresh {
                should_generate_manifest,
            } => should_generate_manifest,
            AssignedRun::Retry => false,
        };

        let runner_strategy_generator = RunnerStrategyGenerator::new(
            async_client.boxed_clone(),
            run_id.clone(),
            queue_results_addr,
            work_server_addr,
            local_results_handler,
            max_run_number,
            assigned,
            should_send_results,
        );

        let pool_config = WorkerPoolConfig {
            size: num_workers,
            some_runner_should_generate_manifest,
            tag,
            first_runner_entity,
            runner_kind,
            runner_strategy_generator: &runner_strategy_generator,
            results_batch_size_hint,
            worker_context,
            run_id,
            debug_native_runner,
            has_stdout_reporters,
            protocol_version_timeout,
            test_timeout,
        };

        tracing::debug!("Starting worker pool");
        let pool = WorkerPool::new(pool_config).await;
        tracing::debug!("Started worker pool");

        Ok(NegotiatedWorkers::Pool(pool))
    }
}

/// Waits to receive worker execution context from a queue negotiator.
/// This call will block on the result, but be composed of only non-blocking calls to the queue
/// negotiator.
/// Turns Ok(Err(redundant)) if this set of workers would be redundant, and
/// Ok(Ok(context)) if instead the set of workers should start with the given context.
#[instrument(level = "debug", skip(client))]
async fn wait_for_execution_context(
    client: &dyn net_async::ConfiguredClient,
    entity: Entity,
    queue_negotiator_addr: SocketAddr,
    invoke_data: InvokeWork,
) -> Result<Result<ExecutionContext, NegotiatedWorkers>, WorkersNegotiateError> {
    let mut decay = Duration::from_millis(10);
    let max_decay = Duration::from_secs(3);
    loop {
        let mut conn = client.connect(queue_negotiator_addr).await?;
        let wants_to_attach = negotiate::Request {
            entity,
            message: MessageToQueueNegotiator::WantsToAttach {
                invoke_data: invoke_data.clone(),
            },
        };
        net_protocol::async_write(&mut conn, &wants_to_attach).await?;

        let worker_set_decision = match net_protocol::async_read(&mut conn).await? {
            MessageFromQueueNegotiator::ExecutionContext(ctx) => Ok(ctx),
            MessageFromQueueNegotiator::RunAlreadyCompleted { exit_code } => {
                Err(NegotiatedWorkers::Redundant { exit_code })
            }
            MessageFromQueueNegotiator::FatalError(err) => {
                return Err(WorkersNegotiateError::FatalErrorCreatingRun(err))
            }
            MessageFromQueueNegotiator::RunUnknown => {
                // We are still waiting for this run, sleep on the decay and retry.
                //
                // TODO: timeout if we go too long without finding the run or its execution context.
                tokio::time::sleep(decay).await;
                decay *= 2;
                if decay >= max_decay {
                    tracing::info!("hit max decay limit for requesting initialization context");
                    decay = max_decay;
                }
                continue;
            }
        };

        return Ok(worker_set_decision);
    }
}

/// The queue side of the negotiation.
pub struct QueueNegotiator {
    addr: SocketAddr,
    listener_handle: Option<JoinHandle<Result<(), QueueNegotiatorServerError>>>,
}

/// Address of a queue negotiator.
#[derive(Clone, Copy)]
pub struct QueueNegotiatorHandle(pub SocketAddr);

#[derive(Debug, Error)]
pub enum QueueNegotiatorHandleError {
    #[error("could not connect to the queue")]
    CouldNotConnect,
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("this abq ({local_version}) is incompatible with the remote queue version ({remote_version})")]
    IncompatibleVersion {
        local_version: &'static str,
        remote_version: String,
    },
}

impl QueueNegotiatorHandle {
    pub fn get_address(&self) -> SocketAddr {
        self.0
    }

    pub fn ask_queue(
        entity: Entity,
        run_id: RunId,
        queue_addr: SocketAddr,
        client_options: ClientOptions<User>,
        deprecations: DeprecationRecord,
    ) -> Result<Self, QueueNegotiatorHandleError> {
        use QueueNegotiatorHandleError::*;

        let client = client_options.build()?;
        let mut conn = client.connect(queue_addr).map_err(|_| CouldNotConnect)?;

        let request = net_protocol::queue::Request {
            entity,
            message: net_protocol::queue::Message::NegotiatorInfo {
                run_id,
                deprecations,
            },
        };

        net_protocol::write(&mut conn, request).map_err(|_| CouldNotConnect)?;
        let NegotiatorInfo {
            negotiator_address,
            version,
        } = net_protocol::read(&mut conn).map_err(|_| CouldNotConnect)?;

        if version != abq_utils::VERSION {
            return Err(QueueNegotiatorHandleError::IncompatibleVersion {
                local_version: abq_utils::VERSION,
                remote_version: version,
            });
        }

        Ok(Self(negotiator_address))
    }
}

#[derive(Debug)]
pub enum QueueNegotiateError {
    CouldNotConnect,
    BadWorkersMessage,
}

/// An error that happens in the construction or execution of the queue negotiation server.
///
/// Does not include errors in the handling of requests to the server, but does include errors in
/// the acception or dispatch of connections.
#[derive(Debug, Error)]
pub enum QueueNegotiatorServerError {
    /// An IO-related error.
    #[error("{0}")]
    Io(#[from] io::Error),

    /// Any other opaque error that occurred.
    #[error("{0}")]
    Other(#[from] Box<dyn Error + Send + Sync>),
}

struct QueueNegotiatorCtx<GA>
where
    GA: GetAssignedRun + Send + Sync,
{
    /// Fetches the status of an assigned run, and yields immediately.
    get_assigned_run: Arc<GA>,
    advertised_queue_work_scheduler_addr: SocketAddr,
    advertised_queue_results_addr: SocketAddr,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
}

impl<GA> QueueNegotiatorCtx<GA>
where
    GA: GetAssignedRun + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            get_assigned_run: Arc::clone(&self.get_assigned_run),
            advertised_queue_work_scheduler_addr: self.advertised_queue_work_scheduler_addr,
            advertised_queue_results_addr: self.advertised_queue_results_addr,
            handshake_ctx: Arc::clone(&self.handshake_ctx),
        }
    }
}

impl QueueNegotiator {
    /// Starts a queue negotiator on an async executor.
    ///
    /// * `get_assigned_run` - should fetch the status of an assigned run and yield immediately.
    pub async fn start<GA>(
        public_ip: IpAddr,
        listener: Box<dyn net_async::ServerListener>,
        mut shutdown_rx: ShutdownReceiver,
        queue_work_scheduler_addr: SocketAddr,
        queue_results_addr: SocketAddr,
        get_assigned_run: GA,
    ) -> Self
    where
        GA: GetAssignedRun + Send + Sync + 'static,
    {
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let advertised_queue_work_scheduler_addr =
                publicize_addr(queue_work_scheduler_addr, public_ip);
            let advertised_queue_results_addr = publicize_addr(queue_results_addr, public_ip);

            tracing::debug!("Starting negotiator on {}", addr);

            // Initialize the listener in the async context.
            let handshake_ctx = listener.handshake_ctx();

            let ctx = QueueNegotiatorCtx {
                get_assigned_run: Arc::new(get_assigned_run),
                advertised_queue_work_scheduler_addr,
                advertised_queue_results_addr,
                handshake_ctx: Arc::new(handshake_ctx),
            };

            loop {
                let (client, _) = tokio::select! {
                    conn = listener.accept() => {
                        match conn {
                            Ok(conn) => conn,
                            Err(e) => {
                                tracing::error!("error accepting connection to negotiator: {:?}", e);
                                continue;
                            }
                        }
                    }
                    _ = shutdown_rx.recv_shutdown_immediately() => {
                        break;
                    }
                };

                tokio::spawn({
                    let ctx = ctx.clone();
                    async move {
                        let result = Self::handle_conn(ctx, client).await;
                        if let Err(error) = result {
                            log_entityful_error!(
                                error,
                                "error handling connection to negotiator: {:?}"
                            );
                        }
                    }
                });
            }

            Ok(())
        });

        Self {
            addr,
            listener_handle: Some(handle),
        }
    }

    async fn handle_conn<GA>(
        ctx: QueueNegotiatorCtx<GA>,
        stream: net_async::UnverifiedServerStream,
    ) -> Result<(), EntityfulError>
    where
        GA: GetAssignedRun + Send + Sync + 'static,
    {
        let mut stream = ctx
            .handshake_ctx
            .handshake(stream)
            .await
            .located(here!())
            .no_entity()?;
        let Request { entity, message } = net_protocol::async_read(&mut stream)
            .await
            .located(here!())
            .no_entity()?;

        use MessageToQueueNegotiator::*;
        let result = match message {
            HealthCheck => {
                let write_result =
                    net_protocol::async_write(&mut stream, &net_protocol::health::healthy())
                        .await
                        .located(here!());
                if let Err(err) = &write_result {
                    tracing::warn!("error sending health check: {}", err.to_string());
                }
                write_result
            }
            WantsToAttach { invoke_data } => {
                let run_id = &invoke_data.run_id;

                tracing::debug!(run_id=?run_id, "New worker set negotiating");

                let assigned_run_result = ctx
                    .get_assigned_run
                    .get_assigned_run(entity, &invoke_data)
                    .await;

                use AssignedRunStatus::*;
                let msg = match assigned_run_result {
                    Run(assigned) => {
                        tracing::debug!(?run_id, "found run for worker set");

                        MessageFromQueueNegotiator::ExecutionContext(ExecutionContext {
                            work_server_addr: ctx.advertised_queue_work_scheduler_addr,
                            queue_results_addr: ctx.advertised_queue_results_addr,
                            assigned,
                        })
                    }
                    AlreadyDone { exit_code } => {
                        tracing::debug!(?run_id, "run already completed");
                        MessageFromQueueNegotiator::RunAlreadyCompleted { exit_code }
                    }
                    RunUnknown => {
                        tracing::debug!(?run_id, "run not yet known");
                        MessageFromQueueNegotiator::RunUnknown
                    }
                    FatalError(error) => {
                        tracing::error!(?run_id, "fatal error trying to fetch run");
                        MessageFromQueueNegotiator::FatalError(error)
                    }
                };

                net_protocol::async_write(&mut stream, &msg)
                    .await
                    .located(here!())
            }
        };

        result.entity(entity)
    }

    pub fn get_handle(&self) -> QueueNegotiatorHandle {
        QueueNegotiatorHandle(self.addr)
    }

    pub async fn join(&mut self) {
        self.listener_handle.take().unwrap().await.unwrap().unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{MessageToQueueNegotiator, QueueNegotiator, WorkersNegotiator};
    use crate::assigned_run::fake::MockGetAssignedRun;
    use crate::negotiate::{AssignedRunStatus, WorkersConfig};
    use crate::workers::{WorkerContext, WorkersExitStatus};
    use crate::{AssignedRun, DEFAULT_RUNNER_TEST_TIMEOUT};
    use abq_generic_test_runner::DEFAULT_PROTOCOL_VERSION_TIMEOUT;
    use abq_test_utils::one_nonzero;
    use abq_utils::auth::{
        build_strategies, Admin, AdminToken, ClientAuthStrategy, ServerAuthStrategy, User,
        UserToken,
    };
    use abq_utils::exit::ExitCode;
    use abq_utils::net_opt::{ClientOptions, ServerOptions};
    use abq_utils::net_protocol::entity::{Entity, WorkerTag};
    use abq_utils::net_protocol::queue::{InvokeWork, TestSpec, TestStrategy};
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, ProtocolWitness, Status, Test, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::work_server::{
        self, InitContext, InitContextResponse, NextTestRequest, NextTestResponse,
    };
    use abq_utils::net_protocol::workers::{
        Eow, ManifestResult, NextWorkBundle, ReportedManifest, RunId, RunnerKind, TestLikeRunner,
        WorkId, WorkerTest, INIT_RUN_NUMBER,
    };
    use abq_utils::results_handler::NoopResultsHandler;
    use abq_utils::server_shutdown::ShutdownManager;
    use abq_utils::tls::{ClientTlsStrategy, ServerTlsStrategy};
    use abq_utils::{net_async, net_protocol};
    use abq_with_protocol_version::with_protocol_version;
    use parking_lot::Mutex;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tracing_test::internal::logs_with_scope_contain;
    use tracing_test::traced_test;

    pub fn build_random_strategies() -> (
        ServerAuthStrategy,
        ClientAuthStrategy<User>,
        ClientAuthStrategy<Admin>,
    ) {
        build_strategies(UserToken::new_random(), AdminToken::new_random())
    }

    type Messages = Arc<Mutex<Vec<TestResult>>>;
    type ManifestCollector = Arc<Mutex<Option<ReportedManifest>>>;

    type QueueNextWork = (SocketAddr, mpsc::Sender<()>, JoinHandle<()>);
    type QueueResults = (Messages, SocketAddr, mpsc::Sender<()>, JoinHandle<()>);

    async fn mock_queue_next_work_server(manifest_collector: ManifestCollector) -> QueueNextWork {
        let server = ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
            .bind_async("0.0.0.0:0")
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(2);

        let handle = tokio::spawn(async move {
            let mut work_to_write = loop {
                let manifest_result = { manifest_collector.lock().take() };
                match manifest_result {
                    Some(man) => {
                        let work: Vec<_> = man
                            .manifest
                            .flatten()
                            .0
                            .into_iter()
                            .enumerate()
                            .map(|(i, (spec, ..))| WorkerTest {
                                spec: TestSpec {
                                    test_case: spec.test_case,
                                    work_id: WorkId([i as _; 16]),
                                },
                                run_number: INIT_RUN_NUMBER,
                            })
                            .collect();
                        break work;
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            };

            work_to_write.reverse();
            let mut recv_init_context = false;
            loop {
                tokio::select! {
                    conn = server.accept() => {
                        let (conn, _) = conn.unwrap();
                        let mut worker = server.handshake_ctx().handshake(conn).await.unwrap();

                        if !recv_init_context {
                            recv_init_context = true;
                            net_protocol::async_write(
                                &mut worker,
                                &InitContextResponse::InitContext(InitContext {
                                    init_meta: Default::default(),
                                }),
                            )
                            .await
                            .unwrap();
                        } else {
                            let _connect_msg: work_server::Request =
                                net_protocol::async_read(&mut worker).await.unwrap();
                            let mut all_done = false;
                            while !all_done {
                                let work = work_to_write.pop().map(|work| vec![work]).unwrap_or_default();
                                all_done = work.is_empty();
                                let work_bundle = NextWorkBundle::new(work, Eow(all_done));

                                let NextTestRequest {} =
                                    match net_protocol::async_read(&mut worker).await {
                                        Ok(r) => r,
                                        _ => break, // worker disconnected
                                    };
                                let response = NextTestResponse::Bundle(work_bundle);
                                net_protocol::async_write(&mut worker, &response)
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        return
                    }
                }
            }
        });

        (server_addr, shutdown_tx, handle)
    }

    async fn mock_queue_results_server(manifest_collector: ManifestCollector) -> QueueResults {
        let msgs = Arc::new(Mutex::new(Vec::new()));
        let msgs2 = Arc::clone(&msgs);

        let server = ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
            .bind_async("0.0.0.0:0")
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(2);

        let handle = tokio::spawn(async move {
            loop {
                let (conn, _) = tokio::select! {
                    conn = server.accept() => {
                        conn.unwrap()
                    }
                    _ = shutdown_rx.recv() => {
                        return;
                    }
                };

                let mut client = server.handshake_ctx().handshake(conn).await.unwrap();

                let request: net_protocol::queue::Request =
                    net_protocol::async_read(&mut client).await.unwrap();
                match request.message {
                    net_protocol::queue::Message::WorkerResult(_, results) => {
                        net_protocol::async_write(
                            &mut client,
                            &net_protocol::queue::AckTestResults {},
                        )
                        .await
                        .unwrap();
                        for tr in results {
                            msgs2.lock().extend(tr.results);
                        }
                    }
                    net_protocol::queue::Message::ManifestResult(_, manifest_result) => {
                        let manifest = match manifest_result {
                            ManifestResult::Manifest(man) => man,
                            _ => unreachable!(),
                        };
                        let old_manifest = manifest_collector.lock().replace(manifest);
                        debug_assert!(
                            old_manifest.is_none(),
                            "replacing existing manifest! This is a bug in our tests."
                        );
                        net_protocol::async_write(
                            &mut client,
                            &net_protocol::queue::AckManifest {},
                        )
                        .await
                        .unwrap();
                    }
                    net_protocol::queue::Message::RunStatus(_) => {
                        net_protocol::async_write(
                            &mut client,
                            &net_protocol::queue::RunStatus::InitialManifestDone {
                                num_active_workers: 0,
                            },
                        )
                        .await
                        .unwrap();
                    }
                    net_protocol::queue::Message::WorkerRanAllTests(_) => {
                        net_protocol::async_write(
                            &mut client,
                            &net_protocol::queue::AckWorkerRanAllTests {},
                        )
                        .await
                        .unwrap();
                    }
                    _ => unreachable!(),
                }
            }
        });

        (msgs, server_addr, shutdown_tx, handle)
    }

    async fn close_queue_servers(
        shutdown_next_work_server: mpsc::Sender<()>,
        next_work_handle: JoinHandle<()>,
        shutdown_results_server: mpsc::Sender<()>,
        results_handle: JoinHandle<()>,
    ) {
        let (r1, r2, r3, r4) = tokio::join!(
            shutdown_next_work_server.send(()),
            next_work_handle,
            shutdown_results_server.send(()),
            results_handle
        );
        r1.unwrap();
        r2.unwrap();
        r3.unwrap();
        r4.unwrap();
    }

    async fn await_messages<F>(msgs: Messages, timeout: Duration, predicate: F)
    where
        F: Fn(&Messages) -> bool,
    {
        let duration = Instant::now();

        while !predicate(&msgs) {
            if duration.elapsed() >= timeout {
                panic!(
                    "Failed to match the predicate within the timeout. Current messages: {:?}",
                    msgs
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn echo_test(protocol: ProtocolWitness, echo_msg: String) -> TestOrGroup {
        TestOrGroup::test(Test::new(protocol, echo_msg, [], Default::default()))
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn queue_and_workers_lifecycle() {
        let manifest_collector = ManifestCollector::default();
        let (next_work_addr, shutdown_next_work_server, next_work_handle) =
            mock_queue_next_work_server(Arc::clone(&manifest_collector)).await;
        let (msgs, results_addr, shutdown_results_server, results_handle) =
            mock_queue_results_server(manifest_collector).await;

        let run_id = RunId::unique();

        let manifest = ManifestMessage::new(Manifest::new(
            [echo_test(proto, "hello".to_string())],
            Default::default(),
        ));

        let get_assigned_run = MockGetAssignedRun::new(|| {
            AssignedRunStatus::Run(AssignedRun::Fresh {
                should_generate_manifest: true,
            })
        });

        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let listener =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
                .bind_async("0.0.0.0:0")
                .await
                .unwrap();
        let mut queue_negotiator = QueueNegotiator::start(
            "0.0.0.0".parse().unwrap(),
            listener,
            shutdown_rx,
            next_work_addr,
            results_addr,
            get_assigned_run,
        )
        .await;
        let workers_config = WorkersConfig {
            tag: WorkerTag::new(0),
            num_workers: NonZeroUsize::new(1).unwrap(),
            runner_kind: RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest)),
            local_results_handler: Box::new(NoopResultsHandler),
            worker_context: WorkerContext::AssumeLocal,
            debug_native_runner: false,
            has_stdout_reporters: false,
            results_batch_size_hint: 1,
            max_run_number: INIT_RUN_NUMBER,
            protocol_version_timeout: DEFAULT_PROTOCOL_VERSION_TIMEOUT,
            test_timeout: DEFAULT_RUNNER_TEST_TIMEOUT,
            should_send_results: true,
        };

        let invoke_data = InvokeWork {
            run_id,
            batch_size_hint: one_nonzero(),
            test_strategy: TestStrategy::ByTest,
        };

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue_negotiator.get_handle(),
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls()),
            invoke_data,
        )
        .await
        .unwrap();

        await_messages(msgs, Duration::from_secs(1), |msgs| {
            let msgs = msgs.lock();
            if msgs.len() != 1 {
                return false;
            }

            let result = msgs.last().unwrap();
            result.status == Status::Success && result.output.as_ref().unwrap() == "hello"
        })
        .await;

        workers.wait().await;
        let workers_exit = workers.shutdown().await;
        assert!(matches!(
            workers_exit.status,
            WorkersExitStatus::Completed(ExitCode::SUCCESS)
        ));

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join().await;

        close_queue_servers(
            shutdown_next_work_server,
            next_work_handle,
            shutdown_results_server,
            results_handle,
        )
        .await;
    }

    async fn test_negotiator(
        server: Box<dyn net_async::ServerListener>,
    ) -> (QueueNegotiator, ShutdownManager) {
        let (shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();

        let negotiator = QueueNegotiator::start(
            "0.0.0.0".parse().unwrap(),
            server,
            shutdown_rx,
            // Below parameters are faux because they are unnecessary for healthchecks.
            "0.0.0.0:0".parse().unwrap(),
            "0.0.0.0:0".parse().unwrap(),
            MockGetAssignedRun::new(|| {
                AssignedRunStatus::Run(AssignedRun::Fresh {
                    should_generate_manifest: true,
                })
            }),
        )
        .await;

        (negotiator, shutdown_tx)
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn queue_negotiator_healthcheck() {
        let server = ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
            .bind_async("0.0.0.0:0")
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut queue_negotiator, mut shutdown_tx) = test_negotiator(server).await;

        let client = ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls())
            .build_async()
            .unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        net_protocol::async_write(
            &mut conn,
            &super::Request {
                entity: Entity::local_client(),
                message: MessageToQueueNegotiator::HealthCheck,
            },
        )
        .await
        .unwrap();
        let health_msg: net_protocol::health::Health =
            net_protocol::async_read(&mut conn).await.unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join().await;
    }

    #[tokio::test]
    #[traced_test]
    #[with_protocol_version]
    async fn queue_negotiator_with_auth_okay() {
        let (server_auth, client_auth, _) = build_random_strategies();

        let server = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls())
            .bind_async("0.0.0.0:0")
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut queue_negotiator, mut shutdown_tx) = test_negotiator(server).await;

        let client = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls())
            .build_async()
            .unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        net_protocol::async_write(
            &mut conn,
            &super::Request {
                entity: Entity::local_client(),
                message: MessageToQueueNegotiator::HealthCheck,
            },
        )
        .await
        .unwrap();
        let health_msg: net_protocol::health::Health =
            net_protocol::async_read(&mut conn).await.unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join().await;

        logs_with_scope_contain("", "error handling connection");
    }

    #[tokio::test]
    #[traced_test]
    #[with_protocol_version]
    async fn queue_negotiator_connect_with_no_auth_fails() {
        let (server_auth, _, _) = build_random_strategies();

        let server = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls())
            .bind_async("0.0.0.0:0")
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut queue_negotiator, mut shutdown_tx) = test_negotiator(server).await;

        let client = ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls())
            .build_async()
            .unwrap();
        let mut conn = client.connect(server_addr).await.unwrap();
        net_protocol::async_write(&mut conn, &MessageToQueueNegotiator::HealthCheck)
            .await
            .unwrap();

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join().await;

        logs_with_scope_contain("", "error handling connection");
    }
}
