//! Module negotiate helps worker pools attach to queues.

use futures::FutureExt;
use serde_derive::{Deserialize, Serialize};
use std::{
    error::Error,
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    thread,
    time::Duration,
};
use thiserror::Error;
use tracing::{error, instrument};

use crate::{
    negotiate, persistent_test_fetcher,
    results_handler::{MultiplexingResultsHandler, QueueResultsSender, ResultsHandlerGenerator},
    workers::{
        GetInitContext, GetNextTestsGenerator, InitContextResult, NotifyManifest,
        NotifyMaterialTestsAllRun, NotifyMaterialTestsAllRunGenerator, RunExitCode, WorkerContext,
        WorkerPool, WorkerPoolConfig, WorkersExit, WorkersExitStatus,
    },
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
        queue::{NegotiatorInfo, RunAlreadyCompleted},
        workers::{RunId, RunnerKind},
    },
    results_handler::SharedResultsHandler,
    retry::{async_retry_n, retry_n},
    shutdown::ShutdownReceiver,
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
    /// Whether the queue wants a worker to generate the work manifest.
    worker_should_generate_manifest: bool,
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
    RunCompleteButExitCodeNotKnown,
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub entity: Entity,
    pub message: MessageToQueueNegotiator,
}

#[derive(Serialize, Deserialize)]
pub enum MessageToQueueNegotiator {
    HealthCheck,
    WantsToAttach {
        /// The run the worker wants to run tests for. The queue must respect assignment of
        /// work related to this run, or return an error.
        run_id: RunId,
    },
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
    pub supervisor_in_band: bool,
    pub debug_native_runner: bool,
    /// Hint for how many test results should be sent back in a batch.
    pub results_batch_size_hint: u64,
}

#[derive(Debug, Error)]
pub enum WorkersNegotiateError {
    #[error("could not connect to queue")]
    CouldNotConnect,
    #[error("illegal message received from queue")]
    BadQueueMessage,
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
    pub fn shutdown(&mut self) -> WorkersExit {
        match self {
            &mut NegotiatedWorkers::Redundant { exit_code, .. } => {
                let status = if exit_code == ExitCode::SUCCESS {
                    WorkersExitStatus::Success
                } else {
                    WorkersExitStatus::Failure { exit_code }
                };
                WorkersExit {
                    status,
                    manifest_generation_output: None,
                    final_captured_outputs: Default::default(),
                }
            }
            NegotiatedWorkers::Pool(pool) => pool.shutdown(),
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
    /// Runs the workers-side of the negotiation, returning the configured worker pool once
    /// negotiation is complete.
    #[instrument(level = "trace", skip(workers_config, queue_negotiator_handle, client_options), fields(
        num_workers = workers_config.num_workers
    ))]
    pub fn negotiate_and_start_pool(
        workers_config: WorkersConfig,
        queue_negotiator_handle: QueueNegotiatorHandle,
        client_options: ClientOptions<User>,
        run_id: RunId,
    ) -> Result<NegotiatedWorkers, WorkersNegotiateError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(Self::negotiate_and_start_pool_on_executor(
            workers_config,
            queue_negotiator_handle,
            client_options,
            run_id,
        ))
    }

    #[instrument(level = "trace", skip(workers_config, queue_negotiator_handle, client_options), fields(
        num_workers = workers_config.num_workers
    ))]
    pub async fn negotiate_and_start_pool_on_executor(
        workers_config: WorkersConfig,
        queue_negotiator_handle: QueueNegotiatorHandle,
        client_options: ClientOptions<User>,
        run_id: RunId,
    ) -> Result<NegotiatedWorkers, WorkersNegotiateError> {
        let first_runner_entity = Entity::runner(workers_config.tag, 1);
        let client = client_options.clone().build()?;
        let async_client = client_options.build_async()?;

        let execution_decision = wait_for_execution_context(
            &*async_client,
            first_runner_entity,
            &run_id,
            queue_negotiator_handle.0,
        )
        .await?;

        let ExecutionContext {
            work_server_addr,
            queue_results_addr,
            worker_should_generate_manifest,
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
            supervisor_in_band,
            debug_native_runner,
            results_batch_size_hint,
        } = workers_config;

        let results_handler_generator: ResultsHandlerGenerator = {
            let client = async_client.boxed_clone();
            let run_id = run_id.clone();
            let local_results_handler = local_results_handler.boxed_clone();
            &move |entity| {
                let queue_handler = QueueResultsSender::new(
                    client.boxed_clone(),
                    queue_results_addr,
                    entity,
                    run_id.clone(),
                );
                let notifier = MultiplexingResultsHandler::new(
                    queue_handler,
                    local_results_handler.boxed_clone(),
                );
                Box::new(notifier)
            }
        };

        let get_init_context: GetInitContext = Arc::new({
            let client = client.boxed_clone();
            let run_id = run_id.clone();

            move |entity| {
                let span = tracing::trace_span!("get_init_context", run_id=?run_id, new_work_server=?work_server_addr);
                let _get_next_work = span.enter();

                wait_for_init_context(entity, &*client, work_server_addr, run_id.clone())
            }
        });

        // A function to generate a [GetNextTests] closure.
        let get_next_tests_generator: GetNextTestsGenerator = {
            let async_client = async_client.boxed_clone();
            let run_id = run_id.clone();
            &move |entity| {
                persistent_test_fetcher::start(
                    entity,
                    work_server_addr,
                    async_client.boxed_clone(),
                    run_id.clone(),
                )
            }
        };

        let notify_manifest: Option<NotifyManifest> = if worker_should_generate_manifest {
            Some(Box::new({
                let client = client.boxed_clone();

                move |entity, run_id, manifest_result| {
                    let span = tracing::trace_span!("notify_manifest", ?entity, run_id=?run_id, queue_server=?queue_results_addr);
                    let _notify_manifest = span.enter();

                    let message = net_protocol::queue::Request {
                        entity,
                        message: net_protocol::queue::Message::ManifestResult(
                            run_id.clone(),
                            manifest_result,
                        ),
                    };

                    let mut stream = retry_n(5, Duration::from_secs(3), |attempt| {
                        if attempt > 1 {
                            tracing::info!(
                                "reattempting connection to queue for manifest {}",
                                attempt
                            );
                        }
                        client.connect(queue_results_addr)
                    })
                    .expect("results server not available");

                    net_protocol::write(&mut stream, &message).unwrap();
                    let net_protocol::queue::AckManifest {} =
                        net_protocol::read(&mut stream).unwrap();
                }
            }))
        } else {
            None
        };

        let notify_all_tests_run_generator: NotifyMaterialTestsAllRunGenerator = {
            let run_id = run_id.clone();
            &move || {
                let async_client = async_client.boxed_clone();
                let run_id = run_id.clone();
                let notifier: NotifyMaterialTestsAllRun = Box::new(move |entity| {
                    async move {
                        let async_client_ref = &async_client;
                        let mut stream =
                            async_retry_n(5, Duration::from_secs(3), |attempt| async move {
                                if attempt > 1 {
                                    tracing::info!(
                                        "reattempting connection to results server {}",
                                        attempt
                                    );
                                }
                                async_client_ref.connect(queue_results_addr).await
                            })
                            .await
                            .expect("results server not available");

                        let message = net_protocol::queue::Request {
                            entity,
                            message: net_protocol::queue::Message::WorkerRanAllTests(run_id),
                        };

                        net_protocol::async_write(&mut stream, &message)
                            .await
                            .unwrap();
                        let net_protocol::queue::AckWorkerRanAllTests {} =
                            net_protocol::async_read(&mut stream).await.unwrap();
                    }
                    .boxed()
                });

                notifier
            }
        };

        let run_exit_code: RunExitCode = Arc::new({
            // When our worker finishes and polls the queue for the final status of the run,
            // there still may be active work. So for now, let's poll every second; we can make
            // the strategy here more sophisticated in the future, e.g. with exponential backoffs.
            const BACKOFF: Duration = Duration::from_secs(1);
            let run_id = run_id.clone();

            move |entity| {
                let span = tracing::trace_span!("run_completed_successfully", run_id=?run_id, queue_addr=?queue_results_addr);
                let _run_completed_successfully = span.enter();

                use net_protocol::queue::{Message, Request, TotalRunResult};
                loop {
                    let mut stream = retry_n(5, Duration::from_secs(3), |attempt| {
                        if attempt > 1 {
                            tracing::info!("reattempting connection to work server {}", attempt);
                        }
                        client.connect(queue_results_addr)
                    })
                    .expect("work server not available");

                    let request = Request {
                        entity,
                        message: Message::RequestTotalRunResult(run_id.clone()),
                    };
                    net_protocol::write(&mut stream, request).unwrap();
                    let run_result = net_protocol::read(&mut stream).unwrap();

                    match run_result {
                        TotalRunResult::Pending => {
                            std::thread::sleep(BACKOFF);
                            continue;
                        }
                        TotalRunResult::Completed { exit_code } => return exit_code,
                    }
                }
            }
        });

        let pool_config = WorkerPoolConfig {
            size: num_workers,
            tag,
            first_runner_entity,
            runner_kind,
            get_next_tests_generator,
            get_init_context,
            results_handler_generator,
            notify_all_tests_run_generator,
            run_exit_code,
            results_batch_size_hint,
            worker_context,
            run_id,
            notify_manifest,
            supervisor_in_band,
            debug_native_runner,
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
    run_id: &RunId,
    queue_negotiator_addr: SocketAddr,
) -> Result<Result<ExecutionContext, NegotiatedWorkers>, WorkersNegotiateError> {
    let mut decay = Duration::from_millis(10);
    let max_decay = Duration::from_secs(3);
    loop {
        let mut conn = client.connect(queue_negotiator_addr).await?;
        let wants_to_attach = negotiate::Request {
            entity,
            message: MessageToQueueNegotiator::WantsToAttach {
                run_id: run_id.clone(),
            },
        };
        net_protocol::async_write(&mut conn, &wants_to_attach).await?;

        let worker_set_decision = match net_protocol::async_read(&mut conn).await? {
            MessageFromQueueNegotiator::ExecutionContext(ctx) => Ok(ctx),
            MessageFromQueueNegotiator::RunAlreadyCompleted { exit_code } => {
                Err(NegotiatedWorkers::Redundant { exit_code })
            }
            MessageFromQueueNegotiator::RunUnknown
            | MessageFromQueueNegotiator::RunCompleteButExitCodeNotKnown => {
                // We are still waiting for this run, sleep on the decay and retry.
                // Both "unknown run" and "complete, but exit code not known" states are the same
                // from our perspective - we need to re-poll until the relevant state hits the
                // queue.
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

/// Asks the work server for native runner initialization context.
/// Blocks on the result, repeatedly pinging the server until work is available.
fn wait_for_init_context(
    entity: Entity,
    client: &dyn net::ConfiguredClient,
    work_server_addr: SocketAddr,
    run_id: RunId,
) -> io::Result<InitContextResult> {
    use net_protocol::work_server::{InitContextResponse, Message, Request};

    // The work server may be waiting for the manifest, which the initialization context is blocked on;
    // to avoid pinging the server too often, let's decay on the frequency of our requests.
    let mut decay = Duration::from_millis(10);
    let max_decay = Duration::from_secs(3);
    loop {
        let mut stream = client.connect(work_server_addr)?;

        let next_test_request = Request {
            entity,
            message: Message::InitContext {
                run_id: run_id.clone(),
            },
        };

        net_protocol::write(&mut stream, next_test_request)?;
        match net_protocol::read(&mut stream)? {
            InitContextResponse::WaitingForManifest => {
                thread::sleep(decay);
                decay *= 2;
                if decay >= max_decay {
                    tracing::info!("hit max decay limit for requesting initialization context");
                    decay = max_decay;
                }
                continue;
            }
            InitContextResponse::InitContext(init_context) => return Ok(Ok(init_context)),
            InitContextResponse::RunAlreadyCompleted => return Ok(Err(RunAlreadyCompleted {})),
        }
    }
}

/// The queue side of the negotiation.
pub struct QueueNegotiator {
    addr: SocketAddr,
    listener_handle: Option<thread::JoinHandle<Result<(), QueueNegotiatorServerError>>>,
}

/// Address of a queue negotiator.
#[derive(Clone, Copy)]
pub struct QueueNegotiatorHandle(SocketAddr);

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

/// The test run a worker should ask for work on.
pub struct AssignedRun {
    pub run_id: RunId,
    pub should_generate_manifest: bool,
}

/// A marker that a test run should work on has already completed by the time the worker started
/// up, and so the worker can exit immediately.
pub struct AssignedRunCompeleted {
    pub success: bool,
}

pub enum AssignedRunStatus {
    RunUnknown,
    Run(AssignedRun),
    CompleteButExitCodeNotKnown,
    AlreadyDone { exit_code: ExitCode },
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

struct QueueNegotiatorCtx<GetAssignedRun>
where
    GetAssignedRun: Fn(&RunId, Entity) -> AssignedRunStatus + Send + Sync + 'static,
{
    /// Fetches the status of an assigned run, and yields immediately.
    get_assigned_run: Arc<GetAssignedRun>,
    advertised_queue_work_scheduler_addr: SocketAddr,
    advertised_queue_results_addr: SocketAddr,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
}

impl<GetAssignedRun> QueueNegotiatorCtx<GetAssignedRun>
where
    GetAssignedRun: Fn(&RunId, Entity) -> AssignedRunStatus + Send + Sync + 'static,
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
    /// Starts a queue negotiator on a new thread.
    ///
    /// * `get_assigned_run` - should fetch the status of an assigned run and yield immediately.
    pub fn new<GetAssignedRun>(
        public_ip: IpAddr,
        listener: Box<dyn net::ServerListener>,
        mut shutdown_rx: ShutdownReceiver,
        queue_work_scheduler_addr: SocketAddr,
        queue_results_addr: SocketAddr,
        get_assigned_run: GetAssignedRun,
    ) -> Result<Self, QueueNegotiatorServerError>
    where
        GetAssignedRun: Fn(&RunId, Entity) -> AssignedRunStatus + Send + Sync + 'static,
    {
        let addr = listener.local_addr()?;

        let advertised_queue_work_scheduler_addr =
            publicize_addr(queue_work_scheduler_addr, public_ip);
        let advertised_queue_results_addr = publicize_addr(queue_results_addr, public_ip);

        tracing::debug!("Starting negotiator on {}", addr);

        let listener_handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let server_result: Result<(), QueueNegotiatorServerError> = rt.block_on(async {
                // Initialize the listener in the async context.
                let listener = listener.into_async()?;
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
                                log_entityful_error!(error, "error handling connection to negotiator: {:?}");
                            }
                        }
                    });
                }

                Ok(())
            });

            server_result
        });

        Ok(Self {
            addr,
            listener_handle: Some(listener_handle),
        })
    }

    async fn handle_conn<GetAssignedRun>(
        ctx: QueueNegotiatorCtx<GetAssignedRun>,
        stream: net_async::UnverifiedServerStream,
    ) -> Result<(), EntityfulError>
    where
        GetAssignedRun: Fn(&RunId, Entity) -> AssignedRunStatus + Send + Sync + 'static,
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
            WantsToAttach { run_id } => {
                let attach = tracing::debug_span!("Worker set negotiating", run_id=?run_id);
                let _attach_span = attach.enter();
                tracing::debug!(run_id=?run_id, "New worker set negotiating");

                let assigned_run_result = (ctx.get_assigned_run)(&run_id, entity);

                use AssignedRunStatus::*;
                let msg = match assigned_run_result {
                    Run(AssignedRun {
                        run_id,
                        should_generate_manifest,
                    }) => {
                        tracing::debug!(
                            ?should_generate_manifest,
                            ?run_id,
                            "found run for worker set"
                        );

                        MessageFromQueueNegotiator::ExecutionContext(ExecutionContext {
                            work_server_addr: ctx.advertised_queue_work_scheduler_addr,
                            queue_results_addr: ctx.advertised_queue_results_addr,
                            worker_should_generate_manifest: should_generate_manifest,
                        })
                    }
                    AlreadyDone { exit_code } => {
                        tracing::debug!(?run_id, "run already completed");
                        MessageFromQueueNegotiator::RunAlreadyCompleted { exit_code }
                    }
                    CompleteButExitCodeNotKnown => {
                        tracing::debug!(
                            ?run_id,
                            "run already completed, but exit code not yet known"
                        );
                        MessageFromQueueNegotiator::RunCompleteButExitCodeNotKnown
                    }
                    RunUnknown => {
                        tracing::debug!(?run_id, "run not yet known");
                        MessageFromQueueNegotiator::RunUnknown
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

    pub fn join(&mut self) {
        self.listener_handle
            .take()
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }
}

impl Drop for QueueNegotiator {
    fn drop(&mut self) {
        if self.listener_handle.is_some() {
            // `shutdown` was not called manually before this drop
            self.join();
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use super::{AssignedRun, MessageToQueueNegotiator, QueueNegotiator, WorkersNegotiator};
    use crate::negotiate::{AssignedRunStatus, WorkersConfig};
    use crate::workers::{WorkerContext, WorkersExitStatus};
    use abq_utils::auth::{
        build_strategies, Admin, AdminToken, ClientAuthStrategy, ServerAuthStrategy, User,
        UserToken,
    };
    use abq_utils::exit::ExitCode;
    use abq_utils::net_opt::{ClientOptions, ServerOptions};
    use abq_utils::net_protocol::entity::{Entity, WorkerTag};
    use abq_utils::net_protocol::queue::TestSpec;
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, ProtocolWitness, Status, Test, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::work_server::{
        self, InitContext, InitContextResponse, NextTestRequest, NextTestResponse,
    };
    use abq_utils::net_protocol::workers::{
        ManifestResult, NextWork, NextWorkBundle, ReportedManifest, RunId, RunnerKind,
        TestLikeRunner, WorkId, WorkerTest, INIT_RUN_NUMBER,
    };
    use abq_utils::results_handler::NoopResultsHandler;
    use abq_utils::shutdown::ShutdownManager;
    use abq_utils::tls::{ClientTlsStrategy, ServerTlsStrategy};
    use abq_utils::{net, net_protocol};
    use abq_with_protocol_version::with_protocol_version;
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

    fn mock_queue_next_work_server(manifest_collector: ManifestCollector) -> QueueNextWork {
        let server = ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
            .bind("0.0.0.0:0")
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let mut work_to_write = loop {
                match manifest_collector.lock().unwrap().take() {
                    Some(man) => {
                        let work: Vec<_> = man
                            .manifest
                            .flatten()
                            .0
                            .into_iter()
                            .enumerate()
                            .map(|(i, spec)| {
                                NextWork::Work(WorkerTest {
                                    spec: TestSpec {
                                        test_case: spec.test_case,
                                        work_id: WorkId([i as _; 16]),
                                    },
                                    run_number: INIT_RUN_NUMBER,
                                })
                            })
                            .collect();
                        break work;
                    }
                    None => continue,
                }
            };

            work_to_write.reverse();
            server.set_nonblocking(true).unwrap();
            let mut recv_init_context = false;
            loop {
                match server.accept() {
                    Ok((mut worker, _)) => {
                        if !recv_init_context {
                            recv_init_context = true;
                            net_protocol::write(
                                &mut worker,
                                InitContextResponse::InitContext(InitContext {
                                    init_meta: Default::default(),
                                }),
                            )
                            .unwrap();
                        } else {
                            let _connect_msg: work_server::Request =
                                net_protocol::read(&mut worker).unwrap();
                            let mut all_done = false;
                            while !all_done {
                                let work = work_to_write.pop().unwrap_or(NextWork::EndOfWork);
                                all_done = matches!(work, NextWork::EndOfWork);
                                let work_bundle = NextWorkBundle::new(vec![work]);

                                let NextTestRequest {} = match net_protocol::read(&mut worker) {
                                    Ok(r) => r,
                                    _ => break, // worker disconnected
                                };
                                let response = NextTestResponse::Bundle(work_bundle);
                                net_protocol::write(&mut worker, response).unwrap();
                            }
                        }
                    }

                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        match shutdown_rx.try_recv() {
                            Ok(()) => return,
                            Err(_) => {
                                thread::sleep(Duration::from_millis(10));
                                continue;
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });

        (server_addr, shutdown_tx, handle)
    }

    fn mock_queue_results_server(manifest_collector: ManifestCollector) -> QueueResults {
        let msgs = Arc::new(Mutex::new(Vec::new()));
        let msgs2 = Arc::clone(&msgs);

        let server = ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
            .bind("0.0.0.0:0")
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        server.set_nonblocking(true).unwrap();

        let handle = thread::spawn(move || loop {
            let mut client = match server.accept() {
                Ok((client, _)) => client,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    match shutdown_rx.try_recv() {
                        Ok(()) => return,
                        Err(_) => {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                    }
                }
                Err(_) => unreachable!(),
            };

            let request: net_protocol::queue::Request = net_protocol::read(&mut client).unwrap();
            match request.message {
                net_protocol::queue::Message::WorkerResult(_, results) => {
                    net_protocol::write(&mut client, net_protocol::queue::AckTestResults {})
                        .unwrap();
                    for tr in results {
                        msgs2.lock().unwrap().extend(tr.results);
                    }
                }
                net_protocol::queue::Message::ManifestResult(_, manifest_result) => {
                    let manifest = match manifest_result {
                        ManifestResult::Manifest(man) => man,
                        _ => unreachable!(),
                    };
                    let old_manifest = manifest_collector.lock().unwrap().replace(manifest);
                    debug_assert!(
                        old_manifest.is_none(),
                        "replacing existing manifest! This is a bug in our tests."
                    );
                    net_protocol::write(&mut client, net_protocol::queue::AckManifest {}).unwrap();
                }
                net_protocol::queue::Message::RequestTotalRunResult(_) => {
                    let failed = msgs2
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|result| result.status.is_fail_like());
                    net_protocol::write(
                        &mut client,
                        net_protocol::queue::TotalRunResult::Completed {
                            exit_code: ExitCode::new(failed as _),
                        },
                    )
                    .unwrap();
                }
                net_protocol::queue::Message::WorkerRanAllTests(_) => {
                    net_protocol::write(&mut client, net_protocol::queue::AckWorkerRanAllTests {})
                        .unwrap();
                }
                _ => unreachable!(),
            }
        });

        (msgs, server_addr, shutdown_tx, handle)
    }

    fn close_queue_servers(
        shutdown_next_work_server: mpsc::Sender<()>,
        next_work_handle: JoinHandle<()>,
        shutdown_results_server: mpsc::Sender<()>,
        results_handle: JoinHandle<()>,
    ) {
        shutdown_next_work_server.send(()).unwrap();
        next_work_handle.join().unwrap();
        shutdown_results_server.send(()).unwrap();
        results_handle.join().unwrap();
    }

    fn await_messages<F>(msgs: Messages, timeout: Duration, predicate: F)
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
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn echo_test(protocol: ProtocolWitness, echo_msg: String) -> TestOrGroup {
        TestOrGroup::test(Test::new(protocol, echo_msg, [], Default::default()))
    }

    #[test]
    #[with_protocol_version]
    fn queue_and_workers_lifecycle() {
        let manifest_collector = ManifestCollector::default();
        let (next_work_addr, shutdown_next_work_server, next_work_handle) =
            mock_queue_next_work_server(Arc::clone(&manifest_collector));
        let (msgs, results_addr, shutdown_results_server, results_handle) =
            mock_queue_results_server(manifest_collector);

        let run_id = RunId::unique();

        let manifest = ManifestMessage::new(Manifest::new(
            [echo_test(proto, "hello".to_string())],
            Default::default(),
        ));

        let get_assigned_run = move |run_id: &RunId, _entity| {
            AssignedRunStatus::Run(AssignedRun {
                run_id: run_id.clone(),
                should_generate_manifest: true,
            })
        };

        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();
        let mut queue_negotiator = QueueNegotiator::new(
            "0.0.0.0".parse().unwrap(),
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
                .bind("0.0.0.0:0")
                .unwrap(),
            shutdown_rx,
            next_work_addr,
            results_addr,
            get_assigned_run,
        )
        .unwrap();
        let workers_config = WorkersConfig {
            tag: WorkerTag::new(0),
            num_workers: NonZeroUsize::new(1).unwrap(),
            runner_kind: RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest)),
            local_results_handler: Box::new(NoopResultsHandler),
            worker_context: WorkerContext::AssumeLocal,
            supervisor_in_band: false,
            debug_native_runner: false,
            results_batch_size_hint: 1,
        };
        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue_negotiator.get_handle(),
            ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls()),
            run_id,
        )
        .unwrap();

        await_messages(msgs, Duration::from_secs(1), |msgs| {
            let msgs = msgs.lock().unwrap();
            if msgs.len() != 1 {
                return false;
            }

            let result = msgs.last().unwrap();
            result.status == Status::Success && result.output.as_ref().unwrap() == "hello"
        });

        let workers_exit = workers.shutdown();
        assert!(matches!(workers_exit.status, WorkersExitStatus::Success));

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join();

        close_queue_servers(
            shutdown_next_work_server,
            next_work_handle,
            shutdown_results_server,
            results_handle,
        );
    }

    fn test_negotiator(server: Box<dyn net::ServerListener>) -> (QueueNegotiator, ShutdownManager) {
        let (shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();

        let negotiator = QueueNegotiator::new(
            "0.0.0.0".parse().unwrap(),
            server,
            shutdown_rx,
            // Below parameters are faux because they are unnecessary for healthchecks.
            "0.0.0.0:0".parse().unwrap(),
            "0.0.0.0:0".parse().unwrap(),
            move |_, _| {
                AssignedRunStatus::Run(AssignedRun {
                    run_id: RunId::unique(),
                    should_generate_manifest: true,
                })
            },
        )
        .unwrap();

        (negotiator, shutdown_tx)
    }

    #[test]
    #[with_protocol_version]
    fn queue_negotiator_healthcheck() {
        let server = ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
            .bind("0.0.0.0:0")
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut queue_negotiator, mut shutdown_tx) = test_negotiator(server);

        let client = ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls())
            .build()
            .unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            super::Request {
                entity: Entity::local_client(),
                message: MessageToQueueNegotiator::HealthCheck,
            },
        )
        .unwrap();
        let health_msg: net_protocol::health::Health = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join();
    }

    #[test]
    #[traced_test]
    #[with_protocol_version]
    fn queue_negotiator_with_auth_okay() {
        let (server_auth, client_auth, _) = build_random_strategies();

        let server = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls())
            .bind("0.0.0.0:0")
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut queue_negotiator, mut shutdown_tx) = test_negotiator(server);

        let client = ClientOptions::new(client_auth, ClientTlsStrategy::no_tls())
            .build()
            .unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            super::Request {
                entity: Entity::local_client(),
                message: MessageToQueueNegotiator::HealthCheck,
            },
        )
        .unwrap();
        let health_msg: net_protocol::health::Health = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::healthy());

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join();

        logs_with_scope_contain("", "error handling connection");
    }

    #[test]
    #[traced_test]
    #[with_protocol_version]
    fn queue_negotiator_connect_with_no_auth_fails() {
        let (server_auth, _, _) = build_random_strategies();

        let server = ServerOptions::new(server_auth, ServerTlsStrategy::no_tls())
            .bind("0.0.0.0:0")
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let (mut queue_negotiator, mut shutdown_tx) = test_negotiator(server);

        let client = ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls())
            .build()
            .unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, MessageToQueueNegotiator::HealthCheck).unwrap();

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join();

        logs_with_scope_contain("", "error handling connection");
    }
}
