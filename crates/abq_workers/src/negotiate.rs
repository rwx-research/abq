//! Module negotiate helps worker pools attach to queues.

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

use crate::workers::{
    GetInitContext, GetNextWorkBundle, InitContextResult, NotifyManifest, NotifyResult,
    RunCompletedSuccessfully, WorkerContext, WorkerPool, WorkerPoolConfig, WorkersExit,
};
use abq_utils::{
    auth::User,
    net, net_async,
    net_opt::ClientOptions,
    net_protocol::{
        self,
        entity::EntityId,
        publicize_addr,
        queue::RunAlreadyCompleted,
        workers::{NextWorkBundle, RunId, RunnerKind},
    },
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
    /// The work run we are connecting to.
    run_id: RunId,
    /// Where workers should receive messages from.
    queue_new_work_addr: SocketAddr,
    /// Where workers should send results to.
    queue_results_addr: SocketAddr,
    /// The kind of native runner workers should start.
    runner_kind: RunnerKind,
    /// Whether the queue wants a worker to generate the work manifest.
    worker_should_generate_manifest: bool,
    // TODO: do we want the queue to be able to modify the # workers configured and their
    // capabilities (timeout, retries, etc)?
}

#[derive(Serialize, Deserialize)]
enum MessageFromQueueNegotiator {
    /// The run a worker set is negotiating for has already completed, and the set should
    /// immediately exit.
    RunAlreadyCompleted { success: bool },
    /// The context a worker set should execute a run with.
    ExecutionContext(ExecutionContext),
}

#[derive(Serialize, Deserialize)]
pub enum MessageToQueueNegotiator {
    HealthCheck,
    WantsToAttach {
        /// The run the worker wants to run tests for. The queue must respect assignment of
        /// work related to this run, or return an error.
        run: RunId,
    },
}

#[derive(Clone, Debug)]
pub struct WorkersConfig {
    pub num_workers: NonZeroUsize,
    /// Context under which workers should operate.
    /// TODO: should this be user specified, or inferred, or either-or?
    pub worker_context: WorkerContext,
    pub work_timeout: Duration,
    pub work_retries: u8,
    pub debug_native_runner: bool,
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
    Redundant {
        /// Whether the run was successful when completed.
        success: bool,
    },
    /// A pool of workers were created.
    Pool(WorkerPool),
}

impl NegotiatedWorkers {
    pub fn shutdown(&mut self) -> WorkersExit {
        match self {
            NegotiatedWorkers::Redundant { success } => {
                if *success {
                    WorkersExit::Success
                } else {
                    WorkersExit::Failure
                }
            }
            NegotiatedWorkers::Pool(pool) => pool.shutdown(),
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
        wanted_run_id: RunId,
    ) -> Result<NegotiatedWorkers, WorkersNegotiateError> {
        use WorkersNegotiateError::*;

        let client = client_options.build()?;
        let mut conn = client
            .connect(queue_negotiator_handle.0)
            .map_err(|_| CouldNotConnect)?;

        let wants_to_attach = MessageToQueueNegotiator::WantsToAttach { run: wanted_run_id };
        tracing::debug!("Writing attach message");
        net_protocol::write(&mut conn, wants_to_attach).map_err(|_| CouldNotConnect)?;
        tracing::debug!("Wrote attach message");

        tracing::debug!("Awaiting execution message");

        let ExecutionContext {
            run_id,
            queue_new_work_addr,
            queue_results_addr,
            runner_kind,
            worker_should_generate_manifest,
        } = match net_protocol::read(&mut conn).map_err(|_| BadQueueMessage)? {
            MessageFromQueueNegotiator::RunAlreadyCompleted { success } => {
                return Ok(NegotiatedWorkers::Redundant { success });
            }
            MessageFromQueueNegotiator::ExecutionContext(ctx) => ctx,
        };

        tracing::debug!(
            "Recieved execution message. New work addr: {:?}, results addr: {:?}",
            queue_new_work_addr,
            queue_results_addr
        );

        let WorkersConfig {
            num_workers,
            worker_context,
            work_timeout,
            work_retries,
            debug_native_runner,
        } = workers_config;

        let notify_result: NotifyResult = Arc::new({
            let client = client.boxed_clone();

            move |entity, run_id, work_id, result| {
                let span = tracing::trace_span!("notify_result", run_id=?run_id, work_id=?work_id, queue_server=?queue_results_addr);
                let _notify_result = span.enter();

                // TODO: error handling
                let mut stream = client
                    .connect(queue_results_addr)
                    .expect("results server not available");

                let request = net_protocol::queue::Request {
                    entity,
                    message: net_protocol::queue::Message::WorkerResult(
                        run_id.clone(),
                        work_id,
                        result,
                    ),
                };

                // TODO: error handling
                net_protocol::write(&mut stream, request).unwrap();
            }
        });

        let get_init_context: GetInitContext = Arc::new({
            let client = client.boxed_clone();
            let run_id = run_id.clone();

            move || {
                let span = tracing::trace_span!("get_init_context", run_id=?run_id, new_work_server=?queue_new_work_addr);
                let _get_next_work = span.enter();

                // TODO: error handling
                wait_for_init_context(&*client, queue_new_work_addr, run_id.clone()).unwrap()
            }
        });

        let get_next_work: GetNextWorkBundle = Arc::new({
            let client = client.boxed_clone();
            let run_id = run_id.clone();

            move || {
                let span = tracing::trace_span!("get_next_work", run_id=?run_id, new_work_server=?queue_new_work_addr);
                let _get_next_work = span.enter();

                // TODO: error handling
                wait_for_next_work_bundle(&*client, queue_new_work_addr, run_id.clone()).unwrap()
            }
        });

        let notify_manifest: Option<NotifyManifest> = if worker_should_generate_manifest {
            Some(Box::new({
                let client = client.boxed_clone();

                move |entity, run_id, manifest_result| {
                    let span = tracing::trace_span!("notify_manifest", ?entity, run_id=?run_id, queue_server=?queue_results_addr);
                    let _notify_manifest = span.enter();

                    // TODO: error handling
                    let mut stream = client
                        .connect(queue_results_addr)
                        .expect("results server not available");

                    let message = net_protocol::queue::Request {
                        entity,
                        message: net_protocol::queue::Message::ManifestResult(
                            run_id.clone(),
                            manifest_result,
                        ),
                    };

                    // TODO: error handling
                    net_protocol::write(&mut stream, message).unwrap();
                    let net_protocol::workers::AckManifest =
                        net_protocol::read(&mut stream).unwrap();
                }
            }))
        } else {
            None
        };

        let run_completed_successfully: RunCompletedSuccessfully = Arc::new({
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
                    // TODO: error handling, here and below.
                    // In particular, the work server may have shut down and we can't connect. In that
                    // case the worker should shutdown too.
                    let mut stream = client
                        .connect(queue_results_addr)
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
                        TotalRunResult::Completed { succeeded } => return succeeded,
                    }
                }
            }
        });

        let pool_config = WorkerPoolConfig {
            size: num_workers,
            runner_kind,
            get_next_work,
            get_init_context,
            notify_result,
            run_completed_successfully,
            worker_context,
            work_timeout,
            work_retries,
            run_id,
            notify_manifest,
            debug_native_runner,
        };

        tracing::debug!("Starting worker pool");
        let pool = WorkerPool::new(pool_config);
        tracing::debug!("Started worker pool");

        Ok(NegotiatedWorkers::Pool(pool))
    }
}

/// Asks the work server for native runner initialization context.
/// Blocks on the result, repeatedly pinging the server until work is available.
fn wait_for_init_context(
    client: &dyn net::ConfiguredClient,
    work_server_addr: SocketAddr,
    run_id: RunId,
) -> Result<InitContextResult, io::Error> {
    use net_protocol::work_server::{InitContextResponse, WorkServerRequest};

    let next_test_request = WorkServerRequest::InitContext { run_id };

    // The work server may be waiting for the manifest, which the initialization context is blocked on;
    // to avoid pinging the server too often, let's decay on the frequency of our requests.
    let mut decay = Duration::from_millis(10);
    let max_decay = Duration::from_secs(3);
    loop {
        let mut stream = client.connect(work_server_addr)?;
        net_protocol::write(&mut stream, next_test_request.clone())?;
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

/// Asks the work server for the next item of work to run.
/// Blocks on the result, repeatedly pinging the server until work is available.
fn wait_for_next_work_bundle(
    client: &dyn net::ConfiguredClient,
    work_server_addr: SocketAddr,
    run_id: RunId,
) -> Result<NextWorkBundle, io::Error> {
    use net_protocol::work_server::NextTestResponse;

    let next_test_request = net_protocol::work_server::WorkServerRequest::NextTest { run_id };

    let mut stream = client.connect(work_server_addr)?;
    net_protocol::write(&mut stream, next_test_request)?;
    match net_protocol::read(&mut stream)? {
        NextTestResponse::Bundle(bundle) => Ok(bundle),
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
}

impl QueueNegotiatorHandle {
    pub fn get_address(&self) -> SocketAddr {
        self.0
    }

    pub fn ask_queue(
        entity: EntityId,
        queue_addr: SocketAddr,
        client_options: ClientOptions<User>,
    ) -> Result<Self, QueueNegotiatorHandleError> {
        use QueueNegotiatorHandleError::*;

        let client = client_options.build()?;
        let mut conn = client.connect(queue_addr).map_err(|_| CouldNotConnect)?;

        let request = net_protocol::queue::Request {
            entity,
            message: net_protocol::queue::Message::NegotiatorAddr,
        };

        net_protocol::write(&mut conn, request).map_err(|_| CouldNotConnect)?;
        let negotiator_addr: SocketAddr =
            net_protocol::read(&mut conn).map_err(|_| CouldNotConnect)?;

        Ok(Self(negotiator_addr))
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
    pub runner_kind: RunnerKind,
    pub should_generate_manifest: bool,
}

/// A marker that a test run should work on has already completed by the time the worker started
/// up, and so the worker can exit immediately.
pub struct AssignedRunCompeleted {
    pub success: bool,
}

pub type AssignedRunResult = Result<AssignedRun, AssignedRunCompeleted>;

/// An error that happens in the construction or execution of the queue negotiation server.
///
/// Does not include errors in the handling of requests to the server, but does include errors in
/// the acception or dispatch of connections.
#[derive(Debug, Error)]
pub enum QueueNegotiatorServerError {
    /// An IO-related error.
    #[error("{0}")]
    Io(#[from] io::Error),

    /// Any other opaque error that occured.
    #[error("{0}")]
    Other(#[from] Box<dyn Error + Send + Sync>),
}

struct QueueNegotiatorCtx<GetAssignedRun>
where
    GetAssignedRun: Fn(&RunId) -> AssignedRunResult + Send + Sync + 'static,
{
    get_assigned_run: Arc<GetAssignedRun>,
    advertised_queue_work_scheduler_addr: SocketAddr,
    advertised_queue_results_addr: SocketAddr,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
}

impl<GetAssignedRun> QueueNegotiatorCtx<GetAssignedRun>
where
    GetAssignedRun: Fn(&RunId) -> AssignedRunResult + Send + Sync + 'static,
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
    pub fn new<GetAssignedRun>(
        public_ip: IpAddr,
        listener: Box<dyn net::ServerListener>,
        mut shutdown_rx: ShutdownReceiver,
        queue_work_scheduler_addr: SocketAddr,
        queue_results_addr: SocketAddr,
        get_assigned_run: GetAssignedRun,
    ) -> Result<Self, QueueNegotiatorServerError>
    where
        GetAssignedRun: Fn(&RunId) -> AssignedRunResult + Send + Sync + 'static,
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
                                    tracing::error!("error accepting connection: {:?}", e);
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
                            if let Err(err) = result {
                                tracing::error!("error handling connection: {:?}", err);
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
    ) -> io::Result<()>
    where
        GetAssignedRun: Fn(&RunId) -> AssignedRunResult + Send + Sync + 'static,
    {
        let mut stream = ctx.handshake_ctx.handshake(stream).await?;
        let msg: MessageToQueueNegotiator = net_protocol::async_read(&mut stream).await?;

        use MessageToQueueNegotiator::*;
        match msg {
            HealthCheck => {
                let write_result =
                    net_protocol::async_write(&mut stream, &net_protocol::health::HEALTHY).await;
                if let Err(err) = write_result {
                    tracing::debug!("error sending health check: {}", err.to_string());
                }
            }
            WantsToAttach { run: wanted_run_id } => {
                tracing::debug!("New worker set negotiating");

                // Choose an `abq test ...` run the workers should perform work
                // for.
                // TODO: this fully blocks the negotiator, so nothing else can connect
                // while we wait for an run, and moreover we won't respect
                // shutdown messages.
                tracing::debug!("Finding assigned run");

                let assigned_run_result = (ctx.get_assigned_run)(&wanted_run_id);

                tracing::debug!("Found assigned run");

                let msg = match assigned_run_result {
                    Ok(AssignedRun {
                        run_id,
                        runner_kind,
                        should_generate_manifest,
                    }) => {
                        debug_assert_eq!(run_id, wanted_run_id);

                        MessageFromQueueNegotiator::ExecutionContext(ExecutionContext {
                            queue_new_work_addr: ctx.advertised_queue_work_scheduler_addr,
                            queue_results_addr: ctx.advertised_queue_results_addr,
                            runner_kind,
                            worker_should_generate_manifest: should_generate_manifest,
                            run_id,
                        })
                    }
                    Err(AssignedRunCompeleted { success }) => {
                        MessageFromQueueNegotiator::RunAlreadyCompleted { success }
                    }
                };

                tracing::debug!("Sending execution context");
                net_protocol::async_write(&mut stream, &msg).await?;
                tracing::debug!("Sent execution context");
            }
        }

        Ok(())
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
    use std::path::PathBuf;
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use super::{AssignedRun, MessageToQueueNegotiator, QueueNegotiator, WorkersNegotiator};
    use crate::negotiate::WorkersConfig;
    use crate::workers::{WorkerContext, WorkersExit};
    use abq_utils::auth::{
        build_strategies, Admin, AdminToken, ClientAuthStrategy, ServerAuthStrategy, User,
        UserToken,
    };
    use abq_utils::net_opt::{ClientOptions, ServerOptions};
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, ManifestResult, Status, Test, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::work_server::{
        InitContext, InitContextResponse, NextTestResponse, WorkServerRequest,
    };
    use abq_utils::net_protocol::workers::{
        NextWork, NextWorkBundle, RunId, RunnerKind, TestLikeRunner, WorkContext, WorkId,
    };
    use abq_utils::shutdown::ShutdownManager;
    use abq_utils::tls::{ClientTlsStrategy, ServerTlsStrategy};
    use abq_utils::{flatten_manifest, net, net_protocol};
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
    type ManifestCollector = Arc<Mutex<Option<ManifestMessage>>>;

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
                        let work: Vec<_> = flatten_manifest(man.manifest)
                            .0
                            .into_iter()
                            .enumerate()
                            .map(|(i, test_case)| NextWork::Work {
                                test_case,
                                context: WorkContext {
                                    working_dir: PathBuf::from("/"),
                                },
                                run_id: RunId::unique(),
                                work_id: WorkId(i.to_string()),
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
                        worker.set_nonblocking(false).unwrap();

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
                            let work = work_to_write.pop().unwrap_or(NextWork::EndOfWork);
                            let work_bundle = NextWorkBundle(vec![work]);

                            let _msg: WorkServerRequest = net_protocol::read(&mut worker).unwrap();
                            let response = NextTestResponse::Bundle(work_bundle);
                            net_protocol::write(&mut worker, response).unwrap();
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

            client.set_nonblocking(false).unwrap();
            let request: net_protocol::queue::Request = net_protocol::read(&mut client).unwrap();
            match request.message {
                net_protocol::queue::Message::WorkerResult(_, _, result) => {
                    msgs2.lock().unwrap().push(result);
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
                    net_protocol::write(&mut client, net_protocol::workers::AckManifest).unwrap();
                }
                net_protocol::queue::Message::RequestTotalRunResult(_) => {
                    let succeeded = msgs2
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|result| !result.status.is_fail_like());
                    net_protocol::write(
                        &mut client,
                        net_protocol::queue::TotalRunResult::Completed { succeeded },
                    )
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

    fn echo_test(echo_msg: String) -> TestOrGroup {
        TestOrGroup::Test(Test {
            id: echo_msg,
            tags: Default::default(),
            meta: Default::default(),
        })
    }

    #[test]
    fn queue_and_workers_lifecycle() {
        let manifest_collector = ManifestCollector::default();
        let (next_work_addr, shutdown_next_work_server, next_work_handle) =
            mock_queue_next_work_server(Arc::clone(&manifest_collector));
        let (msgs, results_addr, shutdown_results_server, results_handle) =
            mock_queue_results_server(manifest_collector);

        let run_id = RunId::unique();

        let get_assigned_run = |run_id: &RunId| {
            let manifest = ManifestMessage {
                manifest: Manifest {
                    members: vec![echo_test("hello".to_string())],
                    init_meta: Default::default(),
                },
            };
            Ok(AssignedRun {
                run_id: run_id.clone(),
                runner_kind: RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest),
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
            num_workers: NonZeroUsize::new(1).unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_timeout: Duration::from_secs(1),
            work_retries: 0,
            debug_native_runner: false,
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

        let status = workers.shutdown();
        assert!(matches!(status, WorkersExit::Success));

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
            |_| {
                Ok(AssignedRun {
                    run_id: RunId::unique(),
                    runner_kind: RunnerKind::TestLikeRunner(
                        TestLikeRunner::Echo,
                        ManifestMessage {
                            manifest: Manifest {
                                members: vec![],
                                init_meta: Default::default(),
                            },
                        },
                    ),
                    should_generate_manifest: true,
                })
            },
        )
        .unwrap();

        (negotiator, shutdown_tx)
    }

    #[test]
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
        net_protocol::write(&mut conn, MessageToQueueNegotiator::HealthCheck).unwrap();
        let health_msg: net_protocol::health::HEALTH = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::HEALTHY);

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join();
    }

    #[test]
    #[traced_test]
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
        net_protocol::write(&mut conn, MessageToQueueNegotiator::HealthCheck).unwrap();
        let health_msg: net_protocol::health::HEALTH = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::HEALTHY);

        shutdown_tx.shutdown_immediately().unwrap();
        queue_negotiator.join();

        logs_with_scope_contain("", "error handling connection");
    }

    #[test]
    #[traced_test]
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
