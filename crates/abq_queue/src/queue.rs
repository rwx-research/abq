use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use abq_utils::auth::ServerAuthStrategy;
use abq_utils::flatten_manifest;
use abq_utils::net;
use abq_utils::net_async;
use abq_utils::net_opt::{ServerOptions, Tls};
use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::publicize_addr;
use abq_utils::net_protocol::queue::{AssociatedTestResult, InvokerTestResult, Request};
use abq_utils::net_protocol::runners::{ManifestMessage, ManifestResult, TestCase, TestResult};
use abq_utils::net_protocol::work_server::WorkServerRequest;
use abq_utils::net_protocol::workers::{NextWorkBundle, RunnerKind, WorkContext};
use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, Message},
    workers::{NextWork, RunId, WorkId},
};
use abq_workers::negotiate::{
    AssignedRun, AssignedRunCompeleted, QueueNegotiator, QueueNegotiatorHandle,
};

use thiserror::Error;
use tracing::instrument;

// TODO: we probably want something more sophisticated here, in particular a concurrent
// work-stealing queue.
#[derive(Default, Debug)]
struct JobQueue {
    queue: VecDeque<NextWork>,
}

impl JobQueue {
    fn add_batch_work(&mut self, work: impl Iterator<Item = NextWork>) {
        self.queue.extend(work);
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn get_work(&mut self, n: NonZeroU64) -> impl Iterator<Item = NextWork> + '_ {
        // We'll give back the number of tests that were asked for, or everything if there aren't
        // that many tests left.
        let chop_off = std::cmp::min(self.queue.len(), n.get() as usize);
        self.queue.drain(..chop_off)
    }
}

#[derive(Debug)]
enum RunState {
    WaitingForFirstWorker,
    WaitingForManifest,
    HasWork {
        queue: JobQueue,

        /// The number of tests to batch to a worker at a time, as hinted by an invoker of the work.
        // Co-locate this here so that we don't have to index `run_data` when grabbing a
        // test.
        batch_size_hint: NonZeroU64,
    },
    Done {
        succeeded: bool,
    },
}

struct RunData {
    runner: RunnerKind,
    /// The number of tests to batch to a worker at a time, as hinted by an invoker of the work.
    batch_size_hint: NonZeroU64,
}

struct WaitingForManifestError;

#[derive(Default)]
struct RunQueues {
    /// One queue per `abq test ...` run, at least for now.
    queues: HashMap<RunId, RunState>,
    run_data: HashMap<RunId, RunData>,
}

/// A reason a new queue for a run could not be created.
#[derive(Debug)]
enum NewQueueError {
    /// The run ID has an active run.
    RunIdAlreadyInProgress,
    /// The run ID was previously completed.
    RunIdPreviouslyCompleted,
}

enum AssignedRunLookup {
    Some(AssignedRun),
    /// There is no associated run yet known.
    NotFound,
    /// An associated run is known, but it has already completed; a worker should exit immediately.
    AlreadyDone {
        success: bool,
    },
}

impl RunQueues {
    /// Attempts to create a new queue for a run.
    /// If the given run ID already has an associated queue, an error is returned.
    pub fn create_queue(
        &mut self,
        run_id: RunId,
        runner: RunnerKind,
        batch_size_hint: NonZeroU64,
    ) -> Result<(), NewQueueError> {
        if let Some(state) = self.queues.get(&run_id) {
            let err = match state {
                RunState::WaitingForFirstWorker
                | RunState::WaitingForManifest
                | RunState::HasWork { .. } => NewQueueError::RunIdAlreadyInProgress,
                RunState::Done { .. } => NewQueueError::RunIdPreviouslyCompleted,
            };

            return Err(err);
        }

        // The run ID is fresh; create a new queue for it.
        let old_queue = self
            .queues
            .insert(run_id.clone(), RunState::WaitingForFirstWorker);
        debug_assert!(old_queue.is_none());

        let old_run_data = self.run_data.insert(
            run_id,
            RunData {
                runner,
                batch_size_hint,
            },
        );
        debug_assert!(old_run_data.is_none());

        Ok(())
    }

    // Chooses a run for a set of workers to attach to. Returns `None` if an appropriate run is not
    // found.
    pub fn get_run_for_worker(&mut self, run_id: &RunId) -> AssignedRunLookup {
        let run_state = match self.queues.get_mut(run_id) {
            Some(st) => st,
            None => return AssignedRunLookup::NotFound,
        };

        let run_data = match self.run_data.get(run_id) {
            Some(data) => data,
            None => {
                // If there is an active run, then the run data exists iff the run state exists;
                // however, if the run state is known to be complete, the run data will already
                // have been pruned, and the worker should not be given a run.
                match run_state {
                    RunState::Done { succeeded } => {
                        return AssignedRunLookup::AlreadyDone {
                            success: *succeeded,
                        }
                    }
                    st => {
                        tracing::error!(
                            run_state=?st,
                            "run data is missing, but run state is present and not marked as Done"
                        );
                        unreachable!(
                            "run state should only be `Done` if data is missing, but it was {:?}",
                            st
                        );
                    }
                }
            }
        };

        let RunData {
            runner,
            batch_size_hint: _,
        } = run_data;

        let assigned_run = match run_state {
            RunState::WaitingForFirstWorker => {
                // This is the first worker to attach; ask it to generate the manifest.
                *run_state = RunState::WaitingForManifest;
                AssignedRun {
                    run_id: run_id.clone(),
                    runner_kind: runner.clone(),
                    should_generate_manifest: true,
                }
            }
            _ => {
                // Otherwise we are already waiting for the manifest, or already have it; just tell
                // the worker what runner it should set up.
                AssignedRun {
                    run_id: run_id.clone(),
                    runner_kind: runner.clone(),
                    should_generate_manifest: false,
                }
            }
        };

        AssignedRunLookup::Some(assigned_run)
    }

    pub fn add_manifest(&mut self, run_id: RunId, flat_manifest: Vec<TestCase>) {
        let state = self.queues.get_mut(&run_id).expect("no queue for run");

        let work_from_manifest = flat_manifest.into_iter().map(|test_case| {
            NextWork::Work {
                test_case,
                run_id: run_id.clone(),
                // TODO: populate correctly
                work_id: WorkId("".to_string()),
                // TODO: populate correctly
                context: WorkContext {
                    working_dir: std::env::current_dir().unwrap(),
                },
            }
        });

        let mut queue = JobQueue::default();
        queue.add_batch_work(work_from_manifest);

        *state = RunState::HasWork {
            queue,
            batch_size_hint: self
                .run_data
                .get(&run_id)
                .expect("no data for run")
                .batch_size_hint,
        }
    }

    pub fn next_work(&mut self, run_id: RunId) -> Result<NextWorkBundle, WaitingForManifestError> {
        match self
            .queues
            .get_mut(&run_id)
            .expect("no queue state for run")
        {
            RunState::WaitingForFirstWorker => Err(WaitingForManifestError),
            RunState::WaitingForManifest => Err(WaitingForManifestError),
            RunState::HasWork {
                queue,
                batch_size_hint,
            } => {
                // NB: in the future, the batch size is likely to be determined intellegently, i.e.
                // from off-line timing data. But for now, we use the hint the client provided.
                let batch_size = *batch_size_hint;

                let mut bundle = Vec::with_capacity(batch_size.get() as _);
                bundle.extend(queue.get_work(batch_size));
                if bundle.len() < batch_size.get() as _ {
                    // We've hit the end of the manifest but have space for more tests to send in
                    // this batch. Let the worker know this is the end, so they don't have to
                    // round-trip.
                    bundle.push(NextWork::EndOfWork);
                }
                Ok(NextWorkBundle(bundle))
            }
            RunState::Done { .. } => Ok(NextWorkBundle(vec![NextWork::EndOfWork])),
        }
    }

    pub fn mark_complete(&mut self, run_id: RunId, succeeded: bool) {
        let state = self
            .queues
            .get_mut(&run_id)
            .expect("no queue state for run");
        match state {
            RunState::HasWork {
                queue,
                batch_size_hint: _,
            } => {
                assert!(queue.is_empty(), "Invalid state - queue is not complete!");
            }
            RunState::WaitingForFirstWorker
            | RunState::WaitingForManifest
            | RunState::Done { .. } => {
                unreachable!("Invalid state");
            }
        }

        // TODO: right now, we are just marking something complete so that future workers asking
        // for work about the run get an EndOfWork message. But this leaks memory, how can
        // we avoid that?
        *state = RunState::Done { succeeded };
        self.run_data.remove(&run_id);
    }

    pub fn mark_failed_to_start(&mut self, run_id: RunId) {
        let state = self
            .queues
            .get_mut(&run_id)
            .expect("no queue state for run");
        match state {
            RunState::WaitingForManifest => {
                // okay
            }
            RunState::WaitingForFirstWorker | RunState::HasWork { .. } | RunState::Done { .. } => {
                unreachable!("Invalid state - can only fail to start while waiting for manifest");
            }
        }

        *state = RunState::Done { succeeded: false };
        self.run_data.remove(&run_id);
    }

    fn get_run_state(&self, run_id: &RunId) -> Option<&RunState> {
        self.queues.get(run_id)
    }

    fn num_active_runs(&self) -> usize {
        self.queues
            .values()
            .filter(|state| match state {
                RunState::WaitingForFirstWorker
                | RunState::WaitingForManifest
                | RunState::HasWork { .. } => true,
                RunState::Done { .. } => false,
            })
            .count()
    }
}

type SharedRunQueues = Arc<Mutex<RunQueues>>;

/// Executes an initialization sequence that must be performed before any queue can be created.
/// [init] only needs to be run once in a process, even if multiple [Abq] instances are crated, but
/// it must be run before any instance is created.
/// Do not use this in tests. The initialization sequence for test is done in the [crate root][crate].
#[cfg(not(test))]
pub fn init() {
    // We must initialize the workers, in particular what's needed for the process pool.
    abq_workers::workers::init();
}

pub struct Abq {
    server_addr: SocketAddr,
    server_handle: Option<JoinHandle<Result<(), QueueServerError>>>,
    server_shutdown_tx: tokio::sync::mpsc::Sender<()>,

    work_scheduler_handle: Option<JoinHandle<Result<(), WorkSchedulerError>>>,
    work_scheduler_shutdown_tx: tokio::sync::mpsc::Sender<()>,
    negotiator: QueueNegotiator,

    active: bool,
}

#[derive(Debug, Error)]
pub enum AbqError {
    #[error("{0}")]
    Queue(#[from] QueueServerError),

    #[error("{0}")]
    WorkScheduler(#[from] WorkSchedulerError),
}

impl Abq {
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    pub fn start(config: QueueConfig) -> Self {
        start_queue(config)
    }

    pub fn wait_forever(&mut self) {
        self.server_handle.take().map(JoinHandle::join);
        self.work_scheduler_handle.take().map(JoinHandle::join);
    }

    /// Sends a signal to shutdown immediately.
    #[instrument(level = "trace", skip(self))]
    pub fn shutdown(&mut self) -> Result<(), AbqError> {
        debug_assert!(self.active);

        self.active = false;

        // Shut down all of our servers.
        self.server_shutdown_tx
            .blocking_send(())
            .map_err(|_| QueueServerError::Other("queue server channels have died".into()))?;
        self.work_scheduler_shutdown_tx
            .blocking_send(())
            .map_err(|_| WorkSchedulerError::Other("work scheduler channels have died".into()))?;

        self.negotiator.shutdown();

        self.server_handle
            .take()
            .expect("server handle must be available during shutdown")
            .join()
            .unwrap()?;

        self.work_scheduler_handle
            .take()
            .expect("worker handle must be available during shutdown")
            .join()
            .unwrap()?;

        Ok(())
    }

    pub fn get_negotiator_handle(&self) -> QueueNegotiatorHandle {
        self.negotiator.get_handle()
    }
}

impl Drop for Abq {
    fn drop(&mut self) {
        if self.active {
            // Our user never called shutdown; try to perform a clean exit.
            // We can't do anything with an error, since this is a drop.
            let _ = self.shutdown();
        }
    }
}

/// Configures initialization of the queue.
pub struct QueueConfig {
    /// The IP address of sockets the queue should advertise, e.g. when negotiating with a worker.
    pub public_ip: IpAddr,
    /// The IP address sockets the queue creates should actually bind to. This can be different
    /// than [public_ip], but is not required to be.
    pub bind_ip: IpAddr,
    /// The port the mainline queue server should bind to. Binds to any port if `0`.
    pub server_port: u16,
    /// The port the work-scheduling server should bind to. Binds to any port if `0`.
    pub work_port: u16,
    /// The port the negotiator server should bind to. Binds to any port if `0`.
    pub negotiator_port: u16,
    /// How the queue should construct its servers.
    pub server_options: ServerOptions,
}

impl Default for QueueConfig {
    /// Creates a [`QueueConfig`] that always binds and advertises on INADDR_ANY, with arbitrary
    /// ports for its servers.
    fn default() -> Self {
        Self {
            public_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            server_port: 0,
            work_port: 0,
            negotiator_port: 0,
            server_options: ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO),
        }
    }
}

/// Initializes a queue, binding the queue negotiator to the given address.
/// All other public channels to the queue (the work and results server) are exposed
/// on the same host IP address as the negotiator is.
fn start_queue(config: QueueConfig) -> Abq {
    let QueueConfig {
        public_ip,
        bind_ip,
        server_port,
        work_port,
        negotiator_port,
        server_options,
    } = config;

    let queues: SharedRunQueues = Default::default();

    let server_listener = server_options.bind((bind_ip, server_port)).unwrap();
    let server_addr = server_listener.local_addr().unwrap();

    let negotiator_listener = server_options.bind((bind_ip, negotiator_port)).unwrap();
    let negotiator_addr = negotiator_listener.local_addr().unwrap();
    let public_negotiator_addr = publicize_addr(negotiator_addr, public_ip);

    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::mpsc::channel(1);

    let server_handle = thread::spawn({
        let queue_server = QueueServer::new(Arc::clone(&queues), public_negotiator_addr);
        move || queue_server.start_on(server_listener, server_shutdown_rx)
    });

    let new_work_server = server_options.bind((bind_ip, work_port)).unwrap();
    let new_work_server_addr = new_work_server.local_addr().unwrap();

    let (work_scheduler_shutdown_tx, work_scheduler_shutdown_rx) = tokio::sync::mpsc::channel(1);

    let work_scheduler_handle = thread::spawn({
        let scheduler = WorkScheduler {
            queues: Arc::clone(&queues),
        };
        move || scheduler.start_on(new_work_server, work_scheduler_shutdown_rx)
    });

    // Chooses a test run for a set of workers to attach to.
    // This blocks until there is an run available for the particular worker.
    // TODO: blocking is not good; workers that can connect fast may be blocked on workers waiting
    // for a test run to be established. Make this cede control to a runtime if the worker's run is
    // not yet available.
    let choose_run_for_worker = move |wanted_run_id: &RunId| loop {
        let opt_assigned = { queues.lock().unwrap().get_run_for_worker(wanted_run_id) };
        match opt_assigned {
            AssignedRunLookup::NotFound => {
                // Nothing yet; release control and sleep for a bit in case something comes.
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            AssignedRunLookup::AlreadyDone { success } => {
                return Err(AssignedRunCompeleted { success })
            }
            AssignedRunLookup::Some(assigned) => {
                return Ok(assigned);
            }
        }
    };
    let negotiator = QueueNegotiator::new(
        public_ip,
        negotiator_listener,
        new_work_server_addr,
        server_addr,
        choose_run_for_worker,
    )
    .unwrap();

    Abq {
        server_addr,
        server_handle: Some(server_handle),
        server_shutdown_tx,

        work_scheduler_handle: Some(work_scheduler_handle),
        work_scheduler_shutdown_tx,
        negotiator,

        active: true,
    }
}

#[derive(Debug)]
struct BufferedResults {
    buffer: Vec<AssociatedTestResult>,
    max_size: usize,
}

impl BufferedResults {
    /// Default suggested sizing of the buffer. In general you should try to use a more precise
    /// size when possible.
    const DEFAULT_SIZE: usize = 10;

    fn new(max_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(max_size),
            max_size,
        }
    }

    /// Adds a new test result to the buffer. If the buffer capacity is exceeded, all test results
    /// to be drained are returned.
    fn push(&mut self, result: AssociatedTestResult) -> Option<Vec<AssociatedTestResult>> {
        self.buffer.push(result);
        if self.buffer.len() >= self.max_size {
            return Some(self.buffer.drain(..).collect());
        }
        None
    }

    fn drain_all(&mut self) -> Vec<AssociatedTestResult> {
        std::mem::take(&mut self.buffer)
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// Handler for sending test results back to the abq client that issued a test run.
#[derive(Debug)]
enum ClientResponder {
    /// We have an open stream upon which to send back the test results.
    DirectStream {
        stream: Box<dyn net_async::ServerStream>,
        buffer: BufferedResults,
    },
    /// There is not yet an open connection we can stream the test results back on.
    /// This state may be reached during the time a client is disconnected.
    Disconnected {
        results: Vec<AssociatedTestResult>,
        /// Size of the [buffer][BufferedResults] we should create when moving back into connected
        /// mode.
        buffer_size: usize,
    },
}

impl ClientResponder {
    fn new(stream: Box<dyn net_async::ServerStream>, buffer_size: usize) -> Self {
        Self::DirectStream {
            stream,
            buffer: BufferedResults::new(buffer_size),
        }
    }

    async fn send_test_result_batch_help(
        stream: &mut Box<dyn net_async::ServerStream>,
        buffer: &mut BufferedResults,
        results: Vec<AssociatedTestResult>,
    ) -> Result<(), Self> {
        use net_protocol::client::AckTestResult;

        let batch_msg = InvokerTestResult::Results(results);

        // Send the test results and wait for an ack. If either fails, suppose the client is
        // disconnected.
        let write_result = net_protocol::async_write(stream, &batch_msg).await;
        let client_disconnected = match write_result {
            Err(_) => true,
            Ok(()) => {
                let ack = net_protocol::async_read(stream).await;
                match ack {
                    Err(_) => true,
                    Ok(AckTestResult {}) => false,
                }
            }
        };

        if client_disconnected {
            // Demote ourselves to the disconnected state until reconnection happens.
            let results = match batch_msg {
                InvokerTestResult::Results(results) => results,
                _ => unreachable!(),
            };

            // NOTE: we could store old, empty `buffer` here, so we moving back to
            // `DirectStream` doesn't cost an allocation.
            return Err(Self::Disconnected {
                results,
                buffer_size: buffer.max_size,
            });
        }

        Ok(())
    }

    async fn send_test_result(&mut self, test_result: AssociatedTestResult) {
        match self {
            ClientResponder::DirectStream { stream, buffer } => {
                let opt_results_to_drain = buffer.push(test_result);

                if let Some(results) = opt_results_to_drain {
                    let opt_disconnected =
                        Self::send_test_result_batch_help(stream, buffer, results).await;
                    if let Err(disconnected) = opt_disconnected {
                        *self = disconnected;
                    }
                }
            }
            ClientResponder::Disconnected {
                results,
                buffer_size: _,
            } => {
                results.push(test_result);
            }
        }
    }

    async fn flush_results(&mut self) {
        match self {
            ClientResponder::DirectStream { stream, buffer } => {
                let remaining_results = buffer.drain_all();

                if !remaining_results.is_empty() {
                    let opt_disconnected =
                        Self::send_test_result_batch_help(stream, buffer, remaining_results).await;

                    debug_assert!(
                        buffer.is_empty(),
                        "buffer should remain empty after drainage"
                    );

                    if let Err(disconnected) = opt_disconnected {
                        *self = disconnected;
                    }
                }
            }
            ClientResponder::Disconnected { .. } => {
                // nothing we can do
            }
        }
    }

    /// Updates the responder with a new connection. If there are any pending test results that
    /// failed to be sent from a previous connections, they are streamed to the new connection
    /// before the future returned from this function completes.
    async fn update_connection_to(&mut self, new_conn: Box<dyn net_async::ServerStream>) {
        // There's no great way for us to check whether the existing client connection is,
        // in fact, closed. Instead we accept all faithful reconnection requests, and
        // assume that the client is well-behaved (it will not attempt to reconnect unless
        // it is explicitly disconnected).
        //
        // Even if we could detect closed connections at this point, we'd have an TOCTOU race -
        // it may be the case that a stream closes between the time that we check it is closed,
        // and when we issue an error for the reconnection attempt.

        let buffer_size_hint = match self {
            ClientResponder::DirectStream { buffer, .. } => buffer.max_size,
            ClientResponder::Disconnected { buffer_size, .. } => *buffer_size,
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
            ClientResponder::Disconnected { results, .. } => {
                // We need to send back all the buffered results to the new stream.
                // Must send all test results in sequence, since each writes to the
                // same stream back to the client.
                for test_result in results {
                    self.send_test_result(test_result).await;
                }
            }
            ClientResponder::DirectStream { .. } => {
                // nothing more to do
            }
        }
    }
}

/// run ID -> result responder
type ActiveRunResponders = Arc<
    tokio::sync::RwLock<
        //
        HashMap<RunId, tokio::sync::Mutex<ClientResponder>>,
    >,
>;

struct ActiveRunState {
    /// Amount of work items left for the run.
    work_left: usize,
    /// So far, has the run been entirely successful?
    is_successful: bool,
}

/// Cache of the current state of active runs, so that we don't have to lock the queue all the time.
// TODO: consider using DashMap
type RunStateCache = Arc<Mutex<HashMap<RunId, ActiveRunState>>>;

/// Central server listening for new test run runs and results.
struct QueueServer {
    queues: SharedRunQueues,
    public_negotiator_addr: SocketAddr,
}

/// An error that happens in the construction or execution of the queue server.
///
/// Does not include errors in the handling of requests to the queue, but does include errors in
/// the acception or dispatch of connections.
#[derive(Debug, Error)]
pub enum QueueServerError {
    /// An IO-related error.
    #[error("{0}")]
    Io(#[from] io::Error),

    /// Any other opaque error that occured.
    #[error("{0}")]
    Other(#[from] Box<dyn Error + Send + Sync>),
}

/// An error that occurs when an abq client attempts to re-connect to the queue.
#[derive(Debug, Error, PartialEq)]
enum ClientReconnectionError {
    /// The client sent an initial test run request to the queue.
    #[error("{0} was never used to invoke work on the queue")]
    NeverInvoked(RunId),
}

#[derive(Clone)]
struct QueueServerCtx {
    queues: SharedRunQueues,
    /// When all results for a particular run are communicated, we want to make sure that the
    /// responder thread is closed and that the entry here is dropped.
    active_runs: ActiveRunResponders,
    state_cache: RunStateCache,
    public_negotiator_addr: SocketAddr,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
}

impl QueueServer {
    fn new(queues: SharedRunQueues, public_negotiator_addr: SocketAddr) -> Self {
        Self {
            queues,
            public_negotiator_addr,
        }
    }

    fn start_on(
        self,
        server_listener: Box<dyn net::ServerListener>,
        mut server_shutdown: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<(), QueueServerError> {
        // Create a new tokio runtime solely for handling requests that come into the queue server.
        //
        // Note that all of the work done by the queue is purely coordination, so we're heavily I/O
        // bound. As such we put all this work on one thread.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        // Start the server in the tokio runtime.
        let server_result: Result<(), QueueServerError> = rt.block_on(async {
            // Initialize the async server listener.
            let server_listener = server_listener.into_async()?;

            let Self {
                queues,
                public_negotiator_addr,
            } = self;

            let ctx = QueueServerCtx {
                queues,
                active_runs: Default::default(),
                state_cache: Default::default(),
                public_negotiator_addr,
                handshake_ctx: Arc::new(server_listener.handshake_ctx()),
            };

            loop {
                let (client, _) = tokio::select! {
                    conn = server_listener.accept() => {
                        match conn {
                            Ok(conn) => conn,
                            Err(e) => {
                                tracing::error!("error accepting connection: {:?}", e);
                                continue;
                            }
                        }
                    }
                    _ = server_shutdown.recv() => {
                        break;
                    }
                };

                tokio::spawn({
                    let ctx = ctx.clone();
                    async move {
                        let result = Self::handle(ctx, client).await;
                        if let Err(err) = result {
                            tracing::error!("error handling connection to queue: {:?}", err);
                        }
                    }
                });
            }

            Ok(())
        });

        server_result
    }

    #[inline(always)]
    async fn handle(
        ctx: QueueServerCtx,
        stream: net_async::UnverifiedServerStream,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut stream = ctx.handshake_ctx.handshake(stream).await?;
        let Request { entity, message } = net_protocol::async_read(&mut stream).await?;

        let result: Result<(), Box<dyn Error + Send + Sync>> = match message {
            Message::HealthCheck => {
                // TODO here and in other servers - healthchecks currently require auth, whenever
                // auth is configured. Should they?
                Self::handle_healthcheck(entity, stream).await
            }
            Message::ActiveTestRuns => {
                Self::handle_active_test_runs(ctx.queues, entity, stream).await
            }
            Message::NegotiatorAddr => {
                Self::handle_negotiator_addr(entity, stream, ctx.public_negotiator_addr).await
            }
            Message::InvokeWork(invoke_work) => {
                Self::handle_invoked_work(ctx.active_runs, ctx.queues, entity, stream, invoke_work)
                    .await
            }
            Message::Reconnect(run_id) => {
                Self::handle_invoker_reconnection(ctx.active_runs, entity, stream, run_id)
                    .await
                    // Upcast the reconnection error into a generic error
                    .map_err(|e| Box::new(e) as _)
            }
            Message::ManifestResult(run_id, manifest_result) => {
                Self::handle_manifest_result(
                    ctx.queues,
                    ctx.active_runs,
                    ctx.state_cache,
                    entity,
                    run_id,
                    manifest_result,
                    stream,
                )
                .await
            }
            Message::WorkerResult(run_id, work_id, result) => {
                // Recording and sending the test result back to the abq test client may
                // be expensive, with multiple IO transactions. There is no reason to block the
                // client connection on that; recall that the worker side of the connection will
                // move on to the next test as soon as it sends a test result back.
                //
                // So, we have no use for the connection as soon as we've parsed the test result out,
                // and we'd prefer to close it immediately.
                drop(stream);

                // Record the test result and notify the test client out-of-band.
                Self::handle_worker_result(
                    ctx.queues,
                    ctx.active_runs,
                    ctx.state_cache,
                    entity,
                    run_id,
                    work_id,
                    result,
                )
                .await
            }

            Message::RequestTotalRunResult(run_id) => {
                Self::handle_total_run_result_request(ctx.queues, run_id, stream).await
            }
        };

        result
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(stream))]
    async fn handle_healthcheck(
        entity: EntityId,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Right now nothing interesting is owned by the queue, so nothing extra to check.
        net_protocol::async_write(&mut stream, &net_protocol::health::HEALTHY).await?;
        Ok(())
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(queues, stream))]
    async fn handle_active_test_runs(
        queues: SharedRunQueues,
        entity: EntityId,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let active_runs = queues.lock().unwrap().num_active_runs().try_into()?;
        let response = net_protocol::queue::ActiveTestRunsResponse { active_runs };
        net_protocol::async_write(&mut stream, &response).await?;
        Ok(())
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(stream, public_negotiator_addr))]
    async fn handle_negotiator_addr(
        entity: EntityId,
        mut stream: Box<dyn net_async::ServerStream>,
        public_negotiator_addr: SocketAddr,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        net_protocol::async_write(&mut stream, &public_negotiator_addr).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(active_runs, queues, stream))]
    async fn handle_invoked_work(
        active_runs: ActiveRunResponders,
        queues: SharedRunQueues,
        entity: EntityId,
        mut stream: Box<dyn net_async::ServerStream>,
        invoke_work: InvokeWork,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let InvokeWork {
            run_id,
            runner,
            batch_size_hint,
        } = invoke_work;

        let could_create_queue =
            queues
                .lock()
                .unwrap()
                .create_queue(run_id.clone(), runner, batch_size_hint);

        match could_create_queue {
            Ok(()) => {
                {
                    tracing::info!(?run_id, ?entity, "created new queue");

                    // Only after telling the client that we are ready for it to move into listing
                    // for test results, associate the results responder.
                    //
                    // NB: an exclusive lock here is important! As unlikely as it may be, sharing the
                    // responder before the response message is sent may result in test results being
                    // sent *before* this response message is sent.
                    let mut active_runs = active_runs.write().await;

                    // TODO: if this write fails, we will be in an inconsistent state and have leaked a
                    // run ID - there will be an active queue, but no active responder for when test
                    // results start streaming back in.
                    // If this run fails, we should switch the queue state to a mode where we say
                    // "something has went wrong" and disallow workers to pull tests until either the
                    // invoker re-invokes the run, or the run gets deleted.
                    net_protocol::async_write(
                        &mut stream,
                        &net_protocol::queue::InvokeWorkResponse::Success,
                    )
                    .await?;

                    // TODO: we could determine a more optimal sizing of the test results buffer by
                    // e.g. considering the size of the test manifest.
                    let results_buffering_size = BufferedResults::DEFAULT_SIZE;

                    let results_responder = ClientResponder::new(stream, results_buffering_size);
                    let old_responder =
                        active_runs.insert(run_id, tokio::sync::Mutex::new(results_responder));
                    debug_assert!(
                        old_responder.is_none(),
                        "Existing stream for expected unique run id"
                    );
                }

                Ok(())
            }
            Err(err) => {
                tracing::info!(
                    ?run_id,
                    ?entity,
                    "failed to create new queue for invocation"
                );

                let failure_reason = match err {
                    NewQueueError::RunIdAlreadyInProgress => {
                        net_protocol::queue::InvokeFailureReason::DuplicateRunId {
                            recently_completed: false,
                        }
                    }
                    NewQueueError::RunIdPreviouslyCompleted => {
                        net_protocol::queue::InvokeFailureReason::DuplicateRunId {
                            recently_completed: true,
                        }
                    }
                };
                let failure_msg = net_protocol::queue::InvokeWorkResponse::Failure(failure_reason);

                net_protocol::async_write(&mut stream, &failure_msg).await?;

                Ok(())
            }
        }
    }

    #[instrument(level = "trace", skip(active_runs, new_stream))]
    async fn handle_invoker_reconnection(
        active_runs: ActiveRunResponders,
        entity: EntityId,
        new_stream: Box<dyn net_async::ServerStream>,
        run_id: RunId,
    ) -> Result<(), ClientReconnectionError> {
        use std::collections::hash_map::Entry;

        // When an abq client loses connection with the queue, they may attempt to reconnect.
        // We'll need to update the connection from streaming results to the client to the new one.
        let mut active_runs = active_runs.write().await;
        let active_conn_entry = active_runs.entry(run_id.clone());
        match active_conn_entry {
            Entry::Occupied(mut occupied) => {
                let new_stream_addr = new_stream.peer_addr();

                occupied
                    .get_mut()
                    .lock()
                    .await
                    .update_connection_to(new_stream)
                    .await;

                tracing::debug!(
                    "rerouted connection for {:?} to {:?}",
                    run_id,
                    new_stream_addr
                );

                Ok(())
            }
            Entry::Vacant(_) => {
                // There was never a connection for this run ID!
                Err(ClientReconnectionError::NeverInvoked(run_id))
            }
        }
    }

    #[instrument(level = "trace", skip(queues, state_cache))]
    async fn handle_manifest_result(
        queues: SharedRunQueues,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        entity: EntityId,
        run_id: RunId,
        manifest_result: ManifestResult,
        stream: Box<dyn net_async::ServerStream>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match manifest_result {
            ManifestResult::Manifest(manifest) => {
                Self::handle_manifest_success(queues, state_cache, entity, run_id, manifest, stream)
                    .await
            }
            ManifestResult::TestRunnerError { error } => {
                Self::handle_manifest_failure(
                    queues,
                    active_runs,
                    state_cache,
                    entity,
                    run_id,
                    error,
                    stream,
                )
                .await
            }
        }
    }

    #[instrument(level = "trace", skip(queues, state_cache, manifest))]
    async fn handle_manifest_success(
        queues: SharedRunQueues,
        state_cache: RunStateCache,
        entity: EntityId,
        run_id: RunId,
        manifest: ManifestMessage,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Record the manifest for this run in its appropriate queue.
        // TODO: actually record the manifest metadata
        let flat_manifest = flatten_manifest(manifest.manifest);

        tracing::info!(?run_id, ?entity, size=?flat_manifest.len(), "received manifest");

        let run_state = ActiveRunState {
            work_left: flat_manifest.len(),
            is_successful: true,
        };

        state_cache
            .lock()
            .unwrap()
            .insert(run_id.clone(), run_state);

        queues.lock().unwrap().add_manifest(run_id, flat_manifest);

        net_protocol::async_write(&mut stream, &net_protocol::workers::AckManifest).await?;

        Ok(())
    }

    #[instrument(
        level = "trace",
        skip(queues, state_cache, opaque_manifest_generation_error)
    )]
    async fn handle_manifest_failure(
        queues: SharedRunQueues,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        entity: EntityId,
        run_id: RunId,
        opaque_manifest_generation_error: String,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // If a worker failed to generate a manifest, we're going to immediately end the test run,
        // as this indicates a failure in the underlying test runners.

        tracing::info!(?run_id, ?entity, "failure notification for manifest");

        {
            // Immediately ACK to the worker responsible for the manifest, so they can relinquish
            // blocking and exit.
            net_protocol::async_write(&mut stream, &net_protocol::workers::AckManifest).await?;
            drop(stream);
        }

        {
            // We should not have an entry for the run in the state cache, since we only ever know
            // the initial state of a run after receiving the manifest.
            debug_assert!(!state_cache.lock().unwrap().contains_key(&run_id));
        }

        let responder = {
            // Remove and take over the responder; since the manifest is not known, we should be the
            // only task that ever communicates with the client.
            let responder = active_runs.write().await.remove(&run_id).unwrap();
            responder.into_inner()
        };

        // Tell the client that the test has failed.
        match responder {
            ClientResponder::DirectStream { mut stream, buffer } => {
                debug_assert!(buffer.is_empty(), "messages before manifest received");

                net_protocol::async_write(
                    &mut stream,
                    &InvokerTestResult::TestCommandError {
                        error: opaque_manifest_generation_error,
                    },
                )
                .await?;

                drop(stream);
            }
            ClientResponder::Disconnected {
                results,
                buffer_size: _,
            } => {
                // Unfortunately, since we still don't have an active connection to the abq
                // client, we can't do anything here.
                // To avoid leaking memory we assume the client is dead and drop the results.
                //
                // TODO: rather than dropping the failure message, consider attaching it back
                // to the queue. If the client re-connects, we'll let them know that the test
                // has failed to startup.
                let _ = results;
            }
        }

        queues.lock().unwrap().mark_failed_to_start(run_id);

        Ok(())
    }

    #[instrument(level = "trace", skip(queues, active_runs, state_cache))]
    async fn handle_worker_result(
        queues: SharedRunQueues,
        active_runs: ActiveRunResponders,
        state_cache: RunStateCache,
        entity: EntityId,
        run_id: RunId,
        work_id: WorkId,
        result: TestResult,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let test_status = result.status;

        {
            // First up, chuck the test result back over to `abq test`. Make sure we don't steal
            // the lock on active_runs for any longer than it takes to send that request.
            let active_runs = active_runs.read().await;

            let invoker_stream = active_runs.get(&run_id).expect("run is not active");

            invoker_stream
                .lock()
                .await
                .send_test_result((work_id, result))
                .await;
        }

        let (no_more_work, is_successful) = {
            // Update the amount of work we have left; again, steal the map of work for only as
            // long is it takes for us to do that.
            let mut state_cache = state_cache.lock().unwrap();
            let run_state = state_cache
                .get_mut(&run_id)
                .expect("run id not recorded, but we got a result for it?");

            run_state.work_left -= 1;
            run_state.is_successful = run_state.is_successful && !test_status.is_fail_like();

            (run_state.work_left == 0, run_state.is_successful)
        };

        if no_more_work {
            tracing::info!(?run_id, ?entity, "run completed");

            // Now, we have to take both the active runs, and the map of work left, to mark
            // the work for the current run as complete in both cases. It's okay for this to
            // be slow(er), since it happens only once per test run.
            //
            // Note however that this does block requests indexing into the maps for other `abq
            // test` runs.
            // We may want to use concurrent hashmaps that index into partitioned buckets for that
            // use case (e.g. DashMap).
            let responder = active_runs.write().await.remove(&run_id).unwrap();
            let mut responder = responder.into_inner();

            responder.flush_results().await;

            match responder {
                ClientResponder::DirectStream { mut stream, buffer } => {
                    assert!(
                        buffer.is_empty(),
                        "no results should be buffered after drainage"
                    );

                    net_protocol::async_write(&mut stream, &InvokerTestResult::EndOfResults)
                        .await?;
                    drop(stream);

                    tracing::info!(?run_id, "closed connected results responder");
                }
                ClientResponder::Disconnected {
                    results,
                    buffer_size: _,
                } => {
                    tracing::warn!(?run_id, "dropping disconnected results responder");

                    // Unfortunately, since we still don't have an active connection to the abq
                    // client, we can't send any buffered test results or end-of-tests anywhere.
                    // To avoid leaking memory we assume the client is dead and drop the results.
                    //
                    // TODO: rather than dropping any pending results, consider attaching any
                    // pending results back to the queue. If the client re-connects, send them all
                    // back at that point. The queue can run a weep to cleanup any uncolleted
                    // results on some time interval.
                    let _ = results;
                }
            }

            state_cache.lock().unwrap().remove(&run_id);
            queues.lock().unwrap().mark_complete(run_id, is_successful);
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(queues, stream))]
    async fn handle_total_run_result_request(
        queues: SharedRunQueues,
        run_id: RunId,
        mut stream: Box<dyn net_async::ServerStream>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let response = {
            let queues = queues.lock().unwrap();
            let run_state = queues
                .get_run_state(&run_id)
                .ok_or("Invalid state: worker is requesting run result for non-existent ID")?;

            match run_state {
                RunState::WaitingForFirstWorker | RunState::WaitingForManifest => return Err(
                    "Invalid state: worker is requesting run result before any tests have been run"
                        .into(),
                ),
                RunState::HasWork { .. } => net_protocol::queue::TotalRunResult::Pending,
                RunState::Done { succeeded } => net_protocol::queue::TotalRunResult::Completed {
                    succeeded: *succeeded,
                },
            }
        };

        net_protocol::async_write(&mut stream, &response).await?;

        Ok(())
    }
}

/// Sends work as workers request it.
/// This does not schedule work in any interesting way today, but it may in the future.
struct WorkScheduler {
    queues: SharedRunQueues,
}

/// An error that happens in the construction or execution of the work-scheduling server.
///
/// Does not include errors in the handling of requests to the server, but does include errors in
/// the acception or dispatch of connections.
#[derive(Debug, Error)]
pub enum WorkSchedulerError {
    /// An IO-related error.
    #[error("{0}")]
    Io(#[from] io::Error),

    /// Any other opaque error that occured.
    #[error("{0}")]
    Other(#[from] Box<dyn Error + Send + Sync>),
}

#[derive(Clone)]
struct SchedulerCtx {
    queues: SharedRunQueues,
    handshake_ctx: Arc<Box<dyn net_async::ServerHandshakeCtx>>,
}

impl WorkScheduler {
    fn start_on(
        self,
        listener: Box<dyn net::ServerListener>,
        mut shutdown: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<(), WorkSchedulerError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let server_result: Result<(), WorkSchedulerError> = rt.block_on(async {
            let listener = listener.into_async()?;

            let Self { queues } = self;
            let ctx = SchedulerCtx {
                queues,
                handshake_ctx: Arc::new(listener.handshake_ctx()),
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
                    _ = shutdown.recv() => {
                        break;
                    }
                };

                tokio::spawn({
                    let ctx = ctx.clone();
                    async move {
                        let result = Self::handle_work_conn(ctx, client).await;
                        if let Err(err) = result {
                            tracing::error!("error handling connection: {:?}", err);
                        }
                    }
                });
            }

            Ok(())
        });

        server_result
    }

    #[instrument(level = "trace", skip(ctx, stream))]
    async fn handle_work_conn(
        ctx: SchedulerCtx,
        stream: net_async::UnverifiedServerStream,
    ) -> io::Result<()> {
        let mut stream = ctx.handshake_ctx.handshake(stream).await?;

        // Get the run this worker wants work for.
        let request: WorkServerRequest = net_protocol::async_read(&mut stream).await?;

        match request {
            WorkServerRequest::HealthCheck => {
                let write_result =
                    net_protocol::async_write(&mut stream, &net_protocol::health::HEALTHY).await;
                if let Err(err) = write_result {
                    tracing::debug!("error sending health check: {}", err.to_string());
                }
            }
            WorkServerRequest::NextTest { run_id } => {
                // Pull the next bundle of work.
                let opt_work_bundle = { ctx.queues.lock().unwrap().next_work(run_id.clone()) };

                use net_protocol::work_server::NextTestResponse;
                let response = match opt_work_bundle {
                    Ok(bundle) => NextTestResponse::Bundle(bundle),
                    Err(WaitingForManifestError) => {
                        // Nothing yet; have the worker ask again later.
                        NextTestResponse::WaitingForManifest
                    }
                };

                net_protocol::async_write(&mut stream, &response).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        io,
        net::SocketAddr,
        num::NonZeroU64,
        sync::Arc,
        thread::{self, JoinHandle},
        time::Duration,
    };

    use super::{Abq, RunStateCache};
    use crate::{
        invoke::{Client, InvocationError, TestResultError, DEFAULT_CLIENT_POLL_TIMEOUT},
        queue::{
            ActiveRunResponders, BufferedResults, ClientReconnectionError, ClientResponder,
            QueueServer, RunQueues, SharedRunQueues, WorkScheduler,
        },
    };
    use abq_utils::{
        auth::{AuthToken, ClientAuthStrategy, ServerAuthStrategy},
        net_async,
        net_opt::{ClientOptions, ServerOptions, Tls},
        net_protocol::{
            self,
            entity::EntityId,
            runners::{Manifest, ManifestMessage, Status, Test, TestOrGroup, TestResult},
            workers::{NativeTestRunnerParams, RunId, RunnerKind, TestLikeRunner, WorkId},
        },
    };
    use abq_workers::{
        negotiate::{NegotiatedWorkers, WorkersConfig, WorkersNegotiator},
        workers::{WorkerContext, WorkersExit},
    };
    use futures::future;
    use ntest::timeout;
    use tokio::sync::Mutex;
    use tracing_test::{internal::logs_with_scope_contain, traced_test};

    fn sort_results(results: &mut [(String, TestResult)]) -> Vec<&str> {
        let mut results = results
            .iter()
            .map(|(_id, result)| result.output.as_ref().unwrap().as_str())
            .collect::<Vec<_>>();
        results.sort_unstable();
        results
    }

    fn echo_test(echo_msg: String) -> TestOrGroup {
        TestOrGroup::Test(Test {
            id: echo_msg,
            tags: Default::default(),
            meta: Default::default(),
        })
    }

    fn fake_test_result() -> TestResult {
        TestResult {
            status: Status::Success,
            id: "".to_string(),
            display_name: "".to_string(),
            output: None,
            runtime: 0.,
            meta: Default::default(),
        }
    }

    fn one_nonzero() -> NonZeroU64 {
        1.try_into().unwrap()
    }

    async fn accept_handshake(
        listener: &dyn net_async::ServerListener,
    ) -> io::Result<(Box<dyn net_async::ServerStream>, SocketAddr)> {
        let (unverified, addr) = listener.accept().await?;
        let stream = listener.handshake_ctx().handshake(unverified).await?;
        Ok((stream, addr))
    }

    fn make_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn empty_manifest() -> ManifestMessage {
        ManifestMessage {
            manifest: Manifest { members: vec![] },
        }
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_jobs_complete() {
        let mut queue = Abq::start(Default::default());

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                ],
            },
        };

        let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest);

        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(1),
        };

        let queue_server_addr = queue.server_addr();

        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let run_id = RunId::unique();
        let results_handle = thread::spawn({
            let run_id = run_id.clone();
            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                runtime.block_on(async {
                    let mut results = Vec::default();
                    let client = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id,
                        runner,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();
                    client
                        .stream_results(|id, result| {
                            results.push((id.0, result));
                        })
                        .await
                        .unwrap();
                    results
                })
            }
        });

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            client_opts,
            run_id,
        )
        .unwrap();

        let mut results = results_handle.join().unwrap();

        let exit = workers.shutdown();
        assert!(matches!(exit, WorkersExit::Success));
        queue.shutdown().unwrap();

        let results = sort_results(&mut results);

        assert_eq!(results, [("echo1"), ("echo2")]);
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_invokers() {
        let mut queue = Abq::start(Default::default());

        let manifest1 = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                ],
            },
        };

        let runner1 = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest1);

        let manifest2 = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo3".to_string()),
                    echo_test("echo4".to_string()),
                    echo_test("echo5".to_string()),
                ],
            },
        };

        let runner2 = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest2);

        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(1),
        };

        let queue_server_addr = queue.server_addr();

        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let run1 = RunId::unique();
        let run2 = RunId::unique();
        let results_handle = thread::spawn({
            let (run1, run2) = (run1.clone(), run2.clone());

            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                runtime.block_on(async {
                    let mut results1 = Vec::default();
                    let mut results2 = Vec::default();

                    let client1 = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run1,
                        runner1,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();
                    let client2 = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run2,
                        runner2,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();

                    let fut1 = client1.stream_results(|id, result| {
                        results1.push((id.0, result));
                    });
                    let fut2 = client2.stream_results(|id, result| {
                        results2.push((id.0, result));
                    });

                    let (res1, res2) = future::join(fut1, fut2).await;
                    res1.unwrap();
                    res2.unwrap();

                    (results1, results2)
                })
            }
        });

        let mut workers_for_run1 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config.clone(),
            queue.get_negotiator_handle(),
            client_opts,
            run1,
        )
        .unwrap();

        let mut workers_for_run2 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            client_opts,
            run2,
        )
        .unwrap();

        let (mut results1, mut results2) = results_handle.join().unwrap();

        let exit1 = workers_for_run1.shutdown();
        assert!(matches!(exit1, WorkersExit::Success));
        let exit2 = workers_for_run2.shutdown();
        assert!(matches!(exit2, WorkersExit::Success));

        queue.shutdown().unwrap();

        let results1 = sort_results(&mut results1);
        let results2 = sort_results(&mut results2);

        assert_eq!(results1, [("echo1"), ("echo2")]);
        assert_eq!(results2, [("echo3"), ("echo4"), "echo5"]);
    }

    // TODO write some tests that smoke over # of workers and batch sizes
    #[test]
    #[timeout(1000)] // 1 second
    fn batch_two_requests_at_a_time() {
        let batch_size = 2.try_into().unwrap();

        let mut queue = Abq::start(Default::default());

        let manifest1 = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                    echo_test("echo3".to_string()),
                    echo_test("echo4".to_string()),
                ],
            },
        };

        let runner1 = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest1);

        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(1),
        };

        let queue_server_addr = queue.server_addr();

        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let run = RunId::unique();
        let results_handle = thread::spawn({
            let run = run.clone();

            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                runtime.block_on(async {
                    let mut results = Vec::default();

                    let client = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run,
                        runner1,
                        batch_size,
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();

                    client
                        .stream_results(|id, result| {
                            results.push((id.0, result));
                        })
                        .await
                        .unwrap();

                    results
                })
            }
        });

        let mut workers_for_run1 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            client_opts,
            run,
        )
        .unwrap();

        let mut results = results_handle.join().unwrap();

        let exit1 = workers_for_run1.shutdown();
        assert!(matches!(exit1, WorkersExit::Success));

        queue.shutdown().unwrap();

        let results = sort_results(&mut results);

        assert_eq!(results, ["echo1", "echo2", "echo3", "echo4"]);
    }

    #[test]
    #[traced_test]
    fn bad_message_doesnt_take_down_server() {
        let server = QueueServer::new(Default::default(), "0.0.0.0:0".parse().unwrap());

        let server_opts = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();

        logs_with_scope_contain("", "error handling connection");
    }

    #[tokio::test]
    async fn invoker_reconnection_succeeds() {
        let run_id = RunId::unique();
        let active_runs = ActiveRunResponders::default();

        let server_opts = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        // Set up an initial connection for streaming test results targetting the given run ID
        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        {
            // Register the initial connection
            let (client_res, server_res) = futures::join!(
                client.connect(fake_server_addr),
                accept_handshake(&*fake_server)
            );
            let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let mut active_runs = active_runs.write().await;
            active_runs.insert(
                run_id.clone(),
                Mutex::new(ClientResponder::DirectStream {
                    stream: server_conn,
                    buffer: BufferedResults::new(5),
                }),
            );
            // Drop the connection
            drop(client_conn);
        };

        // Attempt to reconnect
        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());
        let new_conn_addr = client_conn.local_addr().unwrap();

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_runs),
            EntityId::new(),
            server_conn,
            run_id.clone(),
        )
        .await;

        // Validate that the reconnection was granted
        assert!(reconnection_result.is_ok());
        let active_conn_addr = {
            let mut active_runs = active_runs.write().await;
            let responder = active_runs.remove(&run_id).unwrap().into_inner();
            match responder {
                ClientResponder::DirectStream { stream, .. } => stream.peer_addr().unwrap(),
                ClientResponder::Disconnected { .. } => unreachable!(),
            }
        };
        assert_eq!(active_conn_addr, new_conn_addr);
    }

    #[tokio::test]
    async fn invoker_reconnection_fails_never_invoked() {
        let run_id = RunId::unique();
        let active_runs = ActiveRunResponders::default();

        let server_opts = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (_client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_runs),
            EntityId::new(),
            server_conn,
            run_id.clone(),
        )
        .await;

        // Validate that the reconnection was granted
        assert!(reconnection_result.is_err());
        let err = reconnection_result.unwrap_err();
        assert_eq!(err, ClientReconnectionError::NeverInvoked(run_id));
    }

    #[tokio::test]
    async fn client_disconnect_then_connect_gets_buffered_results() {
        let run_id = RunId::unique();
        let active_runs = ActiveRunResponders::default();
        let run_queues = SharedRunQueues::default();
        let run_state = RunStateCache::default();

        let client_entity = EntityId::new();

        // Pretend we have infinite work so the queue always streams back results to the client.
        run_state.lock().unwrap().insert(
            run_id.clone(),
            super::ActiveRunState {
                work_left: usize::MAX,
                is_successful: true,
            },
        );

        let server_opts = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let fake_server = server_opts.bind_async("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = client_opts.build_async().unwrap();

        {
            // Register the initial connection
            let (client_res, server_res) = futures::join!(
                client.connect(fake_server_addr),
                accept_handshake(&*fake_server)
            );
            let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let mut active_runs = active_runs.write().await;
            let _responder = active_runs.insert(
                run_id.clone(),
                Mutex::new(ClientResponder::DirectStream {
                    stream: server_conn,
                    buffer: BufferedResults::new(0),
                }),
            );

            // Drop the client connection
            drop(client_conn);
        };

        let buffered_work_id = WorkId("test1".to_string());
        {
            // Send a test result, will force the responder to go into disconnected mode
            QueueServer::handle_worker_result(
                Arc::clone(&run_queues),
                Arc::clone(&active_runs),
                Arc::clone(&run_state),
                EntityId::new(),
                run_id.clone(),
                buffered_work_id.clone(),
                fake_test_result(),
            )
            .await
            .unwrap();

            // Check the internal state of the responder - it should be in disconnected mode.
            let responders = active_runs.write().await;
            let responder = responders.get(&run_id).unwrap().lock().await;
            match &*responder {
                ClientResponder::Disconnected { results, .. } => {
                    assert_eq!(results.len(), 1)
                }
                _ => unreachable!(),
            }
        }

        // Reconnect back to the queue
        let (client_res, server_res) = futures::join!(
            client.connect(fake_server_addr),
            accept_handshake(&*fake_server)
        );
        let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let mut client = Client {
            entity: client_entity,
            abq_server_addr: fake_server_addr,
            client: client_opts.build_async().unwrap(),
            run_id: run_id.clone(),
            stream: client_conn,
            poll_timeout: DEFAULT_CLIENT_POLL_TIMEOUT,
        };
        {
            let reconnection_future = tokio::spawn(QueueServer::handle_invoker_reconnection(
                Arc::clone(&active_runs),
                EntityId::new(),
                server_conn,
                run_id.clone(),
            ));

            let (work_id, _) = client.next().await.unwrap().unwrap().pop().unwrap();
            assert_eq!(work_id, buffered_work_id);

            let reconnection_result = reconnection_future.await.unwrap();
            assert!(reconnection_result.is_ok());
        }

        {
            // Make sure that new test results also get send to the new client connectiion.
            let second_work_id = WorkId("test2".to_string());
            let worker_result_future = tokio::spawn(QueueServer::handle_worker_result(
                run_queues,
                Arc::clone(&active_runs),
                run_state,
                EntityId::new(),
                run_id,
                second_work_id.clone(),
                fake_test_result(),
            ));

            let (work_id, _) = client.next().await.unwrap().unwrap().pop().unwrap();
            assert_eq!(work_id, second_work_id);

            worker_result_future.await.unwrap().unwrap();
        }
    }

    #[test]
    fn queue_server_healthcheck() {
        let server_opts = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let server = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::mpsc::channel(1);

        let queue_server = QueueServer {
            queues: Default::default(),
            public_negotiator_addr: "0.0.0.0:0".parse().unwrap(),
        };
        let queue_handle = thread::spawn(|| {
            queue_server.start_on(server, server_shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();

        net_protocol::write(
            &mut conn,
            net_protocol::queue::Request {
                entity: EntityId::new(),
                message: net_protocol::queue::Message::HealthCheck,
            },
        )
        .unwrap();
        let health_msg: net_protocol::health::HEALTH = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::HEALTHY);

        server_shutdown_tx.blocking_send(()).unwrap();
        queue_handle.join().unwrap();
    }

    #[test]
    fn work_server_healthcheck() {
        let server_opts = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let server = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::mpsc::channel(1);

        let work_scheduler = WorkScheduler {
            queues: Default::default(),
        };
        let work_server_handle = thread::spawn(|| {
            work_scheduler.start_on(server, server_shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            net_protocol::work_server::WorkServerRequest::HealthCheck,
        )
        .unwrap();
        let health_msg: net_protocol::health::HEALTH = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::HEALTHY);

        server_shutdown_tx.blocking_send(()).unwrap();
        work_server_handle.join().unwrap();
    }

    #[test]
    #[traced_test]
    fn bad_message_doesnt_take_down_work_scheduling_server() {
        let server_opts = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let server = WorkScheduler {
            queues: Default::default(),
        };

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();

        logs_with_scope_contain("", "error handling connection");
    }

    #[test]
    #[traced_test]
    fn connecting_to_queue_server_with_auth_okay() {
        let negotiator_addr = "0.0.0.0:0".parse().unwrap();
        let server = QueueServer::new(Default::default(), negotiator_addr);

        let (server_auth, client_auth) = AuthToken::new_random().build_strategies();
        let server_opts = ServerOptions::new(server_auth, Tls::NO);
        let client_opts = ClientOptions::new(client_auth, Tls::NO);

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            net_protocol::queue::Request {
                entity: EntityId::new(),
                message: net_protocol::queue::Message::NegotiatorAddr,
            },
        )
        .unwrap();

        let recv_negotiator_addr = net_protocol::read(&mut conn).unwrap();
        assert_eq!(negotiator_addr, recv_negotiator_addr);

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    #[traced_test]
    fn connecting_to_queue_server_with_no_auth_fails() {
        let negotiator_addr = "0.0.0.0:0".parse().unwrap();
        let server = QueueServer::new(Default::default(), negotiator_addr);

        let (server_auth, _) = AuthToken::new_random().build_strategies();

        let server_opts = ServerOptions::new(server_auth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            net_protocol::queue::Request {
                entity: EntityId::new(),
                message: net_protocol::queue::Message::NegotiatorAddr,
            },
        )
        .unwrap();

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();

        logs_with_scope_contain("", "error handling connection");
    }

    #[test]
    #[traced_test]
    fn connecting_to_work_server_with_auth_okay() {
        let server = WorkScheduler {
            queues: Default::default(),
        };

        let (server_auth, client_auth) = AuthToken::new_random().build_strategies();
        let server_opts = ServerOptions::new(server_auth, Tls::NO);
        let client_opts = ClientOptions::new(client_auth, Tls::NO);

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            net_protocol::work_server::WorkServerRequest::HealthCheck,
        )
        .unwrap();

        let health_msg: net_protocol::health::HEALTH = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::HEALTHY);

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    #[traced_test]
    fn connecting_to_work_server_with_no_auth_fails() {
        let server = WorkScheduler {
            queues: Default::default(),
        };

        let (server_auth, _) = AuthToken::new_random().build_strategies();
        let server_opts = ServerOptions::new(server_auth, Tls::NO);
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = client_opts.build().unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(
            &mut conn,
            net_protocol::work_server::WorkServerRequest::HealthCheck,
        )
        .unwrap();

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();

        logs_with_scope_contain("", "error handling connection");
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn worker_exits_with_failure_if_test_fails() {
        let mut queue = Abq::start(Default::default());

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![echo_test("echo1".to_string())],
            },
        };

        // Set up the runner so that it times out, always issuing an error.
        let runner = RunnerKind::TestLikeRunner(TestLikeRunner::InduceTimeout, manifest);
        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(0),
        };

        let queue_server_addr = queue.server_addr();
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);
        let run_id = RunId::unique();
        let results_handle = thread::spawn({
            let run_id = run_id.clone();

            move || {
                let runtime = make_runtime();

                runtime.block_on(async {
                    let client = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id,
                        runner,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();
                    client.stream_results(|_, _| {}).await.unwrap();
                })
            }
        });

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            client_opts,
            run_id,
        )
        .unwrap();

        results_handle.join().unwrap();

        let exit = workers.shutdown();
        assert!(matches!(exit, WorkersExit::Failure));

        queue.shutdown().unwrap();
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_worker_sets_all_exit_with_failure_if_any_test_fails() {
        let mut queue = Abq::start(Default::default());

        // NOTE: we set up two workers and unleash them on the following manifest, setting both
        // workers to fail only on the `echo_fail` case. That way, no matter which one picks it up,
        // we should observe a failure for both workers sets.

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                    echo_test("echo_fail".to_string()),
                    echo_test("echo3".to_string()),
                    echo_test("echo4".to_string()),
                ],
            },
        };

        let runner = RunnerKind::TestLikeRunner(
            TestLikeRunner::FailOnTestName("echo_fail".to_string()),
            manifest,
        );
        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(0),
        };

        let queue_server_addr = queue.server_addr();
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);
        let run_id = RunId::unique();
        let results_handle = thread::spawn({
            let run_id = run_id.clone();

            move || {
                let runtime = make_runtime();

                runtime.block_on(async {
                    let client = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id,
                        runner,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();
                    client.stream_results(|_, _| {}).await.unwrap();
                })
            }
        });

        // NOTE: it is very possible that workers1 completes all the tests before workers2 gets to
        // start, because we are not controlling the order tests are delivered across the network
        // here. However, the result should be the same no matter what, so this test should never
        // flake.
        //
        // In the future, we'll probably want to test different networking order conditions. See
        // also https://github.com/rwx-research/abq/issues/29.
        let mut workers1 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config.clone(),
            queue.get_negotiator_handle(),
            client_opts,
            run_id.clone(),
        )
        .unwrap();
        let mut workers2 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            client_opts,
            run_id,
        )
        .unwrap();

        results_handle.join().unwrap();

        let exit1 = workers1.shutdown();
        assert!(matches!(exit1, WorkersExit::Failure));
        let exit2 = workers2.shutdown();
        assert!(matches!(exit2, WorkersExit::Failure));

        queue.shutdown().unwrap();
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn invoke_work_with_duplicate_id_is_an_error() {
        let queue = Abq::start(Default::default());

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                ],
            },
        };

        let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest);
        let queue_server_addr = queue.server_addr();

        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let run_id = RunId::unique();

        let handle = thread::spawn({
            let run_id = run_id.clone();
            move || {
                make_runtime().block_on(async {
                    // Start one client with the run ID
                    let _client1 = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id.clone(),
                        runner.clone(),
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();

                    // Start a second, with the same run ID
                    let client2_result = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id.clone(),
                        runner,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await;

                    match client2_result {
                        Err(err) => err,
                        Ok(_) => unreachable!(),
                    }
                })
            }
        });

        match handle.join().unwrap() {
            InvocationError::DuplicateRun(run) => assert_eq!(run, run_id),
            _ => unreachable!(),
        }
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn invoke_work_with_duplicate_id_after_completion_is_an_error() {
        let mut queue = Abq::start(Default::default());

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![echo_test("echo1".to_string())],
            },
        };

        let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest);
        let workers_config = WorkersConfig {
            num_workers: 1.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(0),
        };

        let queue_server_addr = queue.server_addr();
        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);
        let run_id = RunId::unique();
        let results_handle = thread::spawn({
            let run_id = run_id.clone();

            move || {
                make_runtime().block_on(async {
                    // Start one client, and have it drain its test queue
                    let client = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id.clone(),
                        runner.clone(),
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();

                    client.stream_results(|_, _| {}).await.unwrap();

                    // Start a second, with the same run ID
                    let client2_result = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id,
                        runner,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await;

                    match client2_result {
                        Err(err) => err,
                        Ok(_) => unreachable!(),
                    }
                })
            }
        });

        // Start the workers for the first run
        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            client_opts,
            run_id.clone(),
        )
        .unwrap();

        let invocation_error = results_handle.join().unwrap();

        match invocation_error {
            InvocationError::DuplicateCompletedRun(run) => assert_eq!(run, run_id),
            _ => unreachable!(),
        }

        let _ = workers.shutdown();
        queue.shutdown().unwrap();
    }

    fn create_work_scheduler(
        server_auth: ServerAuthStrategy,
        queues: SharedRunQueues,
    ) -> (JoinHandle<()>, tokio::sync::mpsc::Sender<()>, SocketAddr) {
        let server = WorkScheduler { queues };
        let server_opts = ServerOptions::new(server_auth, Tls::NO);

        let listener = server_opts.bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        (server_thread, shutdown_tx, server_addr)
    }

    #[test]
    #[traced_test]
    fn get_work_from_work_server_waiting_for_first_worker() {
        let (server_auth, client_auth) = AuthToken::new_random().build_strategies();

        let run_id = RunId::unique();

        // Set up the queue so that a run ID is invoked, but no worker has connected yet.
        let mut queues = RunQueues::default();
        queues
            .create_queue(
                run_id.clone(),
                RunnerKind::TestLikeRunner(
                    TestLikeRunner::Echo,
                    ManifestMessage {
                        manifest: Manifest { members: vec![] },
                    },
                ),
                one_nonzero(),
            )
            .unwrap();

        let (server_thread, server_shutdown, server_addr) =
            create_work_scheduler(server_auth, Arc::new(std::sync::Mutex::new(queues)));

        let client_opts = ClientOptions::new(client_auth, Tls::NO);
        let client = client_opts.build().unwrap();

        let mut conn = client.connect(server_addr).unwrap();

        use net_protocol::work_server::{NextTestResponse, WorkServerRequest};

        // Ask the server for the next test; we should be told a manifest is still TBD.
        net_protocol::write(&mut conn, WorkServerRequest::NextTest { run_id }).unwrap();

        let response: NextTestResponse = net_protocol::read(&mut conn).unwrap();

        assert!(matches!(response, NextTestResponse::WaitingForManifest));

        server_shutdown.blocking_send(()).unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    #[traced_test]
    fn get_work_from_work_server_waiting_for_manifest() {
        let (server_auth, client_auth) = AuthToken::new_random().build_strategies();

        let run_id = RunId::unique();

        // Set up the queue so a run ID is invoked, but the manifest is still unknown.
        let mut queues = RunQueues::default();
        queues
            .create_queue(
                run_id.clone(),
                RunnerKind::TestLikeRunner(
                    TestLikeRunner::Echo,
                    ManifestMessage {
                        manifest: Manifest { members: vec![] },
                    },
                ),
                one_nonzero(),
            )
            .unwrap();
        // Get the run for a worker; since this is the first one to ask, it will be told to grab
        // the manifest.
        let _ = queues.get_run_for_worker(&run_id);

        let (server_thread, server_shutdown, server_addr) =
            create_work_scheduler(server_auth, Arc::new(std::sync::Mutex::new(queues)));

        let client_opts = ClientOptions::new(client_auth, Tls::NO);
        let client = client_opts.build().unwrap();

        let mut conn = client.connect(server_addr).unwrap();

        use net_protocol::work_server::{NextTestResponse, WorkServerRequest};

        // Ask the server for the next test; we should be told a manifest is still TBD.
        net_protocol::write(&mut conn, WorkServerRequest::NextTest { run_id }).unwrap();

        let response: NextTestResponse = net_protocol::read(&mut conn).unwrap();

        assert!(matches!(response, NextTestResponse::WaitingForManifest));

        server_shutdown.blocking_send(()).unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    fn getting_run_after_work_is_complete_returns_nothing() {
        let mut queue = Abq::start(Default::default());

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                ],
            },
        };

        let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest);

        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(1),
        };

        let queue_server_addr = queue.server_addr();

        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let run_id = RunId::unique();

        // First off, run the test suite to completion. It should complete successfully.
        {
            let results_handle = thread::spawn({
                let run_id = run_id.clone();
                move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();

                    runtime.block_on(async {
                        let mut results = Vec::default();
                        let client = Client::invoke_work(
                            EntityId::new(),
                            queue_server_addr,
                            client_opts,
                            run_id,
                            runner,
                            one_nonzero(),
                            DEFAULT_CLIENT_POLL_TIMEOUT,
                        )
                        .await
                        .unwrap();
                        client
                            .stream_results(|id, result| {
                                results.push((id.0, result));
                            })
                            .await
                            .unwrap();
                        results
                    })
                }
            });

            let mut workers = WorkersNegotiator::negotiate_and_start_pool(
                workers_config.clone(),
                queue.get_negotiator_handle(),
                client_opts,
                run_id.clone(),
            )
            .unwrap();

            let mut results = results_handle.join().unwrap();

            let exit = workers.shutdown();
            assert!(matches!(exit, WorkersExit::Success));

            let results = sort_results(&mut results);

            assert_eq!(results, [("echo1"), ("echo2")]);
        }

        // Now, simulate a new set of workers connecting again. Negotiation should succeed, but
        // they should be marked as redundant.
        {
            let mut new_workers = WorkersNegotiator::negotiate_and_start_pool(
                workers_config,
                queue.get_negotiator_handle(),
                client_opts,
                run_id,
            )
            .unwrap();

            assert!(matches!(
                new_workers,
                NegotiatedWorkers::Redundant { success: true }
            ));

            let exit = new_workers.shutdown();
            assert!(matches!(exit, WorkersExit::Success));
        }

        queue.shutdown().unwrap();
    }

    #[test]
    fn failure_to_run_worker_command_exits_gracefully() {
        let mut queue = Abq::start(Default::default());

        let runner = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
            cmd: "__zzz_not_a_command__".to_string(),
            args: Default::default(),
            extra_env: Default::default(),
        });

        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(1),
        };

        let queue_server_addr = queue.server_addr();

        let client_opts = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO);

        let run_id = RunId::unique();

        let results_handle = thread::spawn({
            let run_id = run_id.clone();
            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                runtime.block_on(async {
                    let client = Client::invoke_work(
                        EntityId::new(),
                        queue_server_addr,
                        client_opts,
                        run_id,
                        runner,
                        one_nonzero(),
                        DEFAULT_CLIENT_POLL_TIMEOUT,
                    )
                    .await
                    .unwrap();

                    client.stream_results(|_, _| {}).await.unwrap_err()
                })
            }
        });

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            client_opts,
            run_id,
        )
        .unwrap();

        let results_error = results_handle.join().unwrap();

        assert!(matches!(
            results_error,
            TestResultError::TestCommandError(..)
        ));

        let exit = workers.shutdown();
        assert!(matches!(exit, WorkersExit::Failure));

        queue.shutdown().unwrap();
    }

    #[test]
    fn no_active_runs_when_non_enqueued() {
        let queues = RunQueues::default();

        assert_eq!(queues.num_active_runs(), 0);
    }

    #[test]
    fn active_runs_when_some_waiting_on_workers() {
        let mut queues = RunQueues::default();

        queues
            .create_queue(
                RunId::unique(),
                RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest()),
                one_nonzero(),
            )
            .unwrap();

        assert_eq!(queues.num_active_runs(), 1);
    }

    #[test]
    fn active_runs_when_some_waiting_on_manifest() {
        let mut queues = RunQueues::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest()),
                one_nonzero(),
            )
            .unwrap();

        queues.get_run_for_worker(&run_id);

        assert_eq!(queues.num_active_runs(), 1);
    }

    #[test]
    fn active_runs_when_running() {
        let mut queues = RunQueues::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest()),
                one_nonzero(),
            )
            .unwrap();

        queues.get_run_for_worker(&run_id);
        queues.add_manifest(run_id, vec![]);

        assert_eq!(queues.num_active_runs(), 1);
    }

    #[test]
    fn active_runs_when_all_done() {
        let mut queues = RunQueues::default();

        let run_id = RunId::unique();

        queues
            .create_queue(
                run_id.clone(),
                RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest()),
                one_nonzero(),
            )
            .unwrap();

        queues.get_run_for_worker(&run_id);
        queues.add_manifest(run_id.clone(), vec![]);
        queues.mark_complete(run_id, true);

        assert_eq!(queues.num_active_runs(), 0);
    }

    #[test]
    fn active_runs_multiple_in_various_states() {
        let mut queues = RunQueues::default();

        let run_id1 = RunId::unique(); // DONE
        let run_id2 = RunId::unique(); // DONE
        let run_id3 = RunId::unique(); // WAITING
        let run_id4 = RunId::unique(); // RUNNING
        let expected_active = 2;

        for run_id in [&run_id1, &run_id2, &run_id3, &run_id4] {
            queues
                .create_queue(
                    run_id.clone(),
                    RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest()),
                    one_nonzero(),
                )
                .unwrap();

            queues.get_run_for_worker(run_id);
        }

        for run_id in [&run_id1, &run_id2, &run_id4] {
            queues.add_manifest(run_id.clone(), vec![]);
        }

        for run_id in [run_id1, run_id2] {
            queues.mark_complete(run_id, true);
        }

        assert_eq!(queues.num_active_runs(), expected_active);
    }
}
