use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use abq_utils::auth::ServerAuthStrategy;
use abq_utils::flatten_manifest;
use abq_utils::net;
use abq_utils::net_async;
use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::publicize_addr;
use abq_utils::net_protocol::queue::{InvokerResponse, Request};
use abq_utils::net_protocol::runners::{ManifestMessage, TestCase, TestResult};
use abq_utils::net_protocol::work_server::WorkServerRequest;
use abq_utils::net_protocol::workers::{RunnerKind, WorkContext};
use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, Message},
    workers::{InvocationId, NextWork, WorkId},
};
use abq_workers::negotiate::{AssignedRun, QueueNegotiator, QueueNegotiatorHandle};

use thiserror::Error;
use tracing::instrument;

// TODO: we probably want something more sophisticated here, in particular a concurrent
// work-stealing queue.
#[derive(Default)]
struct JobQueue {
    queue: VecDeque<NextWork>,
}

impl JobQueue {
    pub fn add_batch_work(&mut self, work: impl Iterator<Item = NextWork>) {
        self.queue.extend(work);
    }

    pub fn get_work(&mut self) -> Option<NextWork> {
        let work = self.queue.pop_front()?;
        Some(work)
    }
}

enum InvocationState {
    WaitingForFirstWorker,
    WaitingForManifest,
    HasWork(JobQueue),
    Done,
}

struct InvocationData {
    runner: RunnerKind,
}

struct WaitingForManifestError;

#[derive(Default)]
struct InvocationQueues {
    /// One queue per `abq test ...` invocation, at least for now.
    queues: HashMap<InvocationId, InvocationState>,
    invocation_data: HashMap<InvocationId, InvocationData>,
}

impl InvocationQueues {
    pub fn create_queue(&mut self, invocation_id: InvocationId, runner: RunnerKind) {
        let old_queue = self
            .queues
            .insert(invocation_id, InvocationState::WaitingForFirstWorker);
        debug_assert!(old_queue.is_none());

        let old_invocation_data = self
            .invocation_data
            .insert(invocation_id, InvocationData { runner });
        debug_assert!(old_invocation_data.is_none());
    }

    // Chooses a run for a set of workers to attach to. Returns `None` if an appropriate run is not
    // found.
    pub fn get_run_for_worker(&mut self, invocation_id: InvocationId) -> Option<AssignedRun> {
        let invocation_state = self.queues.get_mut(&invocation_id)?;

        let InvocationData { runner } = self.invocation_data.get(&invocation_id).unwrap();

        let assigned_run = match invocation_state {
            InvocationState::WaitingForFirstWorker => {
                // This is the first worker to attach; ask it to generate the manifest.
                *invocation_state = InvocationState::WaitingForManifest;
                AssignedRun {
                    invocation_id,
                    runner_kind: runner.clone(),
                    should_generate_manifest: true,
                }
            }
            _ => {
                // Otherwise we are already waiting for the manifest, or already have it; just tell
                // the worker what runner it should set up.
                AssignedRun {
                    invocation_id,
                    runner_kind: runner.clone(),
                    should_generate_manifest: false,
                }
            }
        };

        Some(assigned_run)
    }

    pub fn add_manifest(&mut self, invocation_id: InvocationId, flat_manifest: Vec<TestCase>) {
        let state = self
            .queues
            .get_mut(&invocation_id)
            .expect("no queue for invocation");

        let work_from_manifest = flat_manifest.into_iter().map(|test_case| {
            NextWork::Work {
                test_case,
                invocation_id,
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

        *state = InvocationState::HasWork(queue);
    }

    pub fn next_work(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<NextWork, WaitingForManifestError> {
        match self
            .queues
            .get_mut(&invocation_id)
            .expect("no queue state for invocation")
        {
            InvocationState::WaitingForFirstWorker => Err(WaitingForManifestError),
            InvocationState::WaitingForManifest => Err(WaitingForManifestError),
            InvocationState::HasWork(queue) => Ok(queue.get_work().unwrap_or(NextWork::EndOfWork)),
            InvocationState::Done => Ok(NextWork::EndOfWork),
        }
    }

    pub fn mark_complete(&mut self, invocation_id: InvocationId) {
        let state = self
            .queues
            .get_mut(&invocation_id)
            .expect("no queue state for invocation");
        match state {
            InvocationState::HasWork(queue) => {
                assert!(
                    queue.get_work().is_none(),
                    "Invalid state - queue is not complete!"
                );
            }
            InvocationState::WaitingForFirstWorker
            | InvocationState::WaitingForManifest
            | InvocationState::Done => {
                unreachable!("Invalid state");
            }
        }

        // TODO: right now, we are just marking something complete so that future workers asking
        // for work about the invocation get an EndOfWork message. But this leaks memory, how can
        // we avoid that?
        *state = InvocationState::Done;
        self.invocation_data.remove(&invocation_id);
    }
}

type SharedInvocationQueues = Arc<Mutex<InvocationQueues>>;

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
    /// How the queue should authenticate and authorize incoming connections.
    pub auth_strategy: ServerAuthStrategy,
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
            auth_strategy: ServerAuthStrategy::NoAuth,
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
        auth_strategy,
    } = config;

    let queues: SharedInvocationQueues = Default::default();

    let server_listener = net::ServerListener::bind(auth_strategy, (bind_ip, server_port)).unwrap();
    let server_addr = server_listener.local_addr().unwrap();

    let negotiator_listener =
        net::ServerListener::bind(auth_strategy, (bind_ip, negotiator_port)).unwrap();
    let negotiator_addr = negotiator_listener.local_addr().unwrap();
    let public_negotiator_addr = publicize_addr(negotiator_addr, public_ip);

    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::mpsc::channel(1);

    let server_handle = thread::spawn({
        let queue_server = QueueServer::new(Arc::clone(&queues), public_negotiator_addr);
        move || queue_server.start_on(server_listener, server_shutdown_rx)
    });

    let new_work_server = net::ServerListener::bind(auth_strategy, (bind_ip, work_port)).unwrap();
    let new_work_server_addr = new_work_server.local_addr().unwrap();

    let (work_scheduler_shutdown_tx, work_scheduler_shutdown_rx) = tokio::sync::mpsc::channel(1);

    let work_scheduler_handle = thread::spawn({
        let scheduler = WorkScheduler {
            queues: Arc::clone(&queues),
        };
        move || scheduler.start_on(new_work_server, work_scheduler_shutdown_rx)
    });

    // Chooses a test run for a set of workers to attach to.
    // This blocks until there is an invocation available for the particular worker.
    // TODO: blocking is not good; workers that can connect fast may be blocked on workers waiting
    // for a test run to be established. Make this cede control to a runtime if the worker's run is
    // not yet available.
    let choose_run_for_worker = move |wanted_invocation_id| loop {
        let opt_assigned = {
            queues
                .lock()
                .unwrap()
                .get_run_for_worker(wanted_invocation_id)
        };
        match opt_assigned {
            None => {
                // Nothing yet; release control and sleep for a bit in case something comes.
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Some(assigned) => {
                return assigned;
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

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

/// Handler for sending test results back to the abq client that issued a test run.
#[derive(Debug)]
enum ClientResponder {
    /// We have an open stream upon which to send back the test results.
    DirectStream(net_async::ServerStream),
    /// There is not yet an open connection we can stream the test results back on.
    /// This state may be reached during the time a client is disconnected.
    Disconnected {
        buffered_results: Vec<InvokerResponse>,
    },
}

impl ClientResponder {
    fn new(stream: net_async::ServerStream) -> Self {
        Self::DirectStream(stream)
    }

    async fn send_and_ack_one_test_result(&mut self, test_result: InvokerResponse) {
        use net_protocol::client::AckTestResult;

        match self {
            ClientResponder::DirectStream(conn) => {
                // Send the test result and wait for an ack. If either fails, suppose the client is
                // disconnected.
                let write_result = net_protocol::async_write(conn, &test_result).await;
                let client_disconnected = match write_result {
                    Err(_) => true,
                    Ok(()) => {
                        let ack = net_protocol::async_read(conn).await;
                        match ack {
                            Err(_) => true,
                            Ok(AckTestResult {}) => false,
                        }
                    }
                };

                if client_disconnected {
                    // Demote ourselves to the disconnected state until reconnection happens.
                    *self = Self::Disconnected {
                        buffered_results: vec![test_result],
                    }
                }
            }
            ClientResponder::Disconnected { buffered_results } => {
                buffered_results.push(test_result);
            }
        }
    }

    /// Updates the responder with a new connection. If there are any pending test results that
    /// failed to be sent from a previous connections, they are streamed to the new connection
    /// before the future returned from this function completes.
    async fn reconnect_with(&mut self, new_conn: net_async::ServerStream) {
        // There's no great way for us to check whether the existing client connection is,
        // in fact, closed. Instead we accept all faithful reconnection requests, and
        // assume that the client is well-behaved (it will not attempt to reconnect unless
        // it is explicitly disconnected).
        //
        // Even if we could detect closed connections at this point, we'd have an TOCTOU race -
        // it may be the case that a stream closes between the time that we check it is closed,
        // and when we issue an error for the reconnection attempt.

        let old_conn = {
            let mut new_stream = Self::DirectStream(new_conn);
            std::mem::swap(self, &mut new_stream);
            new_stream
        };

        match old_conn {
            ClientResponder::Disconnected { buffered_results } => {
                // We need to send back all the buffered results to the new stream.
                // Must send all test results in sequence, since each writes to the
                // same stream back to the client.
                for test_result in buffered_results {
                    self.send_and_ack_one_test_result(test_result).await;
                }
            }
            ClientResponder::DirectStream(_) => {
                // nothing more to do
            }
        }
    }
}

/// invocation -> (results sender, responder handle)
///
// TODO: certainly we can be smarter here, for example we have an invariant that a stream will only
// ever be added and removed for an invocation ID once. Can we use that to get rid of the mutex?
type ActiveInvocations = Arc<tokio::sync::Mutex<HashMap<InvocationId, ClientResponder>>>;

/// Cache of how much is left in the queue for a particular invocation, so that we don't have to
/// lock the queue all the time.
// TODO: consider using DashMap
type WorkLeftForInvocations = Arc<Mutex<HashMap<InvocationId, usize>>>;

/// Central server listening for new test run invocations and results.
struct QueueServer {
    queues: SharedInvocationQueues,
    /// When all results for a particular invocation are communicated, we want to make sure that the
    /// responder thread is closed and that the entry here is dropped.
    active_invocations: ActiveInvocations,
    work_left_for_invocations: WorkLeftForInvocations,

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
    /// The client sent an initial test invocation request to the queue.
    #[error("{0} was never used to invoke work on the queue")]
    NeverInvoked(InvocationId),
}

impl QueueServer {
    fn new(queues: SharedInvocationQueues, public_negotiator_addr: SocketAddr) -> Self {
        Self {
            queues,
            active_invocations: Default::default(),
            work_left_for_invocations: Default::default(),
            public_negotiator_addr,
        }
    }

    fn clone(&self) -> Self {
        Self {
            queues: Arc::clone(&self.queues),
            active_invocations: Arc::clone(&self.active_invocations),
            work_left_for_invocations: Arc::clone(&self.work_left_for_invocations),
            public_negotiator_addr: self.public_negotiator_addr,
        }
    }

    fn start_on(
        self,
        server_listener: net::ServerListener,
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
            let server_listener = net_async::ServerListener::from_sync(server_listener)?;

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
                    let this = self.clone();
                    async move {
                        let result = this.handle(client).await;
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

    #[inline(always)]
    async fn handle(
        self,
        mut stream: net_async::ServerStream,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let Request { entity, message } = net_protocol::async_read(&mut stream).await?;

        let result: Result<(), Box<dyn Error + Send + Sync>> = match message {
            Message::HealthCheck => {
                // TODO here and in other servers - healthchecks currently require auth, whenever
                // auth is configured. Should they?
                Self::handle_healthcheck(entity, stream).await
            }
            Message::NegotiatorAddr => {
                Self::handle_negotiator_addr(entity, stream, self.public_negotiator_addr).await
            }
            Message::InvokeWork(invoke_work) => {
                Self::handle_invoked_work(
                    self.active_invocations,
                    self.queues,
                    entity,
                    stream,
                    invoke_work,
                )
                .await
            }
            Message::Reconnect(invocation_id) => {
                Self::handle_invoker_reconnection(
                    self.active_invocations,
                    entity,
                    stream,
                    invocation_id,
                )
                .await
                // Upcast the reconnection error into a generic error
                .map_err(|e| Box::new(e) as _)
            }
            Message::Manifest(invocation_id, manifest) => Self::handle_manifest(
                self.queues,
                self.work_left_for_invocations,
                entity,
                invocation_id,
                manifest,
            ),
            Message::WorkerResult(invocation_id, work_id, result) => {
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
                    self.queues,
                    self.active_invocations,
                    self.work_left_for_invocations,
                    entity,
                    invocation_id,
                    work_id,
                    result,
                )
                .await
            }
        };

        result
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(stream))]
    async fn handle_healthcheck(
        entity: EntityId,
        mut stream: net_async::ServerStream,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Right now nothing interesting is owned by the queue, so nothing extra to check.
        net_protocol::async_write(&mut stream, &net_protocol::health::HEALTHY).await?;
        Ok(())
    }

    #[inline(always)]
    #[instrument(level = "trace", skip(stream, public_negotiator_addr))]
    async fn handle_negotiator_addr(
        entity: EntityId,
        mut stream: net_async::ServerStream,
        public_negotiator_addr: SocketAddr,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        net_protocol::async_write(&mut stream, &public_negotiator_addr).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(active_invocations, queues, stream))]
    async fn handle_invoked_work(
        active_invocations: ActiveInvocations,
        queues: SharedInvocationQueues,
        entity: EntityId,
        stream: net_async::ServerStream,
        invoke_work: InvokeWork,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let InvokeWork {
            invocation_id,
            runner,
        } = invoke_work;

        let results_responder = ClientResponder::new(stream);
        let old_responder = active_invocations
            .lock()
            .await
            .insert(invocation_id, results_responder);

        debug_assert!(
            old_responder.is_none(),
            "Existing stream for expected unique invocation id"
        );

        // Create a new work queue for this invocation.
        queues.lock().unwrap().create_queue(invocation_id, runner);

        Ok(())
    }

    #[instrument(level = "trace", skip(active_invocations, new_stream))]
    async fn handle_invoker_reconnection(
        active_invocations: ActiveInvocations,
        entity: EntityId,
        new_stream: net_async::ServerStream,
        invocation_id: InvocationId,
    ) -> Result<(), ClientReconnectionError> {
        use std::collections::hash_map::Entry;

        // When an abq client loses connection with the queue, they may attempt to reconnect.
        // We'll need to update the connection from streaming results to the client to the new one.
        let mut active_invocations = active_invocations.lock().await;
        let active_conn_entry = active_invocations.entry(invocation_id);
        match active_conn_entry {
            Entry::Occupied(mut occupied) => {
                let new_stream_addr = new_stream.peer_addr();

                occupied.get_mut().reconnect_with(new_stream).await;

                tracing::debug!(
                    "rerouted connection for {:?} to {:?}",
                    invocation_id,
                    new_stream_addr
                );

                Ok(())
            }
            Entry::Vacant(_) => {
                // There was never a connection for this invocation ID!
                Err(ClientReconnectionError::NeverInvoked(invocation_id))
            }
        }
    }

    #[instrument(level = "trace", skip(queues, work_left_for_invocations))]
    fn handle_manifest(
        queues: SharedInvocationQueues,
        work_left_for_invocations: WorkLeftForInvocations,
        entity: EntityId,
        invocation_id: InvocationId,
        manifest: ManifestMessage,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Record the manifest for this invocation in its appropriate queue.
        // TODO: actually record the manifest metadata
        let flat_manifest = flatten_manifest(manifest.manifest);
        work_left_for_invocations
            .lock()
            .unwrap()
            .insert(invocation_id, flat_manifest.len());

        queues
            .lock()
            .unwrap()
            .add_manifest(invocation_id, flat_manifest);

        Ok(())
    }

    #[instrument(
        level = "trace",
        skip(queues, active_invocations, work_left_for_invocations)
    )]
    async fn handle_worker_result(
        queues: SharedInvocationQueues,
        active_invocations: ActiveInvocations,
        work_left_for_invocations: WorkLeftForInvocations,
        entity: EntityId,
        invocation_id: InvocationId,
        work_id: WorkId,
        result: TestResult,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        {
            // First up, chuck the test result back over to `abq test`. Make sure we don't steal
            // the lock on active_invocations for any longer than it takes to send that request.
            let mut active_invocations = active_invocations.lock().await;

            let invoker_stream = active_invocations
                .get_mut(&invocation_id)
                .expect("invocation is not active");

            invoker_stream
                .send_and_ack_one_test_result(InvokerResponse::Result(work_id, result))
                .await;
        }

        let no_more_work = {
            // Update the amount of work we have left; again, steal the map of work for only as
            // long is it takes for us to do that.
            let mut work_left_for_invocations = work_left_for_invocations.lock().unwrap();
            let work_left = work_left_for_invocations
                .get_mut(&invocation_id)
                .expect("invocation id not recorded, but we got a result for it?");

            *work_left -= 1;
            *work_left == 0
        };

        if no_more_work {
            // Now, we have to take both the active invocations, and the map of work left, to mark
            // the work for the current invocation as complete in both cases. It's okay for this to
            // be slow(er), since it happens only once per test run.
            //
            // Note however that this does block requests indexing into the maps for other `abq
            // test` invocations.
            // We may want to use concurrent hashmaps that index into partitioned buckets for that
            // use case (e.g. DashMap).
            let responder = active_invocations
                .lock()
                .await
                .remove(&invocation_id)
                .unwrap();

            match responder {
                ClientResponder::DirectStream(mut stream) => {
                    net_protocol::async_write(&mut stream, &InvokerResponse::EndOfResults).await?;
                    tokio::io::AsyncWriteExt::shutdown(&mut stream).await?;
                }
                ClientResponder::Disconnected { buffered_results } => {
                    // Unfortunately, since we still don't have an active connection to the abq
                    // client, we can't send any buffered test results or end-of-tests anywhere.
                    // To avoid leaking memory we assume the client is dead and drop the results.
                    //
                    // TODO: rather than dropping any pending results, consider attaching any
                    // pending results back to the queue. If the client re-connects, send them all
                    // back at that point. The queue can run a weep to cleanup any uncolleted
                    // results on some time interval.
                    let _ = buffered_results;
                }
            }

            work_left_for_invocations
                .lock()
                .unwrap()
                .remove(&invocation_id);
            queues.lock().unwrap().mark_complete(invocation_id);
        }

        Ok(())
    }
}

/// Sends work as workers request it.
/// This does not schedule work in any interesting way today, but it may in the future.
struct WorkScheduler {
    queues: SharedInvocationQueues,
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

impl WorkScheduler {
    fn clone(&self) -> Self {
        Self {
            queues: Arc::clone(&self.queues),
        }
    }

    fn start_on(
        self,
        listener: net::ServerListener,
        mut shutdown: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<(), WorkSchedulerError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let server_result: Result<(), WorkSchedulerError> = rt.block_on(async {
            let listener = net_async::ServerListener::from_sync(listener)?;

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
                    let mut this = self.clone();
                    async move {
                        let result = this.handle_work_conn(client).await;
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

    #[instrument(level = "trace", skip(self, stream))]
    async fn handle_work_conn(&mut self, mut stream: net_async::ServerStream) -> io::Result<()> {
        // Get the invocation this worker wants work for.
        let request: WorkServerRequest = net_protocol::async_read(&mut stream).await?;

        match request {
            WorkServerRequest::HealthCheck => {
                let write_result =
                    net_protocol::async_write(&mut stream, &net_protocol::health::HEALTHY).await;
                if let Err(err) = write_result {
                    tracing::debug!("error sending health check: {}", err.to_string());
                }
            }
            WorkServerRequest::NextTest { invocation_id } => {
                // Pull the next work item.
                let next_work = loop {
                    let opt_work = { self.queues.lock().unwrap().next_work(invocation_id) };
                    match opt_work {
                        Ok(work) => break work,
                        Err(WaitingForManifestError) => {
                            // Nothing yet; release control and sleep for a bit in case the
                            // manifest comes in.
                            tokio::time::sleep(POLL_WAIT_TIME).await;
                            continue;
                        }
                    }
                };

                net_protocol::async_write(&mut stream, &next_work).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread, time::Duration};

    use super::Abq;
    use crate::{
        invoke::Client,
        queue::{
            ActiveInvocations, ClientReconnectionError, ClientResponder, QueueServer,
            SharedInvocationQueues, WorkLeftForInvocations, WorkScheduler,
        },
    };
    use abq_utils::{
        auth::{AuthToken, ClientAuthStrategy, ServerAuthStrategy},
        net, net_async,
        net_protocol::{
            self,
            entity::EntityId,
            runners::{Manifest, ManifestMessage, Status, Test, TestOrGroup, TestResult},
            workers::{InvocationId, RunnerKind, TestLikeRunner, WorkId},
        },
    };
    use abq_workers::{
        negotiate::{WorkersConfig, WorkersNegotiator},
        workers::WorkerContext,
    };
    use futures::future;
    use ntest::timeout;
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

        let invocation_id = InvocationId::new();
        let results_handle = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            runtime.block_on(async {
                let mut results = Vec::default();
                let client = Client::invoke_work(
                    EntityId::new(),
                    queue_server_addr,
                    ClientAuthStrategy::NoAuth,
                    invocation_id,
                    runner,
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
        });

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            ClientAuthStrategy::NoAuth,
            invocation_id,
        )
        .unwrap();

        let mut results = results_handle.join().unwrap();

        workers.shutdown();
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

        let invocation1 = InvocationId::new();
        let invocation2 = InvocationId::new();
        let results_handle = thread::spawn(move || {
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
                    ClientAuthStrategy::NoAuth,
                    invocation1,
                    runner1,
                )
                .await
                .unwrap();
                let client2 = Client::invoke_work(
                    EntityId::new(),
                    queue_server_addr,
                    ClientAuthStrategy::NoAuth,
                    invocation2,
                    runner2,
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
        });

        let mut workers_for_run1 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config.clone(),
            queue.get_negotiator_handle(),
            ClientAuthStrategy::NoAuth,
            invocation1,
        )
        .unwrap();

        let mut workers_for_run2 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            ClientAuthStrategy::NoAuth,
            invocation2,
        )
        .unwrap();

        let (mut results1, mut results2) = results_handle.join().unwrap();

        workers_for_run1.shutdown();
        workers_for_run2.shutdown();
        queue.shutdown().unwrap();

        let results1 = sort_results(&mut results1);
        let results2 = sort_results(&mut results2);

        assert_eq!(results1, [("echo1"), ("echo2")]);
        assert_eq!(results2, [("echo3"), ("echo4"), "echo5"]);
    }

    #[test]
    #[traced_test]
    fn bad_message_doesnt_take_down_server() {
        let server = QueueServer::new(Default::default(), "0.0.0.0:0".parse().unwrap());

        let listener = net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();

        logs_with_scope_contain("", "error handling connection");
    }

    #[tokio::test]
    async fn invoker_reconnection_succeeds() {
        use net_async::{ConfiguredClient, ServerListener};

        let invocation_id = InvocationId::new();
        let active_invocations = ActiveInvocations::default();

        // Set up an initial connection for streaming test results targetting the given invocation ID
        let fake_server = ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0")
            .await
            .unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();

        {
            // Register the initial connection
            let (client_res, server_res) =
                futures::join!(client.connect(fake_server_addr), fake_server.accept());
            let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let mut active_invocations = active_invocations.lock().await;
            active_invocations
                .entry(invocation_id)
                .or_insert(ClientResponder::DirectStream(server_conn));
            // Drop the connection
            drop(client_conn);
        };

        // Attempt to reconnect
        let (client_res, server_res) =
            futures::join!(client.connect(fake_server_addr), fake_server.accept());
        let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());
        let new_conn_addr = client_conn.local_addr().unwrap();

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_invocations),
            EntityId::new(),
            server_conn,
            invocation_id,
        )
        .await;

        // Validate that the reconnection was granted
        assert!(reconnection_result.is_ok());
        let active_conn_addr = {
            let active_invocations = active_invocations.lock().await;
            match active_invocations.get(&invocation_id).unwrap() {
                ClientResponder::DirectStream(conn) => conn.peer_addr().unwrap(),
                ClientResponder::Disconnected { .. } => unreachable!(),
            }
        };
        assert_eq!(active_conn_addr, new_conn_addr);
    }

    #[tokio::test]
    async fn invoker_reconnection_fails_never_invoked() {
        use net_async::{ConfiguredClient, ServerListener};

        let invocation_id = InvocationId::new();
        let active_invocations = ActiveInvocations::default();

        let fake_server = ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0")
            .await
            .unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();

        let (client_res, server_res) =
            futures::join!(client.connect(fake_server_addr), fake_server.accept());
        let (_client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_invocations),
            EntityId::new(),
            server_conn,
            invocation_id,
        )
        .await;

        // Validate that the reconnection was granted
        assert!(reconnection_result.is_err());
        let err = reconnection_result.unwrap_err();
        assert_eq!(err, ClientReconnectionError::NeverInvoked(invocation_id));
    }

    #[tokio::test]
    async fn client_disconnect_then_connect_gets_buffered_results() {
        use net_async::{ConfiguredClient, ServerListener};

        let invocation_id = InvocationId::new();
        let active_invocations = ActiveInvocations::default();
        let invocation_queues = SharedInvocationQueues::default();
        let work_left = WorkLeftForInvocations::default();

        let client_entity = EntityId::new();

        // Pretend we have infinite work so the queue always streams back results to the client.
        work_left.lock().unwrap().insert(invocation_id, usize::MAX);

        let fake_server = ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0")
            .await
            .unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let client = ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();

        {
            // Register the initial connection
            let (client_res, server_res) =
                futures::join!(client.connect(fake_server_addr), fake_server.accept());
            let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());

            let mut active_invocations = active_invocations.lock().await;
            let _responder = active_invocations
                .entry(invocation_id)
                .or_insert(ClientResponder::DirectStream(server_conn));

            // Drop the client connection
            drop(client_conn);
        };

        let buffered_work_id = WorkId("test1".to_string());
        {
            // Send a test result, will force the responder to go into disconnected mode
            QueueServer::handle_worker_result(
                Arc::clone(&invocation_queues),
                Arc::clone(&active_invocations),
                Arc::clone(&work_left),
                EntityId::new(),
                invocation_id,
                buffered_work_id.clone(),
                fake_test_result(),
            )
            .await
            .unwrap();

            // Check the internal state of the responder - it should be in disconnected mode.
            let responders = active_invocations.lock().await;
            let responder = responders.get(&invocation_id).unwrap();
            assert!(
                matches!(responder, ClientResponder::Disconnected { .. }),
                "{:?}",
                responder
            );
            match responder {
                ClientResponder::Disconnected { buffered_results } => {
                    assert_eq!(buffered_results.len(), 1)
                }
                _ => unreachable!(),
            }
        }

        // Reconnect back to the queue
        let (client_res, server_res) =
            futures::join!(client.connect(fake_server_addr), fake_server.accept());
        let (client_conn, (server_conn, _)) = (client_res.unwrap(), server_res.unwrap());
        let mut client = Client {
            entity: client_entity,
            abq_server_addr: fake_server_addr,
            auth_strategy: ClientAuthStrategy::NoAuth,
            invocation_id,
            stream: client_conn,
        };
        {
            let reconnection_future = tokio::spawn(QueueServer::handle_invoker_reconnection(
                Arc::clone(&active_invocations),
                EntityId::new(),
                server_conn,
                invocation_id,
            ));

            let (work_id, _) = client.next().await.unwrap().unwrap();
            assert_eq!(work_id, buffered_work_id);

            let reconnection_result = reconnection_future.await.unwrap();
            assert!(reconnection_result.is_ok());
        }

        {
            // Make sure that new test results also get send to the new client connectiion.
            let second_work_id = WorkId("test2".to_string());
            let worker_result_future = tokio::spawn(QueueServer::handle_worker_result(
                invocation_queues,
                Arc::clone(&active_invocations),
                work_left,
                EntityId::new(),
                invocation_id,
                second_work_id.clone(),
                fake_test_result(),
            ));

            let (work_id, _) = client.next().await.unwrap().unwrap();
            assert_eq!(work_id, second_work_id);

            worker_result_future.await.unwrap().unwrap();
        }
    }

    #[test]
    fn queue_server_healthcheck() {
        let server = net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::mpsc::channel(1);

        let queue_server = QueueServer {
            queues: Default::default(),
            active_invocations: Default::default(),
            work_left_for_invocations: Default::default(),
            public_negotiator_addr: "0.0.0.0:0".parse().unwrap(),
        };
        let queue_handle = thread::spawn(|| {
            queue_server.start_on(server, server_shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
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
        let server = net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::mpsc::channel(1);

        let work_scheduler = WorkScheduler {
            queues: Default::default(),
        };
        let work_server_handle = thread::spawn(|| {
            work_scheduler.start_on(server, server_shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
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
        let server = WorkScheduler {
            queues: Default::default(),
        };

        let listener = net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
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

        let listener = net::ServerListener::bind(server_auth, "0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(client_auth).unwrap();
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

        let listener = net::ServerListener::bind(server_auth, "0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
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

        let listener = net::ServerListener::bind(server_auth, "0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(client_auth).unwrap();
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

        let listener = net::ServerListener::bind(server_auth, "0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
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
}
