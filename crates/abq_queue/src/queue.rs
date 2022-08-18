use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use abq_utils::flatten_manifest;
use abq_utils::net_protocol::publicize_addr;
use abq_utils::net_protocol::queue::InvokerResponse;
use abq_utils::net_protocol::runners::{ManifestMessage, TestCase, TestResult};
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

    send_worker: mpsc::Sender<WorkerSchedulerMsg>,
    work_scheduler_handle: Option<JoinHandle<()>>,
    negotiator: QueueNegotiator,

    active: bool,
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
    pub fn shutdown(&mut self) {
        debug_assert!(self.active);

        self.active = false;

        self.server_shutdown_tx.blocking_send(()).unwrap();

        self.negotiator.shutdown();

        // TODO: do something with the result
        let _server_result = self
            .server_handle
            .take()
            .expect("server handle must be available during Drop")
            .join()
            .unwrap();

        // Server has closed; shut down all workers.
        self.send_worker.send(WorkerSchedulerMsg::Shutdown).unwrap();
        self.work_scheduler_handle
            .take()
            .expect("worker handle must be available during Drop")
            .join()
            .unwrap();
    }

    pub fn get_negotiator_handle(&self) -> QueueNegotiatorHandle {
        self.negotiator.get_handle()
    }
}

impl Drop for Abq {
    fn drop(&mut self) {
        if self.active {
            // Our user never called shutdown; try to perform a clean exit.
            self.shutdown();
        }
    }
}

enum WorkerSchedulerMsg {
    Shutdown,
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
    } = config;

    let queues: SharedInvocationQueues = Default::default();

    let server_listener = TcpListener::bind((bind_ip, server_port)).unwrap();
    let server_addr = server_listener.local_addr().unwrap();

    let negotiator_listener = TcpListener::bind((bind_ip, negotiator_port)).unwrap();
    let negotiator_addr = negotiator_listener.local_addr().unwrap();
    let public_negotiator_addr = publicize_addr(negotiator_addr, public_ip);

    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::mpsc::channel(1);

    let (send_worker, recv_worker) = mpsc::channel();
    let server_handle = thread::spawn({
        let queue_server = QueueServer::new(Arc::clone(&queues), public_negotiator_addr);
        move || queue_server.start_on(server_listener, server_shutdown_rx)
    });

    let new_work_server = TcpListener::bind((bind_ip, work_port)).unwrap();
    let new_work_server_addr = new_work_server.local_addr().unwrap();

    let work_scheduler_handle = thread::spawn({
        let scheduler = WorkScheduler {
            queues: Arc::clone(&queues),
            msg_recv: recv_worker,
        };
        move || scheduler.start_on(new_work_server)
    });

    // Chooses a test run for a set of workers to attach to.
    // This blocks until there is an invocation available.
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
    );

    Abq {
        server_addr,
        server_handle: Some(server_handle),
        server_shutdown_tx,

        send_worker,
        work_scheduler_handle: Some(work_scheduler_handle),
        negotiator,

        active: true,
    }
}

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

/// Handler for sending test results back to the abq client that issued a test run.
#[derive(Debug)]
enum ClientResponder {
    /// We have an open stream upon which to send back the test results.
    DirectStream(tokio::net::TcpStream),
    /// There is not yet an open connection we can stream the test results back on.
    /// This state may be reached during the time a client is disconnected.
    Disconnected {
        buffered_results: Vec<InvokerResponse>,
    },
}

impl ClientResponder {
    fn new(stream: tokio::net::TcpStream) -> Self {
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
    async fn reconnect_with(&mut self, new_conn: tokio::net::TcpStream) {
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
enum QueueServerError {
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
        server_listener: TcpListener,
        mut server_shutdown: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<(), QueueServerError> {
        // Before we hand the server listener over to the async runtime, we must set it into
        // non-blocking mode (as the async runtime will not block!)
        server_listener.set_nonblocking(true)?;

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
            let server_listener = tokio::net::TcpListener::from_std(server_listener)?;

            loop {
                let (client, _) = tokio::select! {
                    conn = server_listener.accept() => conn?,
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
        mut client: tokio::net::TcpStream,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let msg = net_protocol::async_read(&mut client).await?;

        let result: Result<(), Box<dyn Error + Send + Sync>> = match msg {
            Message::NegotiatorAddr => {
                Self::handle_negotiator_addr(client, self.public_negotiator_addr).await
            }
            Message::InvokeWork(invoke_work) => {
                Self::handle_invoked_work(self.active_invocations, self.queues, client, invoke_work)
                    .await
            }
            Message::Reconnect(invocation_id) => {
                Self::handle_invoker_reconnection(self.active_invocations, client, invocation_id)
                    .await
                    // Upcast the reconnection error into a generic error
                    .map_err(|e| Box::new(e) as _)
            }
            Message::Manifest(invocation_id, manifest) => Self::handle_manifest(
                self.queues,
                self.work_left_for_invocations,
                invocation_id,
                manifest,
            ),
            Message::WorkerResult(invocation_id, work_id, result) => {
                Self::handle_worker_result(
                    self.queues,
                    self.active_invocations,
                    self.work_left_for_invocations,
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
    #[instrument(level = "trace", skip(invoker, public_negotiator_addr))]
    async fn handle_negotiator_addr(
        mut invoker: tokio::net::TcpStream,
        public_negotiator_addr: SocketAddr,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        net_protocol::async_write(&mut invoker, &public_negotiator_addr).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(active_invocations, queues, invoker))]
    async fn handle_invoked_work(
        active_invocations: ActiveInvocations,
        queues: SharedInvocationQueues,
        invoker: tokio::net::TcpStream,
        invoke_work: InvokeWork,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let InvokeWork {
            invocation_id,
            runner,
        } = invoke_work;

        let results_responder = ClientResponder::new(invoker);
        let old_responder = active_invocations
            .lock()
            .await
            .insert(invocation_id, results_responder);

        debug_assert!(
            old_responder.is_none(),
            "Existing invoker for expected unique invocation id"
        );

        // Create a new work queue for this invocation.
        queues.lock().unwrap().create_queue(invocation_id, runner);

        Ok(())
    }

    #[instrument(level = "trace", skip(active_invocations, new_invoker))]
    async fn handle_invoker_reconnection(
        active_invocations: ActiveInvocations,
        new_invoker: tokio::net::TcpStream,
        invocation_id: InvocationId,
    ) -> Result<(), ClientReconnectionError> {
        use std::collections::hash_map::Entry;

        // When an abq client loses connection with the queue, they may attempt to reconnect.
        // We'll need to update the connection from streaming results to the client to the new one.
        let mut active_invocations = active_invocations.lock().await;
        let active_conn_entry = active_invocations.entry(invocation_id);
        match active_conn_entry {
            Entry::Occupied(mut occupied) => {
                let new_invoker_addr = new_invoker.peer_addr();

                occupied.get_mut().reconnect_with(new_invoker).await;

                tracing::debug!(
                    "rerouted connection for {:?} to {:?}",
                    invocation_id,
                    new_invoker_addr
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
    /// Channel on which the scheduler itself will receive messages from the parent queue.
    msg_recv: mpsc::Receiver<WorkerSchedulerMsg>,
}

impl WorkScheduler {
    fn start_on(mut self, listener: TcpListener) {
        // Make `accept()` non-blocking so that we have time to check whether we should shutdown, if
        // there are no active connections into the queue.
        listener
            .set_nonblocking(true)
            .expect("Failed to set stream as non-blocking");

        for client in listener.incoming() {
            match client {
                Ok(client) => {
                    // Reads and writes to the client will be buffered, so we must make sure
                    // the stream is blocking. Note that we inherit non-blocking from the server
                    // listener, but that this adjustment does not affect the server.
                    client
                        .set_nonblocking(false)
                        .expect("Failed to set client stream as blocking");

                    self.handle_work_conn(client);
                }
                Err(ref e) => {
                    match e.kind() {
                        io::ErrorKind::WouldBlock => {
                            match self.msg_recv.try_recv() {
                                Ok(WorkerSchedulerMsg::Shutdown) => return,
                                Err(TryRecvError::Empty) => {
                                    // Sleep before polling for the next work connection.
                                    thread::sleep(POLL_WAIT_TIME);
                                }
                                Err(TryRecvError::Disconnected) => {
                                    todo!("disconnected from parent queue")
                                }
                            }
                        }
                        _ => todo!("Unhandled IO error: {e:?}"),
                    }
                }
            }
        }
    }

    #[instrument(level = "trace", skip(self, worker))]
    fn handle_work_conn(&mut self, mut worker: TcpStream) {
        // Get the invocation this worker wants work for.
        // TODO: error handling
        let invocation_id: InvocationId =
            net_protocol::read(&mut worker).expect("Failed to read message");

        // Pull the next work item.
        let next_work = loop {
            let opt_work = { self.queues.lock().unwrap().next_work(invocation_id) };
            match opt_work {
                Ok(work) => break work,
                Err(WaitingForManifestError) => {
                    // Nothing yet; release control and sleep for a bit in case the
                    // manifest comes in.
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
            }
        };

        // TODO: error handling
        net_protocol::write(&mut worker, next_work).unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::{
        net::{TcpListener, TcpStream},
        sync::Arc,
        thread,
        time::Duration,
    };

    use super::Abq;
    use crate::{
        invoke,
        queue::{
            ActiveInvocations, ClientReconnectionError, ClientResponder, QueueServer,
            SharedInvocationQueues, WorkLeftForInvocations,
        },
    };
    use abq_utils::net_protocol::{
        self,
        queue::InvokerResponse,
        runners::{Manifest, ManifestMessage, Status, Test, TestOrGroup, TestResult},
        workers::{InvocationId, RunnerKind, TestLikeRunner, WorkId},
    };
    use abq_workers::{
        negotiate::{WorkersConfig, WorkersNegotiator},
        workers::WorkerContext,
    };
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
            let mut results = Vec::default();
            invoke::invoke_work(queue_server_addr, invocation_id, runner, |id, result| {
                results.push((id.0, result))
            });
            results
        });

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            invocation_id,
        )
        .unwrap();

        let mut results = results_handle.join().unwrap();

        queue.shutdown();
        workers.shutdown();

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
        let results1_handle = thread::spawn(move || {
            let mut results = Vec::default();
            invoke::invoke_work(queue_server_addr, invocation1, runner1, |id, result| {
                results.push((id.0, result))
            });
            results
        });

        let invocation2 = InvocationId::new();
        let results2_handle = thread::spawn(move || {
            let mut results = Vec::default();
            invoke::invoke_work(queue_server_addr, invocation2, runner2, |id, result| {
                results.push((id.0, result))
            });
            results
        });

        let mut workers_for_run1 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config.clone(),
            queue.get_negotiator_handle(),
            invocation1,
        )
        .unwrap();

        let mut workers_for_run2 = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
            invocation2,
        )
        .unwrap();

        let mut results1 = results1_handle.join().unwrap();
        let mut results2 = results2_handle.join().unwrap();

        queue.shutdown();
        workers_for_run1.shutdown();
        workers_for_run2.shutdown();

        let results1 = sort_results(&mut results1);
        let results2 = sort_results(&mut results2);

        assert_eq!(results1, [("echo1"), ("echo2")]);
        assert_eq!(results2, [("echo3"), ("echo4"), "echo5"]);
    }

    #[test]
    #[traced_test]
    fn bad_message_doesnt_take_down_server() {
        let server = QueueServer::new(Default::default(), "0.0.0.0:0".parse().unwrap());

        let listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let server_addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let server_thread = thread::spawn(move || {
            server.start_on(listener, shutdown_rx).unwrap();
        });

        let mut conn = TcpStream::connect(server_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();

        shutdown_tx.blocking_send(()).unwrap();
        server_thread.join().unwrap();

        logs_with_scope_contain("", "error handling connection");
    }

    #[tokio::test]
    async fn invoker_reconnection_succeeds() {
        use tokio::net::{TcpListener, TcpStream};

        let invocation_id = InvocationId::new();
        let active_invocations = ActiveInvocations::default();

        // Set up an initial connection for streaming test results targetting the given invocation ID
        let fake_server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        {
            // Register the initial connection
            let client_conn = TcpStream::connect(fake_server_addr).await.unwrap();
            let (server_conn, _) = fake_server.accept().await.unwrap();
            let mut active_invocations = active_invocations.lock().await;
            active_invocations
                .entry(invocation_id)
                .or_insert(ClientResponder::DirectStream(server_conn));
            // Drop the connection
            drop(client_conn);
        };

        // Attempt to reconnect
        let new_conn = TcpStream::connect(fake_server_addr).await.unwrap();
        let new_conn_addr = new_conn.local_addr().unwrap();

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_invocations),
            new_conn,
            invocation_id,
        )
        .await;

        // Validate that the reconnection was granted
        assert!(reconnection_result.is_ok());
        let active_conn_addr = {
            let active_invocations = active_invocations.lock().await;
            match active_invocations.get(&invocation_id).unwrap() {
                ClientResponder::DirectStream(conn) => conn.local_addr().unwrap(),
                ClientResponder::Disconnected { .. } => unreachable!(),
            }
        };
        assert_eq!(active_conn_addr, new_conn_addr);
    }

    #[tokio::test]
    async fn invoker_reconnection_fails_never_invoked() {
        use tokio::net::{TcpListener, TcpStream};

        let invocation_id = InvocationId::new();
        let active_invocations = ActiveInvocations::default();

        let fake_server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        let conn = TcpStream::connect(fake_server_addr).await.unwrap();

        let reconnection_result = QueueServer::handle_invoker_reconnection(
            Arc::clone(&active_invocations),
            conn,
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
        use tokio::net::{TcpListener, TcpStream};

        let invocation_id = InvocationId::new();
        let active_invocations = ActiveInvocations::default();
        let invocation_queues = SharedInvocationQueues::default();
        let work_left = WorkLeftForInvocations::default();

        // Pretend we have infinite work so the queue always streams back results to the client.
        work_left.lock().unwrap().insert(invocation_id, usize::MAX);

        let fake_server = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let fake_server_addr = fake_server.local_addr().unwrap();

        {
            // Register the initial connection
            let client_conn = TcpStream::connect(fake_server_addr).await.unwrap();
            let (server_conn, _) = fake_server.accept().await.unwrap();
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
        let mut client_conn = TcpStream::connect(fake_server_addr).await.unwrap();
        {
            let (server_conn, _) = fake_server.accept().await.unwrap();
            let reconnection_future = tokio::spawn({
                let active_invocations = Arc::clone(&active_invocations);
                async move {
                    QueueServer::handle_invoker_reconnection(
                        active_invocations,
                        server_conn,
                        invocation_id,
                    )
                    .await
                }
            });

            // The new connection should now have a test result waiting for us.
            let test_result = net_protocol::async_read(&mut client_conn).await.unwrap();
            match test_result {
                InvokerResponse::Result(work_id, _) => {
                    assert_eq!(work_id, buffered_work_id);
                }
                InvokerResponse::EndOfResults => unreachable!(),
            }
            // Send an ack
            net_protocol::async_write(&mut client_conn, &net_protocol::client::AckTestResult {})
                .await
                .unwrap();

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
                invocation_id,
                second_work_id.clone(),
                fake_test_result(),
            ));

            let test_result = net_protocol::async_read(&mut client_conn).await.unwrap();
            net_protocol::async_write(&mut client_conn, &net_protocol::client::AckTestResult {})
                .await
                .unwrap();

            worker_result_future.await.unwrap().unwrap();

            match test_result {
                InvokerResponse::Result(work_id, _) => {
                    assert_eq!(work_id, second_work_id);
                }
                InvokerResponse::EndOfResults => unreachable!(),
            }
        }
    }
}
