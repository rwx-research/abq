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
    GetNextWork, NotifyManifest, NotifyResult, WorkerContext, WorkerPool, WorkerPoolConfig,
};
use abq_utils::{
    auth::ClientAuthStrategy,
    net, net_async,
    net_protocol::{
        self,
        entity::EntityId,
        publicize_addr,
        workers::{InvocationId, RunnerKind},
    },
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
enum MessageFromQueueNegotiator {
    ExecutionContext {
        /// The work invocation we are connecting to.
        invocation_id: InvocationId,
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
    },
}

#[derive(Serialize, Deserialize)]
pub enum MessageToQueueNegotiator {
    HealthCheck,
    WantsToAttach {
        /// The invocation the worker wants to run tests for. The queue must respect assignment of
        /// work related to this invocation, or return an error.
        invocation: InvocationId,
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
pub struct WorkersNegotiator(net::ClientStream, WorkerContext);

impl WorkersNegotiator {
    /// Runs the workers-side of the negotiation, returning the configured worker pool once
    /// negotiation is complete.
    #[instrument(level = "trace", skip(workers_config, queue_negotiator_handle, auth_strategy), fields(
        num_workers = workers_config.num_workers
    ))]
    pub fn negotiate_and_start_pool(
        workers_config: WorkersConfig,
        queue_negotiator_handle: QueueNegotiatorHandle,
        auth_strategy: ClientAuthStrategy,
        wanted_invocation_id: InvocationId,
    ) -> Result<WorkerPool, WorkersNegotiateError> {
        use WorkersNegotiateError::*;

        let client = net::ConfiguredClient::new(auth_strategy)?;
        let mut conn = client
            .connect(queue_negotiator_handle.0)
            .map_err(|_| CouldNotConnect)?;

        let wants_to_attach = MessageToQueueNegotiator::WantsToAttach {
            invocation: wanted_invocation_id,
        };
        tracing::debug!("Writing attach message");
        net_protocol::write(&mut conn, wants_to_attach).map_err(|_| CouldNotConnect)?;
        tracing::debug!("Wrote attach message");

        tracing::debug!("Awaiting execution message");
        let MessageFromQueueNegotiator::ExecutionContext {
            invocation_id,
            queue_new_work_addr,
            queue_results_addr,
            runner_kind,
            worker_should_generate_manifest,
        } = net_protocol::read(&mut conn).map_err(|_| BadQueueMessage)?;
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
        } = workers_config;

        let notify_result: NotifyResult = Arc::new({
            #[allow(clippy::clone_on_copy)] // not all clients implement copy.
            let client = client.clone();

            move |entity, invocation_id, work_id, result| {
                let span = tracing::trace_span!("notify_result", invocation_id=?invocation_id, work_id=?work_id, queue_server=?queue_results_addr);
                let _notify_result = span.enter();

                // TODO: error handling
                let mut stream = client
                    .connect(queue_results_addr)
                    .expect("results server not available");

                let request = net_protocol::queue::Request {
                    entity,
                    message: net_protocol::queue::Message::WorkerResult(
                        invocation_id,
                        work_id,
                        result,
                    ),
                };

                // TODO: error handling
                net_protocol::write(&mut stream, request).unwrap();
            }
        });

        let get_next_work: GetNextWork = Arc::new({
            #[allow(clippy::clone_on_copy)] // not all clients implement copy.
            let client = client.clone();

            move || {
                let span = tracing::trace_span!("get_next_work", invocation_id=?invocation_id, new_work_server=?queue_new_work_addr);
                let _get_next_work = span.enter();

                // TODO: error handling
                // In particular, the work server may have shut down and we can't connect. In that
                // case the worker should shutdown too.
                let mut stream = client
                    .connect(queue_new_work_addr)
                    .expect("work server not available");

                // Write the invocation ID we want work for
                // TODO: error handling
                net_protocol::write(
                    &mut stream,
                    net_protocol::work_server::WorkServerRequest::NextTest { invocation_id },
                )
                .unwrap();

                // TODO: error handling
                net_protocol::read(&mut stream).unwrap()
            }
        });

        let notify_manifest: Option<NotifyManifest> = if worker_should_generate_manifest {
            Some(Box::new(move |entity, invocation_id, manifest| {
                let span = tracing::trace_span!("notify_manifest", ?entity, invocation_id=?invocation_id, queue_server=?queue_results_addr);
                let _notify_manifest = span.enter();

                // TODO: error handling
                let mut stream = client
                    .connect(queue_results_addr)
                    .expect("results server not available");

                let request = net_protocol::queue::Request {
                    entity,
                    message: net_protocol::queue::Message::Manifest(invocation_id, manifest),
                };

                // TODO: error handling
                net_protocol::write(&mut stream, request).unwrap();
            }))
        } else {
            None
        };

        let pool_config = WorkerPoolConfig {
            size: num_workers,
            runner_kind,
            get_next_work,
            notify_result,
            worker_context,
            work_timeout,
            work_retries,
            invocation_id,
            notify_manifest,
        };

        tracing::debug!("Starting worker pool");
        let pool = WorkerPool::new(pool_config);
        tracing::debug!("Started worker pool");

        Ok(pool)
    }
}

/// The queue side of the negotiation.
pub struct QueueNegotiator {
    addr: SocketAddr,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
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
        auth_strategy: ClientAuthStrategy,
    ) -> Result<Self, QueueNegotiatorHandleError> {
        use QueueNegotiatorHandleError::*;

        let client = net::ConfiguredClient::new(auth_strategy)?;
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
    pub invocation_id: InvocationId,
    pub runner_kind: RunnerKind,
    pub should_generate_manifest: bool,
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

    /// Any other opaque error that occured.
    #[error("{0}")]
    Other(#[from] Box<dyn Error + Send + Sync>),
}

struct QueueNegotiatorCtx<GetAssignedRun>
where
    GetAssignedRun: Fn(InvocationId) -> AssignedRun + Send + Sync + 'static,
{
    get_assigned_run: Arc<GetAssignedRun>,
    advertised_queue_work_scheduler_addr: SocketAddr,
    advertised_queue_results_addr: SocketAddr,
}

impl<GetAssignedRun> QueueNegotiatorCtx<GetAssignedRun>
where
    GetAssignedRun: Fn(InvocationId) -> AssignedRun + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            get_assigned_run: Arc::clone(&self.get_assigned_run),
            advertised_queue_work_scheduler_addr: self.advertised_queue_work_scheduler_addr,
            advertised_queue_results_addr: self.advertised_queue_results_addr,
        }
    }
}

impl QueueNegotiator {
    /// Starts a queue negotiator on a new thread.
    pub fn new<GetAssignedRun>(
        public_ip: IpAddr,
        listener: net::ServerListener,
        queue_work_scheduler_addr: SocketAddr,
        queue_results_addr: SocketAddr,
        get_assigned_run: GetAssignedRun,
    ) -> Result<Self, QueueNegotiatorServerError>
    where
        GetAssignedRun: Fn(InvocationId) -> AssignedRun + Send + Sync + 'static,
    {
        let addr = listener.local_addr()?;

        let advertised_queue_work_scheduler_addr =
            publicize_addr(queue_work_scheduler_addr, public_ip);
        let advertised_queue_results_addr = publicize_addr(queue_results_addr, public_ip);

        tracing::debug!("Starting negotiator on {}", addr);

        let (server_shutdown_tx, mut server_shutdown_rx) = tokio::sync::mpsc::channel(1);

        let ctx = QueueNegotiatorCtx {
            get_assigned_run: Arc::new(get_assigned_run),
            advertised_queue_work_scheduler_addr,
            advertised_queue_results_addr,
        };

        let listener_handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let server_result: Result<(), QueueNegotiatorServerError> = rt.block_on(async {
                // Initialize the listener in the async context.
                let listener = net_async::ServerListener::from_sync(listener)?;

                loop {
                    let (client, _) = tokio::select! {
                        conn = listener.accept() => conn?,
                        _ = server_shutdown_rx.recv() => {
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
            shutdown_tx: server_shutdown_tx,
            listener_handle: Some(listener_handle),
        })
    }

    async fn handle_conn<GetAssignedRun>(
        ctx: QueueNegotiatorCtx<GetAssignedRun>,
        mut stream: net_async::ServerStream,
    ) -> io::Result<()>
    where
        GetAssignedRun: Fn(InvocationId) -> AssignedRun + Send + Sync + 'static,
    {
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
            WantsToAttach {
                invocation: wanted_invocation_id,
            } => {
                tracing::debug!("New worker set negotiating");

                // Choose an `abq test ...` invocation the workers should perform work
                // for.
                // TODO: this fully blocks the negotiator, so nothing else can connect
                // while we wait for an invocation, and moreover we won't respect
                // shutdown messages.
                tracing::debug!("Finding assigned run");
                let AssignedRun {
                    invocation_id,
                    runner_kind,
                    should_generate_manifest,
                } = (ctx.get_assigned_run)(wanted_invocation_id);

                tracing::debug!("Found assigned run");

                debug_assert_eq!(invocation_id, wanted_invocation_id);

                let execution_context = MessageFromQueueNegotiator::ExecutionContext {
                    queue_new_work_addr: ctx.advertised_queue_work_scheduler_addr,
                    queue_results_addr: ctx.advertised_queue_results_addr,
                    runner_kind,
                    worker_should_generate_manifest: should_generate_manifest,
                    invocation_id,
                };

                tracing::debug!("Sending execution context");
                net_protocol::async_write(&mut stream, &execution_context).await?;
                tracing::debug!("Sent execution context");
            }
        }

        Ok(())
    }

    pub fn get_handle(&self) -> QueueNegotiatorHandle {
        QueueNegotiatorHandle(self.addr)
    }

    pub fn shutdown(&mut self) {
        self.shutdown_tx.blocking_send(()).unwrap();
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
            self.shutdown();
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
    use crate::workers::WorkerContext;
    use abq_utils::auth::{AuthToken, ClientAuthStrategy, ServerAuthStrategy};
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, Status, Test, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::work_server::WorkServerRequest;
    use abq_utils::net_protocol::workers::{
        InvocationId, NextWork, RunnerKind, TestLikeRunner, WorkContext, WorkId,
    };
    use abq_utils::{flatten_manifest, net, net_protocol};
    use tracing_test::internal::logs_with_scope_contain;
    use tracing_test::traced_test;

    type Messages = Arc<Mutex<Vec<TestResult>>>;
    type ManifestCollector = Arc<Mutex<Option<ManifestMessage>>>;

    type QueueNextWork = (SocketAddr, mpsc::Sender<()>, JoinHandle<()>);
    type QueueResults = (Messages, SocketAddr, mpsc::Sender<()>, JoinHandle<()>);

    fn mock_queue_next_work_server(manifest_collector: ManifestCollector) -> QueueNextWork {
        let server = net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let mut work_to_write = loop {
                match manifest_collector.lock().unwrap().take() {
                    Some(man) => {
                        let work: Vec<_> = flatten_manifest(man.manifest)
                            .into_iter()
                            .enumerate()
                            .map(|(i, test_case)| NextWork::Work {
                                test_case,
                                context: WorkContext {
                                    working_dir: PathBuf::from("/"),
                                },
                                invocation_id: InvocationId::new(),
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
            loop {
                match server.accept() {
                    Ok((mut worker, _)) => {
                        worker.set_nonblocking(false).unwrap();
                        let work = work_to_write.pop().unwrap_or(NextWork::EndOfWork);

                        let _msg: WorkServerRequest = net_protocol::read(&mut worker).unwrap();
                        net_protocol::write(&mut worker, work).unwrap();
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

        let server = net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap();
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
                net_protocol::queue::Message::Manifest(_, manifest) => {
                    let old_manifest = manifest_collector.lock().unwrap().replace(manifest);
                    debug_assert!(
                        old_manifest.is_none(),
                        "replacing existing manifest! This is a bug in our tests."
                    );
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

        let invocation_id = InvocationId::new();

        let get_assigned_run = |invocation_id| {
            let manifest = ManifestMessage {
                manifest: Manifest {
                    members: vec![echo_test("hello".to_string())],
                },
            };
            AssignedRun {
                invocation_id,
                runner_kind: RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest),
                should_generate_manifest: true,
            }
        };

        let mut queue_negotiator = QueueNegotiator::new(
            "0.0.0.0".parse().unwrap(),
            net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap(),
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
        };
        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue_negotiator.get_handle(),
            ClientAuthStrategy::NoAuth,
            invocation_id,
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

        workers.shutdown();
        queue_negotiator.shutdown();

        close_queue_servers(
            shutdown_next_work_server,
            next_work_handle,
            shutdown_results_server,
            results_handle,
        );
    }

    fn test_negotiator(server: net::ServerListener) -> QueueNegotiator {
        QueueNegotiator::new(
            "0.0.0.0".parse().unwrap(),
            server,
            // Below parameters are faux because they are unnecessary for healthchecks.
            "0.0.0.0:0".parse().unwrap(),
            "0.0.0.0:0".parse().unwrap(),
            |_| AssignedRun {
                invocation_id: InvocationId::new(),
                runner_kind: RunnerKind::TestLikeRunner(
                    TestLikeRunner::Echo,
                    ManifestMessage {
                        manifest: Manifest { members: vec![] },
                    },
                ),
                should_generate_manifest: true,
            },
        )
        .unwrap()
    }

    #[test]
    fn queue_negotiator_healthcheck() {
        let server = net::ServerListener::bind(ServerAuthStrategy::NoAuth, "0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let mut queue_negotiator = test_negotiator(server);

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, MessageToQueueNegotiator::HealthCheck).unwrap();
        let health_msg: net_protocol::health::HEALTH = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::HEALTHY);

        queue_negotiator.shutdown();
    }

    #[test]
    #[traced_test]
    fn queue_negotiator_with_auth_okay() {
        let (server_auth, client_auth) = AuthToken::new_random().build_strategies();

        let server = net::ServerListener::bind(server_auth, "0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let mut queue_negotiator = test_negotiator(server);

        let client = net::ConfiguredClient::new(client_auth).unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, MessageToQueueNegotiator::HealthCheck).unwrap();
        let health_msg: net_protocol::health::HEALTH = net_protocol::read(&mut conn).unwrap();

        assert_eq!(health_msg, net_protocol::health::HEALTHY);

        queue_negotiator.shutdown();

        logs_with_scope_contain("", "error handling connection");
    }

    #[test]
    #[traced_test]
    fn queue_negotiator_connect_with_no_auth_fails() {
        let (server_auth, _) = AuthToken::new_random().build_strategies();

        let server = net::ServerListener::bind(server_auth, "0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let mut queue_negotiator = test_negotiator(server);

        let client = net::ConfiguredClient::new(ClientAuthStrategy::NoAuth).unwrap();
        let mut conn = client.connect(server_addr).unwrap();
        net_protocol::write(&mut conn, MessageToQueueNegotiator::HealthCheck).unwrap();

        queue_negotiator.shutdown();

        logs_with_scope_contain("", "error handling connection");
    }
}
