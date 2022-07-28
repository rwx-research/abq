//! Module negotiate helps worker pools attach to queues.

use serde_derive::{Deserialize, Serialize};
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
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
use abq_utils::net_protocol::{
    self,
    workers::{InvocationId, RunnerKind},
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
enum MessageToQueueNegotiator {
    WantsToAttach {
        /// The invocation the worker wants to run tests for. The queue must respect assignment of
        /// work related to this invocation, or return an error.
        invocation: InvocationId,
    },
    Shutdown,
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
}

/// The worker pool side of the negotiation.
pub struct WorkersNegotiator(TcpListener, WorkerContext);

impl WorkersNegotiator {
    /// Runs the workers-side of the negotiation, returning the configured worker pool once
    /// negotiation is complete.
    #[instrument(level = "trace", skip(workers_config, queue_negotiator_handle), fields(
        num_workers = workers_config.num_workers
    ))]
    pub fn negotiate_and_start_pool(
        workers_config: WorkersConfig,
        queue_negotiator_handle: QueueNegotiatorHandle,
        wanted_invocation_id: InvocationId,
    ) -> Result<WorkerPool, WorkersNegotiateError> {
        use WorkersNegotiateError::*;

        let mut conn =
            TcpStream::connect(&queue_negotiator_handle.0).map_err(|_| CouldNotConnect)?;

        let wants_to_attach = MessageToQueueNegotiator::WantsToAttach {
            invocation: wanted_invocation_id,
        };
        net_protocol::write(&mut conn, wants_to_attach).map_err(|_| CouldNotConnect)?;

        let MessageFromQueueNegotiator::ExecutionContext {
            invocation_id,
            queue_new_work_addr,
            queue_results_addr,
            runner_kind,
            worker_should_generate_manifest,
        } = net_protocol::read(&mut conn).map_err(|_| BadQueueMessage)?;

        let WorkersConfig {
            num_workers,
            worker_context,
            work_timeout,
            work_retries,
        } = workers_config;

        let notify_result: NotifyResult = Arc::new(move |invocation_id, work_id, result| {
            let span = tracing::trace_span!("notify_result", invocation_id=?invocation_id, work_id=?work_id, queue_server=?queue_results_addr);
            let _notify_result = span.enter();

            // TODO: error handling
            let mut stream =
                TcpStream::connect(queue_results_addr).expect("results server not available");

            // TODO: error handling
            net_protocol::write(
                &mut stream,
                net_protocol::queue::Message::WorkerResult(invocation_id, work_id, result),
            )
            .unwrap();
        });

        let get_next_work: GetNextWork = Arc::new(move || {
            let span = tracing::trace_span!("get_next_work", invocation_id=?invocation_id, new_work_server=?queue_new_work_addr);
            let _get_next_work = span.enter();

            // TODO: error handling
            // In particular, the work server may have shut down and we can't connect. In that
            // case the worker should shutdown too.
            let mut stream =
                TcpStream::connect(queue_new_work_addr).expect("work server not available");

            // Write the invocation ID we want work for
            // TODO: error handling
            net_protocol::write(&mut stream, invocation_id).unwrap();

            // TODO: error handling
            net_protocol::read(&mut stream).unwrap()
        });

        let notify_manifest: Option<NotifyManifest> = if worker_should_generate_manifest {
            Some(Box::new(move |invocation_id, manifest| {
                let span = tracing::trace_span!("notify_manifest", invocation_id=?invocation_id, queue_server=?queue_results_addr);
                let _notify_manifest = span.enter();

                // TODO: error handling
                let mut stream =
                    TcpStream::connect(queue_results_addr).expect("results server not available");

                // TODO: error handling
                net_protocol::write(
                    &mut stream,
                    net_protocol::queue::Message::Manifest(invocation_id, manifest),
                )
                .unwrap();
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

        let pool = WorkerPool::new(pool_config);

        Ok(pool)
    }
}

/// The queue side of the negotiation.
pub struct QueueNegotiator {
    addr: SocketAddr,
    listener_handle: Option<thread::JoinHandle<()>>,
}

/// Address of a queue negotiator.
pub struct QueueNegotiatorHandle(SocketAddr);

#[derive(Debug, Error)]
#[error("not an address to a queue negotiator")]
pub struct NotAQueueNegotiator;

impl QueueNegotiatorHandle {
    pub fn get_address(&self) -> SocketAddr {
        self.0
    }

    pub fn from_raw_address(addr: SocketAddr) -> Result<Self, NotAQueueNegotiator> {
        // TODO: actually verify that the address is a queue negotiator
        Ok(Self(addr))
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

impl QueueNegotiator {
    /// Starts a queue negotiator on a new thread.
    pub fn new<GetAssignedRun>(
        bind_addr: SocketAddr,
        queue_next_work_addr: SocketAddr,
        queue_results_addr: SocketAddr,
        mut get_assigned_run: GetAssignedRun,
    ) -> Self
    where
        GetAssignedRun: FnMut(InvocationId) -> AssignedRun + Send + 'static,
    {
        // TODO: error handling when `bind_addr` is taken.
        let listener = TcpListener::bind(bind_addr).unwrap();
        let addr = listener.local_addr().unwrap();

        let listener_handle = thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                // TODO: error handling
                let msg: MessageToQueueNegotiator = net_protocol::read(&mut stream).unwrap();

                use MessageToQueueNegotiator::*;
                match msg {
                    WantsToAttach {
                        invocation: wanted_invocation_id,
                    } => {
                        // Choose an `abq test ...` invocation the workers should perform work
                        // for.
                        // TODO: this fully blocks the negotiator, so nothing else can connect
                        // while we wait for an invocation, and moreover we won't respect
                        // shutdown messages.
                        let AssignedRun {
                            invocation_id,
                            runner_kind,
                            should_generate_manifest,
                        } = get_assigned_run(wanted_invocation_id);

                        debug_assert_eq!(invocation_id, wanted_invocation_id);

                        let execution_context = MessageFromQueueNegotiator::ExecutionContext {
                            queue_new_work_addr: queue_next_work_addr,
                            queue_results_addr,
                            runner_kind,
                            worker_should_generate_manifest: should_generate_manifest,
                            invocation_id,
                        };

                        // TODO: error handling
                        net_protocol::write(&mut stream, execution_context).unwrap();
                    }
                    Shutdown => return,
                }
            }
        });

        Self {
            addr,
            listener_handle: Some(listener_handle),
        }
    }

    pub fn get_handle(&self) -> QueueNegotiatorHandle {
        QueueNegotiatorHandle(self.addr)
    }

    pub fn shutdown(&mut self) {
        let mut conn = TcpStream::connect(self.addr).unwrap();
        net_protocol::write(&mut conn, MessageToQueueNegotiator::Shutdown).unwrap();
        self.listener_handle.take().unwrap().join().unwrap();
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
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use super::{AssignedRun, QueueNegotiator, WorkersNegotiator};
    use crate::negotiate::WorkersConfig;
    use crate::workers::WorkerContext;
    use abq_utils::net_protocol::queue::Shutdown;
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, Status, Test, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::workers::{
        InvocationId, NextWork, RunnerKind, TestLikeRunner, WorkContext, WorkId,
    };
    use abq_utils::{flatten_manifest, net_protocol};

    type Messages = Arc<Mutex<Vec<TestResult>>>;
    type ManifestCollector = Arc<Mutex<Option<ManifestMessage>>>;

    type QueueNextWork = (SocketAddr, mpsc::Sender<()>, JoinHandle<()>);
    type QueueResults = (Messages, SocketAddr, JoinHandle<()>);

    fn mock_queue_next_work_server(manifest_collector: ManifestCollector) -> QueueNextWork {
        let server = TcpListener::bind("0.0.0.0:0").unwrap();
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

                        let _invocation_id: InvocationId = net_protocol::read(&mut worker).unwrap();
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

        let server = TcpListener::bind("0.0.0.0:0").unwrap();
        let server_addr = server.local_addr().unwrap();

        let handle = thread::spawn(move || {
            for client in server.incoming() {
                let mut client = client.unwrap();
                let message = net_protocol::read(&mut client).unwrap();
                match message {
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
                    net_protocol::queue::Message::Shutdown(_) => {
                        return;
                    }
                    _ => unreachable!(),
                }
            }
        });

        (msgs, server_addr, handle)
    }

    fn close_queue_servers(
        shutdown_next_work_server: mpsc::Sender<()>,
        next_work_handle: JoinHandle<()>,
        results_addr: SocketAddr,
        results_handle: JoinHandle<()>,
    ) {
        shutdown_next_work_server.send(()).unwrap();
        next_work_handle.join().unwrap();

        let mut stream = TcpStream::connect(results_addr).unwrap();
        net_protocol::write(
            &mut stream,
            net_protocol::queue::Message::Shutdown(Shutdown {}),
        )
        .unwrap();
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
        let (msgs, results_addr, results_handle) = mock_queue_results_server(manifest_collector);

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
            "0.0.0.0:0".parse().unwrap(),
            next_work_addr,
            results_addr,
            get_assigned_run,
        );
        let workers_config = WorkersConfig {
            num_workers: NonZeroUsize::new(1).unwrap(),
            worker_context: WorkerContext::AssumeLocal,
            work_timeout: Duration::from_secs(1),
            work_retries: 0,
        };
        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue_negotiator.get_handle(),
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
            results_addr,
            results_handle,
        );
    }
}
