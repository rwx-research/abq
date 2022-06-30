use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use abq_utils::flatten_manifest;
use abq_utils::net_protocol::queue::InvokerResponse;
use abq_utils::net_protocol::runners::TestId;
use abq_utils::net_protocol::workers::WorkContext;
use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, Message, Shutdown},
    workers::{InvocationId, NextWork, WorkId, WorkerResult},
};
use abq_workers::negotiate::{QueueNegotiator, QueueNegotiatorHandle};

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
    WaitingForManifest,
    HasWork(JobQueue),
    Done,
}

struct WaitingForManifestError;

#[derive(Default)]
struct InvocationQueues {
    /// One queue per `abq test ...` invocation, at least for now.
    queues: HashMap<InvocationId, InvocationState>,
}

impl InvocationQueues {
    pub fn create_queue(&mut self, invocation_id: InvocationId) {
        let old_queue = self
            .queues
            .insert(invocation_id, InvocationState::WaitingForManifest);
        debug_assert!(old_queue.is_none());
    }

    pub fn add_manifest(&mut self, invocation_id: InvocationId, flat_manifest: Vec<TestId>) {
        let state = self
            .queues
            .get_mut(&invocation_id)
            .expect("no queue for invocation");

        let work_from_manifest = flat_manifest.into_iter().map(|test_id| {
            NextWork::Work {
                test_id,
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
        match state
        {
            InvocationState::WaitingForManifest => unreachable!("Invalid state - can't mark invocation queue complete when we still need the manifest!"),
            InvocationState::HasWork(queue) => {
                assert!(queue.get_work().is_none(), "Invalid state - queue is not complete!");
            }
            InvocationState::Done => {
                unreachable!("Invalid state - trying to mark state as done twice!");
            }
        }

        *state = InvocationState::Done;
    }

    // Chooses one invocation id for a set of workers to attach to. Returns `None` if there are
    // none.
    pub fn choose(&self) -> Option<InvocationId> {
        self.queues.iter().next().map(|(id, _)| *id)
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
    server_handle: Option<JoinHandle<()>>,
    send_worker: mpsc::Sender<WorkerSchedulerMsg>,
    work_scheduler_handle: Option<JoinHandle<()>>,
    negotiator: QueueNegotiator,

    active: bool,
}

impl Abq {
    pub fn server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    pub fn start() -> Self {
        start_queue()
    }

    pub fn wait_forever(&mut self) {
        self.server_handle.take().map(JoinHandle::join);
        self.work_scheduler_handle.take().map(JoinHandle::join);
    }

    /// Sends a signal to shutdown immediately.
    pub fn shutdown(&mut self) {
        debug_assert!(self.active);

        self.active = false;

        let mut queue_server =
            TcpStream::connect(self.server_addr()).expect("server not available");
        net_protocol::write(&mut queue_server, Message::Shutdown(Shutdown {})).unwrap();

        self.negotiator.shutdown();

        self.server_handle
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

/// Initializes a queue.
fn start_queue() -> Abq {
    let queues: SharedInvocationQueues = Default::default();

    let server_listener = TcpListener::bind("0.0.0.0:0").unwrap();
    let server_addr = server_listener.local_addr().unwrap();

    let (send_worker, recv_worker) = mpsc::channel();
    let server_handle = thread::spawn({
        let queues = Arc::clone(&queues);
        move || start_queue_server(server_listener, queues)
    });

    let new_work_server = TcpListener::bind("0.0.0.0:0").unwrap();
    let new_work_server_addr = new_work_server.local_addr().unwrap();

    let work_scheduler_handle = thread::spawn({
        let queues = Arc::clone(&queues);
        let scheduler_config = SchedulerConfig {
            queues,
            msg_recv: recv_worker,
        };
        move || start_work_scheduler(scheduler_config, new_work_server)
    });

    // Chooses one invocation id for a set of workers to attach to.
    // This blocks until there is an invocation available.
    // TODO: make sure what the worker is configured for actually agrees with what the invocation
    // was created for.
    let choose_invocation_queue_for_worker = move || loop {
        let opt_invocation = { queues.lock().unwrap().choose() };
        match opt_invocation {
            None => {
                // Nothing yet; release control and sleep for a bit in case something comes.
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Some(id) => {
                return id;
            }
        }
    };
    let negotiator = QueueNegotiator::new(
        new_work_server_addr,
        server_addr,
        choose_invocation_queue_for_worker,
    );

    Abq {
        server_addr,
        server_handle: Some(server_handle),
        send_worker,
        work_scheduler_handle: Some(work_scheduler_handle),
        negotiator,

        active: true,
    }
}

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

fn start_queue_server(server_listener: TcpListener, queues: SharedInvocationQueues) {
    // Make `accept()` non-blocking so that we have time to check whether we should shutdown, if
    // there are no active connections into the queue.
    server_listener
        .set_nonblocking(true)
        .expect("Failed to set stream as non-blocking");

    // invocation -> (results sender, responder thread handle)
    type ActiveInvocations =
        HashMap<InvocationId, (mpsc::Sender<InvocationResponderMsg>, JoinHandle<()>)>;

    // When all results for a particular invocation are communicated, we want to make sure that the
    // responder thread is closed and that the entry here is dropped.
    let mut active_invocations: ActiveInvocations = Default::default();
    // Cache of how much is left in the queue for a particular invocation, so that we don't have to
    // lock the queue all the time.
    let mut work_left_for_invocations: HashMap<InvocationId, usize> = Default::default();

    for client in server_listener.incoming() {
        match client {
            Ok(mut client) => {
                // Reads and writes to the client will be buffered, so we must make sure
                // the stream is blocking. Note that we inherit non-blocking from the server
                // listener, but that this adjustment does not affect the server.
                client
                    .set_nonblocking(false)
                    .expect("Failed to set client stream as blocking");

                let msg = net_protocol::read(&mut client).expect("Failed to read message");

                match msg {
                    Message::InvokeWork(InvokeWork { invocation_id }) => {
                        // Spawn a new thread to communicate work results back to the invoker. The
                        // main queue (in this thread) will use a channel to communicate relevant
                        // results as they come in.
                        let (results_tx, results_rx) = mpsc::channel();
                        let responder_handle =
                            thread::spawn(move || start_invocation_responder(client, results_rx));

                        let old_tx = active_invocations
                            .insert(invocation_id, (results_tx, responder_handle));
                        debug_assert!(
                            old_tx.is_none(),
                            "Existing transmitter for expected unique invocation id"
                        );

                        // Create a new work queue for this invocation.
                        queues.lock().unwrap().create_queue(invocation_id);
                    }
                    Message::Manifest(invocation_id, manifest) => {
                        // Record the manifest for this invocation in its appropriate queue.
                        // TODO: actually record the manifest metadata
                        let flat_manifest = flatten_manifest(manifest.manifest);
                        work_left_for_invocations.insert(invocation_id, flat_manifest.len());
                        queues
                            .lock()
                            .unwrap()
                            .add_manifest(invocation_id, flat_manifest);
                    }
                    Message::WorkerResult(invocation_id, work_id, result) => {
                        let (results_tx, _) = active_invocations
                            .get_mut(&invocation_id)
                            .expect("invocation is not active");

                        results_tx
                            .send(InvocationResponderMsg::WorkResult(work_id, result))
                            .unwrap();

                        let work_left = work_left_for_invocations
                            .get_mut(&invocation_id)
                            .expect("invocation id not recorded, but we got a result for it?");

                        *work_left -= 1;
                        if *work_left == 0 {
                            // Results for this invocation are all done. Let the notifier thread
                            // know, and clean up all remenants of this invocation so as to avoid
                            // memory leaks.
                            let (results_tx, responder_handle) =
                                active_invocations.remove(&invocation_id).unwrap();

                            results_tx.send(InvocationResponderMsg::EndOfWork).unwrap();
                            responder_handle.join().unwrap();

                            work_left_for_invocations.remove(&invocation_id);
                            queues.lock().unwrap().mark_complete(invocation_id);
                        }
                    }
                    Message::Shutdown(Shutdown {}) => {
                        break;
                    }
                }
            }
            Err(ref e) => {
                match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        // Sleep and wait for the next message.
                        //
                        // Idea for (much) later: if it's been a long time since the last Work
                        // message, it's likely that there are a burst of pending jobs that need to
                        // be completed. In this case we may want to consider sleeping longer than
                        // the default timeout.
                        thread::sleep(POLL_WAIT_TIME);
                    }
                    _ => panic!("Unhandled IO error: {e:?}"),
                }
            }
        }
    }
}

/// Message to send to the invocation responder thread.
enum InvocationResponderMsg {
    WorkResult(WorkId, WorkerResult),
    EndOfWork,
}

/// Communicates results of work back to an invoker as they are received by the queue.
/// Once all the work submitted is sent back, the connection to the invoker is terminated.
fn start_invocation_responder(
    mut invoker: TcpStream,
    results_rx: mpsc::Receiver<InvocationResponderMsg>,
) {
    loop {
        match results_rx.recv() {
            Ok(InvocationResponderMsg::WorkResult(work_id, work_result)) => {
                net_protocol::write(&mut invoker, InvokerResponse::Result(work_id, work_result))
                    .unwrap();
            }
            Ok(InvocationResponderMsg::EndOfWork) => break,
            Err(_) => {
                // TODO: there are many reasonable cases why there might be a receive error,
                // including the invoker process was terminated.
                // This needs to be handled gracefully, and we must tell the parent that it
                // should drop the channel results are being sent over when it happens.
                panic!("Invoker shutdown before all our messages were received")
            }
        }
    }
    net_protocol::write(&mut invoker, InvokerResponse::EndOfResults).unwrap();
    invoker.shutdown(std::net::Shutdown::Write).unwrap();
}

struct SchedulerConfig {
    queues: SharedInvocationQueues,
    /// Channel on which the scheduler itself will receive messages from the parent queue.
    msg_recv: mpsc::Receiver<WorkerSchedulerMsg>,
}

/// Starts an out-of-band work scheduler to send work as workers request it.
fn start_work_scheduler(config: SchedulerConfig, listener: TcpListener) {
    let SchedulerConfig { queues, msg_recv } = config;

    // Make `accept()` non-blocking so that we have time to check whether we should shutdown, if
    // there are no active connections into the queue.
    listener
        .set_nonblocking(true)
        .expect("Failed to set stream as non-blocking");

    for client in listener.incoming() {
        match client {
            Ok(mut client) => {
                // Reads and writes to the client will be buffered, so we must make sure
                // the stream is blocking. Note that we inherit non-blocking from the server
                // listener, but that this adjustment does not affect the server.
                client
                    .set_nonblocking(false)
                    .expect("Failed to set client stream as blocking");

                // Get the invocation this worker wants work for.
                // TODO: error handling
                let invocation_id: InvocationId =
                    net_protocol::read(&mut client).expect("Failed to read message");

                // Pull the next work item.
                let next_work = loop {
                    let opt_work = { queues.lock().unwrap().next_work(invocation_id) };
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
                net_protocol::write(&mut client, next_work).unwrap();
            }
            Err(ref e) => {
                match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        match msg_recv.try_recv() {
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

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use super::Abq;
    use crate::invoke;
    use abq_utils::net_protocol::{
        runners::{Manifest, ManifestMessage, Test, TestOrGroup},
        workers::{RunnerKind, TestLikeRunner, WorkerResult},
    };
    use abq_workers::{
        negotiate::{WorkersConfig, WorkersNegotiator},
        workers::WorkerContext,
    };
    use ntest::timeout;

    fn sort_results(results: &mut [(String, WorkerResult)]) -> Vec<&str> {
        let mut results = results
            .iter()
            .map(|(_id, output)| match output {
                WorkerResult::Output(o) => (o.message.as_str()),
                o => panic!("unexpected output {o:?}"),
            })
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

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_jobs_complete() {
        let mut queue = Abq::start();

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                ],
            },
        };

        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            runner_kind: RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(1),
        };

        let queue_server_addr = queue.server_addr();
        let results_handle = thread::spawn(move || {
            let mut results = Vec::default();
            invoke::invoke_work(queue_server_addr, |id, result| results.push((id.0, result)));
            results
        });

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
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
    #[ignore = "TODO: this doesn't work yet because we don't keep track of which workers are doing what invocations, nor do we schedule work for various invocations correctly."]
    fn multiple_invokers() {
        let mut queue = Abq::start();

        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![
                    echo_test("echo1".to_string()),
                    echo_test("echo2".to_string()),
                ],
            },
        };

        let workers_config = WorkersConfig {
            num_workers: 4.try_into().unwrap(),
            runner_kind: RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest),
            worker_context: WorkerContext::AssumeLocal,
            work_retries: 0,
            work_timeout: Duration::from_secs(1),
        };

        let queue_server_addr = queue.server_addr();

        let results1_handle = thread::spawn(move || {
            let mut results = Vec::default();
            invoke::invoke_work(queue_server_addr, |id, result| results.push((id.0, result)));
            results
        });

        let results2_handle = thread::spawn(move || {
            let mut results = Vec::default();
            invoke::invoke_work(queue_server_addr, |id, result| results.push((id.0, result)));
            results
        });

        let mut workers = WorkersNegotiator::negotiate_and_start_pool(
            workers_config,
            queue.get_negotiator_handle(),
        )
        .unwrap();

        let mut results1 = results1_handle.join().unwrap();
        let mut results2 = results2_handle.join().unwrap();

        queue.shutdown();
        workers.shutdown();

        let results1 = sort_results(&mut results1);
        let results2 = sort_results(&mut results2);

        assert_eq!(results1, [("echo1"), ("echo2")]);
        assert_eq!(results2, [("echo3"), ("echo4")]);
    }
}
