use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use abq_utils::flatten_manifest;
use abq_utils::net_protocol::queue::InvokerResponse;
use abq_utils::net_protocol::runners::{TestCase, TestResult};
use abq_utils::net_protocol::workers::{RunnerKind, WorkContext};
use abq_utils::net_protocol::{
    self,
    queue::{InvokeWork, Message, Shutdown},
    workers::{InvocationId, NextWork, WorkId},
};
use abq_workers::negotiate::{AssignedRun, QueueNegotiator, QueueNegotiatorHandle};

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

    pub fn start(negoatiator_bind_addr: SocketAddr) -> Self {
        start_queue(negoatiator_bind_addr)
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

/// Initializes a queue, binding the queue negotiator to the given address.
/// All other public channels to the queue (the work and results server) are exposed
/// on the same host IP address as the negotiator is.
fn start_queue(negoatiator_bind_addr: SocketAddr) -> Abq {
    let bind_hostname = negoatiator_bind_addr.ip();

    let queues: SharedInvocationQueues = Default::default();

    let server_listener = TcpListener::bind((bind_hostname, 0)).unwrap();
    let server_addr = server_listener.local_addr().unwrap();

    let (send_worker, recv_worker) = mpsc::channel();
    let server_handle = thread::spawn({
        let queues = Arc::clone(&queues);
        move || start_queue_server(server_listener, queues)
    });

    let new_work_server = TcpListener::bind((bind_hostname, 0)).unwrap();
    let new_work_server_addr = new_work_server.local_addr().unwrap();

    let work_scheduler_handle = thread::spawn({
        let queues = Arc::clone(&queues);
        let scheduler_config = SchedulerConfig {
            queues,
            msg_recv: recv_worker,
        };
        move || start_work_scheduler(scheduler_config, new_work_server)
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
        negoatiator_bind_addr,
        new_work_server_addr,
        server_addr,
        choose_run_for_worker,
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
                    Message::InvokeWork(InvokeWork {
                        invocation_id,
                        runner,
                    }) => {
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
                        queues.lock().unwrap().create_queue(invocation_id, runner);
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
    WorkResult(WorkId, TestResult),
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
        runners::{Manifest, ManifestMessage, Test, TestOrGroup, TestResult},
        workers::{InvocationId, RunnerKind, TestLikeRunner},
    };
    use abq_workers::{
        negotiate::{WorkersConfig, WorkersNegotiator},
        workers::WorkerContext,
    };
    use ntest::timeout;

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

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_jobs_complete() {
        let mut queue = Abq::start("0.0.0.0:0".parse().unwrap());

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
        let mut queue = Abq::start("0.0.0.0:0".parse().unwrap());

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
}
