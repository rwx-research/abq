use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::protocol::{
    self, InvocationId, InvokeWork, Message, Shutdown, WorkId, WorkerAction, WorkerResult,
};
use crate::workers::{NotifyResult, WorkerPool, WorkerPoolConfig};

type WorkUnit = (InvocationId, WorkId, WorkerAction);

// TODO: we probably want something more sophisticated here, in particular a concurrent
// work-stealing queue.
#[derive(Default)]
struct JobQueue {
    queue: VecDeque<WorkUnit>,
}

impl JobQueue {
    pub fn add_batch_work(&mut self, work: impl Iterator<Item = WorkUnit>) {
        self.queue.extend(work);
    }

    pub fn get_work(&mut self) -> Option<WorkUnit> {
        let work = self.queue.pop_front()?;
        Some(work)
    }
}

type SharedJobQueue = Arc<Mutex<JobQueue>>;

/// Executes an initialization sequence that must be performed before any queue can be created.
/// [init] only needs to be run once in a process, even if multiple [Abq] instances are crated, but
/// it must be run before any instance is created.
/// Do not use this in tests. The initialization sequence for test is done in the [crate root][crate].
#[cfg(not(test))]
pub fn init() {
    // We must initialize the workers, in particular what's needed for the process pool.
    crate::workers::init();
}

pub struct Abq {
    socket: PathBuf,
    server_handle: Option<JoinHandle<()>>,
    send_worker: mpsc::Sender<QueueWorkerMsg>,
    worker_handle: Option<JoinHandle<()>>,

    active: bool,
}

impl Abq {
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub fn start(socket: PathBuf) -> Self {
        start_queue(socket)
    }

    pub fn wait_forever(&mut self) {
        self.server_handle.take().map(JoinHandle::join);
        self.worker_handle.take().map(JoinHandle::join);
    }

    /// Sends a signal to shutdown immediately.
    pub fn shutdown(&mut self) {
        self.shutdown_help(Duration::from_millis(0))
    }

    /// Sends a signal to shutdown after [timeout], and returns the completed work in the queue
    /// once complete.
    pub fn shutdown_in(&mut self, timeout: Duration) {
        self.shutdown_help(timeout)
    }

    fn shutdown_help(&mut self, timeout: Duration) {
        debug_assert!(self.active);

        self.active = false;

        let mut queue_server = UnixStream::connect(self.socket()).expect("server not available");
        protocol::write(&mut queue_server, Message::Shutdown(Shutdown { timeout })).unwrap();

        self.server_handle
            .take()
            .expect("server handle must be available during Drop")
            .join()
            .unwrap();

        // Server has closed; shut down all workers.
        self.send_worker.send(QueueWorkerMsg::Shutdown).unwrap();
        self.worker_handle
            .take()
            .expect("worker handle must be available during Drop")
            .join()
            .unwrap();
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

enum QueueWorkerMsg {
    Shutdown,
}

/// Initializes a queue.
/// Right now the only initialization option is
///   - in-process, the queue taking and acting on work in threads
///   - communication over a unix socket
fn start_queue(socket: PathBuf) -> Abq {
    // TODO: we probably want something more sophisticated here, in particular
    // a concurrent work-stealing queue.
    let queue = Arc::new(Mutex::new(JobQueue::default()));
    let stream = UnixListener::bind(&socket).unwrap();

    let (send_worker, recv_worker) = mpsc::channel();
    let server_handle = thread::spawn({
        let queue = Arc::clone(&queue);
        move || start_queue_server(stream, queue)
    });

    let worker_handle = thread::spawn({
        let socket_path = socket.clone();
        let queue = Arc::clone(&queue);
        move || start_queue_worker(socket_path, queue, recv_worker)
    });

    Abq {
        socket,
        server_handle: Some(server_handle),
        send_worker,
        worker_handle: Some(worker_handle),

        active: true,
    }
}

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

fn start_queue_server(server_listener: UnixListener, queue: SharedJobQueue) {
    // Stores information about shutdown messages received.
    let mut shutdown_inst_and_timeout = None;

    // Make `accept()` non-blocking so that we have time to check whether we should shutdown, if
    // there are no active connections into the queue.
    server_listener
        .set_nonblocking(true)
        .expect("Failed to set stream as non-blocking");

    // invocation -> (results sender, work left, responder thread handle)
    type ActiveInvocations =
        HashMap<InvocationId, (mpsc::Sender<(WorkId, WorkerResult)>, usize, JoinHandle<()>)>;

    // When all results for a particular invocation are communicated, we want to make sure that the
    // responder thread is closed and that the entry here is dropped.
    let mut active_invocations: ActiveInvocations = Default::default();

    use std::io::ErrorKind;
    for client in server_listener.incoming() {
        match client {
            Ok(mut client) => {
                // Reads and writes to the client will be buffered, so we must make sure
                // the stream is blocking. Note that we inherit non-blocking from the server
                // listener, but that this adjustment does not affect the server.
                client
                    .set_nonblocking(false)
                    .expect("Failed to set client stream as blocking");

                let msg = protocol::read(&mut client).expect("Failed to read message");

                match msg {
                    Message::InvokeWork(InvokeWork {
                        invocation_id,
                        work,
                    }) => {
                        if shutdown_inst_and_timeout.is_some() {
                            todo!("send shutdown message to client");
                        }

                        let work_size = work.len();

                        // Spawn a new thread to communicate work results back to the invoker. The
                        // main queue (in this thread) will use a channel to communicate relevant
                        // results as they come in.
                        let (results_tx, results_rx) = mpsc::channel();
                        let responder_handle = thread::spawn(move || {
                            crate::invoke::respond(client, results_rx, work_size)
                        });

                        let old_tx = active_invocations
                            .insert(invocation_id, (results_tx, work_size, responder_handle));
                        debug_assert!(
                            old_tx.is_none(),
                            "Existing transmitter for expected unique invocation id"
                        );

                        // Add all the work to the queue.
                        let work_with_invoker = work
                            .into_iter()
                            .map(|(work_id, action)| (invocation_id, work_id, action));

                        queue.lock().unwrap().add_batch_work(work_with_invoker);
                    }
                    Message::WorkerResult(invocation_id, work_id, result) => {
                        let (results_tx, num_results, _) = active_invocations
                            .get_mut(&invocation_id)
                            .expect("invocation is not active");

                        results_tx.send((work_id, result)).unwrap();

                        // We are done reporting work for this invocation. Make sure the
                        // communicator thread closes and our record of the invocation is dropped.
                        *num_results -= 1;
                        if *num_results == 0 {
                            let (_, _, responder_handle) =
                                active_invocations.remove(&invocation_id).unwrap();
                            responder_handle.join().unwrap();
                        }
                    }
                    Message::Shutdown(Shutdown { timeout }) => {
                        debug_assert!(
                            shutdown_inst_and_timeout.is_none(),
                            "Two shutdown messages received"
                        );

                        shutdown_inst_and_timeout = Some((Instant::now(), timeout));
                    }
                }
            }
            Err(ref e) => {
                match e.kind() {
                    ErrorKind::WouldBlock => {
                        if let Some((shutdown_inst, min_timeout)) = shutdown_inst_and_timeout {
                            let duration_since_shutdown = Instant::now() - shutdown_inst;
                            if duration_since_shutdown >= min_timeout {
                                // The server is done.
                                break;
                            }
                        }

                        // Otherwise we sleep and wait for the next message.
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

fn start_queue_worker(
    queue_socket: PathBuf,
    queue: SharedJobQueue,
    recv: mpsc::Receiver<QueueWorkerMsg>,
) {
    let notify_result: NotifyResult = Box::new(move |invocation_id, work_id, result| {
        let mut queue_stream = UnixStream::connect(&queue_socket).expect("socket not available");
        protocol::write(
            &mut queue_stream,
            Message::WorkerResult(invocation_id, work_id, result),
        )
        .unwrap();
    });

    let worker_config = WorkerPoolConfig {
        size: NonZeroUsize::new(4).unwrap(),
        notify_result,
        work_timeout: Duration::from_secs(60),
        work_retries: 1,
    };
    let mut worker_pool = WorkerPool::new(worker_config);

    loop {
        match recv.try_recv() {
            Ok(QueueWorkerMsg::Shutdown) => {
                // We've been asked to shutdown
                break;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                // The corresponding sender has closed; assume this means we should close too.
                break;
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }

        let opt_work = queue.lock().unwrap().get_work();
        match opt_work {
            Some((invocation_id, work_id, action)) => {
                worker_pool.send_work(invocation_id, work_id, action)
            }
            None => {
                // Relinquish the thread before we poll again to avoid spinning cpu.
                thread::sleep(POLL_WAIT_TIME);
            }
        }
    }

    worker_pool.shutdown()
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::Abq;
    use crate::invoke;
    use crate::protocol::{WorkId, WorkerAction, WorkerResult};
    use ntest::timeout;
    use tempfile::TempDir;

    fn temp_socket() -> (TempDir, PathBuf) {
        let socket_dir = tempfile::TempDir::new().unwrap();
        let socket_path = socket_dir.path().join("abq.socket");
        (socket_dir, socket_path)
    }

    #[test]
    fn socket_alive() {
        let (_dir, socket) = temp_socket();
        let queue = Abq::start(socket);
        assert!(queue.socket().exists());
    }

    fn sort_results(results: &mut [(String, WorkerResult)]) -> Vec<(&str, &str)> {
        results.sort_by(|w1, w2| w1.0.cmp(&w2.0));
        results
            .iter()
            .map(|(id, output)| match output {
                WorkerResult::Output(o) => (id.as_str(), o.output.as_str()),
                o => panic!("unexpected output {o:?}"),
            })
            .collect::<Vec<_>>()
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_jobs_complete() {
        let (_dir, socket) = temp_socket();
        let mut queue = Abq::start(socket);

        let id1 = WorkId("Test1".to_string());
        let id2 = WorkId("Test2".to_string());
        let work = vec![
            (id1, WorkerAction::Echo("echo1".to_string())),
            (id2, WorkerAction::Echo("echo2".to_string())),
        ];

        let mut results = Vec::default();
        invoke::invoke_work(queue.socket(), work, |id, result| {
            results.push((id.0, result))
        });

        queue.shutdown();

        let results = sort_results(&mut results);

        assert_eq!(results, [("Test1", "echo1"), ("Test2", "echo2")]);
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_invokers() {
        let (_dir, socket) = temp_socket();
        let mut queue = Abq::start(socket);

        let id1 = WorkId("Test1".to_string());
        let id2 = WorkId("Test2".to_string());
        let work = vec![
            (id1, WorkerAction::Echo("echo1".to_string())),
            (id2, WorkerAction::Echo("echo2".to_string())),
        ];

        let mut results1 = Vec::default();
        invoke::invoke_work(queue.socket(), work, |id, result| {
            results1.push((id.0, result))
        });

        let id1 = WorkId("Test3".to_string());
        let id2 = WorkId("Test4".to_string());
        let work = vec![
            (id1, WorkerAction::Echo("echo3".to_string())),
            (id2, WorkerAction::Echo("echo4".to_string())),
        ];

        let mut results2 = Vec::default();
        invoke::invoke_work(queue.socket(), work, |id, result| {
            results2.push((id.0, result))
        });

        queue.shutdown();

        let results1 = sort_results(&mut results1);
        let results2 = sort_results(&mut results2);

        assert_eq!(results1, [("Test1", "echo1"), ("Test2", "echo2")]);
        assert_eq!(results2, [("Test3", "echo3"), ("Test4", "echo4")]);
    }
}
