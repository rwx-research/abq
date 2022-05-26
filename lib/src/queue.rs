use std::collections::VecDeque;
use std::io::Read;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tempfile::TempDir;

use crate::notify;
use crate::protocol::{Message, Shutdown, WorkAction, WorkItem, WorkResult};

// TODO: we probably want something more sophisticated here, in particular a concurrent
// work-stealing queue.
#[derive(Default)]
struct JobQueue {
    new_work: VecDeque<WorkItem>,
    completed_work: Vec<WorkResult>,
}

impl JobQueue {
    pub fn add_work(&mut self, item: WorkItem) {
        self.new_work.push_back(item);
    }

    pub fn get_work(&mut self) -> Option<WorkItem> {
        let work = self.new_work.pop_front()?;
        Some(work)
    }

    pub fn add_completed(&mut self, completed: WorkResult) {
        self.completed_work.push(completed);
    }
}

struct TempSocket {
    _owning_dir: TempDir,
    path: PathBuf,
}

type SharedJobQueue = Arc<Mutex<JobQueue>>;

pub struct Abq {
    queue: SharedJobQueue,

    socket: TempSocket,
    server_handle: Option<JoinHandle<()>>,
    send_worker: mpsc::Sender<QueueWorkerMsg>,
    worker_handle: Option<JoinHandle<()>>,

    active: bool,
}

impl Abq {
    pub fn socket(&self) -> &Path {
        &self.socket.path
    }

    pub fn start() -> Self {
        start_queue()
    }

    /// Sends a signal to shutdown after [timeout], and returns the completed work in the queue
    /// once complete.
    pub fn shutdown_in(&mut self, timeout: Duration) -> Vec<WorkResult> {
        self.shutdown_help(None, timeout)
    }

    /// Shutdown after there are [n] items in the results list.
    pub fn shutdown_after_n(&mut self, n: usize) -> Vec<WorkResult> {
        self.shutdown_help(Some(n), Duration::MAX)
    }

    fn shutdown_help(&mut self, n: Option<usize>, timeout: Duration) -> Vec<WorkResult> {
        debug_assert!(self.active);

        self.active = false;

        notify::send_shutdown(self.socket(), n, timeout);

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

        let mut results = vec![];
        let queue = &mut *self.queue.lock().unwrap();
        std::mem::swap(&mut results, &mut queue.completed_work);
        results
    }
}

impl Drop for Abq {
    fn drop(&mut self) {
        if self.active {
            // Our user never called shutdown; try to perform a clean exit.
            self.shutdown_in(Duration::from_millis(0));
        }
    }
}

enum QueueWorkerMsg {
    Shutdown,
}

// type NotifyStart = Arc<(Mutex<bool>, Condvar)>;

/// Initializes a queue.
/// Right now the only initialization option is
///   - in-process, the queue taking and acting on work in threads
///   - communication over a unix socket
fn start_queue() -> Abq {
    // Create a fresh socket for the queue. In time we'll want ephemeral sockets (for a daemon),
    // and other communication options.
    let socket_dir = tempfile::TempDir::new().unwrap();
    let socket_path = socket_dir.path().join("abq.socket");

    // TODO: we probably want something more sophisticated here, in particular
    // a concurrent work-stealing queue.
    let queue = Arc::new(Mutex::new(JobQueue::default()));
    let stream = UnixListener::bind(&socket_path).unwrap();

    let (send_worker, recv_worker) = mpsc::channel();
    let server_handle = thread::spawn({
        let queue = Arc::clone(&queue);
        move || start_queue_server(stream, queue)
    });

    let worker_handle = thread::spawn({
        let socket_path = socket_path.clone();
        let queue = Arc::clone(&queue);
        move || start_queue_woker(socket_path, queue, recv_worker)
    });

    Abq {
        queue,
        socket: TempSocket {
            _owning_dir: socket_dir,
            path: socket_path,
        },
        server_handle: Some(server_handle),
        send_worker,
        worker_handle: Some(worker_handle),

        active: true,
    }
}

fn start_queue_server(stream: UnixListener, queue: SharedJobQueue) {
    stream
        .set_nonblocking(true)
        .expect("Failed to set stream as non-blocking");

    let mut shutdown_inst_and_timeout = None;
    let mut shutdown_after_n_results = None;
    let message_wait_time = Duration::from_millis(10);

    use std::io::ErrorKind;
    for client in stream.incoming() {
        match client {
            Ok(mut client) => {
                let mut msg_buf = String::new();
                loop {
                    match client.read_to_string(&mut msg_buf) {
                        Ok(_) => break,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                        Err(e) => panic!("Unhandled IO error: {e:?}"),
                    }
                }

                let msg: Message =
                    serde_json::from_str(&msg_buf).expect("Bad message over the protocol");

                match msg {
                    Message::Work(work_item) => {
                        // If we're only waiting for pending work, no new work is accepted.
                        if shutdown_inst_and_timeout.is_none() {
                            queue.lock().unwrap().add_work(work_item);
                        }
                    }
                    Message::Result(result) => queue.lock().unwrap().add_completed(result),
                    Message::Shutdown(Shutdown {
                        timeout,
                        opt_expect_n,
                    }) => {
                        debug_assert!(shutdown_inst_and_timeout.is_none());
                        debug_assert!(shutdown_after_n_results.is_none());

                        shutdown_inst_and_timeout = Some((Instant::now(), timeout));
                        shutdown_after_n_results = opt_expect_n;
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

                        if let Some(n) = shutdown_after_n_results {
                            if queue.lock().unwrap().completed_work.len() >= n {
                                break;
                            }
                        }

                        // Otherwise we sleep and wait for the next message.
                        //
                        // Idea for (much) later: if it's been a long time since the last Work
                        // message, it's likely that there are a burst of pending jobs that need to
                        // be completed. In this case we may want to consider sleeping longer than
                        // the default timeout.
                        thread::sleep(message_wait_time);
                    }
                    _ => panic!("Unhandled IO error: {e:?}"),
                }
            }
        }
    }
}

fn start_queue_woker(socket: PathBuf, queue: SharedJobQueue, recv: mpsc::Receiver<QueueWorkerMsg>) {
    loop {
        if matches!(
            recv.try_recv(),
            Ok(QueueWorkerMsg::Shutdown) | Err(mpsc::TryRecvError::Disconnected)
        ) {
            // Time to close the queue.
            break;
        }

        if let Some(WorkItem { id, action }) = queue.lock().unwrap().get_work() {
            match action {
                WorkAction::Echo(s) => notify::send_result(&socket, WorkResult { id, message: s }),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::Abq;
    use crate::notify;
    use crate::protocol::{WorkAction, WorkId, WorkItem, WorkResult};
    use ntest::timeout;

    #[test]
    fn socket_alive() {
        let mut queue = Abq::start();
        assert!(queue.socket().exists());
        let results = queue.shutdown_in(Duration::from_millis(0));
        assert!(results.is_empty());
    }

    #[test]
    #[timeout(1000)] // 1 second
    fn multiple_jobs_complete() {
        let mut queue = Abq::start();

        let id1 = WorkId("Test1".to_string());
        let id2 = WorkId("Test2".to_string());
        notify::send_work(
            queue.socket(),
            WorkItem {
                id: id1.clone(),
                action: WorkAction::Echo("echo1".to_string()),
            },
        );
        notify::send_work(
            queue.socket(),
            WorkItem {
                id: id2.clone(),
                action: WorkAction::Echo("echo2".to_string()),
            },
        );
        let mut results = queue.shutdown_after_n(2);

        results.sort_by(|w1, w2| w1.id.cmp(&w2.id));

        assert_eq!(
            results,
            vec![
                WorkResult {
                    id: id1,
                    message: "echo1".to_string()
                },
                WorkResult {
                    id: id2,
                    message: "echo2".to_string()
                }
            ]
        );
    }
}
