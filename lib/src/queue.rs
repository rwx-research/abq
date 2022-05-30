use std::collections::VecDeque;
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tempfile::TempDir;

use crate::notify;
use crate::protocol::{Message, Shutdown, WorkAction, WorkId, WorkResult};
use crate::workers::{NotifyResult, WorkerPool, WorkerPoolConfig};

// TODO: we probably want something more sophisticated here, in particular a concurrent
// work-stealing queue.
#[derive(Default)]
struct JobQueue {
    new_work: VecDeque<(WorkId, WorkAction)>,
    completed_work: Vec<(WorkId, WorkResult)>,
}

impl JobQueue {
    pub fn add_work(&mut self, id: WorkId, action: WorkAction) {
        self.new_work.push_back((id, action));
    }

    pub fn get_work(&mut self) -> Option<(WorkId, WorkAction)> {
        let work = self.new_work.pop_front()?;
        Some(work)
    }

    pub fn add_completed(&mut self, id: WorkId, completed: WorkResult) {
        self.completed_work.push((id, completed));
    }
}

struct TempSocket {
    _owning_dir: TempDir,
    path: PathBuf,
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
    pub fn shutdown_in(&mut self, timeout: Duration) -> Vec<(WorkId, WorkResult)> {
        self.shutdown_help(None, timeout)
    }

    /// Shutdown after there are [n] items in the results list.
    pub fn shutdown_after_n(&mut self, n: usize) -> Vec<(WorkId, WorkResult)> {
        self.shutdown_help(Some(n), Duration::MAX)
    }

    fn shutdown_help(&mut self, n: Option<usize>, timeout: Duration) -> Vec<(WorkId, WorkResult)> {
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

static POLL_WAIT_TIME: Duration = Duration::from_millis(10);

fn read_all_nonpolling(read_from: &mut impl Read) -> io::Result<String> {
    use std::io::ErrorKind;

    let mut msg_buf = String::new();
    loop {
        match read_from.read_to_string(&mut msg_buf) {
            Ok(_) => return Ok(msg_buf),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            Err(e) => return Err(e),
        }
    }
}

fn start_queue_server(stream: UnixListener, queue: SharedJobQueue) {
    stream
        .set_nonblocking(true)
        .expect("Failed to set stream as non-blocking");

    let mut shutdown_inst_and_timeout = None;
    let mut shutdown_after_n_results = None;

    use std::io::ErrorKind;
    for client in stream.incoming() {
        match client {
            Ok(mut client) => {
                let msg_buf = read_all_nonpolling(&mut client).unwrap();
                let msg: Message =
                    serde_json::from_str(&msg_buf).expect("Bad message over the protocol");

                match msg {
                    Message::Work(id, action) => {
                        // If we're only waiting for pending work, no new work is accepted.
                        if shutdown_inst_and_timeout.is_none() {
                            queue.lock().unwrap().add_work(id, action);
                        }
                    }
                    Message::Result(id, result) => queue.lock().unwrap().add_completed(id, result),
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
                        thread::sleep(POLL_WAIT_TIME);
                    }
                    _ => panic!("Unhandled IO error: {e:?}"),
                }
            }
        }
    }
}

fn start_queue_woker(socket: PathBuf, queue: SharedJobQueue, recv: mpsc::Receiver<QueueWorkerMsg>) {
    let notify_result: NotifyResult = Box::new(move |id, result| {
        notify::send_result(&socket, id, result);
    });
    let worker_config = WorkerPoolConfig {
        size: NonZeroUsize::new(4).unwrap(),
        notify_result,
        work_timeout: Duration::from_secs(5),
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
            Some((id, action)) => worker_pool.send_work(id, action),
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
    use std::time::Duration;

    use super::Abq;
    use crate::notify;
    use crate::protocol::{WorkAction, WorkId, WorkResult};
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
        notify::send_work(queue.socket(), id1, WorkAction::Echo("echo1".to_string()));
        notify::send_work(queue.socket(), id2, WorkAction::Echo("echo2".to_string()));
        let mut results = queue.shutdown_after_n(2);

        results.sort_by(|w1, w2| w1.0.cmp(&w2.0));
        let results = results
            .into_iter()
            .map(|(_, output)| match output {
                WorkResult::Output(o) => o.output,
                o => panic!("unexpected output {o:?}"),
            })
            .collect::<Vec<_>>();

        assert_eq!(results.as_ref(), ["echo1", "echo2"]);
    }
}
