use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{sync::mpsc, thread};

use abq_echo_worker as echo;
use abq_exec_worker as exec;
use abq_runner_protocol::{Output, Runner};

use abq_utils::net_protocol;
use procspawn as proc;
use serde::{Deserialize, Serialize};

use crate::protocol::{InvocationId, WorkAction, WorkContext, WorkId, WorkUnit, WorkerResult};

#[derive(Serialize, Deserialize)]
struct ReceivedWork(InvocationId, WorkId, WorkUnit);

enum WorkerMessage {
    Work(ReceivedWork),
    Shutdown,
}

type SharedMessageRx = Arc<Mutex<mpsc::Receiver<WorkerMessage>>>;

pub type NotifyResult = Box<dyn Fn(InvocationId, WorkId, WorkerResult) + Send + Sync + 'static>;

#[derive(Clone)]
pub enum WorkerContext {
    /// Assume all context needed to run a unit of work is available in the local filesystem/network namespace.
    AssumeLocal,
    /// Assume all the context we need is available locally, but that we should always work in the
    /// provided directory.
    /// E.g. in CI, we may expect someone to stand up a cloned project directory from which workers
    /// should operate.
    AlwaysWorkIn {
        /// Directory to work in.
        working_dir: PathBuf,
    },
}

/// Configuration for a [WorkerPool].
pub struct WorkerPoolConfig {
    pub size: NonZeroUsize,
    /// How should results be communicated back?
    pub notify_result: NotifyResult,
    /// Context under which workers should operate.
    pub worker_context: WorkerContext,
    /// Timeout for a single unit of work in the pool.
    pub work_timeout: Duration,
    /// How many times failed work processes should be retried.
    ///
    /// NOTE: this refers to retrying a worker process when the process itself has failed, either
    /// during startup or exit. Failure modes related to the result of a cleanly-exiting piece of
    /// work (e.g. a test fails, but the process exits cleanly) are not accounted for here.
    pub work_retries: u8,
}

/// Executes an initialization sequence that must be performed before any worker pool can be created.
/// Do not use this in tests. The initialization sequence for test is done in the [crate root][crate].
#[cfg(not(test))]
pub fn init() {
    // Must initialize the process pool state.
    proc::init();
}

/// Manages a pool of threads and processes upon which work is run and reported back.
///
/// A pool of size N has N worker threads that listen to incoming work (given by
/// [WorkerPool::send_work]). Each thread is given access to a pool of N processes where
/// they execute a given piece of work. Each process in the pool is partially loaded and
/// forked when new work is sent; this is managed by the [procspawn] library.
///
/// Work execution is done in the process pool so that the managing worker threads can easily
/// terminate the process if needed, and panics in the process pool don't induce a panic in the
/// worker pool process.
pub struct WorkerPool {
    process_pool: Arc<proc::Pool>,
    listener_addr: SocketAddr,
    listener_handle: Option<thread::JoinHandle<()>>,
    shutdown_listener_tx: mpsc::Sender<()>,

    active: bool,
}

impl WorkerPool {
    pub fn new(config: WorkerPoolConfig) -> Self {
        let WorkerPoolConfig {
            size,
            notify_result,
            worker_context,
            work_timeout,
            work_retries,
        } = config;

        let num_workers = size.get();
        let process_pool = Arc::new(proc::Pool::new(num_workers).unwrap());
        let mut workers = Vec::with_capacity(num_workers);

        let (worker_msg_tx, worker_msg_rx) = mpsc::channel();
        let shared_worker_msg_rx = Arc::new(Mutex::new(worker_msg_rx));

        let notify_result = Arc::new(notify_result);

        for _id in 0..num_workers {
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let process_pool = Arc::clone(&process_pool);
            let notify_result = Arc::clone(&notify_result);

            workers.push(ThreadWorker::new(
                msg_rx,
                process_pool,
                notify_result,
                worker_context.clone(),
                work_timeout,
                work_retries,
            ));
        }

        // Server to listen for [ReceivedWork] sent to the pool.
        // TODO: allow customizing host/port
        let listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let listener_addr = listener.local_addr().unwrap();
        let (shutdown_listener_tx, shutdown_listener_rx) = mpsc::channel();
        let listener_handle = thread::spawn(move || {
            start_pool_work_listener(listener, shutdown_listener_rx, worker_msg_tx, workers)
        });

        Self {
            process_pool,
            listener_addr,
            listener_handle: Some(listener_handle),
            shutdown_listener_tx,
            active: true,
        }
    }

    pub fn get_handle(&self) -> WorkerPoolHandle {
        WorkerPoolHandle {
            listener_addr: self.listener_addr,
        }
    }

    pub fn shutdown(&mut self) {
        self.active = false;

        self.shutdown_listener_tx.send(()).unwrap();
        self.listener_handle.take().unwrap().join().unwrap();

        self.process_pool.shutdown();
    }
}

fn start_pool_work_listener(
    listener: TcpListener,
    shutdown_rx: mpsc::Receiver<()>,
    worker_msg_tx: mpsc::Sender<WorkerMessage>,
    mut workers: Vec<ThreadWorker>,
) {
    const POLL_WAIT_TIME: Duration = Duration::from_micros(10);

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

                let received_work: ReceivedWork = net_protocol::read(&mut client).unwrap();
                worker_msg_tx
                    .send(WorkerMessage::Work(received_work))
                    .unwrap();
            }
            Err(e) => {
                match e.kind() {
                    ErrorKind::WouldBlock => {
                        // Take this opportunity to check if we should shutdown.
                        if let Ok(()) = shutdown_rx.try_recv() {
                            // Tell each worker to shutdown, then exit ourselves.
                            for _ in &workers {
                                worker_msg_tx.send(WorkerMessage::Shutdown).unwrap();
                            }

                            for mut worker in workers.drain(..) {
                                if let Some(thread) = worker.handle.take() {
                                    thread.join().unwrap();
                                }
                            }

                            break;
                        }

                        // Otherwise we sleep and wait for the next message.
                        thread::sleep(POLL_WAIT_TIME);
                    }
                    _ => panic!("Unhandled IO error: {e:?}"),
                }
            }
        }
    }

    drop(listener)
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        if self.active {
            self.shutdown()
        }
    }
}

/// A handle to an [`WorkerPool`] on the network, from which to send work.
/// A given pool can have many handles; use [`WorkerPool::get_handle`] to get one.
#[derive(Clone, Copy)]
pub struct WorkerPoolHandle {
    listener_addr: SocketAddr,
}

impl WorkerPoolHandle {
    pub fn send_work(&self, invocation_id: InvocationId, work_id: WorkId, work: WorkUnit) {
        let mut conn = TcpStream::connect(self.listener_addr).unwrap();
        net_protocol::write(&mut conn, ReceivedWork(invocation_id, work_id, work)).unwrap();
    }
}

struct ThreadWorker {
    handle: Option<thread::JoinHandle<()>>,
}

enum AttemptError {
    ShouldRetry,
    Panic(String),
    Timeout(Duration),
}

type AttemptResult = Result<Output, AttemptError>;

impl ThreadWorker {
    pub fn new(
        msg_rx: SharedMessageRx,
        pool: Arc<proc::Pool>,
        notify_result: Arc<NotifyResult>,
        context: WorkerContext,
        work_timeout: Duration,
        work_retries: u8,
    ) -> Self {
        let handle = thread::spawn(move || loop {
            let try_message = msg_rx.lock().unwrap().try_recv();

            let ReceivedWork(invocation_id, work_id, work) = match try_message {
                Ok(WorkerMessage::Work(work)) => work,
                Ok(WorkerMessage::Shutdown) => {
                    break;
                }
                Err(mpsc::TryRecvError::Disconnected) => panic!("Pool died before worker did"),
                Err(mpsc::TryRecvError::Empty) => {
                    // Wait for a message to come in
                    continue;
                }
            };

            // We received work to do. Try it once + how ever many retries were requested.
            let allowed_attempts = 1 + work_retries;
            'attempts: for attempt_number in 1.. {
                let attempt_result = attempt_work(
                    &pool,
                    &context,
                    work.clone(),
                    work_timeout,
                    attempt_number,
                    allowed_attempts,
                );

                let result = match attempt_result {
                    Ok(output) => WorkerResult::Output(output),
                    Err(AttemptError::ShouldRetry) => continue 'attempts,
                    Err(AttemptError::Panic(msg)) => WorkerResult::Panic(msg),
                    Err(AttemptError::Timeout(time)) => WorkerResult::Timeout(time),
                };

                notify_result(invocation_id, work_id.clone(), result);
                break 'attempts;
            }
        });

        Self {
            handle: Some(handle),
        }
    }
}

#[inline(always)]
fn attempt_work(
    pool: &Arc<proc::Pool>,
    my_context: &WorkerContext,
    work: WorkUnit,
    timeout: Duration,
    attempt: u8,
    allowed_attempts: u8,
) -> AttemptResult {
    let WorkUnit {
        action,
        context: requested_context,
    } = work;

    let working_dir = resolve_context(my_context, &requested_context).to_owned();

    let handle = proc::spawn!(in pool, (working_dir, action, attempt) || {
        std::env::set_current_dir(working_dir).unwrap();
        multiplex_action(action, attempt)
    });

    let result = handle.join_timeout(timeout);
    match result {
        Ok(output) => Ok(output),
        Err(e) => {
            if attempt < allowed_attempts {
                Err(AttemptError::ShouldRetry)
            } else if let Some(info) = e.panic_info() {
                Err(AttemptError::Panic(info.to_string()))
            } else if e.is_timeout() {
                Err(AttemptError::Timeout(timeout))
            } else if e.is_cancellation() {
                panic!("Never cancelled the job, but error is cancellation");
            } else if e.is_remote_close() {
                panic!("Process in the pool closed our channel before exit");
            } else {
                panic!("Unknown error {e:?}");
            }
        }
    }
}

fn resolve_context<'a>(
    worker_context: &'a WorkerContext,
    work_context: &'a WorkContext,
) -> &'a Path {
    let WorkContext {
        working_dir: asked_working_dir,
    } = work_context;

    match worker_context {
        WorkerContext::AssumeLocal => {
            // Everything is available locally, switch to the directory the work asked for.
            asked_working_dir
        }
        WorkerContext::AlwaysWorkIn { working_dir } => {
            // We must always work in the specified `working_dir`, regardless of what the unit of
            // work thinks.
            // TODO: it would be nice to have some way to verify that our working dir is actually
            // in line with `asked_working_dir`.
            working_dir
        }
    }
}

#[cfg(not(test))]
fn multiplex_action(action: WorkAction, _attempt: u8) -> Output {
    match action {
        WorkAction::Echo(s) => echo::EchoWorker::run(echo::EchoWork { message: s }),
        WorkAction::Exec { cmd, args } => exec::ExecWorker::run(exec::Work { cmd, args }),
    }
}

#[cfg(test)]
fn multiplex_action(action: WorkAction, attempt: u8) -> Output {
    match action {
        WorkAction::Echo(s) => echo::EchoWorker::run(echo::EchoWork { message: s }),
        WorkAction::Exec { cmd, args } => exec::ExecWorker::run(exec::Work { cmd, args }),
        WorkAction::InduceTimeout => {
            thread::sleep(Duration::MAX);
            unreachable!()
        }
        WorkAction::EchoOnRetry(succeed_on, s) => {
            if succeed_on == attempt {
                echo::EchoWorker::run(echo::EchoWork { message: s })
            } else {
                panic!("Failed to echo!");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use abq_runner_protocol::Output;
    use tempfile::TempDir;

    use super::{NotifyResult, WorkerContext, WorkerPool};
    use crate::protocol::{InvocationId, WorkAction, WorkContext, WorkId, WorkUnit, WorkerResult};
    use crate::workers::WorkerPoolConfig;

    type ResultsCollector = Arc<Mutex<HashMap<String, WorkerResult>>>;

    fn results_collector() -> (ResultsCollector, NotifyResult) {
        let results: ResultsCollector = Default::default();
        let results2 = Arc::clone(&results);
        let notify_result: NotifyResult = Box::new(move |_, work_id, result| {
            let old_result = results2.lock().unwrap().insert(work_id.0, result);
            debug_assert!(old_result.is_none(), "Overwriting a result! This is either a bug in your test, or the worker pool implementation.");
        });
        (results, notify_result)
    }

    fn default_pool_config(notify_result: NotifyResult) -> WorkerPoolConfig {
        WorkerPoolConfig {
            size: NonZeroUsize::new(1).unwrap(),
            notify_result,
            worker_context: WorkerContext::AssumeLocal,
            work_timeout: Duration::from_secs(5),
            work_retries: 0,
        }
    }

    fn local_work(action: WorkAction) -> WorkUnit {
        WorkUnit {
            action,
            context: WorkContext {
                working_dir: std::env::current_dir().unwrap(),
            },
        }
    }

    fn test_echo_n(num_workers: usize, num_echos: usize) {
        let (results, notify_result) = results_collector();

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(num_workers).unwrap(),
            ..default_pool_config(notify_result)
        };

        let mut pool = WorkerPool::new(config);

        let mut expected_results = HashMap::new();

        for i in 0..num_echos {
            let echo_string = format!("echo {}", i);
            expected_results.insert(i.to_string(), echo_string.clone());

            pool.get_handle().send_work(
                InvocationId::new(),
                WorkId(i.to_string()),
                local_work(WorkAction::Echo(echo_string)),
            );
        }

        pool.shutdown();

        let results: HashMap<_, _> = results
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    match v {
                        WorkerResult::Output(o) => o.message,
                        res => panic!("unexpected result {res:?}"),
                    },
                )
            })
            .collect();

        assert_eq!(results, expected_results);
    }

    #[test]
    fn test_1_worker_1_echo() {
        test_echo_n(1, 1);
    }

    #[test]
    fn test_2_workers_1_echo() {
        test_echo_n(2, 1);
    }

    #[test]
    fn test_1_worker_2_echos() {
        test_echo_n(1, 2);
    }

    #[test]
    fn test_2_workers_2_echos() {
        test_echo_n(2, 2);
    }

    #[test]
    fn test_2_workers_8_echos() {
        test_echo_n(2, 8);
    }

    #[test]
    fn test_timeout() {
        let (results, notify_result) = results_collector();

        let timeout = Duration::from_millis(1);
        let config = WorkerPoolConfig {
            work_timeout: timeout,
            work_retries: 0,
            ..default_pool_config(notify_result)
        };
        let mut pool = WorkerPool::new(config);

        pool.get_handle().send_work(
            InvocationId::new(),
            WorkId("id1".to_string()),
            local_work(WorkAction::InduceTimeout),
        );
        pool.shutdown();

        assert_eq!(
            results.lock().unwrap().get("id1").unwrap(),
            &WorkerResult::Timeout(timeout)
        );
    }

    #[test]
    fn test_panic_no_retries() {
        let (results, notify_result) = results_collector();

        let config = WorkerPoolConfig {
            work_retries: 0,
            ..default_pool_config(notify_result)
        };
        let mut pool = WorkerPool::new(config);

        pool.get_handle().send_work(
            InvocationId::new(),
            WorkId("id1".to_string()),
            local_work(WorkAction::EchoOnRetry(10, "".to_string())),
        );
        pool.shutdown();

        assert_eq!(
            results.lock().unwrap().get("id1").unwrap(),
            &WorkerResult::Panic("Failed to echo!".to_string())
        );
    }

    #[test]
    fn test_panic_succeed_after_retry() {
        let (results, notify_result) = results_collector();

        let config = WorkerPoolConfig {
            work_retries: 1,
            ..default_pool_config(notify_result)
        };
        let mut pool = WorkerPool::new(config);

        pool.get_handle().send_work(
            InvocationId::new(),
            WorkId("id1".to_string()),
            local_work(WorkAction::EchoOnRetry(2, "okay".to_string())),
        );
        pool.shutdown();

        assert_eq!(
            results.lock().unwrap().get("id1").unwrap(),
            &WorkerResult::Output(Output {
                success: true,
                message: "okay".to_string()
            })
        );
    }

    #[test]
    fn work_in_constant_context() {
        let (results, notify_result) = results_collector();

        let working_dir = TempDir::new().unwrap();
        std::fs::write(working_dir.path().join("testfile"), "testcontent").unwrap();

        let worker_context = WorkerContext::AlwaysWorkIn {
            working_dir: working_dir.path().to_owned(),
        };

        let pool_config = WorkerPoolConfig {
            worker_context,
            ..default_pool_config(notify_result)
        };
        let mut pool = WorkerPool::new(pool_config);

        let action = WorkAction::Exec {
            cmd: "cat".to_string(),
            args: vec!["testfile".to_string()],
        };

        // Send the unit of work with a fake working directory to make sure the worker actually
        // uses the working directory given in the worker's context.
        let fake_working_dir_of_work = PathBuf::from("/zzz/i/do/not/exist");
        assert!(!fake_working_dir_of_work.exists());

        let context = WorkContext {
            working_dir: fake_working_dir_of_work,
        };

        pool.get_handle().send_work(
            InvocationId::new(),
            WorkId("id1".to_string()),
            WorkUnit { action, context },
        );
        pool.shutdown();

        assert_eq!(
            results.lock().unwrap().get("id1").unwrap(),
            &WorkerResult::Output(Output {
                success: true,
                message: "testcontent".to_string()
            })
        );
    }
}
