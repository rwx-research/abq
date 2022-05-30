use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{sync::mpsc, thread};

use abq_echo_worker as echo;
use abq_worker_protocol::{Output, Worker};

use procspawn as proc;
use serde::{Deserialize, Serialize};

use crate::protocol::{WorkAction, WorkId, WorkResult};

#[derive(Serialize, Deserialize)]
enum Message {
    Work(WorkId, WorkAction),
    Shutdown,
}

type SharedMessageRx = Arc<Mutex<mpsc::Receiver<Message>>>;

pub type NotifyResult = Box<dyn Fn(WorkId, WorkResult) + Send + Sync + 'static>;

/// Configuration for a [WorkerPool].
pub struct WorkerPoolConfig {
    pub size: NonZeroUsize,
    /// How should results be communicated back?
    pub notify_result: NotifyResult,
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
    msg_tx: mpsc::Sender<Message>,
    workers: Vec<ThreadWorker>,

    active: bool,
}

impl WorkerPool {
    pub fn new(config: WorkerPoolConfig) -> Self {
        let WorkerPoolConfig {
            size,
            notify_result,
            work_timeout,
            work_retries,
        } = config;

        let size = size.get();
        let process_pool = Arc::new(proc::Pool::new(size).unwrap());
        let mut workers = Vec::with_capacity(size);

        let (msg_tx, msg_rx) = mpsc::channel();
        let shared_msg_rx = Arc::new(Mutex::new(msg_rx));

        let notify_result = Arc::new(notify_result);

        for _id in 0..size {
            let msg_rx = Arc::clone(&shared_msg_rx);
            let process_pool = Arc::clone(&process_pool);
            let notify_result = Arc::clone(&notify_result);

            workers.push(ThreadWorker::new(
                msg_rx,
                process_pool,
                notify_result,
                work_timeout,
                work_retries,
            ));
        }

        Self {
            process_pool,
            msg_tx,
            workers,
            active: true,
        }
    }

    pub fn send_work(&self, id: WorkId, action: WorkAction) {
        self.msg_tx.send(Message::Work(id, action)).unwrap();
    }

    pub fn shutdown(&mut self) {
        self.active = false;

        for _ in &self.workers {
            self.msg_tx.send(Message::Shutdown).unwrap();
        }

        for mut worker in self.workers.drain(..) {
            if let Some(thread) = worker.handle.take() {
                thread.join().unwrap();
            }
        }

        self.process_pool.shutdown();
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        if !self.active {
            self.shutdown()
        }
    }
}

struct ThreadWorker {
    handle: Option<thread::JoinHandle<()>>,
}

impl ThreadWorker {
    pub fn new(
        msg_rx: SharedMessageRx,
        pool: Arc<proc::Pool>,
        notify_result: Arc<NotifyResult>,
        work_timeout: Duration,
        work_retries: u8,
    ) -> Self {
        let handle = thread::spawn(move || loop {
            let try_message = msg_rx.lock().unwrap().try_recv();

            let (id, action) = match try_message {
                Ok(Message::Work(id, action)) => (id, action),
                Ok(Message::Shutdown) => {
                    break;
                }
                Err(mpsc::TryRecvError::Disconnected) => panic!("Pool died before worker did"),
                Err(mpsc::TryRecvError::Empty) => {
                    // Wait for a message to come in
                    continue;
                }
            };

            // We received work to do. Try it once + how ever many retries were requested.
            let mut attempts_left = 1 + work_retries;
            let mut attempt_number = 0;
            'attempts: while attempts_left > 0 {
                attempts_left -= 1;
                attempt_number += 1;

                let action = action.clone();
                let handle = proc::spawn!(in pool, (action, attempt_number) || multiplex_action(action, attempt_number));

                let result = handle.join_timeout(work_timeout);
                match result {
                    Ok(output) => {
                        notify_result(id.clone(), WorkResult::Output(output));
                        break 'attempts;
                    }
                    Err(e) => {
                        if attempts_left > 0 {
                            continue 'attempts;
                        }

                        let result = if let Some(info) = e.panic_info() {
                            WorkResult::Panic(info.message().to_string())
                        } else if e.is_timeout() {
                            WorkResult::Timeout(work_timeout)
                        } else if e.is_cancellation() {
                            panic!("Never cancelled the job, but error is cancellation");
                        } else if e.is_remote_close() {
                            panic!("Process in the pool closed our channel before exit");
                        } else {
                            panic!("Unknown error {e:?}");
                        };

                        notify_result(id.clone(), result);
                    }
                }
            }
        });

        Self {
            handle: Some(handle),
        }
    }
}

#[cfg(not(test))]
fn multiplex_action(action: WorkAction, _attempt: usize) -> Output {
    match action {
        WorkAction::Echo(s) => echo::EchoWorker::run(echo::EchoWork { message: s }),
    }
}

#[cfg(test)]
fn multiplex_action(action: WorkAction, attempt: usize) -> Output {
    match action {
        WorkAction::Echo(s) => echo::EchoWorker::run(echo::EchoWork { message: s }),
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
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use abq_worker_protocol::Output;

    use super::{NotifyResult, WorkerPool};
    use crate::protocol::{WorkAction, WorkId, WorkResult};
    use crate::workers::WorkerPoolConfig;

    type ResultsCollector = Arc<Mutex<HashMap<String, WorkResult>>>;

    fn results_collector() -> (ResultsCollector, NotifyResult) {
        let results: ResultsCollector = Default::default();
        let results2 = Arc::clone(&results);
        let notify_result = Box::new(move |id: WorkId, result: WorkResult| {
            let old_result = results2.lock().unwrap().insert(id.0, result);
            debug_assert!(old_result.is_none(), "Overwriting a result! This is either a bug in your test, or the worker pool implementation.");
        });
        (results, notify_result)
    }

    fn test_echo_n(num_workers: usize, num_echos: usize) {
        let (results, notify_result) = results_collector();

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(num_workers).unwrap(),
            notify_result,
            work_timeout: Duration::from_millis(100),
            work_retries: 0,
        };

        let mut pool = WorkerPool::new(config);

        let mut expected_results = HashMap::new();

        for i in 0..num_echos {
            let echo_string = format!("echo {}", i);
            expected_results.insert(i.to_string(), echo_string.clone());

            pool.send_work(WorkId(i.to_string()), WorkAction::Echo(echo_string));
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
                        WorkResult::Output(o) => o.output,
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
            size: NonZeroUsize::new(1).unwrap(),
            notify_result,
            work_timeout: timeout,
            work_retries: 0,
        };
        let mut pool = WorkerPool::new(config);

        pool.send_work(WorkId("id1".to_string()), WorkAction::InduceTimeout);
        pool.shutdown();

        assert_eq!(
            results.lock().unwrap().get("id1").unwrap(),
            &WorkResult::Timeout(timeout)
        );
    }

    #[test]
    fn test_panic_no_retries() {
        let (results, notify_result) = results_collector();

        let timeout = Duration::from_secs(1);
        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(1).unwrap(),
            notify_result,
            work_timeout: timeout,
            work_retries: 0,
        };
        let mut pool = WorkerPool::new(config);

        pool.send_work(
            WorkId("id1".to_string()),
            WorkAction::EchoOnRetry(10, "".to_string()),
        );
        pool.shutdown();

        assert_eq!(
            results.lock().unwrap().get("id1").unwrap(),
            &WorkResult::Panic("Failed to echo!".to_string())
        );
    }

    #[test]
    fn test_panic_succeed_after_retry() {
        let (results, notify_result) = results_collector();

        let timeout = Duration::from_secs(5);
        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(1).unwrap(),
            notify_result,
            work_timeout: timeout,
            work_retries: 1,
        };
        let mut pool = WorkerPool::new(config);

        pool.send_work(
            WorkId("id1".to_string()),
            WorkAction::EchoOnRetry(2, "okay".to_string()),
        );
        pool.shutdown();

        assert_eq!(
            results.lock().unwrap().get("id1").unwrap(),
            &WorkResult::Output(Output {
                output: "okay".to_string()
            })
        );
    }
}
