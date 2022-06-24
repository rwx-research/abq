use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{sync::mpsc, thread};

use abq_echo_worker as echo;
use abq_exec_worker as exec;
use abq_runner_protocol::Runner;
use abq_utils::net_protocol::runners::{Action, Manifest};
use abq_utils::net_protocol::workers::TestLikeRunner;
use abq_utils::net_protocol::{
    runners::Output,
    workers::{InvocationId, NextWork, RunnerKind, WorkContext, WorkId, WorkerResult},
};

use procspawn as proc;

enum MessageFromPool {
    Shutdown,
}

type MessageFromPoolRx = Arc<Mutex<mpsc::Receiver<MessageFromPool>>>;

pub type GetNextWork = Box<dyn Fn() -> NextWork + Send + Sync + 'static>;
pub type NotifyManifest = Box<dyn Fn(InvocationId, Manifest) + Send + Sync + 'static>;
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
    /// Number of workers.
    pub size: NonZeroUsize,
    /// The kind of runners the workers should start.
    pub runner_kind: RunnerKind,
    /// The work invocation we're working for.
    pub invocation_id: InvocationId,
    /// When [`Some`], one worker must be chosen to generate and return the manifest the following
    /// way.
    pub notify_manifest: Option<NotifyManifest>,
    /// How should a worker get the next unit of work it needs to run?
    pub get_next_work: GetNextWork,
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
    active: bool,
    workers: Vec<ThreadWorker>,
    worker_msg_tx: mpsc::Sender<MessageFromPool>,
}

impl WorkerPool {
    pub fn new(config: WorkerPoolConfig) -> Self {
        let WorkerPoolConfig {
            size,
            runner_kind,
            invocation_id,
            get_next_work,
            notify_result,
            notify_manifest,
            worker_context,
            work_timeout,
            work_retries,
        } = config;

        let num_workers = size.get();
        let mut workers = Vec::with_capacity(num_workers);

        let (worker_msg_tx, worker_msg_rx) = mpsc::channel();
        let shared_worker_msg_rx = Arc::new(Mutex::new(worker_msg_rx));

        // TODO: can we get rid of ref-counting here?
        let notify_result = Arc::new(notify_result);
        let get_next_work = Arc::new(get_next_work);

        {
            // Provision the first worker independently, so that if we need to generate a manifest,
            // only it gets the manifest notifier.
            // TODO: consider hiding this behind a macro for code duplication purposes
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let notify_result = Arc::clone(&notify_result);
            let get_next_work = Arc::clone(&get_next_work);

            let worker_env = WorkerEnv {
                msg_from_pool_rx: msg_rx,
                invocation_id,
                get_next_work,
                notify_result,
                context: worker_context.clone(),
                work_timeout,
                work_retries,
                notify_manifest,
            };

            workers.push(ThreadWorker::new(runner_kind.clone(), worker_env));
        }

        // Provision the rest of the workers.
        for _id in 1..num_workers {
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let notify_result = Arc::clone(&notify_result);
            let get_next_work = Arc::clone(&get_next_work);

            let worker_env = WorkerEnv {
                msg_from_pool_rx: msg_rx,
                invocation_id,
                get_next_work,
                notify_result,
                context: worker_context.clone(),
                work_timeout,
                work_retries,
                notify_manifest: None,
            };

            workers.push(ThreadWorker::new(runner_kind.clone(), worker_env));
        }

        Self {
            active: true,
            workers,
            worker_msg_tx,
        }
    }

    pub fn shutdown(&mut self) {
        debug_assert!(self.active);

        self.active = false;

        for _ in 0..self.workers.len() {
            // It's possible the worker already exited if it couldn't connect to the queue; in that
            // case, we won't be able to send across the channel, but that's okay.
            let _ = self.worker_msg_tx.send(MessageFromPool::Shutdown);
        }

        for worker_thead in self.workers.iter_mut() {
            worker_thead
                .handle
                .take()
                .expect("worker thread already stolen")
                .join()
                .unwrap();
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        if self.active {
            self.shutdown()
        }
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

struct WorkerEnv {
    msg_from_pool_rx: MessageFromPoolRx,
    invocation_id: InvocationId,
    notify_manifest: Option<NotifyManifest>,
    get_next_work: Arc<GetNextWork>,
    notify_result: Arc<NotifyResult>,
    context: WorkerContext,
    work_timeout: Duration,
    work_retries: u8,
}

impl ThreadWorker {
    pub fn new(runner_kind: RunnerKind, worker_env: WorkerEnv) -> Self {
        let handle = thread::spawn(move || match runner_kind {
            RunnerKind::GenericTestRunner => todo!(),
            RunnerKind::TestLikeRunner(runner, manifest) => {
                start_test_like_runner(worker_env, runner, manifest)
            }
        });

        Self {
            handle: Some(handle),
        }
    }
}

fn start_test_like_runner(env: WorkerEnv, runner: TestLikeRunner, manifest: Manifest) {
    let WorkerEnv {
        msg_from_pool_rx,
        get_next_work,
        invocation_id,
        notify_result,
        context,
        work_timeout,
        work_retries,
        notify_manifest,
    } = env;

    if let Some(notify_manifest) = notify_manifest {
        notify_manifest(invocation_id, manifest);
    }

    loop {
        // First, check if we have a message from our owner.
        let parent_message = msg_from_pool_rx.lock().unwrap().try_recv();

        let (action, work_context, invocation_id, work_id) = match parent_message {
            Ok(MessageFromPool::Shutdown) => {
                return;
            }
            Err(mpsc::TryRecvError::Disconnected) => panic!("Pool died before worker did"),
            Err(mpsc::TryRecvError::Empty) => {
                // No message from the parent. Wait for the next action to come in.
                //
                // TODO: add a timeout here, in case we get a message from the parent while
                // blocking on the next action from the queue.
                match get_next_work() {
                    NextWork::EndOfWork => {
                        // Shut down the worker
                        return;
                    }
                    NextWork::Work {
                        action,
                        context,
                        invocation_id,
                        work_id,
                    } => (action, context, invocation_id, work_id),
                }
            }
        };

        // Try the action once + how ever many retries were requested.
        let allowed_attempts = 1 + work_retries;
        'attempts: for attempt_number in 1.. {
            let attempt_result = attempt_action_for_test_like_runner(
                &context,
                runner,
                action.clone(),
                &work_context,
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
    }
}

#[inline(always)]
fn attempt_action_for_test_like_runner(
    my_context: &WorkerContext,
    runner: TestLikeRunner,
    action: Action,
    requested_context: &WorkContext,
    timeout: Duration,
    attempt: u8,
    allowed_attempts: u8,
) -> AttemptResult {
    let working_dir = resolve_context(my_context, requested_context).to_owned();

    use Action as A;
    use TestLikeRunner as R;

    let result_handle = proc::spawn!(
        (runner, action, working_dir, attempt) || {
            std::env::set_current_dir(working_dir).unwrap();
            let _attempt = attempt;
            match (runner, action) {
                (R::Echo, A::Echo(s)) => echo::EchoWorker::run(echo::EchoWork { message: s }),
                (R::Exec, A::Exec { cmd, args }) => exec::ExecWorker::run(exec::Work { cmd, args }),
                #[cfg(feature = "test-actions")]
                (R::InduceTimeout, _) => {
                    thread::sleep(Duration::MAX);
                    unreachable!()
                }
                #[cfg(feature = "test-actions")]
                (R::EchoOnRetry(succeed_on), A::Echo(s)) => {
                    if succeed_on == _attempt {
                        echo::EchoWorker::run(echo::EchoWork { message: s })
                    } else {
                        panic!("Failed to echo!");
                    }
                }
                (runner, action) => unreachable!(
                    "Invalid runner/action combination: {:?} and {:?}",
                    runner, action
                ),
            }
        }
    );

    let result = result_handle.join_timeout(timeout);
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

#[cfg(test)]
mod test {
    use std::collections::{HashMap, VecDeque};
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use abq_utils::net_protocol::runners::{Action, Manifest};
    use abq_utils::net_protocol::workers::{NextWork, TestLikeRunner};
    use tempfile::TempDir;

    use super::{GetNextWork, NotifyManifest, NotifyResult, WorkerContext, WorkerPool};
    use crate::workers::WorkerPoolConfig;
    use abq_utils::net_protocol::{
        runners::Output,
        workers::{InvocationId, RunnerKind, WorkContext, WorkId, WorkerResult},
    };

    type ResultsCollector = Arc<Mutex<HashMap<String, WorkerResult>>>;
    type ManifestCollector = Arc<Mutex<Option<Manifest>>>;

    fn work_writer() -> (impl Fn(NextWork), GetNextWork) {
        let writer: Arc<Mutex<VecDeque<NextWork>>> = Default::default();
        let reader = Arc::clone(&writer);
        let write_work = move |work| {
            writer.lock().unwrap().push_back(work);
        };
        let get_next_work: GetNextWork = Box::new(move || loop {
            if let Some(work) = reader.lock().unwrap().pop_front() {
                return work;
            }
        });
        (write_work, get_next_work)
    }

    fn manifest_collector() -> (ManifestCollector, NotifyManifest) {
        let man: ManifestCollector = Arc::new(Mutex::new(None));
        let man2 = Arc::clone(&man);
        let notify_result: NotifyManifest = Box::new(move |_, man| {
            let old_manifest = man2.lock().unwrap().replace(man);
            debug_assert!(old_manifest.is_none(), "Overwriting a manifest! This is either a bug in your test, or the worker pool implementation.");
        });
        (man, notify_result)
    }

    fn results_collector() -> (ResultsCollector, NotifyResult) {
        let results: ResultsCollector = Default::default();
        let results2 = Arc::clone(&results);
        let notify_result: NotifyResult = Box::new(move |_, work_id, result| {
            let old_result = results2.lock().unwrap().insert(work_id.0, result);
            debug_assert!(old_result.is_none(), "Overwriting a result! This is either a bug in your test, or the worker pool implementation.");
        });
        (results, notify_result)
    }

    fn setup_pool(
        runner: TestLikeRunner,
        invocation_id: InvocationId,
        manifest: Manifest,
        get_next_work: GetNextWork,
        notify_result: NotifyResult,
    ) -> (WorkerPoolConfig, ManifestCollector) {
        let (manifest_collector, notify_manifest) = manifest_collector();

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(1).unwrap(),
            get_next_work,
            runner_kind: RunnerKind::TestLikeRunner(runner, manifest),
            invocation_id,
            notify_manifest: Some(notify_manifest),
            notify_result,
            worker_context: WorkerContext::AssumeLocal,
            work_timeout: Duration::from_secs(5),
            work_retries: 0,
        };

        (config, manifest_collector)
    }

    fn local_work(action: Action, invocation_id: InvocationId, work_id: WorkId) -> NextWork {
        NextWork::Work {
            action,
            context: WorkContext {
                working_dir: std::env::current_dir().unwrap(),
            },
            invocation_id,
            work_id,
        }
    }

    fn await_manifest_actions(manifest: ManifestCollector) -> Vec<Action> {
        loop {
            match manifest.lock().unwrap().take() {
                Some(manifest) => return manifest.actions,
                None => continue,
            }
        }
    }

    fn await_results<F>(results: ResultsCollector, predicate: F)
    where
        F: Fn(&ResultsCollector) -> bool,
    {
        let duration = Instant::now();

        while !predicate(&results) {
            if duration.elapsed() >= Duration::from_secs(5) {
                panic!(
                    "Failed to match the predicate within 5 seconds. Current results: {:?}",
                    results
                );
            }
            // spin
        }
    }

    fn test_echo_n(num_workers: usize, num_echos: usize) {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let invocation_id = InvocationId::new();
        let mut expected_results = HashMap::new();
        let actions = (0..num_echos)
            .into_iter()
            .map(|i| {
                let echo_string = format!("echo {}", i);
                expected_results.insert(i.to_string(), echo_string.clone());

                Action::Echo(echo_string)
            })
            .collect();
        let manifest = Manifest { actions };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::Echo,
            invocation_id,
            manifest,
            get_next_work,
            notify_result,
        );

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(num_workers).unwrap(),
            ..default_config
        };

        let mut pool = WorkerPool::new(config);

        // Write the work
        let actions = await_manifest_actions(manifest_collector);

        for (i, action) in actions.into_iter().enumerate() {
            write_work(local_work(action, invocation_id, WorkId(i.to_string())))
        }

        for _ in 0..num_workers {
            write_work(NextWork::EndOfWork);
        }

        // Spin until the timeout, or we got the results we expect.
        await_results(results, |results| {
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

            results == expected_results
        });

        pool.shutdown();
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
    #[cfg(feature = "test-actions")]
    fn test_timeout() {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let invocation_id = InvocationId::new();
        let manifest = Manifest {
            actions: vec![Action::Echo("mona lisa".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::InduceTimeout,
            invocation_id,
            manifest,
            get_next_work,
            notify_result,
        );

        let timeout = Duration::from_millis(1);
        let config = WorkerPoolConfig {
            work_timeout: timeout,
            work_retries: 0,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for action in await_manifest_actions(manifest_collector) {
            write_work(local_work(action, invocation_id, WorkId("id1".to_string())));
        }

        write_work(NextWork::EndOfWork);

        await_results(results, |results| {
            let results = results.lock().unwrap();
            if results.is_empty() {
                return false;
            }

            results.get("id1").unwrap() == &WorkerResult::Timeout(timeout)
        });

        pool.shutdown();
    }

    #[test]
    #[cfg(feature = "test-actions")]
    fn test_panic_no_retries() {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let invocation_id = InvocationId::new();
        let manifest = Manifest {
            actions: vec![Action::Echo("".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::EchoOnRetry(10),
            invocation_id,
            manifest,
            get_next_work,
            notify_result,
        );

        let config = WorkerPoolConfig {
            work_retries: 0,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for action in await_manifest_actions(manifest_collector) {
            write_work(local_work(action, invocation_id, WorkId("id1".to_string())));
        }
        write_work(NextWork::EndOfWork);

        await_results(results, |results| {
            let results = results.lock().unwrap();
            if results.is_empty() {
                return false;
            }

            results.get("id1").unwrap() == &WorkerResult::Panic("Failed to echo!".to_string())
        });

        pool.shutdown();
    }

    #[test]
    #[cfg(feature = "test-actions")]
    fn test_panic_succeed_after_retry() {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let invocation_id = InvocationId::new();
        let manifest = Manifest {
            actions: vec![Action::Echo("okay".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::EchoOnRetry(2),
            invocation_id,
            manifest,
            get_next_work,
            notify_result,
        );

        let config = WorkerPoolConfig {
            work_retries: 1,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for action in await_manifest_actions(manifest_collector) {
            write_work(local_work(action, invocation_id, WorkId("id1".to_string())));
        }
        write_work(NextWork::EndOfWork);

        await_results(results, |results| {
            let results = results.lock().unwrap();
            if results.is_empty() {
                return false;
            }

            results.get("id1").unwrap()
                == &WorkerResult::Output(Output {
                    success: true,
                    message: "okay".to_string(),
                })
        });

        pool.shutdown();
    }

    #[test]
    fn work_in_constant_context() {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let working_dir = TempDir::new().unwrap();
        std::fs::write(working_dir.path().join("testfile"), "testcontent").unwrap();

        let worker_context = WorkerContext::AlwaysWorkIn {
            working_dir: working_dir.path().to_owned(),
        };

        let invocation_id = InvocationId::new();
        let manifest = Manifest {
            actions: vec![Action::Exec {
                cmd: "cat".to_string(),
                args: vec!["testfile".to_string()],
            }],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::Exec,
            invocation_id,
            manifest,
            get_next_work,
            notify_result,
        );

        let pool_config = WorkerPoolConfig {
            worker_context,
            ..default_config
        };
        let mut pool = WorkerPool::new(pool_config);

        // Send the unit of work with a fake working directory to make sure the worker actually
        // uses the working directory given in the worker's context.
        let fake_working_dir_of_work = PathBuf::from("/zzz/i/do/not/exist");
        assert!(!fake_working_dir_of_work.exists());

        let context = WorkContext {
            working_dir: fake_working_dir_of_work,
        };

        for action in await_manifest_actions(manifest_collector) {
            write_work(NextWork::Work {
                action,
                context: context.clone(),
                invocation_id,
                work_id: WorkId("id1".to_string()),
            });
        }
        write_work(NextWork::EndOfWork);

        await_results(results, |results| {
            let results = results.lock().unwrap();
            if results.is_empty() {
                return false;
            }

            results.get("id1").unwrap()
                == &WorkerResult::Output(Output {
                    success: true,
                    message: "testcontent".to_string(),
                })
        });

        pool.shutdown();
    }
}