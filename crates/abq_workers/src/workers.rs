use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{sync::mpsc, thread};

use abq_echo_worker as echo;
use abq_exec_worker as exec;
use abq_generic_test_runner::{GenericTestRunner, RunnerError};
use abq_runner_protocol::Runner;
use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::runners::{ManifestMessage, Status, TestId, TestResult};
use abq_utils::net_protocol::workers::{
    InvocationId, NextWork, NextWorkBundle, RunnerKind, WorkContext, WorkId,
};
use abq_utils::net_protocol::workers::{NativeTestRunnerParams, TestLikeRunner};

use procspawn as proc;

enum MessageFromPool {
    Shutdown,
}

type MessageFromPoolRx = Arc<Mutex<mpsc::Receiver<MessageFromPool>>>;

pub type GetNextWorkBundle = Arc<dyn Fn() -> NextWorkBundle + Send + Sync + 'static>;
pub type NotifyManifest =
    Box<dyn Fn(EntityId, InvocationId, ManifestMessage) + Send + Sync + 'static>;
pub type NotifyResult =
    Arc<dyn Fn(EntityId, InvocationId, WorkId, TestResult) + Send + Sync + 'static>;

type MarkWorkerComplete = Arc<dyn Fn() + Send + Sync + 'static>;

#[derive(Clone, Debug)]
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
    pub get_next_work: GetNextWorkBundle,
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
    live_count: LiveWorkers,
}

struct LiveWorkers(Arc<AtomicUsize>);

impl LiveWorkers {
    fn new(count: usize) -> Self {
        LiveWorkers(Arc::new(AtomicUsize::new(count)))
    }

    fn dec(&self) {
        self.0.fetch_sub(1, atomic::Ordering::SeqCst);
    }

    /// *Not* guaranteed to be atomic in its answer.
    fn read(&self) -> usize {
        self.0.load(atomic::Ordering::SeqCst)
    }

    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
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

        let live_count = LiveWorkers::new(num_workers);
        tracing::debug!("Starting workers with live count {}", live_count.read());

        let (worker_msg_tx, worker_msg_rx) = mpsc::channel();
        let shared_worker_msg_rx = Arc::new(Mutex::new(worker_msg_rx));

        let mark_worker_complete = {
            let live_count: LiveWorkers = live_count.clone();
            move || {
                live_count.dec();
                tracing::debug!("worker done, {} left", live_count.read());
            }
        };
        let mark_worker_complete: MarkWorkerComplete = Arc::new(Box::new(mark_worker_complete));

        {
            // Provision the first worker independently, so that if we need to generate a manifest,
            // only it gets the manifest notifier.
            // TODO: consider hiding this behind a macro for code duplication purposes
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let notify_result = Arc::clone(&notify_result);
            let get_next_work = Arc::clone(&get_next_work);
            let mark_worker_complete = Arc::clone(&mark_worker_complete);

            let worker_env = WorkerEnv {
                msg_from_pool_rx: msg_rx,
                invocation_id,
                get_next_work_bundle: get_next_work,
                notify_result,
                context: worker_context.clone(),
                work_timeout,
                work_retries,
                notify_manifest,
                mark_worker_complete,
            };

            workers.push(ThreadWorker::new(runner_kind.clone(), worker_env));
        }

        // Provision the rest of the workers.
        for _id in 1..num_workers {
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let notify_result = Arc::clone(&notify_result);
            let get_next_work = Arc::clone(&get_next_work);
            let mark_worker_complete = Arc::clone(&mark_worker_complete);

            let worker_env = WorkerEnv {
                msg_from_pool_rx: msg_rx,
                invocation_id,
                get_next_work_bundle: get_next_work,
                notify_result,
                context: worker_context.clone(),
                work_timeout,
                work_retries,
                notify_manifest: None,
                mark_worker_complete,
            };

            workers.push(ThreadWorker::new(runner_kind.clone(), worker_env));
        }

        Self {
            active: true,
            workers,
            worker_msg_tx,
            live_count,
        }
    }

    /// Answers whether there are any workers that are still alive.
    /// Not guaranteed to be correct atomically.
    pub fn workers_alive(&self) -> bool {
        self.live_count.read() > 0
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
                .expect("runner thread panicked rather than erroring")
                .expect("runner failed, but we didn't catch it");
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
    handle: Option<thread::JoinHandle<Result<(), RunnerError>>>,
}

enum AttemptError {
    ShouldRetry,
    Panic(String),
    Timeout(Duration),
}

type AttemptResult = Result<String, AttemptError>;

struct WorkerEnv {
    msg_from_pool_rx: MessageFromPoolRx,
    invocation_id: InvocationId,
    notify_manifest: Option<NotifyManifest>,
    get_next_work_bundle: GetNextWorkBundle,
    notify_result: NotifyResult,
    context: WorkerContext,
    work_timeout: Duration,
    work_retries: u8,
    mark_worker_complete: MarkWorkerComplete,
}

impl ThreadWorker {
    pub fn new(runner_kind: RunnerKind, worker_env: WorkerEnv) -> Self {
        let handle = thread::spawn(move || match runner_kind {
            RunnerKind::GenericNativeTestRunner(params) => {
                start_generic_test_runner(worker_env, params)
            }
            RunnerKind::TestLikeRunner(runner, manifest) => {
                start_test_like_runner(worker_env, runner, manifest);
                Ok(())
            }
        });

        Self {
            handle: Some(handle),
        }
    }
}

fn start_generic_test_runner(
    env: WorkerEnv,
    native_runner_params: NativeTestRunnerParams,
) -> Result<(), RunnerError> {
    let WorkerEnv {
        get_next_work_bundle,
        invocation_id,
        notify_result,
        notify_manifest,
        context,
        msg_from_pool_rx,
        // TODO: actually use these
        work_timeout: _,
        work_retries: _,
        mark_worker_complete,
    } = env;

    let entity = EntityId::new();

    tracing::debug!(?entity, "Starting new generic test runner");

    let notify_manifest = notify_manifest
        .map(|notify_manifest| move |manifest| notify_manifest(entity, invocation_id, manifest));

    let get_next_work_bundle = move || get_next_work_bundle();

    let send_test_result = move |work_id, test_result: TestResult| {
        notify_result(entity, invocation_id, work_id, test_result)
    };

    let polling_should_shutdown = || {
        matches!(
            msg_from_pool_rx.lock().unwrap().try_recv(),
            Ok(MessageFromPool::Shutdown)
        )
    };

    let working_dir = match context {
        WorkerContext::AssumeLocal => std::env::current_dir().unwrap(),
        WorkerContext::AlwaysWorkIn { working_dir } => working_dir,
    };

    GenericTestRunner::run(
        native_runner_params,
        &working_dir,
        polling_should_shutdown,
        notify_manifest,
        get_next_work_bundle,
        send_test_result,
    )?;

    mark_worker_complete();

    Ok(())
}

fn start_test_like_runner(env: WorkerEnv, runner: TestLikeRunner, manifest: ManifestMessage) {
    let WorkerEnv {
        msg_from_pool_rx,
        get_next_work_bundle: get_next_work,
        invocation_id,
        notify_result,
        context,
        work_timeout,
        work_retries,
        notify_manifest,
        mark_worker_complete,
    } = env;

    let entity = EntityId::new();

    if let Some(notify_manifest) = notify_manifest {
        notify_manifest(entity, invocation_id, manifest);
    }

    'tests_done: loop {
        // First, check if we have a message from our owner.
        let parent_message = msg_from_pool_rx.lock().unwrap().try_recv();

        let bundle = match parent_message {
            Ok(MessageFromPool::Shutdown) => {
                break;
            }
            Err(mpsc::TryRecvError::Disconnected) => panic!("Pool died before worker did"),
            Err(mpsc::TryRecvError::Empty) => {
                // No message from the parent. Wait for the next test_id to come in.
                //
                // TODO: add a timeout here, in case we get a message from the parent while
                // blocking on the next test_id from the queue.
                let NextWorkBundle(bundle) = get_next_work();
                bundle
            }
        };

        for next_work in bundle {
            match next_work {
                NextWork::EndOfWork => {
                    // Shut down the worker
                    break 'tests_done;
                }
                NextWork::Work {
                    test_case,
                    context: work_context,
                    invocation_id,
                    work_id,
                } => {
                    // Try the test_id once + how ever many retries were requested.
                    let allowed_attempts = 1 + work_retries;
                    'attempts: for attempt_number in 1.. {
                        let start_time = Instant::now();
                        let attempt_result = attempt_test_id_for_test_like_runner(
                            &context,
                            runner,
                            test_case.id.clone(),
                            &work_context,
                            work_timeout,
                            attempt_number,
                            allowed_attempts,
                        );
                        let runtime = start_time.elapsed().as_millis() as f64;

                        let (status, output) = match attempt_result {
                            Ok(output) => (Status::Success, output),
                            Err(AttemptError::ShouldRetry) => continue 'attempts,
                            Err(AttemptError::Panic(msg)) => (Status::Error, msg),
                            Err(AttemptError::Timeout(time)) => {
                                (Status::Error, format!("Timeout: {}ms", time.as_millis()))
                            }
                        };
                        let result = TestResult {
                            status,
                            id: test_case.id.clone(),
                            display_name: test_case.id.clone(),
                            output: Some(output),
                            runtime,
                            meta: Default::default(),
                        };

                        notify_result(entity, invocation_id, work_id.clone(), result);
                        break 'attempts;
                    }
                }
            }
        }
    }

    mark_worker_complete()
}

#[inline(always)]
fn attempt_test_id_for_test_like_runner(
    my_context: &WorkerContext,
    runner: TestLikeRunner,
    test_id: TestId,
    requested_context: &WorkContext,
    timeout: Duration,
    attempt: u8,
    allowed_attempts: u8,
) -> AttemptResult {
    let working_dir = resolve_context(my_context, requested_context).to_owned();

    use TestLikeRunner as R;

    let result_handle = proc::spawn!(
        (runner, test_id, working_dir, attempt) || {
            std::env::set_current_dir(working_dir).unwrap();
            let _attempt = attempt;
            match (runner, test_id) {
                (R::Echo, s) => echo::EchoWorker::run(echo::EchoWork { message: s }),
                (R::Exec, cmd_and_args) => {
                    let mut args = cmd_and_args
                        .split(' ')
                        .map(ToOwned::to_owned)
                        .collect::<Vec<String>>();
                    let cmd = args.remove(0);
                    exec::ExecWorker::run(exec::Work { cmd, args })
                }
                #[cfg(feature = "test-test_ids")]
                (R::InduceTimeout, _) => {
                    thread::sleep(Duration::MAX);
                    unreachable!()
                }
                #[cfg(feature = "test-test_ids")]
                (R::EchoOnRetry(succeed_on), s) => {
                    if succeed_on == _attempt {
                        echo::EchoWorker::run(echo::EchoWork { message: s })
                    } else {
                        panic!("Failed to echo!");
                    }
                }
                #[cfg(feature = "test-actions")]
                (runner, test_id) => unreachable!(
                    "Invalid runner/test_id combination: {:?} and {:?}",
                    runner, test_id
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

    use abq_utils::auth::{ClientAuthStrategy, ServerAuthStrategy};
    use abq_utils::net_opt::{ClientOptions, ServerOptions, Tls};
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, Status, Test, TestCase, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::workers::{NextWork, NextWorkBundle, TestLikeRunner};
    use abq_utils::{flatten_manifest, net_protocol};
    use tempfile::TempDir;
    use tracing_test::internal::logs_with_scope_contain;
    use tracing_test::traced_test;

    use super::{GetNextWorkBundle, NotifyManifest, NotifyResult, WorkerContext, WorkerPool};
    use crate::negotiate::QueueNegotiator;
    use crate::workers::WorkerPoolConfig;
    use abq_utils::net_protocol::workers::{InvocationId, RunnerKind, WorkContext, WorkId};

    type ResultsCollector = Arc<Mutex<HashMap<String, TestResult>>>;
    type ManifestCollector = Arc<Mutex<Option<ManifestMessage>>>;

    fn work_writer() -> (impl Fn(NextWork), GetNextWorkBundle) {
        let writer: Arc<Mutex<VecDeque<NextWork>>> = Default::default();
        let reader = Arc::clone(&writer);
        let write_work = move |work| {
            writer.lock().unwrap().push_back(work);
        };
        let get_next_work: GetNextWorkBundle = Arc::new(move || loop {
            if let Some(work) = reader.lock().unwrap().pop_front() {
                return NextWorkBundle(vec![work]);
            }
        });
        (write_work, get_next_work)
    }

    fn manifest_collector() -> (ManifestCollector, NotifyManifest) {
        let man: ManifestCollector = Arc::new(Mutex::new(None));
        let man2 = Arc::clone(&man);
        let notify_result: NotifyManifest = Box::new(move |_, _, man| {
            let old_manifest = man2.lock().unwrap().replace(man);
            debug_assert!(old_manifest.is_none(), "Overwriting a manifest! This is either a bug in your test, or the worker pool implementation.");
        });
        (man, notify_result)
    }

    fn results_collector() -> (ResultsCollector, NotifyResult) {
        let results: ResultsCollector = Default::default();
        let results2 = Arc::clone(&results);
        let notify_result: NotifyResult = Arc::new(move |_, _, work_id, result| {
            let old_result = results2.lock().unwrap().insert(work_id.0, result);
            debug_assert!(old_result.is_none(), "Overwriting a result! This is either a bug in your test, or the worker pool implementation.");
        });
        (results, notify_result)
    }

    fn setup_pool(
        runner: TestLikeRunner,
        invocation_id: InvocationId,
        manifest: ManifestMessage,
        get_next_work: GetNextWorkBundle,
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

    fn local_work(test: TestCase, invocation_id: InvocationId, work_id: WorkId) -> NextWork {
        NextWork::Work {
            test_case: test,
            context: WorkContext {
                working_dir: std::env::current_dir().unwrap(),
            },
            invocation_id,
            work_id,
        }
    }

    pub fn echo_test(echo_msg: String) -> TestOrGroup {
        TestOrGroup::Test(Test {
            id: echo_msg,
            tags: Default::default(),
            meta: Default::default(),
        })
    }

    pub fn exec_test(cmd: &str, args: &[&str]) -> TestOrGroup {
        let exec_str = std::iter::once(cmd)
            .chain(args.iter().copied())
            .collect::<Vec<_>>()
            .join(" ");

        TestOrGroup::Test(Test {
            id: exec_str,
            tags: Default::default(),
            meta: Default::default(),
        })
    }

    fn await_manifest_test_cases(manifest: ManifestCollector) -> Vec<TestCase> {
        loop {
            match manifest.lock().unwrap().take() {
                Some(manifest) => return flatten_manifest(manifest.manifest),
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
        let tests = (0..num_echos)
            .into_iter()
            .map(|i| {
                let echo_string = format!("echo {}", i);
                expected_results.insert(i.to_string(), echo_string.clone());

                echo_test(echo_string)
            })
            .collect();
        let manifest = ManifestMessage {
            manifest: Manifest { members: tests },
        };

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
        let test_ids = await_manifest_test_cases(manifest_collector);

        for (i, test_id) in test_ids.into_iter().enumerate() {
            write_work(local_work(test_id, invocation_id, WorkId(i.to_string())))
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
                .map(|(k, v)| (k, v.output.unwrap()))
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
    #[cfg(feature = "test-test_ids")]
    fn test_timeout() {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let invocation_id = InvocationId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("mona lisa".to_string())],
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

        for test_id in await_manifest_test_cases(manifest_collector) {
            write_work(local_work(
                test_id,
                invocation_id,
                WorkId("id1".to_string()),
            ));
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
    #[cfg(feature = "test-test_ids")]
    fn test_panic_no_retries() {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let invocation_id = InvocationId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("".to_string())],
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

        for test_id in await_manifest_test_cases(manifest_collector) {
            write_work(local_work(
                test_id,
                invocation_id,
                WorkId("id1".to_string()),
            ));
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
    #[cfg(feature = "test-test_ids")]
    fn test_panic_succeed_after_retry() {
        let (write_work, get_next_work) = work_writer();
        let (results, notify_result) = results_collector();

        let invocation_id = InvocationId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("okay".to_string())],
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

        for test_id in await_manifest_test_cases(manifest_collector) {
            write_work(local_work(
                test_id,
                invocation_id,
                WorkId("id1".to_string()),
            ));
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
        let manifest = ManifestMessage {
            manifest: Manifest {
                members: vec![exec_test("cat", &["testfile"])],
            },
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

        for test_case in await_manifest_test_cases(manifest_collector) {
            write_work(NextWork::Work {
                test_case,
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

            let result = results.get("id1").unwrap();
            result.status == Status::Success && result.output.as_ref().unwrap() == "testcontent"
        });

        pool.shutdown();
    }

    #[test]
    #[traced_test]
    fn bad_message_doesnt_take_down_queue_negotiator_server() {
        let listener = ServerOptions::new(ServerAuthStrategy::NoAuth, Tls::NO)
            .bind("0.0.0.0:0")
            .unwrap();
        let listener_addr = listener.local_addr().unwrap();
        let mut negotiator = QueueNegotiator::new(
            listener_addr.ip(),
            listener,
            "0.0.0.0:0".parse().unwrap(),
            "0.0.0.0:0".parse().unwrap(),
            |_| panic!("should not ask for assigned run in this test"),
        )
        .unwrap();

        let client = ClientOptions::new(ClientAuthStrategy::NoAuth, Tls::NO)
            .build()
            .unwrap();
        let mut conn = client.connect(listener_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();

        negotiator.shutdown();

        logs_with_scope_contain("", "error handling connection");
    }
}
