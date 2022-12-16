use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{sync::mpsc, thread};

use abq_echo_worker as echo;
use abq_exec_worker as exec;
use abq_generic_test_runner::{GenericRunnerError, GenericTestRunner};
use abq_runner_protocol::Runner;
use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::queue::{AssociatedTestResult, RunAlreadyCompleted};
use abq_utils::net_protocol::runners::{
    AbqProtocolVersion, ManifestMessage, NativeRunnerSpecification, OutOfBandError, Status, TestId,
    TestResult, TestResultSpec, TestRuntime,
};
use abq_utils::net_protocol::work_server::InitContext;
use abq_utils::net_protocol::workers::{
    ManifestResult, NativeTestRunnerParams, ReportedManifest, TestLikeRunner, WorkerTest,
};
use abq_utils::net_protocol::workers::{NextWork, NextWorkBundle, RunId, RunnerKind, WorkContext};

use futures::future::BoxFuture;

enum MessageFromPool {
    Shutdown,
}

type MessageFromPoolRx = Arc<Mutex<mpsc::Receiver<MessageFromPool>>>;

pub type InitContextResult = Result<InitContext, RunAlreadyCompleted>;

pub type GetNextTests = Box<dyn Fn() -> BoxFuture<'static, NextWorkBundle> + Send + 'static>;
pub type GetNextTestsGenerator<'a> = &'a dyn Fn() -> GetNextTests;

pub type GetInitContext = Arc<dyn Fn() -> InitContextResult + Send + Sync + 'static>;
pub type NotifyManifest = Box<dyn Fn(EntityId, &RunId, ManifestResult) + Send + Sync + 'static>;
pub type NotifyResults = Arc<
    dyn Fn(EntityId, &RunId, Vec<AssociatedTestResult>) -> BoxFuture<'static, ()>
        + Send
        + Sync
        + 'static,
>;
/// Did a given run complete successfully? Returns true if so.
pub type RunCompletedSuccessfully = Arc<dyn Fn(EntityId) -> bool + Send + Sync + 'static>;

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
pub struct WorkerPoolConfig<'a> {
    /// Number of workers.
    pub size: NonZeroUsize,
    /// The kind of runners the workers should start.
    pub runner_kind: RunnerKind,
    /// The work run we're working for.
    pub run_id: RunId,
    /// When [`Some`], one worker must be chosen to generate and return the manifest the following
    /// way.
    pub notify_manifest: Option<NotifyManifest>,
    /// How should workers initialize their native test runners?
    pub get_init_context: GetInitContext,
    /// How should a worker get the next unit of work it needs to run?
    pub get_next_tests_generator: GetNextTestsGenerator<'a>,
    /// How should results be communicated back?
    pub notify_results: NotifyResults,
    /// How many results should be sent back at a time?
    pub results_batch_size_hint: u64,
    /// Query whether the assigned run completed successfully, without any failure.
    /// Will block until the result is well-known.
    pub run_completed_successfully: RunCompletedSuccessfully,
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

    // Whether to allow passthrough of stdout/stderr from the native runner process.
    pub debug_native_runner: bool,
}

/// Manages a pool of threads and processes upon which work is run and reported back.
///
/// A pool of size N has N worker threads that listen to incoming work (given by
/// [WorkerPool::send_work]). Each thread is given access to a pool of N processes where
/// they execute a given piece of work.
///
/// Work execution is done in the process pool so that the managing worker threads can easily
/// terminate the process if needed, and panics in the process pool don't induce a panic in the
/// worker pool process.
pub struct WorkerPool {
    active: bool,
    workers: Vec<ThreadWorker>,
    worker_msg_tx: mpsc::Sender<MessageFromPool>,
    live_count: LiveWorkers,
    /// Query whether the run assigned for this pool completed successfully.
    run_completed_successfully: RunCompletedSuccessfully,
    /// The entity of the worker pool itself.
    entity: EntityId,
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

#[derive(PartialEq, Eq, Debug)]
pub enum WorkersExit {
    /// This pool of workers was determined to complete successfully.
    /// Corresponds to all workers for a given [run][RunId] having only work that was
    /// successful.
    Success,
    /// This pool of workers was determined to fail.
    /// Corresponds to any workers for a given [run][RunId] having any work that failed.
    Failure,
    /// Some worker in this pool errored in an unexpected way.
    Error,
}

impl WorkerPool {
    pub fn new(config: WorkerPoolConfig) -> Self {
        let WorkerPoolConfig {
            size,
            runner_kind,
            run_id,
            get_init_context,
            get_next_tests_generator,
            results_batch_size_hint: results_batch_size,
            notify_results,
            notify_manifest,
            run_completed_successfully,
            worker_context,
            work_timeout,
            work_retries,
            debug_native_runner,
        } = config;

        let num_workers = size.get();
        let mut workers = Vec::with_capacity(num_workers);

        let live_count = LiveWorkers::new(num_workers);
        tracing::debug!(live_count=?live_count.read(), ?results_batch_size, ?run_id, "Starting worker pool");

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
            let notify_results = Arc::clone(&notify_results);
            let get_init_context = Arc::clone(&get_init_context);
            let mark_worker_complete = Arc::clone(&mark_worker_complete);

            let worker_env = WorkerEnv {
                msg_from_pool_rx: msg_rx,
                run_id: run_id.clone(),
                get_init_context,
                get_next_tests: get_next_tests_generator(),
                results_batch_size,
                notify_results,
                context: worker_context.clone(),
                work_timeout,
                work_retries,
                notify_manifest,
                mark_worker_complete,
                debug_native_runner,
            };

            workers.push(ThreadWorker::new(runner_kind.clone(), worker_env));
        }

        // Provision the rest of the workers.
        for _id in 1..num_workers {
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let notify_results = Arc::clone(&notify_results);
            let get_init_context = Arc::clone(&get_init_context);
            let mark_worker_complete = Arc::clone(&mark_worker_complete);

            let worker_env = WorkerEnv {
                msg_from_pool_rx: msg_rx,
                run_id: run_id.clone(),
                get_init_context,
                get_next_tests: get_next_tests_generator(),
                results_batch_size,
                notify_results,
                context: worker_context.clone(),
                work_timeout,
                work_retries,
                notify_manifest: None,
                mark_worker_complete,
                debug_native_runner,
            };

            workers.push(ThreadWorker::new(runner_kind.clone(), worker_env));
        }

        Self {
            active: true,
            workers,
            worker_msg_tx,
            live_count,
            run_completed_successfully,
            entity: EntityId::new(),
        }
    }

    /// Answers whether there are any workers that are still alive.
    /// Not guaranteed to be correct atomically.
    pub fn workers_alive(&self) -> bool {
        self.live_count.read() > 0
    }

    /// Shuts down the worker pool, returning the pool [exit status][WorkersExit].
    #[must_use]
    pub fn shutdown(&mut self) -> WorkersExit {
        debug_assert!(self.active);

        self.active = false;

        for _ in 0..self.workers.len() {
            // It's possible the worker already exited if it couldn't connect to the queue; in that
            // case, we won't be able to send across the channel, but that's okay.
            let _ = self.worker_msg_tx.send(MessageFromPool::Shutdown);
        }

        let mut worker_has_error = false;
        for worker_thead in self.workers.iter_mut() {
            let opt_err = worker_thead
                .handle
                .take()
                .expect("worker thread already stolen")
                .join()
                .expect("runner thread panicked rather than erroring");

            if let Err(error) = opt_err {
                tracing::error!(?error, "worker thread exited with error");
                eprintln!("{}", error);
                worker_has_error = true;
            }
        }

        if worker_has_error {
            WorkersExit::Error
        } else if (self.run_completed_successfully)(self.entity) {
            WorkersExit::Success
        } else {
            WorkersExit::Failure
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        if self.active {
            // We can't do anything with the exit status at this point.
            let _exit = self.shutdown();
        }
    }
}

struct ThreadWorker {
    handle: Option<thread::JoinHandle<Result<(), GenericRunnerError>>>,
}

enum AttemptError {
    ShouldRetry,
    Panic(String),
    Timeout(Duration),
}

type AttemptResult = Result<String, AttemptError>;

struct WorkerEnv {
    msg_from_pool_rx: MessageFromPoolRx,
    run_id: RunId,
    notify_manifest: Option<NotifyManifest>,
    get_init_context: GetInitContext,
    get_next_tests: GetNextTests,
    results_batch_size: u64,
    notify_results: NotifyResults,
    context: WorkerContext,
    work_timeout: Duration,
    work_retries: u8,
    mark_worker_complete: MarkWorkerComplete,
    debug_native_runner: bool,
}

impl ThreadWorker {
    pub fn new(runner_kind: RunnerKind, worker_env: WorkerEnv) -> Self {
        let handle = thread::spawn(move || match runner_kind {
            RunnerKind::GenericNativeTestRunner(params) => {
                start_generic_test_runner(worker_env, params)
            }
            RunnerKind::TestLikeRunner(runner, manifest) => {
                start_test_like_runner(worker_env, runner, *manifest)
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
) -> Result<(), GenericRunnerError> {
    let WorkerEnv {
        get_next_tests: get_next_tests_bundle,
        run_id,
        get_init_context,
        notify_results,
        notify_manifest,
        results_batch_size,
        context,
        msg_from_pool_rx,
        // TODO: actually use these
        work_timeout: _,
        work_retries: _,
        mark_worker_complete,
        debug_native_runner,
    } = env;

    let entity = EntityId::new();

    tracing::debug!(?entity, ?run_id, "Starting new generic test runner");

    // We expose the worker ID to the end user, even without tracing to standard pipes enabled,
    // so that they can correlate failures observed in workers with the workers they've launched.
    eprintln!("Worker started with id {:?}", entity);

    let notify_manifest = notify_manifest.map(|notify_manifest| {
        let run_id = run_id.clone();
        move |manifest_result| notify_manifest(entity, &run_id, manifest_result)
    });

    let get_init_context = move || get_init_context();

    let get_next_tests_bundle: abq_generic_test_runner::GetNextTests =
        &move || get_next_tests_bundle();

    let send_test_result: abq_generic_test_runner::SendTestResults =
        &move |results| notify_results(entity, &run_id, results);

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

    let opt_runner_err = GenericTestRunner::run(
        entity,
        native_runner_params,
        &working_dir,
        polling_should_shutdown,
        results_batch_size,
        notify_manifest,
        get_init_context,
        get_next_tests_bundle,
        send_test_result,
        debug_native_runner,
    );

    mark_worker_complete();

    opt_runner_err
}

fn build_test_like_runner_manifest_result(
    manifest_message: ManifestMessage,
) -> Result<ReportedManifest, OutOfBandError> {
    let manifest = match manifest_message {
        ManifestMessage::Success(m) => m.manifest,
        ManifestMessage::Failure(fail) => return Err(fail.error),
    };

    let native_runner_protocol = AbqProtocolVersion::V0_1;
    let native_runner_specification = NativeRunnerSpecification {
        name: "unknown-test-like-runner".to_string(),
        version: "0.0.1".to_owned(),
        test_framework: Some("rspec".to_owned()),
        test_framework_version: Some("3.12.0".to_owned()),
        language: Some("ruby".to_owned()),
        language_version: Some("3.1.2p20".to_owned()),
        host: Some("ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned()),
    };

    Ok(ReportedManifest {
        manifest,
        native_runner_protocol,
        native_runner_specification: Box::new(native_runner_specification),
    })
}

fn start_test_like_runner(
    env: WorkerEnv,
    runner: TestLikeRunner,
    manifest: ManifestMessage,
) -> Result<(), GenericRunnerError> {
    let WorkerEnv {
        msg_from_pool_rx,
        get_next_tests,
        get_init_context,
        run_id,
        results_batch_size: _,
        notify_results,
        context,
        work_timeout,
        work_retries,
        notify_manifest,
        mark_worker_complete,
        debug_native_runner: _,
    } = env;

    let entity = EntityId::new();

    if matches!(runner, TestLikeRunner::NeverReturnManifest) {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "will not return manifest").into());
    }

    if let Some(notify_manifest) = notify_manifest {
        let manifest_result = build_test_like_runner_manifest_result(manifest);
        match manifest_result {
            Ok(manifest) => {
                notify_manifest(entity, &run_id, ManifestResult::Manifest(manifest));
            }
            Err(oob) => {
                notify_manifest(
                    entity,
                    &run_id,
                    ManifestResult::TestRunnerError {
                        error: oob.to_string(),
                    },
                );
                return Err(GenericRunnerError::NativeRunner(oob.into()));
            }
        };
    }

    let init_context = match get_init_context() {
        Ok(ctx) => ctx,
        Err(RunAlreadyCompleted {}) => return Ok(()),
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    'tests_done: loop {
        // First, check if we have a message from our owner.
        let parent_message = msg_from_pool_rx.lock().unwrap().try_recv();

        let bundle = match parent_message {
            Ok(MessageFromPool::Shutdown) => {
                if matches!(&runner, TestLikeRunner::NeverReturnOnTest(..)) {
                    return Err(
                        io::Error::new(io::ErrorKind::Unsupported, "will not return test").into(),
                    );
                }
                break;
            }
            Err(mpsc::TryRecvError::Disconnected) => panic!("Pool died before worker did"),
            Err(mpsc::TryRecvError::Empty) => {
                // No message from the parent. Wait for the next test_id to come in.
                //
                // TODO: add a timeout here, in case we get a message from the parent while
                // blocking on the next test_id from the queue.
                let NextWorkBundle(bundle) = rt.block_on(get_next_tests());
                bundle
            }
        };

        for next_work in bundle {
            match next_work {
                NextWork::EndOfWork => {
                    // Shut down the worker
                    break 'tests_done;
                }
                NextWork::Work(WorkerTest {
                    test_case,
                    context: work_context,
                    run_id,
                    work_id,
                }) => {
                    if matches!(&runner, TestLikeRunner::NeverReturnOnTest(t) if t == test_case.id() )
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "will not return test",
                        )
                        .into());
                    }

                    // Try the test_id once + how ever many retries were requested.
                    let allowed_attempts = 1 + work_retries;
                    'attempts: for attempt_number in 1.. {
                        let start_time = Instant::now();
                        let attempt_result = attempt_test_id_for_test_like_runner(
                            &context,
                            runner.clone(),
                            test_case.id().clone(),
                            init_context.clone(),
                            &work_context,
                            work_timeout,
                            attempt_number,
                            allowed_attempts,
                        );
                        let runtime = start_time.elapsed().as_millis() as f64;

                        let (status, output) = match attempt_result {
                            Ok(output) => (Status::Success, output),
                            Err(AttemptError::ShouldRetry) => continue 'attempts,
                            Err(AttemptError::Panic(msg)) => (
                                Status::Error {
                                    exception: None,
                                    backtrace: None,
                                },
                                msg,
                            ),
                            Err(AttemptError::Timeout(time)) => (
                                Status::Error {
                                    exception: None,
                                    backtrace: None,
                                },
                                format!("Timeout: {}ms", time.as_millis()),
                            ),
                        };
                        let result = TestResult::new(TestResultSpec {
                            status,
                            id: test_case.id().clone(),
                            display_name: test_case.id().clone(),
                            output: Some(output),
                            runtime: TestRuntime::Milliseconds(runtime),
                            meta: Default::default(),
                            ..TestResultSpec::fake()
                        });

                        rt.block_on(notify_results(
                            entity,
                            &run_id,
                            vec![(work_id.clone(), result)],
                        ));
                        break 'attempts;
                    }
                }
            }
        }
    }

    mark_worker_complete();

    Ok(())
}

#[inline(always)]
fn attempt_test_id_for_test_like_runner(
    _my_context: &WorkerContext,
    runner: TestLikeRunner,
    test_id: TestId,
    init_context: InitContext,
    _requested_context: &WorkContext,
    timeout: Duration,
    attempt: u8,
    allowed_attempts: u8,
) -> AttemptResult {
    use TestLikeRunner as R;

    let init_context = serde_json::to_string(&init_context).unwrap();

    let (send_shutdown, recv_shutdown) = std::sync::mpsc::channel();

    let started = Instant::now();
    let result_handle = thread::spawn(move || {
        let _attempt = attempt;
        match (runner, test_id) {
            (R::Echo, s) => echo::EchoWorker::run(echo::EchoWork { message: s }),
            (R::EchoInitContext, _) => echo::EchoWorker::run(echo::EchoWork {
                message: init_context,
            }),
            (R::Exec, cmd_and_args) => {
                let mut args = cmd_and_args
                    .split(' ')
                    .map(ToOwned::to_owned)
                    .collect::<Vec<String>>();
                let cmd = args.remove(0);
                exec::ExecWorker::run(exec::Work { cmd, args })
            }
            (R::InduceTimeout, _) => {
                // Sleep until we get the shutdown signal.
                recv_shutdown.recv().unwrap();
                unreachable!()
            }
            (R::FailOnTestName(fail_name), test) => {
                if test == fail_name {
                    panic!("INDUCED FAIL")
                } else {
                    "PASS".to_string()
                }
            }
            (R::NeverReturnManifest, _) => unreachable!(),
            (R::NeverReturnOnTest(..), _) => unreachable!(),
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
    });

    let mut timed_out = false;
    while !result_handle.is_finished() {
        if started.elapsed() >= timeout {
            timed_out = true;
            let _ = send_shutdown.send(());
            break;
        }

        thread::sleep(Duration::from_micros(100));
    }
    let result = result_handle.join();
    match result {
        Ok(output) => Ok(output),
        Err(e) => {
            if attempt < allowed_attempts {
                Err(AttemptError::ShouldRetry)
            } else if timed_out {
                Err(AttemptError::Timeout(timeout))
            } else {
                let msg = if let Some(msg) = e.downcast_ref::<&'static str>() {
                    msg.to_string()
                } else {
                    format!("???unknown panic {:?}", e)
                };

                Err(AttemptError::Panic(msg))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, VecDeque};
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use abq_utils::auth::{ClientAuthStrategy, ServerAuthStrategy};
    use abq_utils::net_opt::{ClientOptions, ServerOptions};
    use abq_utils::net_protocol;
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, ProtocolWitness, Test, TestCase, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        ManifestResult, NativeTestRunnerParams, NextWork, NextWorkBundle, TestLikeRunner,
        WorkerTest,
    };
    use abq_utils::shutdown::ShutdownManager;
    use abq_utils::tls::{ClientTlsStrategy, ServerTlsStrategy};
    use abq_with_protocol_version::with_protocol_version;
    use futures::FutureExt;
    use tracing_test::internal::logs_with_scope_contain;
    use tracing_test::traced_test;

    use super::{
        GetNextTests, GetNextTestsGenerator, InitContextResult, NotifyManifest, NotifyResults,
        RunCompletedSuccessfully, WorkerContext, WorkerPool, WorkersExit,
    };
    use crate::negotiate::QueueNegotiator;
    use crate::workers::WorkerPoolConfig;
    use abq_utils::net_protocol::workers::{RunId, RunnerKind, WorkContext, WorkId};

    type ResultsCollector = Arc<Mutex<HashMap<String, TestResult>>>;
    type ManifestCollector = Arc<Mutex<Option<ManifestResult>>>;

    fn work_writer() -> (impl Fn(NextWork), impl Fn() -> GetNextTests) {
        let writer: Arc<Mutex<VecDeque<NextWork>>> = Default::default();
        let reader = Arc::clone(&writer);
        let write_work = move |work| {
            writer.lock().unwrap().push_back(work);
        };
        let get_next_tests = move || {
            let reader = reader.clone();
            let get_next_tests: GetNextTests = Box::new(move || loop {
                if let Some(work) = reader.lock().unwrap().pop_front() {
                    return async { NextWorkBundle(vec![work]) }.boxed();
                }
            });
            get_next_tests
        };
        (write_work, get_next_tests)
    }

    fn manifest_collector() -> (ManifestCollector, NotifyManifest) {
        let man: ManifestCollector = Arc::new(Mutex::new(None));
        let man2 = Arc::clone(&man);
        let notify_results: NotifyManifest = Box::new(move |_, _, man| {
            let old_manifest = man2.lock().unwrap().replace(man);
            debug_assert!(old_manifest.is_none(), "Overwriting a manifest! This is either a bug in your test, or the worker pool implementation.");
        });
        (man, notify_results)
    }

    fn results_collector() -> (ResultsCollector, NotifyResults, RunCompletedSuccessfully) {
        let results: ResultsCollector = Default::default();
        let results2 = Arc::clone(&results);
        let notify_results: NotifyResults = Arc::new(move |_, _, results| {
            for (work_id, result) in results {
                let old_result = results2.lock().unwrap().insert(work_id.0, result);
                debug_assert!(old_result.is_none(), "Overwriting a result! This is either a bug in your test, or the worker pool implementation.");
            }
            Box::pin(async {})
        });
        let results3 = Arc::clone(&results);
        let get_completed_status: RunCompletedSuccessfully = Arc::new(move |_| {
            results3
                .lock()
                .unwrap()
                .iter()
                .all(|(_, result)| !result.status.is_fail_like())
        });
        (results, notify_results, get_completed_status)
    }

    fn empty_init_context() -> InitContextResult {
        Ok(InitContext {
            init_meta: Default::default(),
        })
    }

    fn setup_pool(
        runner_kind: RunnerKind,
        run_id: RunId,
        get_next_tests_generator: GetNextTestsGenerator,
        notify_results: NotifyResults,
        run_completed_successfully: RunCompletedSuccessfully,
    ) -> (WorkerPoolConfig, ManifestCollector) {
        let (manifest_collector, notify_manifest) = manifest_collector();

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(1).unwrap(),
            get_next_tests_generator,
            get_init_context: Arc::new(empty_init_context),
            results_batch_size_hint: 5,
            runner_kind,
            run_id,
            notify_manifest: Some(notify_manifest),
            notify_results,
            run_completed_successfully,
            worker_context: WorkerContext::AssumeLocal,
            work_timeout: Duration::from_secs(5),
            work_retries: 0,
            debug_native_runner: false,
        };

        (config, manifest_collector)
    }

    fn local_work(test: TestCase, run_id: RunId, work_id: WorkId) -> NextWork {
        NextWork::Work(WorkerTest {
            test_case: test,
            context: WorkContext {
                working_dir: std::env::current_dir().unwrap(),
            },
            run_id,
            work_id,
        })
    }

    pub fn echo_test(protocol: ProtocolWitness, echo_msg: String) -> TestOrGroup {
        TestOrGroup::test(Test::new(protocol, echo_msg, [], Default::default()))
    }

    fn await_manifest_test_cases(manifest: ManifestCollector) -> Vec<TestCase> {
        loop {
            match manifest.lock().unwrap().take() {
                Some(ManifestResult::Manifest(manifest)) => return manifest.manifest.flatten().0,
                Some(ManifestResult::TestRunnerError { .. }) => unreachable!(),
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

    fn test_echo_n(protocol: ProtocolWitness, num_workers: usize, num_echos: usize) {
        let (write_work, get_next_tests) = work_writer();
        let (results, notify_results, run_completed_successfully) = results_collector();

        let run_id = RunId::unique();
        let mut expected_results = HashMap::new();
        let tests = (0..num_echos).into_iter().map(|i| {
            let echo_string = format!("echo {}", i);
            expected_results.insert(i.to_string(), echo_string.clone());

            echo_test(protocol, echo_string)
        });
        let manifest = ManifestMessage::new(Manifest::new(tests, Default::default()));

        let (default_config, manifest_collector) = setup_pool(
            RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest)),
            run_id.clone(),
            &get_next_tests,
            notify_results,
            run_completed_successfully,
        );

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(num_workers).unwrap(),
            ..default_config
        };

        let mut pool = WorkerPool::new(config);

        // Write the work
        let test_ids = await_manifest_test_cases(manifest_collector);

        for (i, test_id) in test_ids.into_iter().enumerate() {
            write_work(local_work(test_id, run_id.clone(), WorkId(i.to_string())))
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
                .map(|(k, v)| (k, v.into_spec().output.unwrap()))
                .collect();

            results == expected_results
        });

        let exit = pool.shutdown();
        assert!(matches!(exit, WorkersExit::Success));
    }

    #[test]
    #[with_protocol_version]
    fn test_1_worker_1_echo() {
        test_echo_n(proto, 1, 1);
    }

    #[test]
    #[with_protocol_version]
    fn test_2_workers_1_echo() {
        test_echo_n(proto, 2, 1);
    }

    #[test]
    #[with_protocol_version]
    fn test_1_worker_2_echos() {
        test_echo_n(proto, 1, 2);
    }

    #[test]
    #[with_protocol_version]
    fn test_2_workers_2_echos() {
        test_echo_n(proto, 2, 2);
    }

    #[test]
    #[with_protocol_version]
    fn test_2_workers_8_echos() {
        test_echo_n(proto, 2, 8);
    }

    #[test]
    #[cfg(feature = "test-test_ids")]
    fn test_timeout() {
        let (write_work, get_next_tests) = work_writer();
        let (results, notify_results) = results_collector();

        let run_id = RunId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("mona lisa".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::InduceTimeout,
            run_id,
            manifest,
            get_next_tests,
            notify_results,
        );

        let timeout = Duration::from_millis(1);
        let config = WorkerPoolConfig {
            work_timeout: timeout,
            work_retries: 0,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for test_id in await_manifest_test_cases(manifest_collector) {
            write_work(local_work(test_id, run_id, WorkId("id1".to_string())));
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
        let (write_work, get_next_tests) = work_writer();
        let (results, notify_results) = results_collector();

        let run_id = RunId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::EchoOnRetry(10),
            run_id,
            manifest,
            get_next_tests,
            notify_results,
        );

        let config = WorkerPoolConfig {
            work_retries: 0,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for test_id in await_manifest_test_cases(manifest_collector) {
            write_work(local_work(test_id, run_id, WorkId("id1".to_string())));
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
        let (write_work, get_next_tests) = work_writer();
        let (results, notify_results) = results_collector();

        let run_id = RunId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("okay".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::EchoOnRetry(2),
            run_id,
            manifest,
            get_next_tests,
            notify_results,
        );

        let config = WorkerPoolConfig {
            work_retries: 1,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for test_id in await_manifest_test_cases(manifest_collector) {
            write_work(local_work(test_id, run_id, WorkId("id1".to_string())));
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
    #[traced_test]
    fn bad_message_doesnt_take_down_queue_negotiator_server() {
        let listener =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
                .bind("0.0.0.0:0")
                .unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();

        let mut negotiator = QueueNegotiator::new(
            listener_addr.ip(),
            listener,
            shutdown_rx,
            "0.0.0.0:0".parse().unwrap(),
            "0.0.0.0:0".parse().unwrap(),
            |_| panic!("should not ask for assigned run in this test"),
        )
        .unwrap();

        let client = ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls())
            .build()
            .unwrap();
        let mut conn = client.connect(listener_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();

        shutdown_tx.shutdown_immediately().unwrap();
        negotiator.join();

        logs_with_scope_contain("", "error handling connection");
    }

    #[test]
    fn exit_with_error_if_worker_errors() {
        let (_write_work, get_next_tests) = work_writer();
        let (_results, notify_results, run_completed_successfully) = results_collector();
        let (default_config, _manifest_collector) = setup_pool(
            RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                // This command should cause the worker to error, since it can't even be executed
                cmd: "__zzz_not_a_command__".into(),
                args: Default::default(),
                extra_env: Default::default(),
            }),
            RunId::unique(),
            &get_next_tests,
            notify_results,
            run_completed_successfully,
        );

        let mut pool = WorkerPool::new(default_config);

        let pool_exit = pool.shutdown();

        assert!(matches!(pool_exit, WorkersExit::Error));
    }
}
