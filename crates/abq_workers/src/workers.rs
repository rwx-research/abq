use std::io;
use std::num::NonZeroUsize;
use std::panic;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{sync::mpsc, thread};

use abq_generic_test_runner::{GenericRunnerError, GenericRunnerErrorKind, GenericTestRunner};
use abq_utils::error::{here, ErrorLocation, LocatedError, Location};
use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::{self, Entity, RunnerMeta, WorkerRunner, WorkerTag};
use abq_utils::net_protocol::queue::{
    AssociatedTestResults, NativeRunnerInfo, RunAlreadyCompleted, TestSpec,
};
use abq_utils::net_protocol::runners::{
    AbqProtocolVersion, CapturedOutput, ManifestMessage, NativeRunnerSpecification, OutOfBandError,
    Status, TestId, TestResult, TestResultSpec, TestRunnerExit, TestRuntime,
};
use abq_utils::net_protocol::work_server::InitContext;
use abq_utils::net_protocol::workers::{
    ManifestResult, NativeTestRunnerParams, ReportedManifest, TestLikeRunner, WorkerTest,
};
use abq_utils::net_protocol::workers::{NextWork, NextWorkBundle, RunId, RunnerKind};
use abq_utils::results_handler::ResultsHandler;

use crate::liveness::LiveCount;
use crate::results_handler::ResultsHandlerGenerator;
use crate::test_like_runner;

enum MessageFromPool {
    Shutdown,
}

type MessageFromPoolRx = Arc<Mutex<mpsc::Receiver<MessageFromPool>>>;

pub type InitContextResult = Result<InitContext, RunAlreadyCompleted>;

pub use abq_generic_test_runner::TestsFetcher;

pub type GetNextTests = Box<dyn TestsFetcher + Send>;
pub type GetNextTestsGenerator<'a> = &'a (dyn Fn(Entity) -> GetNextTests + Send + Sync);

pub type GetInitContext =
    Arc<dyn Fn(Entity) -> io::Result<InitContextResult> + Send + Sync + 'static>;
pub type NotifyManifest = Box<dyn Fn(Entity, &RunId, ManifestResult) + Send + Sync + 'static>;

pub type NotifyMaterialTestsAllRun = abq_generic_test_runner::NotifyMaterialTestsAllRun;
pub type NotifyMaterialTestsAllRunGenerator<'a> =
    &'a (dyn Fn() -> NotifyMaterialTestsAllRun + Send + Sync);

type MarkWorkerComplete = Box<dyn FnOnce() + Send + Sync + 'static>;

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
    /// Number of runners to start in the pool.
    pub size: NonZeroUsize,
    /// The tagged number of the worker pool in this test run.
    pub tag: WorkerTag,
    /// The entity of the first runner in the pool.
    pub first_runner_entity: Entity,
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
    pub results_handler_generator: ResultsHandlerGenerator<'a>,
    /// How should a runner notify the queue it's run all assigned tests.
    pub notify_all_tests_run_generator: NotifyMaterialTestsAllRunGenerator<'a>,
    /// How many results should be sent back at a time?
    pub results_batch_size_hint: u64,
    /// Context under which workers should operate.
    pub worker_context: WorkerContext,

    /// Whether an ABQ supervisor is running in-band this worker process.
    pub supervisor_in_band: bool,

    /// Whether to allow passthrough of stdout/stderr from the native runner process.
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
    runners: Vec<(RunnerMeta, ThreadWorker)>,
    worker_msg_tx: mpsc::Sender<MessageFromPool>,
    live_count: LiveCount,
}

#[derive(PartialEq, Eq, Debug)]
pub enum WorkersExitStatus {
    /// This pool of workers completed. The exit code is the highest exit code of any native runner.
    Completed(ExitCode),
    Error {
        errors: Vec<String>,
    },
}

impl WorkersExitStatus {
    pub const SUCCESS: Self = Self::Completed(ExitCode::SUCCESS);
}

#[derive(Debug)]
pub struct WorkersExit {
    pub status: WorkersExitStatus,
    pub native_runner_info: Option<NativeRunnerInfo>,
    pub manifest_generation_output: Option<(RunnerMeta, CapturedOutput)>,
    /// Final captured output of each runner, after all tests were run on each runner.
    pub final_captured_outputs: Vec<(RunnerMeta, CapturedOutput)>,
}

impl WorkerPool {
    pub async fn new(config: WorkerPoolConfig<'_>) -> Self {
        let WorkerPoolConfig {
            size,
            first_runner_entity,
            tag: workers_tag,
            runner_kind,
            run_id,
            get_init_context,
            get_next_tests_generator,
            results_batch_size_hint: results_batch_size,
            results_handler_generator,
            notify_manifest,
            notify_all_tests_run_generator,
            worker_context,
            supervisor_in_band,
            debug_native_runner,
        } = config;

        let num_workers = size.get();
        let mut runners = Vec::with_capacity(num_workers);

        let (live_count, signal_completed) = LiveCount::new(num_workers).await;
        tracing::debug!(live_count=?live_count.read(), ?results_batch_size, ?run_id, "Starting worker pool");

        let (worker_msg_tx, worker_msg_rx) = mpsc::channel();
        let shared_worker_msg_rx = Arc::new(Mutex::new(worker_msg_rx));

        let mark_worker_complete = || {
            let signal_completed = signal_completed.clone();
            Box::new(move || {
                signal_completed.completed();
                tracing::debug!("worker done");
            })
        };

        let is_singleton_runner = num_workers == 1;

        {
            // Provision the first worker independently, so that if we need to generate a manifest,
            // only it gets the manifest notifier.
            // TODO: consider hiding this behind a macro for code duplication purposes
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let get_init_context = Arc::clone(&get_init_context);
            let mark_worker_complete = mark_worker_complete();

            let entity = first_runner_entity;
            let runner = WorkerRunner::new(workers_tag, entity::RunnerTag::new(1));
            let runner_meta = RunnerMeta::new(runner, is_singleton_runner);

            debug_assert_eq!(entity.tag, entity::Tag::Runner(runner));

            let worker_env = RunnerEnv {
                entity,
                runner_meta,
                msg_from_pool_rx: msg_rx,
                run_id: run_id.clone(),
                get_init_context,
                tests_fetcher: get_next_tests_generator(entity),
                results_batch_size,
                results_handler: results_handler_generator(entity),
                context: worker_context.clone(),
                notify_manifest,
                notify_all_tests_run: notify_all_tests_run_generator(),
                supervisor_in_band,
                debug_native_runner,
            };

            let runner_kind = {
                let mut runner = runner_kind.clone();
                runner.set_runner_id(1);
                runner
            };

            runners.push((
                runner_meta,
                ThreadWorker::new(runner_kind, worker_env, mark_worker_complete),
            ));
        }

        // Provision the rest of the workers.
        for runner_id in 1..num_workers {
            let msg_rx = Arc::clone(&shared_worker_msg_rx);
            let get_init_context = Arc::clone(&get_init_context);
            let mark_worker_complete = mark_worker_complete();

            let runner_id = runner_id + 1;
            let entity = Entity::runner(workers_tag, runner_id as u32);
            let runner = WorkerRunner::new(workers_tag, entity::RunnerTag::new(runner_id as u32));
            let runner_meta = RunnerMeta::new(runner, is_singleton_runner);
            debug_assert!(!is_singleton_runner);

            let worker_env = RunnerEnv {
                entity,
                runner_meta,
                msg_from_pool_rx: msg_rx,
                run_id: run_id.clone(),
                get_init_context,
                tests_fetcher: get_next_tests_generator(entity),
                results_batch_size,
                results_handler: results_handler_generator(entity),
                notify_all_tests_run: notify_all_tests_run_generator(),
                context: worker_context.clone(),
                notify_manifest: None,
                supervisor_in_band,
                debug_native_runner,
            };

            let runner_kind = {
                let mut runner = runner_kind.clone();
                runner.set_runner_id(runner_id);
                runner
            };

            runners.push((
                runner_meta,
                ThreadWorker::new(runner_kind, worker_env, mark_worker_complete),
            ));
        }

        Self {
            active: true,
            runners,
            worker_msg_tx,
            live_count,
        }
    }

    pub async fn wait(&mut self) {
        self.live_count.wait().await
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

        for _ in 0..self.runners.len() {
            // It's possible the worker already exited if it couldn't connect to the queue; in that
            // case, we won't be able to send across the channel, but that's okay.
            let _ = self.worker_msg_tx.send(MessageFromPool::Shutdown);
        }

        let mut errors = vec![];
        let mut highest_exit_code = ExitCode::new(0);
        let mut final_captured_outputs = Vec::with_capacity(self.runners.len());
        let mut manifest_generation_output = None;
        let mut native_runner_info = None;
        for (runner_meta, runner_thread) in self.runners.iter_mut() {
            let opt_err = runner_thread
                .handle
                .take()
                .expect("worker thread already stolen")
                .join()
                .expect("runner thread panicked rather than erroring");

            let final_captured_output = match opt_err {
                Ok(TestRunnerExit {
                    exit_code,
                    manifest_generation_output: this_manifest_output,
                    final_captured_output,
                    native_runner_info: this_native_runner_info,
                }) => {
                    native_runner_info = native_runner_info.or(this_native_runner_info);

                    debug_assert!(
                        this_manifest_output.is_none() || manifest_generation_output.is_none(),
                    );

                    manifest_generation_output =
                        this_manifest_output.map(|output| (*runner_meta, output));

                    // Choose the highest exit code of all the test runners this worker started to
                    // be the exit code of the worker.
                    highest_exit_code = exit_code.max(exit_code);
                    final_captured_output
                }
                Err(GenericRunnerError {
                    error: kind,
                    output,
                }) => {
                    let LocatedError {
                        error,
                        location: Location { file, line, column },
                    } = &kind;

                    tracing::error!(
                        ?error,
                        file,
                        line,
                        column,
                        "worker thread exited with error"
                    );

                    errors.push(kind.to_string());
                    output
                }
            };
            final_captured_outputs.push((*runner_meta, final_captured_output));
        }

        let status = if !errors.is_empty() {
            WorkersExitStatus::Error { errors }
        } else {
            WorkersExitStatus::Completed(highest_exit_code)
        };

        WorkersExit {
            manifest_generation_output,
            final_captured_outputs,
            status,
            native_runner_info,
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
    handle: Option<thread::JoinHandle<Result<TestRunnerExit, GenericRunnerError>>>,
}

enum AttemptError {
    ShouldRetry,
    Panic(String),
}

type AttemptResult = Result<Vec<String>, AttemptError>;

struct RunnerEnv {
    entity: Entity,
    runner_meta: RunnerMeta,
    msg_from_pool_rx: MessageFromPoolRx,
    run_id: RunId,
    notify_manifest: Option<NotifyManifest>,
    get_init_context: GetInitContext,
    tests_fetcher: GetNextTests,
    results_batch_size: u64,
    results_handler: ResultsHandler,
    notify_all_tests_run: NotifyMaterialTestsAllRun,
    context: WorkerContext,
    // Is this running in-band with a supervisor process?
    supervisor_in_band: bool,
    debug_native_runner: bool,
}

impl ThreadWorker {
    pub fn new(
        runner_kind: RunnerKind,
        worker_env: RunnerEnv,
        mark_worker_complete: MarkWorkerComplete,
    ) -> Self {
        let handle = thread::spawn(move || {
            let result_wrapper =
                panic::catch_unwind(panic::AssertUnwindSafe(|| match runner_kind {
                    RunnerKind::GenericNativeTestRunner(params) => {
                        start_generic_test_runner(worker_env, params)
                    }
                    RunnerKind::TestLikeRunner(runner, manifest) => {
                        start_test_like_runner(worker_env, runner, *manifest)
                    }
                }));

            mark_worker_complete();

            result_wrapper.unwrap()
        });

        Self {
            handle: Some(handle),
        }
    }
}

fn start_generic_test_runner(
    env: RunnerEnv,
    native_runner_params: NativeTestRunnerParams,
) -> Result<TestRunnerExit, GenericRunnerError> {
    let RunnerEnv {
        entity,
        runner_meta,
        tests_fetcher,
        run_id,
        get_init_context,
        results_handler,
        notify_manifest,
        notify_all_tests_run,
        results_batch_size,
        context,
        msg_from_pool_rx,
        supervisor_in_band,
        debug_native_runner,
    } = env;

    tracing::debug!(?entity, ?run_id, "Starting new generic test runner");

    if !supervisor_in_band {
        // We expose the worker ID to the end user, even without tracing to standard pipes enabled,
        // so that they can correlate failures observed in workers with the workers they've launched.
        eprintln!("Generic test runner started on worker {:?}", entity);
    }

    let notify_manifest = notify_manifest.map(|notify_manifest| {
        let run_id = run_id.clone();
        move |manifest_result| notify_manifest(entity, &run_id, manifest_result)
    });

    let get_init_context = move || get_init_context(entity);

    let get_next_tests_bundle: abq_generic_test_runner::GetNextTests = tests_fetcher;

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
        entity,
        runner_meta,
        native_runner_params,
        &working_dir,
        polling_should_shutdown,
        results_batch_size,
        notify_manifest,
        get_init_context,
        get_next_tests_bundle,
        results_handler,
        notify_all_tests_run,
        debug_native_runner,
    )
}

fn build_test_like_runner_manifest_result(
    manifest_message: ManifestMessage,
) -> Result<ReportedManifest, OutOfBandError> {
    let manifest = match manifest_message {
        ManifestMessage::Success(m) => m.manifest,
        ManifestMessage::Failure(fail) => return Err(fail.error),
    };

    let native_runner_protocol = AbqProtocolVersion::V0_2;
    let native_runner_specification = NativeRunnerSpecification {
        name: "unknown-test-like-runner".to_string(),
        version: "0.0.1".to_owned(),
        test_framework: "rspec".to_owned(),
        test_framework_version: "3.12.0".to_owned(),
        language: "ruby".to_owned(),
        language_version: "3.1.2p20".to_owned(),
        host: "ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned(),
    };

    Ok(ReportedManifest {
        manifest,
        native_runner_protocol,
        native_runner_specification: Box::new(native_runner_specification),
    })
}

fn start_test_like_runner(
    env: RunnerEnv,
    runner: TestLikeRunner,
    manifest: ManifestMessage,
) -> Result<TestRunnerExit, GenericRunnerError> {
    let RunnerEnv {
        entity,
        runner_meta,
        msg_from_pool_rx,
        mut tests_fetcher,
        get_init_context,
        run_id,
        results_batch_size: _,
        mut results_handler,
        notify_all_tests_run,
        context,
        notify_manifest,
        supervisor_in_band: _,
        debug_native_runner: _,
    } = env;

    match runner {
        TestLikeRunner::NeverReturnManifest => {
            return Err(GenericRunnerError::no_captures(
                io::Error::new(io::ErrorKind::Unsupported, "will not return manifest")
                    .located(here!()),
            ));
        }
        TestLikeRunner::ExitWith(ec) => {
            return Ok(TestRunnerExit {
                exit_code: ExitCode::new(ec),
                manifest_generation_output: None,
                final_captured_output: CapturedOutput::empty(),
                native_runner_info: None,
            });
        }
        TestLikeRunner::Panic => {
            panic!("forced TestLikeRunner::Panic")
        }
        _ => { /* pass through */ }
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
                        output: CapturedOutput::empty(),
                    },
                );
                return Err(GenericRunnerError::no_captures(
                    GenericRunnerErrorKind::NativeRunner(oob.into()).located(here!()),
                ));
            }
        };
    }

    let init_context = match get_init_context(entity) {
        Ok(context_result) => match context_result {
            Ok(ctx) => ctx,
            Err(RunAlreadyCompleted {}) => {
                return Ok(TestRunnerExit {
                    exit_code: ExitCode::SUCCESS,
                    manifest_generation_output: None,
                    final_captured_output: CapturedOutput::empty(),
                    native_runner_info: None,
                });
            }
        },
        Err(io_err) => {
            tracing::error!(io_err = io_err.to_string(), "Error getting initial context");
            return Ok(TestRunnerExit {
                exit_code: ExitCode::ABQ_ERROR,
                manifest_generation_output: None,
                final_captured_output: CapturedOutput::empty(),
                native_runner_info: None,
            });
        }
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
                    return Err(GenericRunnerError::no_captures(
                        io::Error::new(io::ErrorKind::Unsupported, "will not return test")
                            .located(here!()),
                    ));
                }
                break;
            }
            Err(mpsc::TryRecvError::Disconnected) => panic!("Pool died before worker did"),
            Err(mpsc::TryRecvError::Empty) => {
                // No message from the parent. Wait for the next test_id to come in.
                let NextWorkBundle { work: bundle, .. } =
                    rt.block_on(tests_fetcher.get_next_tests());
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
                    spec: TestSpec { test_case, work_id },
                    run_number,
                }) => {
                    if matches!(&runner, TestLikeRunner::NeverReturnOnTest(t) if t == test_case.id() )
                    {
                        return Err(GenericRunnerError::no_captures(
                            io::Error::new(io::ErrorKind::Unsupported, "will not return test")
                                .located(here!()),
                        ));
                    }

                    // Try the test_id once + how ever many retries were requested.
                    let allowed_attempts = 1;
                    'attempts: for attempt_number in 1.. {
                        let start_time = Instant::now();
                        let attempt_result = attempt_test_id_for_test_like_runner(
                            &context,
                            runner.clone(),
                            test_case.id().clone(),
                            init_context.clone(),
                            attempt_number,
                            allowed_attempts,
                            run_number,
                        );
                        let runtime = start_time.elapsed().as_millis() as f64;

                        let (status, outputs) = match attempt_result {
                            Ok(output) => (Status::Success, output),
                            Err(AttemptError::ShouldRetry) => continue 'attempts,
                            Err(AttemptError::Panic(msg)) => (
                                Status::Error {
                                    exception: None,
                                    backtrace: None,
                                },
                                vec![msg],
                            ),
                        };
                        let results = outputs
                            .into_iter()
                            .map(|output| {
                                TestResult::new(
                                    runner_meta,
                                    TestResultSpec {
                                        status: status.clone(),
                                        id: test_case.id().clone(),
                                        display_name: test_case.id().clone(),
                                        output: Some(output),
                                        runtime: TestRuntime::Milliseconds(runtime),
                                        meta: Default::default(),
                                        ..TestResultSpec::fake()
                                    },
                                )
                            })
                            .collect();

                        let associated_result = AssociatedTestResults {
                            work_id,
                            run_number,
                            results,
                            before_any_test: CapturedOutput::empty(),
                            after_all_tests: None,
                        };

                        rt.block_on(results_handler.send_results(vec![associated_result]));
                        break 'attempts;
                    }
                }
            }
        }
    }

    rt.block_on(notify_all_tests_run(entity));

    Ok(TestRunnerExit {
        exit_code: ExitCode::SUCCESS,
        manifest_generation_output: None,
        final_captured_output: CapturedOutput::empty(),
        native_runner_info: None,
    })
}

#[inline(always)]
fn attempt_test_id_for_test_like_runner(
    _my_context: &WorkerContext,
    runner: TestLikeRunner,
    test_id: TestId,
    init_context: InitContext,
    attempt: u8,
    allowed_attempts: u8,
    run_number: u32,
) -> AttemptResult {
    use TestLikeRunner as R;

    let init_context = serde_json::to_string(&init_context).unwrap();

    let result_handle = thread::spawn(move || {
        let _attempt = attempt;
        match (runner, test_id) {
            (R::Echo, s) => {
                let result = test_like_runner::echo(s);
                vec![result]
            }
            (R::EchoMany { separator }, s) => {
                let split_results = s
                    .split(separator)
                    .map(|s| test_like_runner::echo(s.to_string()));
                split_results.collect()
            }
            (R::EchoInitContext, _) => {
                let ctx_result = test_like_runner::echo(init_context);
                vec![ctx_result]
            }
            (R::Exec, cmd_and_args) => {
                let mut args = cmd_and_args
                    .split(' ')
                    .map(ToOwned::to_owned)
                    .collect::<Vec<String>>();
                let cmd = args.remove(0);
                let result = test_like_runner::exec(test_like_runner::ExecWork { cmd, args });
                vec![result]
            }
            (R::FailOnTestName(fail_name), test) => {
                if test == fail_name {
                    vec![]
                } else {
                    vec!["PASS".to_string()]
                }
            }
            (R::FailUntilAttemptNumber(n), test) => {
                if n != run_number {
                    vec![]
                } else {
                    vec![test]
                }
            }
            #[cfg(feature = "test-test_ids")]
            (R::EchoOnRetry(succeed_on), s) => {
                if succeed_on == _attempt {
                    let result = echo::EchoWorker::run(echo::EchoWork { message: s });
                    vec![result]
                } else {
                    panic!("Failed to echo!");
                }
            }
            (runner, test_id) => unreachable!(
                "Invalid runner/test_id combination: {:?} and {:?}",
                runner, test_id
            ),
        }
    });

    let result = result_handle.join();
    match result {
        Ok(output) => {
            if output.is_empty() {
                Err(AttemptError::Panic("INDUCED FAIL".to_owned()))
            } else {
                Ok(output)
            }
        }
        Err(e) => {
            if attempt < allowed_attempts {
                Err(AttemptError::ShouldRetry)
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
    use std::io;
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use abq_test_utils::artifacts_dir;
    use abq_utils::auth::{ClientAuthStrategy, ServerAuthStrategy};
    use abq_utils::exit::ExitCode;
    use abq_utils::net_opt::{ClientOptions, ServerOptions};
    use abq_utils::net_protocol::entity::{Entity, WorkerTag};
    use abq_utils::net_protocol::queue::{AssociatedTestResults, TestSpec};
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, ProtocolWitness, Test, TestCase, TestOrGroup, TestResult,
    };
    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        ManifestResult, NativeTestRunnerParams, NextWork, NextWorkBundle, TestLikeRunner,
        WorkerTest, INIT_RUN_NUMBER,
    };
    use abq_utils::results_handler::{NotifyResults, ResultsHandler};
    use abq_utils::shutdown::ShutdownManager;
    use abq_utils::tls::{ClientTlsStrategy, ServerTlsStrategy};
    use abq_utils::{atomic, net_protocol};
    use abq_with_protocol_version::with_protocol_version;
    use async_trait::async_trait;
    use futures::FutureExt;
    use tracing_test::internal::logs_with_scope_contain;
    use tracing_test::traced_test;

    use super::{
        GetNextTests, GetNextTestsGenerator, InitContextResult, NotifyManifest,
        NotifyMaterialTestsAllRun, NotifyMaterialTestsAllRunGenerator, TestsFetcher, WorkerContext,
        WorkerPool,
    };
    use crate::negotiate::QueueNegotiator;
    use crate::results_handler::ResultsHandlerGenerator;
    use crate::workers::{WorkerPoolConfig, WorkersExitStatus};
    use abq_utils::net_protocol::workers::{RunId, RunnerKind, WorkId};

    type ResultsCollector = Arc<Mutex<HashMap<WorkId, Vec<TestResult>>>>;
    type ManifestCollector = Arc<Mutex<Option<ManifestResult>>>;

    struct Fetcher {
        reader: Arc<Mutex<VecDeque<NextWork>>>,
    }

    #[async_trait]
    impl TestsFetcher for Fetcher {
        async fn get_next_tests(&mut self) -> NextWorkBundle {
            loop {
                if let Some(work) = self.reader.lock().unwrap().pop_front() {
                    return NextWorkBundle::new(vec![work]);
                }
            }
        }
    }

    fn work_writer() -> (impl Fn(NextWork), impl Fn(Entity) -> GetNextTests) {
        let writer: Arc<Mutex<VecDeque<NextWork>>> = Default::default();
        let reader = Arc::clone(&writer);
        let write_work = move |work| {
            writer.lock().unwrap().push_back(work);
        };

        let get_next_tests = move |_| {
            let reader = reader.clone();
            let get_next_tests: GetNextTests = Box::new(Fetcher { reader });
            get_next_tests
        };
        (write_work, get_next_tests)
    }

    fn manifest_collector() -> (ManifestCollector, NotifyManifest) {
        let man: ManifestCollector = Arc::new(Mutex::new(None));
        let man2 = Arc::clone(&man);
        let results_handler: NotifyManifest = Box::new(move |_, _, man| {
            let old_manifest = man2.lock().unwrap().replace(man);
            debug_assert!(old_manifest.is_none(), "Overwriting a manifest! This is either a bug in your test, or the worker pool implementation.");
        });
        (man, results_handler)
    }

    struct StaticResultsCollector {
        results: ResultsCollector,
    }

    #[async_trait]
    impl NotifyResults for StaticResultsCollector {
        async fn send_results(&mut self, results: Vec<AssociatedTestResults>) {
            for AssociatedTestResults {
                work_id, results, ..
            } in results
            {
                let old_result = self.results.lock().unwrap().insert(work_id, results);
                debug_assert!(old_result.is_none(), "Overwriting a result! This is either a bug in your test, or the worker pool implementation.");
            }
        }
    }

    fn results_collector() -> (ResultsCollector, impl Fn(Entity) -> ResultsHandler) {
        let results: ResultsCollector = Default::default();
        let results2 = results.clone();

        let results_handler_generator = move |_: Entity| {
            let results_notifier: ResultsHandler = Box::new(StaticResultsCollector {
                results: results2.clone(),
            });
            results_notifier
        };
        (results, results_handler_generator)
    }

    fn notify_all_tests_run() -> (
        Arc<Mutex<Vec<Arc<AtomicBool>>>>,
        impl Fn() -> NotifyMaterialTestsAllRun,
    ) {
        let all = Arc::new(Mutex::new(vec![]));
        let generator = {
            let all = all.clone();
            move || {
                let atomic = Arc::new(AtomicBool::new(false));
                all.lock().unwrap().push(atomic.clone());

                let notifier = move |_| {
                    async move {
                        atomic.store(true, atomic::ORDERING);
                    }
                    .boxed()
                };
                let notifier: NotifyMaterialTestsAllRun = Box::new(notifier);
                notifier
            }
        };
        (all, generator)
    }

    fn empty_init_context(_entity: Entity) -> io::Result<InitContextResult> {
        Ok(Ok(InitContext {
            init_meta: Default::default(),
        }))
    }

    fn setup_pool<'a>(
        runner_kind: RunnerKind,
        worker_pool_tag: WorkerTag,
        run_id: RunId,
        get_next_tests_generator: GetNextTestsGenerator<'a>,
        results_handler_generator: ResultsHandlerGenerator<'a>,
        notify_all_tests_run_generator: NotifyMaterialTestsAllRunGenerator<'a>,
    ) -> (WorkerPoolConfig<'a>, ManifestCollector) {
        let (manifest_collector, notify_manifest) = manifest_collector();

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(1).unwrap(),
            tag: worker_pool_tag,
            first_runner_entity: Entity::first_runner(worker_pool_tag),
            get_next_tests_generator,
            get_init_context: Arc::new(empty_init_context),
            results_batch_size_hint: 5,
            runner_kind,
            run_id,
            notify_manifest: Some(notify_manifest),
            results_handler_generator,
            notify_all_tests_run_generator,
            worker_context: WorkerContext::AssumeLocal,
            supervisor_in_band: false,
            debug_native_runner: false,
        };

        (config, manifest_collector)
    }

    fn local_work(test: TestCase, work_id: WorkId) -> NextWork {
        NextWork::Work(WorkerTest {
            spec: TestSpec {
                test_case: test,
                work_id,
            },
            run_number: INIT_RUN_NUMBER,
        })
    }

    pub fn echo_test(protocol: ProtocolWitness, echo_msg: String) -> TestOrGroup {
        TestOrGroup::test(Test::new(protocol, echo_msg, [], Default::default()))
    }

    fn await_manifest_tests(manifest: ManifestCollector) -> Vec<TestSpec> {
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

    async fn test_echo_n(protocol: ProtocolWitness, num_workers: usize, num_echos: usize) {
        let (write_work, get_next_tests) = work_writer();
        let (results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();

        let run_id = RunId::unique();
        let mut expected_results = HashMap::new();
        let tests = (0..num_echos).into_iter().map(|i| {
            let echo_string = format!("echo {}", i);
            expected_results.insert(WorkId([i as _; 16]), vec![echo_string.clone()]);

            echo_test(protocol, echo_string)
        });
        let manifest = ManifestMessage::new(Manifest::new(tests, Default::default()));

        let (default_config, manifest_collector) = setup_pool(
            RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest)),
            WorkerTag::new(0),
            run_id,
            &get_next_tests,
            &results_handler_generator,
            &notify_all_tests_run_generator,
        );

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(num_workers).unwrap(),
            ..default_config
        };

        let mut pool = WorkerPool::new(config).await;

        // Write the work
        let tests = await_manifest_tests(manifest_collector);

        for (i, test) in tests.into_iter().enumerate() {
            write_work(local_work(test.test_case, WorkId([i as _; 16])))
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
                .map(|(k, rs)| {
                    let results_outputs = rs.into_iter().map(|r| r.into_spec().output.unwrap());
                    (k, results_outputs.collect())
                })
                .collect();

            results == expected_results
        });

        let exit = pool.shutdown();
        assert!(matches!(
            exit.status,
            WorkersExitStatus::Completed(ExitCode::SUCCESS)
        ));

        let all_completed = all_completed.lock().unwrap();
        assert_eq!(all_completed.len(), num_workers);
        assert!(all_completed.iter().all(|b| b.load(atomic::ORDERING)));
    }

    fn abqtest_write_runner_number_path() -> PathBuf {
        artifacts_dir().join("abqtest_write_runner_number")
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn test_1_worker_1_echo() {
        test_echo_n(proto, 1, 1).await;
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn test_2_workers_1_echo() {
        test_echo_n(proto, 2, 1).await;
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn test_1_worker_2_echos() {
        test_echo_n(proto, 1, 2).await;
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn test_2_workers_2_echos() {
        test_echo_n(proto, 2, 2).await;
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn test_2_workers_8_echos() {
        test_echo_n(proto, 2, 8).await;
    }

    #[test]
    #[cfg(feature = "test-test_ids")]
    fn test_timeout() {
        let (write_work, get_next_tests) = work_writer();
        let (results, results_handler) = results_collector();

        let run_id = RunId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("mona lisa".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::InduceTimeout,
            run_id,
            manifest,
            get_next_tests,
            results_handler,
        );

        let timeout = Duration::from_millis(1);
        let config = WorkerPoolConfig {
            work_timeout: timeout,
            work_retries: 0,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for test_id in await_manifest_test_specs(manifest_collector) {
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
        let (results, results_handler) = results_collector();

        let run_id = RunId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::EchoOnRetry(10),
            run_id,
            manifest,
            get_next_tests,
            results_handler,
        );

        let config = WorkerPoolConfig {
            work_retries: 0,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for test_id in await_manifest_test_specs(manifest_collector) {
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
        let (results, results_handler) = results_collector();

        let run_id = RunId::new();
        let manifest = ManifestMessage {
            test_ids: vec![TestId::Echo("okay".to_string())],
        };

        let (default_config, manifest_collector) = setup_pool(
            TestLikeRunner::EchoOnRetry(2),
            run_id,
            manifest,
            get_next_tests,
            results_handler,
        );

        let config = WorkerPoolConfig {
            work_retries: 1,
            ..default_config
        };
        let mut pool = WorkerPool::new(config);

        for test_id in await_manifest_test_specs(manifest_collector) {
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
            |_, _| panic!("should not ask for assigned run in this test"),
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

    #[tokio::test]
    async fn exit_with_error_if_worker_errors() {
        let (_write_work, get_next_tests) = work_writer();
        let (_results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();
        let (default_config, _manifest_collector) = setup_pool(
            RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                // This command should cause the worker to error, since it can't even be executed
                cmd: "__zzz_not_a_command__".into(),
                args: Default::default(),
                extra_env: Default::default(),
            }),
            WorkerTag::new(0),
            RunId::unique(),
            &get_next_tests,
            &results_handler_generator,
            &notify_all_tests_run_generator,
        );

        let mut pool = WorkerPool::new(default_config).await;

        let pool_exit = pool.shutdown();

        assert!(matches!(pool_exit.status, WorkersExitStatus::Error { .. }));

        let all_completed = all_completed.lock().unwrap();
        assert_eq!(all_completed.len(), 1);
        assert!(!all_completed[0].load(atomic::ORDERING));
    }

    #[tokio::test]
    async fn sets_abq_native_runner_env_vars() {
        let (_write_work, get_next_tests) = work_writer();
        let (_results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();

        let writefile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let writefile_path = writefile.to_path_buf();

        let (mut config, _manifest_collector) = setup_pool(
            RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                cmd: abqtest_write_runner_number_path().display().to_string(),
                args: vec![writefile_path.display().to_string()],
                extra_env: Default::default(),
            }),
            WorkerTag::new(0),
            RunId::unique(),
            &get_next_tests,
            &results_handler_generator,
            &notify_all_tests_run_generator,
        );

        config.size = NonZeroUsize::new(5).unwrap();

        let mut pool = WorkerPool::new(config).await;
        let _pool_exit = pool.shutdown();

        let worker_ids = std::fs::read_to_string(writefile).unwrap();
        let mut worker_ids: Vec<_> = worker_ids.trim().split('\n').collect();
        worker_ids.sort();
        assert_eq!(worker_ids, &["1", "2", "3", "4", "5"]);

        let all_completed = all_completed.lock().unwrap();
        assert_eq!(all_completed.len(), 5);
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn worker_exits_with_runner_exit() {
        let (_write_work, get_next_tests) = work_writer();
        let (_results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();

        let manifest = ManifestMessage::new(Manifest::new(
            [echo_test(proto, "test1".to_string())],
            Default::default(),
        ));
        let (mut config, _manifest_collector) = setup_pool(
            RunnerKind::TestLikeRunner(TestLikeRunner::ExitWith(27), Box::new(manifest)),
            WorkerTag::new(0),
            RunId::unique(),
            &get_next_tests,
            &results_handler_generator,
            &notify_all_tests_run_generator,
        );

        config.size = NonZeroUsize::new(1).unwrap();

        let mut pool = WorkerPool::new(config).await;
        let pool_exit = pool.shutdown();

        assert!(
            matches!(
                pool_exit.status,
                WorkersExitStatus::Completed(ec) if ec.get() == 27
            ),
            "{pool_exit:?}"
        );

        let all_completed = all_completed.lock().unwrap();
        assert_eq!(all_completed.len(), 1);
    }
}
