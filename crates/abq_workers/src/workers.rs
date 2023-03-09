use std::io;
use std::num::NonZeroUsize;
use std::panic;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use abq_generic_test_runner::{GenericRunnerError, GenericRunnerErrorKind};
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
    ManifestResult, NativeTestRunnerParams, NextWorkBundle, ReportedManifest, TestLikeRunner,
    WorkerTest,
};
use abq_utils::net_protocol::workers::{RunId, RunnerKind};
use abq_utils::oneshot_notify::{self, OneshotRx, OneshotTx};
use abq_utils::results_handler::ResultsHandler;
use async_trait::async_trait;
use futures::FutureExt;

use crate::liveness::{CompletedSignaler, LiveCount};
use crate::runner_strategy::RunnerStrategy;
use crate::{runner_strategy, test_like_runner};

pub type InitContextResult = Result<InitContext, RunAlreadyCompleted>;

pub use abq_generic_test_runner::GetNextTests;
pub use abq_generic_test_runner::NotifyMaterialTestsAllRun;
pub use abq_generic_test_runner::TestsFetcher;

pub type NotifyManifest = abq_generic_test_runner::SendManifest;
pub type GetInitContext = abq_generic_test_runner::GetInitContext;

#[async_trait]
pub trait NotifyCancellation: Send {
    async fn cancel(self: Box<Self>);
}

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
    /// Whether a runner on this pool should generate the manifest.
    pub some_runner_should_generate_manifest: bool,
    /// How runners should communicate.
    pub runner_strategy_generator: &'a dyn runner_strategy::StrategyGenerator,
    /// How should the workers notify cancellation.
    pub notify_cancellation: Box<dyn NotifyCancellation>,
    /// How many results should be sent back at a time?
    pub results_batch_size_hint: u64,
    /// Context under which workers should operate.
    pub worker_context: WorkerContext,

    /// Whether to allow passthrough of stdout/stderr from the native runner process.
    pub debug_native_runner: bool,
    /// The maximum amount of time to wait for the protocol version message.
    pub protocol_version_timeout: Duration,
    pub test_timeout: Duration,
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
    /// Transmitters of immediate-shutdown signals to each runner.
    /// `None` if shutdown has already been called.
    runners_shutdown: Option<Vec<OneshotTx>>,
    live_count: LiveCount,

    notify_cancellation: Option<Box<dyn NotifyCancellation>>,
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
#[must_use]
pub struct WorkersExit {
    pub status: WorkersExitStatus,
    pub native_runner_info: Option<NativeRunnerInfo>,
    pub manifest_generation_output: Option<(RunnerMeta, CapturedOutput)>,
    /// Final captured output of each runner, after all tests were run on each runner.
    pub final_captured_outputs: Vec<(RunnerMeta, CapturedOutput)>,
}

struct SignalRunnerCompletion {
    runner_id: usize,
    signal_completed: CompletedSignaler,
}

impl SignalRunnerCompletion {
    async fn completed(self) {
        self.signal_completed.completed().await;
        tracing::debug!("runner {} done", self.runner_id);
    }
}

impl WorkerPool {
    pub async fn new(config: WorkerPoolConfig<'_>) -> Self {
        let WorkerPoolConfig {
            size,
            first_runner_entity,
            tag: workers_tag,
            runner_kind,
            run_id,
            some_runner_should_generate_manifest,
            results_batch_size_hint: results_batch_size,
            runner_strategy_generator,
            notify_cancellation,
            worker_context,
            debug_native_runner,
            protocol_version_timeout,
            test_timeout,
        } = config;

        let num_workers = size.get();
        let mut runners = Vec::with_capacity(num_workers);

        let (live_count, signal_completed) = LiveCount::new(num_workers).await;
        tracing::debug!(live_count=?live_count.read(), ?results_batch_size, ?run_id, "Starting worker pool");

        let mut runners_shutdown = Vec::with_capacity(num_workers);

        let is_singleton_runner = num_workers == 1;

        for runner_id in 1..=num_workers {
            let (shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();
            runners_shutdown.push(shutdown_tx);

            let mark_runner_complete = SignalRunnerCompletion {
                runner_id,
                signal_completed: signal_completed.clone(),
            };

            let entity = if runner_id == 1 {
                first_runner_entity
            } else {
                Entity::runner(workers_tag, runner_id as u32)
            };
            let runner = WorkerRunner::new(workers_tag, entity::RunnerTag::new(runner_id as u32));
            let runner_meta = RunnerMeta::new(runner, is_singleton_runner);

            // Have the first runner generate the manifest, if applicable.
            let should_generate_manifest = some_runner_should_generate_manifest && runner_id == 1;

            let RunnerStrategy {
                notify_manifest,
                get_init_context,
                get_next_tests,
                results_handler,
                notify_all_tests_run,
            } = runner_strategy_generator.generate(entity, should_generate_manifest);

            let runner_env = RunnerEnv {
                entity,
                runner_meta,
                shutdown_immediately: shutdown_rx,
                run_id: run_id.clone(),
                get_init_context,
                tests_fetcher: get_next_tests,
                results_batch_size,
                results_handler,
                notify_all_tests_run,
                context: worker_context.clone(),
                notify_manifest,
                debug_native_runner,
                protocol_version_timeout,
                test_timeout,
            };

            let runner_kind = {
                let mut runner = runner_kind.clone();
                runner.set_runner_id(runner_id);
                runner
            };

            runners.push((
                runner_meta,
                ThreadWorker::new(runner_kind, runner_env, mark_runner_complete),
            ));
        }

        Self {
            active: true,
            runners,
            runners_shutdown: Some(runners_shutdown),
            live_count,
            notify_cancellation: Some(notify_cancellation),
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

    pub async fn cancel(&mut self) {
        let notify_cancellation = std::mem::take(&mut self.notify_cancellation)
            .expect("illegal state - cannot cancel more than once");
        notify_cancellation.cancel().await;
    }

    /// Shuts down the worker pool, returning the pool [exit status][WorkersExit].
    pub async fn shutdown(&mut self) -> WorkersExit {
        debug_assert!(self.active);

        self.active = false;

        if let Some(runners_shutdown) = std::mem::take(&mut self.runners_shutdown) {
            for tx_shutdown in runners_shutdown {
                // It's possible the runner already exited, for example if it already finished, or
                // errored early. In that case, we won't be able to send across the channel, but that's okay.
                let _ = tx_shutdown.notify();
            }
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
                .await
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
    handle: Option<tokio::task::JoinHandle<Result<TestRunnerExit, GenericRunnerError>>>,
}

enum AttemptError {
    ShouldRetry,
    Panic(String),
}

type AttemptResult = Result<Vec<String>, AttemptError>;

struct RunnerEnv {
    entity: Entity,
    runner_meta: RunnerMeta,
    shutdown_immediately: OneshotRx,
    run_id: RunId,
    notify_manifest: Option<NotifyManifest>,
    get_init_context: GetInitContext,
    tests_fetcher: GetNextTests,
    results_batch_size: u64,
    results_handler: ResultsHandler,
    notify_all_tests_run: NotifyMaterialTestsAllRun,
    context: WorkerContext,
    protocol_version_timeout: Duration,
    test_timeout: Duration,
    debug_native_runner: bool,
}

impl ThreadWorker {
    pub fn new(
        runner_kind: RunnerKind,
        worker_env: RunnerEnv,
        mark_runner_complete: SignalRunnerCompletion,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let result_wrapper = match runner_kind {
                RunnerKind::GenericNativeTestRunner(params) => {
                    panic::AssertUnwindSafe(start_generic_test_runner(worker_env, params))
                        .catch_unwind()
                        .await
                }
                RunnerKind::TestLikeRunner(runner, manifest) => {
                    panic::AssertUnwindSafe(start_test_like_runner(worker_env, runner, *manifest))
                        .catch_unwind()
                        .await
                }
            };

            mark_runner_complete.completed().await;

            result_wrapper.unwrap()
        });

        Self {
            handle: Some(handle),
        }
    }
}

async fn start_generic_test_runner(
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
        shutdown_immediately,
        protocol_version_timeout,
        test_timeout,
        debug_native_runner,
    } = env;

    tracing::debug!(?entity, ?run_id, "Starting new generic test runner");

    // We expose the worker ID to the end user, even without tracing to standard pipes enabled,
    // so that they can correlate failures observed in workers with the workers they've launched.
    eprintln!(
        "Generic test runner for {:?} started on worker {:?}",
        run_id, entity
    );

    let get_next_tests_bundle: abq_generic_test_runner::GetNextTests = tests_fetcher;

    let working_dir = match context {
        WorkerContext::AssumeLocal => std::env::current_dir().unwrap(),
        WorkerContext::AlwaysWorkIn { working_dir } => working_dir,
    };

    // Running in its own thread.
    abq_generic_test_runner::run_async(
        runner_meta,
        native_runner_params,
        protocol_version_timeout,
        test_timeout,
        working_dir,
        shutdown_immediately,
        results_batch_size,
        notify_manifest,
        get_init_context,
        get_next_tests_bundle,
        results_handler,
        notify_all_tests_run,
        debug_native_runner,
    )
    .await
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

async fn start_test_like_runner(
    env: RunnerEnv,
    runner: TestLikeRunner,
    manifest: ManifestMessage,
) -> Result<TestRunnerExit, GenericRunnerError> {
    let RunnerEnv {
        runner_meta,
        shutdown_immediately,
        tests_fetcher,
        get_init_context,
        results_handler,
        notify_all_tests_run,
        context,
        notify_manifest,
        ..
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
                notify_manifest
                    .send_manifest(ManifestResult::Manifest(manifest))
                    .await;
            }
            Err(oob) => {
                notify_manifest
                    .send_manifest(ManifestResult::TestRunnerError {
                        error: oob.to_string(),
                        output: CapturedOutput::empty(),
                    })
                    .await;
                return Err(GenericRunnerError::no_captures(
                    GenericRunnerErrorKind::NativeRunner(oob.into()).located(here!()),
                ));
            }
        };
    }

    let init_context_result = get_init_context.get_init_context().await;
    let init_context = match init_context_result {
        Ok(context_result) => match context_result {
            Ok(ctx) => ctx,
            Err(RunAlreadyCompleted { cancelled }) => {
                let exit_code = if cancelled {
                    ExitCode::CANCELLED
                } else {
                    ExitCode::SUCCESS
                };

                return Ok(TestRunnerExit {
                    exit_code,
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

    tokio::select! {
        exit_early = test_like_runner_exec_loop(tests_fetcher, runner.clone(), context, init_context, runner_meta, results_handler) => {
            if let Some(value) = exit_early
            {
                return value;
            }
        }
        _ = shutdown_immediately => {
            if matches!(runner, TestLikeRunner::NeverReturnOnTest(..)) {
                return Err(GenericRunnerError::no_captures(
                    io::Error::new(io::ErrorKind::Unsupported, "will not return test")
                        .located(here!()),
                ));
            }
        }
    }

    notify_all_tests_run().await;

    Ok(TestRunnerExit {
        exit_code: ExitCode::SUCCESS,
        manifest_generation_output: None,
        final_captured_output: CapturedOutput::empty(),
        native_runner_info: None,
    })
}

async fn test_like_runner_exec_loop(
    mut tests_fetcher: GetNextTests,
    runner: TestLikeRunner,
    context: WorkerContext,
    init_context: InitContext,
    runner_meta: RunnerMeta,
    mut results_handler: ResultsHandler,
) -> Option<Result<TestRunnerExit, GenericRunnerError>> {
    'tests_done: loop {
        let NextWorkBundle { work, eow } = match tests_fetcher.get_next_tests().await {
            Ok(bundle) => bundle,
            Err(e) => return Some(Err(GenericRunnerError::no_captures(e.located(here!())))),
        };

        for WorkerTest {
            spec: TestSpec { test_case, work_id },
            run_number,
        } in work
        {
            if matches!(&runner, TestLikeRunner::NeverReturnOnTest(t) if t == test_case.id() ) {
                return Some(Err(GenericRunnerError::no_captures(
                    io::Error::new(io::ErrorKind::Unsupported, "will not return test")
                        .located(here!()),
                )));
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
                )
                .await;
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

                results_handler.send_results(vec![associated_result]).await;
                break 'attempts;
            }
        }

        if eow.0 {
            break 'tests_done;
        }
    }

    None
}

#[inline(always)]
async fn attempt_test_id_for_test_like_runner(
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

    let result_handle = tokio::spawn(async move {
        let _attempt = attempt;
        match (runner, test_id) {
            (R::Echo, s) => {
                let result = test_like_runner::echo(s);
                vec![result]
            }
            (R::InduceTimeout, _) => {
                tokio::time::sleep(Duration::from_secs(600)).await;
                unreachable!();
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

    let result = result_handle.await;
    match result {
        Ok(output) => {
            if output.is_empty() {
                Err(AttemptError::Panic("INDUCED FAIL".to_owned()))
            } else {
                Ok(output)
            }
        }
        Err(e) => {
            let e = e.into_panic();
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
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use abq_generic_test_runner::{
        StaticGetInitContext, StaticManifestCollector, DEFAULT_PROTOCOL_VERSION_TIMEOUT,
    };
    use abq_test_utils::artifacts_dir;
    use abq_utils::auth::{ClientAuthStrategy, ServerAuthStrategy};
    use abq_utils::exit::ExitCode;
    use abq_utils::net_opt::{ClientOptions, ServerOptions};
    use abq_utils::net_protocol::entity::{Entity, WorkerTag};
    use abq_utils::net_protocol::error::FetchTestsError;
    use abq_utils::net_protocol::queue::{AssociatedTestResults, TestSpec};
    use abq_utils::net_protocol::runners::{
        Manifest, ManifestMessage, ProtocolWitness, Test, TestCase, TestOrGroup, TestResult,
    };

    use abq_utils::net_protocol::work_server::InitContext;
    use abq_utils::net_protocol::workers::{
        Eow, ManifestResult, NativeTestRunnerParams, NextWorkBundle, TestLikeRunner, WorkerTest,
        INIT_RUN_NUMBER,
    };
    use abq_utils::results_handler::{NotifyResults, ResultsHandler};
    use abq_utils::server_shutdown::ShutdownManager;
    use abq_utils::tls::{ClientTlsStrategy, ServerTlsStrategy};
    use abq_utils::{atomic, net_protocol};
    use abq_with_protocol_version::with_protocol_version;
    use async_trait::async_trait;
    use futures::FutureExt;
    use parking_lot::Mutex;
    use tracing_test::internal::logs_with_scope_contain;
    use tracing_test::traced_test;

    use super::{
        GetNextTests, InitContextResult, NotifyCancellation, NotifyManifest,
        NotifyMaterialTestsAllRun, TestsFetcher, WorkerContext, WorkerPool,
    };
    use crate::negotiate::QueueNegotiator;
    use crate::runner_strategy::{self, RunnerStrategy};
    use crate::workers::{WorkerPoolConfig, WorkersExitStatus};
    use crate::DEFAULT_RUNNER_TEST_TIMEOUT;
    use abq_utils::net_protocol::workers::{RunId, RunnerKind, WorkId};

    type ResultsCollector = Arc<Mutex<HashMap<WorkId, Vec<TestResult>>>>;
    type ManifestCollector = Arc<Mutex<Option<ManifestResult>>>;

    struct Fetcher {
        reader: Arc<Mutex<VecDeque<WorkerTest>>>,
        end: Arc<AtomicBool>,
    }

    #[async_trait]
    impl TestsFetcher for Fetcher {
        async fn get_next_tests(&mut self) -> Result<NextWorkBundle, FetchTestsError> {
            loop {
                let head = { self.reader.lock().pop_front() };
                match head {
                    Some(work) => return Ok(NextWorkBundle::new(vec![work], Eow(false))),
                    None => {
                        if self.end.load(atomic::ORDERING) {
                            return Ok(NextWorkBundle::new([], Eow(true)));
                        }

                        tokio::time::sleep(Duration::from_micros(10)).await
                    }
                }
            }
        }
    }

    fn work_writer() -> (
        impl Fn(WorkerTest),
        Arc<AtomicBool>,
        impl Fn(Entity) -> GetNextTests,
    ) {
        let writer: Arc<Mutex<VecDeque<WorkerTest>>> = Default::default();
        let set_done = Arc::new(AtomicBool::new(false));
        let reader = Arc::clone(&writer);
        let write_work = move |work| {
            writer.lock().push_back(work);
        };

        let get_next_tests = {
            let set_done = set_done.clone();
            move |_| {
                let reader = reader.clone();
                let get_next_tests: GetNextTests = Box::new(Fetcher {
                    reader,
                    end: set_done.clone(),
                });
                get_next_tests
            }
        };
        (write_work, set_done, get_next_tests)
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
                let old_result = self.results.lock().insert(work_id, results);
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
                all.lock().push(atomic.clone());

                let notifier = move || {
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

    fn empty_init_context() -> InitContextResult {
        Ok(InitContext {
            init_meta: Default::default(),
        })
    }

    type GetNextTestsGenerator<'a> = &'a (dyn Fn(Entity) -> GetNextTests + Send + Sync);
    type NotifyMaterialTestsAllRunGenerator<'a> =
        &'a (dyn Fn() -> NotifyMaterialTestsAllRun + Send + Sync);
    type ResultsHandlerGenerator<'a> = &'a (dyn Fn(Entity) -> ResultsHandler + Send + Sync);

    struct StaticRunnerStrategy<'a> {
        manifest_collector: ManifestCollector,
        get_next_tests_generator: GetNextTestsGenerator<'a>,
        results_handler_generator: ResultsHandlerGenerator<'a>,
        notify_all_tests_run_generator: NotifyMaterialTestsAllRunGenerator<'a>,
    }

    impl<'a> runner_strategy::StrategyGenerator for StaticRunnerStrategy<'a> {
        fn generate(
            &self,
            runner_entity: Entity,
            should_generate_manifest: bool,
        ) -> RunnerStrategy {
            let notify_manifest: Option<NotifyManifest> = if should_generate_manifest {
                Some(Box::new(StaticManifestCollector::new(
                    self.manifest_collector.clone(),
                )))
            } else {
                None
            };

            let get_init_context = StaticGetInitContext::new(empty_init_context());

            RunnerStrategy {
                notify_manifest,
                get_init_context: Box::new(get_init_context),
                get_next_tests: (self.get_next_tests_generator)(runner_entity),
                results_handler: (self.results_handler_generator)(runner_entity),
                notify_all_tests_run: (self.notify_all_tests_run_generator)(),
            }
        }
    }

    fn setup_pool<'a>(
        runner_kind: RunnerKind,
        worker_pool_tag: WorkerTag,
        run_id: RunId,
        runner_strategy_generator: &'a StaticRunnerStrategy<'a>,
        notify_cancellation: Box<dyn NotifyCancellation>,
    ) -> WorkerPoolConfig<'a> {
        WorkerPoolConfig {
            size: NonZeroUsize::new(1).unwrap(),
            some_runner_should_generate_manifest: true,
            tag: worker_pool_tag,
            first_runner_entity: Entity::first_runner(worker_pool_tag),
            results_batch_size_hint: 5,
            runner_kind,
            runner_strategy_generator,
            run_id,
            notify_cancellation,
            worker_context: WorkerContext::AssumeLocal,
            debug_native_runner: false,
            protocol_version_timeout: DEFAULT_PROTOCOL_VERSION_TIMEOUT,
            test_timeout: DEFAULT_RUNNER_TEST_TIMEOUT,
        }
    }

    fn local_work(test: TestCase, work_id: WorkId) -> WorkerTest {
        WorkerTest {
            spec: TestSpec {
                test_case: test,
                work_id,
            },
            run_number: INIT_RUN_NUMBER,
        }
    }

    pub fn echo_test(protocol: ProtocolWitness, echo_msg: String) -> TestOrGroup {
        TestOrGroup::test(Test::new(protocol, echo_msg, [], Default::default()))
    }

    async fn await_manifest_tests(manifest: ManifestCollector) -> Vec<TestSpec> {
        loop {
            let man = { manifest.lock().take() };
            match man {
                Some(ManifestResult::Manifest(manifest)) => return manifest.manifest.flatten().0,
                Some(ManifestResult::TestRunnerError { .. }) => unreachable!(),
                None => {
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    continue;
                }
            }
        }
    }

    async fn await_results<F>(results: ResultsCollector, predicate: F)
    where
        F: Fn(&ResultsCollector) -> bool,
    {
        let duration = Instant::now();

        while !predicate(&results) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if duration.elapsed() >= Duration::from_secs(5) {
                panic!(
                    "Failed to match the predicate within 5 seconds. Current results: {:?}",
                    results
                );
            }
        }
    }

    fn noop_notify_cancellation() -> Box<dyn NotifyCancellation> {
        struct Noop {}
        #[async_trait]
        impl NotifyCancellation for Noop {
            async fn cancel(self: Box<Self>) {}
        }
        Box::new(Noop {})
    }

    async fn test_echo_n(protocol: ProtocolWitness, num_workers: usize, num_echos: usize) {
        let (write_work, set_done, get_next_tests) = work_writer();
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
        let manifest_collector = ManifestCollector::default();

        let runner_strategy = StaticRunnerStrategy {
            manifest_collector: manifest_collector.clone(),
            get_next_tests_generator: &get_next_tests,
            results_handler_generator: &results_handler_generator,
            notify_all_tests_run_generator: &notify_all_tests_run_generator,
        };

        let default_config = setup_pool(
            RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest)),
            WorkerTag::new(0),
            run_id,
            &runner_strategy,
            noop_notify_cancellation(),
        );

        let config = WorkerPoolConfig {
            size: NonZeroUsize::new(num_workers).unwrap(),
            ..default_config
        };

        let mut pool = WorkerPool::new(config).await;

        // Write the work
        let tests = await_manifest_tests(manifest_collector).await;

        for (i, test) in tests.into_iter().enumerate() {
            write_work(local_work(test.test_case, WorkId([i as _; 16])))
        }

        set_done.store(true, atomic::ORDERING);

        // Spin until the timeout, or we got the results we expect.
        await_results(results, |results| {
            let results: HashMap<_, _> = results
                .lock()
                .clone()
                .into_iter()
                .map(|(k, rs)| {
                    let results_outputs = rs.into_iter().map(|r| r.into_spec().output.unwrap());
                    (k, results_outputs.collect())
                })
                .collect();

            results == expected_results
        })
        .await;

        pool.wait().await;
        let exit = pool.shutdown().await;
        assert!(matches!(
            exit.status,
            WorkersExitStatus::Completed(ExitCode::SUCCESS)
        ));

        let all_completed = all_completed.lock();
        assert_eq!(all_completed.len(), num_workers);
        assert!(all_completed.iter().all(|b| b.load(atomic::ORDERING)));
    }

    fn abqtest_write_runner_number_path() -> PathBuf {
        artifacts_dir().join("abqtest_write_runner_number")
    }

    fn abqtest_write_worker_number_path() -> PathBuf {
        artifacts_dir().join("abqtest_write_worker_number")
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

    #[tokio::test]
    #[traced_test]
    async fn bad_message_doesnt_take_down_queue_negotiator_server() {
        let listener =
            ServerOptions::new(ServerAuthStrategy::no_auth(), ServerTlsStrategy::no_tls())
                .bind_async("0.0.0.0:0")
                .await
                .unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let (mut shutdown_tx, shutdown_rx) = ShutdownManager::new_pair();

        let mut negotiator = QueueNegotiator::start(
            listener_addr.ip(),
            listener,
            shutdown_rx,
            "0.0.0.0:0".parse().unwrap(),
            "0.0.0.0:0".parse().unwrap(),
            |_, _| panic!("should not ask for assigned run in this test"),
        )
        .await;

        let client = ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls())
            .build()
            .unwrap();
        let mut conn = client.connect(listener_addr).unwrap();
        net_protocol::write(&mut conn, "bad message").unwrap();

        shutdown_tx.shutdown_immediately().unwrap();
        negotiator.join().await;

        logs_with_scope_contain("", "error handling connection");
    }

    #[tokio::test]
    async fn exit_with_error_if_worker_errors() {
        let (_write_work, _set_done, get_next_tests) = work_writer();
        let (_results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();
        let manifest_collector = ManifestCollector::default();

        let runner_strategy = StaticRunnerStrategy {
            manifest_collector: manifest_collector.clone(),
            get_next_tests_generator: &get_next_tests,
            results_handler_generator: &results_handler_generator,
            notify_all_tests_run_generator: &notify_all_tests_run_generator,
        };

        let default_config = setup_pool(
            RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                // This command should cause the worker to error, since it can't even be executed
                cmd: "__zzz_not_a_command__".into(),
                args: Default::default(),
                extra_env: Default::default(),
            }),
            WorkerTag::new(0),
            RunId::unique(),
            &runner_strategy,
            noop_notify_cancellation(),
        );

        let mut pool = WorkerPool::new(default_config).await;

        pool.wait().await;
        let pool_exit = pool.shutdown().await;

        assert!(matches!(pool_exit.status, WorkersExitStatus::Error { .. }));

        let all_completed = all_completed.lock();
        assert_eq!(all_completed.len(), 1);
        assert!(!all_completed[0].load(atomic::ORDERING));
    }

    #[tokio::test]
    async fn sets_abq_native_runner_env_vars() {
        let (_write_work, _set_done, get_next_tests) = work_writer();
        let (_results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();
        let manifest_collector = ManifestCollector::default();

        let runner_strategy = StaticRunnerStrategy {
            manifest_collector: manifest_collector.clone(),
            get_next_tests_generator: &get_next_tests,
            results_handler_generator: &results_handler_generator,
            notify_all_tests_run_generator: &notify_all_tests_run_generator,
        };

        let writefile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let writefile_path = writefile.to_path_buf();

        let mut config = setup_pool(
            RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                cmd: abqtest_write_runner_number_path().display().to_string(),
                args: vec![writefile_path.display().to_string()],
                extra_env: Default::default(),
            }),
            WorkerTag::new(0),
            RunId::unique(),
            &runner_strategy,
            noop_notify_cancellation(),
        );

        config.size = NonZeroUsize::new(5).unwrap();
        config.protocol_version_timeout = Duration::from_millis(100);

        let mut pool = WorkerPool::new(config).await;
        pool.wait().await;
        let _pool_exit = pool.shutdown();

        let worker_ids = std::fs::read_to_string(writefile).unwrap();
        let mut worker_ids: Vec<_> = worker_ids.trim().split('\n').collect();
        worker_ids.sort();
        assert_eq!(worker_ids, &["1", "2", "3", "4", "5"]);

        let all_completed = all_completed.lock();
        assert_eq!(all_completed.len(), 5);
    }

    #[tokio::test]
    async fn sets_abq_native_worker_env_vars() {
        let (_write_work, _set_done, get_next_tests) = work_writer();
        let (_results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();
        let manifest_collector = ManifestCollector::default();

        let runner_strategy = StaticRunnerStrategy {
            manifest_collector: manifest_collector.clone(),
            get_next_tests_generator: &get_next_tests,
            results_handler_generator: &results_handler_generator,
            notify_all_tests_run_generator: &notify_all_tests_run_generator,
        };

        let writefile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let writefile_path = writefile.to_path_buf();

        let mut config = setup_pool(
            RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
                cmd: abqtest_write_worker_number_path().display().to_string(),
                args: vec![writefile_path.display().to_string()],
                extra_env: Default::default(),
            }),
            WorkerTag::new(152),
            RunId::unique(),
            &runner_strategy,
            noop_notify_cancellation(),
        );

        config.size = NonZeroUsize::new(3).unwrap();
        config.protocol_version_timeout = Duration::from_millis(100);

        let mut pool = WorkerPool::new(config).await;
        pool.wait().await;
        let _pool_exit = pool.shutdown();

        let worker_ids = std::fs::read_to_string(writefile).unwrap();
        assert_eq!(worker_ids, "152\n152\n152\n");

        let all_completed = all_completed.lock();
        assert_eq!(all_completed.len(), 3);
    }

    #[tokio::test]
    #[with_protocol_version]
    async fn worker_exits_with_runner_exit() {
        let (_write_work, _set_done, get_next_tests) = work_writer();
        let (_results, results_handler_generator) = results_collector();
        let (all_completed, notify_all_tests_run_generator) = notify_all_tests_run();
        let manifest_collector = ManifestCollector::default();

        let runner_strategy = StaticRunnerStrategy {
            manifest_collector: manifest_collector.clone(),
            get_next_tests_generator: &get_next_tests,
            results_handler_generator: &results_handler_generator,
            notify_all_tests_run_generator: &notify_all_tests_run_generator,
        };

        let manifest = ManifestMessage::new(Manifest::new(
            [echo_test(proto, "test1".to_string())],
            Default::default(),
        ));
        let mut config = setup_pool(
            RunnerKind::TestLikeRunner(TestLikeRunner::ExitWith(27), Box::new(manifest)),
            WorkerTag::new(0),
            RunId::unique(),
            &runner_strategy,
            noop_notify_cancellation(),
        );

        config.size = NonZeroUsize::new(1).unwrap();
        config.protocol_version_timeout = Duration::from_millis(100);

        let mut pool = WorkerPool::new(config).await;
        pool.wait().await;
        let pool_exit = pool.shutdown().await;

        assert!(
            matches!(
                pool_exit.status,
                WorkersExitStatus::Completed(ec) if ec.get() == 27
            ),
            "{pool_exit:?}"
        );

        let all_completed = all_completed.lock();
        assert_eq!(all_completed.len(), 1);
    }
}
