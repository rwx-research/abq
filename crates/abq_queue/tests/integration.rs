use std::{
    collections::HashMap,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    panic,
    pin::Pin,
    sync::Arc,
    thread,
    time::Duration,
};

use abq_queue::{
    invoke::{
        self, run_cancellation_pair, Client, CompletedRunData, RunCancellationTx, TestResultError,
        DEFAULT_CLIENT_POLL_TIMEOUT, DEFAULT_TICK_INTERVAL,
    },
    queue::{Abq, QueueConfig},
    timeout::{RunTimeoutStrategy, TimeoutReason},
};
use abq_test_utils::{artifacts_dir, assert_scoped_log};
use abq_utils::{
    auth::{ClientAuthStrategy, User},
    exit::ExitCode,
    net::{ClientStream, ConfiguredClient},
    net_opt::ClientOptions,
    net_protocol::{
        self,
        entity::{Entity, RunnerMeta, WorkerTag},
        queue::{NativeRunnerInfo, TestSpec},
        runners::{
            AbqProtocolVersion, InitSuccessMessage, Location, Manifest, ManifestMessage,
            MetadataMap, OutOfBandError, ProtocolWitness, RawTestResultMessage, Status, Test,
            TestOrGroup, TestResult, TestResultSpec,
        },
        work_server::{InitContext, InitContextResponse},
        workers::{
            NativeTestRunnerParams, RunId, RunnerKind, TestLikeRunner, WorkId, INIT_RUN_NUMBER,
        },
    },
    tls::ClientTlsStrategy,
};

use abq_with_protocol_version::with_protocol_version;
use abq_workers::{
    negotiate::{NegotiatedWorkers, WorkersConfig, WorkersNegotiator},
    workers::{WorkerContext, WorkersExit, WorkersExitStatus},
};
use futures::FutureExt;
use ntest::timeout;
use parking_lot::Mutex;
use serial_test::serial;
use tracing_test::traced_test;
use Action::*;
use Assert::*;
use Servers::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Run(usize);

/// Worker ID
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Wid(usize);

/// Supervisor ID
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Sid(usize);

/// External party ID
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ExternId(usize);

/// ID of a spawned action
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SpawnId(usize);

enum Servers {
    Unified(QueueConfig),
}

impl Default for Servers {
    fn default() -> Self {
        Self::from_config(Default::default())
    }
}

impl Servers {
    fn from_config(config: QueueConfig) -> Self {
        Self::Unified(config)
    }
}

fn one_nonzero() -> NonZeroU64 {
    1.try_into().unwrap()
}
fn one_nonzero_usize() -> NonZeroUsize {
    1.try_into().unwrap()
}

fn echo_test(proto: ProtocolWitness, echo_msg: String) -> TestOrGroup {
    TestOrGroup::test(Test::new(proto, echo_msg, [], Default::default()))
}

fn empty_manifest_msg() -> Box<ManifestMessage> {
    Box::new(ManifestMessage::new(Manifest::new([], Default::default())))
}

fn workers_config(tag: impl Into<WorkerTag>, runner_kind: RunnerKind) -> WorkersConfig {
    WorkersConfig {
        tag: tag.into(),
        num_workers: 2.try_into().unwrap(),
        runner_kind,
        worker_context: WorkerContext::AssumeLocal,
        supervisor_in_band: false,
        debug_native_runner: false,
        results_batch_size_hint: 1,
    }
}

fn sort_results(results: &mut [(WorkId, u32, TestResult)]) -> Vec<(u32, &str)> {
    let mut results = results
        .iter()
        .map(|(_, n, result)| (*n, result.output.as_ref().unwrap().as_str()))
        .collect::<Vec<_>>();
    results.sort_unstable();
    results
}

fn sort_results_owned(results: &mut [(WorkId, u32, TestResult)]) -> Vec<(u32, String)> {
    sort_results(results)
        .into_iter()
        .map(|(attempt, r)| (attempt, r.to_string()))
        .collect()
}

// TODO: put this on [Client] directly
struct SupervisorConfig {
    batch_size: NonZeroU64,
    timeout: Duration,
    tick_interval: Duration,
    retries: u32,
}

impl SupervisorConfig {
    fn new() -> Self {
        Self {
            batch_size: one_nonzero(),
            timeout: DEFAULT_CLIENT_POLL_TIMEOUT,
            tick_interval: DEFAULT_TICK_INTERVAL,
            retries: 0,
        }
    }

    fn with_batch_size(self, batch_size: u64) -> Self {
        Self {
            batch_size: batch_size.try_into().unwrap(),
            ..self
        }
    }

    fn with_timeout(self, timeout: Duration) -> Self {
        Self { timeout, ..self }
    }

    fn with_tick_interval(self, tick_interval: Duration) -> Self {
        Self {
            tick_interval,
            ..self
        }
    }

    fn with_retries(self, retries: u32) -> Self {
        Self { retries, ..self }
    }
}

fn native_runner_simulation_bin() -> String {
    artifacts_dir()
        .join("abqtest_native_runner_simulation")
        .display()
        .to_string()
}

type GetConn<'a> = &'a dyn Fn() -> Box<dyn ClientStream>;

enum Action {
    RunTest(Run, Sid, SupervisorConfig),
    StartTest(Run, Sid, SupervisorConfig),
    CancelTest(Sid),

    // TODO: consolidate start/stop workers by making worker pools async by default
    StartWorkers(Run, Wid, WorkersConfig),
    StopWorkers(Wid),

    /// Make a connection to the work server, the callback will test a request.
    WSRunRequest(Run, Box<dyn Fn(GetConn, RunId) + Send + Sync>),

    /// Spawn an action in a new task. Do this if the action is blocking.
    Spawn(SpawnId, Box<Action>),
    /// Wait for a spawned action to complete.
    Join(SpawnId),
}

#[allow(clippy::type_complexity)]
enum Assert<'a> {
    CheckSupervisor(Sid, &'a dyn Fn(&Result<Client, invoke::Error>) -> bool),

    TestExit(Sid, &'a dyn Fn(&Result<CompletedRunData, invoke::Error>)),
    TestExitWithoutErr(Sid),
    TestResults(Sid, &'a dyn Fn(&[(WorkId, u32, TestResult)]) -> bool),

    WaitForWorkerDead(Wid),
    WorkersAreRedundant(Wid),
    WorkerExitStatus(Wid, &'a dyn Fn(&WorkersExitStatus)),
    WorkerResultStatus(Wid, &'a dyn Fn(&WorkersResult)),
}

type Steps<'a> = Vec<(Vec<Action>, Vec<Assert<'a>>)>;

#[derive(Default)]
struct TestBuilder<'a> {
    servers: Servers,
    steps: Steps<'a>,
}

impl<'a> TestBuilder<'a> {
    fn new(servers: Servers) -> Self {
        Self {
            servers,
            steps: Default::default(),
        }
    }

    fn step(
        mut self,
        actions: impl IntoIterator<Item = Action>,
        asserts: impl IntoIterator<Item = Assert<'a>>,
    ) -> Self {
        self.steps
            .push((actions.into_iter().collect(), asserts.into_iter().collect()));
        self
    }

    fn act(mut self, actions: impl IntoIterator<Item = Action>) -> Self {
        self.steps.push((actions.into_iter().collect(), vec![]));
        self
    }

    fn test(self) {
        run_test(self.servers, self.steps)
    }
}

type Supervisors = Arc<Mutex<HashMap<Sid, Result<Client, invoke::Error>>>>;
type SupervisorData = Arc<tokio::sync::Mutex<HashMap<Sid, (RunCancellationTx, ResultsCollector)>>>;
type SupervisorResults =
    Arc<tokio::sync::Mutex<HashMap<Sid, Result<CompletedRunData, invoke::Error>>>>;

type Workers = Arc<Mutex<HashMap<Wid, NegotiatedWorkers>>>;
type WorkersRedundant = Arc<Mutex<HashMap<Wid, bool>>>;
enum WorkersResult {
    Exit(WorkersExit),
    Panic,
}
type WorkersResults = Arc<Mutex<HashMap<Wid, WorkersResult>>>;

type BgTasks = HashMap<SpawnId, tokio::task::JoinHandle<()>>;

#[derive(Default, Clone)]
struct ResultsCollector {
    results: Arc<Mutex<Vec<(WorkId, u32, TestResult)>>>,
    manifest: Arc<Mutex<Option<Vec<TestSpec>>>>,
}

impl invoke::ResultHandler for ResultsCollector {
    type InitData = ResultsCollector;

    fn create(manifest: Vec<TestSpec>, data: Self) -> Self {
        *data.manifest.lock() = Some(manifest);
        data
    }

    fn on_result(
        &mut self,
        work_id: WorkId,
        run_number: u32,
        result: net_protocol::client::ReportedResult,
    ) {
        self.results
            .lock()
            .push((work_id, run_number, result.test_result))
    }

    fn get_ordered_retry_manifest(&mut self, run_number: u32) -> Vec<TestSpec> {
        let manifest = self.manifest.lock();
        let manifest = manifest.as_ref().unwrap();
        let results = self.results.lock();

        results
            .iter()
            .filter_map(|(work_id, run, result)| {
                if (*run == run_number) && result.status.is_fail_like() {
                    let spec = manifest.iter().find(|spec| spec.work_id == *work_id);
                    Some(spec.unwrap().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_exit_code(&self) -> ExitCode {
        let results = self.results.lock();
        let max_run = results
            .iter()
            .map(|(_, run, _)| *run)
            .max()
            .unwrap_or(INIT_RUN_NUMBER);
        let any_latest_failure = results
            .iter()
            .any(|(_, run, result)| *run == max_run && result.status.is_fail_like());
        if any_latest_failure {
            ExitCode::FAILURE
        } else {
            ExitCode::SUCCESS
        }
    }

    fn tick(&mut self) {}
}

fn action_to_fut(
    queue: &Abq,
    run_ids: &mut HashMap<Run, RunId>,
    client: Box<dyn ConfiguredClient>,
    client_opts: ClientOptions<User>,

    supervisors: Supervisors,
    supervisor_data: SupervisorData,
    supervisor_results: SupervisorResults,

    workers: Workers,
    workers_redundant: WorkersRedundant,
    worker_results: WorkersResults,

    background_tasks: &mut BgTasks,

    action: Action,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    use Action::*;

    macro_rules! get_run_id {
        ($n:expr) => {
            run_ids
                .entry($n)
                .or_insert_with(|| RunId($n.0.to_string()))
                .clone()
        };
    }

    let is_run_to_completion = matches!(action, RunTest(..));

    match action {
        StartTest(n, super_id, config) | RunTest(n, super_id, config) => {
            let run_to_completion = is_run_to_completion;

            let run_id = get_run_id!(n);

            let (cancellation_tx, cancellation_rx) = run_cancellation_pair();

            let queue_server_addr = queue.server_addr();
            let SupervisorConfig {
                batch_size,
                timeout,
                tick_interval,
                retries,
            } = config;
            async move {
                let client = Client::invoke_work(
                    Entity::supervisor(),
                    queue_server_addr,
                    client_opts,
                    run_id,
                    batch_size,
                    timeout,
                    retries,
                    tick_interval,
                    cancellation_rx,
                )
                .await;

                let results_collector = ResultsCollector::default();

                supervisor_data
                    .lock()
                    .await
                    .insert(super_id, (cancellation_tx, results_collector.clone()));

                if !run_to_completion {
                    supervisors.lock().insert(super_id, client);
                    return;
                }

                let client = client.unwrap();

                let result = client
                    .stream_results::<ResultsCollector>(results_collector)
                    .await
                    .map(|(_handler, summary)| summary);

                supervisor_results.lock().await.insert(super_id, result);
            }
            .boxed()
        }

        CancelTest(super_id) => async move {
            loop {
                let data = supervisor_data.lock().await;
                if let Some((cancellation_tx, _)) = data.get(&super_id) {
                    cancellation_tx.send().await.unwrap();
                    return;
                }
            }
        }
        .boxed(),

        StartWorkers(n, worker_id, workers_config) => {
            let run_id = get_run_id!(n);
            let negotiator = queue.get_negotiator_handle();

            async move {
                let worker_pool = WorkersNegotiator::negotiate_and_start_pool_on_executor(
                    workers_config,
                    negotiator,
                    client_opts.clone(),
                    run_id,
                )
                .await
                .unwrap();

                workers_redundant.lock().insert(
                    worker_id,
                    matches!(worker_pool, NegotiatedWorkers::Redundant { .. }),
                );

                workers.lock().insert(worker_id, worker_pool);
            }
            .boxed()
        }

        StopWorkers(n) => async move {
            let mut worker_pool = workers.lock().remove(&n).unwrap();
            let workers_result =
                match panic::catch_unwind(panic::AssertUnwindSafe(|| worker_pool.shutdown())) {
                    Ok(exit) => WorkersResult::Exit(exit),
                    Err(_) => WorkersResult::Panic,
                };
            worker_results.lock().insert(n, workers_result);
        }
        .boxed(),

        WSRunRequest(n, callback) => {
            let run_id = get_run_id!(n);
            let work_scheduler_addr = queue.work_server_addr();
            async move {
                let get_conn = &|| client.connect(work_scheduler_addr).unwrap();
                callback(get_conn, run_id);
            }
            .boxed()
        }

        Spawn(id, action) => {
            let fut = action_to_fut(
                queue,
                run_ids,
                client,
                client_opts,
                supervisors,
                supervisor_data,
                supervisor_results,
                workers,
                workers_redundant,
                worker_results,
                background_tasks,
                *action,
            );

            let handle = tokio::spawn(fut);

            background_tasks.insert(id, handle);

            async {}.boxed()
        }

        Join(id) => {
            let handle = background_tasks.remove(&id).unwrap();
            async move {
                handle.await.unwrap();
            }
            .boxed()
        }
    }
}

fn run_test(servers: Servers, steps: Steps) {
    let mut queue = match servers {
        Unified(config) => Abq::start(config),
    };

    let client_opts =
        ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());
    let client = client_opts.clone().build().unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut run_ids: HashMap<Run, RunId> = Default::default();

        let supervisors = Supervisors::default();
        let supervisor_data = SupervisorData::default();
        let supervisor_results = SupervisorResults::default();

        let workers = Workers::default();
        let workers_redundant = WorkersRedundant::default();
        let worker_results = WorkersResults::default();

        let mut background_tasks = BgTasks::default();

        for (action_set, asserts) in steps {
            let mut futs = vec![];
            for action in action_set {
                let fut = action_to_fut(
                    &queue,
                    &mut run_ids,
                    client.boxed_clone(),
                    client_opts.clone(),
                    supervisors.clone(),
                    supervisor_data.clone(),
                    supervisor_results.clone(),
                    workers.clone(),
                    workers_redundant.clone(),
                    worker_results.clone(),
                    &mut background_tasks,
                    action,
                );

                futs.push(fut);
            }

            // Wait for all steps to complete, then we start the round asserts.
            futures::future::join_all(futs).await;

            for check in asserts {
                match check {
                    TestExit(n, check) => loop {
                        let results = supervisor_results.lock().await;
                        if let Some(exit) = results.get(&n) {
                            check(exit);
                            break;
                        }
                    },

                    TestExitWithoutErr(n) => {
                        let results = supervisor_results.lock().await;
                        let exit = results.get(&n).expect("supervisor exit not found");
                        assert!(exit.is_ok(), "{:?}", exit);
                        let CompletedRunData { native_runner_info } = exit.as_ref().unwrap();
                        let NativeRunnerInfo {
                            protocol_version,
                            specification: _,
                        } = native_runner_info;
                        assert_eq!(protocol_version, &AbqProtocolVersion::V0_2);
                    }

                    TestResults(n, check) => {
                        let supervisor_data = supervisor_data.lock().await;
                        let (_, collector) = supervisor_data.get(&n).expect("supervisor not found");
                        let results = collector.results.lock();
                        assert!(check(&results));
                    }

                    WorkersAreRedundant(n) => {
                        assert!(workers_redundant.lock().get(&n).unwrap());
                    }

                    WorkerExitStatus(n, workers_exit) => {
                        let results = worker_results.lock();
                        let real_result = results.get(&n).expect("workers result not found");
                        match real_result {
                            WorkersResult::Exit(exit) => workers_exit(&exit.status),
                            WorkersResult::Panic => panic!("expected exit result, not panic"),
                        }
                    }

                    WorkerResultStatus(n, workers_result) => {
                        let results = worker_results.lock();
                        let real_result = results.get(&n).expect("workers result not found");
                        workers_result(real_result)
                    }

                    WaitForWorkerDead(n) => {
                        let mut attempts = 0;
                        loop {
                            attempts += 1;

                            let workers = workers.lock();
                            let worker = workers.get(&n).expect("worker not found");
                            if worker.workers_alive() {
                                if attempts > 100 {
                                    panic!("waited too long for worker to be dead");
                                }
                                thread::sleep(Duration::from_millis(10))
                            } else {
                                break;
                            }
                        }
                    }

                    CheckSupervisor(n, check) => {
                        let supervisors = supervisors.lock();
                        let supervisor = supervisors.get(&n).expect("supervisor not found");
                        check(supervisor);
                    }
                }
            }
        }
    });

    queue.shutdown().unwrap();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
#[traced_test]
fn multiple_jobs_complete() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == [(1, "echo1"), (1, "echo2")]
                }),
            ],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        .test();

    // Should log how long this worker took
    assert_scoped_log(
        "abq_queue::worker_timings",
        "worker post completion idle seconds",
    );
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn multiple_invokers() {
    let runner1 = {
        let manifest = ManifestMessage::new(Manifest::new(
            [
                echo_test(proto, "echo1".to_string()),
                echo_test(proto, "echo2".to_string()),
            ],
            Default::default(),
        ));
        RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest))
    };

    let runner2 = {
        let manifest = ManifestMessage::new(Manifest::new(
            [
                echo_test(proto, "echo3".to_string()),
                echo_test(proto, "echo4".to_string()),
                echo_test(proto, "echo5".to_string()),
            ],
            Default::default(),
        ));
        RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest))
    };

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner1)),
                //
                RunTest(Run(2), Sid(2), SupervisorConfig::new()),
                StartWorkers(Run(2), Wid(2), workers_config(2, runner2)),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == [(1, "echo1"), (1, "echo2")]
                }),
                //
                TestExitWithoutErr(Sid(2)),
                TestResults(Sid(2), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == [(1, "echo3"), (1, "echo4"), (1, "echo5")]
                }),
            ],
        )
        .step(
            [StopWorkers(Wid(1)), StopWorkers(Wid(2))],
            [
                WorkerExitStatus(Wid(1), &|e| (assert_eq!(e, &WorkersExitStatus::Success))),
                WorkerExitStatus(Wid(2), &|e| (assert_eq!(e, &WorkersExitStatus::Success))),
            ],
        )
        .test();
}

// TODO write some tests that smoke over # of workers and batch sizes
#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn batch_two_requests_at_a_time() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
            echo_test(proto, "echo3".to_string()),
            echo_test(proto, "echo4".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new().with_batch_size(2)),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == [(1, "echo1"), (1, "echo2"), (1, "echo3"), (1, "echo4")]
                }),
            ],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn worker_exits_with_failure_if_test_fails() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo".to_string())],
        Default::default(),
    ));

    // Set up the runner so that it times out, always issuing an error.
    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::FailOnTestName("echo".to_string()),
        Box::new(manifest),
    );

    let workers_config = WorkersConfig {
        ..workers_config(1, runner)
    };

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config),
            ],
            [],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert!(matches!(
                    e,
                    WorkersExitStatus::Failure {
                        exit_code: ExitCode::FAILURE
                    }
                ))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn multiple_worker_sets_all_exit_with_failure_if_any_test_fails() {
    // NOTE: we set up two workers and unleash them on the following manifest, setting both
    // workers to fail only on the `echo_fail` case. That way, no matter which one picks it up,
    // we should observe a failure for both workers sets.

    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
            echo_test(proto, "echo_fail".to_string()),
            echo_test(proto, "echo3".to_string()),
            echo_test(proto, "echo4".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::FailOnTestName("echo_fail".to_string()),
        Box::new(manifest),
    );

    let workers_config = WorkersConfig {
        ..workers_config(1, runner)
    };

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config.clone()),
                StartWorkers(Run(1), Wid(2), workers_config),
            ],
            [],
        )
        // NOTE: it is very possible that workers1 completes all the tests before workers2 gets to
        // start, because we are not controlling the order tests are delivered across the network
        // here. However, the result should be the same no matter what, so this test should never
        // flake.
        //
        // In the future, we'll probably want to test different networking order conditions. See
        // also https://github.com/rwx-research/abq/issues/29.
        .step(
            [StopWorkers(Wid(1)), StopWorkers(Wid(2))],
            [
                WorkerExitStatus(Wid(1), &|e| {
                    assert!(matches!(
                        e,
                        WorkersExitStatus::Failure {
                            exit_code: ExitCode::FAILURE
                        }
                    ))
                }),
                WorkerExitStatus(Wid(2), &|e| {
                    assert!(matches!(
                        e,
                        WorkersExitStatus::Failure {
                            exit_code: ExitCode::FAILURE
                        }
                    ))
                }),
            ],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn invoke_work_with_duplicate_id_is_an_error() {
    TestBuilder::default()
        // Start one client with the run ID
        .step(
            [StartTest(Run(1), Sid(1), SupervisorConfig::new())],
            [CheckSupervisor(Sid(1), &|s| s.is_ok())],
        )
        // Start a second, with the same run ID
        .step(
            [StartTest(Run(1), Sid(2), SupervisorConfig::new())],
            [CheckSupervisor(Sid(2), &|s| {
                matches!(s, Err(invoke::Error::DuplicateRun(..)))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn invoke_work_with_duplicate_id_after_completion_is_an_error() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        // Start one client, and have it drain its test queue
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [TestExit(Sid(1), &|result| assert!(result.is_ok()))],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        // Start a second, with the same run ID
        .step(
            [StartTest(Run(1), Sid(1), SupervisorConfig::new())],
            [CheckSupervisor(Sid(1), &|supervisor| {
                matches!(supervisor, Err(invoke::Error::DuplicateCompletedRun(..)))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
fn empty_manifest_exits_gracefully() {
    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest_msg());

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| results.is_empty()),
            ],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        .test();
}

#[test]
#[traced_test]
#[with_protocol_version]
fn get_init_context_from_work_server_waiting_for_first_worker() {
    TestBuilder::default()
        // Set up the queue so that a run ID is invoked, but no worker has connected yet.
        .act([StartTest(Run(1), Sid(1), SupervisorConfig::new())])
        .act([WSRunRequest(
            Run(1),
            Box::new(|get_conn, run_id| {
                use net_protocol::work_server::{Message, Request};

                let mut conn = get_conn();

                // Ask the server for the next test; we should be told a manifest is still TBD.
                net_protocol::write(
                    &mut conn,
                    Request {
                        entity: Entity::local_client(),
                        message: Message::InitContext { run_id },
                    },
                )
                .unwrap();

                let response: InitContextResponse = net_protocol::read(&mut conn).unwrap();

                assert!(matches!(response, InitContextResponse::WaitingForManifest));
            }),
        )])
        .test();
}

#[test]
#[with_protocol_version]
fn get_init_context_from_work_server_waiting_for_manifest() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::NeverReturnManifest, manifest.into());

    TestBuilder::default()
        .act([
            StartTest(Run(1), Sid(1), SupervisorConfig::new()),
            StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
        ])
        .act([WSRunRequest(
            Run(1),
            Box::new(|get_conn, run_id| {
                use net_protocol::work_server::{Message, Request};

                let mut conn = get_conn();

                // Ask the server for the next test; we should be told a manifest is still TBD.
                net_protocol::write(
                    &mut conn,
                    Request {
                        entity: Entity::local_client(),
                        message: Message::InitContext { run_id },
                    },
                )
                .unwrap();

                let response: InitContextResponse = net_protocol::read(&mut conn).unwrap();

                assert!(matches!(response, InitContextResponse::WaitingForManifest));
            }),
        )])
        .act([StopWorkers(Wid(1))])
        .test();
}

#[test]
#[with_protocol_version]
fn get_init_context_from_work_server_active() {
    let expected_init_meta = {
        let mut meta = MetadataMap::default();
        meta.insert("hello".to_string(), "world".into());
        meta.insert("ab".to_string(), "queue".into());
        meta
    };

    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        expected_init_meta.clone(),
    ));

    // Set up the runner to return the manifest, but not run any test.
    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::NeverReturnOnTest("echo1".to_owned()),
        Box::new(manifest),
    );

    let workers_config = WorkersConfig {
        num_workers: one_nonzero_usize(),
        ..workers_config(1, runner)
    };

    TestBuilder::default()
        .act([
            StartTest(Run(1), Sid(1), SupervisorConfig::new()),
            StartWorkers(Run(1), Wid(1), workers_config),
        ])
        .act([WSRunRequest(
            Run(1),
            Box::new(move |get_conn, run_id| {
                use net_protocol::work_server::{Message, Request};

                // Ask the server for the work context, it should align with the init_meta we gave
                // to begin with.
                loop {
                    let mut conn = get_conn();

                    net_protocol::write(
                        &mut conn,
                        Request {
                            entity: Entity::local_client(),
                            message: Message::InitContext {
                                run_id: run_id.clone(),
                            },
                        },
                    )
                    .unwrap();

                    match net_protocol::read(&mut conn).unwrap() {
                        InitContextResponse::WaitingForManifest => continue,
                        InitContextResponse::InitContext(InitContext { init_meta }) => {
                            assert_eq!(init_meta, expected_init_meta);
                            return;
                        }
                        InitContextResponse::RunAlreadyCompleted => unreachable!(),
                    }
                }
            }),
        )])
        .act([StopWorkers(Wid(1))])
        .test();
}

#[test]
#[with_protocol_version]
fn get_init_context_after_run_already_completed() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [TestExitWithoutErr(Sid(1))],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        .act([WSRunRequest(
            Run(1),
            Box::new(|get_conn, run_id| {
                use net_protocol::work_server::{Message, Request};

                let mut conn = get_conn();

                // Ask the server for the work context, we should be told the run is already done.
                net_protocol::write(
                    &mut conn,
                    Request {
                        entity: Entity::local_client(),
                        message: Message::InitContext { run_id },
                    },
                )
                .unwrap();

                let response: InitContextResponse = net_protocol::read(&mut conn).unwrap();

                assert!(matches!(response, InitContextResponse::RunAlreadyCompleted));
            }),
        )])
        .test();
}

#[test]
#[with_protocol_version]
fn getting_run_after_work_is_complete_returns_nothing() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        // First off, run the test suite to completion. It should complete successfully.
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner.clone())),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == [(1, "echo1"), (1, "echo2")]
                }),
            ],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        // Now, simulate a new set of workers connecting again. Negotiation should succeed, but
        // they should be marked as redundant.
        .step(
            [StartWorkers(Run(1), Wid(2), workers_config(2, runner))],
            [WorkersAreRedundant(Wid(2))],
        )
        .step(
            [StopWorkers(Wid(2))],
            [WorkerExitStatus(Wid(2), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
fn test_cancellation_drops_remaining_work() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));
    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::NeverReturnOnTest("echo1".to_owned()),
        Box::new(manifest),
    );

    TestBuilder::default()
        .act([Spawn(
            SpawnId(1),
            Box::new(RunTest(Run(1), Sid(1), SupervisorConfig::new())),
        )])
        .step(
            [CancelTest(Sid(1))],
            [TestExit(Sid(1), &|result| {
                assert!(matches!(
                    result,
                    Err(invoke::Error::TestResultError(TestResultError::Cancelled))
                ))
            })],
        )
        .act([StartWorkers(Run(1), Wid(1), workers_config(1, runner))])
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(
                    e,
                    &WorkersExitStatus::Failure {
                        exit_code: ExitCode::CANCELLED
                    }
                )
            })],
        )
        .test();
}

#[test]
fn failure_to_run_worker_command_exits_gracefully() {
    let runner = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
        cmd: "__zzz_not_a_command__".to_string(),
        args: Default::default(),
        extra_env: Default::default(),
    });

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [TestExit(Sid(1), &|results_err| {
                assert!(matches!(
                    results_err.as_ref().unwrap_err(),
                    invoke::Error::TestResultError(TestResultError::TestCommandError(..))
                ))
            })],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert!(matches!(e, WorkersExitStatus::Error { .. }))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
fn cancel_test_run_upon_timeout_after_last_test_handed_out() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));
    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::NeverReturnOnTest("echo1".to_owned()),
        Box::new(manifest),
    );

    let workers_config = WorkersConfig {
        num_workers: one_nonzero_usize(),
        ..workers_config(1, runner)
    };

    fn zero(_: TimeoutReason) -> Duration {
        Duration::ZERO
    }

    TestBuilder::new(Servers::from_config(QueueConfig {
        timeout_strategy: RunTimeoutStrategy::constant(zero),
        ..Default::default()
    }))
    .step(
        [
            RunTest(Run(1), Sid(1), SupervisorConfig::new()),
            // After pulling the only test, the queue should time us out.
            StartWorkers(Run(1), Wid(1), workers_config),
        ],
        [TestExit(Sid(1), &|result| {
            assert!(matches!(
                result,
                Err(invoke::Error::TestResultError(TestResultError::TimedOut(
                    ..
                )))
            ));
        })],
    )
    .act([StopWorkers(Wid(1))])
    .test();
}

#[test]
fn pending_worker_attachment_does_not_block_other_attachers() {
    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest_msg());

    TestBuilder::default()
        // Start a set of workers that will never find their execution context, and just
        // continuously poll the queue.
        .act([Spawn(
            SpawnId(1),
            Box::new(StartWorkers(
                Run(1),
                Wid(1),
                workers_config(1, runner.clone()),
            )),
        )])
        // In the meantime, start and execute a run that should complete successfully.
        .step(
            {
                [
                    RunTest(Run(2), Sid(2), SupervisorConfig::new()),
                    StartWorkers(Run(2), Wid(2), workers_config(2, runner)),
                ]
            },
            [TestExitWithoutErr(Sid(2))],
        )
        .step(
            [StopWorkers(Wid(2))],
            [WorkerExitStatus(Wid(2), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        .test();
}

#[test]
fn native_runner_returns_manifest_failure() {
    let manifest = ManifestMessage::new_failure(OutOfBandError {
        message: "1 != 2".to_owned(),
        backtrace: Some(vec!["cmp.x".to_string(), "add.x".to_string()]),
        exception: Some("CompareException".to_string()),
        location: Some(Location {
            file: "cmp.x".to_string(),
            line: Some(10),
            column: Some(15),
        }),
        meta: None,
    });

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [TestExit(Sid(1), &|result| {
                let err = result.as_ref().unwrap_err();
                assert!(matches!(err, invoke::Error::TestResultError(TestResultError::TestCommandError(..))));
                let msg = err.to_string();
                insta::assert_snapshot!(msg, @r###"
                The given test command failed to be executed by all workers. The recorded error message is:
                1 != 2

                CompareException at cmp.x[10:15]
                cmp.x
                add.x
                "###);
            })],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| assert!(matches!(e, WorkersExitStatus::Error {..})))],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn multiple_tests_per_work_id_reported() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1,echo2,echo3".to_string()),
            echo_test(proto, "echo4,echo5,echo6".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::EchoMany { separator: ',' },
        Box::new(manifest),
    );

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results
                        == [
                            (1, "echo1"),
                            (1, "echo2"),
                            (1, "echo3"),
                            (1, "echo4"),
                            (1, "echo5"),
                            (1, "echo6"),
                        ]
                }),
            ],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert!(matches!(e, WorkersExitStatus::Success))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1500)] // 1.5 second
fn runner_panic_stops_worker() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Panic, Box::new(manifest));

    TestBuilder::default()
        .step(
            [
                StartTest(Run(1), Sid(1), SupervisorConfig::new()),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [WaitForWorkerDead(Wid(1))],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerResultStatus(Wid(1), &|e| {
                assert!(matches!(e, WorkersResult::Panic))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
#[traced_test]
fn multiple_workers_start_before_supervisor() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        .act([
            Spawn(
                SpawnId(1),
                Box::new(StartWorkers(
                    Run(1),
                    Wid(1),
                    workers_config(1, runner.clone()),
                )),
            ),
            Spawn(
                SpawnId(2),
                Box::new(StartWorkers(Run(1), Wid(2), workers_config(2, runner))),
            ),
        ])
        .step(
            [RunTest(Run(1), Sid(1), SupervisorConfig::new())],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == [(1, "echo1"), (1, "echo2")]
                }),
            ],
        )
        .act([Join(SpawnId(1)), Join(SpawnId(2))])
        .step(
            [StopWorkers(Wid(1)), StopWorkers(Wid(2))],
            [
                WorkerExitStatus(Wid(1), &|e| assert_eq!(e, &WorkersExitStatus::Success)),
                WorkerExitStatus(Wid(2), &|e| assert_eq!(e, &WorkersExitStatus::Success)),
            ],
        )
        .test();

    // Should log how long these workers were idle before supervisor
    // Should log how long these workers were idle before manifest
    // Should log how long these worker took
    assert_scoped_log(
        "abq_queue::worker_timings",
        "worker pre-supervisor idle seconds",
    );
    assert_scoped_log(
        "abq_queue::worker_timings",
        "worker pre-manifest idle seconds",
    );
    assert_scoped_log(
        "abq_queue::worker_timings",
        "worker post completion idle seconds",
    );
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn timeout_with_slower_poll_timeout_than_tick_interval() {
    // We should see the supervisor time out, even though the tick interval yields far more often
    // than the timeout interval.
    let config = SupervisorConfig::new()
        .with_tick_interval(Duration::from_micros(10))
        .with_timeout(Duration::from_micros(1000));

    TestBuilder::default()
        .step(
            [RunTest(Run(1), Sid(1), config)],
            [TestExit(Sid(1), &|result| {
                assert!(matches!(
                    result,
                    Err(invoke::Error::TestResultError(TestResultError::TimedOut(
                        ..
                    )))
                ))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[serial]
#[timeout(2000)]
fn many_retries_complete() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
            echo_test(proto, "echo3".to_string()),
            echo_test(proto, "echo4".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::FailUntilAttemptNumber(4),
        Box::new(manifest),
    );

    let supervisor_config = SupervisorConfig::new().with_retries(3);

    let mut expected_results = vec![];
    for attempt in 1..=4 {
        for t in 1..=4 {
            if attempt < 4 {
                expected_results.push((attempt, "INDUCED FAIL".to_string()));
            } else {
                expected_results.push((attempt, format!("echo{t}")));
            }
        }
    }

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), supervisor_config),
                StartWorkers(Run(1), Wid(1), workers_config(1, runner)),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results_owned(&mut results);
                    results == expected_results
                }),
            ],
        )
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert!(matches!(e, &WorkersExitStatus::Success))
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[serial]
#[timeout(2000)]
fn many_retries_many_workers_complete() {
    let attempts = 4;
    let num_tests = 64;
    let num_workers = 6;

    let mut manifest = vec![];
    let mut expected_results = vec![];

    for attempt in 1..=attempts {
        for t in 1..=num_tests {
            if attempt == 1 {
                manifest.push(echo_test(proto, format!("echo{t}")));
            }

            if attempt < 4 {
                expected_results.push((attempt, "INDUCED FAIL".to_string()));
            } else {
                expected_results.push((attempt, format!("echo{t}")));
            }
        }
    }

    expected_results.sort();

    let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));

    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::FailUntilAttemptNumber(4),
        Box::new(manifest),
    );

    let supervisor_config = SupervisorConfig::new().with_retries(3);

    let mut start_actions = vec![RunTest(Run(1), Sid(1), supervisor_config)];
    let mut end_workers_actions = vec![];
    let mut end_workers_asserts = vec![];
    for i in 1..=num_workers {
        start_actions.push(StartWorkers(
            Run(1),
            Wid(i),
            workers_config(i as u32, runner.clone()),
        ));
        end_workers_actions.push(StopWorkers(Wid(i)));
        end_workers_asserts.push(WorkerExitStatus(Wid(i), &|e| {
            assert!(matches!(e, &WorkersExitStatus::Success))
        }));
    }

    TestBuilder::default()
        .step(
            start_actions,
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results_owned(&mut results);
                    results == expected_results
                }),
            ],
        )
        .step(end_workers_actions, end_workers_asserts)
        .test();
}

#[test]
#[traced_test]
#[with_protocol_version]
#[serial]
#[timeout(2000)] // 2 seconds
fn many_retries_many_workers_complete_native() {
    let attempts = 4;
    let num_tests = 64;
    let num_workers = 6;

    let mut manifest = vec![];
    let mut expected_results = vec![];

    for attempt in 1..=attempts {
        for t in 1..=num_tests {
            if attempt == 1 {
                manifest.push(TestOrGroup::test(Test::new(
                    proto,
                    t.to_string(),
                    [],
                    Default::default(),
                )));
            }

            expected_results.push((attempt, "INDUCED FAIL".to_string()));
        }
    }

    expected_results.sort();

    use abq_native_runner_simulation::{legal_spawned_message, pack, pack_msgs, Msg::*};

    let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));

    let fake_test_result = TestResult::new(
        RunnerMeta::fake(),
        TestResultSpec {
            output: Some("INDUCED FAIL".to_string()),
            status: Status::Failure {
                exception: None,
                backtrace: None,
            },
            ..TestResultSpec::fake()
        },
    );

    let simulation = [
        Connect,
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Write the manifest if we need to.
        // Otherwise handle the one test.
        IfGenerateManifest {
            then_do: vec![
                OpaqueWrite(pack(&manifest)),
                Stdout(b"hello from manifest stdout".to_vec()),
                Stderr(b"hello from manifest stderr".to_vec()),
            ],
            else_do: {
                let mut run_tests = vec![
                    //
                    // Read init context message + write ACK
                    OpaqueRead,
                    OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                ];

                for _ in 0..num_tests {
                    // If the socket is alive (i.e. we have a test to run), pull it and give back a
                    // faux result.
                    // Otherwise assume we ran out of tests on our node and exit.
                    run_tests.push(IfOpaqueReadSocketAlive {
                        then_do: vec![OpaqueWrite(pack(RawTestResultMessage::from_test_result(
                            proto,
                            fake_test_result.clone(),
                        )))],
                        else_do: vec![Exit(0)],
                    })
                }
                run_tests
            },
        },
        //
        // Finish
        Exit(0),
    ];
    let simulation_msg = pack_msgs(simulation);
    let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let simfile_path = simfile.to_path_buf();
    std::fs::write(&simfile_path, simulation_msg).unwrap();

    let runner = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![simfile_path.display().to_string()],
        extra_env: Default::default(),
    });

    let supervisor_config = SupervisorConfig::new().with_retries(3);

    let mut start_actions = vec![RunTest(Run(1), Sid(1), supervisor_config)];
    let mut end_workers_actions = vec![];
    let mut end_workers_asserts = vec![];
    for i in 1..=num_workers {
        start_actions.push(StartWorkers(
            Run(1),
            Wid(i),
            workers_config(i as u32, runner.clone()),
        ));
        end_workers_actions.push(StopWorkers(Wid(i)));
        end_workers_asserts.push(WorkerExitStatus(Wid(i), &|e| {
            assert!(matches!(e, &WorkersExitStatus::Failure { .. }))
        }));
    }

    TestBuilder::default()
        .step(
            start_actions,
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results_owned(&mut results);
                    results == expected_results
                }),
            ],
        )
        .step(end_workers_actions, end_workers_asserts)
        .test();
}
