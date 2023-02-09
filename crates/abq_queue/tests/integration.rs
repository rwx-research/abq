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
        self, run_cancellation_pair, Client, CompletedSummary, InvocationError, RunCancellationTx,
        TestResultError, DEFAULT_CLIENT_POLL_TIMEOUT,
    },
    queue::{Abq, QueueConfig},
    timeout::{RunTimeoutStrategy, TimeoutReason},
};
use abq_test_utils::assert_scoped_log;
use abq_utils::{
    auth::{ClientAuthStrategy, User},
    exit::ExitCode,
    net::{ClientStream, ConfiguredClient},
    net_opt::ClientOptions,
    net_protocol::{
        self,
        entity::EntityId,
        queue::{
            CancelReason, CannotSetOOBExitCodeReason, NativeRunnerInfo,
            SetOutOfBandExitCodeResponse,
        },
        runners::{
            AbqProtocolVersion, Location, Manifest, ManifestMessage, MetadataMap, OutOfBandError,
            ProtocolWitness, Test, TestOrGroup, TestResult,
        },
        work_server::{InitContext, InitContextResponse},
        workers::{NativeTestRunnerParams, RunId, RunnerKind, TestLikeRunner},
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

fn default_workers_config() -> WorkersConfig {
    WorkersConfig {
        num_workers: 2.try_into().unwrap(),
        worker_context: WorkerContext::AssumeLocal,
        supervisor_in_band: false,
        debug_native_runner: false,
    }
}

fn sort_results(results: &mut [TestResult]) -> Vec<&str> {
    let mut results = results
        .iter()
        .map(|result| result.output.as_ref().unwrap().as_str())
        .collect::<Vec<_>>();
    results.sort_unstable();
    results
}

// TODO: put this on [Client] directly
struct SupervisorConfig {
    runner_kind: RunnerKind,
    batch_size: NonZeroU64,
    timeout: Duration,
    track_exit_code_in_band: bool,
}

impl SupervisorConfig {
    fn new(runner_kind: RunnerKind) -> Self {
        Self {
            runner_kind,
            batch_size: one_nonzero(),
            timeout: DEFAULT_CLIENT_POLL_TIMEOUT,
            track_exit_code_in_band: true,
        }
    }

    fn with_batch_size(self, batch_size: u64) -> Self {
        Self {
            batch_size: batch_size.try_into().unwrap(),
            ..self
        }
    }

    fn with_track_exit_code_in_band(self, track_exit_code_in_band: bool) -> Self {
        Self {
            track_exit_code_in_band,
            ..self
        }
    }
}

type GetConn<'a> = &'a dyn Fn() -> Box<dyn ClientStream>;

enum Action {
    RunTest(Run, Sid, SupervisorConfig),
    StartTest(Run, Sid, SupervisorConfig),
    WaitForLiveSupervisor(Sid),
    CancelTest(Sid),

    // TODO: consolidate start/stop workers by making worker pools async by default
    StartWorkers(Run, Wid, WorkersConfig),
    StopWorkers(Wid),

    SendOOBExitCode(ExternId, Run, ExitCode),

    /// Make a connection to the work server, the callback will test a request.
    WSRunRequest(Run, Box<dyn Fn(GetConn, RunId) + Send + Sync>),

    /// Spawn an action in a new task. Do this if the action is blocking.
    Spawn(SpawnId, Box<Action>),
}

#[allow(clippy::type_complexity)]
enum Assert<'a> {
    CheckSupervisor(Sid, &'a dyn Fn(&Result<Client, InvocationError>) -> bool),

    TestExit(Sid, &'a dyn Fn(&Result<CompletedSummary, TestResultError>)),
    TestExitWithoutErr(Sid),
    TestResults(Sid, &'a dyn Fn(&[TestResult]) -> bool),

    SetOOBExitCode(ExternId, &'a dyn Fn(&SetOutOfBandExitCodeResponse)),

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

type Supervisors = Arc<Mutex<HashMap<Sid, Result<Client, InvocationError>>>>;
type SupervisorData =
    Arc<tokio::sync::Mutex<HashMap<Sid, (RunCancellationTx, Arc<Mutex<Vec<TestResult>>>)>>>;
type SupervisorResults =
    Arc<tokio::sync::Mutex<HashMap<Sid, Result<CompletedSummary, TestResultError>>>>;

type Workers = Arc<Mutex<HashMap<Wid, NegotiatedWorkers>>>;
type WorkersRedundant = Arc<Mutex<HashMap<Wid, bool>>>;
enum WorkersResult {
    Exit(WorkersExit),
    Panic,
}
type WorkersResults = Arc<Mutex<HashMap<Wid, WorkersResult>>>;

type SetOOBExitCodes = Arc<Mutex<HashMap<ExternId, SetOutOfBandExitCodeResponse>>>;

type BgTasks = HashMap<SpawnId, tokio::task::JoinHandle<()>>;

struct Handler {
    results: Arc<Mutex<Vec<TestResult>>>,
}

impl invoke::ResultHandler for Handler {
    fn on_result(&mut self, result: net_protocol::client::ReportedResult) {
        self.results.lock().push(result.test_result)
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

    set_oob_exit_codes: SetOOBExitCodes,

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
                runner_kind,
                batch_size,
                timeout,
                track_exit_code_in_band,
            } = config;
            async move {
                let client = Client::invoke_work(
                    EntityId::new(),
                    queue_server_addr,
                    client_opts,
                    run_id,
                    runner_kind,
                    batch_size,
                    timeout,
                    cancellation_rx,
                    track_exit_code_in_band,
                )
                .await;

                let collected_results = Arc::new(Mutex::new(Vec::new()));

                supervisor_data
                    .lock()
                    .await
                    .insert(super_id, (cancellation_tx, collected_results.clone()));

                if !run_to_completion {
                    supervisors.lock().insert(super_id, client);
                    return;
                }

                let client = client.unwrap();

                let handler = Handler {
                    results: collected_results.clone(),
                };

                let result = client.stream_results(handler).await;

                supervisor_results.lock().await.insert(super_id, result);
            }
            .boxed()
        }

        SendOOBExitCode(extern_id, n, exit_code) => {
            let run_id = get_run_id!(n);

            let queue_addr = queue.server_addr();
            async move {
                let mut conn = client.connect(queue_addr).unwrap();
                let request = net_protocol::queue::Request {
                    entity: EntityId::new(),
                    message: net_protocol::queue::Message::SetOutOfBandExitCode(run_id, exit_code),
                };
                // TODO: use async clients here
                net_protocol::write(&mut conn, &request).unwrap();
                let resp: net_protocol::queue::SetOutOfBandExitCodeResponse =
                    net_protocol::read(&mut conn).unwrap();

                set_oob_exit_codes.lock().insert(extern_id, resp);
            }
            .boxed()
        }

        WaitForLiveSupervisor(super_id) => async move {
            loop {
                let data = supervisor_data.lock().await;
                if data.contains_key(&super_id) {
                    return;
                }
            }
        }
        .boxed(),

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
                set_oob_exit_codes,
                background_tasks,
                *action,
            );

            let handle = tokio::spawn(fut);

            background_tasks.insert(id, handle);

            async {}.boxed()
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

        let set_oob_exit_codes = SetOOBExitCodes::default();

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
                    set_oob_exit_codes.clone(),
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
                        assert!(exit.is_ok());
                        let CompletedSummary { native_runner_info } = exit.as_ref().unwrap();
                        let NativeRunnerInfo {
                            protocol_version,
                            specification: _,
                        } = native_runner_info;
                        assert_eq!(protocol_version, &AbqProtocolVersion::V0_1);
                    }

                    TestResults(n, check) => {
                        let supervisor_data = supervisor_data.lock().await;
                        let (_, results) = supervisor_data.get(&n).expect("supervisor not found");
                        let results = results.lock();
                        assert!(check(&results));
                    }

                    SetOOBExitCode(extern_id, check) => {
                        let responses = set_oob_exit_codes.lock();
                        let resp = responses
                            .get(&extern_id)
                            .expect("OOB exit code response for extern not found");
                        check(resp);
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo1", "echo2"]
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
    assert_scoped_log("abq_queue::queue", "worker post completion idle seconds");
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner1)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
                //
                RunTest(Run(2), Sid(2), SupervisorConfig::new(runner2)),
                StartWorkers(Run(2), Wid(2), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo1", "echo2"]
                }),
                //
                TestExitWithoutErr(Sid(2)),
                TestResults(Sid(2), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo3", "echo4", "echo5"]
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
                RunTest(
                    Run(1),
                    Sid(1),
                    SupervisorConfig::new(runner).with_batch_size(2),
                ),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo1", "echo2", "echo3", "echo4"]
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
        ..default_workers_config()
    };

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
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
        ..default_workers_config()
    };

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
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
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        // Start one client with the run ID
        .step(
            [StartTest(
                Run(1),
                Sid(1),
                SupervisorConfig::new(runner.clone()),
            )],
            [CheckSupervisor(Sid(1), &|s| s.is_ok())],
        )
        // Start a second, with the same run ID
        .step(
            [StartTest(Run(1), Sid(2), SupervisorConfig::new(runner))],
            [CheckSupervisor(Sid(2), &|s| {
                matches!(s, Err(InvocationError::DuplicateRun(..)))
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner.clone())),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
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
            [StartTest(Run(1), Sid(1), SupervisorConfig::new(runner))],
            [CheckSupervisor(Sid(1), &|supervisor| {
                matches!(supervisor, Err(InvocationError::DuplicateCompletedRun(..)))
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
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
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, manifest.into());

    TestBuilder::default()
        // Set up the queue so that a run ID is invoked, but no worker has connected yet.
        .act([StartTest(Run(1), Sid(1), SupervisorConfig::new(runner))])
        .act([WSRunRequest(
            Run(1),
            Box::new(|get_conn, run_id| {
                use net_protocol::work_server::WorkServerRequest;

                let mut conn = get_conn();

                // Ask the server for the next test; we should be told a manifest is still TBD.
                net_protocol::write(&mut conn, WorkServerRequest::InitContext { run_id }).unwrap();

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
            StartTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
            StartWorkers(Run(1), Wid(1), default_workers_config()),
        ])
        .act([WSRunRequest(
            Run(1),
            Box::new(|get_conn, run_id| {
                use net_protocol::work_server::WorkServerRequest;

                let mut conn = get_conn();

                // Ask the server for the next test; we should be told a manifest is still TBD.
                net_protocol::write(&mut conn, WorkServerRequest::InitContext { run_id }).unwrap();

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
        ..default_workers_config()
    };

    TestBuilder::default()
        .act([
            StartTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
            StartWorkers(Run(1), Wid(1), workers_config),
        ])
        .act([WSRunRequest(
            Run(1),
            Box::new(move |get_conn, run_id| {
                use net_protocol::work_server::WorkServerRequest;

                // Ask the server for the work context, it should align with the init_meta we gave
                // to begin with.
                loop {
                    let mut conn = get_conn();

                    net_protocol::write(
                        &mut conn,
                        WorkServerRequest::InitContext {
                            run_id: run_id.clone(),
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
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
                use net_protocol::work_server::WorkServerRequest;

                let mut conn = get_conn();

                // Ask the server for the work context, we should be told the run is already done.
                net_protocol::write(&mut conn, WorkServerRequest::InitContext { run_id }).unwrap();

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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == [("echo1"), ("echo2")]
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
            [StartWorkers(Run(1), Wid(2), default_workers_config())],
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
            Box::new(RunTest(Run(1), Sid(1), SupervisorConfig::new(runner))),
        )])
        .step(
            [CancelTest(Sid(1))],
            [TestExit(Sid(1), &|result| {
                assert!(matches!(result, Err(TestResultError::Cancelled)))
            })],
        )
        .act([StartWorkers(Run(1), Wid(1), default_workers_config())])
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [TestExit(Sid(1), &|results_err| {
                assert!(matches!(
                    results_err.as_ref().unwrap_err(),
                    TestResultError::TestCommandError(..)
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
        ..default_workers_config()
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
            RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
            // After pulling the only test, the queue should time us out.
            StartWorkers(Run(1), Wid(1), workers_config),
        ],
        [TestExit(Sid(1), &|result| {
            assert!(matches!(result, Err(TestResultError::TimedOut(..))));
        })],
    )
    .act([StopWorkers(Wid(1))])
    .test();
}

#[test]
fn pending_worker_attachment_does_not_block_other_attachers() {
    TestBuilder::default()
        // Start a set of workers that will never find their execution context, and just
        // continuously poll the queue.
        .act([Spawn(
            SpawnId(1),
            Box::new(StartWorkers(Run(1), Wid(1), default_workers_config())),
        )])
        // In the meantime, start and execute a run that should complete successfully.
        .step(
            {
                let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest_msg());
                [
                    RunTest(Run(2), Sid(2), SupervisorConfig::new(runner)),
                    StartWorkers(Run(2), Wid(2), default_workers_config()),
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [TestExit(Sid(1), &|result| {
                let err = result.as_ref().unwrap_err();
                assert!(matches!(err, TestResultError::TestCommandError(..)));
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
                RunTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo1", "echo2", "echo3", "echo4", "echo5", "echo6"]
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
#[timeout(1000)] // 1 second
fn set_exit_code_out_of_band() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    let supervisor_config = SupervisorConfig::new(runner).with_track_exit_code_in_band(false);

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), supervisor_config),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo1"]
                }),
            ],
        )
        // Send the OOB exit code.
        .step(
            [SendOOBExitCode(ExternId(1), Run(1), ExitCode::new(72))],
            [SetOOBExitCode(ExternId(1), &|resp| {
                assert!(matches!(resp, SetOutOfBandExitCodeResponse::Success));
            })],
        )
        // Workers should exit with the OOB exit code.
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(
                    e,
                    &WorkersExitStatus::Failure {
                        exit_code: ExitCode::new(72)
                    }
                )
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn trying_to_set_oob_exit_code_during_run_is_error() {
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
            Box::new(RunTest(Run(1), Sid(1), SupervisorConfig::new(runner))),
        )])
        .act([WaitForLiveSupervisor(Sid(1))])
        .step(
            [SendOOBExitCode(ExternId(1), Run(1), ExitCode::new(72))],
            [SetOOBExitCode(ExternId(1), &|resp| {
                assert!(matches!(
                    resp,
                    SetOutOfBandExitCodeResponse::Failure(CannotSetOOBExitCodeReason::RunIsActive)
                ));
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn trying_to_set_oob_exit_code_for_cancelled_run_is_error() {
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
            Box::new(RunTest(Run(1), Sid(1), SupervisorConfig::new(runner))),
        )])
        .step(
            [CancelTest(Sid(1))],
            [TestExit(Sid(1), &|result| {
                assert!(matches!(result, Err(TestResultError::Cancelled)))
            })],
        )
        .step(
            [SendOOBExitCode(ExternId(1), Run(1), ExitCode::new(72))],
            [SetOOBExitCode(ExternId(1), &|resp| {
                assert!(matches!(
                    resp,
                    SetOutOfBandExitCodeResponse::Failure(
                        CannotSetOOBExitCodeReason::RunWasCancelled {
                            reason: CancelReason::User
                        }
                    )
                ));
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn trying_to_set_oob_exit_code_for_completed_run_is_error() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    let supervisor_config = SupervisorConfig::new(runner);

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), supervisor_config),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo1"]
                }),
            ],
        )
        // Workers should exit successfully thanks to the completed run.
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(e, &WorkersExitStatus::Success)
            })],
        )
        // Send the OOB exit code.
        .step(
            [SendOOBExitCode(ExternId(1), Run(1), ExitCode::new(72))],
            [SetOOBExitCode(ExternId(1), &|resp| {
                assert!(matches!(
                    resp,
                    SetOutOfBandExitCodeResponse::Failure(
                        CannotSetOOBExitCodeReason::ExitCodeAlreadyDetermined
                    )
                ));
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn trying_to_set_oob_exit_code_twice_is_error() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    let supervisor_config = SupervisorConfig::new(runner).with_track_exit_code_in_band(false);

    TestBuilder::default()
        .step(
            [
                RunTest(Run(1), Sid(1), supervisor_config),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
            ],
            [
                TestExitWithoutErr(Sid(1)),
                TestResults(Sid(1), &|results| {
                    let mut results = results.to_vec();
                    let results = sort_results(&mut results);
                    results == ["echo1"]
                }),
            ],
        )
        // Send the OOB exit code.
        .step(
            [SendOOBExitCode(ExternId(1), Run(1), ExitCode::new(72))],
            [SetOOBExitCode(ExternId(1), &|resp| {
                assert!(matches!(resp, SetOutOfBandExitCodeResponse::Success));
            })],
        )
        // Send the OOB exit code again.
        .step(
            [SendOOBExitCode(ExternId(1), Run(1), ExitCode::new(91))],
            [SetOOBExitCode(ExternId(1), &|resp| {
                assert!(matches!(
                    resp,
                    SetOutOfBandExitCodeResponse::Failure(
                        CannotSetOOBExitCodeReason::ExitCodeAlreadyDetermined
                    )
                ));
            })],
        )
        // Workers should exit with the original OOB exit code.
        .step(
            [StopWorkers(Wid(1))],
            [WorkerExitStatus(Wid(1), &|e| {
                assert_eq!(
                    e,
                    &WorkersExitStatus::Failure {
                        exit_code: ExitCode::new(72)
                    }
                )
            })],
        )
        .test();
}

#[test]
#[with_protocol_version]
fn cancel_test_run_upon_timeout_for_out_of_band_exit_code() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    let supervisor_config = SupervisorConfig::new(runner).with_track_exit_code_in_band(false);

    fn zero_for_oob_timeout(reason: TimeoutReason) -> Duration {
        match reason {
            TimeoutReason::ResultNotReceived => Duration::MAX,
            TimeoutReason::OOBExitCodeNotReceived => Duration::ZERO,
        }
    }

    TestBuilder::new(Servers::from_config(QueueConfig {
        timeout_strategy: RunTimeoutStrategy::constant(zero_for_oob_timeout),
        ..Default::default()
    }))
    .step(
        [
            RunTest(Run(1), Sid(1), supervisor_config),
            StartWorkers(Run(1), Wid(1), default_workers_config()),
        ],
        [TestExitWithoutErr(Sid(1))],
    )
    // Workers should exit seeing that the test run was cancelled.
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
                StartTest(Run(1), Sid(1), SupervisorConfig::new(runner)),
                StartWorkers(Run(1), Wid(1), default_workers_config()),
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
