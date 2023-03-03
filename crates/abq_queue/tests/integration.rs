use std::{
    collections::HashMap,
    future::Future,
    iter::once,
    num::{NonZeroU64, NonZeroUsize},
    panic,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use abq_native_runner_simulation::pack_msgs_to_disk;
use abq_queue::{
    persistence::manifest::{self, SharedPersistManifest},
    queue::{Abq, QueueConfig, DEFAULT_CLIENT_POLL_TIMEOUT},
};
use abq_test_utils::artifacts_dir;
use abq_utils::{
    auth::{ClientAuthStrategy, User},
    exit::ExitCode,
    net::{ClientStream, ConfiguredClient},
    net_opt::ClientOptions,
    net_protocol::{
        self,
        entity::{Entity, RunnerMeta, WorkerTag},
        queue::{self, AssociatedTestResults, InvokeWork},
        runners::{
            InitSuccessMessage, Location, Manifest, ManifestMessage, MetadataMap, OutOfBandError,
            ProtocolWitness, RawTestResultMessage, Status, Test, TestOrGroup, TestResult,
            TestResultSpec,
        },
        work_server::{InitContext, InitContextResponse},
        workers::{
            NativeTestRunnerParams, RunId, RunnerKind, TestLikeRunner, WorkId, INIT_RUN_NUMBER,
        },
    },
    results_handler::{NoopResultsHandler, SharedAssociatedTestResults, StaticResultsHandler},
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
use tempfile::TempDir;
use tracing_test::traced_test;
use Action::*;
use Assert::*;
use Servers::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Run(usize);

/// Worker ID
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Wid(usize);

/// External party ID
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ExternId(usize);

/// ID of a spawned action
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SpawnId(usize);

/// External dependencies that must be preserved while a test is ongoing.
struct QueueExtDeps {
    _manifests_path: TempDir,
}

enum Servers {
    Unified(QueueConfig, QueueExtDeps),
}

impl Default for Servers {
    fn default() -> Self {
        let manifests_path = tempfile::tempdir().unwrap();
        let persist_manifest = SharedPersistManifest::new(manifest::fs::FilesystemPersistor::new(
            manifests_path.path(),
        ));
        let config = QueueConfig::new(persist_manifest);
        let deps = QueueExtDeps {
            _manifests_path: manifests_path,
        };
        Self::Unified(config, deps)
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

struct WorkersConfigBuilder {
    config: WorkersConfig,
    batch_size_hint: NonZeroU64,
    test_results_timeout: Duration,
}

impl WorkersConfigBuilder {
    fn new(tag: impl Into<WorkerTag>, runner_kind: RunnerKind) -> Self {
        let config = WorkersConfig {
            tag: tag.into(),
            num_workers: 2.try_into().unwrap(),
            max_run_number: INIT_RUN_NUMBER,
            runner_kind,
            local_results_handler: Box::new(NoopResultsHandler),
            worker_context: WorkerContext::AssumeLocal,
            debug_native_runner: false,
            results_batch_size_hint: 1,
        };
        Self {
            config,
            batch_size_hint: one_nonzero(),
            test_results_timeout: DEFAULT_CLIENT_POLL_TIMEOUT,
        }
    }

    fn with_max_run_number(mut self, max_run_number: u32) -> Self {
        self.config.max_run_number = max_run_number;
        self
    }

    fn with_num_workers(mut self, num_workers: NonZeroUsize) -> Self {
        self.config.num_workers = num_workers;
        self
    }
}

fn sort_results<'a>(results: &mut [FlatResult<'a>]) -> Vec<(u32, &'a str)> {
    let mut results = results
        .iter()
        .map(|(_, n, result)| (*n, result.output.as_ref().unwrap().as_str()))
        .collect::<Vec<_>>();
    results.sort_unstable();
    results
}

fn sort_results_owned(results: &mut [FlatResult<'_>]) -> Vec<(u32, String)> {
    sort_results(results)
        .into_iter()
        .map(|(attempt, r)| (attempt, r.to_string()))
        .collect()
}

fn native_runner_simulation_bin() -> String {
    artifacts_dir()
        .join("abqtest_native_runner_simulation")
        .display()
        .to_string()
}

type GetConn<'a> = &'a dyn Fn() -> Box<dyn ClientStream>;

enum Action {
    // TODO: consolidate start/stop workers by making worker pools async by default
    StartWorkers(Run, Wid, WorkersConfigBuilder),
    StopWorkers(Wid),
    CancelWorkers(Wid),

    #[allow(unused)] // for now
    WaitForCompletedRun(Run),

    /// Make a connection to the work server, the callback will test a request.
    WSRunRequest(Run, Box<dyn Fn(GetConn, RunId) + Send + Sync>),
}

type FlatResult<'a> = (WorkId, u32, &'a TestResult);

#[allow(clippy::type_complexity)]
enum Assert<'a> {
    TestResults(Run, Box<dyn Fn(&[FlatResult<'_>]) -> bool>),

    WorkersAreRedundant(Wid),
    WorkerExitStatus(Wid, Box<dyn Fn(&WorkersExitStatus)>),
    WorkerResultStatus(Wid, &'a dyn Fn(&WorkersExitKind)),
}

type Steps<'a> = Vec<(Vec<Action>, Vec<Assert<'a>>)>;

#[derive(Default)]
struct TestBuilder<'a> {
    servers: Servers,
    steps: Steps<'a>,
}

impl<'a> TestBuilder<'a> {
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

type Workers = Arc<tokio::sync::Mutex<HashMap<Wid, NegotiatedWorkers>>>;
type WorkersRedundant = Arc<Mutex<HashMap<Wid, bool>>>;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum WorkersExitKind {
    Exit(WorkersExit),
    Panic,
}

type WorkersExitKinds = Arc<Mutex<HashMap<Wid, WorkersExitKind>>>;

fn action_to_fut(
    queue: &Abq,
    run_ids: &mut HashMap<Run, RunId>,
    run_results: &mut HashMap<Run, SharedAssociatedTestResults>,
    client: Box<dyn ConfiguredClient>,
    client_opts: ClientOptions<User>,

    workers: Workers,
    workers_redundant: WorkersRedundant,
    worker_exit_kinds: WorkersExitKinds,

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

    macro_rules! get_run_results {
        ($n:expr) => {
            run_results
                .entry($n)
                .or_insert_with(Default::default)
                .clone()
        };
    }

    match action {
        StartWorkers(n, worker_id, workers_config_builder) => {
            let run_id = get_run_id!(n);
            let run_results = get_run_results!(n);
            let negotiator = queue.get_negotiator_handle();

            let WorkersConfigBuilder {
                config,
                batch_size_hint,
                test_results_timeout,
            } = workers_config_builder;

            let invoke_work = InvokeWork {
                run_id,
                batch_size_hint,
                test_results_timeout,
            };

            let config = WorkersConfig {
                local_results_handler: Box::new(StaticResultsHandler::new(run_results)),
                ..config
            };

            async move {
                let worker_pool = WorkersNegotiator::negotiate_and_start_pool_on_executor(
                    config,
                    negotiator,
                    client_opts.clone(),
                    invoke_work,
                )
                .await
                .unwrap();

                workers_redundant.lock().insert(
                    worker_id,
                    matches!(worker_pool, NegotiatedWorkers::Redundant { .. }),
                );

                workers.lock().await.insert(worker_id, worker_pool);
            }
            .boxed()
        }

        StopWorkers(n) => async move {
            let mut worker_pool = workers.lock().await.remove(&n).unwrap();

            worker_pool.wait().await;

            let workers_result =
                match panic::catch_unwind(panic::AssertUnwindSafe(|| worker_pool.shutdown())) {
                    Ok(exit) => WorkersExitKind::Exit(exit),
                    Err(_) => WorkersExitKind::Panic,
                };
            worker_exit_kinds.lock().insert(n, workers_result);
        }
        .boxed(),

        CancelWorkers(worker_id) => async move {
            let worker_pool = workers.lock().await.remove(&worker_id).unwrap();

            let workers_result =
                match panic::catch_unwind(panic::AssertUnwindSafe(|| worker_pool.cancel())) {
                    Ok(exit) => WorkersExitKind::Exit(exit),
                    Err(_) => WorkersExitKind::Panic,
                };
            worker_exit_kinds.lock().insert(worker_id, workers_result);
        }
        .boxed(),

        WaitForCompletedRun(n) => {
            let run_id = get_run_id!(n);
            let queue_addr = queue.server_addr();
            let client = client_opts.build_async().unwrap();
            async move {
                loop {
                    let mut conn = client.connect(queue_addr).await.unwrap();
                    net_protocol::async_write(
                        &mut conn,
                        &queue::Request {
                            entity: Entity::local_client(),
                            message: queue::Message::RunStatus(run_id.clone()),
                        },
                    )
                    .await
                    .unwrap();

                    use queue::RunStatus::*;
                    match net_protocol::async_read(&mut conn).await.unwrap() {
                        InitialManifestDone {
                            num_active_workers: 0,
                        } => {
                            break;
                        }
                        Cancelled => {
                            break;
                        }
                        Active | InitialManifestDone { .. } => {
                            tokio::time::sleep(Duration::from_micros(500)).await;
                            continue;
                        }
                    }
                }
            }
            .boxed()
        }

        WSRunRequest(n, callback) => {
            let run_id = get_run_id!(n);
            let work_scheduler_addr = queue.work_server_addr();
            async move {
                let get_conn = &|| client.connect(work_scheduler_addr).unwrap();
                callback(get_conn, run_id);
            }
            .boxed()
        }
    }
}

fn run_test(servers: Servers, steps: Steps) {
    let (mut queue, _deps) = match servers {
        Unified(config, deps) => (Abq::start(config), deps),
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
        let mut run_results: HashMap<Run, SharedAssociatedTestResults> = Default::default();

        let workers = Workers::default();
        let workers_redundant = WorkersRedundant::default();
        let worker_exit_kinds = WorkersExitKinds::default();

        for (action_set, asserts) in steps {
            let mut futs = vec![];
            for action in action_set {
                let fut = action_to_fut(
                    &queue,
                    &mut run_ids,
                    &mut run_results,
                    client.boxed_clone(),
                    client_opts.clone(),
                    workers.clone(),
                    workers_redundant.clone(),
                    worker_exit_kinds.clone(),
                    action,
                );

                futs.push(fut);
            }

            // Wait for all steps to complete, then we start the round asserts.
            futures::future::join_all(futs).await;

            for check in asserts {
                match check {
                    TestResults(n, check) => {
                        let results = run_results.get(&n).expect("run results not found");
                        let results = results.lock();

                        let flattened_results: Vec<_> = results
                            .iter()
                            .flat_map(
                                |AssociatedTestResults {
                                     work_id,
                                     run_number,
                                     results,
                                     ..
                                 }| {
                                    results.iter().map(|result| (*work_id, *run_number, result))
                                },
                            )
                            .collect();

                        assert!(check(&flattened_results));
                    }

                    WorkersAreRedundant(n) => {
                        assert!(workers_redundant.lock().get(&n).unwrap());
                    }

                    WorkerExitStatus(n, workers_exit) => {
                        let results = worker_exit_kinds.lock();
                        let real_result = results.get(&n).expect("workers result not found");
                        match real_result {
                            WorkersExitKind::Exit(exit) => workers_exit(&exit.status),
                            WorkersExitKind::Panic => panic!("expected exit result, not panic"),
                        }
                    }

                    WorkerResultStatus(n, workers_result) => {
                        let results = worker_exit_kinds.lock();
                        let real_result = results.get(&n).expect("workers result not found");
                        workers_result(real_result)
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))],
            [
                TestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo1"), (1, "echo2")]
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
        )
        .test();
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
        .act([
            StartWorkers(Run(1), Wid(1), WorkersConfigBuilder::new(1, runner1)),
            //
            StartWorkers(Run(2), Wid(2), WorkersConfigBuilder::new(2, runner2)),
        ])
        .step(
            [
                StopWorkers(Wid(1)),
                StopWorkers(Wid(2)),
                WaitForCompletedRun(Run(1)),
            ],
            [
                TestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo1"), (1, "echo2")]
                    }),
                ),
                //
                TestResults(
                    Run(2),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo3"), (1, "echo4"), (1, "echo5")]
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| (assert_eq!(e, &WorkersExitStatus::SUCCESS))),
                ),
                WorkerExitStatus(
                    Wid(2),
                    Box::new(|e| (assert_eq!(e, &WorkersExitStatus::SUCCESS))),
                ),
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))],
            [
                TestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo1"), (1, "echo2"), (1, "echo3"), (1, "echo4")]
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
        )
        .test();
}

#[test]
#[with_protocol_version]
fn empty_manifest_exits_gracefully() {
    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest_msg());

    TestBuilder::default()
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))],
            [
                TestResults(Run(1), Box::new(|results| results.is_empty())),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
        )
        .test();
}

#[test]
#[traced_test]
#[with_protocol_version]
fn get_init_context_from_work_server_waiting_for_first_worker() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
            echo_test(proto, "echo3".to_string()),
            echo_test(proto, "echo4".to_string()),
        ],
        Default::default(),
    ));

    let runner =
        RunnerKind::TestLikeRunner(TestLikeRunner::NeverReturnManifest, Box::new(manifest));

    TestBuilder::default()
        // Set up the queue so that a run ID is invoked, but no worker has connected yet.
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
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

    let workers_config = WorkersConfigBuilder::new(1, runner).with_num_workers(one_nonzero_usize());

    TestBuilder::default()
        .act([StartWorkers(Run(1), Wid(1), workers_config)])
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
                        InitContextResponse::RunAlreadyCompleted { .. } => unreachable!(),
                    }
                }
            }),
        )])
        .act([StopWorkers(Wid(1))])
        .test();
}

#[test]
#[with_protocol_version]
#[traced_test]
fn get_init_context_after_run_already_completed() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    TestBuilder::default()
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))],
            [WorkerExitStatus(
                Wid(1),
                Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
            )],
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

                assert!(matches!(
                    response,
                    InitContextResponse::RunAlreadyCompleted { cancelled: false }
                ));
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner.clone()),
        )])
        .step(
            [StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))],
            [
                TestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo1"), (1, "echo2")]
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
        )
        // Now, simulate a new set of workers connecting again. Negotiation should succeed, but
        // they should be marked as redundant.
        .step(
            [StartWorkers(
                Run(1),
                Wid(2),
                WorkersConfigBuilder::new(2, runner),
            )],
            [WorkersAreRedundant(Wid(2))],
        )
        .step(
            [
                StopWorkers(Wid(2)),
                // Run should still be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [WorkerExitStatus(
                Wid(2),
                Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
            )],
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner.clone()),
        )])
        .step(
            [
                CancelWorkers(Wid(1)),
                // Run should now be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [WorkerExitStatus(
                Wid(1),
                Box::new(|e| assert_eq!(e, &WorkersExitStatus::Completed(ExitCode::CANCELLED))),
            )],
        )
        .act([StartWorkers(
            Run(1),
            Wid(2),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [
                StopWorkers(Wid(2)),
                // Run should still be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [WorkerExitStatus(
                Wid(2),
                Box::new(|e| assert_eq!(e, &WorkersExitStatus::Completed(ExitCode::CANCELLED))),
            )],
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [
                StopWorkers(Wid(1)),
                // Run should be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [WorkerExitStatus(
                Wid(1),
                Box::new(|e| assert!(matches!(e, WorkersExitStatus::Error { .. }))),
            )],
        )
        .test();
}

#[test]
fn native_runner_fails_due_to_manifest_failure() {
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [
                StopWorkers(Wid(1)),
                // Run should be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [WorkerExitStatus(
                Wid(1),
                Box::new(|e| assert!(matches!(e, WorkersExitStatus::Error { .. }))),
            )],
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [
                StopWorkers(Wid(1)),
                // Run should be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [
                TestResults(
                    Run(1),
                    Box::new(|results| {
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
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
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
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [StopWorkers(Wid(1))],
            [WorkerResultStatus(Wid(1), &|e| {
                assert!(matches!(e, WorkersExitKind::Panic))
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

    let workers_config = WorkersConfigBuilder::new(1, runner).with_max_run_number(4);

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
        .act([StartWorkers(Run(1), Wid(1), workers_config)])
        .step(
            [
                StopWorkers(Wid(1)),
                // Run should be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [
                TestResults(
                    Run(1),
                    Box::new(move |results| {
                        let mut results = results.to_vec();
                        let results = sort_results_owned(&mut results);
                        results == expected_results
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
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

            if attempt < attempts {
                expected_results.push((attempt, "INDUCED FAIL".to_string()));
            } else {
                expected_results.push((attempt, format!("echo{t}")));
            }
        }
    }

    expected_results.sort();

    let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));

    let runner = RunnerKind::TestLikeRunner(
        TestLikeRunner::FailUntilAttemptNumber(attempts),
        Box::new(manifest),
    );

    let mut start_actions = vec![];
    let mut end_workers_actions = vec![];
    let mut end_workers_asserts = vec![];
    for i in 1..=num_workers {
        let workers_config =
            WorkersConfigBuilder::new(i as u32, runner.clone()).with_max_run_number(attempts);
        start_actions.push(StartWorkers(Run(1), Wid(i), workers_config));

        end_workers_actions.push(StopWorkers(Wid(i)));
        end_workers_asserts.push(WorkerExitStatus(
            Wid(i),
            Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
        ));
    }

    TestBuilder::default()
        .act(start_actions)
        .step(
            end_workers_actions.into_iter().chain(once(
                // Run should be seen as completed
                WaitForCompletedRun(Run(1)),
            )),
            end_workers_asserts.into_iter().chain(once(TestResults(
                Run(1),
                Box::new(move |results| {
                    let mut results = results.to_vec();
                    let results = sort_results_owned(&mut results);
                    results == expected_results
                }),
            ))),
        )
        .test();
}

#[test]
#[with_protocol_version]
#[serial]
#[timeout(3000)]
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

    let mut start_actions = vec![];
    let mut end_workers_actions = vec![];
    for i in 1..=num_workers {
        let config =
            WorkersConfigBuilder::new(i as u32, runner.clone()).with_max_run_number(attempts);
        start_actions.push(StartWorkers(Run(1), Wid(i), config));
        end_workers_actions.push(StopWorkers(Wid(i)));
    }

    // Run should be seen as completed
    end_workers_actions.push(WaitForCompletedRun(Run(1)));

    TestBuilder::default()
        .act(start_actions)
        .step(
            end_workers_actions,
            [TestResults(
                Run(1),
                Box::new(move |results| {
                    let mut results = results.to_vec();
                    let results = sort_results_owned(&mut results);
                    results == expected_results
                }),
            )],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[serial]
#[timeout(2000)] // 2 seconds
fn cancellation_native() {
    use abq_native_runner_simulation::{legal_spawned_message, pack, Msg::*};

    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
            echo_test(proto, "echo3".to_string()),
            echo_test(proto, "echo4".to_string()),
        ],
        Default::default(),
    ));

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
            else_do: vec![
                //
                // Read init context message + write ACK
                OpaqueRead,
                OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                //
                // Sleep forever
                Sleep(Duration::from_secs(600)),
            ],
        },
        //
        // Finish
        Exit(0),
    ];
    let simulation = pack_msgs_to_disk(simulation);

    let runner = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![simulation.path.display().to_string()],
        extra_env: Default::default(),
    });

    TestBuilder::default()
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner.clone()),
        )])
        .step(
            [
                CancelWorkers(Wid(1)),
                // Run should be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::Completed(ExitCode::CANCELLED))),
                ),
                TestResults(Run(1), Box::new(|results| results.is_empty())),
            ],
        )
        // A second pair of workers must also be cancelled.
        .act([StartWorkers(
            Run(1),
            Wid(2),
            WorkersConfigBuilder::new(1, runner),
        )])
        .step(
            [
                StopWorkers(Wid(2)),
                // Run should still be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [WorkerExitStatus(
                Wid(2),
                Box::new(|e| assert_eq!(e, &WorkersExitStatus::Completed(ExitCode::CANCELLED))),
            )],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn retry_out_of_process_worker() {
    let manifest1 = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
        ],
        Default::default(),
    ));
    let runner1 = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest1));

    // Second attempt of the runner produces no manifest, and should be fed our retry manifest from
    // the first pass.
    let manifest2 = ManifestMessage::new(Manifest::new([], Default::default()));
    let runner2 = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest2));

    TestBuilder::default()
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner1),
        )])
        .step(
            [StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))],
            [
                TestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo1"), (1, "echo2")]
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
        )
        // Second attempt of the run should run the same tests, produce the same results.
        .act([StartWorkers(
            Run(1),
            Wid(2),
            WorkersConfigBuilder::new(1, runner2),
        )])
        .step(
            [StopWorkers(Wid(2)), WaitForCompletedRun(Run(1))],
            [
                TestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo1"), (1, "echo1"), (1, "echo2"), (1, "echo2")]
                    }),
                ),
                WorkerExitStatus(
                    Wid(2),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
            ],
        )
        .test();
}

#[test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
fn many_retries_of_many_out_of_process_workers() {
    let num_tests = 64;
    let num_workers = 6;
    let num_out_of_process_retries = 4;

    let mut manifest = vec![];
    let mut expected_results_one_pass = vec![];

    for t in 1..=num_tests {
        manifest.push(echo_test(proto, format!("echo{t}")));
        expected_results_one_pass.push((INIT_RUN_NUMBER, format!("echo{t}")));
    }

    let manifest = ManifestMessage::new(Manifest::new(manifest, Default::default()));
    let empty_manifest = ManifestMessage::new(Manifest::new(vec![], Default::default()));

    let runner0 = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));
    let runner_after_0 = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(empty_manifest));

    let mut builder = TestBuilder::default();
    let run = Run(1);
    for retry in 1..=num_out_of_process_retries {
        let mut start_actions = vec![];
        let mut end_workers_actions = vec![];
        let mut end_workers_asserts = vec![];

        // Push on the workers for this set of out-of-process retries
        for i in 0..num_workers {
            let runner = if retry == 1 {
                runner0.clone()
            } else {
                runner_after_0.clone()
            };
            let workers_config = WorkersConfigBuilder::new(i as u32, runner);

            let worker_uuid = retry * num_workers + i;

            start_actions.push(StartWorkers(run, Wid(worker_uuid), workers_config));

            end_workers_actions.push(StopWorkers(Wid(worker_uuid)));
            end_workers_asserts.push(WorkerExitStatus(
                Wid(worker_uuid),
                Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
            ));
        }
        end_workers_actions.push(WaitForCompletedRun(run));

        // The number of expected results is now the results * how many retries we had
        let mut expected_results: Vec<_> = std::iter::repeat(expected_results_one_pass.clone())
            .take(retry)
            .flatten()
            .collect();
        expected_results.sort();
        end_workers_asserts.push(TestResults(
            run,
            Box::new(move |results| {
                let mut results = results.to_vec();
                let results = sort_results_owned(&mut results);
                results == expected_results
            }),
        ));

        // Chain on the test, then do the next retry iteration.
        builder = builder
            .act(start_actions)
            .step(end_workers_actions, end_workers_asserts);
    }

    builder.test();
}
