use std::{
    collections::HashMap,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    panic,
    path::Path,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use abq_native_runner_simulation::{legal_spawned_message, pack, pack_msgs_to_disk};
use abq_queue::{
    persistence::{
        self,
        manifest::ManifestView,
        remote::{LoadedRunState, PersistenceKind, RemotePersistence, RemotePersister},
        SerializableRunState,
    },
    queue::{Abq, QueueConfig},
    RunTimeoutStrategy, TimeoutReason,
};
use abq_test_utils::{artifacts_dir, assert_scoped_log, s};
use abq_utils::{
    auth::{ClientAuthStrategy, User},
    error::{ErrorLocation, OpaqueResult},
    exit::ExitCode,
    here,
    net_async::{ClientStream, ConfiguredClient},
    net_opt::ClientOptions,
    net_protocol::{
        self,
        entity::{Entity, RunnerMeta, WorkerTag},
        queue::{self, AssociatedTestResults, InvokeWork, TestResultsResponse, WorkStrategy},
        results::{OpaqueLazyAssociatedTestResults, ResultsLine, Summary},
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
    DEFAULT_PROTOCOL_VERSION_TIMEOUT, DEFAULT_RUNNER_TEST_TIMEOUT,
};
use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use ntest::timeout;
use parking_lot::Mutex;
use serial_test::serial;
use tempfile::TempDir;
use tracing_test::traced_test;
use Action::*;
use Assert::*;

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
    _results_path: TempDir,
}

struct Server {
    config: QueueConfig,
    deps: QueueExtDeps,
    remote: InMemoryRemote,
}

impl Default for Server {
    fn default() -> Self {
        let raw_remote = InMemoryRemote::default();
        let remote: RemotePersister = raw_remote.clone().into();

        let manifests_path = tempfile::tempdir().unwrap();
        let persist_manifest = persistence::manifest::FilesystemPersistor::new_shared(
            manifests_path.path(),
            remote.clone(),
        );

        let results_path = tempfile::tempdir().unwrap();
        let persist_results = persistence::results::FilesystemPersistor::new_shared(
            results_path.path(),
            10,
            remote.clone(),
        );

        let config = QueueConfig::new(persist_manifest, persist_results, remote);
        let deps = QueueExtDeps {
            _manifests_path: manifests_path,
            _results_path: results_path,
        };
        Self {
            config,
            deps,
            remote: raw_remote,
        }
    }
}

impl Server {
    fn with_timeout_strategy(mut self, run_timeout_strategy: RunTimeoutStrategy) -> Self {
        self.config.run_timeout_strategy = run_timeout_strategy;
        self
    }
}

#[derive(Default)]
struct InMemoryRemoteInner {
    manifests: HashMap<RunId, ManifestView>,
    results: HashMap<RunId, OpaqueLazyAssociatedTestResults>,
    run_states: HashMap<RunId, SerializableRunState>,
}

#[derive(Default, Clone)]
struct InMemoryRemote(Arc<tokio::sync::Mutex<InMemoryRemoteInner>>);

#[async_trait]
impl RemotePersistence for InMemoryRemote {
    /// Stores a file from the local filesystem to the remote persistence.
    async fn store_from_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        from_local_path: &Path,
    ) -> OpaqueResult<()> {
        let data = tokio::fs::read(from_local_path).await.unwrap();
        let mut inner = self.0.lock().await;
        match kind {
            PersistenceKind::Manifest => {
                let manifest: ManifestView = serde_json::from_slice(&data).unwrap();
                inner.manifests.insert(run_id.clone(), manifest);
            }
            PersistenceKind::Results => {
                use tokio::io::AsyncBufReadExt;

                let mut iter = tokio::io::BufReader::new(data.as_slice()).lines();
                let mut opaque_jsonl = vec![];
                while let Some(line) = iter.next_line().await.unwrap() {
                    opaque_jsonl.push(serde_json::value::RawValue::from_string(line).unwrap())
                }

                let results = OpaqueLazyAssociatedTestResults::from_raw_json_lines(opaque_jsonl);
                inner.results.insert(run_id.clone(), results);
            }
            PersistenceKind::RunState => {
                unimplemented!();
            }
        }
        Ok(())
    }

    /// Loads a file from the remote persistence to the local filesystem.
    /// The given local path must have all intermediate directories already created.
    async fn load_to_disk(
        &self,
        kind: PersistenceKind,
        run_id: &RunId,
        into_local_path: &Path,
    ) -> OpaqueResult<()> {
        let data = {
            let inner = self.0.lock().await;
            match kind {
                PersistenceKind::Manifest => {
                    let manifest = inner
                        .manifests
                        .get(run_id)
                        .ok_or_else(|| "manifest for entry missing".located(here!()))?;
                    serde_json::to_vec(manifest).unwrap()
                }
                PersistenceKind::Results => {
                    let results = inner
                        .results
                        .get(run_id)
                        .ok_or_else(|| "results for entry missing".located(here!()))?;
                    serde_json::to_vec(results).unwrap()
                }
                PersistenceKind::RunState => {
                    unimplemented!();
                }
            }
        };

        tokio::fs::write(into_local_path, data).await.unwrap();

        Ok(())
    }

    async fn store_run_state(
        &self,
        run_id: &RunId,
        run_state: SerializableRunState,
    ) -> OpaqueResult<()> {
        let mut inner = self.0.lock().await;
        inner.run_states.insert(run_id.clone(), run_state);
        Ok(())
    }

    async fn try_load_run_state(&self, run_id: &RunId) -> OpaqueResult<LoadedRunState> {
        let inner = self.0.lock().await;
        match inner.run_states.get(run_id) {
            Some(run_state) => Ok(LoadedRunState::Found(run_state.clone().into_run_state())),
            None => Ok(LoadedRunState::NotFound),
        }
    }

    fn boxed_clone(&self) -> Box<dyn RemotePersistence + Send + Sync> {
        Box::new(self.clone())
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
            has_stdout_reporters: false,
            results_batch_size_hint: 1,
            protocol_version_timeout: DEFAULT_PROTOCOL_VERSION_TIMEOUT,
            test_timeout: DEFAULT_RUNNER_TEST_TIMEOUT,
        };
        Self {
            config,
            batch_size_hint: one_nonzero(),
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

fn flatten_queue_results(
    results: impl std::borrow::Borrow<OpaqueLazyAssociatedTestResults>,
) -> (Vec<(u32, String)>, Summary) {
    let results = results.borrow();
    let lines = results.decode().unwrap();
    let mut collected = Vec::with_capacity(lines.len());
    let mut summary = None;
    for result_line in lines {
        match result_line {
            ResultsLine::Results(results) => {
                for AssociatedTestResults {
                    run_number,
                    results,
                    ..
                } in results
                {
                    for r in results {
                        collected.push((run_number, r.output.as_ref().unwrap().clone()));
                    }
                }
            }
            ResultsLine::Summary(s) => {
                let old = summary.replace(s);
                if old.is_some() {
                    panic!("multiple summaries received")
                }
            }
        }
    }

    collected.sort_unstable();

    (collected, summary.expect("summary not persisted"))
}

fn native_runner_simulation_bin() -> String {
    artifacts_dir()
        .join("abqtest_native_runner_simulation")
        .display()
        .to_string()
}

type GetConn<'a> = Box<dyn Fn() -> BoxFuture<'static, Box<dyn ClientStream>> + Sync + Send>;
type UseConn = Box<dyn Fn(GetConn, RunId) -> BoxFuture<'static, ()> + Sync + Send>;

enum Action {
    // TODO: consolidate start/stop workers by making worker pools async by default
    StartWorkers(Run, Wid, WorkersConfigBuilder),
    StopWorkers(Wid),
    CancelWorkers(Wid),

    WaitForCompletedRun(Run),
    WaitForNoPendingResults(Run),

    /// Make a connection to the work server, the callback will test a request.
    WSRunRequest(Run, UseConn),
}

type FlatResult<'a> = (WorkId, u32, &'a TestResult);

#[allow(clippy::type_complexity)]
enum Assert<'a> {
    /// Fetch the test results observed by the workers of a run.
    WorkerTestResults(Run, Box<dyn Fn(&[FlatResult<'_>]) -> bool>),
    /// Fetch the test results status observed by the queue.
    QueueTestResults(Run, Box<dyn Fn(TestResultsResponse)>),

    WorkersAreRedundant(Wid),
    WorkerExitStatus(Wid, Box<dyn Fn(&WorkersExitStatus)>),
    WorkerResultStatus(Wid, &'a dyn Fn(&WorkersExitKind)),

    RemoteManifest(Run, Box<dyn Fn(&ManifestView)>),
    RemoteResults(Run, Box<dyn Fn(&OpaqueLazyAssociatedTestResults)>),
}

type Steps<'a> = Vec<(Vec<Action>, Vec<Assert<'a>>)>;

#[derive(Default)]
struct TestBuilder<'a> {
    server: Server,
    steps: Steps<'a>,
}

impl<'a> TestBuilder<'a> {
    fn new(server: Server) -> Self {
        Self {
            server,
            ..Default::default()
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

    fn assert(mut self, asserts: impl IntoIterator<Item = Assert<'a>>) -> Self {
        self.steps.push((vec![], asserts.into_iter().collect()));
        self
    }

    async fn test(self) {
        run_test(self.server, self.steps).await
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
            } = workers_config_builder;

            let invoke_work = InvokeWork {
                run_id,
                batch_size_hint,
                // TODO: use a real value
                work_strategy: WorkStrategy::ByTest,
            };

            let config = WorkersConfig {
                local_results_handler: Box::new(StaticResultsHandler::new(run_results)),
                ..config
            };

            async move {
                let worker_pool = WorkersNegotiator::negotiate_and_start_pool(
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

            let workers_result = match panic::AssertUnwindSafe(worker_pool.shutdown())
                .catch_unwind()
                .await
            {
                Ok(exit) => WorkersExitKind::Exit(exit),
                Err(_) => WorkersExitKind::Panic,
            };
            worker_exit_kinds.lock().insert(n, workers_result);
        }
        .boxed(),

        CancelWorkers(worker_id) => async move {
            let mut worker_pool = workers.lock().await.remove(&worker_id).unwrap();

            worker_pool.cancel().await;
            let workers_result = match panic::AssertUnwindSafe(worker_pool.shutdown())
                .catch_unwind()
                .await
            {
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

        WaitForNoPendingResults(n) => {
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
                            message: queue::Message::TestResults(run_id.clone()),
                        },
                    )
                    .await
                    .unwrap();

                    use queue::TestResultsResponse::*;
                    match net_protocol::async_read(&mut conn).await.unwrap() {
                        Results { .. } => {
                            break;
                        }
                        _ => {
                            tokio::time::sleep(Duration::from_micros(100)).await;
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
                let get_conn: GetConn = Box::new(move || {
                    let client = client.boxed_clone();
                    async move { client.connect(work_scheduler_addr).await.unwrap() }.boxed()
                });
                callback(get_conn, run_id).await;
            }
            .boxed()
        }
    }
}

async fn run_test(server: Server, steps: Steps<'_>) {
    let Server {
        config,
        deps: _deps,
        remote,
    } = server;
    let mut queue = Abq::start(config).await;

    let client_opts =
        ClientOptions::new(ClientAuthStrategy::no_auth(), ClientTlsStrategy::no_tls());
    let client = client_opts.clone().build_async().unwrap();

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
                WorkerTestResults(n, check) => {
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

                QueueTestResults(n, check) => {
                    let run_id = run_ids.get(&n).unwrap().clone();
                    let mut conn = client.connect(queue.server_addr()).await.unwrap();
                    net_protocol::async_write(
                        &mut conn,
                        &net_protocol::queue::Request {
                            entity: Entity::local_client(),
                            message: net_protocol::queue::Message::TestResults(run_id),
                        },
                    )
                    .await
                    .unwrap();
                    let response = net_protocol::async_read(&mut conn).await.unwrap();
                    check(response)
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

                RemoteManifest(n, check) => {
                    let run_id = run_ids.get(&n).unwrap().clone();
                    let remote = remote.0.lock().await;
                    let manifest = remote.manifests.get(&run_id).unwrap();
                    check(manifest)
                }

                RemoteResults(n, check) => {
                    let run_id = run_ids.get(&n).unwrap().clone();
                    let remote = remote.0.lock().await;
                    let results = remote.results.get(&run_id).unwrap();
                    check(results)
                }
            }
        }
    }

    queue.shutdown().await.unwrap();
}

#[tokio::test]
#[traced_test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn multiple_jobs_complete() {
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
        .act([StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))])
        .step(
            [WaitForNoPendingResults(Run(1))],
            [
                WorkerTestResults(
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
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results, vec![(1, s!("echo1")), (1, s!("echo2"))]);
                            assert_eq!(summary.manifest_size_nonce, 2);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        // Check the remote
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 2);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(results, vec![(1, s!("echo1")), (1, s!("echo2"))]);
                    assert_eq!(summary.manifest_size_nonce, 2);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[traced_test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn multiple_worker_count() {
    let manifest = ManifestMessage::new(Manifest::new(
        [
            echo_test(proto, "echo1".to_string()),
            echo_test(proto, "echo2".to_string()),
            echo_test(proto, "echo3".to_string()),
            echo_test(proto, "echo4".to_string()),
            echo_test(proto, "echo5".to_string()),
        ],
        Default::default(),
    ));

    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, Box::new(manifest));

    let start_worker = |n| {
        StartWorkers(
            Run(73495),
            Wid(n),
            WorkersConfigBuilder::new(n as u32, runner.clone())
                .with_num_workers(NonZeroUsize::new(1).unwrap()),
        )
    };

    TestBuilder::default()
        .act([start_worker(4), start_worker(5), start_worker(6)])
        .act([
            StopWorkers(Wid(4)),
            StopWorkers(Wid(6)),
            StopWorkers(Wid(5)),
            WaitForCompletedRun(Run(73495)),
        ])
        .step(
            [WaitForNoPendingResults(Run(73495))],
            [
                WorkerTestResults(
                    Run(73495),
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
                            ]
                    }),
                ),
                WorkerExitStatus(
                    Wid(4),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                WorkerExitStatus(
                    Wid(6),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                WorkerExitStatus(
                    Wid(5),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                QueueTestResults(
                    Run(73495),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(
                                results,
                                vec![
                                    (1, s!("echo1")),
                                    (1, s!("echo2")),
                                    (1, s!("echo3")),
                                    (1, s!("echo4")),
                                    (1, s!("echo5"))
                                ]
                            );
                            assert_eq!(summary.manifest_size_nonce, 5);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .assert([
            RemoteManifest(
                Run(73495),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 5);
                }),
            ),
            RemoteResults(
                Run(73495),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(
                        results,
                        vec![
                            (1, s!("echo1")),
                            (1, s!("echo2")),
                            (1, s!("echo3")),
                            (1, s!("echo4")),
                            (1, s!("echo5"))
                        ]
                    );
                    assert_eq!(summary.manifest_size_nonce, 5);
                }),
            ),
        ])
        .test()
        .await;

    let log_message = format!(
        "marking end of manifest run_id={} worker_count={}",
        73495, 3
    );
    assert_scoped_log("abq_queue::queue", &log_message);
}

#[tokio::test]
#[traced_test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn multiple_invokers() {
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
        .act([
            StopWorkers(Wid(1)),
            StopWorkers(Wid(2)),
            WaitForCompletedRun(Run(1)),
            WaitForCompletedRun(Run(2)),
        ])
        .step(
            [
                WaitForNoPendingResults(Run(1)),
                WaitForNoPendingResults(Run(2)),
            ],
            [
                WorkerTestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results == [(1, "echo1"), (1, "echo2")]
                    }),
                ),
                //
                WorkerTestResults(
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
                //
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results, vec![(1, s!("echo1")), (1, s!("echo2"))]);
                            assert_eq!(summary.manifest_size_nonce, 2);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
                QueueTestResults(
                    Run(2),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(
                                results,
                                vec![(1, s!("echo3")), (1, s!("echo4")), (1, s!("echo5"))]
                            );
                            assert_eq!(summary.manifest_size_nonce, 3);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 2);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(results, vec![(1, s!("echo1")), (1, s!("echo2"))]);
                    assert_eq!(summary.manifest_size_nonce, 2);
                }),
            ),
            //
            RemoteManifest(
                Run(2),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 3);
                }),
            ),
            RemoteResults(
                Run(2),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(
                        results,
                        vec![(1, s!("echo3")), (1, s!("echo4")), (1, s!("echo5"))]
                    );
                    assert_eq!(summary.manifest_size_nonce, 3);
                }),
            ),
        ])
        .test()
        .await;
}

// TODO write some tests that smoke over # of workers and batch sizes
#[tokio::test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn batch_two_requests_at_a_time() {
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
        .act([StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))])
        .step(
            [WaitForNoPendingResults(Run(1))],
            [
                WorkerTestResults(
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
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(
                                results,
                                [
                                    (1, s!("echo1")),
                                    (1, s!("echo2")),
                                    (1, s!("echo3")),
                                    (1, s!("echo4"))
                                ]
                            );
                            assert_eq!(summary.manifest_size_nonce, 4);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 4);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(
                        results,
                        [
                            (1, s!("echo1")),
                            (1, s!("echo2")),
                            (1, s!("echo3")),
                            (1, s!("echo4"))
                        ]
                    );
                    assert_eq!(summary.manifest_size_nonce, 4);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
async fn empty_manifest_exits_gracefully() {
    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::Echo, empty_manifest_msg());

    TestBuilder::default()
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner),
        )])
        .act([StopWorkers(Wid(1)), WaitForCompletedRun(Run(1))])
        .step(
            [WaitForNoPendingResults(Run(1))],
            [
                WorkerTestResults(Run(1), Box::new(|results| results.is_empty())),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results, []);
                            assert_eq!(summary.manifest_size_nonce, 0);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .assert([RemoteResults(
            Run(1),
            Box::new(|results| {
                let (results, summary) = flatten_queue_results(results);
                assert_eq!(results, []);
                assert_eq!(summary.manifest_size_nonce, 0);
            }),
        )])
        .test()
        .await;
}

#[tokio::test]
#[traced_test]
#[with_protocol_version]
async fn get_init_context_from_work_server_waiting_for_first_worker() {
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
                async move {
                    use net_protocol::work_server::{Message, Request};

                    let mut conn = get_conn().await;

                    // Ask the server for the next test; we should be told a manifest is still TBD.
                    net_protocol::async_write(
                        &mut conn,
                        &Request {
                            entity: Entity::local_client(),
                            message: Message::InitContext { run_id },
                        },
                    )
                    .await
                    .unwrap();

                    let response: InitContextResponse =
                        net_protocol::async_read(&mut conn).await.unwrap();

                    assert!(matches!(response, InitContextResponse::WaitingForManifest));
                }
                .boxed()
            }),
        )])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
async fn get_init_context_from_work_server_waiting_for_manifest() {
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
                async move {
                    use net_protocol::work_server::{Message, Request};

                    let mut conn = get_conn().await;

                    // Ask the server for the next test; we should be told a manifest is still TBD.
                    net_protocol::async_write(
                        &mut conn,
                        &Request {
                            entity: Entity::local_client(),
                            message: Message::InitContext { run_id },
                        },
                    )
                    .await
                    .unwrap();

                    let response: InitContextResponse =
                        net_protocol::async_read(&mut conn).await.unwrap();

                    assert!(matches!(response, InitContextResponse::WaitingForManifest));
                }
                .boxed()
            }),
        )])
        .act([StopWorkers(Wid(1))])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
async fn get_init_context_from_work_server_active() {
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
                let expected_init_meta = expected_init_meta.clone();
                async move {
                    use net_protocol::work_server::{Message, Request};

                    // Ask the server for the work context, it should align with the init_meta we gave
                    // to begin with.
                    loop {
                        let mut conn = get_conn().await;

                        net_protocol::async_write(
                            &mut conn,
                            &Request {
                                entity: Entity::local_client(),
                                message: Message::InitContext {
                                    run_id: run_id.clone(),
                                },
                            },
                        )
                        .await
                        .unwrap();

                        match net_protocol::async_read(&mut conn).await.unwrap() {
                            InitContextResponse::WaitingForManifest => continue,
                            InitContextResponse::InitContext(InitContext { init_meta }) => {
                                assert_eq!(init_meta, expected_init_meta);
                                return;
                            }
                            InitContextResponse::RunAlreadyCompleted { .. } => unreachable!(),
                        }
                    }
                }
                .boxed()
            }),
        )])
        .act([StopWorkers(Wid(1))])
        .test()
        .await;
}

#[tokio::test]
#[traced_test]
#[with_protocol_version]
async fn get_init_context_after_run_already_completed() {
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
                async move {
                    use net_protocol::work_server::{Message, Request};

                    let mut conn = get_conn().await;

                    // Ask the server for the work context, we should be told the run is already done.
                    net_protocol::async_write(
                        &mut conn,
                        &Request {
                            entity: Entity::local_client(),
                            message: Message::InitContext { run_id },
                        },
                    )
                    .await
                    .unwrap();

                    let response: InitContextResponse =
                        net_protocol::async_read(&mut conn).await.unwrap();

                    assert!(matches!(
                        response,
                        InitContextResponse::RunAlreadyCompleted { cancelled: false }
                    ));
                }
                .boxed()
            }),
        )])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
async fn getting_run_after_work_is_complete_returns_nothing() {
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
            [
                StopWorkers(Wid(1)),
                WaitForCompletedRun(Run(1)),
                WaitForNoPendingResults(Run(1)),
            ],
            [
                WorkerTestResults(
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
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results, [(1, s!("echo1")), (1, s!("echo2"))]);
                            assert_eq!(summary.manifest_size_nonce, 2);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
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
                WaitForNoPendingResults(Run(1)),
            ],
            [
                WorkerExitStatus(
                    Wid(2),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                // Observed test results should not change.
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results, [(1, s!("echo1")), (1, s!("echo2"))]);
                            assert_eq!(summary.manifest_size_nonce, 2);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        // Remote should also see the same results.
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 2);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(results, [(1, s!("echo1")), (1, s!("echo2"))]);
                    assert_eq!(summary.manifest_size_nonce, 2);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
async fn test_cancellation_drops_remaining_work() {
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
            WorkersConfigBuilder::new(1, runner.clone()).with_num_workers(one_nonzero_usize()),
        )])
        .step(
            [
                CancelWorkers(Wid(1)),
                // Run should now be seen as completed
                WaitForCompletedRun(Run(1)),
            ],
            [
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::Completed(ExitCode::CANCELLED))),
                ),
                // Queue should see a cancelled run as it relates to the test results.
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Error(s) => {
                            assert!(s.contains("cancelled"));
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
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
            [
                WorkerExitStatus(
                    Wid(2),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::Completed(ExitCode::CANCELLED))),
                ),
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Error(s) => {
                            assert!(s.contains("cancelled"));
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .test()
        .await;
}

#[tokio::test]
async fn failure_to_run_worker_command_exits_gracefully() {
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
            [
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert!(matches!(e, WorkersExitStatus::Error { .. }))),
                ),
                // Queue should say that no results could be provided because manifest couldn't be
                // generated.
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Error(s) => {
                            assert!(s.contains("manifest failed to be generated"), "{s:?}");
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .test()
        .await;
}

#[tokio::test]
async fn native_runner_fails_due_to_manifest_failure() {
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
            [
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert!(matches!(e, WorkersExitStatus::Error { .. }))),
                ),
                // Queue should say that no results could be provided because manifest couldn't be
                // generated.
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Error(s) => {
                            assert!(s.contains("manifest failed to be generated"), "{s:?}");
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn multiple_tests_per_work_id_reported() {
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
        .act([
            StopWorkers(Wid(1)),
            // Run should be seen as completed
            WaitForCompletedRun(Run(1)),
        ])
        .step(
            [WaitForNoPendingResults(Run(1))],
            [
                WorkerTestResults(
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
                QueueTestResults(
                    Run(1),
                    Box::new(|resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(
                                vec![
                                    (1, s!("echo1")),
                                    (1, s!("echo2")),
                                    (1, s!("echo3")),
                                    (1, s!("echo4")),
                                    (1, s!("echo5")),
                                    (1, s!("echo6")),
                                ],
                                results
                            );
                            assert_eq!(summary.manifest_size_nonce, 2);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 2);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(
                        vec![
                            (1, s!("echo1")),
                            (1, s!("echo2")),
                            (1, s!("echo3")),
                            (1, s!("echo4")),
                            (1, s!("echo5")),
                            (1, s!("echo6")),
                        ],
                        results
                    );
                    assert_eq!(summary.manifest_size_nonce, 2);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[timeout(1500)] // 1.5 second
async fn runner_panic_stops_worker() {
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
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[serial]
#[timeout(2000)]
async fn many_retries_complete() {
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
    let expected_worker_results = expected_results.clone();
    let expected_queue_results = expected_results;

    TestBuilder::default()
        .act([StartWorkers(Run(1), Wid(1), workers_config)])
        .step(
            [
                StopWorkers(Wid(1)),
                // Run should be seen as completed
                WaitForCompletedRun(Run(1)),
                WaitForNoPendingResults(Run(1)),
            ],
            [
                WorkerTestResults(
                    Run(1),
                    Box::new(move |results| {
                        let mut results = results.to_vec();
                        let results = sort_results_owned(&mut results);
                        results == expected_worker_results
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                QueueTestResults(
                    Run(1),
                    Box::new({
                        let expected_queue_results = expected_queue_results.clone();
                        move |resp| match resp {
                            TestResultsResponse::Results {
                                chunk,
                                final_chunk: true,
                            } => {
                                let (results, summary) = flatten_queue_results(chunk);
                                assert_eq!(results, expected_queue_results);
                                assert_eq!(summary.manifest_size_nonce, 4);
                            }
                            _ => unreachable!("{resp:?}"),
                        }
                    }),
                ),
            ],
        )
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 4);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(move |results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(results, expected_queue_results);
                    assert_eq!(summary.manifest_size_nonce, 4);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[serial]
#[timeout(2000)]
async fn many_retries_many_workers_complete() {
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
    let expected_workers_results = expected_results.clone();
    let expected_queue_results = expected_results;

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
    end_workers_actions.extend([
        // Run should be seen as completed
        WaitForCompletedRun(Run(1)),
        // Wait for all results to be flushed
        WaitForNoPendingResults(Run(1)),
    ]);
    end_workers_asserts.push(WorkerTestResults(
        Run(1),
        Box::new(move |results| {
            let mut results = results.to_vec();
            let results = sort_results_owned(&mut results);
            results == expected_workers_results
        }),
    ));
    end_workers_asserts.push({
        let expected_queue_results = expected_queue_results.clone();
        QueueTestResults(
            Run(1),
            Box::new(move |resp| match resp {
                TestResultsResponse::Results {
                    chunk,
                    final_chunk: true,
                } => {
                    let (results, summary) = flatten_queue_results(chunk);
                    assert_eq!(results, expected_queue_results);
                    assert_eq!(summary.manifest_size_nonce, num_tests);
                }
                _ => unreachable!("{resp:?}"),
            }),
        )
    });

    let remote_asserts = [
        RemoteManifest(
            Run(1),
            Box::new(move |manifest| {
                assert_eq!(manifest.len(), num_tests as usize);
            }),
        ),
        RemoteResults(
            Run(1),
            Box::new(move |results| {
                let (results, summary) = flatten_queue_results(results);
                assert_eq!(results, expected_queue_results);
                assert_eq!(summary.manifest_size_nonce, num_tests);
            }),
        ),
    ];

    TestBuilder::default()
        .act(start_actions)
        .step(end_workers_actions, end_workers_asserts)
        .assert(remote_asserts)
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[serial]
#[timeout(3000)]
async fn many_retries_many_workers_complete_native() {
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
    let expected_workers_results = expected_results.clone();
    let expected_queue_results = expected_results;

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

    end_workers_actions.extend([
        // Run should be seen as completed
        WaitForCompletedRun(Run(1)),
        // Wait for all results to be flushed
        WaitForNoPendingResults(Run(1)),
    ]);

    TestBuilder::default()
        .act(start_actions)
        .step(
            end_workers_actions,
            [
                WorkerTestResults(
                    Run(1),
                    Box::new(move |results| {
                        let mut results = results.to_vec();
                        let results = sort_results_owned(&mut results);
                        results == expected_workers_results
                    }),
                ),
                QueueTestResults(
                    Run(1),
                    Box::new({
                        let expected_queue_results = expected_queue_results.clone();
                        move |resp| match resp {
                            TestResultsResponse::Results {
                                chunk,
                                final_chunk: true,
                            } => {
                                let (results, summary) = flatten_queue_results(chunk);
                                assert_eq!(results, expected_queue_results);
                                assert_eq!(summary.manifest_size_nonce, num_tests);
                            }
                            _ => unreachable!("{resp:?}"),
                        }
                    }),
                ),
            ],
        )
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(move |manifest| {
                    assert_eq!(manifest.len(), num_tests as usize);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(move |results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(results, expected_queue_results);
                    assert_eq!(summary.manifest_size_nonce, num_tests);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[serial]
#[timeout(2000)] // 2 seconds
async fn cancellation_native() {
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
                WorkerTestResults(Run(1), Box::new(|results| results.is_empty())),
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
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn retry_out_of_process_worker() {
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
            [
                StopWorkers(Wid(1)),
                WaitForCompletedRun(Run(1)),
                WaitForNoPendingResults(Run(1)),
            ],
            [
                WorkerTestResults(
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
                QueueTestResults(
                    Run(1),
                    Box::new(move |resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results, [(1, s!("echo1")), (1, s!("echo2"))]);
                            assert_eq!(summary.manifest_size_nonce, 2);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
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
                WorkerTestResults(
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
        .step(
            [WaitForNoPendingResults(Run(1))],
            [QueueTestResults(
                Run(1),
                Box::new(move |resp| match resp {
                    TestResultsResponse::Results {
                        chunk,
                        final_chunk: true,
                    } => {
                        let (results, summary) = flatten_queue_results(chunk);
                        assert_eq!(
                            results,
                            [
                                (1, s!("echo1")),
                                (1, s!("echo1")),
                                (1, s!("echo2")),
                                (1, s!("echo2"))
                            ]
                        );
                        assert_eq!(summary.manifest_size_nonce, 2);
                    }
                    _ => unreachable!("{resp:?}"),
                }),
            )],
        )
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 2);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(
                        results,
                        [
                            (1, s!("echo1")),
                            (1, s!("echo1")),
                            (1, s!("echo2")),
                            (1, s!("echo2"))
                        ]
                    );
                    assert_eq!(summary.manifest_size_nonce, 2);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn many_retries_of_many_out_of_process_workers() {
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
        // Steps that the workers should take
        let mut end_workers_actions = vec![];
        let mut end_workers_asserts = vec![];
        // Steps that we should check regarding the state of the queue, after the workers are
        // complete. Needs to be run separately, because all worker tasks will be run concurrently
        // with the queue checks otherwise!
        let mut end_run_actions = vec![];
        let mut end_run_asserts = vec![];

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
        end_run_actions.extend([WaitForCompletedRun(run), WaitForNoPendingResults(run)]);

        // The number of expected results is now the results * how many retries we had
        let mut expected_results: Vec<_> = std::iter::repeat(expected_results_one_pass.clone())
            .take(retry)
            .flatten()
            .collect();
        expected_results.sort();

        let expected_workers_results = expected_results.clone();
        let expected_queue_results = expected_results;

        end_run_asserts.extend([
            WorkerTestResults(
                run,
                Box::new(move |results| {
                    let mut results = results.to_vec();
                    let results = sort_results_owned(&mut results);
                    results == expected_workers_results
                }),
            ),
            {
                let expected_queue_results = expected_queue_results.clone();
                QueueTestResults(
                    run,
                    Box::new(move |resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results, expected_queue_results);
                            assert_eq!(summary.manifest_size_nonce, num_tests);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                )
            },
        ]);

        let remote_asserts = [
            RemoteManifest(
                Run(1),
                Box::new(move |manifest| {
                    assert_eq!(manifest.len(), num_tests as usize);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(move |results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(results, expected_queue_results);
                    assert_eq!(summary.manifest_size_nonce, num_tests);
                }),
            ),
        ];

        // Chain on the test, then do the next retry iteration.
        builder = builder
            .act(start_actions)
            .step(end_workers_actions, end_workers_asserts)
            .step(end_run_actions, end_run_asserts)
            .assert(remote_asserts);
    }

    builder.test().await;
}

#[tokio::test]
#[with_protocol_version]
#[timeout(1000)] // 1 second
async fn cancellation_of_out_of_process_retry_does_not_cancel_run() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));

    use abq_native_runner_simulation::Msg::*;

    let sim1 = pack_msgs_to_disk([
        Connect,
        OpaqueWrite(pack(legal_spawned_message(proto))),
        IfGenerateManifest {
            then_do: vec![OpaqueWrite(pack(&manifest))],
            else_do: vec![
                //
                // Read init context message + write ACK
                OpaqueRead,
                OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                //
                // If we got a fast-exit message, die; otherwise, read + write the test result
                IfOpaqueReadSocketAlive {
                    then_do: vec![OpaqueWrite(pack(RawTestResultMessage::fake(proto)))],
                    else_do: vec![Exit(0)],
                },
            ],
        },
        Exit(0),
    ]);
    let runner1 = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![sim1.path.display().to_string()],
        extra_env: Default::default(),
    });

    // Second and third attempt of the runner produces no manifest,
    // and should be fed our retry manifest from the first pass.
    let sim2 = pack_msgs_to_disk([
        Connect,
        OpaqueWrite(pack(legal_spawned_message(proto))),
        IfGenerateManifest {
            then_do: vec![OpaqueWrite(pack(&manifest))],
            else_do: vec![
                //
                // Read init context message + write ACK
                OpaqueRead,
                OpaqueWrite(pack(InitSuccessMessage::new(proto))),
                //
                // Send result
                OpaqueRead,
                Sleep(Duration::from_secs(600)),
            ],
        },
        Exit(0),
    ]);
    let runner2 = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![sim2.path.display().to_string()],
        extra_env: Default::default(),
    });
    let runner3 = RunnerKind::GenericNativeTestRunner(NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![sim1.path.display().to_string()],
        extra_env: Default::default(),
    });

    let two = NonZeroUsize::try_from(2).unwrap();

    TestBuilder::default()
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner1).with_num_workers(two),
        )])
        .act([
            StopWorkers(Wid(1)),
            // Run should now be seen as completed
            WaitForCompletedRun(Run(1)),
        ])
        .step(
            [WaitForNoPendingResults(Run(1))],
            [
                WorkerTestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results.len() == 1
                    }),
                ),
                WorkerExitStatus(
                    Wid(1),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                QueueTestResults(
                    Run(1),
                    Box::new(move |resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results.len(), 1);
                            assert_eq!(summary.manifest_size_nonce, 1);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        // Second attempt of the run will be cancelled, but that shouldn't impact the run.
        .act([StartWorkers(
            Run(1),
            Wid(2),
            WorkersConfigBuilder::new(1, runner2).with_num_workers(two),
        )])
        .act([CancelWorkers(Wid(2)), WaitForCompletedRun(Run(1))])
        .step(
            [WaitForNoPendingResults(Run(1))],
            [
                WorkerTestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results.len() == 1
                    }),
                ),
                WorkerExitStatus(
                    Wid(2),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::Completed(ExitCode::CANCELLED))),
                ),
                QueueTestResults(
                    Run(1),
                    Box::new(move |resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results.len(), 1);
                            assert_eq!(summary.manifest_size_nonce, 1);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        // Third attempt should succeed
        .act([StartWorkers(
            Run(1),
            Wid(3),
            WorkersConfigBuilder::new(1, runner3).with_num_workers(two),
        )])
        .act([StopWorkers(Wid(3)), WaitForCompletedRun(Run(1))])
        .step(
            [WaitForNoPendingResults(Run(1))],
            [
                WorkerTestResults(
                    Run(1),
                    Box::new(|results| {
                        let mut results = results.to_vec();
                        let results = sort_results(&mut results);
                        results.len() == 2
                    }),
                ),
                WorkerExitStatus(
                    Wid(3),
                    Box::new(|e| assert_eq!(e, &WorkersExitStatus::SUCCESS)),
                ),
                QueueTestResults(
                    Run(1),
                    Box::new(move |resp| match resp {
                        TestResultsResponse::Results {
                            chunk,
                            final_chunk: true,
                        } => {
                            let (results, summary) = flatten_queue_results(chunk);
                            assert_eq!(results.len(), 2);
                            assert_eq!(summary.manifest_size_nonce, 1);
                        }
                        _ => unreachable!("{resp:?}"),
                    }),
                ),
            ],
        )
        .assert([
            RemoteManifest(
                Run(1),
                Box::new(|manifest| {
                    assert_eq!(manifest.len(), 1);
                }),
            ),
            RemoteResults(
                Run(1),
                Box::new(|results| {
                    let (results, summary) = flatten_queue_results(results);
                    assert_eq!(results.len(), 2);
                    assert_eq!(summary.manifest_size_nonce, 1);
                }),
            ),
        ])
        .test()
        .await;
}

#[tokio::test]
#[with_protocol_version]
#[traced_test]
async fn cancel_test_run_if_no_manifest_progress() {
    let manifest = ManifestMessage::new(Manifest::new(
        [echo_test(proto, "echo1".to_string())],
        Default::default(),
    ));
    let runner = RunnerKind::TestLikeRunner(TestLikeRunner::HangOnTestStart, Box::new(manifest));

    let server = {
        fn timeout_zero(_reason: TimeoutReason) -> Duration {
            Duration::ZERO
        }
        Server::default().with_timeout_strategy(RunTimeoutStrategy::constant(timeout_zero))
    };

    TestBuilder::new(server)
        // First workers - let them go, the queue should cancel the run soon.
        .act([StartWorkers(
            Run(1),
            Wid(1),
            WorkersConfigBuilder::new(1, runner.clone()).with_num_workers(one_nonzero_usize()),
        )])
        .act([
            // Run should be seen as completed (cancelled) soon.
            WaitForCompletedRun(Run(1)),
        ])
        // If we start another set of workers, they should exit as cancelled immediately.
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
        .test()
        .await;

    assert_scoped_log(
        "abq_queue::queue",
        "run cancelled because manifest made no progress",
    );
}
