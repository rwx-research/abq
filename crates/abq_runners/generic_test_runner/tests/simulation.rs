use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use abq_generic_test_runner::{
    noop_notify_cancellation, GenericRunnerError, GetNextTests, ImmediateTests, SendManifest,
    StaticGetInitContext, StaticManifestCollector, DEFAULT_PROTOCOL_VERSION_TIMEOUT,
    DEFAULT_RUNNER_TEST_TIMEOUT,
};
use abq_native_runner_simulation::{pack, pack_msgs, Msg};
use abq_test_utils::{artifacts_dir, sanitize_output};
use abq_utils::capture_output::{CaptureChildOutputStrategy, ProcessOutput};
use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::RunnerMeta;
use abq_utils::net_protocol::queue::{AssociatedTestResults, TestSpec};
use abq_utils::net_protocol::runners::{
    InitSuccessMessage, Manifest, ManifestMessage, NativeRunnerSpecification, ProtocolWitness,
    RawNativeRunnerSpawnedMessage, RawTestResultMessage, StdioOutput, Test, TestCase, TestOrGroup,
    TestRunnerExit,
};
use abq_utils::net_protocol::work_server::InitContext;
use abq_utils::net_protocol::workers::{
    Eow, ManifestResult, NativeTestRunnerParams, NextWorkBundle, WorkId, WorkerTest,
    INIT_RUN_NUMBER,
};
use abq_utils::oneshot_notify::OneshotTx;
use abq_utils::results_handler::{SharedAssociatedTestResults, StaticResultsHandler};
use abq_utils::{atomic, oneshot_notify};

use abq_with_protocol_version::with_protocol_version;
use futures::FutureExt;
use parking_lot::Mutex;
use tempfile::{NamedTempFile, TempPath};

fn native_runner_simulation_bin() -> String {
    artifacts_dir()
        .join("abqtest_native_runner_simulation")
        .display()
        .to_string()
}

fn legal_spawned_message(proto: ProtocolWitness) -> RawNativeRunnerSpawnedMessage {
    let protocol_version = proto.get_version();
    let runner_specification = NativeRunnerSpecification {
        name: "test".to_string(),
        version: "0.0.0".to_string(),
        test_framework: "rspec".to_owned(),
        test_framework_version: "3.12.0".to_owned(),
        language: "ruby".to_owned(),
        language_version: "3.1.2p20".to_owned(),
        host: "ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned(),
    };
    RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification)
}

struct RunnerState {
    _simfile: TempPath,
    shutdown: OneshotTx,
    all_results: SharedAssociatedTestResults,
    all_tests_run: Arc<AtomicBool>,
}

fn get_simulated_runner(
    simulation: impl IntoIterator<Item = Msg>,
    with_manifest: Option<SendManifest>,
    get_next_test: GetNextTests,
) -> (
    impl Future<Output = abq_generic_test_runner::Outcome>,
    RunnerState,
) {
    let simulation_msg = pack_msgs(simulation);
    let simfile = NamedTempFile::new().unwrap().into_temp_path();
    let simfile_path = simfile.to_path_buf();
    std::fs::write(&simfile_path, simulation_msg).unwrap();

    let input = NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![simfile_path.display().to_string()],
        extra_env: Default::default(),
    };

    let all_results: SharedAssociatedTestResults = Default::default();
    let all_tests_run = Arc::new(AtomicBool::new(false));

    let get_init_context = StaticGetInitContext::new(Ok(InitContext {
        init_meta: Default::default(),
    }));
    let results_handler = Box::new(StaticResultsHandler::new(all_results.clone()));

    let notify_all_tests_run = {
        let all_run = all_tests_run.clone();
        move || {
            async move {
                all_run.store(true, atomic::ORDERING);
            }
            .boxed()
        }
    };

    let (shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();

    let child_output_strategy = CaptureChildOutputStrategy::new(true);

    let runner_task = abq_generic_test_runner::run_async(
        RunnerMeta::fake(),
        input,
        DEFAULT_PROTOCOL_VERSION_TIMEOUT,
        DEFAULT_RUNNER_TEST_TIMEOUT,
        std::env::current_dir().unwrap(),
        shutdown_rx,
        5,
        with_manifest,
        Box::new(get_init_context),
        get_next_test,
        results_handler,
        Box::new(notify_all_tests_run),
        noop_notify_cancellation(),
        Box::new(child_output_strategy),
        false,
    );

    let state = RunnerState {
        _simfile: simfile,
        shutdown: shutdown_tx,
        all_results,
        all_tests_run,
    };

    (runner_task, state)
}

fn run_simulated_runner(
    simulation: impl IntoIterator<Item = Msg>,
    with_manifest: Option<SendManifest>,
    get_next_test: GetNextTests,
) -> (
    Vec<AssociatedTestResults>,
    Option<ProcessOutput>,
    StdioOutput,
    bool,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let (runner_task, state) = get_simulated_runner(simulation, with_manifest, get_next_test);

    let runner_result = rt.block_on(runner_task);

    let TestRunnerExit {
        final_stdio_output,
        manifest_generation_output,
        exit_code,
        ..
    } = runner_result.unwrap();

    assert_eq!(exit_code, ExitCode::SUCCESS);

    (
        Arc::try_unwrap(state.all_results)
            .expect("outstanding refs to all results")
            .into_inner(),
        manifest_generation_output,
        final_stdio_output,
        state.all_tests_run.load(atomic::ORDERING),
    )
}

fn run_simulated_runner_to_error(
    simulation: impl IntoIterator<Item = Msg>,
    with_manifest: Option<SendManifest>,
    get_next_test: GetNextTests,
) -> (GenericRunnerError, Vec<AssociatedTestResults>, bool) {
    let simulation_msg = pack_msgs(simulation);
    let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let simfile_path = simfile.to_path_buf();
    std::fs::write(&simfile_path, simulation_msg).unwrap();

    let input = NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![simfile_path.display().to_string()],
        extra_env: Default::default(),
    };

    let get_init_context = StaticGetInitContext::new(Ok(InitContext {
        init_meta: Default::default(),
    }));
    let all_results: Arc<Mutex<Vec<AssociatedTestResults>>> = Default::default();

    let results_handler = Box::new(StaticResultsHandler::new(all_results.clone()));

    let all_test_run = Arc::new(AtomicBool::new(false));
    let notify_all_tests_run = {
        let all_run = all_test_run.clone();
        move || {
            async move {
                all_run.store(true, atomic::ORDERING);
            }
            .boxed()
        }
    };

    let (_shutdown_tx, shutdown_rx) = oneshot_notify::make_pair();

    let child_output_strategy = CaptureChildOutputStrategy::new(true);

    let err = abq_generic_test_runner::run_sync(
        RunnerMeta::fake(),
        input,
        DEFAULT_PROTOCOL_VERSION_TIMEOUT,
        DEFAULT_RUNNER_TEST_TIMEOUT,
        std::env::current_dir().unwrap(),
        shutdown_rx,
        5,
        with_manifest,
        Box::new(get_init_context),
        get_next_test,
        results_handler,
        Box::new(notify_all_tests_run),
        noop_notify_cancellation(),
        Box::new(child_output_strategy),
        false,
    )
    .unwrap_err();

    (
        err,
        Arc::try_unwrap(all_results)
            .expect("outstanding refs to all results")
            .into_inner(),
        all_test_run.load(atomic::ORDERING),
    )
}

fn debug_bytes(bytes: &[u8]) -> impl std::fmt::Display + '_ {
    String::from_utf8_lossy(bytes)
}

macro_rules! check_bytes {
    ($out:expr, $expected:expr) => {
        assert_eq!($out, $expected, "{}", debug_bytes($out));
    };
}

#[test]
#[with_protocol_version]
fn capture_output_before_and_during_tests() {
    use Msg::*;

    let simulation = [
        Connect,
        // Write out stdout/stderr
        Stdout(b"stdout1".to_vec()),
        Stderr(b"stderr1".to_vec()),
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Read init context message + write ACK
        OpaqueRead,
        OpaqueWrite(pack(InitSuccessMessage::new(proto))),
        //
        // Read, write first test
        OpaqueRead,
        Stdout(b"stdout-test1".to_vec()),
        Stderr(b"stderr-test1".to_vec()),
        OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
        //
        // Read, write second test
        OpaqueRead,
        Stdout(b"stdout-test2".to_vec()),
        Stderr(b"stderr-test2".to_vec()),
        OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
        //
        // Output after all tests
        Stdout(b"stdout-after".to_vec()),
        Stderr(b"stderr-after".to_vec()),
        //
        // Finish
        Exit(0),
    ];

    fn work_bundle(proto: ProtocolWitness) -> NextWorkBundle {
        NextWorkBundle::new(
            [
                WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "test1", Default::default()),
                        work_id: WorkId([1; 16]),
                    },
                    run_number: INIT_RUN_NUMBER,
                },
                WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "test2", Default::default()),
                        work_id: WorkId([2; 16]),
                    },
                    run_number: INIT_RUN_NUMBER,
                },
            ],
            Eow(true),
        )
    }
    let get_next_tests = ImmediateTests::new(vec![work_bundle(proto)]);

    let (mut results, _man_output, final_captures, notified_all_run) =
        run_simulated_runner(simulation, None, Box::new(get_next_tests));

    assert!(notified_all_run);

    results.sort_by_key(|r| r.work_id);

    // Unfortunately we cannot force a guarantee that all stdout/stderr in the child ends up in the
    // parent pipe during the time that we read from the pipe. This is because pipes are
    // asynchronous and need not be populated exactly as we would like at the time a message is
    // written to an output stream.
    //
    // Instead, check that we capture the expected stdout/stderr exhaustively.
    let mut all_stdout: Vec<u8> = vec![];
    let mut all_stderr: Vec<u8> = vec![];

    assert_eq!(results.len(), 2);
    {
        let AssociatedTestResults {
            work_id,
            run_number: _,
            results,
            before_any_test,
            after_all_tests,
        } = &results[0];
        assert_eq!(work_id.0, [1; 16]);

        all_stdout.extend(&before_any_test.stdout);
        all_stderr.extend(&before_any_test.stderr);

        assert_eq!(results.len(), 1);
        all_stdout.extend(results[0].stdout.as_deref().unwrap_or_default());
        all_stderr.extend(results[0].stderr.as_deref().unwrap_or_default());

        assert!(after_all_tests.is_none());
    }
    {
        let AssociatedTestResults {
            work_id,
            run_number: _,
            results,
            before_any_test,
            after_all_tests,
        } = &results[1];
        assert_eq!(work_id.0, [2; 16]);

        all_stdout.extend(&before_any_test.stdout);
        all_stderr.extend(&before_any_test.stderr);

        assert_eq!(results.len(), 1);
        all_stdout.extend(results[0].stdout.as_deref().unwrap_or_default());
        all_stderr.extend(results[0].stderr.as_deref().unwrap_or_default());

        assert!(after_all_tests.is_none());
    }

    all_stdout.extend(final_captures.stdout);
    all_stderr.extend(final_captures.stderr);

    check_bytes!(&all_stdout, b"stdout1stdout-test1stdout-test2stdout-after");
    check_bytes!(&all_stderr, b"stderr1stderr-test1stderr-test2stderr-after");
}

#[test]
#[with_protocol_version]
fn big_manifest() {
    use Msg::*;

    let test_name = "y".repeat(1_000);
    let members = std::iter::repeat_with(|| {
        TestOrGroup::test(Test::new(
            proto,
            test_name.clone(),
            vec![],
            Default::default(),
        ))
    })
    .take(10_000);

    let huge_manifest = ManifestMessage::new(Manifest::new(members, Default::default()));

    assert!(abq_utils::net_protocol::is_large_message(&huge_manifest).unwrap());

    let simulation = [
        Connect,
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Write the manifest if we need to.
        // Otherwise we should get no requests for tests.
        IfGenerateManifest {
            then_do: vec![OpaqueWrite(pack(&huge_manifest))],
            else_do: vec![
                //
                // Read init context message + write ACK
                OpaqueRead,
                OpaqueWrite(pack(InitSuccessMessage::new(proto))),
            ],
        },
        //
        // Finish
        Exit(0),
    ];

    let manifest: Arc<Mutex<Option<ManifestResult>>> = Default::default();
    let with_manifest = StaticManifestCollector::new(manifest.clone());
    let get_next_tests = ImmediateTests::new(vec![NextWorkBundle::new([], Eow(true))]);

    let (results, _man_output, _, notified_all_ran) = run_simulated_runner(
        simulation,
        Some(Box::new(with_manifest)),
        Box::new(get_next_tests),
    );

    assert!(notified_all_ran);

    assert!(results.is_empty());

    let manifest = Arc::try_unwrap(manifest).unwrap().into_inner().unwrap();
    let manifest = match manifest {
        ManifestResult::Manifest(man) => man.manifest,
        ManifestResult::TestRunnerError { .. } => unreachable!(),
    };
    assert_eq!(Manifest::flatten_manifest(manifest.members).len(), 10_000);
}

#[test]
#[with_protocol_version]
fn capture_output_during_manifest_gen() {
    use Msg::*;

    let test_name = "y".repeat(10);
    let members = std::iter::repeat_with(|| {
        TestOrGroup::test(Test::new(
            proto,
            test_name.clone(),
            vec![],
            Default::default(),
        ))
    })
    .take(10);

    let manifest = ManifestMessage::new(Manifest::new(members, Default::default()));

    let simulation = [
        Connect,
        Stdout(b"init stdout".to_vec()),
        Stderr(b"init stderr".to_vec()),
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Write the manifest if we need to.
        // Otherwise we should get no requests for tests.
        IfGenerateManifest {
            then_do: vec![
                Stdout(b"hello from manifest stdout".to_vec()),
                Stderr(b"hello from manifest stderr".to_vec()),
                OpaqueWrite(pack(&manifest)),
            ],
            else_do: vec![
                //
                // Read init context message + write ACK
                OpaqueRead,
                OpaqueWrite(pack(InitSuccessMessage::new(proto))),
            ],
        },
        //
        // Finish
        Exit(0),
    ];

    let manifest: Arc<Mutex<Option<ManifestResult>>> = Default::default();
    let with_manifest = StaticManifestCollector::new(manifest.clone());
    let get_next_tests = ImmediateTests::new(vec![NextWorkBundle::new([], Eow(true))]);

    let (results, man_output, _, notified_all_ran) = run_simulated_runner(
        simulation,
        Some(Box::new(with_manifest)),
        Box::new(get_next_tests),
    );

    assert!(notified_all_ran);

    assert!(results.is_empty());

    assert!(man_output.is_some());

    let man_output = man_output.unwrap();
    let man_output = String::from_utf8_lossy(&man_output);
    // The output isn't strictly ordered between stdout and stderr.
    assert!(man_output.contains("init stdout"), "output:\n{man_output}");
    assert!(man_output.contains("init stderr"), "output:\n{man_output}");
    assert!(
        man_output.contains("hello from manifest stdout"),
        "output:\n{man_output}"
    );
    assert!(
        man_output.contains("hello from manifest stderr"),
        "output:\n{man_output}"
    );

    let manifest = Arc::try_unwrap(manifest).unwrap().into_inner().unwrap();
    let manifest = match manifest {
        ManifestResult::Manifest(man) => man.manifest,
        ManifestResult::TestRunnerError { .. } => unreachable!(),
    };
    assert_eq!(Manifest::flatten_manifest(manifest.members).len(), 10);
}

#[test]
#[with_protocol_version]
fn native_runner_respawn_for_higher_run_numbers() {
    use Msg::*;

    // Create a work bundle that should require three launches of the native runner.
    fn work_bundle(proto: ProtocolWitness) -> NextWorkBundle {
        NextWorkBundle::new(
            vec![
                WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "test1", Default::default()),
                        work_id: WorkId([1; 16]),
                    },
                    run_number: INIT_RUN_NUMBER,
                },
                WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "test2", Default::default()),
                        work_id: WorkId([2; 16]),
                    },
                    run_number: INIT_RUN_NUMBER + 1,
                },
                WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "test3", Default::default()),
                        work_id: WorkId([3; 16]),
                    },
                    run_number: INIT_RUN_NUMBER + 2,
                },
            ],
            Eow(true),
        )
    }
    let get_next_tests = ImmediateTests::new(vec![work_bundle(proto)]);

    let simulation = [
        Connect,
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Read init context message + write ACK
        OpaqueRead,
        OpaqueWrite(pack(InitSuccessMessage::new(proto))),
        //
        // Read, write one test. We should only see one at a time, since the native runner should
        // be re-launched.
        OpaqueRead,
        OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
        //
        // Finish
        Exit(0),
    ];

    let (mut results, _, _, _) = run_simulated_runner(simulation, None, Box::new(get_next_tests));

    assert_eq!(results.len(), 3);
    results.sort_by_key(|r| r.work_id);

    assert_eq!(results[0].work_id.0, [1; 16]);
    assert_eq!(results[1].work_id.0, [2; 16]);
    assert_eq!(results[2].work_id.0, [3; 16]);
}

#[test]
#[with_protocol_version]
fn native_runner_fails_while_executing_tests() {
    use Msg::*;

    let simulation = [
        Connect,
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Read init context message + write ACK
        OpaqueRead,
        OpaqueWrite(pack(InitSuccessMessage::new(proto))),
        //
        // Read, write first test
        OpaqueRead,
        OpaqueWrite(pack(RawTestResultMessage::fake(proto))),
        //
        // Bail on the second test
        OpaqueRead,
        Stdout(b"I failed catastrophically".to_vec()),
        Stderr(b"For a reason explainable only by a backtrace".to_vec()),
        Exit(1),
    ];

    fn work_bundle(proto: ProtocolWitness) -> NextWorkBundle {
        NextWorkBundle::new(
            vec![
                WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "test1", Default::default()),
                        work_id: WorkId([1; 16]),
                    },
                    run_number: INIT_RUN_NUMBER,
                },
                WorkerTest {
                    spec: TestSpec {
                        test_case: TestCase::new(proto, "test2", Default::default()),
                        work_id: WorkId([2; 16]),
                    },
                    run_number: INIT_RUN_NUMBER,
                },
            ],
            Eow(true),
        )
    }
    let get_next_tests = ImmediateTests::new(vec![work_bundle(proto)]);

    let (error, mut results, _notified_all_run) =
        run_simulated_runner_to_error(simulation, None, Box::new(get_next_tests));

    let GenericRunnerError {
        error: _,
        output,
        native_runner_info,
    } = error;

    assert!(
        native_runner_info.is_some(),
        "Native runner info should be defined because some tests ran"
    );

    check_bytes!(&output.stdout, b"I failed catastrophically");
    check_bytes!(
        &output.stderr,
        b"For a reason explainable only by a backtrace"
    );

    results.sort_by_key(|r| r.work_id);

    assert_eq!(results.len(), 2, "{results:?}");

    {
        let AssociatedTestResults {
            work_id, results, ..
        } = &results[0];
        assert_eq!(work_id.0, [1; 16]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].output.as_ref().unwrap(), "my test output");
    }
    {
        let AssociatedTestResults {
            work_id, results, ..
        } = &results[1];
        assert_eq!(work_id.0, [2; 16]);

        assert_eq!(results.len(), 1);

        let output = results[0].output.as_ref().unwrap();
        let output = sanitize_output(output);

        insta::assert_snapshot!(output, @r###"
        -- Unexpected Test Runner Failure --

        The test command

        <simulation cmd>

        stopped communicating with its abq worker before completing all test requests.

        Here's the standard output/error we found for the failing command.

        Stdout:

        I failed catastrophically

        Stderr:

        For a reason explainable only by a backtrace

        Please see worker 0, runner X for more details.
        "###);
    }
}

#[tokio::test]
#[with_protocol_version]
async fn cancellation_of_native_runner_succeeds() {
    use Msg::*;

    let simulation = [
        Connect,
        //
        // Write spawn message
        OpaqueWrite(pack(legal_spawned_message(proto))),
        //
        // Read init context message + write ACK
        OpaqueRead,
        OpaqueWrite(pack(InitSuccessMessage::new(proto))),
        //
        // Sleep forever
        Sleep(Duration::from_secs(600)),
    ];

    fn work_bundle(proto: ProtocolWitness) -> NextWorkBundle {
        NextWorkBundle::new(
            vec![WorkerTest {
                spec: TestSpec {
                    test_case: TestCase::new(proto, "test1", Default::default()),
                    work_id: WorkId([1; 16]),
                },
                run_number: INIT_RUN_NUMBER,
            }],
            Eow(true),
        )
    }
    let get_next_tests = ImmediateTests::new(vec![work_bundle(proto)]);

    let (run_tests_task, state) = get_simulated_runner(simulation, None, Box::new(get_next_tests));

    let runner_handle = tokio::spawn(run_tests_task);
    state.shutdown.notify().unwrap();
    let runner_result = runner_handle.await.unwrap();
    let TestRunnerExit { exit_code, .. } = runner_result.unwrap();
    assert_eq!(exit_code, ExitCode::CANCELLED);

    assert!(state.all_results.lock().is_empty());
    assert!(!state.all_tests_run.load(atomic::ORDERING));
}
