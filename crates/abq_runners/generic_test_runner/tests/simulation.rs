use std::sync::Arc;

use abq_generic_test_runner::{GenericTestRunner, GetNextTests, SendTestResultsBoxed};
use abq_native_runner_simulation::{pack, pack_msgs, Msg};
use abq_test_utils::artifacts_dir;
use abq_utils::exit::ExitCode;
use abq_utils::net_protocol::entity::EntityId;
use abq_utils::net_protocol::queue::AssociatedTestResults;
use abq_utils::net_protocol::runners::{
    CapturedOutput, InitSuccessMessage, Manifest, ManifestMessage, NativeRunnerSpecification,
    ProtocolWitness, RawNativeRunnerSpawnedMessage, RawTestResultMessage, Test, TestCase,
    TestOrGroup, TestRunnerExit,
};
use abq_utils::net_protocol::work_server::InitContext;
use abq_utils::net_protocol::workers::{
    ManifestResult, NativeTestRunnerParams, NextWork, NextWorkBundle, WorkId, WorkerTest,
};

use abq_with_protocol_version::with_protocol_version;
use futures::FutureExt;
use parking_lot::Mutex;

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
        test_framework: Some("rspec".to_owned()),
        test_framework_version: Some("3.12.0".to_owned()),
        language: Some("ruby".to_owned()),
        language_version: Some("3.1.2p20".to_owned()),
        host: Some("ruby 3.1.2p20 (2022-04-12 revision 4491bb740a) [x86_64-darwin21]".to_owned()),
    };
    RawNativeRunnerSpawnedMessage::new(proto, protocol_version, runner_specification)
}

const NO_GENERATE_MANIFEST: Option<fn(ManifestResult)> = None;

fn run_simulated_runner<SendManifest: FnMut(ManifestResult)>(
    simulation: impl IntoIterator<Item = Msg>,
    with_manifest: Option<SendManifest>,
    get_next_test: GetNextTests,
) -> (
    Vec<AssociatedTestResults>,
    Option<CapturedOutput>,
    CapturedOutput,
) {
    let simulation_msg = pack_msgs(simulation);
    let simfile = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    let simfile_path = simfile.to_path_buf();
    std::fs::write(&simfile_path, simulation_msg).unwrap();

    let input = NativeTestRunnerParams {
        cmd: native_runner_simulation_bin(),
        args: vec![simfile_path.display().to_string()],
        extra_env: Default::default(),
    };

    let get_init_context = || {
        Ok(InitContext {
            init_meta: Default::default(),
        })
    };
    let all_results: Arc<Mutex<Vec<AssociatedTestResults>>> = Default::default();

    let send_test_result: SendTestResultsBoxed = {
        let all_results = all_results.clone();
        Box::new(move |results| {
            let all_results = all_results.clone();
            Box::pin(async move {
                all_results.lock().extend(results);
            })
        })
    };

    let TestRunnerExit {
        final_captured_output,
        manifest_generation_output,
        exit_code,
    } = GenericTestRunner::run(
        EntityId::new(),
        input,
        &std::env::current_dir().unwrap(),
        || false,
        5,
        with_manifest,
        get_init_context,
        get_next_test,
        &*send_test_result,
        false,
    )
    .unwrap();

    assert_eq!(exit_code, ExitCode::SUCCESS);

    drop(send_test_result);

    (
        Arc::try_unwrap(all_results)
            .expect("outstanding refs to all results")
            .into_inner(),
        manifest_generation_output,
        final_captured_output,
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
        NextWorkBundle(vec![
            NextWork::Work(WorkerTest {
                test_case: TestCase::new(proto, "test1", Default::default()),
                work_id: WorkId([1; 16]),
            }),
            NextWork::Work(WorkerTest {
                test_case: TestCase::new(proto, "test2", Default::default()),
                work_id: WorkId([2; 16]),
            }),
            NextWork::EndOfWork,
        ])
    }
    let get_next_tests = &move || async move { work_bundle(proto) }.boxed();

    let (mut results, _man_output, final_captures) =
        run_simulated_runner(simulation, NO_GENERATE_MANIFEST, get_next_tests);
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

    assert!(abq_utils::net_protocol::validate_max_message_size(
        serde_json::to_vec(&huge_manifest).unwrap().len() as _
    )
    .is_err());

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
    let with_manifest = Box::new({
        let manifest = manifest.clone();
        move |manifest_result| {
            *manifest.lock() = Some(manifest_result);
        }
    });
    let get_next_tests = &move || async move { NextWorkBundle(vec![NextWork::EndOfWork]) }.boxed();

    let (results, _man_output, _) =
        run_simulated_runner(simulation, Some(with_manifest), get_next_tests);
    assert!(results.is_empty());

    let manifest = Arc::try_unwrap(manifest).unwrap().into_inner().unwrap();
    let manifest = match manifest {
        ManifestResult::Manifest(man) => man.manifest,
        ManifestResult::TestRunnerError { .. } => unreachable!(),
    };
    let (tests, meta) = manifest.flatten();
    assert_eq!(tests.len(), 10_000);
    assert!(meta.is_empty());
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
    let with_manifest = Box::new({
        let manifest = manifest.clone();
        move |manifest_result| {
            *manifest.lock() = Some(manifest_result);
        }
    });
    let get_next_tests = &move || async move { NextWorkBundle(vec![NextWork::EndOfWork]) }.boxed();

    let (results, man_output, _) =
        run_simulated_runner(simulation, Some(with_manifest), get_next_tests);
    assert!(results.is_empty());

    assert!(man_output.is_some());
    let man_output = man_output.unwrap();
    assert_eq!(man_output.stdout, b"init stdouthello from manifest stdout");
    assert_eq!(man_output.stderr, b"init stderrhello from manifest stderr");

    let manifest = Arc::try_unwrap(manifest).unwrap().into_inner().unwrap();
    let manifest = match manifest {
        ManifestResult::Manifest(man) => man.manifest,
        ManifestResult::TestRunnerError { .. } => unreachable!(),
    };
    let (tests, meta) = manifest.flatten();
    assert_eq!(tests.len(), 10);
    assert!(meta.is_empty());
}
