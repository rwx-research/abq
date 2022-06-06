use anyhow::Result;
use std::process;

use abq_output::format_result;
use abq_workers::protocol::{WorkId, WorkerAction};

use crate::collect::{CollectInputs, Collector};

#[derive(Default)]
pub struct Ctx {
    next_id: usize,
}

impl Ctx {
    fn next_id(&mut self) -> WorkId {
        self.next_id += 1;
        WorkId(self.next_id.to_string())
    }
}

pub fn collector() -> Collector<String, Ctx, impl CollectInputs<String>> {
    let collect_tests = || match find_cargo_tests() {
        Ok(tests) => tests,
        Err(e) => {
            eprintln!("Failed to find cargo tests: {e}. Is this a Rust project?");
            process::exit(1);
        }
    };

    let create_work = |ctx: &mut Ctx, test_path: String| {
        let id = ctx.next_id();
        let prefix = test_path.clone();

        let action = WorkerAction::Exec {
            cmd: "cargo".to_string(),
            args: vec!["test".to_string(), test_path],
            working_dir: std::env::current_dir().unwrap(),
        };

        (prefix, id, action)
    };

    let report_result = format_result;

    Collector {
        _input: std::marker::PhantomData::default(),
        context: Ctx::default(),
        collect_message: "Discovering tests",
        collect_inputs: collect_tests,
        create_work,
        report_result,
    }
}

/// Returns a list of cargo test names for the crate in the current directory.
fn find_cargo_tests() -> Result<Vec<String>> {
    // Unfortunately cargo doesn't expose a way to list tests in a serialized output.
    // --message-format=json does not affect it, and neither does --format=json to the (native)
    // test runner.
    // That said, with --format=terse, the output is machine readable:
    //
    //   queue::test::multiple_invokers: test
    //   workers::test::test_1_worker_1_echo: test
    //   <etc>
    //
    // We parse out the test path and any trailing newlines.
    let output = process::Command::new("cargo")
        .args(["test", "--", "--list", "--format=terse"])
        .output()?;

    let test_paths: Result<Vec<_>> = output
        .stdout
        .split(|&c| c == b'\n')
        .into_iter()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let suffix = ": test";
            let test_path_bytes = &line[0..line.len() - suffix.len()];
            let test_path = String::from_utf8(test_path_bytes.to_vec())?;
            Ok(test_path)
        })
        .collect();

    test_paths
}
