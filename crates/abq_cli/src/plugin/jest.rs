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
    let collect_tests = || match find_jest_tests() {
        Ok(tests) => {
            if tests.is_empty() {
                eprintln!("Found no tests. Is this a JavaScript project?");
                process::exit(1);
            }
            tests
        }
        Err(e) => {
            eprintln!("Error while trying to find jest tests: {e}");
            process::exit(1);
        }
    };

    let create_work = |ctx: &mut Ctx, test_path: String| {
        let id = ctx.next_id();
        let prefix = test_path.clone();

        let action = WorkerAction::Exec {
            cmd: "yarn".to_string(),
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

/// Returns a list of jest test names for the project in the current directory.
fn find_jest_tests() -> Result<Vec<String>> {
    // We parse out the test path and any trailing newlines.
    let output = process::Command::new("yarn")
        .args(["test", "--listTests", "--json"])
        .output()?;

    find_jest_tests_from_output(output.stdout)
}

fn find_jest_tests_from_output(output: Vec<u8>) -> Result<Vec<String>> {
    let test_path_json = output
        .split(|&c| c == b'\n')
        .into_iter()
        .find(|line| line.starts_with(b"[") && line.ends_with(b"]"))
        .unwrap_or(b"[]");

    let result: Vec<String> = serde_json::from_slice(test_path_json)?;

    Ok(result)
}

#[test]
fn test_find_jest_tests_from_real_output() {
    // This is based on the output of `yarn test --listTests --json` in the React repo
    let output = r#"
yarn run v1.22.15
$ node ./scripts/jest/jest-cli.js --listTests --json
$ NODE_ENV=development RELEASE_CHANNEL=experimental compactConsole=false VARIANT=true node ./scripts/jest/jest.js --config ./scripts/jest/config.source-www.js --listTests --json

Running tests for default (www-modern)...
jest-haste-map: duplicate manual mock found: JSResourceReferenceImpl
The following files share their name; please delete one of them:
    * <rootDir>/packages/react-server-native-relay/src/__mocks__/JSResourceReferenceImpl.js
    * <rootDir>/packages/react-server-dom-relay/src/__mocks__/JSResourceReferenceImpl.js

["/path/to/test/useFocus-test.internal.js","/path/to/other_tests/__tests__/useFocusWithin-test.internal.js"]
✨  Done in 1.21s.
    "#.as_bytes().to_vec();

    let result = find_jest_tests_from_output(output).unwrap();

    assert_eq!(
        result,
        vec![
            "/path/to/test/useFocus-test.internal.js",
            "/path/to/other_tests/__tests__/useFocusWithin-test.internal.js"
        ]
    );
}

#[test]
fn test_find_jest_tests_from_wrong_output() {
    //
    let output = r#"
bash: jest: command not found
    "#
    .as_bytes()
    .to_vec();

    let result = find_jest_tests_from_output(output).unwrap();

    assert!(result.is_empty());
}
