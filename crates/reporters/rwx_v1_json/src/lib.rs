use serde_derive::{Deserialize, Serialize};
use serde_json::Map;
use std::{collections::HashMap, io::Write, ops::Deref};

use abq_utils::net_protocol::{
    entity::RunnerMeta,
    runners::{
        self, MetadataMap, NativeRunnerSpecification, Status, TestResult, TestResultSpec,
        TestRuntime,
    },
};

// Note: this is intentionally permissive right now for `kind` and `language` until we have
// implemented native runner support for specifying language and framework. Once we do that,
// we can handle unexpected frameworks by instantiating an "other" framework w/ provided kind
// and language
#[derive(Serialize, Deserialize, Clone)]
pub struct Framework {
    kind: String,
    language: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "providedKind")]
    provided_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "providedLanguage")]
    provided_language: Option<String>,
}

impl Framework {
    fn other() -> Self {
        Self {
            kind: "other".to_owned(),
            language: "other".to_owned(),
            provided_kind: None,
            provided_language: None,
        }
    }
}

impl From<&NativeRunnerSpecification> for Framework {
    fn from(spec: &NativeRunnerSpecification) -> Self {
        // Normalize kind/language for native runners we know to https://github.com/rwx-research/test-results-schema/blob/main/v1.json
        let mut provided_language = None;
        let mut provided_kind = None;
        let (language, kind) = match (spec.language.as_str(), spec.test_framework.as_str()) {
            ("ruby", "rspec") => ("Ruby", "RSpec"),
            ("javascript", "jest") => ("JavaScript", "Jest"),
            (lang, kind) => {
                provided_language = Some(lang);
                provided_kind = Some(kind);
                ("other", "other")
            }
        };

        Self {
            kind: kind.to_string(),
            language: language.to_string(),
            provided_kind: provided_kind.map(ToOwned::to_owned),
            provided_language: provided_language.map(ToOwned::to_owned),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "kind")]
pub enum SummaryStatus {
    #[serde(rename = "canceled")]
    Canceled,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "successful")]
    Successful,
    #[serde(rename = "timed_out")]
    TimedOut,
}

type SummaryCounter = u64;

#[derive(Serialize, Deserialize, Clone)]
pub struct Summary {
    status: SummaryStatus,
    tests: SummaryCounter,
    #[serde(rename = "otherErrors")]
    other_errors: SummaryCounter,
    retries: SummaryCounter,
    canceled: SummaryCounter,
    failed: SummaryCounter,
    pended: SummaryCounter,
    quarantined: SummaryCounter,
    skipped: SummaryCounter,
    successful: SummaryCounter,
    #[serde(rename = "timedOut")]
    timed_out: SummaryCounter,
    todo: SummaryCounter,
}

impl Default for Summary {
    fn default() -> Self {
        Self {
            status: SummaryStatus::Successful,
            tests: 0,
            other_errors: 0,
            retries: 0,
            canceled: 0,
            failed: 0,
            pended: 0,
            quarantined: 0,
            skipped: 0,
            successful: 0,
            timed_out: 0,
            todo: 0,
        }
    }
}

impl Summary {
    fn account(&mut self, test: &Test) {
        self.tests += 1;

        if matches!(&test.past_attempts, Some(attempts) if !attempts.is_empty()) {
            self.retries += 1;
        }

        match &test.attempt.status {
            AttemptStatus::Canceled => {
                self.status = SummaryStatus::Failed;
                self.canceled += 1;
            }
            AttemptStatus::Failed {
                exception: _,
                message: _,
                backtrace: _,
            } => {
                self.status = SummaryStatus::Failed;
                self.failed += 1;
            }
            AttemptStatus::Pended { message: _ } => {
                self.pended += 1;
            }
            AttemptStatus::Skipped { message: _ } => {
                self.skipped += 1;
            }
            AttemptStatus::Successful => {
                self.successful += 1;
            }
            AttemptStatus::TimedOut => {
                self.status = SummaryStatus::Failed;
                self.timed_out += 1;
            }
            AttemptStatus::Todo { message: _ } => {
                self.todo += 1;
            }
            AttemptStatus::Quarantined { original_status: _ } => {
                self.quarantined += 1;
            }
        }
    }

    fn account_native_runner_errors(&mut self, native_runner_errors: u64) {
        self.other_errors += native_runner_errors;
        if native_runner_errors > 0 {
            self.status = SummaryStatus::Failed;
        }
    }

    fn account_other_errors(&mut self, len: u64) {
        self.other_errors += len;
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Location {
    file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    column: Option<u64>,
}

impl From<&runners::Location> for Location {
    fn from(l: &runners::Location) -> Self {
        Self {
            file: l.file.clone(),
            line: l.line,
            column: l.column,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "kind")]
pub enum AttemptStatus {
    #[serde(rename = "canceled")]
    Canceled,
    #[serde(rename = "failed")]
    Failed {
        #[serde(skip_serializing_if = "Option::is_none")]
        exception: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        backtrace: Option<Vec<String>>,
    },
    #[serde(rename = "pended")]
    Pended {
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    #[serde(rename = "skipped")]
    Skipped {
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    #[serde(rename = "successful")]
    Successful,
    #[serde(rename = "timedOut")]
    TimedOut,
    #[serde(rename = "todo")]
    Todo {
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    #[serde(rename = "quarantined")]
    Quarantined {
        #[serde(rename = "originalStatus")]
        original_status: Box<AttemptStatus>,
    },
}

type Nanoseconds = u64;

#[derive(Serialize, Deserialize, Clone)]
pub struct Attempt {
    #[serde(rename = "durationInNanoseconds")]
    duration: Nanoseconds,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<serde_json::Map<String, serde_json::Value>>,
    status: AttemptStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "startedAt")]
    started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "finishedAt")]
    finished_at: Option<String>,
}

enum OptAttempt {
    SyntheticNativeRunnerError,
    Attempt {
        attempt: Attempt,
        past_attempts: Option<Vec<Attempt>>,
        native_runner_errors: u64,
    },
}

impl Attempt {
    fn from(test_result: &TestResultSpec, runner_meta: RunnerMeta) -> OptAttempt {
        let TestResultSpec {
            status,
            output,
            runtime,
            meta,
            started_at,
            finished_at,
            other_errors: _,
            stderr,
            stdout,
            past_attempts,
            ..
        } = test_result;

        let status = match status {
            Status::Success => AttemptStatus::Successful,
            Status::Failure {
                exception,
                backtrace,
            }
            | Status::Error {
                exception,
                backtrace,
            } => AttemptStatus::Failed {
                exception: exception.clone(),
                message: output.clone(),
                backtrace: backtrace.clone(),
            },
            Status::PrivateNativeRunnerError => return OptAttempt::SyntheticNativeRunnerError,
            Status::Pending => AttemptStatus::Pended {
                message: output.clone(),
            },
            Status::Skipped => AttemptStatus::Skipped {
                message: output.clone(),
            },
            Status::Todo => AttemptStatus::Todo {
                message: output.clone(),
            },
            Status::TimedOut => AttemptStatus::TimedOut,
        };

        let duration = match runtime {
            TestRuntime::Milliseconds(runtime) => (runtime * 1000000.0).round() as Nanoseconds,
            TestRuntime::Nanoseconds(nanos) => *nanos,
        };

        let stderr = stderr
            .as_ref()
            .map(|s| String::from_utf8_lossy(s).to_string());
        let stdout = stdout
            .as_ref()
            .map(|s| String::from_utf8_lossy(s).to_string());

        let worker_runner = runner_meta.runner;
        let meta = {
            let mut meta = meta.clone();

            let mut abq_metadata = Map::new();
            abq_metadata.insert("worker".to_owned(), worker_runner.worker().into());
            abq_metadata.insert("runner".to_owned(), worker_runner.runner().into());

            meta.insert("abq_metadata".to_owned(), abq_metadata.into());
            meta
        };

        let mut other_errors = 0;
        let past_attempts = past_attempts.as_ref().map(|attempts| {
            let mut all_past_attempts = Vec::with_capacity(attempts.len());

            for attempt in attempts.iter() {
                match Attempt::from(attempt, runner_meta) {
                    OptAttempt::SyntheticNativeRunnerError => {
                        other_errors += 1;
                    }
                    OptAttempt::Attempt {
                        attempt,
                        past_attempts,
                        native_runner_errors: past_other_errors,
                    } => {
                        all_past_attempts.push(attempt);
                        if let Some(extra) = past_attempts {
                            all_past_attempts.extend(extra);
                        }
                        other_errors += past_other_errors;
                    }
                }
            }

            all_past_attempts
        });

        let this_attempt = Attempt {
            duration,
            meta: Some(meta),
            status,
            stderr,
            stdout,
            started_at: started_at.as_ref().map(|t| t.0.clone()),
            finished_at: finished_at.as_ref().map(|t| t.0.clone()),
        };

        OptAttempt::Attempt {
            attempt: this_attempt,
            past_attempts,
            native_runner_errors: other_errors,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Test {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    lineage: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Location>,
    attempt: Attempt,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "pastAttempts")]
    past_attempts: Option<Vec<Attempt>>,
}

/// A sketch of a test result we use to assemble results across retries.
#[derive(Clone)]
enum TestSketch {
    /// A material test result.
    Material {
        test: Test,
        native_runner_errors: u64,
    },
    SyntheticNativeRunnerError,
}

impl From<&TestResult> for TestSketch {
    fn from(test_result: &TestResult) -> Self {
        let TestResultSpec {
            id,
            display_name,
            location,
            lineage,
            ..
        } = &test_result.deref();

        match Attempt::from(test_result.deref(), test_result.source) {
            OptAttempt::SyntheticNativeRunnerError => Self::SyntheticNativeRunnerError,
            OptAttempt::Attempt {
                attempt,
                past_attempts,
                native_runner_errors: other_errors,
            } => TestSketch::Material {
                test: Test {
                    id: Some(id.to_owned()),
                    name: display_name.clone(),
                    lineage: lineage.clone(),
                    location: location.as_ref().map(Into::into),
                    attempt,
                    past_attempts,
                },
                native_runner_errors: other_errors,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct OtherError {
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Location>,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<MetadataMap>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exception: Option<String>,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backtrace: Option<Vec<String>>,
}

impl OtherError {
    fn from_message(message: String) -> Self {
        Self {
            location: None,
            meta: None,
            exception: None,
            message,
            backtrace: None,
        }
    }
}

// Note: this does not currently implement support for otherErrors nor derivedFrom
// derivedFrom is not necessary for ABQ (it's not parsing original test results files)
// otherErrors are not exposed to the reporters right now
#[derive(Serialize, Deserialize, Clone)]
pub struct TestResults {
    #[serde(rename = "$schema")]
    schema: String,
    framework: Framework,
    summary: Summary,
    tests: Vec<Test>,
    #[serde(rename = "otherErrors")]
    other_errors: Vec<OtherError>,
}

#[derive(Default, Clone)]
pub struct Collector {
    tests: HashMap<String, Test>,
    native_runner_errors: u64,
}

impl Collector {
    #[inline(always)]
    pub fn push_result(&mut self, test_result: &TestResult) {
        let test = match self.tests.get_mut(&test_result.id) {
            Some(test) => test,
            None => {
                // This will account for the first test result we saw; subsequent test results
                // (due to retries) will trace the branch above.
                match TestSketch::from(test_result) {
                    TestSketch::SyntheticNativeRunnerError => {
                        self.native_runner_errors += 1;
                    }
                    TestSketch::Material {
                        test,
                        native_runner_errors: other_errors,
                    } => {
                        self.native_runner_errors += other_errors;
                        self.tests.insert(test_result.id.clone(), test);
                    }
                }
                return;
            }
        };

        // This is a retry.
        match Attempt::from(&test_result.result, test_result.source) {
            OptAttempt::SyntheticNativeRunnerError => todo!(),
            OptAttempt::Attempt {
                attempt: new_attempt,
                past_attempts: new_attempt_previous_attempts,
                native_runner_errors: other_errors,
            } => {
                // Add the attempt and push the old one back.
                let prev_latest_attempt = std::mem::replace(&mut test.attempt, new_attempt);

                let past_attempts = test.past_attempts.get_or_insert_with(|| {
                    let new_past_attempts = new_attempt_previous_attempts.as_ref();
                    Vec::with_capacity(1 + new_past_attempts.map(|a| a.len()).unwrap_or(0))
                });

                past_attempts.push(prev_latest_attempt);

                // Also add any additional attempts the latest attempt produced.
                if let Some(new_attempt_previous_attempts) = new_attempt_previous_attempts {
                    past_attempts.extend(new_attempt_previous_attempts);
                }

                self.native_runner_errors += other_errors;
            }
        }
    }

    pub fn write_json(
        self,
        writer: impl Write,
        runner_specification: Option<&NativeRunnerSpecification>,
        other_error_messages: Vec<String>,
    ) -> Result<(), String> {
        serde_json::to_writer(
            writer,
            &self.test_results(runner_specification, other_error_messages),
        )
        .map_err(|e| e.to_string())
    }

    pub fn write_json_pretty(
        self,
        writer: impl Write,
        runner_specification: Option<&NativeRunnerSpecification>,
        other_error_messages: Vec<String>,
    ) -> Result<(), String> {
        serde_json::to_writer_pretty(
            writer,
            &self.test_results(runner_specification, other_error_messages),
        )
        .map_err(|e| e.to_string())
    }

    fn test_results(
        self,
        runner_specification: Option<&NativeRunnerSpecification>,
        other_error_messages: Vec<String>,
    ) -> TestResults {
        let mut summary = Summary::default();
        self.tests.values().for_each(|test| summary.account(test));

        summary.account_native_runner_errors(self.native_runner_errors);
        summary.account_other_errors(other_error_messages.len() as _);

        let reified_schema_tests = if cfg!(test) {
            let mut ordered_tests: Vec<Test> = self.tests.into_values().map(Into::into).collect();
            ordered_tests.sort_by(|t1, t2| t1.id.cmp(&t2.id));
            ordered_tests
        } else {
            self.tests.into_values().map(Into::into).collect()
        };

        let framework = match runner_specification {
            Some(spec) => spec.into(),
            None => Framework::other(),
        };

        let other_errors = other_error_messages
            .into_iter()
            .map(OtherError::from_message)
            .collect();

        TestResults {
            schema:
                "https://raw.githubusercontent.com/rwx-research/test-results-schema/main/v1.json"
                    .to_string(),
            framework,
            summary,
            tests: reified_schema_tests,
            other_errors,
        }
    }
}

#[cfg(test)]
mod test {
    use abq_test_utils::TestResultBuilder;
    use abq_utils::net_protocol::{
        entity::{RunnerMeta, WorkerRunner},
        runners::{NativeRunnerSpecification, Status, TestResult, TestResultSpec, TestRuntime},
    };

    use crate::Collector;

    #[test]
    fn generates_rwx_v1_json_for_all_statuses() {
        let mut collector = Collector::default();
        let mut meta = serde_json::Map::new();
        meta.insert(
            "some".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        collector.push_result(&TestResult::new(
            RunnerMeta::new(WorkerRunner::new(3, 1), false, false),
            TestResultSpec {
                status: Status::Success,
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("Test 1 passed".to_string()),
                runtime: TestRuntime::Milliseconds(11.0),
                meta,
                ..TestResultSpec::fake()
            },
        ));
        collector.push_result(&TestResult::new(
            RunnerMeta::new(WorkerRunner::new(2, 4), false, false),
            TestResultSpec {
                status: Status::Failure {
                    exception: Some("test-exception".to_string()),
                    backtrace: Some(vec!["file1.cpp:10".to_string(), "file2.cpp:20".to_string()]),
                },
                id: "id2".to_string(),
                display_name: "app::module::test2".to_string(),
                output: Some("Test 2 failed".to_string()),
                runtime: TestRuntime::Milliseconds(22.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        ));
        collector.push_result(&TestResult::new(
            RunnerMeta::new(WorkerRunner::new(1, 8), false, false),
            TestResultSpec {
                status: Status::Error {
                    exception: None,
                    backtrace: None,
                },
                id: "id3".to_string(),
                display_name: "app::module::test3".to_string(),
                output: None,
                runtime: TestRuntime::Milliseconds(33.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        ));
        collector.push_result(&TestResult::new(
            RunnerMeta::new(WorkerRunner::new(0, 3), false, false),
            TestResultSpec {
                status: Status::Pending,
                id: "id4".to_string(),
                display_name: "app::module::test4".to_string(),
                output: Some("Test 4 pending".to_string()),
                runtime: TestRuntime::Milliseconds(44.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        ));
        collector.push_result(&TestResult::new(
            RunnerMeta::new(WorkerRunner::new(5, 1), false, false),
            TestResultSpec {
                status: Status::Skipped,
                id: "id5".to_string(),
                display_name: "app::module::test5".to_string(),
                output: None,
                runtime: TestRuntime::Milliseconds(55.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        ));

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_all_statuses__compact", json)
        }

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json_pretty(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_all_statuses__pretty", json)
        }
    }

    #[test]
    fn generates_rwx_v1_json_for_successful_runs() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Success,
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("Test 1 passed".to_string()),
                runtime: TestRuntime::Milliseconds(11.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        ));

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_successful_runs__compact", json)
        }

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json_pretty(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_successful_runs__pretty", json)
        }
    }

    #[test]
    fn generates_rwx_v1_json_with_retries_ultimately_failing() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed once".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed again".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I ultimately failed".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
            .expect("failed to write");

        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }

    #[test]
    fn generates_rwx_v1_json_with_retries_ultimately_succeeding() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed once".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed again".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Success,
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("Look at me, i succeeded!".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
            .expect("failed to write");

        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }

    #[test]
    fn retries_with_nested_past_attempts() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed a second time".to_string()),
                past_attempts: Some(vec![TestResultSpec {
                    status: Status::Failure {
                        exception: None,
                        backtrace: None,
                    },
                    id: "id1".to_string(),
                    display_name: "app::module::test1".to_string(),
                    output: Some("I failed a first time".to_string()),
                    ..TestResultSpec::fake()
                }]),
                ..TestResultSpec::fake()
            },
        ));

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed a final time".to_string()),
                past_attempts: Some(vec![TestResultSpec {
                    status: Status::Failure {
                        exception: None,
                        backtrace: None,
                    },
                    id: "id1".to_string(),
                    display_name: "app::module::test1".to_string(),
                    output: Some("I failed a third time".to_string()),
                    ..TestResultSpec::fake()
                }]),
                ..TestResultSpec::fake()
            },
        ));

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
            .expect("failed to write");

        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }

    #[test]
    fn native_runner_error_seen_as_other_error() {
        let mut collector = Collector::default();

        collector
            .push_result(&TestResultBuilder::new("id1", Status::PrivateNativeRunnerError).build());

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, Some(&NativeRunnerSpecification::fake()), vec![])
            .expect("failed to write");

        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }

    #[test]
    fn ruby_rspec_lang() {
        let collector = Collector::default();

        let runner_spec = NativeRunnerSpecification {
            name: "rspec-abq".to_string(),
            version: "1.0.0".to_string(),
            test_framework: "rspec".to_string(),
            test_framework_version: "4.2.1".to_string(),
            language: "ruby".to_string(),
            language_version: "3.1.0".to_string(),
            host: "abqmachine".to_string(),
        };

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, Some(&runner_spec), vec![])
            .expect("failed to write");
        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }

    #[test]
    fn javascript_jest_lang() {
        let collector = Collector::default();

        let runner_spec = NativeRunnerSpecification {
            name: "jest-abq".to_string(),
            version: "29.3.100".to_string(),
            test_framework: "jest".to_string(),
            test_framework_version: "29.3.1".to_string(),
            language: "javascript".to_string(),
            language_version: "node-16.0.0".to_string(),
            host: "abqnode-16.0.0".to_string(),
        };

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, Some(&runner_spec), vec![])
            .expect("failed to write");
        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }

    #[test]
    fn generates_rwx_v1_json_with_unknown_framework_if_runner_info_is_missing() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed once".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, None, vec![])
            .expect("failed to write");

        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }

    #[test]
    fn writes_passed_other_error_messages() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("I failed once".to_string()),
                ..TestResultSpec::fake()
            },
        ));

        let other_errors = vec![
            "Something else weird happened during in test suite".to_owned(),
            "Today, 1 = 2".to_owned(),
        ];

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, None, other_errors)
            .expect("failed to write");

        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }
}
