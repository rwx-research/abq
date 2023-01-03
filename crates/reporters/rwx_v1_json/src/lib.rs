use serde_derive::{Deserialize, Serialize};
use std::{io::Write, ops::Deref};

use abq_utils::net_protocol::runners::{
    self, NativeRunnerSpecification, Status, TestResult, TestResultSpec, TestRuntime,
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

impl From<&NativeRunnerSpecification> for Framework {
    fn from(spec: &NativeRunnerSpecification) -> Self {
        // Normalize kind/language for native runners we know to https://github.com/rwx-research/test-results-schema/blob/main/v1.json
        let mut provided_language = None;
        let mut provided_kind = None;
        let (language, kind) = match (spec.language.as_deref(), spec.test_framework.as_deref()) {
            (Some("ruby"), Some("rspec")) => ("Ruby", "RSpec"),
            (Some("javascript"), Some("jest")) => ("JavaScript", "Jest"),
            (lang, kind) => {
                provided_language = lang;
                provided_kind = kind;
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

impl From<&TestResult> for Attempt {
    fn from(test_result: &TestResult) -> Self {
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
            ..
        } = &test_result.deref();

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
            Status::PrivateNativeRunnerError => AttemptStatus::Failed {
                exception: None,
                message: output.clone(),
                backtrace: None,
            },
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

        Attempt {
            duration,
            meta: Some(meta.clone()),
            status,
            stderr: stderr.clone(),
            stdout: stdout.clone(),
            started_at: started_at.as_ref().map(|t| t.0.clone()),
            finished_at: finished_at.as_ref().map(|t| t.0.clone()),
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

impl From<&TestResult> for Test {
    fn from(test_result: &TestResult) -> Self {
        let TestResultSpec {
            id,
            display_name,
            location,
            lineage,
            past_attempts,
            ..
        } = &test_result.deref();

        let attempt = test_result.into();
        let past_attempts = past_attempts
            .as_ref()
            .map(|attempts| attempts.iter().map(Into::into).collect());

        Test {
            id: Some(id.to_owned()),
            name: display_name.clone(),
            lineage: lineage.clone(),
            location: location.as_ref().map(Into::into),
            attempt,
            past_attempts,
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
}

#[derive(Default, Clone)]
pub struct Collector {
    summary: Summary,
    tests: Vec<Test>,
}

impl Collector {
    #[inline(always)]
    pub fn push_result(&mut self, test_result: &TestResult) {
        let test: Test = test_result.into();

        self.summary.tests += 1;

        if let Some(past_attempts) = &test.past_attempts {
            if !past_attempts.is_empty() {
                self.summary.retries += 1;
            }
        }

        match &test.attempt.status {
            AttemptStatus::Canceled => {
                self.summary.status = SummaryStatus::Failed;
                self.summary.canceled += 1;
            }
            AttemptStatus::Failed {
                exception: _,
                message: _,
                backtrace: _,
            } => {
                self.summary.status = SummaryStatus::Failed;
                self.summary.failed += 1;
            }
            AttemptStatus::Pended { message: _ } => {
                self.summary.pended += 1;
            }
            AttemptStatus::Skipped { message: _ } => {
                self.summary.skipped += 1;
            }
            AttemptStatus::Successful => {
                self.summary.successful += 1;
            }
            AttemptStatus::TimedOut => {
                self.summary.status = SummaryStatus::Failed;
                self.summary.timed_out += 1;
            }
            AttemptStatus::Todo { message: _ } => {
                self.summary.todo += 1;
            }
            AttemptStatus::Quarantined { original_status: _ } => {
                self.summary.quarantined += 1;
            }
        }

        self.tests.push(test);
    }

    pub fn write_json(
        self,
        writer: impl Write,
        runner_specification: &NativeRunnerSpecification,
    ) -> Result<(), String> {
        serde_json::to_writer(writer, &self.test_results(runner_specification))
            .map_err(|e| e.to_string())
    }

    pub fn write_json_pretty(
        self,
        writer: impl Write,
        runner_specification: &NativeRunnerSpecification,
    ) -> Result<(), String> {
        serde_json::to_writer_pretty(writer, &self.test_results(runner_specification))
            .map_err(|e| e.to_string())
    }

    fn test_results(self, runner_specification: &NativeRunnerSpecification) -> TestResults {
        TestResults {
            schema:
                "https://raw.githubusercontent.com/rwx-research/test-results-schema/main/v1.json"
                    .to_string(),
            framework: runner_specification.into(),
            summary: self.summary,
            tests: self.tests,
        }
    }
}

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::runners::{
        NativeRunnerSpecification, Status, TestResult, TestResultSpec, TestRuntime,
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

        collector.push_result(&TestResult::new(TestResultSpec {
            status: Status::Success,
            id: "id1".to_string(),
            display_name: "app::module::test1".to_string(),
            output: Some("Test 1 passed".to_string()),
            runtime: TestRuntime::Milliseconds(11.0),
            meta,
            ..TestResultSpec::fake()
        }));
        collector.push_result(&TestResult::new(TestResultSpec {
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
        }));
        collector.push_result(&TestResult::new(TestResultSpec {
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
        }));
        collector.push_result(&TestResult::new(TestResultSpec {
            status: Status::Pending,
            id: "id4".to_string(),
            display_name: "app::module::test4".to_string(),
            output: Some("Test 4 pending".to_string()),
            runtime: TestRuntime::Milliseconds(44.0),
            meta: Default::default(),
            ..TestResultSpec::fake()
        }));
        collector.push_result(&TestResult::new(TestResultSpec {
            status: Status::Skipped,
            id: "id5".to_string(),
            display_name: "app::module::test5".to_string(),
            output: None,
            runtime: TestRuntime::Milliseconds(55.0),
            meta: Default::default(),
            ..TestResultSpec::fake()
        }));

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json(&mut buf, &NativeRunnerSpecification::fake())
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_all_statuses__compact", json)
        }

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json_pretty(&mut buf, &NativeRunnerSpecification::fake())
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_all_statuses__pretty", json)
        }
    }

    #[test]
    fn generates_rwx_v1_json_for_successful_runs() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult::new(TestResultSpec {
            status: Status::Success,
            id: "id1".to_string(),
            display_name: "app::module::test1".to_string(),
            output: Some("Test 1 passed".to_string()),
            runtime: TestRuntime::Milliseconds(11.0),
            meta: Default::default(),
            ..TestResultSpec::fake()
        }));

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json(&mut buf, &NativeRunnerSpecification::fake())
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_successful_runs__compact", json)
        }

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json_pretty(&mut buf, &NativeRunnerSpecification::fake())
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_successful_runs__pretty", json)
        }
    }

    #[test]
    fn ruby_rspec_lang() {
        let collector = Collector::default();

        let runner_spec = NativeRunnerSpecification {
            name: "rspec-abq".to_string(),
            version: "1.0.0".to_string(),
            test_framework: Some("rspec".to_string()),
            test_framework_version: Some("4.2.1".to_string()),
            language: Some("ruby".to_string()),
            language_version: Some("3.1.0".to_string()),
            host: Some("abqmachine".to_string()),
        };

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, &runner_spec)
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
            test_framework: Some("jest".to_string()),
            test_framework_version: Some("29.3.1".to_string()),
            language: Some("javascript".to_string()),
            language_version: Some("node-16.0.0".to_string()),
            host: Some("abqnode-16.0.0".to_string()),
        };

        let mut buf = vec![];
        collector
            .write_json_pretty(&mut buf, &runner_spec)
            .expect("failed to write");
        let json = String::from_utf8(buf).expect("not utf8 JSON");
        insta::assert_snapshot!(json)
    }
}
