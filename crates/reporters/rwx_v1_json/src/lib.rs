use serde_derive::{Deserialize, Serialize};
use std::io::Write;

use abq_utils::net_protocol::runners::{Status, TestResult};

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
        let status = match test_result.status {
            Status::Error | Status::Failure | Status::PrivateNativeRunnerError => {
                AttemptStatus::Failed {
                    exception: None,
                    message: test_result.output.clone(),
                    backtrace: None,
                }
            }
            Status::Pending => AttemptStatus::Pended { message: None },
            Status::Skipped => AttemptStatus::Skipped { message: None },
            Status::Success => AttemptStatus::Successful,
        };

        let test = Test {
            id: None,
            name: test_result.display_name.clone(),
            lineage: None,
            location: None,
            attempt: Attempt {
                duration: (test_result.runtime * 1000000.0).round() as Nanoseconds,
                meta: Some(test_result.meta.clone()),
                status,
                stderr: None,
                stdout: None,
                started_at: None,
                finished_at: None,
            },
            past_attempts: None,
        };

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

        self.tests.push(test)
    }

    pub fn write_json(self, writer: impl Write) -> Result<(), String> {
        serde_json::to_writer(writer, &self.test_results()).map_err(|e| e.to_string())
    }

    pub fn write_json_pretty(self, writer: impl Write) -> Result<(), String> {
        serde_json::to_writer_pretty(writer, &self.test_results()).map_err(|e| e.to_string())
    }

    fn test_results(self) -> TestResults {
        TestResults {
            schema:
                "https://raw.githubusercontent.com/rwx-research/test-results-schema/main/v1.json"
                    .to_string(),
            framework: Framework {
                kind: "other".to_string(),
                language: "other".to_string(),
                provided_kind: None,
                provided_language: None,
            },
            summary: self.summary,
            tests: self.tests,
        }
    }
}

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::runners::{Status, TestResult};

    use crate::Collector;

    #[test]
    fn generates_rwx_v1_json_for_all_statuses() {
        let mut collector = Collector::default();
        let mut meta = serde_json::Map::new();
        meta.insert(
            "some".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        collector.push_result(&TestResult {
            status: Status::Success,
            id: "id1".to_string(),
            display_name: "app::module::test1".to_string(),
            output: Some("Test 1 passed".to_string()),
            runtime: 11.0,
            meta,
        });
        collector.push_result(&TestResult {
            status: Status::Failure,
            id: "id2".to_string(),
            display_name: "app::module::test2".to_string(),
            output: Some("Test 2 failed".to_string()),
            runtime: 22.0,
            meta: Default::default(),
        });
        collector.push_result(&TestResult {
            status: Status::Error,
            id: "id3".to_string(),
            display_name: "app::module::test3".to_string(),
            output: None,
            runtime: 33.0,
            meta: Default::default(),
        });
        collector.push_result(&TestResult {
            status: Status::Pending,
            id: "id4".to_string(),
            display_name: "app::module::test4".to_string(),
            output: Some("Test 4 pending".to_string()),
            runtime: 44.0,
            meta: Default::default(),
        });
        collector.push_result(&TestResult {
            status: Status::Skipped,
            id: "id5".to_string(),
            display_name: "app::module::test5".to_string(),
            output: None,
            runtime: 55.0,
            meta: Default::default(),
        });

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json(&mut buf)
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_all_statuses__compact", json)
        }

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json_pretty(&mut buf)
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_all_statuses__pretty", json)
        }
    }

    #[test]
    fn generates_rwx_v1_json_for_successful_runs() {
        let mut collector = Collector::default();

        collector.push_result(&TestResult {
            status: Status::Success,
            id: "id1".to_string(),
            display_name: "app::module::test1".to_string(),
            output: Some("Test 1 passed".to_string()),
            runtime: 11.0,
            meta: Default::default(),
        });

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json(&mut buf)
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_successful_runs__compact", json)
        }

        {
            let mut buf = vec![];
            collector
                .clone()
                .write_json_pretty(&mut buf)
                .expect("failed to write");
            let json = String::from_utf8(buf).expect("not utf8 JSON");
            insta::assert_snapshot!("generates_rwx_v1_json_for_successful_runs__pretty", json)
        }
    }
}