use std::{io::Write, ops::Deref};

use abq_utils::net_protocol::runners::{Status, TestResult, TestResultSpec, TestRuntime};
use junit_report as junit;

pub struct Collector {
    test_suite: junit::TestSuite,
}

fn get_current_timestamp() -> junit::OffsetDateTime {
    if cfg!(test) {
        junit::OffsetDateTime::UNIX_EPOCH
    } else {
        junit::OffsetDateTime::now_utc()
    }
}

impl Collector {
    pub fn new(test_suite_name: &str) -> Self {
        let current_timestamp = get_current_timestamp();
        let mut test_suite = junit::TestSuite::new(test_suite_name);
        test_suite.set_timestamp(current_timestamp);

        Self { test_suite }
    }

    #[inline(always)]
    pub fn push_result(&mut self, test_result: &TestResult) {
        let TestResultSpec {
            status,
            id: _,
            display_name,
            output,
            runtime,
            // TODO: use all this
            meta: _,
            location: _,
            started_at: _,
            finished_at: _,
            lineage: _,
            past_attempts: _,
            other_errors: _,
            stderr: _,
            stdout: _,
            timestamp: _,
        } = test_result.deref();

        let duration = match runtime {
            TestRuntime::Milliseconds(ms) => junit::Duration::milliseconds(*ms as _),
            TestRuntime::Nanoseconds(ns) => junit::Duration::nanoseconds(*ns as _),
        };

        let raw_output = output.to_owned().unwrap_or_default();
        // ANSI escape codes can be valid UTF-8, but they are not valid XML -
        // strip them from our XML output.
        let cleaned_output = {
            let cleaned_bytes = strip_ansi_escapes::strip(raw_output)
                .expect("writing to a fresh buffer must not fail");

            // The test output was valid UTF-8 before we stripped the ANSI escape codes; the
            // closure consisting of stripping the codes, which themselves are valid UTF-8,
            // which must leave us with a UTF-8 valid string.
            String::from_utf8(cleaned_bytes).expect("ABQ results must always be valid UTF-8")
        };

        let test_case = match status {
            Status::Failure {
                exception: _, // TODO
                backtrace: _,
            } => {
                // TODO: expose optional failure type on `TestResult`?
                let failure_type = "failure";

                junit::TestCase::failure(display_name, duration, failure_type, &cleaned_output)
            }
            Status::Success => junit::TestCase::success(display_name, duration),
            Status::Error {
                exception: _, // TODO
                backtrace: _,
            }
            | Status::PrivateNativeRunnerError => {
                // TODO: expose optional error type on `TestResult`?
                let error_type = "error";

                junit::TestCase::error(display_name, duration, error_type, &cleaned_output)
            }
            Status::Pending => junit::TestCase::skipped(display_name),
            Status::Skipped => junit::TestCase::skipped(display_name),
            Status::Todo => junit::TestCase::skipped(display_name),
            Status::TimedOut => {
                let error_type = "timed out";

                junit::TestCase::error(display_name, duration, error_type, &cleaned_output)
            }
        };

        self.test_suite.add_testcase(test_case);
    }

    pub fn extend_with_results<'a>(&mut self, results: impl IntoIterator<Item = &'a TestResult>) {
        let results = results.into_iter();
        let size_hint = results.size_hint();
        let size_hint = size_hint.1.unwrap_or(size_hint.0);

        self.test_suite.testcases.reserve(size_hint);

        results.for_each(|result| self.push_result(result));
    }

    pub fn write_xml(self, writer: impl Write) -> Result<(), String> {
        let mut junit_report = junit::Report::new();
        junit_report.add_testsuite(self.test_suite);
        junit_report.write_xml(writer).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::{
        entity::RunnerMeta,
        runners::{Status, TestResult, TestResultSpec, TestRuntime::Milliseconds},
    };

    use crate::Collector;

    #[test]
    fn generates_junit_xml_for_all_statuses() {
        let mut collector = Collector::new("suite");
        collector.extend_with_results(&[
            TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    status: Status::Success,
                    id: "id1".to_string(),
                    display_name: "app::module::test1".to_string(),
                    output: Some("Test 1 passed".to_string()),
                    runtime: Milliseconds(11.0),
                    meta: Default::default(),
                    ..TestResultSpec::fake()
                },
            ),
            TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    status: Status::Failure {
                        exception: Some("test-exception".to_string()),
                        backtrace: Some(vec![
                            "file1.cpp:10".to_string(),
                            "file2.cpp:20".to_string(),
                        ]),
                    },
                    id: "id2".to_string(),
                    display_name: "app::module::test2".to_string(),
                    output: Some("Test 2 failed".to_string()),
                    runtime: Milliseconds(22.0),
                    meta: Default::default(),
                    ..TestResultSpec::fake()
                },
            ),
            TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    status: Status::Error {
                        exception: None,
                        backtrace: None,
                    },
                    id: "id3".to_string(),
                    display_name: "app::module::test3".to_string(),
                    output: Some("Test 3 errored".to_string()),
                    runtime: Milliseconds(33.0),
                    meta: Default::default(),
                    ..TestResultSpec::fake()
                },
            ),
            TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    status: Status::Pending,
                    id: "id4".to_string(),
                    display_name: "app::module::test4".to_string(),
                    output: Some("Test 4 pending".to_string()),
                    runtime: Milliseconds(44.0),
                    meta: Default::default(),
                    ..TestResultSpec::fake()
                },
            ),
            TestResult::new(
                RunnerMeta::fake(),
                TestResultSpec {
                    status: Status::Skipped,
                    id: "id5".to_string(),
                    display_name: "app::module::test5".to_string(),
                    output: Some("Test 5 skipped".to_string()),
                    runtime: Milliseconds(55.0),
                    meta: Default::default(),
                    ..TestResultSpec::fake()
                },
            ),
        ]);

        let mut buf = vec![];
        collector.write_xml(&mut buf).expect("failed to write");
        let xml = String::from_utf8(buf).expect("not utf8 XML");

        assert_eq!(
            xml,
            concat!(
                r#"<?xml version="1.0" encoding="utf-8"?>"#,
                r#"<testsuites>"#,
                r#"<testsuite id="0" name="suite" package="testsuite/suite" tests="5" errors="1" failures="1" hostname="localhost" timestamp="1970-01-01T00:00:00Z" time="0.066">"#,
                r#"<testcase name="app::module::test1" time="0.011"/>"#,
                r#"<testcase name="app::module::test2" time="0.022">"#,
                r#"<failure type="failure" message="Test 2 failed"/>"#,
                r#"</testcase>"#,
                r#"<testcase name="app::module::test3" time="0.033">"#,
                r#"<error type="error" message="Test 3 errored"/>"#,
                r#"</testcase>"#,
                r#"<testcase name="app::module::test4" time="0">"#,
                r#"<skipped/>"#,
                r#"</testcase>"#,
                r#"<testcase name="app::module::test5" time="0">"#,
                r#"<skipped/>"#,
                r#"</testcase>"#,
                r#"</testsuite>"#,
                r#"</testsuites>"#,
            )
        );
    }

    #[test]
    fn extend_appends_tests() {
        let mut collector = Collector::new("suite");
        collector.extend_with_results(&[TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Success,
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("Test 1 passed".to_string()),
                runtime: Milliseconds(11.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        )]);

        collector.extend_with_results(&[TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id2".to_string(),
                display_name: "app::module::test2".to_string(),
                output: Some("Test 2 failed".to_string()),
                runtime: Milliseconds(22.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        )]);

        let mut buf = vec![];
        collector.write_xml(&mut buf).expect("failed to write");
        let xml = String::from_utf8(buf).expect("not utf8 XML");

        assert_eq!(
            xml,
            concat!(
                r#"<?xml version="1.0" encoding="utf-8"?>"#,
                r#"<testsuites>"#,
                r#"<testsuite id="0" name="suite" package="testsuite/suite" tests="2" errors="0" failures="1" hostname="localhost" timestamp="1970-01-01T00:00:00Z" time="0.033">"#,
                r#"<testcase name="app::module::test1" time="0.011"/>"#,
                r#"<testcase name="app::module::test2" time="0.022">"#,
                r#"<failure type="failure" message="Test 2 failed"/>"#,
                r#"</testcase>"#,
                r#"</testsuite>"#,
                r#"</testsuites>"#
            )
        );
    }

    #[test]
    fn failure_with_empty_output_prints_empty_output() {
        let mut collector = Collector::new("suite");
        collector.extend_with_results(&[TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Failure {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: None,
                runtime: Milliseconds(11.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        )]);

        let mut buf = vec![];
        collector.write_xml(&mut buf).expect("failed to write");
        let xml = String::from_utf8(buf).expect("not utf8 XML");

        assert_eq!(
            xml,
            concat!(
                r#"<?xml version="1.0" encoding="utf-8"?>"#,
                r#"<testsuites>"#,
                r#"<testsuite id="0" name="suite" package="testsuite/suite" tests="1" errors="0" failures="1" hostname="localhost" timestamp="1970-01-01T00:00:00Z" time="0.011">"#,
                r#"<testcase name="app::module::test1" time="0.011">"#,
                r#"<failure type="failure" message=""/>"#,
                r#"</testcase>"#,
                r#"</testsuite>"#,
                r#"</testsuites>"#,
            )
        );
    }

    #[test]
    fn error_with_empty_output_prints_empty_output() {
        let mut collector = Collector::new("suite");
        collector.extend_with_results(&[TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Error {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: None,
                runtime: Milliseconds(11.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        )]);

        let mut buf = vec![];
        collector.write_xml(&mut buf).expect("failed to write");
        let xml = String::from_utf8(buf).expect("not utf8 XML");

        assert_eq!(
            xml,
            concat!(
                r#"<?xml version="1.0" encoding="utf-8"?>"#,
                r#"<testsuites>"#,
                r#"<testsuite id="0" name="suite" package="testsuite/suite" tests="1" errors="1" failures="0" hostname="localhost" timestamp="1970-01-01T00:00:00Z" time="0.011">"#,
                r#"<testcase name="app::module::test1" time="0.011">"#,
                r#"<error type="error" message=""/>"#,
                r#"</testcase>"#,
                r#"</testsuite>"#,
                r#"</testsuites>"#
            )
        );
    }

    #[test]
    fn strip_ansi_escape_codes() {
        let mut collector = Collector::new("suite");
        collector.extend_with_results(&[TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Error {
                    exception: None,
                    backtrace: None,
                },
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some(String::from_utf8(b"\x1b[32mRESULT\x1b[m of test".to_vec()).unwrap()),
                runtime: Milliseconds(11.0),
                meta: Default::default(),
                ..TestResultSpec::fake()
            },
        )]);

        let mut buf = vec![];
        collector.write_xml(&mut buf).expect("failed to write");
        let xml = String::from_utf8(buf).expect("not utf8 XML");

        assert_eq!(
            xml,
            concat!(
                r#"<?xml version="1.0" encoding="utf-8"?>"#,
                r#"<testsuites>"#,
                r#"<testsuite id="0" name="suite" package="testsuite/suite" tests="1" errors="1" failures="0" hostname="localhost" timestamp="1970-01-01T00:00:00Z" time="0.011">"#,
                r#"<testcase name="app::module::test1" time="0.011">"#,
                r#"<error type="error" message="RESULT of test"/>"#,
                r#"</testcase>"#,
                r#"</testsuite>"#,
                r#"</testsuites>"#
            )
        );
    }
}
