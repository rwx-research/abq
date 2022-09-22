use std::io::Write;

use abq_utils::net_protocol::runners::{Status, TestResult};
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
        let TestResult {
            status,
            id: _,
            display_name,
            output,
            runtime,
            meta: _,
        } = test_result;

        let duration = junit::Duration::milliseconds(*runtime as _);

        let test_case = match status {
            Status::Failure => {
                // TODO: expose optional failure type on `TestResult`?
                let failure_type = "failure";

                junit::TestCase::failure(
                    display_name,
                    duration,
                    failure_type,
                    output.as_deref().unwrap_or_default(),
                )
            }
            Status::Success => junit::TestCase::success(display_name, duration),
            Status::Error => {
                // TODO: expose optional error type on `TestResult`?
                let error_type = "error";

                junit::TestCase::error(
                    display_name,
                    duration,
                    error_type,
                    output.as_deref().unwrap_or_default(),
                )
            }
            Status::Pending => junit::TestCase::skipped(display_name),
            Status::Skipped => junit::TestCase::skipped(display_name),
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
    use abq_utils::net_protocol::runners::{Status, TestResult};

    use crate::Collector;

    #[test]
    fn generates_junit_xml_for_all_statuses() {
        let mut collector = Collector::new("suite");
        collector.extend_with_results(&[
            TestResult {
                status: Status::Success,
                id: "id1".to_string(),
                display_name: "app::module::test1".to_string(),
                output: Some("Test 1 passed".to_string()),
                runtime: 11.0,
                meta: Default::default(),
            },
            TestResult {
                status: Status::Failure,
                id: "id2".to_string(),
                display_name: "app::module::test2".to_string(),
                output: Some("Test 2 failed".to_string()),
                runtime: 22.0,
                meta: Default::default(),
            },
            TestResult {
                status: Status::Error,
                id: "id3".to_string(),
                display_name: "app::module::test3".to_string(),
                output: Some("Test 3 errored".to_string()),
                runtime: 33.0,
                meta: Default::default(),
            },
            TestResult {
                status: Status::Pending,
                id: "id4".to_string(),
                display_name: "app::module::test4".to_string(),
                output: Some("Test 4 pending".to_string()),
                runtime: 44.0,
                meta: Default::default(),
            },
            TestResult {
                status: Status::Skipped,
                id: "id5".to_string(),
                display_name: "app::module::test5".to_string(),
                output: Some("Test 5 skipped".to_string()),
                runtime: 55.0,
                meta: Default::default(),
            },
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
        collector.extend_with_results(&[TestResult {
            status: Status::Success,
            id: "id1".to_string(),
            display_name: "app::module::test1".to_string(),
            output: Some("Test 1 passed".to_string()),
            runtime: 11.0,
            meta: Default::default(),
        }]);

        collector.extend_with_results(&[TestResult {
            status: Status::Failure,
            id: "id2".to_string(),
            display_name: "app::module::test2".to_string(),
            output: Some("Test 2 failed".to_string()),
            runtime: 22.0,
            meta: Default::default(),
        }]);

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
        collector.extend_with_results(&[TestResult {
            status: Status::Failure,
            id: "id1".to_string(),
            display_name: "app::module::test1".to_string(),
            output: None,
            runtime: 11.0,
            meta: Default::default(),
        }]);

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
        collector.extend_with_results(&[TestResult {
            status: Status::Error,
            id: "id1".to_string(),
            display_name: "app::module::test1".to_string(),
            output: None,
            runtime: 11.0,
            meta: Default::default(),
        }]);

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
}
