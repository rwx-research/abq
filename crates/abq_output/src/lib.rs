use std::fmt::Write;
use std::time::Duration;

use abq_utils::net_protocol::runners::{Status, TestResult};

pub fn format_result_line(result: &TestResult) -> String {
    let mut buf = String::new();
    buf.push_str(&result.display_name);
    buf.push_str(": ");
    buf.push_str(format_status(result.status));
    buf
}

fn format_status(status: Status) -> &'static str {
    match status {
        Status::Failure => "FAILED",
        Status::Success => "ok",
        Status::Error => "ERRORED",
        Status::Pending => "pending",
        Status::Skipped => "skipped",
    }
}

#[allow(unused)]
fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    let seconds = millis / 1000;
    let minutes = seconds / 60;

    let mut buf = String::new();
    if minutes > 0 {
        write!(&mut buf, "{} m", minutes).unwrap();
    }
    if seconds > 0 {
        if !buf.is_empty() {
            buf.push_str(", ");
        }
        write!(&mut buf, "{} s", seconds).unwrap();
    }
    if millis > 0 {
        if !buf.is_empty() {
            buf.push_str(", ");
        }
        write!(&mut buf, "{} ms", millis).unwrap();
    }

    buf
}

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::runners::{Status, TestResult};

    use super::format_result_line;

    fn default_result() -> TestResult {
        TestResult {
            status: Status::Success,
            id: "default id".to_owned(),
            display_name: "default name".to_owned(),
            output: Some("default output".to_owned()),
            runtime: 0.,
            meta: Default::default(),
        }
    }

    macro_rules! test_format {
        ($name:ident, $result:expr, @$expect:literal) => {
            #[test]
            fn $name() {
                let formatted = format_result_line(&$result);
                insta::assert_snapshot!(formatted, @$expect);
            }
        };
    }

    test_format!(
        format_line_success,
        TestResult {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: ok"
    );

    test_format!(
        format_line_failure,
        TestResult {status: Status::Failure, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: FAILED"
    );

    test_format!(
        format_line_error,
        TestResult {status: Status::Error, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: ERRORED"
    );

    test_format!(
        format_line_pending,
        TestResult {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: pending"
    );

    test_format!(
        format_line_skipped,
        TestResult {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: skipped"
    );
}
