use std::fmt::Write;

use abq_utils::net_protocol::runners::{Milliseconds, Status, TestResult};

/// Formats a test result on a single line.
pub fn format_result_line(result: &TestResult) -> String {
    let mut buf = String::new();
    buf.push_str(&result.display_name);
    buf.push_str(": ");
    buf.push_str(format_status(result.status));
    buf.push('\n');
    buf
}

/// Formats a test result as a summary, possibly across multiple lines.
pub fn format_result_summary(result: &TestResult) -> String {
    // --- test/name: {status} ---
    // {output}
    // (completed in {runtime})

    let TestResult {
        status,
        id: _,
        display_name,
        output,
        runtime,
        meta: _,
    } = result;

    let output = output.as_deref().unwrap_or("<no output>");

    let mut buf = String::new();
    buf.push_str("--- ");
    buf.push_str(display_name);
    buf.push_str(": ");
    buf.push_str(format_status(*status));
    buf.push_str(" ---\n");
    buf.push_str(output);
    buf.push_str("\n(completed in ");
    buf.push_str(&format_duration(*runtime));
    buf.push_str(")\n");
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

fn format_duration(millis: Milliseconds) -> String {
    const MILLIS_IN_SECOND: u64 = 1000;
    const MILLIS_IN_MINUTE: u64 = 60 * MILLIS_IN_SECOND;

    let millis = millis as u64;
    let (minutes, millis) = (millis / MILLIS_IN_MINUTE, millis % MILLIS_IN_MINUTE);
    let (seconds, millis) = (millis / MILLIS_IN_SECOND, millis % MILLIS_IN_SECOND);

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

    use super::{format_duration, format_result_line, format_result_summary};
    use std::time::Duration;

    #[allow(clippy::identity_op)]
    fn default_result() -> TestResult {
        TestResult {
            status: Status::Success,
            id: "default id".to_owned(),
            display_name: "default name".to_owned(),
            output: Some("default output".to_owned()),
            runtime: (1 * 60 * 1000 + 15 * 1000 + 3) as _,
            meta: Default::default(),
        }
    }

    macro_rules! test_format {
        ($name:ident, $fn:ident, $item:expr, @$expect:literal) => {
            #[test]
            fn $name() {
                let formatted = $fn($item);
                insta::assert_snapshot!(formatted, @$expect);
            }
        };
    }

    test_format!(
        format_line_success, format_result_line,
        &TestResult {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: ok"
    );

    test_format!(
        format_line_failure, format_result_line,
        &TestResult {status: Status::Failure, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: FAILED"
    );

    test_format!(
        format_line_error, format_result_line,
        &TestResult {status: Status::Error, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: ERRORED"
    );

    test_format!(
        format_line_pending, format_result_line,
        &TestResult {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: pending"
    );

    test_format!(
        format_line_skipped, format_result_line,
        &TestResult {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: skipped"
    );

    test_format!(
        format_exact_minutes, format_duration,
        Duration::from_secs(360).as_millis() as _,
        @"6 m"
    );

    test_format!(
        format_exact_minutes_leftover_seconds_and_millis, format_duration,
        Duration::from_millis(6 * 60 * 1000 + 52 * 1000 + 35).as_millis() as _,
        @"6 m, 52 s, 35 ms"
    );

    test_format!(
        format_exact_seconds, format_duration,
        Duration::from_secs(15).as_millis() as _,
        @"15 s"
    );

    test_format!(
        format_exact_seconds_leftover_millis, format_duration,
        Duration::from_millis(15 * 1000 + 35).as_millis() as _,
        @"15 s, 35 ms"
    );

    test_format!(
        format_exact_millis, format_duration,
        Duration::from_millis(35).as_millis() as _,
        @"35 ms"
    );

    test_format!(
        format_summary_success, format_result_summary,
        &TestResult {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!".to_string()), ..default_result() },
        @r###"
    --- abq/test: ok ---
    Test passed!
    (completed in 1 m, 15 s, 3 ms)
    "###
    );

    test_format!(
        format_summary_failure, format_result_summary,
        &TestResult {status: Status::Failure, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() },
        @r###"
    --- abq/test: FAILED ---
    Assertion failed: 1 != 2
    (completed in 1 m, 15 s, 3 ms)
    "###
    );

    test_format!(
        format_summary_error, format_result_summary,
        &TestResult {status: Status::Error, display_name: "abq/test".to_string(), output: Some("Process at pid 72818 exited early with SIGTERM".to_string()), ..default_result() },
        @r###"
    --- abq/test: ERRORED ---
    Process at pid 72818 exited early with SIGTERM
    (completed in 1 m, 15 s, 3 ms)
    "###
    );

    test_format!(
        format_summary_pending, format_result_summary,
        &TestResult {status: Status::Pending, display_name: "abq/test".to_string(), output: Some(r#"Test not implemented yet for reason: "need to implement feature A""#.to_string()), ..default_result() },
        @r###"
    --- abq/test: pending ---
    Test not implemented yet for reason: "need to implement feature A"
    (completed in 1 m, 15 s, 3 ms)
    "###
    );

    test_format!(
        format_summary_skipped, format_result_summary,
        &TestResult {status: Status::Skipped, display_name: "abq/test".to_string(), output: Some(r#"Test skipped for reason: "only enabled on summer Fridays""#.to_string()), ..default_result() },
        @r###"
    --- abq/test: skipped ---
    Test skipped for reason: "only enabled on summer Fridays"
    (completed in 1 m, 15 s, 3 ms)
    "###
    );

    test_format!(
        format_summary_multiline, format_result_summary,
        &TestResult {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!\nTo see rendered webpage, see:\n\thttps://example.com\n".to_string()), ..default_result() },
        @r###"
    --- abq/test: ok ---
    Test passed!
    To see rendered webpage, see:
    	https://example.com

    (completed in 1 m, 15 s, 3 ms)
    "###
    );

    test_format!(
        format_summary_no_output, format_result_summary,
        &TestResult {status: Status::Success, display_name: "abq/test".to_string(), output: None, ..default_result() },
        @r###"
    --- abq/test: ok ---
    <no output>
    (completed in 1 m, 15 s, 3 ms)
    "###
    );
}
