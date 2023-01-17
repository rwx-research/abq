use std::{io, ops::Deref, time::Duration};

use abq_utils::net_protocol::runners::{Status, TestResult, TestResultSpec, TestRuntime};
use termcolor::{Color, ColorSpec, WriteColor};

/// Formats a test result on a single line.
pub fn format_result_line(writer: &mut impl WriteColor, result: &TestResult) -> io::Result<()> {
    write!(writer, "{}: ", &result.display_name)?;
    format_status(writer, &result.status)?;
    writeln!(writer)
}

/// Formats a test result as a single dot.
pub fn format_result_dot(writer: &mut impl WriteColor, result: &TestResult) -> io::Result<()> {
    let dot = match result.status {
        Status::Failure { .. } | Status::TimedOut => "F",
        Status::Success => ".",
        Status::Error { .. } | Status::PrivateNativeRunnerError => "E",
        Status::Pending => "P",
        Status::Skipped | Status::Todo => "S",
    };
    with_color(writer, status_color(&result.status), |w| write!(w, "{dot}"))
}

/// Formats a test result as a summary, possibly across multiple lines.
pub fn format_result_summary(writer: &mut impl WriteColor, result: &TestResult) -> io::Result<()> {
    // --- test/name: {status} ---
    // {output}
    // (completed in {runtime}; worker [{worker_id}])

    let TestResultSpec {
        status,
        id: _,
        display_name,
        output,
        runtime,
        meta: _,
        // TODO
        location: _,
        started_at: _,
        finished_at: _,
        lineage: _,
        past_attempts: _,
        other_errors: _,
        stderr: _,
        stdout: _,
    } = result.deref();

    let output = output.as_deref().unwrap_or("<no output>");

    write!(writer, "--- {display_name}: ")?;
    format_status(writer, status)?;
    writeln!(writer, " ---")?;
    writeln!(writer, "{output}")?;
    write!(writer, "(completed in ")?;
    format_duration(writer, *runtime)?;
    writeln!(writer, "; worker [{:?}])", result.source)
}

fn status_color(status: &Status) -> Color {
    match status {
        Status::Success { .. } => Color::Green,
        Status::Failure { .. }
        | Status::Error { .. }
        | Status::TimedOut
        | Status::PrivateNativeRunnerError => Color::Red,
        Status::Pending | Status::Todo => Color::Yellow,
        Status::Skipped => Color::Yellow,
    }
}

fn with_color<W>(
    writer: &mut W,
    color: Color,
    f: impl FnOnce(&mut W) -> io::Result<()>,
) -> io::Result<()>
where
    W: WriteColor,
{
    writer.set_color(ColorSpec::new().set_fg(Some(color)))?;
    f(writer)?;
    writer.reset()
}

fn format_status(writer: &mut impl WriteColor, status: &Status) -> io::Result<()> {
    let color = status_color(status);
    let status = match status {
        Status::Failure { .. } => "FAILED",
        Status::Success => "ok",
        Status::Error { .. } | Status::PrivateNativeRunnerError => "ERRORED",
        Status::TimedOut => "TIMED OUT",
        Status::Pending => "pending",
        Status::Todo => "todo",
        Status::Skipped => "skipped",
    };

    with_color(writer, color, |w| write!(w, "{status}"))
}

const MILLIS_IN_SECOND: u64 = 1000;

pub fn format_duration(writer: &mut impl io::Write, duration: TestRuntime) -> io::Result<()> {
    const MILLIS_IN_MINUTE: u64 = 60 * MILLIS_IN_SECOND;

    let millis = match duration {
        TestRuntime::Milliseconds(ms) => ms as u64,
        TestRuntime::Nanoseconds(ns) => ns / 1_000_000,
    };
    let (minutes, millis) = (millis / MILLIS_IN_MINUTE, millis % MILLIS_IN_MINUTE);
    let (seconds, millis) = (millis / MILLIS_IN_SECOND, millis % MILLIS_IN_SECOND);

    let mut written = false;
    if minutes > 0 {
        write!(writer, "{} m", minutes)?;
        written = true;
    }
    if seconds > 0 {
        if written {
            write!(writer, ", ")?;
        }
        write!(writer, "{} s", seconds)?;
        written = true;
    }
    if millis > 0 {
        if written {
            write!(writer, ", ")?;
        }
        write!(writer, "{} ms", millis)?;
    }

    Ok(())
}

pub fn format_duration_to_partial_seconds(
    writer: &mut impl io::Write,
    duration: Duration,
) -> io::Result<()> {
    let millis = duration.as_millis() as f64;
    let seconds = millis / (MILLIS_IN_SECOND as f64);
    write!(writer, "{seconds:.2} seconds")
}

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::{
        entity::EntityId,
        runners::{Status, TestResult, TestResultSpec, TestRuntime},
    };

    use crate::{format_duration_to_partial_seconds, format_result_dot};

    use super::{format_duration, format_result_line, format_result_summary};
    use std::{io, time::Duration};

    #[allow(clippy::identity_op)]
    fn default_result() -> TestResultSpec {
        TestResultSpec {
            status: Status::Success,
            id: "default id".to_owned(),
            display_name: "default name".to_owned(),
            output: Some("default output".to_owned()),
            runtime: TestRuntime::Milliseconds((1 * 60 * 1000 + 15 * 1000 + 3) as _),
            meta: Default::default(),
            ..TestResultSpec::fake()
        }
    }

    struct TestColorWriter<W: io::Write>(W);

    impl<W: io::Write> io::Write for TestColorWriter<W> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }

    impl<W: io::Write> termcolor::WriteColor for TestColorWriter<W> {
        fn supports_color(&self) -> bool {
            true
        }

        fn set_color(&mut self, spec: &termcolor::ColorSpec) -> io::Result<()> {
            assert!(spec.bg().is_none());
            assert!(!spec.bold());
            assert!(!spec.intense());
            assert!(!spec.underline());
            assert!(!spec.dimmed());
            assert!(!spec.italic());
            assert!(spec.reset());

            use termcolor::Color::*;
            let color = match spec.fg().unwrap() {
                Black => "black",
                Blue => "blue",
                Green => "green",
                Red => "red",
                Cyan => "cyan",
                Magenta => "magenta",
                Yellow => "yellow",
                White => "white",
                _ => unreachable!(),
            };

            write!(&mut self.0, "<{color}>")
        }

        fn reset(&mut self) -> io::Result<()> {
            write!(&mut self.0, "<reset>")
        }
    }

    macro_rules! test_format {
        ($name:ident, $fn:ident, $item:expr, @$expect_colored:literal) => {
            #[test]
            fn $name() {
                // Test colored output
                let mut buf = TestColorWriter(vec![]);
                $fn(&mut buf, $item).unwrap();
                let formatted = String::from_utf8(buf.0).unwrap();
                insta::assert_snapshot!(formatted, @$expect_colored);
            }
        };
    }

    test_format!(
        format_line_success, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <green>ok<reset>
    "###
    );

    test_format!(
        format_line_failure, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <red>FAILED<reset>
    "###
    );

    test_format!(
        format_line_error, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <red>ERRORED<reset>
    "###
    );

    test_format!(
        format_line_pending, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <yellow>pending<reset>
    "###
    );

    test_format!(
        format_line_skipped, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_dot_success, format_result_dot,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() }),
        @"<green>.<reset>"
    );

    test_format!(
        format_dot_failure, format_result_dot,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @"<red>F<reset>"
    );

    test_format!(
        format_dot_error, format_result_dot,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @"<red>E<reset>"
    );

    test_format!(
        format_dot_pending, format_result_dot,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() }),
        @"<yellow>P<reset>"
    );

    test_format!(
        format_dot_skipped, format_result_dot,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        @"<yellow>S<reset>"
    );

    test_format!(
        format_exact_minutes, format_duration,
        TestRuntime::Milliseconds(Duration::from_secs(360).as_millis() as _),
        @"6 m"
    );

    test_format!(
        format_exact_minutes_leftover_seconds_and_millis, format_duration,
        TestRuntime::Milliseconds(Duration::from_millis(6 * 60 * 1000 + 52 * 1000 + 35).as_millis() as _),
        @"6 m, 52 s, 35 ms"
    );

    test_format!(
        format_exact_seconds, format_duration,
        TestRuntime::Milliseconds(Duration::from_secs(15).as_millis() as _),
        @"15 s"
    );

    test_format!(
        format_exact_seconds_leftover_millis, format_duration,
        TestRuntime::Milliseconds(Duration::from_millis(15 * 1000 + 35).as_millis() as _),
        @"15 s, 35 ms"
    );

    test_format!(
        format_exact_seconds_leftover_millis_from_nanos, format_duration,
        TestRuntime::Nanoseconds(Duration::from_millis(15 * 1000 + 35).as_nanos() as _),
        @"15 s, 35 ms"
    );

    test_format!(
        format_exact_millis, format_duration,
        TestRuntime::Milliseconds(Duration::from_millis(35).as_millis() as _),
        @"35 ms"
    );

    test_format!(
        format_exact_millis_from_nanos, format_duration,
        TestRuntime::Nanoseconds(35_000_000),
        @"35 ms"
    );

    test_format!(
        format_exact_minutes_partial_seconds, format_duration_to_partial_seconds,
        Duration::from_secs(360),
        @"360.00 seconds"
    );

    test_format!(
        format_exact_minutes_leftover_seconds_and_millis_partial_seconds, format_duration_to_partial_seconds,
        Duration::from_millis(6 * 60 * 1000 + 52 * 1000 + 35),
        @"412.04 seconds"
    );

    test_format!(
        format_exact_seconds_partial_seconds, format_duration_to_partial_seconds,
        Duration::from_secs(15),
        @"15.00 seconds"
    );

    test_format!(
        format_exact_seconds_leftover_millis_partial_seconds, format_duration_to_partial_seconds,
        Duration::from_millis(15 * 1000 + 35),
        @"15.04 seconds"
    );

    test_format!(
        format_exact_millis_partial_seconds, format_duration_to_partial_seconds,
        Duration::from_millis(35),
        @"0.04 seconds"
    );

    test_format!(
        format_summary_success, format_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    Test passed!
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_failure, format_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> ---
    Assertion failed: 1 != 2
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_error, format_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Process at pid 72818 exited early with SIGTERM".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>ERRORED<reset> ---
    Process at pid 72818 exited early with SIGTERM
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_pending, format_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), output: Some(r#"Test not implemented yet for reason: "need to implement feature A""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>pending<reset> ---
    Test not implemented yet for reason: "need to implement feature A"
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_skipped, format_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), output: Some(r#"Test skipped for reason: "only enabled on summer Fridays""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>skipped<reset> ---
    Test skipped for reason: "only enabled on summer Fridays"
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_multiline, format_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!\nTo see rendered webpage, see:\n\thttps://example.com\n".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    Test passed!
    To see rendered webpage, see:
    	https://example.com

    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_no_output, format_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: None, ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    <no output>
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );
}
