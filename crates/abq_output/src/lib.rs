use std::{
    io::{self},
    ops::Deref,
    time::Duration,
};

use abq_utils::net_protocol::{
    entity::EntityId,
    runners::{CapturedOutput, Status, TestResult, TestResultSpec, TestRuntime},
};
use termcolor::{Color, ColorSpec, WriteColor};

/// Formats a test result on a single line.
pub fn format_result_line(
    writer: &mut impl WriteColor,
    result: &TestResult,
    is_first_result: bool,
    output_before: &Option<CapturedOutput>,
    output_after: &Option<CapturedOutput>,
) -> io::Result<()> {
    if let Some(output_before) = output_before {
        if !is_first_result {
            writeln!(writer)?;
        }
        format_worker_output(
            writer,
            result.source,
            &result.display_name,
            OutputOrdering::BeforeTest,
            output_before,
        )?;
    }

    write!(writer, "{}: ", &result.display_name)?;
    format_status(writer, &result.status)?;
    writeln!(writer)?;

    if let Some(output_after) = output_after {
        writeln!(writer)?;
        format_worker_output(
            writer,
            result.source,
            &result.display_name,
            OutputOrdering::AfterTest,
            output_after,
        )?;
    }

    Ok(())
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

pub enum SummaryKind {
    Test(TestResult),
    Output {
        when: OutputOrdering,
        worker: EntityId,
        test_name: String,
        output: CapturedOutput,
    },
}

pub enum OutputOrdering {
    BeforeTest,
    AfterTest,
}

pub fn format_summary(writer: &mut impl WriteColor, summary: SummaryKind) -> io::Result<()> {
    match summary {
        SummaryKind::Test(result) => format_test_result_summary(writer, &result),
        SummaryKind::Output {
            when,
            worker,
            test_name,
            output,
        } => format_worker_output(writer, worker, &test_name, when, &output),
    }
}

/// Formats a test result as a summary, possibly across multiple lines.
pub fn format_test_result_summary(
    writer: &mut impl WriteColor,
    result: &TestResult,
) -> io::Result<()> {
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

fn format_worker_output(
    writer: &mut impl WriteColor,
    worker: EntityId,
    test_name: &str,
    when: OutputOrdering,
    output: &CapturedOutput,
) -> io::Result<()> {
    // --- [worker {worker_id}] BEFORE|AFTER test_name ---
    // ----- STDOUT
    // {stdout}
    // ----- STDERR
    // {stderr}

    let CapturedOutput { stderr, stdout } = output;

    if stderr.is_empty() && stdout.is_empty() {
        // Don't write anything if we have nothing actionable
        return Ok(());
    }

    let when = match when {
        OutputOrdering::BeforeTest => "BEFORE",
        OutputOrdering::AfterTest => "AFTER",
    };

    let mut trailing_newlines = 0;

    writeln!(writer, "--- [worker {worker:?}] {when} {test_name} ---")?;

    if !stdout.is_empty() {
        // TODO: don't print if stdout is only whitespace
        writeln!(writer, "----- STDOUT")?;
        writer.write_all(stdout)?;
        trailing_newlines = push_newline_if_needed(writer, stdout)?;
    }

    if !stderr.is_empty() {
        // TODO: don't print if stderr is only whitespace
        writeln!(writer, "----- STDERR")?;
        writer.write_all(stderr)?;
        trailing_newlines = push_newline_if_needed(writer, stderr)?;
    }

    // Make sure we have at least one blank line between this output and the next
    for _ in 0..(2usize.saturating_sub(trailing_newlines)) {
        writeln!(writer)?;
    }

    Ok(())
}

fn push_newline_if_needed(writer: &mut impl WriteColor, bytes: &[u8]) -> io::Result<usize> {
    let mut trailing_newlines = 0;
    for &byte in bytes.iter().rev() {
        if !byte.is_ascii_whitespace() {
            break;
        }
        trailing_newlines += (byte == b'\n') as usize;
    }
    if trailing_newlines == 0 {
        writeln!(writer)?;
        trailing_newlines += 1;
    }
    Ok(trailing_newlines)
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
        runners::{CapturedOutput, Status, TestResult, TestResultSpec, TestRuntime},
    };

    use crate::{format_duration_to_partial_seconds, format_result_dot};

    use super::{format_duration, format_result_line, format_test_result_summary};
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
        ($name:ident, $fn:ident, $item:expr, $out_before:expr, $out_after:expr, @$expect_colored:literal) => {
            #[test]
            fn $name() {
                // Test colored output
                let mut buf = TestColorWriter(vec![]);
                $fn(&mut buf, $item, true, $out_before, $out_after).unwrap();
                let formatted = String::from_utf8(buf.0).unwrap();
                insta::assert_snapshot!(formatted, @$expect_colored);
            }
        };
    }

    test_format!(
        format_line_success, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <green>ok<reset>
    "###
    );

    test_format!(
        format_line_failure, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <red>FAILED<reset>
    "###
    );

    test_format!(
        format_line_error, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <red>ERRORED<reset>
    "###
    );

    test_format!(
        format_line_pending, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <yellow>pending<reset>
    "###
    );

    test_format!(
        format_line_skipped, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_line_output_before_after, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(CapturedOutput { stderr: b"stderr\nbefore".to_vec(), stdout: b"stdout\nbefore\n".to_vec() }),
        &Some(CapturedOutput { stderr: b"stderr\nafter".to_vec(), stdout: b"stdout\nafter\n".to_vec() }),
        @r###"
    --- [worker 07070707-0707-0707-0707-070707070707] BEFORE abq/test ---
    ----- STDOUT
    stdout
    before
    ----- STDERR
    stderr
    before

    abq/test: <yellow>skipped<reset>

    --- [worker 07070707-0707-0707-0707-070707070707] AFTER abq/test ---
    ----- STDOUT
    stdout
    after
    ----- STDERR
    stderr
    after

    "###
    );

    test_format!(
        format_line_output_only_before, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(CapturedOutput { stderr: b"stderr\nbefore".to_vec(), stdout: b"stdout\nbefore\n".to_vec() }),
        &None,
        @r###"
    --- [worker 07070707-0707-0707-0707-070707070707] BEFORE abq/test ---
    ----- STDOUT
    stdout
    before
    ----- STDERR
    stderr
    before

    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_line_output_only_before_only_stdout, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(CapturedOutput { stderr: b"".to_vec(), stdout: b"stdout\nbefore\n".to_vec() }),
        &None,
        @r###"
    --- [worker 07070707-0707-0707-0707-070707070707] BEFORE abq/test ---
    ----- STDOUT
    stdout
    before

    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_line_output_only_before_only_stderr, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(CapturedOutput { stderr: b"stderr\nbefore".to_vec(), stdout: b"".to_vec() }),
        &None,
        @r###"
    --- [worker 07070707-0707-0707-0707-070707070707] BEFORE abq/test ---
    ----- STDERR
    stderr
    before

    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_line_output_only_after, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None,
        &Some(CapturedOutput { stderr: b"stderr\nafter".to_vec(), stdout: b"stdout\nafter\n".to_vec() }),
        @r###"
    abq/test: <yellow>skipped<reset>

    --- [worker 07070707-0707-0707-0707-070707070707] AFTER abq/test ---
    ----- STDOUT
    stdout
    after
    ----- STDERR
    stderr
    after

    "###
    );

    test_format!(
        format_line_output_only_after_only_stdout, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None,
        &Some(CapturedOutput { stdout: b"stdout\nafter".to_vec(), stderr: b"".to_vec() }),
        @r###"
    abq/test: <yellow>skipped<reset>

    --- [worker 07070707-0707-0707-0707-070707070707] AFTER abq/test ---
    ----- STDOUT
    stdout
    after

    "###
    );

    test_format!(
        format_line_output_only_after_only_stderr, format_result_line,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None,
        &Some(CapturedOutput { stderr: b"stderr\nafter".to_vec(), stdout: b"".to_vec() }),
        @r###"
    abq/test: <yellow>skipped<reset>

    --- [worker 07070707-0707-0707-0707-070707070707] AFTER abq/test ---
    ----- STDERR
    stderr
    after

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
        format_summary_success, format_test_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    Test passed!
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_failure, format_test_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> ---
    Assertion failed: 1 != 2
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_error, format_test_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Process at pid 72818 exited early with SIGTERM".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>ERRORED<reset> ---
    Process at pid 72818 exited early with SIGTERM
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_pending, format_test_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), output: Some(r#"Test not implemented yet for reason: "need to implement feature A""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>pending<reset> ---
    Test not implemented yet for reason: "need to implement feature A"
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_skipped, format_test_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), output: Some(r#"Test skipped for reason: "only enabled on summer Fridays""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>skipped<reset> ---
    Test skipped for reason: "only enabled on summer Fridays"
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );

    test_format!(
        format_summary_multiline, format_test_result_summary,
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
        format_summary_no_output, format_test_result_summary,
        &TestResult::new(EntityId::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: None, ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    <no output>
    (completed in 1 m, 15 s, 3 ms; worker [07070707-0707-0707-0707-070707070707])
    "###
    );
}
