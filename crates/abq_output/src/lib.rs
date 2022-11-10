use std::io;

use abq_utils::net_protocol::runners::{Milliseconds, Status, TestResult};
use termcolor::{Color, ColorSpec, WriteColor};

/// Formats a test result on a single line.
pub fn format_result_line(writer: &mut impl WriteColor, result: &TestResult) -> io::Result<()> {
    write!(writer, "{}: ", &result.display_name)?;
    format_status(writer, result.status)?;
    writeln!(writer)
}

/// Formats a test result as a single dot.
pub fn format_result_dot(writer: &mut impl WriteColor, result: &TestResult) -> io::Result<()> {
    let dot = match result.status {
        Status::Failure => "F",
        Status::Success => ".",
        Status::Error | Status::PrivateNativeRunnerError => "E",
        Status::Pending => "P",
        Status::Skipped => "S",
    };
    with_color(writer, status_color(result.status), |w| write!(w, "{dot}"))
}

/// Formats a test result as a summary, possibly across multiple lines.
pub fn format_result_summary(writer: &mut impl WriteColor, result: &TestResult) -> io::Result<()> {
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

    write!(writer, "--- {display_name}: ")?;
    format_status(writer, *status)?;
    writeln!(writer, " ---")?;
    writeln!(writer, "{output}")?;
    write!(writer, "(completed in ")?;
    format_duration(writer, *runtime)?;
    writeln!(writer, ")")
}

fn status_color(status: Status) -> Color {
    match status {
        Status::Success => Color::Green,
        Status::Failure => Color::Red,
        Status::Error | Status::PrivateNativeRunnerError => Color::Red,
        Status::Pending => Color::Yellow,
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

fn format_status(writer: &mut impl WriteColor, status: Status) -> io::Result<()> {
    let color = status_color(status);
    let status = match status {
        Status::Failure => "FAILED",
        Status::Success => "ok",
        Status::Error | Status::PrivateNativeRunnerError => "ERRORED",
        Status::Pending => "pending",
        Status::Skipped => "skipped",
    };

    with_color(writer, color, |w| write!(w, "{status}"))
}

pub fn format_duration(writer: &mut impl io::Write, millis: Milliseconds) -> io::Result<()> {
    const MILLIS_IN_SECOND: u64 = 1000;
    const MILLIS_IN_MINUTE: u64 = 60 * MILLIS_IN_SECOND;

    let millis = millis as u64;
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

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::runners::{Status, TestResult};

    use crate::format_result_dot;

    use super::{format_duration, format_result_line, format_result_summary};
    use std::{io, time::Duration};

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
        ($name:ident, $fn:ident, $item:expr, @$expect_no_color:literal @$expect_colored:literal) => {
            #[test]
            fn $name() {
                {
                    // Test uncolored output
                    let mut buf = termcolor::NoColor::new(vec![]);
                    $fn(&mut buf, $item).unwrap();
                    let formatted = String::from_utf8(buf.into_inner()).unwrap();
                    insta::assert_snapshot!(formatted, @$expect_no_color);
                }
                {
                    // Test colored output
                    let mut buf = TestColorWriter(vec![]);
                    $fn(&mut buf, $item).unwrap();
                    let formatted = String::from_utf8(buf.0).unwrap();
                    insta::assert_snapshot!(formatted, @$expect_colored);
                }
            }
        };
    }

    test_format!(
        format_line_success, format_result_line,
        &TestResult {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: ok"
        @r###"
    abq/test: <green>ok<reset>
    "###
    );

    test_format!(
        format_line_failure, format_result_line,
        &TestResult {status: Status::Failure, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: FAILED"
        @r###"
    abq/test: <red>FAILED<reset>
    "###
    );

    test_format!(
        format_line_error, format_result_line,
        &TestResult {status: Status::Error, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: ERRORED"
        @r###"
    abq/test: <red>ERRORED<reset>
    "###
    );

    test_format!(
        format_line_pending, format_result_line,
        &TestResult {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: pending"
        @r###"
    abq/test: <yellow>pending<reset>
    "###
    );

    test_format!(
        format_line_skipped, format_result_line,
        &TestResult {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() },
        @"abq/test: skipped"
        @r###"
    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_dot_success, format_result_dot,
        &TestResult {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() },
        @"."
        @"<green>.<reset>"
    );

    test_format!(
        format_dot_failure, format_result_dot,
        &TestResult {status: Status::Failure, display_name: "abq/test".to_string(), ..default_result() },
        @"F"
        @"<red>F<reset>"
    );

    test_format!(
        format_dot_error, format_result_dot,
        &TestResult {status: Status::Error, display_name: "abq/test".to_string(), ..default_result() },
        @"E"
        @"<red>E<reset>"
    );

    test_format!(
        format_dot_pending, format_result_dot,
        &TestResult {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() },
        @"P"
        @"<yellow>P<reset>"
    );

    test_format!(
        format_dot_skipped, format_result_dot,
        &TestResult {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() },
        @"S"
        @"<yellow>S<reset>"
    );

    test_format!(
        format_exact_minutes, format_duration,
        Duration::from_secs(360).as_millis() as _,
        @"6 m"
        @"6 m"
    );

    test_format!(
        format_exact_minutes_leftover_seconds_and_millis, format_duration,
        Duration::from_millis(6 * 60 * 1000 + 52 * 1000 + 35).as_millis() as _,
        @"6 m, 52 s, 35 ms"
        @"6 m, 52 s, 35 ms"
    );

    test_format!(
        format_exact_seconds, format_duration,
        Duration::from_secs(15).as_millis() as _,
        @"15 s"
        @"15 s"
    );

    test_format!(
        format_exact_seconds_leftover_millis, format_duration,
        Duration::from_millis(15 * 1000 + 35).as_millis() as _,
        @"15 s, 35 ms"
        @"15 s, 35 ms"
    );

    test_format!(
        format_exact_millis, format_duration,
        Duration::from_millis(35).as_millis() as _,
        @"35 ms"
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
        @r###"
    --- abq/test: <green>ok<reset> ---
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
        @r###"
    --- abq/test: <red>FAILED<reset> ---
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
        @r###"
    --- abq/test: <red>ERRORED<reset> ---
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
        @r###"
    --- abq/test: <yellow>pending<reset> ---
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
        @r###"
    --- abq/test: <yellow>skipped<reset> ---
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
        @r###"
    --- abq/test: <green>ok<reset> ---
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
        @r###"
    --- abq/test: <green>ok<reset> ---
    <no output>
    (completed in 1 m, 15 s, 3 ms)
    "###
    );
}
