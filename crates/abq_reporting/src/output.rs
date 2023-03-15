use std::{
    borrow::Cow,
    collections::HashMap,
    io::{self},
    ops::Deref,
    time::Duration,
};

use crate::{colors::ColorProvider, ReportingError};
use abq_utils::net_protocol::{
    entity::RunnerMeta,
    runners::{Status, StdioOutput, TestResult, TestResultSpec, TestRuntime},
};
use indoc::formatdoc;
use termcolor::{Color, ColorSpec, WriteColor};

/// Formats a test result on a single line.
pub fn format_result_line(
    writer: &mut impl WriteColor,
    result: &TestResult,
    is_first_result: bool,
    output_before: &Option<StdioOutput>,
    output_after: &Option<StdioOutput>,
) -> io::Result<()> {
    if let Some(output_before) = output_before {
        if !is_first_result && would_write_output(Some(output_before)) {
            writeln!(writer)?;
        }
        format_runner_output(
            writer,
            result.source,
            OutputOrdering::Before(Cow::Borrowed(&result.display_name)),
            output_before,
        )?;
    }

    write!(writer, "{}: ", &result.display_name)?;
    format_status(writer, &result.status)?;
    writeln!(writer)?;

    if let Some(output_after) = output_after {
        if would_write_output(Some(output_after)) {
            writeln!(writer)?;
        }
        format_runner_output(
            writer,
            result.source,
            OutputOrdering::After(Cow::Borrowed(&result.display_name)),
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

pub enum SummaryKind<'a> {
    Test {
        run_number: u32,
        result: TestResult,
    },
    Output {
        when: OutputOrdering<'a>,
        runner: RunnerMeta,
        output: StdioOutput,
    },
}

pub enum OutputOrdering<'a> {
    Before(Cow<'a, str>),
    After(Cow<'a, str>),
    Custom(Cow<'a, str>),
}

pub fn format_summary(writer: &mut impl WriteColor, summary: SummaryKind) -> io::Result<()> {
    match summary {
        SummaryKind::Test { run_number, result } => {
            format_test_result_summary(writer, run_number, &result)
        }
        SummaryKind::Output {
            when,
            runner,
            output,
        } => format_runner_output(writer, runner, when, &output),
    }
}

pub fn would_write_summary(summary: &SummaryKind) -> bool {
    match summary {
        SummaryKind::Test { .. } => true,
        SummaryKind::Output { output, .. } => would_write_output(Some(output)),
    }
}

/// Formats a test result as a summary, possibly across multiple lines.
pub fn format_test_result_summary(
    writer: &mut impl WriteColor,
    run_number: u32,
    result: &TestResult,
) -> io::Result<()> {
    // --- test/name: {status} (attempt {run_number})? ---
    // {output}
    // ----- STDOUT
    // {stdout}
    // ----- STDERR
    // {stderr}
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
        stderr,
        stdout,
    } = result.deref();

    let output = output.as_deref().unwrap_or("<no output>");

    write!(writer, "--- {display_name}: ")?;
    format_status(writer, status)?;
    if run_number != 1 {
        write!(writer, " (attempt {run_number})")?;
    }
    writeln!(writer, " ---")?;
    writeln!(writer, "{output}")?;
    for (stdoutput, kind) in [(stdout, "STDOUT"), (stderr, "STDERR")] {
        if let Some(output) = stdoutput {
            if !output.is_empty() {
                // TODO: don't print if captured output is only whitespace
                writeln!(writer, "----- {kind}")?;
                writer.write_all(output)?;
                push_newline_if_needed(writer, output)?;
            }
        }
    }
    write!(writer, "(completed in ")?;
    format_duration(writer, *runtime)?;

    write!(writer, " ")?;
    format_runner_name(writer, result.source)?;
    writeln!(writer, ")")
}

pub fn would_write_output(output: Option<&StdioOutput>) -> bool {
    match output {
        Some(o) => !o.stderr.is_empty() || !o.stdout.is_empty(),
        None => false,
    }
}

fn format_runner_name(writer: &mut impl io::Write, runner: RunnerMeta) -> io::Result<()> {
    let RunnerMeta {
        runner,
        is_singleton,
    } = runner;

    if is_singleton {
        write!(writer, "[worker {}]", runner.worker())
    } else {
        write!(
            writer,
            "[worker {}, runner {}]",
            runner.worker(),
            runner.runner()
        )
    }
}

pub fn format_runner_output(
    writer: &mut impl io::Write,
    runner: RunnerMeta,
    when: OutputOrdering,
    output: &StdioOutput,
) -> io::Result<()> {
    // --- [worker {worker_id}, runner {runner_id}] DURING|BEFORE|AFTER test_name ---
    // ----- STDOUT
    // {stdout}
    // ----- STDERR
    // {stderr}

    if !would_write_output(Some(output)) {
        // Don't write anything if we have nothing actionable
        return Ok(());
    }

    let StdioOutput { stderr, stdout } = output;

    write!(writer, "--- ")?;
    format_runner_name(writer, runner)?;

    match when {
        OutputOrdering::Before(test) => write!(writer, " BEFORE {test}"),
        OutputOrdering::After(test) => write!(writer, " AFTER {test}"),
        OutputOrdering::Custom(s) => write!(writer, " {s}"),
    }?;

    writeln!(writer, " ---")?;

    let mut trailing_newlines = 0;

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

pub fn format_final_runner_output(
    writer: &mut impl io::Write,
    runner: RunnerMeta,
    output: &StdioOutput,
) -> io::Result<()> {
    // --- [worker {worker_id}, runner {runner_id}] AFTER completion ---
    let after = "AFTER completion".to_string();
    format_runner_output(
        writer,
        runner,
        OutputOrdering::Custom(Cow::Owned(after)),
        output,
    )
}

pub fn format_manifest_generation_output(
    writer: &mut impl io::Write,
    runner: RunnerMeta,
    output: &StdioOutput,
) -> io::Result<()> {
    // --- [worker {worker_id}, runner {runner_id}] MANIFEST GENERATION ---
    format_runner_output(
        writer,
        runner,
        OutputOrdering::Custom(Cow::Borrowed("MANIFEST GENERATION")),
        output,
    )
}

fn push_newline_if_needed(writer: &mut impl io::Write, bytes: &[u8]) -> io::Result<usize> {
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

pub fn format_interactive_progress(
    colors: &ColorProvider,
    elapsed: impl std::fmt::Display,
    num_results: u64,
    num_failing: u64,
) -> String {
    formatdoc! {
        r#"
        {bold}> ABQ status{reset}
        {bold}> [{elapsed}] {num_tests} tests run{reset}, {green_bold}{num_passed} passed{reset}, {fail_color}{num_failing} failing{reset}
        "#,
        num_tests=num_results,
        num_passed=num_results - num_failing,
        num_failing=num_failing,
        reset=colors.reset,
        bold=colors.bold,
        green_bold=colors.green_bold,
        fail_color=if num_failing == 0 { colors.reset } else { colors.red_bold },
    }
}

pub fn format_non_interactive_progress(
    writer: &mut impl WriteColor,
    elapsed: impl std::fmt::Display,
    num_tests: u64,
    num_failing: u64,
) -> io::Result<()> {
    // --- [abq progress] {elapsed} ---
    // {num_tests} tests run, {num_passed} passed, {num_failing} failing

    let num_passed = num_tests - num_failing;

    writeln!(writer, "--- [abq progress] {elapsed} ---")?;
    with_color_spec(writer, &bold_spec(), |w| write!(w, "{num_tests} tests run"))?;
    write!(writer, ", ")?;
    with_color_spec(writer, &green_bold_spec(), |w| {
        write!(w, "{num_passed} passed")
    })?;
    write!(writer, ", ")?;
    with_color_spec(writer, &failing_spec(num_failing), |w| {
        write!(w, "{num_failing} failing")
    })?;
    writeln!(writer)
}

pub struct ShortSummary {
    pub wall_time: Duration,
    pub test_time: Duration,
    pub num_tests: u64,
    pub num_failing: u64,
    pub num_retried: u64,
    pub failures_per_file: HashMap<String, u64>,
}

pub fn format_short_suite_summary(
    writer: &mut impl WriteColor,
    summary: ShortSummary,
) -> io::Result<()> {
    // Finished in X seconds (X seconds spent in test code)
    // M tests, N failures

    let ShortSummary {
        wall_time,
        test_time,
        num_tests,
        num_failing,
        num_retried,
        failures_per_file,
    } = summary;

    with_color_spec(writer, &bold_spec(), |w| {
        write!(w, "Finished in ")?;
        format_duration_to_partial_seconds(w, wall_time)
    })?;
    write!(writer, " (")?;
    format_duration_to_partial_seconds(writer, test_time)?;
    writeln!(writer, " spent in test code)")?;
    with_color_spec(writer, &green_bold_spec(), |w| {
        write!(w, "{num_tests} tests")
    })?;
    write!(writer, ", ")?;
    with_color_spec(writer, &failing_spec(num_failing), |w| {
        write!(w, "{num_failing} failures")
    })?;
    if num_retried > 0 {
        write!(writer, ", ")?;
        with_color_spec(writer, &yellow_bold_spec(), |w| {
            write!(w, "{num_retried} retried")
        })?;
    }
    writeln!(writer)?;

    if !failures_per_file.is_empty() {
        writeln!(writer)?;
        writeln!(writer, "Failures:")?;

        let mut ordered_files: Vec<String> = failures_per_file.keys().map(Into::into).collect();
        ordered_files.sort();

        for file in ordered_files {
            write!(writer, " {: >5}", failures_per_file.get(&file).unwrap())?;
            write!(writer, "   ")?;
            writeln!(writer, "{file}")?;
        }
    }

    Ok(())
}

pub fn write(writer: &mut impl io::Write, buf: &[u8]) -> Result<(), ReportingError> {
    writer
        .write_all(buf)
        .map_err(|_| ReportingError::FailedToWrite)
}

pub fn format_summary_results(
    writer: &mut impl termcolor::WriteColor,
    summaries: Vec<SummaryKind>,
) -> Result<(), ReportingError> {
    for summary in summaries {
        if would_write_summary(&summary) {
            write(writer, &[b'\n'])?;
        }
        format_summary(writer, summary)?;
    }
    Ok(())
}

pub fn deprecation<W>(
    writer: &mut W,
    notice: &str,
    set_deprecation: impl FnOnce(),
) -> io::Result<()>
where
    W: WriteColor,
{
    set_deprecation();
    with_color(writer, Color::Yellow, |w| {
        writeln!(w, "DEPRECATION NOTICE: {notice}")
    })
}

#[inline]
fn bold_spec() -> ColorSpec {
    let mut spec = ColorSpec::new();
    spec.set_bold(true);
    spec
}

#[inline]
fn green_bold_spec() -> ColorSpec {
    let mut spec = ColorSpec::new();
    spec.set_fg(Some(Color::Green)).set_bold(true);
    spec
}

#[inline]
fn yellow_bold_spec() -> ColorSpec {
    let mut spec = ColorSpec::new();
    spec.set_fg(Some(Color::Yellow)).set_bold(true);
    spec
}

#[inline]
fn failing_spec(num_failing: u64) -> ColorSpec {
    let mut spec = ColorSpec::new();
    if num_failing > 0 {
        spec.set_fg(Some(Color::Red)).set_bold(true);
    }
    spec
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

#[inline]
fn with_color<W>(
    writer: &mut W,
    color: Color,
    f: impl FnOnce(&mut W) -> io::Result<()>,
) -> io::Result<()>
where
    W: WriteColor,
{
    with_color_spec(writer, ColorSpec::new().set_fg(Some(color)), f)
}

#[inline]
fn with_color_spec<W>(
    writer: &mut W,
    color_spec: &ColorSpec,
    f: impl FnOnce(&mut W) -> io::Result<()>,
) -> io::Result<()>
where
    W: WriteColor,
{
    writer.set_color(color_spec)?;
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
    if millis > 0 || !written {
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
        entity::{RunnerMeta, WorkerRunner},
        runners::{Status, StdioOutput, TestResult, TestResultSpec, TestRuntime},
    };

    use super::{
        format_duration, format_duration_to_partial_seconds, format_result_dot, format_result_line,
        format_short_suite_summary, format_test_result_summary, ShortSummary,
    };
    use std::{collections::HashMap, io, time::Duration};

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
            assert!(!spec.intense());
            assert!(!spec.underline());
            assert!(!spec.dimmed());
            assert!(!spec.italic());
            assert!(spec.reset());

            use termcolor::Color::*;
            let mut spec_parts = vec![];
            if spec.bold() {
                spec_parts.push("bold")
            }
            if let Some(color) = spec.fg() {
                let co = match color {
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
                spec_parts.push(co);
            }

            write!(&mut self.0, "<{}>", spec_parts.join("-"))
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
        ($name:ident, $fn:ident, attempt $attempt:literal, $item:expr, @$expect_colored:literal) => {
            #[test]
            fn $name() {
                // Test colored output
                let mut buf = TestColorWriter(vec![]);
                $fn(&mut buf, $attempt, $item).unwrap();
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
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <green>ok<reset>
    "###
    );

    test_format!(
        format_line_failure, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <red>FAILED<reset>
    "###
    );

    test_format!(
        format_line_error, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <red>ERRORED<reset>
    "###
    );

    test_format!(
        format_line_pending, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <yellow>pending<reset>
    "###
    );

    test_format!(
        format_line_skipped, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None, &None,
        @r###"
    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_line_output_before_after, format_result_line,
        &TestResult::new(RunnerMeta::singleton(0),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(StdioOutput { stderr: b"stderr\nbefore".to_vec(), stdout: b"stdout\nbefore\n".to_vec() }),
        &Some(StdioOutput { stderr: b"stderr\nafter".to_vec(), stdout: b"stdout\nafter\n".to_vec() }),
        @r###"
    --- [worker 0] BEFORE abq/test ---
    ----- STDOUT
    stdout
    before
    ----- STDERR
    stderr
    before

    abq/test: <yellow>skipped<reset>

    --- [worker 0] AFTER abq/test ---
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
        &TestResult::new(RunnerMeta::singleton(0),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(StdioOutput { stderr: b"stderr\nbefore".to_vec(), stdout: b"stdout\nbefore\n".to_vec() }),
        &None,
        @r###"
    --- [worker 0] BEFORE abq/test ---
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
        &TestResult::new(RunnerMeta::singleton(0),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(StdioOutput { stderr: b"".to_vec(), stdout: b"stdout\nbefore\n".to_vec() }),
        &None,
        @r###"
    --- [worker 0] BEFORE abq/test ---
    ----- STDOUT
    stdout
    before

    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_line_output_only_before_only_stderr, format_result_line,
        &TestResult::new(RunnerMeta::singleton(0),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(StdioOutput { stderr: b"stderr\nbefore".to_vec(), stdout: b"".to_vec() }),
        &None,
        @r###"
    --- [worker 0] BEFORE abq/test ---
    ----- STDERR
    stderr
    before

    abq/test: <yellow>skipped<reset>
    "###
    );

    test_format!(
        format_line_output_only_after, format_result_line,
        &TestResult::new(RunnerMeta::singleton(0),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None,
        &Some(StdioOutput { stderr: b"stderr\nafter".to_vec(), stdout: b"stdout\nafter".to_vec() }),
        @r###"
    abq/test: <yellow>skipped<reset>

    --- [worker 0] AFTER abq/test ---
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
        &TestResult::new(RunnerMeta::singleton(0),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None,
        &Some(StdioOutput { stderr: b"".to_vec(), stdout: b"stdout\nafter\n".to_vec() }),
        @r###"
    abq/test: <yellow>skipped<reset>

    --- [worker 0] AFTER abq/test ---
    ----- STDOUT
    stdout
    after

    "###
    );

    test_format!(
        format_line_output_only_after_only_stderr, format_result_line,
        &TestResult::new(RunnerMeta::singleton(0),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &None,
        &Some(StdioOutput { stderr: b"stderr\nafter".to_vec(), stdout: b"".to_vec() }),
        @r###"
    abq/test: <yellow>skipped<reset>

    --- [worker 0] AFTER abq/test ---
    ----- STDERR
    stderr
    after

    "###
    );

    test_format!(
        format_line_output_before_after_non_singleton, format_result_line,
        &TestResult::new(RunnerMeta::new(WorkerRunner::new(5, 6), false),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        &Some(StdioOutput { stderr: b"stderr\nbefore".to_vec(), stdout: b"stdout\nbefore\n".to_vec() }),
        &Some(StdioOutput { stderr: b"stderr\nafter".to_vec(), stdout: b"stdout\nafter\n".to_vec() }),
        @r###"
    --- [worker 5, runner 6] BEFORE abq/test ---
    ----- STDOUT
    stdout
    before
    ----- STDERR
    stderr
    before

    abq/test: <yellow>skipped<reset>

    --- [worker 5, runner 6] AFTER abq/test ---
    ----- STDOUT
    stdout
    after
    ----- STDERR
    stderr
    after

    "###
    );

    test_format!(
        format_dot_success, format_result_dot,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() }),
        @"<green>.<reset>"
    );

    test_format!(
        format_dot_failure, format_result_dot,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @"<red>F<reset>"
    );

    test_format!(
        format_dot_error, format_result_dot,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @"<red>E<reset>"
    );

    test_format!(
        format_dot_pending, format_result_dot,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() }),
        @"<yellow>P<reset>"
    );

    test_format!(
        format_dot_skipped, format_result_dot,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
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
        format_zero_ms, format_duration,
        TestRuntime::Milliseconds(0.),
        @"0 ms"
    );

    test_format!(
        format_zero_ns, format_duration,
        TestRuntime::Nanoseconds(0),
        @"0 ms"
    );

    test_format!(
        format_summary_success, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    Test passed!
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_failure, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> ---
    Assertion failed: 1 != 2
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_error, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Process at pid 72818 exited early with SIGTERM".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>ERRORED<reset> ---
    Process at pid 72818 exited early with SIGTERM
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_pending, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), output: Some(r#"Test not implemented yet for reason: "need to implement feature A""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>pending<reset> ---
    Test not implemented yet for reason: "need to implement feature A"
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_skipped, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), output: Some(r#"Test skipped for reason: "only enabled on summer Fridays""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>skipped<reset> ---
    Test skipped for reason: "only enabled on summer Fridays"
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_multiline, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!\nTo see rendered webpage, see:\n\thttps://example.com\n".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    Test passed!
    To see rendered webpage, see:
    	https://example.com

    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_no_output, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: None, ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    <no output>
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_only_stdout, format_test_result_summary,
        attempt 1,
        &TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!".to_string()),
                stdout: Some(b"my stdout1\nmy stdout2".to_vec()),
                stderr: Some(b"".to_vec()),
                ..default_result()
            }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    Test passed!
    ----- STDOUT
    my stdout1
    my stdout2
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_only_stderr, format_test_result_summary,
        attempt 1,
        &TestResult::new(
            RunnerMeta::fake(),
            TestResultSpec {
                status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!".to_string()),
                stdout: Some(b"".to_vec()),
                stderr: Some(b"my stderr1\nmy stderr2".to_vec()),
                ..default_result()
            }),
        @r###"
    --- abq/test: <green>ok<reset> ---
    Test passed!
    ----- STDERR
    my stderr1
    my stderr2
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_summary_failure_non_singleton_runner, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::new(WorkerRunner::new(5, 6), false),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> ---
    Assertion failed: 1 != 2
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 5, runner 6])
    "###
    );

    test_format!(
        format_summary_failure_multi_attempt, format_test_result_summary,
        attempt 3,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> (attempt 3) ---
    Assertion failed: 1 != 2
    ----- STDOUT
    my stderr
    ----- STDERR
    my stdout
    (completed in 1 m, 15 s, 3 ms [worker 0])
    "###
    );

    test_format!(
        format_short_suite_summary_with_failing_and_retried, format_short_suite_summary,
        ShortSummary { wall_time: Duration::from_secs(10), test_time: Duration::from_secs(9), num_tests: 100, num_failing: 12, num_retried: 15, failures_per_file: HashMap::from([("file2.rs".to_owned(), 10), ("file1.rs".to_owned(), 2)]) },
        @r###"
    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <bold-red>12 failures<reset>, <bold-yellow>15 retried<reset>

    Failures:
         2   file1.rs
        10   file2.rs
    "###
    );

    test_format!(
        format_short_suite_summary_with_failing_without_retried, format_short_suite_summary,
        ShortSummary { wall_time: Duration::from_secs(10), test_time: Duration::from_secs(9), num_tests: 100, num_failing: 10, num_retried: 0, failures_per_file: HashMap::from([("file2.rs".to_owned(), 6), ("file1.rs".to_owned(), 4)]) },
        @r###"
    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <bold-red>10 failures<reset>

    Failures:
         4   file1.rs
         6   file2.rs
    "###
    );

    test_format!(
        format_short_suite_summary_with_no_failing_no_retried, format_short_suite_summary,
        ShortSummary { wall_time: Duration::from_secs(10), test_time: Duration::from_secs(9), num_tests: 100, num_failing: 0, num_retried: 0, failures_per_file: HashMap::default() },
        @r###"
    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <>0 failures<reset>
    "###
    );

    test_format!(
        format_short_suite_summary_with_no_failing_but_retried, format_short_suite_summary,
        ShortSummary { wall_time: Duration::from_secs(10), test_time: Duration::from_secs(9), num_tests: 100, num_failing: 0, num_retried: 10, failures_per_file: HashMap::default() },
        @r###"
    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <>0 failures<reset>, <bold-yellow>10 retried<reset>
    "###
    );
}
