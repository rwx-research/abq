use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    io,
    ops::Deref,
    time::Duration,
};

use crate::{colors::ColorProvider, ReportingError};
use abq_utils::{
    net_protocol::{
        entity::{RunnerMeta, WorkerRunner},
        runners::{Status, StdioOutput, TestResult, TestResultSpec, TestRuntime},
    },
    whitespace::is_blank,
};
use indoc::formatdoc;
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

pub fn would_write_output(output: Option<&StdioOutput>) -> bool {
    match output {
        Some(o) => !o.stderr.is_empty() || !o.stdout.is_empty(),
        None => false,
    }
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

#[derive(Debug, Clone)]
pub struct RunnerSummary {
    pub runner: WorkerRunner,
    pub failures_per_file: HashMap<String, u64>,
    pub retries_per_file: HashMap<String, u64>,
}

impl RunnerSummary {
    pub fn empty(runner: WorkerRunner) -> Self {
        Self {
            runner,
            failures_per_file: HashMap::new(),
            retries_per_file: HashMap::new(),
        }
    }

    pub fn has_failures(&self) -> bool {
        !self.failures_per_file.is_empty()
    }

    pub fn has_retries(&self) -> bool {
        !self.retries_per_file.is_empty()
    }

    fn union(&mut self, other: &RunnerSummary) {
        for (file, other_failures) in other.failures_per_file.iter() {
            let total_failures = self.failures_per_file.entry(file.into()).or_insert(0);
            *total_failures += other_failures;
        }

        for (file, other_retries) in other.retries_per_file.iter() {
            let total_retries = self.retries_per_file.entry(file.into()).or_insert(0);
            *total_retries += other_retries;
        }
    }
}

/// ShortSummary groups output by worker or runner, never both.
#[derive(Debug)]
pub enum ShortSummaryGrouping {
    /// Group by worker when output is for a cross-worker format like `abq report`.
    Worker,
    /// Group by runner when output is only for the results on a single worker node, such as `abq test`.
    Runner,
}

#[derive(Debug)]
pub struct ShortSummary {
    pub wall_time: Duration,
    pub test_time: Duration,
    pub num_tests: u64,
    pub num_failing: u64,
    pub num_retried: u64,
    pub runner_summaries: Vec<RunnerSummary>,
    pub grouping: ShortSummaryGrouping,
}

impl ShortSummary {
    /// Group runner summaries and sum counts for all (worker, runner) instances.
    pub fn grouped_runner_summaries(&self) -> Vec<RunnerSummary> {
        let mut summaries: BTreeMap<u32, RunnerSummary> = BTreeMap::new();
        for runner_summary in &self.runner_summaries {
            let key = self.grouping_key(runner_summary.runner);

            match summaries.get_mut(&key) {
                None => {
                    summaries.insert(key, runner_summary.clone());
                }
                Some(existing_summary) => {
                    existing_summary.union(runner_summary);
                }
            };
        }
        summaries.into_values().collect()
    }

    fn grouping_key(&self, runner: WorkerRunner) -> u32 {
        match self.grouping {
            ShortSummaryGrouping::Runner => runner.runner(),
            ShortSummaryGrouping::Worker => runner.worker(),
        }
    }

    fn grouping_label(&self, runner: WorkerRunner) -> String {
        match self.grouping {
            ShortSummaryGrouping::Runner => {
                format!("runner {}", runner.runner())
            }
            ShortSummaryGrouping::Worker => {
                format!("worker {}", runner.worker())
            }
        }
    }
}

const ABQ_HEADER: &str = concat!(
    "--------------------------------------------------------------------------------\n",
    "------------------------------------- ABQ --------------------------------------\n",
    "--------------------------------------------------------------------------------\n",
);

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
        ..
    } = summary;

    writer.write_all(ABQ_HEADER.as_bytes())?;
    writeln!(writer)?;

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

    let summaries = summary.grouped_runner_summaries();

    let retry_summaries: Vec<_> = summaries.iter().filter(|s| s.has_retries()).collect();
    if !retry_summaries.is_empty() {
        writeln!(writer)?;
        writeln!(writer, "Retries:")?;
        writeln!(writer)?;

        for (i, retry_summary) in retry_summaries.iter().enumerate() {
            if i > 0 {
                writeln!(writer)?;
            }

            let ordered_files: BTreeMap<String, u64> = retry_summary
                .retries_per_file
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect();

            writeln!(
                writer,
                "    {}:",
                summary.grouping_label(retry_summary.runner)
            )?;

            for (file, count) in ordered_files {
                write!(writer, "    {: >5}", count)?;
                write!(writer, "   ")?;
                writeln!(writer, "{file}")?;
            }
        }
    }

    let failure_summaries: Vec<_> = summaries.iter().filter(|s| s.has_failures()).collect();
    if !failure_summaries.is_empty() {
        writeln!(writer)?;
        writeln!(writer, "Failures:")?;
        writeln!(writer)?;

        for (i, failure_summary) in failure_summaries.iter().enumerate() {
            if i > 0 {
                writeln!(writer)?;
            }

            writeln!(
                writer,
                "    {}:",
                summary.grouping_label(failure_summary.runner)
            )?;

            let ordered_files: BTreeMap<String, u64> = failure_summary
                .failures_per_file
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect();

            for (file, count) in ordered_files {
                write!(writer, "    {: >5}", count)?;
                write!(writer, "   ")?;
                writeln!(writer, "{file}")?;
            }
        }
    }

    Ok(())
}

/// Formats a test result as a summary, possibly across multiple lines.
pub fn format_test_result_summary(
    writer: &mut impl WriteColor,
    run_number: u32,
    result: &TestResult,
) -> io::Result<()> {
    // --- test/name: {status} (attempt {run_number})? ---
    // {output}
    // ------ STDOUT
    // {stdout}
    // ------ STDERR
    // {stderr}

    let TestResultSpec {
        status,
        id: _,
        display_name,
        output,
        runtime: _,
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
        timestamp: _,
    } = result.deref();

    write!(writer, "--- {display_name}: ")?;
    format_status(writer, status)?;
    if run_number != 1 {
        write!(writer, " (attempt {run_number})")?;
    }
    writeln!(writer, " --- [worker {}]", result.source.runner.worker())?;

    match output.as_deref() {
        Some(output) if !is_blank(output.as_bytes()) => {
            writeln!(writer, "{output}")?;
        }
        _ => {}
    };

    for (stdoutput, kind) in [(stdout, "STDOUT"), (stderr, "STDERR")] {
        if let Some(output) = stdoutput {
            if is_blank(output) {
                continue;
            }

            writeln!(writer, "------ {kind}")?;
            writer.write_all(output)?;
            push_newline_if_needed(writer, output)?;
        }
    }
    Ok(())
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

pub fn write(writer: &mut impl io::Write, buf: &[u8]) -> Result<(), ReportingError> {
    writer
        .write_all(buf)
        .map_err(|_| ReportingError::FailedToWrite)
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
pub fn yellow_bold_spec() -> ColorSpec {
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
    use abq_test_utils::color_writer::TestColorWriter;
    use abq_utils::net_protocol::{
        entity::{RunnerMeta, WorkerRunner},
        runners::{Status, TestResult, TestResultSpec, TestRuntime},
    };

    use crate::output::format_test_result_summary;

    use super::{
        format_duration, format_duration_to_partial_seconds, format_result_dot, format_result_line,
        format_short_suite_summary, RunnerSummary, ShortSummary, ShortSummaryGrouping,
    };
    use std::{collections::HashMap, time::Duration};

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

    macro_rules! test_format {
        ($name:ident, $fn:ident, $item:expr, @$expect_colored:literal) => {
            #[test]
            fn $name() {
                // Test colored output
                let mut buf = TestColorWriter::new(vec![]);
                $fn(&mut buf, $item).unwrap();
                let formatted = String::from_utf8(buf.get()).unwrap();
                insta::assert_snapshot!(formatted, @$expect_colored);
            }
        };
        ($name:ident, $fn:ident, attempt $attempt:literal, $item:expr, @$expect_colored:literal) => {
            #[test]
            fn $name() {
                // Test colored output
                let mut buf = TestColorWriter::new(vec![]);
                $fn(&mut buf, $attempt, $item).unwrap();
                let formatted = String::from_utf8(buf.get()).unwrap();
                insta::assert_snapshot!(formatted, @$expect_colored);
            }
        };
    }

    test_format!(
        format_line_success, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <green>ok<reset>
    "###
    );

    test_format!(
        format_line_failure, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <red>FAILED<reset>
    "###
    );

    test_format!(
        format_line_error, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <red>ERRORED<reset>
    "###
    );

    test_format!(
        format_line_pending, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <yellow>pending<reset>
    "###
    );

    test_format!(
        format_line_skipped, format_result_line,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), ..default_result() }),
        @r###"
    abq/test: <yellow>skipped<reset>
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
        format_summary_success, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> --- [worker 0]
    Test passed!
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
    );

    test_format!(
        format_summary_failure, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> --- [worker 0]
    Assertion failed: 1 != 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
    );

    test_format!(
        format_summary_error, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Error { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Process at pid 72818 exited early with SIGTERM".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>ERRORED<reset> --- [worker 0]
    Process at pid 72818 exited early with SIGTERM
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
    );

    test_format!(
        format_summary_pending, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Pending, display_name: "abq/test".to_string(), output: Some(r#"Test not implemented yet for reason: "need to implement feature A""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>pending<reset> --- [worker 0]
    Test not implemented yet for reason: "need to implement feature A"
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
    );

    test_format!(
        format_summary_skipped, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Skipped, display_name: "abq/test".to_string(), output: Some(r#"Test skipped for reason: "only enabled on summer Fridays""#.to_string()), ..default_result() }),
        @r###"
    --- abq/test: <yellow>skipped<reset> --- [worker 0]
    Test skipped for reason: "only enabled on summer Fridays"
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
    );

    test_format!(
        format_summary_multiline, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: Some("Test passed!\nTo see rendered webpage, see:\n\thttps://example.com\n".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> --- [worker 0]
    Test passed!
    To see rendered webpage, see:
    	https://example.com

    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
    );

    test_format!(
        format_summary_no_output, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Success, display_name: "abq/test".to_string(), output: None, ..default_result() }),
        @r###"
    --- abq/test: <green>ok<reset> --- [worker 0]
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
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
    --- abq/test: <green>ok<reset> --- [worker 0]
    Test passed!
    ------ STDOUT
    my stdout1
    my stdout2
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
    --- abq/test: <green>ok<reset> --- [worker 0]
    Test passed!
    ------ STDERR
    my stderr1
    my stderr2
    "###
    );

    test_format!(
        format_summary_failure_non_singleton_runner, format_test_result_summary,
        attempt 1,
        &TestResult::new(RunnerMeta::new(WorkerRunner::new(5, 6), false, false),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> --- [worker 5]
    Assertion failed: 1 != 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
    );

    test_format!(
        format_summary_failure_multi_attempt, format_test_result_summary,
        attempt 3,
        &TestResult::new(RunnerMeta::fake(),TestResultSpec {status: Status::Failure { exception: None, backtrace: None }, display_name: "abq/test".to_string(), output: Some("Assertion failed: 1 != 2".to_string()), ..default_result() }),
        @r###"
    --- abq/test: <red>FAILED<reset> (attempt 3) --- [worker 0]
    Assertion failed: 1 != 2
    ------ STDOUT
    my stderr
    ------ STDERR
    my stdout
    "###
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
        format_short_suite_summary_with_failing_without_retried_single_runner, format_short_suite_summary,
        ShortSummary {
            wall_time: Duration::from_secs(10),
            test_time: Duration::from_secs(9),
            num_tests: 100,
            num_failing: 16,
            num_retried: 0,
            runner_summaries: vec![
                RunnerSummary {
                    runner: (0, 1).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 6), ("file1.rs".to_owned(), 10)]),
                    retries_per_file: HashMap::new(),
                },
            ],
            grouping: ShortSummaryGrouping::Runner,
        },
        @r###"
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <bold-red>16 failures<reset>

    Failures:

        runner 1:
           10   file1.rs
            6   file2.rs
    "###
    );

    test_format!(
        format_short_suite_summary_with_failing_with_retried_single_runner, format_short_suite_summary,
        ShortSummary {
            wall_time: Duration::from_secs(10),
            test_time: Duration::from_secs(9),
            num_tests: 100,
            num_failing: 16,
            num_retried: 12,
            runner_summaries: vec![
                RunnerSummary {
                    runner: (0, 1).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 6), ("file1.rs".to_owned(), 10)]),
                    retries_per_file: HashMap::from([("file2.rs".to_owned(), 2), ("file1.rs".to_owned(), 10)]),
                },
            ],
            grouping: ShortSummaryGrouping::Runner,
        },
        @r###"
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <bold-red>16 failures<reset>, <bold-yellow>12 retried<reset>

    Retries:

        runner 1:
           10   file1.rs
            2   file2.rs

    Failures:

        runner 1:
           10   file1.rs
            6   file2.rs
    "###
    );

    test_format!(
        format_short_suite_summary_with_failing_without_retried_multiple_runners, format_short_suite_summary,
        ShortSummary {
            wall_time: Duration::from_secs(10),
            test_time: Duration::from_secs(9),
            num_tests: 100,
            num_failing: 16,
            num_retried: 0,
            runner_summaries: vec![
                RunnerSummary {
                    runner: (0, 1).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 1), ("file1.rs".to_owned(), 10)]),
                    retries_per_file: HashMap::new(),
                },
                RunnerSummary {
                    runner: (0, 3).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 5)]),
                    retries_per_file: HashMap::new(),
                },
            ],
            grouping: ShortSummaryGrouping::Runner,
        },
        @r###"
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <bold-red>16 failures<reset>

    Failures:

        runner 1:
           10   file1.rs
            1   file2.rs

        runner 3:
            5   file2.rs
    "###
    );

    test_format!(
        format_short_suite_summary_with_failing_with_retried_multiple_runners, format_short_suite_summary,
        ShortSummary {
            wall_time: Duration::from_secs(10),
            test_time: Duration::from_secs(9),
            num_tests: 100,
            num_failing: 16,
            num_retried: 0,
            runner_summaries: vec![
                RunnerSummary {
                    runner: (0, 1).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 1), ("file1.rs".to_owned(), 10)]),
                    retries_per_file: HashMap::from([("file1.rs".to_owned(), 10)]),
                },
                RunnerSummary {
                    runner: (0, 3).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 5)]),
                    retries_per_file: HashMap::from([("file2.rs".to_owned(), 1)]),
                },
            ],
            grouping: ShortSummaryGrouping::Runner,
        },
        @r###"
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <bold-red>16 failures<reset>

    Retries:

        runner 1:
           10   file1.rs

        runner 3:
            1   file2.rs

    Failures:

        runner 1:
           10   file1.rs
            1   file2.rs

        runner 3:
            5   file2.rs
    "###
    );

    test_format!(
        format_short_suite_summary_with_failing_without_retried_multiple_workers, format_short_suite_summary,
        ShortSummary {
            wall_time: Duration::from_secs(10),
            test_time: Duration::from_secs(9),
            num_tests: 100,
            num_failing: 16,
            num_retried: 0,
            runner_summaries: vec![
                RunnerSummary {
                    runner: (0, 1).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 1), ("file1.rs".to_owned(), 10)]),
                    retries_per_file: HashMap::new(),
                },
                RunnerSummary {
                    runner: (0, 3).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 3)]),
                    retries_per_file: HashMap::new(),
                },
                RunnerSummary {
                    runner: (2, 4).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 2)]),
                    retries_per_file: HashMap::new(),
                },
            ],
            grouping: ShortSummaryGrouping::Worker,
        },
        @r###"
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <bold-red>16 failures<reset>

    Failures:

        worker 0:
           10   file1.rs
            4   file2.rs

        worker 2:
            2   file2.rs
    "###
    );

    test_format!(
        format_short_suite_summary_with_no_failing_no_retried, format_short_suite_summary,
        ShortSummary {
            wall_time: Duration::from_secs(10),
            test_time: Duration::from_secs(9),
            num_tests: 100,
            num_failing: 0,
            num_retried: 0,
            runner_summaries: Default::default(),
            grouping: ShortSummaryGrouping::Runner,
        },
        @r###"
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <>0 failures<reset>
    "###
    );

    test_format!(
        format_short_suite_summary_with_no_failing_but_retried, format_short_suite_summary,
        ShortSummary {
            wall_time: Duration::from_secs(10),
            test_time: Duration::from_secs(9),
            num_tests: 100,
            num_failing: 0,
            num_retried: 10,
            runner_summaries: Default::default(),
            grouping: ShortSummaryGrouping::Runner,
        },
        @r###"
    --------------------------------------------------------------------------------
    ------------------------------------- ABQ --------------------------------------
    --------------------------------------------------------------------------------

    <bold>Finished in 10.00 seconds<reset> (9.00 seconds spent in test code)
    <bold-green>100 tests<reset>, <>0 failures<reset>, <bold-yellow>10 retried<reset>
    "###
    );
}
