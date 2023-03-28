use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    io,
    time::Duration,
};

use crate::{colors::ColorProvider, ReportingError};
use abq_utils::net_protocol::{
    entity::{RunnerMeta, WorkerRunner},
    runners::{Status, StdioOutput, TestResult, TestRuntime},
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
}

impl RunnerSummary {
    pub fn empty(runner: WorkerRunner) -> Self {
        Self {
            runner,
            failures_per_file: HashMap::new(),
        }
    }

    pub fn has_failures(&self) -> bool {
        !self.failures_per_file.is_empty()
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
    pub fn failure_summaries(&self) -> Vec<RunnerSummary> {
        let mut summaries: BTreeMap<u32, RunnerSummary> = BTreeMap::new();
        for runner_summary in &self.runner_summaries {
            if !runner_summary.has_failures() {
                continue;
            }

            let key = match self.grouping {
                ShortSummaryGrouping::Runner => runner_summary.runner.runner(),
                ShortSummaryGrouping::Worker => runner_summary.runner.worker(),
            };

            match summaries.get_mut(&key) {
                None => {
                    summaries.insert(key, runner_summary.clone());
                }
                Some(existing_summary) => {
                    for (file, failures) in runner_summary.failures_per_file.iter() {
                        let total_failures = existing_summary
                            .failures_per_file
                            .entry(file.into())
                            .or_default();
                        *total_failures += failures;
                    }
                }
            };
        }
        summaries.into_values().collect()
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

    let failure_summaries = summary.failure_summaries();
    if !failure_summaries.is_empty() {
        writeln!(writer)?;
        writeln!(writer, "Failures:")?;
        writeln!(writer)?;

        for (i, failure_summary) in failure_summaries.iter().enumerate() {
            if i > 0 {
                writeln!(writer)?;
            }

            let label = match summary.grouping {
                ShortSummaryGrouping::Runner => {
                    format!("runner {}", failure_summary.runner.runner())
                }
                ShortSummaryGrouping::Worker => {
                    format!("worker {}", failure_summary.runner.worker())
                }
            };
            writeln!(writer, "    {}:", label)?;

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
        entity::RunnerMeta,
        runners::{Status, TestResult, TestResultSpec, TestRuntime},
    };

    use super::{
        format_duration, format_duration_to_partial_seconds, format_result_dot, format_result_line,
        format_short_suite_summary, RunnerSummary, ShortSummary, ShortSummaryGrouping,
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
                },
                RunnerSummary {
                    runner: (0, 3).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 5)]),
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
                },
                RunnerSummary {
                    runner: (0, 3).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 3)]),
                },
                RunnerSummary {
                    runner: (2, 4).into(),
                    failures_per_file: HashMap::from([("file2.rs".to_owned(), 2)]),
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
