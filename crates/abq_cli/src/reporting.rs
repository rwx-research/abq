use std::{fmt::Display, io, path::PathBuf, str::FromStr};

use abq_output::{format_result_dot, format_result_line, format_result_summary};
use abq_utils::net_protocol::runners::{Status, TestResult};
use termcolor::{ColorChoice, StandardStream};
use thiserror::Error;

static DEFAULT_JUNIT_XML_PATH: &str = "abq-test-results.xml";

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum ReporterKind {
    /// Writes results line-by-line to stdout
    Line,
    /// Writes results as dots to stdout
    Dot,
    /// Writes JUnit XML to a file
    JUnitXml(PathBuf),
}

impl Display for ReporterKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReporterKind::Line => write!(f, "line"),
            ReporterKind::Dot => write!(f, "dot"),
            ReporterKind::JUnitXml(path) => write!(f, "junit-xml={}", path.display()),
        }
    }
}

impl FromStr for ReporterKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "line" => Ok(Self::Line),
            "dot" => Ok(Self::Dot),
            other => {
                if other.starts_with("junit-xml") {
                    let mut splits = other.split("junit-xml=");
                    splits.next(); // eat junit-xml=;
                    let path = splits
                        .next()
                        .filter(|path| !path.trim().is_empty())
                        .unwrap_or(DEFAULT_JUNIT_XML_PATH);
                    let path = PathBuf::from_str(path).map_err(|e| e.to_string())?;
                    Ok(ReporterKind::JUnitXml(path))
                } else {
                    Err(format!("Unknown reporter {}", other))
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum ColorPreference {
    Auto,
    Never,
}

impl FromStr for ColorPreference {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            "never" => Ok(Self::Never),
            other => Err(format!("Unknown color option {other}")),
        }
    }
}

#[derive(Debug, Error)]
pub enum ReportingError {
    #[error("failed to format a test result in the reporting format")]
    FailedToFormat,
    #[error("failed to write a report to an output buffer")]
    FailedToWrite,
}

impl From<io::Error> for ReportingError {
    fn from(_: io::Error) -> Self {
        Self::FailedToWrite
    }
}

pub(crate) struct ExitCode(i32);

impl ExitCode {
    pub fn get(&self) -> i32 {
        self.0
    }

    pub fn new(code: i32) -> Self {
        Self(code)
    }
}

pub(crate) struct SuiteResult {
    /// On the whole, did this test suite pass or fail?
    pub success: bool,
    /// Exit code suggested for the test suite.
    pub suggested_exit_code: ExitCode,
}

impl SuiteResult {
    fn account_result(&mut self, test_result: &TestResult) {
        match test_result.status {
            Status::Failure | Status::Error | Status::PrivateNativeRunnerError => {
                self.success = false;
                self.suggested_exit_code = ExitCode(1);
            }

            Status::Pending | Status::Skipped | Status::Success => {}
        }
    }
}

/// A [`Reporter`] defines a way to emit abq test results.
///
/// A reporter is allowed to be side-effectful.
pub(crate) trait Reporter: Send {
    /// Consume the next test result.
    fn push_result(&mut self, test_result: &TestResult) -> Result<(), ReportingError>;

    /// Consume the reporter, and perform any needed finalization steps.
    ///
    /// This method is only called when all test results for a run have been consumed.
    fn finish(self: Box<Self>, overall_result: &SuiteResult) -> Result<(), ReportingError>;
}

fn write(writer: &mut impl io::Write, buf: &[u8]) -> Result<(), ReportingError> {
    writer
        .write_all(buf)
        .map_err(|_| ReportingError::FailedToWrite)
}

fn write_summary_results(
    writer: &mut impl termcolor::WriteColor,
    results: Vec<TestResult>,
) -> Result<(), ReportingError> {
    for test_result in results {
        write(writer, &[b'\n'])?;
        format_result_summary(writer, &test_result)?;
    }
    Ok(())
}

/// Streams all test results line-by-line to an output buffer, and prints a summary for failing and
/// erroring tests at the end.
struct LineReporter {
    /// The output buffer.
    buffer: Box<dyn termcolor::WriteColor + Send>,

    /// Failures and errors for which a longer summary should be printed at the end.
    delayed_failure_reports: Vec<TestResult>,
}

impl Reporter for LineReporter {
    fn push_result(&mut self, test_result: &TestResult) -> Result<(), ReportingError> {
        format_result_line(&mut self.buffer, test_result)?;

        if matches!(test_result.status, Status::PrivateNativeRunnerError) {
            format_result_summary(&mut self.buffer, test_result)?;
        }

        if matches!(test_result.status, Status::Failure | Status::Error) {
            self.delayed_failure_reports.push(test_result.clone());
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>, _overall_result: &SuiteResult) -> Result<(), ReportingError> {
        write_summary_results(&mut self.buffer, self.delayed_failure_reports)?;

        self.buffer
            .flush()
            .map_err(|_| ReportingError::FailedToWrite)?;

        Ok(())
    }
}

/// Max number of dots to print per line for the dot reporter.
const DOT_REPORTER_LINE_LIMIT: u64 = 40;

/// Streams test results as dots to an output buffer, and prints a summary for failing and erroring
/// tests at the end.
struct DotReporter {
    buffer: Box<dyn termcolor::WriteColor + Send>,

    num_results: u64,

    delayed_failure_reports: Vec<TestResult>,
}

impl Reporter for DotReporter {
    fn push_result(&mut self, test_result: &TestResult) -> Result<(), ReportingError> {
        self.num_results += 1;

        format_result_dot(&mut self.buffer, test_result)?;

        if self.num_results % DOT_REPORTER_LINE_LIMIT == 0 {
            // Print a newline
            write(&mut self.buffer, &[b'\n'])?;
        }

        // Make sure to flush the dot out to avoid buffering them!
        self.buffer
            .flush()
            .map_err(|_| ReportingError::FailedToWrite)?;

        if matches!(test_result.status, Status::PrivateNativeRunnerError) {
            format_result_summary(&mut self.buffer, test_result)?;
        }

        if matches!(test_result.status, Status::Failure | Status::Error) {
            self.delayed_failure_reports.push(test_result.clone());
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>, _overall_result: &SuiteResult) -> Result<(), ReportingError> {
        if !self.delayed_failure_reports.is_empty()
            && self.num_results % DOT_REPORTER_LINE_LIMIT != 0
        {
            // We have summaries to print and the last dot would not have printed a newline, so
            // print one before we display the summaries.
            write(&mut self.buffer, &[b'\n'])?;
        }

        write_summary_results(&mut self.buffer, self.delayed_failure_reports)?;

        self.buffer
            .flush()
            .map_err(|_| ReportingError::FailedToWrite)?;

        Ok(())
    }
}

/// Writes test results as JUnit XML to a path.
struct JUnitXmlReporter {
    path: PathBuf,
    collector: abq_junit_xml::Collector,
}

impl Reporter for JUnitXmlReporter {
    fn push_result(&mut self, test_result: &TestResult) -> Result<(), ReportingError> {
        self.collector.push_result(test_result);
        Ok(())
    }

    fn finish(self: Box<Self>, _overall_result: &SuiteResult) -> Result<(), ReportingError> {
        let fd = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.path)
            .map_err(|_| ReportingError::FailedToWrite)?;
        self.collector
            .write_xml(fd)
            .map_err(|_| ReportingError::FailedToFormat)?;
        Ok(())
    }
}

fn reporter_from_kind(
    kind: ReporterKind,
    color_preference: ColorPreference,
    test_suite_name: &str,
) -> Box<dyn Reporter> {
    let color = match color_preference {
        ColorPreference::Auto => {
            if atty::is(atty::Stream::Stdout) {
                ColorChoice::Auto
            } else {
                ColorChoice::Never
            }
        }
        ColorPreference::Never => ColorChoice::Never,
    };
    let stdout = StandardStream::stdout(color);

    match kind {
        ReporterKind::Line => Box::new(LineReporter {
            buffer: Box::new(stdout),
            delayed_failure_reports: Default::default(),
        }),
        ReporterKind::Dot => Box::new(DotReporter {
            buffer: Box::new(stdout),
            num_results: 0,
            delayed_failure_reports: Default::default(),
        }),
        ReporterKind::JUnitXml(path) => Box::new(JUnitXmlReporter {
            path,
            collector: abq_junit_xml::Collector::new(test_suite_name),
        }),
    }
}

pub(crate) struct SuiteReporters {
    reporters: Vec<Box<dyn Reporter>>,
    overall_result: SuiteResult,
}

impl SuiteReporters {
    pub fn new(
        reporter_kinds: impl IntoIterator<Item = ReporterKind>,
        color_preference: ColorPreference,
        test_suite_name: &str,
    ) -> Self {
        Self {
            reporters: reporter_kinds
                .into_iter()
                .map(|kind| reporter_from_kind(kind, color_preference, test_suite_name))
                .collect(),
            overall_result: SuiteResult {
                success: true,
                suggested_exit_code: ExitCode(0),
            },
        }
    }

    pub fn push_result(&mut self, test_result: &TestResult) -> Result<(), Vec<ReportingError>> {
        let errors: Vec<_> = self
            .reporters
            .iter_mut()
            .filter_map(|reporter| reporter.push_result(test_result).err())
            .collect();

        self.overall_result.account_result(test_result);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn finish(self) -> (SuiteResult, Vec<ReportingError>) {
        let Self {
            reporters,
            overall_result,
        } = self;

        let errors: Vec<_> = reporters
            .into_iter()
            .filter_map(|reporter| reporter.finish(&overall_result).err())
            .collect();

        (overall_result, errors)
    }
}

#[cfg(test)]
mod test_reporter_kind {
    use super::{ReporterKind, DEFAULT_JUNIT_XML_PATH};
    use std::{path::PathBuf, str::FromStr};

    #[test]
    fn parse_unknown_reporter() {
        assert_eq!(
            ReporterKind::from_str("not-a-reporter"),
            Err("Unknown reporter not-a-reporter".to_string())
        );
    }

    #[test]
    fn parse_line_reporter() {
        assert_eq!(ReporterKind::from_str("line"), Ok(ReporterKind::Line));
    }

    #[test]
    fn parse_dot_reporter() {
        assert_eq!(ReporterKind::from_str("dot"), Ok(ReporterKind::Dot));
    }

    #[test]
    fn parse_junit_reporter_no_eq_to_default() {
        assert_eq!(
            ReporterKind::from_str("junit-xml"),
            Ok(ReporterKind::JUnitXml(PathBuf::from(
                DEFAULT_JUNIT_XML_PATH
            )))
        );
    }

    #[test]
    fn parse_junit_reporter_eq_no_suffix_to_default() {
        assert_eq!(
            ReporterKind::from_str("junit-xml="),
            Ok(ReporterKind::JUnitXml(PathBuf::from(
                DEFAULT_JUNIT_XML_PATH
            )))
        );
    }

    #[test]
    fn parse_junit_reporter_eq_no_suffix_with_spaces_to_default() {
        assert_eq!(
            ReporterKind::from_str("junit-xml=     "),
            Ok(ReporterKind::JUnitXml(PathBuf::from(
                DEFAULT_JUNIT_XML_PATH
            )))
        );
    }

    #[test]
    fn parse_junit_reporter_eq_with_suffix_to_absolute_path() {
        assert_eq!(
            ReporterKind::from_str("junit-xml=/my/test.xml"),
            Ok(ReporterKind::JUnitXml(PathBuf::from("/my/test.xml")))
        );
    }

    #[test]
    fn parse_junit_reporter_eq_with_suffix_to_relative_path() {
        assert_eq!(
            ReporterKind::from_str("junit-xml=my/test.xml"),
            Ok(ReporterKind::JUnitXml(PathBuf::from("my/test.xml")))
        );
    }
}

#[cfg(test)]
#[derive(Default)]
struct MockWriter {
    pub buffer: Vec<u8>,
    pub num_writes: u64,
    pub num_flushes: u64,
}

#[cfg(test)]
impl termcolor::WriteColor for &mut MockWriter {
    fn supports_color(&self) -> bool {
        false
    }

    fn set_color(&mut self, _spec: &termcolor::ColorSpec) -> io::Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
impl io::Write for &mut MockWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend(buf);
        self.num_writes += 1;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.num_flushes += 1;
        Ok(())
    }
}

#[cfg(test)]
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

#[cfg(test)]
fn suite_failure() -> SuiteResult {
    SuiteResult {
        success: false,
        suggested_exit_code: ExitCode(1),
    }
}

#[cfg(test)]
fn suite_success() -> SuiteResult {
    SuiteResult {
        success: true,
        suggested_exit_code: ExitCode(0),
    }
}

#[cfg(test)]
mod test_line_reporter {
    use abq_utils::net_protocol::runners::{Status, TestResult};

    use crate::reporting::{suite_failure, suite_success};

    use super::{default_result, LineReporter, MockWriter, Reporter};

    fn with_reporter(f: impl FnOnce(Box<LineReporter>)) -> MockWriter {
        let mut mock_writer = MockWriter::default();
        {
            // Safety: mock writer only borrowed for duration of `f`, and exists longer than the
            // reporter.
            let borrow_writer: &'static mut MockWriter =
                unsafe { std::mem::transmute(&mut mock_writer) };

            let reporter = LineReporter {
                buffer: Box::new(borrow_writer),
                delayed_failure_reports: Default::default(),
            };

            f(Box::new(reporter));
        }
        mock_writer
    }

    #[test]
    fn write_on_result() {
        let MockWriter {
            buffer,
            num_writes,
            num_flushes,
        } = with_reporter(|mut reporter| {
            reporter.push_result(&default_result()).unwrap();
            reporter.push_result(&default_result()).unwrap();
        });

        assert!(!buffer.is_empty());
        assert!(num_writes > 1);
        assert_eq!(num_flushes, 0);
    }

    #[test]
    fn flush_buffer_when_test_results_done() {
        let MockWriter {
            buffer,
            num_writes,
            num_flushes,
        } = with_reporter(|reporter| reporter.finish(&suite_success()).unwrap());

        assert!(buffer.is_empty());
        assert_eq!(num_writes, 0);
        assert_eq!(num_flushes, 1);
    }

    #[test]
    fn formats_results_as_lines() {
        let MockWriter {
            buffer,
            num_writes: _,
            num_flushes: _,
        } = with_reporter(|mut reporter| {
            reporter
                .push_result(&TestResult {
                    status: Status::Success,
                    display_name: "abq/test1".to_string(),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Failure,
                    display_name: "abq/test2".to_string(),
                    output: Some("Assertion failed: 1 != 2".to_string()),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Skipped,
                    display_name: "abq/test3".to_string(),
                    output: Some(r#"Skipped for reason: "not a summer Friday""#.to_string()),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Error,
                    display_name: "abq/test4".to_string(),
                    output: Some("Process 28821 terminated early via SIGTERM".to_string()),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Pending,
                    display_name: "abq/test5".to_string(),
                    output: Some(
                        r#"Pending for reason: "implementation blocked on #1729""#.to_string(),
                    ),
                    ..default_result()
                })
                .unwrap();
            reporter.finish(&suite_failure()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        abq/test1: ok
        abq/test2: FAILED
        abq/test3: skipped
        abq/test4: ERRORED
        abq/test5: pending

        --- abq/test2: FAILED ---
        Assertion failed: 1 != 2
        (completed in 1 m, 15 s, 3 ms)

        --- abq/test4: ERRORED ---
        Process 28821 terminated early via SIGTERM
        (completed in 1 m, 15 s, 3 ms)
        "###);
    }
}

#[cfg(test)]
mod test_dot_reporter {
    use abq_utils::net_protocol::runners::{Status, TestResult};

    use crate::reporting::{suite_failure, suite_success, DOT_REPORTER_LINE_LIMIT};

    use super::{default_result, DotReporter, MockWriter, Reporter};

    fn with_reporter(f: impl FnOnce(Box<DotReporter>)) -> MockWriter {
        let mut mock_writer = MockWriter::default();
        {
            // Safety: mock writer only borrowed for duration of `f`, and exists longer than the
            // reporter.
            let borrow_writer: &'static mut MockWriter =
                unsafe { std::mem::transmute(&mut mock_writer) };

            let reporter = DotReporter {
                buffer: Box::new(borrow_writer),
                num_results: 0,
                delayed_failure_reports: Default::default(),
            };

            f(Box::new(reporter));
        }
        mock_writer
    }

    #[test]
    fn write_on_result() {
        let MockWriter {
            buffer,
            num_writes,
            num_flushes,
        } = with_reporter(|mut reporter| {
            reporter.push_result(&default_result()).unwrap();
            reporter.push_result(&default_result()).unwrap();
        });

        assert!(!buffer.is_empty());
        assert_eq!(num_writes, 2);
        assert_eq!(num_flushes, 2, "each dot write should be flushed!");
    }

    #[test]
    fn flush_buffer_when_test_results_done() {
        let MockWriter {
            buffer,
            num_writes,
            num_flushes,
        } = with_reporter(|reporter| reporter.finish(&suite_success()).unwrap());

        assert!(buffer.is_empty());
        assert_eq!(num_writes, 0);
        assert_eq!(num_flushes, 1);
    }

    #[test]
    fn formats_results_as_dots_with_summary() {
        let MockWriter {
            buffer,
            num_writes: _,
            num_flushes: _,
        } = with_reporter(|mut reporter| {
            reporter
                .push_result(&TestResult {
                    status: Status::Success,
                    display_name: "abq/test1".to_string(),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Failure,
                    display_name: "abq/test2".to_string(),
                    output: Some("Assertion failed: 1 != 2".to_string()),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Skipped,
                    display_name: "abq/test3".to_string(),
                    output: Some(r#"Skipped for reason: "not a summer Friday""#.to_string()),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Error,
                    display_name: "abq/test4".to_string(),
                    output: Some("Process 28821 terminated early via SIGTERM".to_string()),
                    ..default_result()
                })
                .unwrap();
            reporter
                .push_result(&TestResult {
                    status: Status::Pending,
                    display_name: "abq/test5".to_string(),
                    output: Some(
                        r#"Pending for reason: "implementation blocked on #1729""#.to_string(),
                    ),
                    ..default_result()
                })
                .unwrap();
            reporter.finish(&suite_failure()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        .FSEP

        --- abq/test2: FAILED ---
        Assertion failed: 1 != 2
        (completed in 1 m, 15 s, 3 ms)

        --- abq/test4: ERRORED ---
        Process 28821 terminated early via SIGTERM
        (completed in 1 m, 15 s, 3 ms)
        "###);
    }

    #[test]
    fn breaks_lines_with_many_dots() {
        let MockWriter {
            buffer,
            num_writes: _,
            num_flushes: _,
        } = with_reporter(|mut reporter| {
            for i in 1..134 {
                let status = if i % 17 == 0 {
                    Status::Skipped
                } else if i % 31 == 0 {
                    Status::Pending
                } else {
                    Status::Success
                };

                reporter
                    .push_result(&TestResult {
                        status,
                        ..default_result()
                    })
                    .unwrap();
            }

            reporter.finish(&suite_success()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        ................S.............P..S......
        ..........S..........P.....S............
        ....S.......P........S................S.
        ...P.........
        "###);
    }

    #[test]
    fn dot_line_limit_fits_on_one_line() {
        let MockWriter {
            buffer,
            num_writes: _,
            num_flushes: _,
        } = with_reporter(|mut reporter| {
            for _ in 0..DOT_REPORTER_LINE_LIMIT {
                reporter.push_result(&default_result()).unwrap();
            }

            reporter.finish(&suite_success()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        ........................................
        "###);
    }

    #[test]
    fn one_newline_before_summaries_line_limit_not_reached() {
        let MockWriter {
            buffer,
            num_writes: _,
            num_flushes: _,
        } = with_reporter(|mut reporter| {
            for i in 0..(DOT_REPORTER_LINE_LIMIT * 2) + DOT_REPORTER_LINE_LIMIT / 3 {
                let status = if i == 1 {
                    Status::Failure
                } else {
                    Status::Success
                };

                reporter
                    .push_result(&TestResult {
                        status,
                        ..default_result()
                    })
                    .unwrap();
            }

            reporter.finish(&suite_failure()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        .F......................................
        ........................................
        .............

        --- default name: FAILED ---
        default output
        (completed in 1 m, 15 s, 3 ms)
        "###);
    }

    #[test]
    fn one_newline_before_summaries_line_limit_reached() {
        let MockWriter {
            buffer,
            num_writes: _,
            num_flushes: _,
        } = with_reporter(|mut reporter| {
            for i in 0..(DOT_REPORTER_LINE_LIMIT * 2) {
                let status = if i == 1 {
                    Status::Failure
                } else {
                    Status::Success
                };

                reporter
                    .push_result(&TestResult {
                        status,
                        ..default_result()
                    })
                    .unwrap();
            }

            reporter.finish(&suite_failure()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        .F......................................
        ........................................

        --- default name: FAILED ---
        default output
        (completed in 1 m, 15 s, 3 ms)
        "###);
    }
}
