mod summary;

mod retry_manifest_tracker;

use std::{
    borrow::Cow,
    fmt::Display,
    io::{self},
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use abq_output::{
    colors::ColorProvider, format_interactive_progress, format_non_interactive_progress,
    format_result_dot, format_result_line, format_runner_output, format_short_suite_summary,
    format_summary, format_test_result_summary, would_write_output, would_write_summary,
    OutputOrdering, ShortSummary, SummaryKind,
};
use abq_queue::invoke::{self, CompletedSummary};
use abq_utils::{
    exit::ExitCode,
    net_protocol::{
        client::ReportedResult,
        queue::TestSpec,
        runners::{CapturedOutput, Status, TestResult, TestRuntime},
        workers::WorkId,
    },
};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use termcolor::{ColorChoice, StandardStream};
use thiserror::Error;

static DEFAULT_XML_PATH: &str = "abq-test-results.xml";
static DEFAULT_JSON_PATH: &str = "abq-test-results.json";

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum ReporterKind {
    /// Writes results line-by-line to stdout
    Line,
    /// Writes results as dots to stdout
    Dot,
    /// Writes results to a progress bar and immediately prints output and failures
    Progress,
    /// Writes JUnit XML to a file
    JUnitXml(PathBuf),
    /// Writes RWX Test Results (https://github.com/rwx-research/test-results-schema/blob/main/v1.json) to a file
    RwxV1Json(PathBuf),
}

impl Display for ReporterKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReporterKind::Line => write!(f, "line"),
            ReporterKind::Dot => write!(f, "dot"),
            ReporterKind::Progress => write!(f, "progress"),
            ReporterKind::JUnitXml(path) => write!(f, "junit-xml={}", path.display()),
            ReporterKind::RwxV1Json(path) => write!(f, "rwx-v1-json={}", path.display()),
        }
    }
}

impl FromStr for ReporterKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "line" => Ok(Self::Line),
            "dot" => Ok(Self::Dot),
            "progress" => Ok(Self::Progress),
            other => {
                let mut splits = other.split('=');
                let reporter = splits.next().filter(|reporter| !reporter.trim().is_empty());
                let path = splits.next().filter(|path| !path.trim().is_empty());

                match reporter {
                    Some("junit-xml") => {
                        let path = PathBuf::from_str(path.unwrap_or(DEFAULT_XML_PATH))
                            .map_err(|e| e.to_string())?;
                        Ok(ReporterKind::JUnitXml(path))
                    }
                    Some("rwx-v1-json") => {
                        let path = PathBuf::from_str(path.unwrap_or(DEFAULT_JSON_PATH))
                            .map_err(|e| e.to_string())?;
                        Ok(ReporterKind::RwxV1Json(path))
                    }
                    Some(other) => Err(format!("Unknown reporter {}", other)),
                    None => Err(format!("Reporter not specified in '{}'", other)),
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
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

pub(crate) struct SuiteResult {
    /// Exit code suggested for the test suite.
    suggested_exit_code: ExitCode,
    /// Total number of tests run.
    count: u64,
    count_failed: u64,
    /// How many individual tests were retried. Not the total count of how many retries were run.
    /// For example, if test A was retried twice, its count in this metric is one.
    tests_retried: u64,
    /// Runtime of the test suite, as accounted between the time the reporter started and the time
    /// it finished.
    wall_time: Duration,
    /// Runtime of the test suite, as accounted for in the actual time in tests.
    test_time: TestRuntime,
}

impl SuiteResult {
    pub fn write_short_summary_lines(&self, w: &mut impl termcolor::WriteColor) -> io::Result<()> {
        format_short_suite_summary(
            w,
            ShortSummary {
                wall_time: self.wall_time,
                test_time: self.test_time.duration(),
                num_tests: self.count,
                num_failing: self.count_failed,
                num_retried: self.tests_retried,
            },
        )
    }

    pub fn suggested_exit_code(&self) -> ExitCode {
        self.suggested_exit_code
    }
}

/// A [`Reporter`] defines a way to emit abq test results.
///
/// A reporter is allowed to be side-effectful.
pub(crate) trait Reporter: Send {
    /// Consume the next test result.
    fn push_result(
        &mut self,
        run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError>;

    fn tick(&mut self);

    /// Runs after the last call to [Self::push_result] and [Self::tick], but before [Self::finish].
    fn after_all_results(&mut self);

    /// Consume the reporter, and perform any needed finalization steps.
    ///
    /// This method is only called when all test results for a run have been consumed.
    fn finish(self: Box<Self>, summary: &CompletedSummary) -> Result<(), ReportingError>;
}

fn write(writer: &mut impl io::Write, buf: &[u8]) -> Result<(), ReportingError> {
    writer
        .write_all(buf)
        .map_err(|_| ReportingError::FailedToWrite)
}

fn write_summary_results(
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

/// Streams all test results line-by-line to an output buffer, and prints a summary for failing and
/// erroring tests at the end.
struct LineReporter {
    /// The output buffer.
    buffer: Box<dyn termcolor::WriteColor + Send>,

    /// Failures and errors for which a longer summary should be printed at the end.
    delayed_summaries: Vec<SummaryKind<'static>>,

    seen_first: bool,
}

impl Reporter for LineReporter {
    fn push_result(
        &mut self,
        run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError> {
        let ReportedResult {
            output_before,
            output_after,
            test_result,
        } = result;

        format_result_line(
            &mut self.buffer,
            test_result,
            !self.seen_first,
            output_before,
            output_after,
        )?;

        self.seen_first = true;

        if matches!(test_result.status, Status::PrivateNativeRunnerError) {
            format_test_result_summary(&mut self.buffer, run_number, test_result)?;
        } else if test_result.status.is_fail_like() {
            self.delayed_summaries.push(SummaryKind::Test {
                run_number,
                result: test_result.clone(),
            });
        }

        Ok(())
    }

    fn tick(&mut self) {}

    fn after_all_results(&mut self) {
        let _ = write_summary_results(
            &mut self.buffer,
            std::mem::take(&mut self.delayed_summaries),
        );
    }

    fn finish(mut self: Box<Self>, _summary: &CompletedSummary) -> Result<(), ReportingError> {
        self.buffer
            .flush()
            .map_err(|_| ReportingError::FailedToWrite)?;

        Ok(())
    }
}

/// Max number of dots to print per line for the dot reporter.
const DOT_REPORTER_LINE_LIMIT: u64 = 80;

/// Streams test results as dots to an output buffer, and prints a summary for failing and erroring
/// tests at the end.
struct DotReporter {
    buffer: Box<dyn termcolor::WriteColor + Send>,

    num_results: u64,

    delayed_summaries: Vec<SummaryKind<'static>>,
}
impl DotReporter {
    fn maybe_push_delayed_output(
        &mut self,
        test_result: &TestResult,
        opt_output: &Option<CapturedOutput>,
        when: OutputOrdering<'static>,
    ) {
        if let Some(output) = opt_output.as_ref() {
            self.delayed_summaries.push(SummaryKind::Output {
                when,
                runner: test_result.source,
                output: output.clone(),
            });
        }
    }
}

impl Reporter for DotReporter {
    fn push_result(
        &mut self,
        run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError> {
        let ReportedResult {
            output_before,
            output_after,
            test_result,
        } = result;

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

        self.maybe_push_delayed_output(
            test_result,
            output_before,
            OutputOrdering::Before(Cow::Owned(test_result.display_name.clone())),
        );

        if matches!(test_result.status, Status::PrivateNativeRunnerError) {
            format_test_result_summary(&mut self.buffer, run_number, test_result)?;
        } else if test_result.status.is_fail_like() {
            self.delayed_summaries.push(SummaryKind::Test {
                run_number,
                result: test_result.clone(),
            });
        }

        self.maybe_push_delayed_output(
            test_result,
            output_after,
            OutputOrdering::After(Cow::Owned(test_result.display_name.clone())),
        );

        Ok(())
    }

    fn tick(&mut self) {}

    fn after_all_results(&mut self) {
        if self.num_results % DOT_REPORTER_LINE_LIMIT != 0 {
            let _ = write(&mut self.buffer, &[b'\n']);
        }

        let _ = write_summary_results(
            &mut self.buffer,
            std::mem::take(&mut self.delayed_summaries),
        );
    }

    fn finish(mut self: Box<Self>, _summary: &CompletedSummary) -> Result<(), ReportingError> {
        self.buffer
            .flush()
            .map_err(|_| ReportingError::FailedToWrite)?;

        Ok(())
    }
}

/// Streams a progress bar of number of executed tests and any failures so far.
/// Writes out failures and captured output as soon as it is received.
struct ProgressReporter {
    buffer: Box<dyn termcolor::WriteColor + Send>,
    color_provider: ColorProvider,
    progress_bar: Option<indicatif::ProgressBar>,
    started_at: Instant,

    ticks: usize,
    num_results: u64,
    num_failing: u64,
    wrote_first_output: bool,
}
impl ProgressReporter {
    fn new(
        buffer: Box<dyn termcolor::WriteColor + Send>,
        color_provider: ColorProvider,
        // false if non-interactive
        opt_progress_bar_target: Option<ProgressDrawTarget>,
    ) -> Self {
        let progress_bar = opt_progress_bar_target.map(|target| {
            ProgressBar::with_draw_target(None, target)
                .with_style(ProgressStyle::with_template("{msg}").unwrap())
        });
        Self {
            buffer,
            progress_bar,
            color_provider,
            started_at: Instant::now(),
            ticks: 0,
            num_results: 0,
            num_failing: 0,
            wrote_first_output: false,
        }
    }

    fn tick_progress(&mut self, timed_tick: bool) {
        // Only include in the tick count explicit calls to `tick()` based on timed metrics;
        // exclude ticks we call when writing results.
        self.ticks += timed_tick as usize;

        if let Some(progress_bar) = &self.progress_bar {
            self.tick_interactive(progress_bar);
        } else if timed_tick {
            // Only tick in a non-interactive context if this is in fact a timed tick.
            let _opt_err = self.tick_non_interactive();
        }
    }

    fn tick_interactive(&self, pb: &ProgressBar) {
        let elapsed = indicatif::HumanDuration(self.started_at.elapsed());
        pb.set_message(format_interactive_progress(
            &self.color_provider,
            elapsed,
            self.num_results,
            self.num_failing,
        ));
        pb.tick();
    }

    fn tick_non_interactive(&mut self) -> io::Result<()> {
        if (self.ticks - 1) % 10 != 0 {
            // In non-interactive contexts, only write every 10 ticks to avoid unnecessary writes
            // to the output.
            return Ok(());
        }

        let elapsed = indicatif::HumanDuration(self.started_at.elapsed());

        if self.wrote_first_output {
            writeln!(&mut self.buffer)?;
        }

        format_non_interactive_progress(
            &mut self.buffer,
            elapsed,
            self.num_results,
            self.num_failing,
        )?;

        self.wrote_first_output = true;

        Ok(())
    }
}

impl Reporter for ProgressReporter {
    fn push_result(
        &mut self,
        run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError> {
        let ReportedResult {
            output_before,
            output_after,
            test_result,
        } = result;

        self.num_results += 1;

        let mut write_result = || {
            let is_fail_like = test_result.status.is_fail_like();
            self.num_failing += is_fail_like as u64;

            let something_to_write = is_fail_like
                || would_write_output(output_before.as_ref())
                || would_write_output(output_after.as_ref());
            if something_to_write && self.wrote_first_output {
                write(&mut self.buffer, &[b'\n'])?;
            }

            if let Some(output) = output_before {
                format_runner_output(
                    &mut self.buffer,
                    test_result.source,
                    OutputOrdering::Before(Cow::Owned(test_result.display_name.clone())),
                    output,
                )?;
            }

            if is_fail_like {
                format_test_result_summary(&mut self.buffer, run_number, test_result)?;
            }

            if let Some(output) = output_after {
                format_runner_output(
                    &mut self.buffer,
                    test_result.source,
                    OutputOrdering::Before(Cow::Owned(test_result.display_name.clone())),
                    output,
                )?;
            }

            self.wrote_first_output = self.wrote_first_output || something_to_write;

            Result::<(), ReportingError>::Ok(())
        };

        if let Some(pb) = self.progress_bar.as_mut() {
            pb.suspend(write_result)?;
        } else {
            write_result()?;
        }

        self.tick_progress(false);

        Ok(())
    }

    fn tick(&mut self) {
        self.tick_progress(true);
    }

    fn after_all_results(&mut self) {
        if let Some(pb) = self.progress_bar.as_mut() {
            pb.finish_and_clear();
        }
    }

    fn finish(mut self: Box<Self>, _summary: &CompletedSummary) -> Result<(), ReportingError> {
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
    fn push_result(
        &mut self,
        _run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError> {
        self.collector.push_result(&result.test_result);
        Ok(())
    }

    fn tick(&mut self) {}

    fn after_all_results(&mut self) {}

    fn finish(self: Box<Self>, _summary: &CompletedSummary) -> Result<(), ReportingError> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        let fd = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.path)?;

        self.collector
            .write_xml(fd)
            .map_err(|_| ReportingError::FailedToFormat)?;
        Ok(())
    }
}

/// Writes test results as JUnit XML to a path.
struct RwxV1JsonReporter {
    path: PathBuf,
    collector: abq_rwx_v1_json::Collector,
}

impl Reporter for RwxV1JsonReporter {
    fn push_result(
        &mut self,
        _run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError> {
        self.collector.push_result(&result.test_result);
        Ok(())
    }

    fn tick(&mut self) {}

    fn after_all_results(&mut self) {}

    fn finish(self: Box<Self>, summary: &CompletedSummary) -> Result<(), ReportingError> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        let fd = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.path)?;

        self.collector
            .write_json(fd, &summary.native_runner_info.specification)
            .map_err(|_| ReportingError::FailedToFormat)?;

        Ok(())
    }
}

fn is_ci_color_term() -> bool {
    std::env::var("GITHUB_ACTIONS").as_deref() == Ok("true")
        || std::env::var("BUILDKITE").as_deref() == Ok("true")
        || std::env::var("CIRCLECI").as_deref() == Ok("true")
}

#[derive(Clone, Copy)]
pub struct StdoutPreferences {
    is_atty: bool,
    color: ColorChoice,
}

impl StdoutPreferences {
    pub fn new(color_preference: ColorPreference) -> Self {
        let is_atty = atty::is(atty::Stream::Stdout);
        let color = match color_preference {
            ColorPreference::Auto => {
                if is_atty || is_ci_color_term() {
                    ColorChoice::Auto
                } else {
                    ColorChoice::Never
                }
            }
            ColorPreference::Never => ColorChoice::Never,
        };
        Self { is_atty, color }
    }

    pub fn stdout_stream(&self) -> impl termcolor::WriteColor {
        StandardStream::stdout(self.color)
    }
}

fn reporter_from_kind(
    kind: ReporterKind,
    stdout_preferences: StdoutPreferences,
    test_suite_name: &str,
) -> Box<dyn Reporter> {
    let stdout = stdout_preferences.stdout_stream();

    match kind {
        ReporterKind::Line => Box::new(LineReporter {
            buffer: Box::new(stdout),
            delayed_summaries: Default::default(),
            seen_first: false,
        }),
        ReporterKind::Dot => Box::new(DotReporter {
            buffer: Box::new(stdout),
            num_results: 0,
            delayed_summaries: Default::default(),
        }),
        ReporterKind::Progress => {
            let color_provider = match stdout_preferences.color {
                ColorChoice::Always | ColorChoice::AlwaysAnsi | ColorChoice::Auto => {
                    ColorProvider::ANSI
                }
                ColorChoice::Never => ColorProvider::NOCOLOR,
            };
            let opt_target = if stdout_preferences.is_atty {
                Some(ProgressDrawTarget::stdout())
            } else {
                None
            };
            Box::new(ProgressReporter::new(
                Box::new(stdout),
                color_provider,
                opt_target,
            ))
        }
        ReporterKind::JUnitXml(path) => Box::new(JUnitXmlReporter {
            path,
            collector: abq_junit_xml::Collector::new(test_suite_name),
        }),
        ReporterKind::RwxV1Json(path) => Box::new(RwxV1JsonReporter {
            path,
            collector: abq_rwx_v1_json::Collector::default(),
        }),
    }
}

pub(crate) struct SuiteReporters {
    reporters: Vec<Box<dyn Reporter>>,
    overall_tracker: summary::SuiteTracker,
}

impl SuiteReporters {
    pub fn new(
        reporter_kinds: Vec<ReporterKind>,
        stdout_preferences: StdoutPreferences,
        test_suite_name: &str,
        manifest: Vec<TestSpec>,
    ) -> Self {
        Self {
            reporters: reporter_kinds
                .into_iter()
                .map(|kind| reporter_from_kind(kind, stdout_preferences, test_suite_name))
                .collect(),
            overall_tracker: summary::SuiteTracker::new(manifest),
        }
    }

    fn push_result(
        &mut self,
        work_id: WorkId,
        run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), Vec<ReportingError>> {
        let errors: Vec<_> = self
            .reporters
            .iter_mut()
            .filter_map(|reporter| reporter.push_result(run_number, result).err())
            .collect();

        self.overall_tracker
            .account_result(work_id, run_number, &result.test_result);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn do_tick(&mut self) {
        self.reporters
            .iter_mut()
            .for_each(|reporter| reporter.tick());
    }

    pub fn after_all_results(&mut self) {
        self.reporters
            .iter_mut()
            .for_each(|reporter| reporter.after_all_results());
    }

    pub fn finish(self, summary: &CompletedSummary) -> (SuiteResult, Vec<ReportingError>) {
        let Self {
            reporters,
            overall_tracker,
        } = self;

        let overall_result = overall_tracker.suite_result();

        let errors: Vec<_> = reporters
            .into_iter()
            .filter_map(|reporter| reporter.finish(summary).err())
            .collect();

        (overall_result, errors)
    }
}

impl invoke::ResultHandler for SuiteReporters {
    type InitData = (Vec<ReporterKind>, StdoutPreferences, String);

    fn create(manifest: Vec<TestSpec>, init: Self::InitData) -> Self {
        let (reporter_kinds, stdout_preferences, test_suite_name) = init;
        Self::new(
            reporter_kinds,
            stdout_preferences,
            &test_suite_name,
            manifest,
        )
    }

    fn on_result(&mut self, work_id: WorkId, run_number: u32, result: ReportedResult) {
        // TODO: is there a reasonable way to surface the error?
        let _opt_error = self.push_result(work_id, run_number, &result);
    }

    fn get_ordered_retry_manifest(&mut self, run_number: u32) -> Vec<TestSpec> {
        self.overall_tracker.ordered_retry_manifest(run_number)
    }

    fn get_exit_code(&self) -> ExitCode {
        self.overall_tracker.exit_code()
    }

    fn tick(&mut self) {
        self.do_tick()
    }
}

#[cfg(test)]
mod test_reporter_kind {
    use super::{ReporterKind, DEFAULT_JSON_PATH, DEFAULT_XML_PATH};
    use std::{path::PathBuf, str::FromStr};

    #[test]
    fn parse_unknown_reporter() {
        assert_eq!(
            ReporterKind::from_str("not-a-reporter"),
            Err("Unknown reporter not-a-reporter".to_string())
        );
    }

    #[test]
    fn parse_only_reporter_path() {
        assert_eq!(
            ReporterKind::from_str("=the/path"),
            Err("Reporter not specified in '=the/path'".to_string())
        );
    }

    #[test]
    fn parse_empty_reporter() {
        assert_eq!(
            ReporterKind::from_str(""),
            Err("Reporter not specified in ''".to_string())
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
    fn parse_progress_reporter() {
        assert_eq!(
            ReporterKind::from_str("progress"),
            Ok(ReporterKind::Progress)
        );
    }

    #[test]
    fn parse_junit_reporter_no_eq_to_default() {
        assert_eq!(
            ReporterKind::from_str("junit-xml"),
            Ok(ReporterKind::JUnitXml(PathBuf::from(DEFAULT_XML_PATH)))
        );
    }

    #[test]
    fn parse_junit_reporter_eq_no_suffix_to_default() {
        assert_eq!(
            ReporterKind::from_str("junit-xml="),
            Ok(ReporterKind::JUnitXml(PathBuf::from(DEFAULT_XML_PATH)))
        );
    }

    #[test]
    fn parse_junit_reporter_eq_no_suffix_with_spaces_to_default() {
        assert_eq!(
            ReporterKind::from_str("junit-xml=     "),
            Ok(ReporterKind::JUnitXml(PathBuf::from(DEFAULT_XML_PATH)))
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

    #[test]
    fn parse_rwx_v1_json_reporter_no_eq_to_default() {
        assert_eq!(
            ReporterKind::from_str("rwx-v1-json"),
            Ok(ReporterKind::RwxV1Json(PathBuf::from(DEFAULT_JSON_PATH)))
        );
    }

    #[test]
    fn parse_rwx_v1_json_reporter_eq_no_suffix_to_default() {
        assert_eq!(
            ReporterKind::from_str("rwx-v1-json="),
            Ok(ReporterKind::RwxV1Json(PathBuf::from(DEFAULT_JSON_PATH)))
        );
    }

    #[test]
    fn parse_rwx_v1_json_reporter_eq_no_suffix_with_spaces_to_default() {
        assert_eq!(
            ReporterKind::from_str("rwx-v1-json=     "),
            Ok(ReporterKind::RwxV1Json(PathBuf::from(DEFAULT_JSON_PATH)))
        );
    }

    #[test]
    fn parse_rwx_v1_json_reporter_eq_with_suffix_to_absolute_path() {
        assert_eq!(
            ReporterKind::from_str("rwx-v1-json=/my/test.xml"),
            Ok(ReporterKind::RwxV1Json(PathBuf::from("/my/test.xml")))
        );
    }

    #[test]
    fn parse_rwx_v1_json_reporter_eq_with_suffix_to_relative_path() {
        assert_eq!(
            ReporterKind::from_str("rwx-v1-json=my/test.xml"),
            Ok(ReporterKind::RwxV1Json(PathBuf::from("my/test.xml")))
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
impl io::Write for MockWriter {
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
use abq_utils::net_protocol::runners::TestResultSpec;

#[cfg(test)]
fn mock_summary() -> CompletedSummary {
    use abq_utils::net_protocol::{queue::NativeRunnerInfo, runners::NativeRunnerSpecification};

    CompletedSummary {
        native_runner_info: NativeRunnerInfo {
            protocol_version: abq_utils::net_protocol::runners::AbqProtocolVersion::V0_2,
            specification: NativeRunnerSpecification {
                name: "test-runner".to_owned(),
                version: "1.2.3".to_owned(),
                test_framework: "zframework".to_owned(),
                test_framework_version: "4.5.6".to_owned(),
                language: "zlang".to_owned(),
                language_version: "7.8.9".to_owned(),
                host: "zmachine".to_owned(),
            },
        },
    }
}

#[cfg(test)]
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

#[cfg(test)]
mod test_line_reporter {
    use abq_utils::net_protocol::{
        client::ReportedResult,
        entity::RunnerMeta,
        runners::{CapturedOutput, Status, TestResult, TestResultSpec},
        workers::INIT_RUN_NUMBER,
    };

    use crate::reporting::mock_summary;

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
                delayed_summaries: Default::default(),
                seen_first: false,
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
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        default_result(),
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        default_result(),
                    )),
                )
                .unwrap();
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
        } = with_reporter(|reporter| reporter.finish(&mock_summary()).unwrap());

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
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Success,
                            display_name: "abq/test1".to_string(),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Failure {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test2".to_string(),
                            output: Some("Assertion failed: 1 != 2".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult {
                        test_result: TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                status: Status::Skipped,
                                display_name: "abq/test3".to_string(),
                                output: Some(
                                    r#"Skipped for reason: "not a summer Friday""#.to_string(),
                                ),
                                ..default_result()
                            },
                        ),
                        output_before: Some(CapturedOutput {
                            stderr: b"test3-stderr".to_vec(),
                            stdout: b"test3-stdout".to_vec(),
                        }),
                        output_after: None,
                    },
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Error {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test4".to_string(),
                            output: Some("Process 28821 terminated early via SIGTERM".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Pending,
                            display_name: "abq/test5".to_string(),
                            output: Some(
                                r#"Pending for reason: "implementation blocked on #1729""#
                                    .to_string(),
                            ),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        abq/test1: ok
        abq/test2: FAILED

        --- [worker 0] BEFORE abq/test3 ---
        ----- STDOUT
        test3-stdout
        ----- STDERR
        test3-stderr

        abq/test3: skipped
        abq/test4: ERRORED
        abq/test5: pending

        --- abq/test2: FAILED ---
        Assertion failed: 1 != 2
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])

        --- abq/test4: ERRORED ---
        Process 28821 terminated early via SIGTERM
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])
        "###);
    }
}

#[cfg(test)]
mod test_dot_reporter {
    use abq_utils::net_protocol::{
        client::ReportedResult,
        entity::RunnerMeta,
        runners::{CapturedOutput, Status, TestResult, TestResultSpec},
        workers::INIT_RUN_NUMBER,
    };

    use crate::reporting::{mock_summary, DOT_REPORTER_LINE_LIMIT};

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
                delayed_summaries: Default::default(),
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
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        default_result(),
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        default_result(),
                    )),
                )
                .unwrap();
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
        } = with_reporter(|reporter| reporter.finish(&mock_summary()).unwrap());

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
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Success,
                            display_name: "abq/test1".to_string(),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Failure {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test2".to_string(),
                            output: Some("Assertion failed: 1 != 2".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult {
                        test_result: TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                status: Status::Skipped,
                                display_name: "abq/test3".to_string(),
                                output: Some(
                                    r#"Skipped for reason: "not a summer Friday""#.to_string(),
                                ),
                                ..default_result()
                            },
                        ),
                        output_before: Some(CapturedOutput {
                            stderr: b"test3-stderr".to_vec(),
                            stdout: b"test3-stdout".to_vec(),
                        }),
                        output_after: None,
                    },
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Error {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test4".to_string(),
                            output: Some("Process 28821 terminated early via SIGTERM".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Pending,
                            display_name: "abq/test5".to_string(),
                            output: Some(
                                r#"Pending for reason: "implementation blocked on #1729""#
                                    .to_string(),
                            ),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        .FSEP

        --- abq/test2: FAILED ---
        Assertion failed: 1 != 2
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])

        --- [worker 0] BEFORE abq/test3 ---
        ----- STDOUT
        test3-stdout
        ----- STDERR
        test3-stderr


        --- abq/test4: ERRORED ---
        Process 28821 terminated early via SIGTERM
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])
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
                    .push_result(
                        INIT_RUN_NUMBER,
                        &ReportedResult::no_captures(TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                status,
                                ..default_result()
                            },
                        )),
                    )
                    .unwrap();
            }

            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        ................S.............P..S................S..........P.....S............
        ....S.......P........S................S....P.........
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
                reporter
                    .push_result(
                        INIT_RUN_NUMBER,
                        &ReportedResult::no_captures(TestResult::new(
                            RunnerMeta::fake(),
                            default_result(),
                        )),
                    )
                    .unwrap();
            }

            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        ................................................................................
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
                    Status::Failure {
                        exception: None,
                        backtrace: None,
                    }
                } else {
                    Status::Success
                };

                reporter
                    .push_result(
                        INIT_RUN_NUMBER,
                        &ReportedResult::no_captures(TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                status,
                                ..default_result()
                            },
                        )),
                    )
                    .unwrap();
            }

            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        .F..............................................................................
        ................................................................................
        ..........................

        --- default name: FAILED ---
        default output
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])
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
                    Status::Failure {
                        exception: None,
                        backtrace: None,
                    }
                } else {
                    Status::Success
                };

                reporter
                    .push_result(
                        INIT_RUN_NUMBER,
                        &ReportedResult::no_captures(TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                status,
                                ..default_result()
                            },
                        )),
                    )
                    .unwrap();
            }

            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        .F..............................................................................
        ................................................................................

        --- default name: FAILED ---
        default output
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])
        "###);
    }
}

#[cfg(test)]
mod test_progress_reporter {
    use std::sync::{Arc, Mutex};

    use abq_output::colors::ColorProvider;
    use abq_utils::net_protocol::{
        client::ReportedResult,
        entity::RunnerMeta,
        runners::{CapturedOutput, Status, TestResult, TestResultSpec},
        workers::INIT_RUN_NUMBER,
    };
    use indicatif::{ProgressDrawTarget, TermLike};

    use crate::reporting::mock_summary;

    use super::{default_result, MockWriter, ProgressReporter, Reporter};

    #[derive(Default, Debug, Clone)]
    struct MockProgressBar {
        cmds: Arc<Mutex<Vec<String>>>,
    }

    impl TermLike for MockProgressBar {
        fn width(&self) -> u16 {
            100
        }
        fn move_cursor_up(&self, _n: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn move_cursor_down(&self, _n: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn move_cursor_right(&self, _n: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn move_cursor_left(&self, _n: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn write_line(&self, s: &str) -> std::io::Result<()> {
            self.cmds.lock().unwrap().push(s.to_string());
            Ok(())
        }
        fn write_str(&self, s: &str) -> std::io::Result<()> {
            self.cmds.lock().unwrap().push(s.to_string());
            Ok(())
        }
        fn clear_line(&self) -> std::io::Result<()> {
            Ok(())
        }
        fn flush(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn with_interactive_reporter(
        f: impl FnOnce(Box<ProgressReporter>),
    ) -> (MockWriter, Vec<String>) {
        let mut mock_writer = MockWriter::default();
        let mock_progress_bar = MockProgressBar::default();
        {
            // Safety: mock writer only borrowed for duration of `f`, and exists longer than the
            // reporter.
            let borrow_writer: &'static mut MockWriter =
                unsafe { std::mem::transmute(&mut mock_writer) };

            let reporter = ProgressReporter::new(
                Box::new(borrow_writer),
                ColorProvider::CMD,
                Some(ProgressDrawTarget::term_like(Box::new(
                    mock_progress_bar.clone(),
                ))),
            );

            f(Box::new(reporter));
        }
        let progress_bar_cmds = Arc::try_unwrap(mock_progress_bar.cmds)
            .unwrap()
            .into_inner()
            .unwrap();
        (mock_writer, progress_bar_cmds)
    }

    fn with_non_interactive_reporter(f: impl FnOnce(Box<ProgressReporter>)) -> MockWriter {
        let mut mock_writer = MockWriter::default();
        {
            // Safety: mock writer only borrowed for duration of `f`, and exists longer than the
            // reporter.
            let borrow_writer: &'static mut MockWriter =
                unsafe { std::mem::transmute(&mut mock_writer) };

            let reporter = ProgressReporter::new(Box::new(borrow_writer), ColorProvider::CMD, None);

            f(Box::new(reporter));
        }
        mock_writer
    }

    #[test]
    fn formats_interactive() {
        let (
            MockWriter {
                buffer,
                num_writes: _,
                num_flushes: _,
            },
            progress_bar_cmds,
        ) = with_interactive_reporter(|mut reporter| {
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Success,
                            display_name: "abq/test1".to_string(),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Failure {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test2".to_string(),
                            output: Some("Assertion failed: 1 != 2".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult {
                        test_result: TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                status: Status::Skipped,
                                display_name: "abq/test3".to_string(),
                                output: Some(
                                    r#"Skipped for reason: "not a summer Friday""#.to_string(),
                                ),
                                ..default_result()
                            },
                        ),
                        output_before: Some(CapturedOutput {
                            stderr: b"test3-stderr".to_vec(),
                            stdout: b"test3-stdout".to_vec(),
                        }),
                        output_after: None,
                    },
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Error {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test4".to_string(),
                            output: Some("Process 28821 terminated early via SIGTERM".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Pending,
                            display_name: "abq/test5".to_string(),
                            output: Some(
                                r#"Pending for reason: "implementation blocked on #1729""#
                                    .to_string(),
                            ),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        --- abq/test2: FAILED ---
        Assertion failed: 1 != 2
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])

        --- [worker 0] BEFORE abq/test3 ---
        ----- STDOUT
        test3-stdout
        ----- STDERR
        test3-stderr


        --- abq/test4: ERRORED ---
        Process 28821 terminated early via SIGTERM
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])
        "###);

        insta::assert_snapshot!(progress_bar_cmds.join("\n"), @r###"
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 1 tests run<reset>, <green-bold>1 passed<reset>, <reset>0 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 1 tests run<reset>, <green-bold>1 passed<reset>, <reset>0 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 1 tests run<reset>, <green-bold>1 passed<reset>, <reset>0 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 2 tests run<reset>, <green-bold>1 passed<reset>, <red-bold>1 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 2 tests run<reset>, <green-bold>1 passed<reset>, <red-bold>1 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 2 tests run<reset>, <green-bold>1 passed<reset>, <red-bold>1 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 3 tests run<reset>, <green-bold>2 passed<reset>, <red-bold>1 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 3 tests run<reset>, <green-bold>2 passed<reset>, <red-bold>1 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 3 tests run<reset>, <green-bold>2 passed<reset>, <red-bold>1 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 4 tests run<reset>, <green-bold>2 passed<reset>, <red-bold>2 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 4 tests run<reset>, <green-bold>2 passed<reset>, <red-bold>2 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 4 tests run<reset>, <green-bold>2 passed<reset>, <red-bold>2 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 5 tests run<reset>, <green-bold>3 passed<reset>, <red-bold>2 failing<reset>

                                                                                                            
        <bold>> ABQ status<reset>
        <bold>> [0 seconds] 5 tests run<reset>, <green-bold>3 passed<reset>, <red-bold>2 failing<reset>

                                                                                                            
        "###);
    }

    #[test]
    fn formats_non_interactive() {
        let MockWriter {
            buffer,
            num_writes: _,
            num_flushes: _,
        } = with_non_interactive_reporter(|mut reporter| {
            // Force a first progress bar
            reporter.tick();

            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Success,
                            display_name: "abq/test1".to_string(),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Failure {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test2".to_string(),
                            output: Some("Assertion failed: 1 != 2".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult {
                        test_result: TestResult::new(
                            RunnerMeta::fake(),
                            TestResultSpec {
                                status: Status::Skipped,
                                display_name: "abq/test3".to_string(),
                                output: Some(
                                    r#"Skipped for reason: "not a summer Friday""#.to_string(),
                                ),
                                ..default_result()
                            },
                        ),
                        output_before: Some(CapturedOutput {
                            stderr: b"test3-stderr".to_vec(),
                            stdout: b"test3-stdout".to_vec(),
                        }),
                        output_after: None,
                    },
                )
                .unwrap();

            // Force a progress tick before the next output write
            for _ in 0..10 {
                reporter.tick();
            }

            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Error {
                                exception: None,
                                backtrace: None,
                            },
                            display_name: "abq/test4".to_string(),
                            output: Some("Process 28821 terminated early via SIGTERM".to_string()),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();
            reporter
                .push_result(
                    INIT_RUN_NUMBER,
                    &ReportedResult::no_captures(TestResult::new(
                        RunnerMeta::fake(),
                        TestResultSpec {
                            status: Status::Pending,
                            display_name: "abq/test5".to_string(),
                            output: Some(
                                r#"Pending for reason: "implementation blocked on #1729""#
                                    .to_string(),
                            ),
                            ..default_result()
                        },
                    )),
                )
                .unwrap();

            // Force a final tick
            for _ in 0..10 {
                reporter.tick();
            }

            reporter.after_all_results();
            reporter.finish(&mock_summary()).unwrap();
        });

        let output = String::from_utf8(buffer).expect("output should be formatted as utf8");
        insta::assert_snapshot!(output, @r###"
        --- [abq progress] 0 seconds ---
        0 tests run, 0 passed, 0 failing

        --- abq/test2: FAILED ---
        Assertion failed: 1 != 2
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])

        --- [worker 0] BEFORE abq/test3 ---
        ----- STDOUT
        test3-stdout
        ----- STDERR
        test3-stderr


        --- [abq progress] 0 seconds ---
        3 tests run, 2 passed, 1 failing

        --- abq/test4: ERRORED ---
        Process 28821 terminated early via SIGTERM
        ----- STDOUT
        my stderr
        ----- STDERR
        my stdout
        (completed in 1 m, 15 s, 3 ms [worker 0])

        --- [abq progress] 0 seconds ---
        5 tests run, 3 passed, 2 failing
        "###);
    }
}

#[cfg(test)]
mod suite {
    use std::time::Duration;

    use abq_utils::net_protocol::runners::TestRuntime;

    use crate::reporting::ExitCode;

    use super::{MockWriter, SuiteResult};

    fn get_short_summary_lines(summary: SuiteResult) -> String {
        let mut mock_writer = MockWriter::default();
        summary
            .write_short_summary_lines(&mut &mut mock_writer)
            .unwrap();
        String::from_utf8(mock_writer.buffer).unwrap()
    }

    #[test]
    fn summary_when_no_tests_fail() {
        let summary = SuiteResult {
            suggested_exit_code: ExitCode::new(0),
            count: 10,
            count_failed: 0,
            tests_retried: 0,
            wall_time: Duration::from_secs(78),
            test_time: TestRuntime::Milliseconds(70200.),
        };
        insta::assert_snapshot!(get_short_summary_lines(summary), @r###"
        Finished in 78.00 seconds (70.20 seconds spent in test code)
        10 tests, 0 failures
        "###);
    }

    #[test]
    fn summary_when_some_tests_fail() {
        let summary = SuiteResult {
            suggested_exit_code: ExitCode::new(0),
            count: 10,
            count_failed: 5,
            tests_retried: 0,
            wall_time: Duration::from_secs(78),
            test_time: TestRuntime::Milliseconds(70200.),
        };
        insta::assert_snapshot!(get_short_summary_lines(summary), @r###"
        Finished in 78.00 seconds (70.20 seconds spent in test code)
        10 tests, 5 failures
        "###);
    }

    #[test]
    fn summary_with_retries() {
        let summary = SuiteResult {
            suggested_exit_code: ExitCode::new(0),
            count: 10,
            count_failed: 5,
            tests_retried: 3,
            wall_time: Duration::from_secs(78),
            test_time: TestRuntime::Milliseconds(70200.),
        };
        insta::assert_snapshot!(get_short_summary_lines(summary), @r###"
        Finished in 78.00 seconds (70.20 seconds spent in test code)
        10 tests, 5 failures, 3 retried
        "###);
    }
}
