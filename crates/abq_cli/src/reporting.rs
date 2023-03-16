use std::collections::HashMap;
use std::{fmt::Display, io, path::PathBuf, str::FromStr, time::Duration};

use abq_dot_reporter::DotReporter;
use abq_line_reporter::LineReporter;
use abq_progress_reporter::ProgressReporter;
use abq_reporting::{colors::ColorProvider, CompletedSummary, ReportedResult, ReportingError};
use abq_reporting::{
    output::{format_short_suite_summary, ShortSummary},
    Reporter,
};
use abq_utils::{exit::ExitCode, net_protocol::runners::TestRuntime};
use indicatif::ProgressDrawTarget;
use termcolor::{ColorChoice, StandardStream};

pub mod summary;

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
    /// Silences stdout reporters, for use when redirected to parent stdio.
    Quiet,
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
            ReporterKind::Quiet => write!(f, "quiet"),
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
            "quiet" => Ok(Self::Quiet),
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

pub struct SuiteResult {
    /// Exit code suggested for the test suite.
    pub suggested_exit_code: ExitCode,
    /// Total number of tests run.
    pub count: u64,
    pub count_failed: u64,
    /// How many individual tests were retried. Not the total count of how many retries were run.
    /// For example, if test A was retried twice, its count in this metric is one.
    pub tests_retried: u64,
    /// Runtime of the test suite, as accounted between the time the reporter started and the time
    /// it finished.
    pub wall_time: Duration,
    /// Runtime of the test suite, as accounted for in the actual time in tests.
    pub test_time: TestRuntime,
    /// File paths of test failures, one entry per failure.
    pub failed_file_paths: Vec<String>,
}

impl SuiteResult {
    pub fn write_short_summary_lines(&self, w: &mut impl termcolor::WriteColor) -> io::Result<()> {
        let mut failures_per_file = HashMap::new();
        for file in &self.failed_file_paths {
            *failures_per_file.entry(file.clone()).or_insert(0) += 1;
        }

        format_short_suite_summary(
            w,
            ShortSummary {
                wall_time: self.wall_time,
                test_time: self.test_time.duration(),
                num_tests: self.count,
                num_failing: self.count_failed,
                num_retried: self.tests_retried,
                failures_per_file,
            },
        )
    }

    pub fn suggested_exit_code(&self) -> ExitCode {
        self.suggested_exit_code
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

        let opt_specification = summary
            .native_runner_info
            .as_ref()
            .map(|runner| &runner.specification);

        self.collector
            .write_json(fd, opt_specification)
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
) -> Option<Box<dyn Reporter>> {
    let stdout = stdout_preferences.stdout_stream();

    match kind {
        ReporterKind::Line => Some(Box::new(LineReporter::new(Box::new(stdout)))),
        ReporterKind::Dot => Some(Box::new(DotReporter::new(Box::new(stdout)))),
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

            Some(Box::new(ProgressReporter::new(
                Box::new(stdout),
                color_provider,
                opt_target,
            )))
        }
        ReporterKind::JUnitXml(path) => Some(Box::new(JUnitXmlReporter {
            path,
            collector: abq_junit_xml::Collector::new(test_suite_name),
        })),
        ReporterKind::RwxV1Json(path) => Some(Box::new(RwxV1JsonReporter {
            path,
            collector: abq_rwx_v1_json::Collector::default(),
        })),
        ReporterKind::Quiet => None,
    }
}

pub fn build_reporters(
    reporter_kinds: Vec<ReporterKind>,
    stdout_preferences: StdoutPreferences,
    test_suite_name: &str,
) -> Vec<Box<dyn Reporter>> {
    reporter_kinds
        .into_iter()
        .filter_map(|kind| reporter_from_kind(kind, stdout_preferences, test_suite_name))
        .collect()
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
    fn parse_quiet_reporter() {
        assert_eq!(ReporterKind::from_str("quiet"), Ok(ReporterKind::Quiet));
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
mod suite {
    use std::time::Duration;

    use abq_reporting_test_utils::MockWriter;
    use abq_utils::net_protocol::runners::TestRuntime;

    use crate::reporting::ExitCode;

    use super::SuiteResult;

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
            failed_file_paths: vec![],
        };
        insta::assert_snapshot!(get_short_summary_lines(summary), @r###"
        --------------------------------------------------------------------------------
        ------------------------------------- ABQ --------------------------------------
        --------------------------------------------------------------------------------

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
            failed_file_paths: vec![],
        };
        insta::assert_snapshot!(get_short_summary_lines(summary), @r###"
        --------------------------------------------------------------------------------
        ------------------------------------- ABQ --------------------------------------
        --------------------------------------------------------------------------------

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
            failed_file_paths: vec![],
        };
        insta::assert_snapshot!(get_short_summary_lines(summary), @r###"
        --------------------------------------------------------------------------------
        ------------------------------------- ABQ --------------------------------------
        --------------------------------------------------------------------------------

        Finished in 78.00 seconds (70.20 seconds spent in test code)
        10 tests, 5 failures, 3 retried
        "###);
    }
}
