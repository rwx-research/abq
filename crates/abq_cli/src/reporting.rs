use std::{fmt::Display, path::PathBuf, str::FromStr};

use abq_output::format_result;
use abq_utils::net_protocol::runners::TestResult;
use thiserror::Error;

static DEFAULT_JUNIT_XML_PATH: &str = "abq-test-results.xml";

#[derive(PartialEq, Eq, Debug)]
pub enum ReporterKind {
    /// Writes to stdout
    Stdout,
    /// Writes JUnit XML to a file
    JUnitXml(PathBuf),
}

impl Display for ReporterKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReporterKind::Stdout => write!(f, "stdout"),
            ReporterKind::JUnitXml(path) => write!(f, "junit-xml={}", path.display()),
        }
    }
}

impl FromStr for ReporterKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "stdout" => Ok(Self::Stdout),
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

#[derive(Debug, Error)]
pub enum ReportingError {
    #[error("failed to format a test result in the reporting format")]
    FailedToFormat,
    #[error("failed to write a report to an output buffer")]
    FailedToWrite,
}

/// A [`Reporter`] defines a way to emit abq test results.
///
/// A reporter is allowed to be side-effectful.
trait Reporter: Send {
    /// Consume the next test result.
    fn push_result(&mut self, test_result: &TestResult) -> Result<(), ReportingError>;

    /// Consume the reporter, and perform any needed finalization steps.
    ///
    /// This method is only called when all test results for a run have been consumed.
    fn finish(self: Box<Self>) -> Result<(), ReportingError>;
}

/// Streams all test results to standard output.
struct StdoutReporter;

impl Reporter for StdoutReporter {
    fn push_result(&mut self, test_result: &TestResult) -> Result<(), ReportingError> {
        println!("{}", format_result(test_result));
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<(), ReportingError> {
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

    fn finish(self: Box<Self>) -> Result<(), ReportingError> {
        let fd = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(self.path)
            .map_err(|_| ReportingError::FailedToWrite)?;
        self.collector
            .write_xml(fd)
            .map_err(|_| ReportingError::FailedToFormat)?;
        Ok(())
    }
}

fn reporter_from_kind(kind: ReporterKind, test_suite_name: &str) -> Box<dyn Reporter> {
    match kind {
        ReporterKind::Stdout => Box::new(StdoutReporter),
        ReporterKind::JUnitXml(path) => Box::new(JUnitXmlReporter {
            path,
            collector: abq_junit_xml::Collector::new(test_suite_name),
        }),
    }
}

pub struct Reporters(Vec<Box<dyn Reporter>>);

impl Reporters {
    pub fn new(kinds: impl IntoIterator<Item = ReporterKind>, test_suite_name: &str) -> Self {
        Self(
            kinds
                .into_iter()
                .map(|kind| reporter_from_kind(kind, test_suite_name))
                .collect(),
        )
    }

    pub fn push_result(&mut self, test_result: &TestResult) -> Result<(), Vec<ReportingError>> {
        let errors: Vec<_> = self
            .0
            .iter_mut()
            .filter_map(|reporter| reporter.push_result(test_result).err())
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn finish(self) -> Result<(), Vec<ReportingError>> {
        let errors: Vec<_> = self
            .0
            .into_iter()
            .filter_map(|reporter| reporter.finish().err())
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
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
    fn parse_stdout_reporter() {
        assert_eq!(ReporterKind::from_str("stdout"), Ok(ReporterKind::Stdout));
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
