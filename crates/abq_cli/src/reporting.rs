use std::{fmt::Display, path::PathBuf, str::FromStr};

use abq_output::format_result;
use abq_utils::net_protocol::runners::TestResult;

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

enum Reporter {
    Stdout,
    JUnitXml(PathBuf, abq_junit_xml::Collector),
}

impl Reporter {
    fn new(kind: ReporterKind, test_suite_name: &str) -> Self {
        match kind {
            ReporterKind::Stdout => Self::Stdout,
            ReporterKind::JUnitXml(path) => {
                Self::JUnitXml(path, abq_junit_xml::Collector::new(test_suite_name))
            }
        }
    }

    fn push_result(&mut self, test_result: &TestResult) {
        match self {
            Self::Stdout => println!("{}", format_result(test_result)),
            Self::JUnitXml(_, collector) => collector.push_result(test_result),
        }
    }

    fn finish(self) -> anyhow::Result<()> {
        match self {
            Reporter::Stdout => Ok(()),
            Reporter::JUnitXml(path, collector) => {
                let fd = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(path)?;
                collector.write_xml(fd)?;
                Ok(())
            }
        }
    }
}

pub struct Reporters(Vec<Reporter>);

impl Reporters {
    pub fn new(kinds: impl IntoIterator<Item = ReporterKind>, test_suite_name: &str) -> Self {
        Self(
            kinds
                .into_iter()
                .map(|kind| Reporter::new(kind, test_suite_name))
                .collect(),
        )
    }

    pub fn push_result(&mut self, test_result: &TestResult) {
        self.0
            .iter_mut()
            .for_each(|reporter| reporter.push_result(test_result));
    }

    pub fn finish(self) -> anyhow::Result<()> {
        self.0
            .into_iter()
            .map(|reporter| reporter.finish())
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(())
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
