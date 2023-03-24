use std::borrow::Cow;

use abq_reporting::{
    output::{format_result_dot, write, OutputOrdering, SummaryKind},
    CompletedSummary, ReportedResult, Reporter, ReportingError,
};
use abq_utils::net_protocol::runners::{Status, StdioOutput, TestResult};

/// Max number of dots to print per line.
const DOT_REPORTER_LINE_LIMIT: u64 = 80;

/// Streams test results as dots to an output buffer, and prints a summary for failing and erroring
/// tests at the end.
pub struct DotReporter {
    buffer: Box<dyn termcolor::WriteColor + Send>,

    num_results: u64,

    delayed_summaries: Vec<SummaryKind<'static>>,
}

impl DotReporter {
    pub fn new(buffer: Box<dyn termcolor::WriteColor + Send>) -> Self {
        Self {
            buffer,
            num_results: 0,
            delayed_summaries: Default::default(),
        }
    }

    fn maybe_push_delayed_output(
        &mut self,
        test_result: &TestResult,
        opt_output: &Option<StdioOutput>,
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
            let output = test_result.output.as_deref().unwrap_or("<no output>");
            writeln!(&mut self.buffer, "{output}")?;
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
    }

    fn finish(mut self: Box<Self>, _summary: &CompletedSummary) -> Result<(), ReportingError> {
        self.buffer
            .flush()
            .map_err(|_| ReportingError::FailedToWrite)?;

        Ok(())
    }
}

#[cfg(test)]
mod test_dot_reporter {
    use abq_reporting::ReportedResult;
    use abq_utils::net_protocol::{
        entity::RunnerMeta,
        runners::{Status, StdioOutput, TestResult, TestResultSpec},
        workers::INIT_RUN_NUMBER,
    };

    use abq_reporting_test_utils::{default_result, mock_summary, MockWriter};

    use super::{DotReporter, Reporter, DOT_REPORTER_LINE_LIMIT};

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
                        output_before: Some(StdioOutput {
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
        "###);
    }
}
