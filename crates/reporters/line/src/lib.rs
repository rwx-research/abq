use abq_reporting::{
    output::{format_result_line, format_summary_results, format_test_result_summary, SummaryKind},
    CompletedSummary, Reporter, ReportingError,
};
use abq_utils::net_protocol::{client::ReportedResult, runners::Status};

/// Streams all test results line-by-line to an output buffer, and prints a summary for failing and
/// erroring tests at the end.
pub struct LineReporter {
    /// The output buffer.
    buffer: Box<dyn termcolor::WriteColor + Send>,

    /// Failures and errors for which a longer summary should be printed at the end.
    delayed_summaries: Vec<SummaryKind<'static>>,

    seen_first: bool,
}

impl LineReporter {
    pub fn new(buffer: Box<dyn termcolor::WriteColor + Send>) -> Self {
        Self {
            buffer,
            delayed_summaries: Default::default(),
            seen_first: false,
        }
    }
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
        let _ = format_summary_results(
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

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::{
        client::ReportedResult,
        entity::RunnerMeta,
        runners::{CapturedOutput, Status, TestResult, TestResultSpec},
        workers::INIT_RUN_NUMBER,
    };

    use abq_reporting_test_utils::{default_result, mock_summary, MockWriter};

    use super::{LineReporter, Reporter};

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
