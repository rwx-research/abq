use std::{io, time::Instant};

use abq_reporting::{
    colors::ColorProvider,
    output::{
        self, format_interactive_progress, format_non_interactive_progress, format_result_line,
    },
    CompletedSummary, ReportedResult, Reporter, ReportingError,
};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Streams a progress bar of number of executed tests and any failures so far.
/// Writes out failures and captured output as soon as it is received.
pub struct ProgressReporter {
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
    pub fn new(
        buffer: Box<dyn termcolor::WriteColor + Send>,
        color_provider: ColorProvider,
        // None if non-interactive
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
        _run_number: u32,
        result: &ReportedResult,
    ) -> Result<(), ReportingError> {
        let ReportedResult { test_result, .. } = result;

        self.num_results += 1;

        let mut write_result = || {
            let is_fail_like = test_result.status.is_fail_like();
            self.num_failing += is_fail_like as u64;

            if is_fail_like {
                if self.wrote_first_output {
                    output::write(&mut self.buffer, &[b'\n'])?;
                }
                format_result_line(&mut self.buffer, test_result)?;
                self.wrote_first_output = true;
            }

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

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use abq_reporting::{colors::ColorProvider, ReportedResult};
    use abq_reporting_test_utils::{default_result, mock_summary, MockWriter};
    use abq_utils::net_protocol::{
        entity::RunnerMeta,
        runners::{Status, StdioOutput, TestResult, TestResultSpec},
        workers::INIT_RUN_NUMBER,
    };
    use indicatif::{ProgressDrawTarget, TermLike};

    use super::{ProgressReporter, Reporter};

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
        abq/test2: FAILED

        abq/test4: ERRORED
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
                        output_before: Some(StdioOutput {
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

        abq/test2: FAILED

        --- [abq progress] 0 seconds ---
        3 tests run, 2 passed, 1 failing

        abq/test4: ERRORED

        --- [abq progress] 0 seconds ---
        5 tests run, 3 passed, 2 failing
        "###);
    }
}
