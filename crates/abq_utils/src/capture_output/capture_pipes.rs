use async_trait::async_trait;
use tokio::io::{self, Stderr, Stdout};
use tokio::process::{ChildStderr, ChildStdout};

use super::muxer::{mux_output, MuxOutput, MuxOutputConfig, SideChannel};
use super::{CapturedOutput, ChildOutputStrategy, HandleChildOutput, StdioOutput};

pub struct CapturePipes {
    stdout: MuxOutput<ChildStdout, Stdout>,
    stderr: MuxOutput<ChildStderr, Stderr>,
    combined_channel: SideChannel,
}

impl CapturePipes {
    pub fn new(
        child_stdout: ChildStdout,
        child_stderr: ChildStderr,
        should_pipe_to_parent_output: bool,
        is_retry: bool,
    ) -> Self {
        let (parent_stdout, parent_stderr) =
            maybe_create_parent_pipes(should_pipe_to_parent_output);

        let stdout_channel = SideChannel::default();
        let stderr_channel = SideChannel::default();
        let combined_channel = SideChannel::default();

        let mut mux_stdout_config =
            MuxOutputConfig::new(parent_stdout, stdout_channel, combined_channel.clone());
        if is_retry {
            mux_stdout_config = mux_stdout_config.with_prefix(RETRY_HEADER);
        }

        let mux_stderr_config =
            MuxOutputConfig::new(parent_stderr, stderr_channel, combined_channel.clone());

        Self {
            stdout: mux_output(child_stdout, mux_stdout_config),
            stderr: mux_output(child_stderr, mux_stderr_config),
            combined_channel,
        }
    }
}

fn maybe_create_parent_pipes(
    should_pipe_to_parent_output: bool,
) -> (Option<Stdout>, Option<Stderr>) {
    if should_pipe_to_parent_output {
        (Some(tokio::io::stdout()), Some(tokio::io::stderr()))
    } else {
        (None, None)
    }
}

const RETRY_HEADER: &str = "\
\
--------------------------------------------------------------------------------
---------------------------------- ABQ RETRY -----------------------------------
--------------------------------------------------------------------------------

";

#[async_trait]
impl HandleChildOutput for CapturePipes {
    fn get_captured(&self) -> StdioOutput {
        // NB: if we ever measure this to be compute-expensive, consider making `end_capture` async
        let stdout = self.stdout.side_channel.get_captured();
        let stderr = self.stderr.side_channel.get_captured();

        StdioOutput { stderr, stdout }
    }

    fn get_captured_ref(&self) -> StdioOutput {
        let stdout = self.stdout.side_channel.get_captured_ref();
        let stderr = self.stderr.side_channel.get_captured_ref();

        StdioOutput { stderr, stdout }
    }

    fn close(&mut self) {
        // Close up the child pipes, replace them with the trivial task.
        self.stdout.copied_all_output.abort();
        self.stdout.copied_all_output = tokio::spawn(async { Ok(()) });

        self.stderr.copied_all_output.abort();
        self.stderr.copied_all_output = tokio::spawn(async { Ok(()) });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn finish(&mut self) -> io::Result<CapturedOutput> {
        let MuxOutput {
            copied_all_output: copied_stdout,
            side_channel: side_stdout,
            ..
        } = &mut self.stdout;
        let MuxOutput {
            copied_all_output: copied_stderr,
            side_channel: side_stderr,
            ..
        } = &mut self.stderr;
        copied_stdout.await.unwrap()?;
        copied_stderr.await.unwrap()?;
        let stdout = side_stdout
            .finish()
            .expect("channel reference must be unique at this point");
        let stderr = side_stderr
            .finish()
            .expect("channel reference must be unique at this point");
        let combined = self
            .combined_channel
            .finish()
            .expect("channel reference must be unique at this point");
        Ok(CapturedOutput {
            stderr,
            stdout,
            combined,
        })
    }
}

pub struct CaptureChildOutputStrategy {
    should_pipe_to_parent_output: bool,
}

impl CaptureChildOutputStrategy {
    pub fn new(should_pipe_to_parent_output: bool) -> Self {
        Self {
            should_pipe_to_parent_output,
        }
    }
}

impl ChildOutputStrategy for CaptureChildOutputStrategy {
    fn create(
        &self,
        child_stdout: ChildStdout,
        child_stderr: ChildStderr,
        is_retry: bool,
    ) -> Box<dyn HandleChildOutput> {
        Box::new(CapturePipes::new(
            child_stdout,
            child_stderr,
            self.should_pipe_to_parent_output,
            is_retry,
        ))
    }
}
