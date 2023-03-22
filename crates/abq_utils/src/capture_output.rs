use async_trait::async_trait;
use std::io;
use tokio::process::{ChildStderr, ChildStdout};

pub use crate::net_protocol::runners::{CapturedOutput, ProcessOutput, StdioOutput};

mod capture_pipes;
mod muxer;

pub trait ChildOutputStrategy: Sync + Send {
    fn create(
        &self,
        child_stdout: ChildStdout,
        child_stderr: ChildStderr,
        is_retry: bool,
    ) -> Box<dyn HandleChildOutput>;
}

pub type ChildOutputStrategyBox = Box<dyn ChildOutputStrategy>;

#[async_trait]
pub trait HandleChildOutput: Send {
    fn get_captured(&self) -> StdioOutput;

    /// Like get_captured, but does not slice off the captured buffer.
    fn get_captured_ref(&self) -> StdioOutput;

    fn close(&mut self);

    async fn finish(&mut self) -> io::Result<CapturedOutput>;
}

pub type ChildOutputHandler = Box<dyn HandleChildOutput>;

pub use capture_pipes::CaptureChildOutputStrategy;
