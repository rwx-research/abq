use std::io;

use abq_reporting::CompletedSummary;
use abq_utils::net_protocol::runners::{Status, TestResultSpec, TestRuntime};

#[derive(Default)]
pub struct MockWriter {
    pub buffer: Vec<u8>,
    pub num_writes: u64,
    pub num_flushes: u64,
}

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

pub fn mock_summary() -> CompletedSummary {
    use abq_utils::net_protocol::{queue::NativeRunnerInfo, runners::NativeRunnerSpecification};

    CompletedSummary {
        native_runner_info: Some(NativeRunnerInfo {
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
        }),
        other_error_messages: vec![],
    }
}

#[allow(clippy::identity_op)]
pub fn default_result() -> TestResultSpec {
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
