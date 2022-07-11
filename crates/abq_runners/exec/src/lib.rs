use std::process;

use abq_runner_protocol::Runner;

pub struct Work {
    pub cmd: String,
    pub args: Vec<String>,
}

pub struct ExecWorker {}

impl Runner for ExecWorker {
    type Input = Work;

    fn run(input: Work) -> String {
        let output = process::Command::new(input.cmd)
            .args(input.args)
            .output()
            // TODO: handle failure
            .unwrap();

        let success = output.status.success();
        let message = if success {
            output.stdout
        } else {
            output.stderr
        };
        // TODO: handle not utf8 encodable
        String::from_utf8(message).unwrap()
    }
}
