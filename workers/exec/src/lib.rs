use std::path::PathBuf;
use std::process::{self, Stdio};

use abq_worker_protocol::{Output, Worker};

pub struct Work {
    pub cmd: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf,
}

pub struct ExecWorker {}

impl Worker for ExecWorker {
    type Input = Work;

    fn run(input: Work) -> Output {
        let status = process::Command::new(input.cmd)
            .args(input.args)
            .current_dir(input.working_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null())
            .status()
            .unwrap();

        let msg = if status.success() { "OK" } else { "FAIL" };

        Output {
            output: msg.to_string(),
        }
    }
}
