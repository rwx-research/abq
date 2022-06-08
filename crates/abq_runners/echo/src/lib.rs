use abq_runner_protocol::{Output, Runner};

pub struct EchoWork {
    pub message: String,
}

pub struct EchoWorker {}

impl Runner for EchoWorker {
    type Input = EchoWork;

    fn run(input: EchoWork) -> Output {
        Output {
            success: true,
            message: input.message,
        }
    }
}
