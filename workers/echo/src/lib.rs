use abq_worker_protocol::{Output, Worker};

pub struct EchoWork {
    pub message: String,
}

pub struct EchoWorker {}

impl Worker for EchoWorker {
    type Input = EchoWork;

    fn run(input: EchoWork) -> Output {
        Output {
            output: input.message,
        }
    }
}
