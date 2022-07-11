use abq_runner_protocol::Runner;

pub struct EchoWork {
    pub message: String,
}

pub struct EchoWorker {}

impl Runner for EchoWorker {
    type Input = EchoWork;

    fn run(input: EchoWork) -> String {
        input.message
    }
}
