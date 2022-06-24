use abq_runner_protocol::Runner;
use abq_utils::net_protocol::runners::Output;

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
