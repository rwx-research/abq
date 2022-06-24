use abq_utils::net_protocol::runners::Output;

pub trait Runner {
    type Input;

    fn run(input: Self::Input) -> Output;
}
