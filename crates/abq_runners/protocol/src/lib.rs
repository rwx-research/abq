pub trait Runner {
    type Input;

    fn run(input: Self::Input) -> String;
}
