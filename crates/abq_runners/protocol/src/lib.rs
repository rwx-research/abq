use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Output {
    pub output: String,
}

pub trait Runner {
    type Input;

    fn run(input: Self::Input) -> Output;
}
