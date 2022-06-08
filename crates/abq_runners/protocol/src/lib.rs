use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Output {
    pub success: bool,
    pub message: String,
}

pub trait Runner {
    type Input;

    fn run(input: Self::Input) -> Output;
}
