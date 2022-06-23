use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum NextTest {
    Id(TestId),
    EndOfTests,
}

#[derive(Serialize, Deserialize)]
pub struct TestResult {
    test_id: TestId,
    success: bool,
    message: String,
}

pub type TestId = String;

#[derive(Serialize, Deserialize)]
pub struct TestManifest {
    pub test_ids: Vec<TestId>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Output {
    pub success: bool,
    pub message: String,
}

pub trait Runner {
    type Input;

    fn run(input: Self::Input) -> Output;
}
