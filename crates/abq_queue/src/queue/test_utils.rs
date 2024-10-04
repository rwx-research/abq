use abq_test_utils::one_nonzero_usize;
use abq_utils::{
    net_protocol::{
        entity::Entity,
        queue::{TestSpec, TestStrategy},
        runners::ProtocolWitness,
        workers::RunId,
    },
    test_command_hash::TestCommandHash,
};

use crate::persistence::remote::RemotePersister;

use super::RunParams;

pub fn fake_test_spec(proto: ProtocolWitness) -> TestSpec {
    use abq_utils::net_protocol::{runners::TestCase, workers::WorkId};

    TestSpec {
        test_case: TestCase::new(proto, "fake-test", Default::default()),
        work_id: WorkId::new(),
    }
}

pub struct RunParamsBuilder<'a>(RunParams<'a>);
impl<'a> RunParamsBuilder<'a> {
    pub fn new(run_id: &'a RunId, remote: &'a RemotePersister) -> Self {
        Self(RunParams {
            run_id,
            batch_size_hint: one_nonzero_usize(),
            test_strategy: TestStrategy::ByTest,
            entity: Entity::runner(0, 1),
            remote,
            runner_test_command_hash: TestCommandHash::random(),
        })
    }

    pub fn entity(mut self, entity: Entity) -> Self {
        self.0.entity = entity;
        self
    }

    pub fn runner_test_command_hash(mut self, hash: TestCommandHash) -> Self {
        self.0.runner_test_command_hash = hash;
        self
    }

    pub(super) fn build(self) -> RunParams<'a> {
        self.0
    }
}
