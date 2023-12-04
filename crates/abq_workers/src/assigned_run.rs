use abq_utils::net_protocol::{entity::Entity, queue::InvokeWork};
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};

/// The test run a worker should ask for work on.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum AssignedRunKind {
    /// This worker is connecting for a fresh run, and should fetch tests online.
    Fresh { should_generate_manifest: bool },
    /// This worker is connecting for a retry, and should fetch its manifest from the queue once.
    Retry,
    /// This worker should pull the retry manifest, and then continue to fetch tests online.
    RetryAndContinue,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AssignedRun {
    pub kind: AssignedRunKind,
    /// [true] if the runner test command differs from the one associated when the run was
    /// created.
    pub runner_test_command_differs: bool,
}

#[must_use]
#[derive(Debug, PartialEq, Eq)]
pub enum AssignedRunStatus {
    RunUnknown,
    Run(AssignedRun),
    AlreadyDone {
        exit_code: abq_utils::exit::ExitCode,
    },
    FatalError(String),
}

impl AssignedRunStatus {
    /// Returns true iff this is the first time the run was ever introduced.
    pub fn freshly_created(&self) -> bool {
        matches!(
            self,
            AssignedRunStatus::Run(AssignedRun {
                kind: AssignedRunKind::Fresh {
                    should_generate_manifest: true,
                },
                ..
            })
        )
    }
}

#[async_trait]
pub trait GetAssignedRun {
    async fn get_assigned_run(&self, entity: Entity, invoke_work: &InvokeWork)
        -> AssignedRunStatus;
}

#[cfg(test)]
pub(crate) mod fake {
    use abq_utils::net_protocol::{entity::Entity, queue::InvokeWork};
    use async_trait::async_trait;

    use crate::GetAssignedRun;

    use super::AssignedRunStatus;

    pub struct MockGetAssignedRun<F> {
        status: F,
    }

    impl<F> MockGetAssignedRun<F> {
        pub fn new(status: F) -> Self {
            Self { status }
        }
    }

    #[async_trait]
    impl<F> GetAssignedRun for MockGetAssignedRun<F>
    where
        F: Fn() -> AssignedRunStatus + Send + Sync,
    {
        async fn get_assigned_run(
            &self,
            _entity: Entity,
            _invoke_work: &InvokeWork,
        ) -> AssignedRunStatus {
            (self.status)()
        }
    }
}
