use std::time::{Duration, SystemTime};

use abq_utils::{
    error::{OpaqueResult, ResultLocation},
    here,
    net_protocol::workers::RunId,
};

#[derive(Clone, Copy, Debug)]
enum OffloadConfigInner {
    Never,
    After(Duration),
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct OffloadConfig(OffloadConfigInner);

impl OffloadConfig {
    pub const NEVER: Self = Self(OffloadConfigInner::Never);

    pub fn new(offload_after: Duration) -> Self {
        Self(OffloadConfigInner::After(offload_after))
    }

    fn should_offload(&self, elapsed_duration: Duration) -> bool {
        match self.0 {
            OffloadConfigInner::Never => false,
            OffloadConfigInner::After(offload_after) => elapsed_duration >= offload_after,
        }
    }

    pub fn file_eligible_for_offload(
        &self,
        time_now: &SystemTime,
        file_metadata: &std::fs::Metadata,
    ) -> OpaqueResult<bool> {
        let size = file_metadata.len();
        let should_offload_time = || {
            let accessed_time = file_metadata.accessed().located(here!())?;
            if accessed_time > *time_now {
                return Ok(false);
            }

            let elapsed = time_now.duration_since(accessed_time).located(here!())?;
            let should_offload = self.should_offload(elapsed);
            Ok(should_offload)
        };
        Ok(size > 0 && should_offload_time()?)
    }
}

impl std::fmt::Debug for OffloadConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[must_use]
pub struct OffloadSummary {
    pub offloaded_run_ids: Vec<RunId>,
}
