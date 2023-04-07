use std::time::Duration;

#[derive(Clone, Copy)]
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

    pub fn should_offload(&self, elapsed_duration: Duration) -> bool {
        match self.0 {
            OffloadConfigInner::Never => false,
            OffloadConfigInner::After(offload_after) => elapsed_duration >= offload_after,
        }
    }
}
