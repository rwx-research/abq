use serde_derive::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct EpochMillis(u128);

impl EpochMillis {
    pub fn now() -> Self {
        Self(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
    }

    pub fn as_millis(self) -> u128 {
        self.0
    }

    pub fn from_millis(millis: u128) -> Self {
        Self(millis)
    }
}
