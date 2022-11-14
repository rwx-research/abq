//! Utilities for interacting with the queue config API

use core::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ApiKey(String);

#[derive(Debug, Error)]
pub enum ApiKeyError {
    #[error("invalid api key")]
    InvalidApiKey,
}

impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ApiKey {
    type Err = ApiKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_owned()))
    }
}
