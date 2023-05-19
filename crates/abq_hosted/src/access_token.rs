//! Utilities for interacting with the queue config API

use core::fmt;
use std::str::FromStr;
use serde_derive::{Serialize, Deserialize};
use thiserror::Error;

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct AccessToken(String);

#[derive(Debug, Error)]
pub enum AccessTokenError {
    #[error("invalid access token")]
    InvalidAccessToken,
}

impl fmt::Display for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for AccessToken {
    type Err = AccessTokenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_owned()))
    }
}
