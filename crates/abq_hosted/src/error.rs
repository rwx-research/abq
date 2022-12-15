use reqwest::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid url {0}")]
    InvalidUrl(String),
    #[error("could not authenticate against the hosted API")]
    Unauthenticated,
    #[error("could not authorize against the hosted API")]
    Unauthorized,
    #[error("hosted API returned an unexpected schema: {0}")]
    SchemaError(String),
    #[error("hosted API returned an unexpected error: {0}")]
    Other(String),
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        match e.status() {
            Some(code) => {
                if code == StatusCode::UNAUTHORIZED {
                    Self::Unauthenticated
                } else if code == StatusCode::FORBIDDEN {
                    Self::Unauthorized
                } else {
                    Self::Other(e.to_string())
                }
            }
            None => Self::Other(e.to_string()),
        }
    }
}
