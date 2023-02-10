use abq_utils::net_protocol::entity::EntityId;
use anyhow::{anyhow, Error, Result};

/// Opaque thread-safe error.
/// Only use this when the error is unrecoverable and intended to reach a sink,
/// like the tracing logs in a server.
pub type AnyError = Error;

#[derive(Debug)]
pub(crate) struct Location {
    pub file: &'static str,
    pub line: u32,
    pub column: u32,
}

/// An error with a location attached.
// TODO: remove this when backtraces in errors are stabilized
//   https://github.com/rust-lang/rust/issues/53487
#[derive(Debug)]
pub(crate) struct LocatedError {
    pub error: AnyError,
    pub location: &'static Location,
}

pub(crate) type OpaqueResult<T> = Result<T, LocatedError>;

pub(crate) trait ResultLocation<T> {
    fn located(self, location: &'static Location) -> OpaqueResult<T>;
}

impl<T, E: Into<Box<dyn std::error::Error + Send + Sync>>> ResultLocation<T> for Result<T, E> {
    #[inline]
    fn located(self, location: &'static Location) -> OpaqueResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(error) => {
                let boxed: Box<dyn std::error::Error + Send + Sync> = error.into();
                let error: anyhow::Error = anyhow!(boxed);
                Err(LocatedError { error, location })
            }
        }
    }
}

/// An error with a source and entity associated.
#[derive(Debug)]
pub(crate) struct EntityfulError {
    pub error: LocatedError,
    pub entity: Option<EntityId>,
}

pub(crate) trait ErrorEntity<T> {
    fn no_entity(self) -> Result<T, EntityfulError>;
    fn entity(self, entity: EntityId) -> Result<T, EntityfulError>;
}

impl<T> ErrorEntity<T> for OpaqueResult<T> {
    #[inline]
    fn no_entity(self) -> Result<T, EntityfulError> {
        match self {
            Ok(v) => Ok(v),
            Err(error) => Err(EntityfulError {
                error,
                entity: None,
            }),
        }
    }

    fn entity(self, entity: EntityId) -> Result<T, EntityfulError> {
        match self {
            Ok(v) => Ok(v),
            Err(error) => Err(EntityfulError {
                error,
                entity: Some(entity),
            }),
        }
    }
}

pub use crate::here;

#[macro_export]
macro_rules! here {
    () => {
        &Location {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}
