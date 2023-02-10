use crate::net_protocol::entity::Entity;
use anyhow::{anyhow, Error, Result};

/// Opaque thread-safe error.
/// Only use this when the error is unrecoverable and intended to reach a sink,
/// like the tracing logs in a server.
pub type AnyError = Error;

#[derive(Debug)]
pub struct Location {
    pub file: &'static str,
    pub line: u32,
    pub column: u32,
}

/// An error with a location attached.
// TODO: remove this when backtraces in errors are stabilized
//   https://github.com/rust-lang/rust/issues/53487
#[derive(Debug)]
pub struct LocatedError {
    pub error: AnyError,
    pub location: &'static Location,
}

impl std::error::Error for LocatedError {}

impl std::fmt::Display for LocatedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            error,
            location: Location { file, line, column },
        } = self;
        write!(f, "{error} at {file}@{line}:{column}")
    }
}

pub type OpaqueResult<T> = Result<T, LocatedError>;

pub trait ResultLocation<T> {
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

pub trait ErrorLocation {
    fn located(self, location: &'static Location) -> LocatedError;
}

impl<E: Into<Box<dyn std::error::Error + Send + Sync>>> ErrorLocation for E {
    #[inline]
    fn located(self, location: &'static Location) -> LocatedError {
        let boxed: Box<dyn std::error::Error + Send + Sync> = self.into();
        let error: anyhow::Error = anyhow!(boxed);
        LocatedError { error, location }
    }
}

/// An error with a source and entity associated.
#[derive(Debug)]
pub struct EntityfulError {
    pub error: LocatedError,
    pub entity: Option<Entity>,
}

pub trait ErrorEntity<T> {
    fn no_entity(self) -> Result<T, EntityfulError>;
    fn entity(self, entity: Entity) -> Result<T, EntityfulError>;
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

    fn entity(self, entity: Entity) -> Result<T, EntityfulError> {
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
        &$crate::error::Location {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}

pub use crate::log_entityful_error;

#[macro_export]
macro_rules! log_entityful_error {
    ($err:expr, $($field:tt)*) => {{
        let $crate::error::EntityfulError {
            error:
                $crate::error::LocatedError {
                    error,
                    location: $crate::error::Location { file, line, column },
                },
            entity,
        } = $err;
        match entity {
            Some(entity) => {
                tracing::error!(
                    entity_id=%entity.display_id(),
                    entity_tag=%entity.tag,
                    file,
                    line,
                    column,
                    $($field)*,
                    error
                );
            }
            None => {
                tracing::error!(
                    entity_id="<unknown>",
                    entity_tag="<unknown>",
                    file,
                    line,
                    column,
                    $($field)*,
                    error
                );
            }
        }
    }};
}
