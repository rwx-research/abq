use std::error::Error;

/// Opaque thread-safe error.
/// Only use this when the error is unrecoverable and intended to reach a sink,
/// like the tracing logs.
pub(crate) type AnyError = Box<dyn Error + Send + Sync>;
#[cfg(test)] // TODO #281
pub(crate) type OpaqueResult<T> = Result<T, AnyError>;
