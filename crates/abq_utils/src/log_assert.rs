/// Perform a runtime assertion, but if it fails, log an error rather than
/// issuing a runtime panic.
#[macro_export]
macro_rules! log_assert {
    ($check:expr, $($field:tt)*) => {{
        if !($check) {
            tracing::error!($($field)*)
        }
    }}
}
