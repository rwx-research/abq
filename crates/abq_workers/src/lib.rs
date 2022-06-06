pub mod protocol;
pub mod workers;

/// Supports running worker pools in tests. This must be run before any tests involving workers are
/// initialized.
#[macro_export]
macro_rules! test_support {
    () => {
        procspawn::enable_test_support!();
    };
}

#[cfg(test)]
test_support!();
