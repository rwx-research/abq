use abq_utils::{log_assert, net_protocol::workers::INIT_RUN_NUMBER};

/// Counts the number of attempts made for a test suite run.
#[derive(Debug)]
pub struct AttemptsCounter {
    last_completed_attempt: u32,
    max_attempt_number: u32,
}

impl AttemptsCounter {
    pub fn new(max_attempt_number: u32) -> Self {
        debug_assert!(max_attempt_number >= INIT_RUN_NUMBER);

        Self {
            last_completed_attempt: INIT_RUN_NUMBER - 1,
            max_attempt_number,
        }
    }

    /// Mark that the test suite run `completed_attempt` was completed.
    pub fn account_last_of_attempt(&mut self, completed_attempt: u32) {
        log_assert!(
            completed_attempt > self.last_completed_attempt,
            "test run attempts must increase monotonically"
        );
        log_assert!(
            completed_attempt <= self.max_attempt_number,
            "should not exceed accounting of the max attempt number"
        );
        self.last_completed_attempt = completed_attempt;
    }

    /// Checks whether all test suite runs, up the given maximum number of attempts, have been
    /// completed.
    pub fn completed_all_attempts(&self) -> bool {
        self.last_completed_attempt == self.max_attempt_number
    }

    pub fn last_completed_attempt(&self) -> u32 {
        self.last_completed_attempt
    }
}

#[cfg(test)]
mod test {
    use abq_utils::net_protocol::workers::INIT_RUN_NUMBER;

    use super::AttemptsCounter;

    #[test]
    fn account_then_completed() {
        let mut counter = AttemptsCounter::new(INIT_RUN_NUMBER + 1);
        counter.account_last_of_attempt(INIT_RUN_NUMBER);
        counter.account_last_of_attempt(INIT_RUN_NUMBER + 1);
        assert!(counter.completed_all_attempts());
    }

    #[test]
    fn account_then_completed_with_skips() {
        let mut counter = AttemptsCounter::new(INIT_RUN_NUMBER + 5);
        counter.account_last_of_attempt(INIT_RUN_NUMBER);
        counter.account_last_of_attempt(INIT_RUN_NUMBER + 5);
        assert!(counter.completed_all_attempts());
    }

    #[test]
    fn not_completed_upon_creation() {
        let counter = AttemptsCounter::new(INIT_RUN_NUMBER);
        assert!(!counter.completed_all_attempts());
    }

    #[test]
    fn account_then_not_completed() {
        let mut counter = AttemptsCounter::new(INIT_RUN_NUMBER + 1);
        counter.account_last_of_attempt(INIT_RUN_NUMBER);
        assert!(!counter.completed_all_attempts());
    }
}
