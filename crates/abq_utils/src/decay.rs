use std::time::Duration;

/// Implements an iterator that forever yields exponential decay times.
#[derive(Clone, Copy)]
pub struct ExpDecay {
    duration: Duration,
    multiplier: u32,
    max: Duration,
}

impl ExpDecay {
    pub const fn constant(duration: Duration) -> Self {
        Self {
            duration,
            multiplier: 1,
            max: duration,
        }
    }

    /// Creates a exponential decay at a quadratic rate.
    pub const fn quadratic(starting: Duration, max: Duration) -> Self {
        Self {
            duration: starting,
            multiplier: 2,
            max,
        }
    }

    pub fn next_duration(&mut self) -> Duration {
        let duration = self.duration;
        self.duration = std::cmp::min(self.max, self.duration * self.multiplier);
        duration
    }
}

impl Iterator for ExpDecay {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next_duration())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::ExpDecay;

    #[test]
    fn constant() {
        let mut i = 0;
        for duration in ExpDecay::constant(Duration::from_secs(1)).take(100) {
            let secs = duration.as_secs();
            let expected_seconds = 1;

            assert_eq!(secs, expected_seconds);
            i += 1;
        }
        assert_eq!(i, 100);
    }

    #[test]
    fn quadratic() {
        let mut i = 0;
        for duration in
            ExpDecay::quadratic(Duration::from_secs(1), Duration::from_secs(40)).take(100)
        {
            let secs = duration.as_secs();
            let expected_seconds = match i {
                0 => 1,
                1 => 2,
                2 => 4,
                3 => 8,
                4 => 16,
                5 => 32,
                n => {
                    assert!(n > 5);
                    40
                }
            };

            assert_eq!(secs, expected_seconds);
            i += 1;
        }
        assert_eq!(i, 100);
    }
}
