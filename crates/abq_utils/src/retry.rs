use std::{future::Future, thread, time::Duration};

pub async fn async_retry_n<O, R, E, F>(
    max_attempts: usize,
    delay: Duration,
    operation: O,
) -> Result<R, E>
where
    F: Future<Output = Result<R, E>>,
    O: Fn(usize) -> F,
{
    let mut attempt = 0;

    loop {
        attempt += 1;

        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if attempt >= max_attempts {
                    return Err(e);
                } else {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

pub fn retry_n<O, R, E>(max_attempts: usize, delay: Duration, operation: O) -> Result<R, E>
where
    O: Fn(usize) -> Result<R, E>,
{
    let mut attempt = 0;

    loop {
        attempt += 1;

        match operation(attempt) {
            Ok(value) => return Ok(value),
            Err(e) => {
                if attempt >= max_attempts {
                    return Err(e);
                } else {
                    thread::sleep(delay);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::retry::{async_retry_n, retry_n};
    use std::time::Duration;

    #[test]
    fn retry_completes_successfully_first_time() {
        let value: Result<&str, &str> = retry_n(2, Duration::from_nanos(1), |attempt| {
            assert_eq!(attempt, 1);
            Ok("success")
        });

        assert_eq!(value, Ok("success"));
    }

    #[test]
    fn retry_completes_successfully_second_time() {
        let value: Result<&str, &str> =
            retry_n(2, Duration::from_nanos(1), |attempt| match attempt {
                1 => Err("forced retry"),
                2 => Ok("success"),
                _ => panic!("too many retry attempts"),
            });

        assert_eq!(value, Ok("success"));
    }

    #[test]
    fn retry_errors_after_retries_exhausted() {
        let value: Result<&str, &str> =
            retry_n(2, Duration::from_nanos(1), |attempt| match attempt {
                1 => Err("forced retry 1"),
                2 => Err("forced retry 2"),
                _ => panic!("too many retry attempts"),
            });

        assert_eq!(value, Err("forced retry 2"));
    }

    #[tokio::test]
    async fn async_retry_completes_successfully_first_time() {
        let value: Result<&str, &str> =
            async_retry_n(2, Duration::from_nanos(1), |attempt| async move {
                assert_eq!(attempt, 1);
                Ok("success")
            })
            .await;

        assert_eq!(value, Ok("success"));
    }

    #[tokio::test]
    async fn async_retry_completes_successfully_second_time() {
        let value: Result<&str, &str> =
            async_retry_n(2, Duration::from_nanos(1), |attempt| async move {
                match attempt {
                    1 => Err("forced retry"),
                    2 => Ok("success"),
                    _ => panic!("too many retry attempts"),
                }
            })
            .await;

        assert_eq!(value, Ok("success"));
    }

    #[tokio::test]
    async fn async_retry_errors_after_retries_exhausted() {
        let value: Result<&str, &str> =
            async_retry_n(2, Duration::from_nanos(1), |attempt| async move {
                match attempt {
                    1 => Err("forced retry 1"),
                    2 => Err("forced retry 2"),
                    _ => panic!("too many retry attempts"),
                }
            })
            .await;

        assert_eq!(value, Err("forced retry 2"));
    }
}
