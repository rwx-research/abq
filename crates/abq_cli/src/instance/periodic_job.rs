use abq_utils::atomic;
use futures::future::BoxFuture;
use std::sync::{atomic::AtomicBool, Arc};

struct ConcurrencyControlledJobInner<F> {
    job_name: &'static str,
    is_active: AtomicBool,
    run: F,
}

#[derive(Clone)]
pub struct ConcurrencyControlledJob<F>(Arc<ConcurrencyControlledJobInner<F>>);

impl<F> ConcurrencyControlledJob<F>
where
    F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
{
    pub fn new(job_name: &'static str, job: F) -> Self {
        Self(Arc::new(ConcurrencyControlledJobInner {
            is_active: AtomicBool::new(false),
            job_name,
            run: job,
        }))
    }

    pub async fn run(&self) {
        let me = &self.0;

        if me
            .is_active
            .compare_exchange(false, true, atomic::ORDERING, atomic::ORDERING)
            .is_err()
        {
            tracing::info!("skipping job {} because it is already running", me.job_name);
            return;
        }

        (me.run)().await;

        me.is_active.store(false, atomic::ORDERING);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use abq_utils::atomic;
    use futures::future::FutureExt;
    use std::sync::atomic::AtomicU8;

    #[tokio::test]
    async fn allows_sequential_runs() {
        let count: Arc<AtomicU8> = Default::default();
        let job = {
            let count = count.clone();
            ConcurrencyControlledJob::new("test", move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, atomic::ORDERING);
                }
                .boxed()
            })
        };

        for i in 0..10 {
            job.run().await;
            assert_eq!(count.load(atomic::ORDERING), i + 1);
        }
    }

    async fn wait_for_flag(flag: &AtomicBool) {
        while !flag.load(atomic::ORDERING) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn blocks_concurrent_runs() {
        let count: Arc<AtomicU8> = Default::default();
        let first_job_active: Arc<AtomicBool> = Default::default();
        let first_job_complete: Arc<AtomicBool> = Default::default();

        let job = {
            let count = count.clone();
            let first_job_active = first_job_active.clone();
            let first_job_complete = first_job_complete.clone();
            ConcurrencyControlledJob::new("test", move || {
                let count = count.clone();
                let first_job_active = first_job_active.clone();
                let first_job_complete = first_job_complete.clone();
                async move {
                    first_job_active.store(true, atomic::ORDERING);
                    count.fetch_add(1, atomic::ORDERING);
                    wait_for_flag(&first_job_complete).await;
                }
                .boxed()
            })
        };

        let first_run = {
            let job = job.clone();
            tokio::spawn(async move { job.run().await })
        };
        wait_for_flag(&first_job_active).await;

        // Launch the second run, it should immediately exit.
        job.run().await;

        first_job_complete.store(true, atomic::ORDERING);
        first_run.await.unwrap();

        assert_eq!(count.load(atomic::ORDERING), 1);
    }
}
