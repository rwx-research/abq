use std::{num::NonZeroUsize, sync::atomic::AtomicUsize};

use abq_utils::{atomic, net_protocol::workers::WorkerTest};

/// Concurrently-accessible job queue for a test suite run.
/// Organized so that concurrent accesses require minimal synchronization, usually
/// a single atomic exchange in the happy path.
#[derive(Default, Debug)]
pub struct JobQueue {
    queue: Vec<WorkerTest>,
    /// The last item popped off the queue.
    ptr: AtomicUsize,
}

impl JobQueue {
    pub fn new(work: Vec<WorkerTest>) -> Self {
        Self {
            queue: work,
            ptr: AtomicUsize::new(0),
        }
    }

    /// Pops up to the next `n` items in the queue.
    pub fn get_work(&self, n: NonZeroUsize) -> impl ExactSizeIterator<Item = WorkerTest> + '_ {
        let n = n.get() as usize;
        let queue_len = self.queue.len();

        let mut start_idx = self.ptr.fetch_add(n, atomic::ORDERING);
        let end_idx = std::cmp::min(start_idx + n, self.queue.len());

        // If either
        //   - the start index was past the end of the queue, or
        //   - the end index is now past the end of the queue
        // clamp them down, and do an additional atomic store to clamp the pointer to the end of
        // the queue.
        //
        // Note that the secondary store is always safe, because if either the start or end index
        // is past the queue, this popping already synchronized reaching the end.
        //
        // NB there is a chance for overflow here, but that would require a test suite with at
        // least 2^32 tests! We would fail far before this section in that case.
        let mut clamp = false;
        if start_idx > self.queue.len() {
            clamp = true;
            start_idx = queue_len;
        }
        clamp = clamp || end_idx > queue_len;
        if clamp {
            self.ptr.store(queue_len, atomic::ORDERING);
        }

        self.queue[start_idx..end_idx].iter().cloned()
    }

    pub fn is_at_end(&self) -> bool {
        self.ptr.load(atomic::ORDERING) >= self.queue.len()
    }
}

#[cfg(test)]
mod test {
    use std::{
        num::NonZeroUsize,
        sync::{atomic::AtomicUsize, Arc},
    };

    use abq_run_n_times::n_times;
    use abq_utils::{
        atomic,
        net_protocol::{
            queue::TestSpec,
            runners::{ProtocolWitness, TestCase},
            workers::{WorkId, WorkerTest, INIT_RUN_NUMBER},
        },
    };

    use super::JobQueue;

    #[test]
    #[n_times(100)]
    fn fuzz_concurrent_access() {
        let num_tests = 10_000;
        let num_threads = 20;
        let num_popped = Arc::new(AtomicUsize::new(0));

        let protocol = ProtocolWitness::iter_all().next().unwrap();
        let manifest = std::iter::repeat(WorkerTest::new(
            TestSpec {
                work_id: WorkId::new(),
                test_case: TestCase::new(protocol, "test", Default::default()),
            },
            INIT_RUN_NUMBER,
        ))
        .take(10_000)
        .collect();

        let queue = Arc::new(JobQueue::new(manifest));

        let mut threads = Vec::with_capacity(num_threads);

        for n in 1..=num_threads {
            let queue = queue.clone();
            let num_popped = num_popped.clone();
            let n = NonZeroUsize::try_from(n).unwrap();
            let handle = std::thread::spawn(move || loop {
                let popped = queue.get_work(n);
                num_popped.fetch_add(popped.len(), atomic::ORDERING);
                if popped.len() == 0 {
                    break;
                }
            });
            threads.push(handle);
        }

        for handle in threads {
            handle.join().unwrap();
        }

        assert_eq!(num_popped.load(atomic::ORDERING), num_tests);
        assert!(queue.is_at_end());
    }
}
