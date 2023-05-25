use std::{cell::Cell, num::NonZeroUsize, sync::atomic::AtomicUsize};

use abq_utils::{
    atomic,
    net_protocol::{entity::Tag, workers::WorkerTest},
};

use crate::persistence;

/// Concurrently-accessible job queue for a test suite run.
/// Organized so that concurrent accesses require minimal synchronization, usually
/// a single atomic exchange in the happy path.
#[derive(Default, Debug)]
pub struct JobQueue {
    queue: Vec<WorkerTest>,
    /// To which worker has each entry in the manifest been assigned to?
    /// Modified as a run progresses, by popping off [Self::get_work]
    assigned_entities: Vec<TagCell>,
    /// The last item popped off the queue.
    ptr: AtomicUsize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(transparent)]
struct TagCell(Cell<Tag>);

impl TagCell {
    fn new(tag: Tag) -> Self {
        Self(Cell::new(tag))
    }
}

/// SAFETY: this wrapper is only used in [JobQueue::assigned_entities], which is only accessed by
/// [JobQueue::get_work], and its access pattern is guarded by the assignment of an uncontested
/// region of the queue to a thread. As such, there will never be conflating writes to an EntityCell
/// (though there may conflating write/reads).
unsafe impl Sync for TagCell {}

impl JobQueue {
    pub fn new(work: Vec<WorkerTest>) -> Self {
        let work_len = work.len();
        Self {
            queue: work,
            assigned_entities: vec![TagCell::new(Tag::ExternalClient); work_len],
            ptr: AtomicUsize::new(0),
        }
    }

    /// Pops up to the next `n` items in the queue and assigns them to the given `entity`.
    pub fn get_work(
        &self,
        entity_tag: Tag,
        n: NonZeroUsize,
    ) -> impl ExactSizeIterator<Item = &WorkerTest> + '_ {
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

        for entity_cell in self.assigned_entities[start_idx..end_idx].iter() {
            entity_cell.0.set(entity_tag);
        }

        self.queue[start_idx..end_idx].iter()
    }

    pub fn is_at_end(&self) -> bool {
        self.ptr.load(atomic::ORDERING) >= self.queue.len()
    }

    /// Gets the subset of the manifest assigned to a given worker.
    pub fn get_partition_for_entity(
        &self,
        entity_tag: Tag,
    ) -> impl Iterator<Item = &WorkerTest> + '_ {
        self.assigned_entities
            .iter()
            .enumerate()
            .filter(move |(_, cell)| cell.0.get() == entity_tag)
            .map(|(i, _)| &self.queue[i])
    }

    pub fn into_manifest_view(self) -> persistence::manifest::ManifestView {
        let Self {
            queue,
            assigned_entities,
            ptr: _,
        } = self;

        // Try to convince the compiler that we can simply transmute the list of TagCells to Tags,
        // and we don't actually need another allocation.
        debug_assert_eq!(std::mem::align_of::<TagCell>(), std::mem::align_of::<Tag>());
        debug_assert_eq!(std::mem::size_of::<TagCell>(), std::mem::size_of::<Tag>());
        let assigned_entities = assigned_entities
            .into_iter()
            .map(|t| t.0.into_inner())
            .collect();

        persistence::manifest::ManifestView::new(queue, assigned_entities)
    }

    /// Reads the current index at a given point in time. Not atomic.
    pub fn read_index(&self) -> usize {
        self.ptr.load(atomic::ORDERING)
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
            entity::Entity,
            queue::TestSpec,
            runners::{ProtocolWitness, TestCase},
            workers::{WorkId, WorkerTest, INIT_RUN_NUMBER},
        },
        vec_map::VecMap,
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
                group_id: None,
            },
            INIT_RUN_NUMBER,
        ))
        .take(10_000)
        .collect();

        let queue = Arc::new(JobQueue::new(manifest));

        let mut threads = Vec::with_capacity(num_threads);
        let mut workers = VecMap::with_capacity(num_threads);

        for n in 1..=num_threads {
            let queue = queue.clone();
            let num_popped = num_popped.clone();
            let entity = Entity::runner(n as u32, n as u32);
            let n = NonZeroUsize::try_from(n).unwrap();
            workers.insert(entity.tag, n);
            let handle = std::thread::spawn(move || loop {
                let popped = queue.get_work(entity.tag, n);
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

        // Now, go through the queue's assigned entities and make sure everything looks okay.
        // There should be no holes, and the runs of assignments should align with how many tests
        // each worker popped off.
        let assigned = &queue.assigned_entities;
        let mut chunks = vec![(assigned.first().unwrap(), 0)];
        for entity in assigned {
            let mut last_chunk = chunks.last_mut();
            let last_chunk = last_chunk.as_mut().unwrap();
            let entity_batch = workers.get(entity.0.get()).unwrap();

            // Add to the latest run, or if the worker pulled more than once in a row, break
            // up the chunks into separate runs.
            if last_chunk.0 == entity && last_chunk.1 < entity_batch.get() {
                last_chunk.1 += 1;
            } else {
                chunks.push((entity, 1));
            }
        }
        let mut chunks_it = chunks.into_iter().peekable();
        while let Some((entity, run)) = chunks_it.next() {
            let entity_batch = workers.get(entity.0.get()).unwrap();
            match chunks_it.peek() {
                Some(_) => assert_eq!(run, entity_batch.get()),
                None => assert!(run <= entity_batch.get()),
            }
        }
    }

    #[test]
    #[n_times(100)]
    fn fuzz_partitions() {
        let num_tests = 10_000;
        let num_threads = 20;
        let num_popped = Arc::new(AtomicUsize::new(0));

        let protocol = ProtocolWitness::iter_all().next().unwrap();
        let manifest = std::iter::repeat(WorkerTest::new(
            TestSpec {
                work_id: WorkId::new(),
                test_case: TestCase::new(protocol, "test", Default::default()),
                group_id: None,
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
            let entity = Entity::runner(n as u32, n as u32);
            let n = NonZeroUsize::try_from(n).unwrap();
            let handle = std::thread::spawn(move || {
                let mut local_manifest = vec![];
                loop {
                    let popped = queue.get_work(entity.tag, n);
                    num_popped.fetch_add(popped.len(), atomic::ORDERING);
                    if popped.len() == 0 {
                        break;
                    }
                    local_manifest.extend(popped.cloned());
                }
                local_manifest
            });
            threads.push((entity, handle));
        }

        for (entity, handle) in threads {
            let local_manifest = handle.join().unwrap();
            let queue_seen_manifest: Vec<_> = queue
                .get_partition_for_entity(entity.tag)
                .cloned()
                .collect();
            assert_eq!(local_manifest, queue_seen_manifest);
        }

        assert_eq!(num_popped.load(atomic::ORDERING), num_tests);
        assert!(queue.is_at_end());
    }
}
