use std::{cell::Cell, num::NonZeroUsize, sync::atomic::AtomicUsize};

use abq_utils::{
    atomic,
    net_protocol::{entity::Tag, queue::TestStrategy, workers::WorkerTest},
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
    test_strategy: TestStrategy,
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
    pub fn new(work: Vec<WorkerTest>, test_strategy: TestStrategy) -> Self {
        let work_len = work.len();
        Self {
            queue: work,
            assigned_entities: vec![TagCell::new(Tag::ExternalClient); work_len],
            ptr: AtomicUsize::new(0),
            test_strategy,
        }
    }

    /// Pops up to the next `n` items in the queue and assigns them to the given `entity`.
    pub fn get_work(
        &self,
        entity_tag: Tag,
        suggested_batch_size: NonZeroUsize,
    ) -> impl ExactSizeIterator<Item = &WorkerTest> + '_ {
        let suggested_batch_size = suggested_batch_size.get() as usize;
        let queue_len = self.queue.len();

        // If the start index was past the end of the queue, return fast

        let (start_idx, end_idx) = match self.test_strategy {
            TestStrategy::ByTest => self.get_bounds_by_test(suggested_batch_size),
            TestStrategy::ByTopLevelGroup => {
                self.get_bounds_by_top_level_group(suggested_batch_size)
            }
        };
        if start_idx >= queue_len {
            self.ptr.store(queue_len, atomic::ORDERING);
            return [].iter();
        }

        // for retries, mark these tests as owned by this worker
        for entity_cell in self.assigned_entities[start_idx..end_idx].iter() {
            entity_cell.0.set(entity_tag);
        }

        self.queue[start_idx..end_idx].iter()
    }

    #[inline]
    fn get_bounds_by_test(&self, suggested_batch_size: usize) -> (usize, usize) {
        let start_idx = self.ptr.fetch_add(suggested_batch_size, atomic::ORDERING);
        (
            start_idx,
            std::cmp::min(start_idx + suggested_batch_size, self.queue.len()),
        )
    }

    #[inline]
    fn get_bounds_by_top_level_group(&self, suggested_batch_size: usize) -> (usize, usize) {
        let queue_len = self.queue.len();
        let mut end_idx = 0;
        let start_idx = self
            .ptr
            .fetch_update(atomic::ORDERING, atomic::ORDERING, |start_idx| {
                end_idx = start_idx;
                if start_idx >= queue_len {
                    return None;
                }
                let mut current_group = self.queue[start_idx].spec.group_id;
                // find idx of the start of the next group
                for next_spec in self.queue[start_idx..].iter() {
                    // if we've walked into a new group...
                    if next_spec.spec.group_id != current_group {
                        // if the current slice is big enough, break
                        if end_idx - start_idx >= suggested_batch_size {
                            break;
                        }
                        // else start pulling from the next group
                        current_group = next_spec.spec.group_id
                    }
                    end_idx += 1; // include next_spec in the slice
                }
                Some(end_idx)
            })
            .unwrap_or(queue_len);
        (start_idx, end_idx)
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
            test_strategy: _,
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
            entity::{Entity, Tag},
            queue::{TestSpec, TestStrategy},
            runners::{ProtocolWitness, TestCase},
            workers::{GroupId, WorkId, WorkerTest, INIT_RUN_NUMBER},
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
                group_id: GroupId::new(),
            },
            INIT_RUN_NUMBER,
        ))
        .take(10_000)
        .collect();

        let queue = Arc::new(JobQueue::new(manifest, TestStrategy::ByTest));

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

        // convert assigned_entities into a series of chunks of (entity, num_popped)
        // if the same entity pulled twice in a row, add two chunks back-to-back
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

        // Now, go through the queue's assigned entities and make sure everything looks okay.
        // There should be no holes, and the runs of assignments should align with how many tests
        // each worker popped off.
        let mut chunks_it = chunks.into_iter().peekable();
        while let Some((entity, run)) = chunks_it.next() {
            let entity_batch = workers.get(entity.0.get()).unwrap();
            match chunks_it.peek() {
                Some(_) => assert_eq!(run, entity_batch.get(), "if there are more chunks, batch size should be full"),
                None => assert!(run <= entity_batch.get(), "if there are no more chunks, the run should be less than or equal to the batch size"),
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
                group_id: GroupId::new(),
            },
            INIT_RUN_NUMBER,
        ))
        .take(10_000)
        .collect();

        let queue = Arc::new(JobQueue::new(manifest, TestStrategy::ByTest));

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

    #[test]
    #[n_times(100)]
    fn fuzz_grouped_partitions() {
        use std::collections::HashMap;
        // first create the manifest
        let min_group_size = 1;
        let max_group_size = 24; // group batch sums cleanly to 300
        let num_batches = 33; // close to 10k tests, like the other tests
        let num_tests = num_batches * (min_group_size..=max_group_size).sum::<usize>();
        let num_threads = 20;
        let num_popped = Arc::new(AtomicUsize::new(0));

        let protocol = ProtocolWitness::iter_all().next().unwrap();

        // build the manifest
        let mut manifest = Vec::with_capacity(num_tests);
        for _ in 0..num_batches {
            for num_tests_in_group in min_group_size..=max_group_size {
                let group_id = GroupId::new();
                manifest.extend(
                    std::iter::repeat(WorkerTest::new(
                        TestSpec {
                            work_id: WorkId::new(),
                            test_case: TestCase::new(protocol, "test", Default::default()),
                            group_id,
                        },
                        INIT_RUN_NUMBER,
                    ))
                    .take(num_tests_in_group),
                );
            }
        }

        let queue = Arc::new(JobQueue::new(manifest, TestStrategy::ByTopLevelGroup));

        let mut threads = Vec::with_capacity(num_threads);

        for n in 1..=num_threads {
            let queue = queue.clone();
            let num_popped = num_popped.clone();
            let mut entity_by_group_id: HashMap<GroupId, Entity> = HashMap::new();
            let entity = Entity::runner(n as u32, n as u32);
            let batch_size = NonZeroUsize::try_from(n).unwrap();
            let handle = std::thread::spawn(move || {
                let mut local_manifest = vec![];
                loop {
                    let popped = queue.get_work(entity.tag, batch_size);
                    num_popped.fetch_add(popped.len(), atomic::ORDERING);
                    if popped.len() == 0 {
                        break;
                    }

                    let popped: Vec<WorkerTest> = popped.cloned().collect();

                    // assert every group is only assigned to one entity
                    for test in popped.clone() {
                        if let Some(previous_entity) =
                            entity_by_group_id.insert(test.spec.group_id, entity)
                        {
                            assert_eq!(previous_entity, entity);
                        }
                    }

                    local_manifest.extend(popped)
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

        for entity in &queue.assigned_entities {
            // entities are all initialized to Tag::ExternalClient then assigned worker tags as they are popped

            // here we ensure all entities have been assigned to workers
            let tag = entity.0.get();
            assert_ne!(tag, Tag::ExternalClient)
        }
    }
}
