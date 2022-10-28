use std::collections::VecDeque;

use net_protocol::runners::{Group, Manifest, MetadataMap, Test, TestCase, TestOrGroup};

pub mod auth;
pub mod net;
pub mod net_async;
pub mod net_opt;
pub mod net_protocol;
pub mod shutdown;
pub mod tls;

/// Flattens a manifest into only [TestId]s, preserving the manifest order.
pub fn flatten_manifest(manifest: Manifest) -> (Vec<TestCase>, MetadataMap) {
    let Manifest {
        members,
        init_meta: meta,
    } = manifest;

    let mut collected = Vec::with_capacity(members.len());
    let mut queue: VecDeque<_> = members.into_iter().collect();
    while let Some(test_or_group) = queue.pop_front() {
        match test_or_group {
            TestOrGroup::Test(Test { id, meta, .. }) => {
                collected.push(TestCase { id, meta });
            }
            TestOrGroup::Group(Group { members, .. }) => {
                for member in members.into_iter().rev() {
                    queue.push_front(member);
                }
            }
        }
    }
    (collected, meta)
}
