use std::collections::VecDeque;

use net_protocol::runners::{Group, Manifest, Test, TestId, TestOrGroup};

pub mod net_protocol;

/// Flattens a manifest into only [TestId]s, preserving the manifest order.
pub fn flatten_manifest(manifest: Manifest) -> Vec<TestId> {
    let mut collected = Vec::with_capacity(manifest.members.len());
    let mut queue: VecDeque<_> = manifest.members.into_iter().collect();
    while let Some(test_or_group) = queue.pop_front() {
        match test_or_group {
            TestOrGroup::Test(Test { id, .. }) => {
                collected.push(id);
            }
            TestOrGroup::Group(Group { members, .. }) => {
                for member in members.into_iter().rev() {
                    queue.push_front(member);
                }
            }
        }
    }
    collected
}
