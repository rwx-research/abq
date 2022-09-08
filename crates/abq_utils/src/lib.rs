use std::collections::VecDeque;

use net_protocol::runners::{Group, Manifest, Test, TestCase, TestOrGroup};

pub mod auth;
pub mod net;
pub mod net_async;
pub mod net_protocol;
#[cfg(feature = "tls")]
mod tls;

/// Flattens a manifest into only [TestId]s, preserving the manifest order.
pub fn flatten_manifest(manifest: Manifest) -> Vec<TestCase> {
    let mut collected = Vec::with_capacity(manifest.members.len());
    let mut queue: VecDeque<_> = manifest.members.into_iter().collect();
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
    collected
}
