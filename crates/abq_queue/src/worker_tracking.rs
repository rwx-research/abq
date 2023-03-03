use abq_utils::{
    log_assert,
    net_protocol::entity::{Entity, Tag},
};

#[derive(Debug)]
pub struct WorkerSet<T>(Vec<(Entity, T)>);

impl<T> Default for WorkerSet<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> WorkerSet<T> {
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    pub fn contains_by_tag(&self, entity: &Entity) -> bool {
        self.iter().any(|(e, _)| e.tag == entity.tag)
    }

    /// Inserts an entity by its tag.
    /// Returns an old entity with the same tag, if any.
    pub fn insert_by_tag(&mut self, entity: Entity, val: T) -> Option<(Entity, T)> {
        log_assert!(
            matches!(entity.tag, Tag::Runner(..)),
            ?entity,
            "somehow, a non-worker entity is being tracked"
        );
        match self.0.iter().position(|(k, _)| k.tag == entity.tag) {
            Some(index) => {
                let mut pair = (entity, val);
                std::mem::swap(&mut pair, &mut self.0[index]);
                Some(pair)
            }
            None => {
                self.0.push((entity, val));
                None
            }
        }
    }

    pub fn insert_by_tag_if_missing(&mut self, entity: Entity, val: T) {
        log_assert!(
            matches!(entity.tag, Tag::Runner(..)),
            ?entity,
            "somehow, a non-worker entity is being tracked"
        );
        if !self.0.iter().any(|(k, _)| k.tag == entity.tag) {
            self.0.push((entity, val));
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(Entity, T)> {
        self.0.iter()
    }
}

impl<V> IntoIterator for WorkerSet<V> {
    type Item = (Entity, V);

    type IntoIter = std::vec::IntoIter<(Entity, V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<V> FromIterator<(Entity, V)> for WorkerSet<V> {
    fn from_iter<T: IntoIterator<Item = (Entity, V)>>(iter: T) -> Self {
        let it = iter.into_iter();
        let (min, max) = it.size_hint();
        let mut map = Self(Vec::with_capacity(max.unwrap_or(min)));
        for (k, v) in it {
            map.insert_by_tag(k, v);
        }
        map
    }
}
