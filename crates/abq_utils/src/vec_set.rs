//! A linear set.
//! Faster than typical hashmaps for small key sizes.

#[derive(Debug)]
pub struct VecSet<K>(Vec<K>);

impl<K> Default for VecSet<K> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K: Eq> VecSet<K> {
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns true if the key was already present.
    pub fn insert(&mut self, key: K) -> bool {
        if self.contains(&key) {
            return true;
        }
        self.0.push(key);
        false
    }

    /// Returns true if the key was present.
    pub fn remove(&mut self, key: K) -> bool {
        let i = self.0.iter().position(|k| k == &key);
        match i {
            Some(i) => {
                self.0.swap_remove(i);
                true
            }
            None => false,
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.0.contains(key)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &K> {
        self.0.iter()
    }
}

impl<K> IntoIterator for VecSet<K> {
    type Item = K;

    type IntoIter = std::vec::IntoIter<K>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<K: Eq> FromIterator<K> for VecSet<K> {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        let it = iter.into_iter();
        let (min, max) = it.size_hint();
        let mut set = Self::with_capacity(max.unwrap_or(min));
        for item in it {
            set.insert(item);
        }
        set
    }
}
