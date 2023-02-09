//! An associative list.
//! Faster than typical hashmaps for small key sizes.
#[derive(Debug)]
pub struct VecMap<K, V> {
    set: Vec<(K, V)>,
}

impl<K, V> Default for VecMap<K, V> {
    fn default() -> Self {
        Self {
            set: Default::default(),
        }
    }
}

impl<K: Eq, V> VecMap<K, V> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            set: Vec::with_capacity(cap),
        }
    }

    pub fn insert(&mut self, key: K, val: V) -> Option<V> {
        match self.set.iter().position(|(k, _)| k == &key) {
            Some(index) => {
                let mut pair = (key, val);
                std::mem::swap(&mut pair, &mut self.set[index]);

                Some(pair.1)
            }
            None => {
                self.set.push((key, val));

                None
            }
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.set.iter().any(|(k, _)| k == key)
    }
}

impl<K, V> IntoIterator for VecMap<K, V> {
    type Item = (K, V);

    type IntoIter = std::vec::IntoIter<(K, V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.set.into_iter()
    }
}
