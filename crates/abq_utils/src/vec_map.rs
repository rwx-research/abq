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

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
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

    pub fn remove(&mut self, key: K) -> Option<V> {
        self.set
            .iter()
            .position(|(k, _)| k == &key)
            .map(|i| self.set.swap_remove(i).1)
    }

    pub fn get(&mut self, key: K) -> Option<&V> {
        self.set.iter().find(|(k, _)| k == &key).map(|(_, v)| v)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.set.iter().any(|(k, _)| k == key)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &(K, V)> {
        self.set.iter()
    }
}

impl<K, V> IntoIterator for VecMap<K, V> {
    type Item = (K, V);

    type IntoIter = std::vec::IntoIter<(K, V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.set.into_iter()
    }
}

impl<K: Eq, V> FromIterator<(K, V)> for VecMap<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let it = iter.into_iter();
        let (min, max) = it.size_hint();
        let mut map = Self::with_capacity(max.unwrap_or(min));
        for (k, v) in it {
            map.insert(k, v);
        }
        map
    }
}
