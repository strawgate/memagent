use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

#[derive(Debug)]
pub(crate) struct BoundedLruCache<K, V> {
    pub(crate) entries: HashMap<K, V>,
    lru: VecDeque<K>,
    capacity: usize,
}

impl<K, V> BoundedLruCache<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    pub(crate) fn get_cloned<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let value = self.entries.get(key).cloned()?;
        self.touch(key);
        Some(value)
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        if self.entries.contains_key(&key) {
            self.entries.insert(key.clone(), value);
            self.touch(&key);
            return;
        }

        self.entries.insert(key.clone(), value);
        self.lru.push_back(key);

        while self.entries.len() > self.capacity {
            if let Some(evicted) = self.lru.pop_front() {
                self.entries.remove(&evicted);
            } else {
                break;
            }
        }
    }

    fn touch<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if let Some(idx) = self.lru.iter().position(|entry| entry.borrow() == key) {
            let key = self.lru.remove(idx).expect("lru index must be valid");
            self.lru.push_back(key);
        }
    }
}
