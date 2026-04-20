use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

/// Capacity-limited least-recently-used cache for UDF pattern objects.
///
/// The cache stores values in `entries` and tracks access order in `lru`.
/// Insertions and successful lookups mark a key as most recently used; when
/// the capacity is exceeded, the least recently used key is evicted. Callers
/// must provide synchronization if the cache is shared across threads.
#[derive(Debug)]
pub(crate) struct BoundedLruCache<K, V> {
    /// Stored cache entries keyed by pattern text.
    pub(crate) entries: HashMap<K, V>,
    lru: VecDeque<K>,
    capacity: usize,
}

impl<K, V> BoundedLruCache<K, V>
where
    K: Clone + Eq + Hash,
{
    /// Create a cache with room for at least one entry.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Return a cloned value and mark the key as recently used.
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

    /// Insert or replace a value and evict least-recently-used entries.
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

#[cfg(test)]
mod tests {
    use super::BoundedLruCache;

    #[test]
    fn zero_capacity_keeps_one_entry() {
        let mut cache = BoundedLruCache::new(0);

        cache.insert("first", 1);
        cache.insert("second", 2);

        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.get_cloned("second"), Some(2));
        assert_eq!(cache.get_cloned("first"), None);
    }
}
