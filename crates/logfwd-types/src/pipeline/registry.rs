//! Generic source identity registry with lifecycle tracking.
//!
//! Tracks the lifecycle of data sources (files, Kafka topics, etc.) through
//! Active → Draining → Committed states. Used alongside PipelineMachine to
//! map source identities to metadata needed for checkpoint persistence.
//!
//! Generic over K (identity key) and M (metadata). For file inputs:
//! K = SourceId (wrapping file fingerprint), M = PathBuf.

use alloc::collections::BTreeMap;

/// Lifecycle state of a data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceState {
    /// Source is actively producing data.
    Active,
    /// Source stopped producing but has in-flight batches.
    Draining,
    /// All in-flight batches resolved, checkpoint persisted.
    Committed,
}

/// Entry in the source registry.
#[derive(Debug, Clone)]
pub struct SourceEntry<M> {
    /// Metadata for checkpoint persistence (e.g., file path).
    pub metadata: M,
    /// Current lifecycle state.
    pub state: SourceState,
    /// Logical clock tick when this entry was last updated.
    /// Used for TTL-based pruning of Committed entries.
    /// (Generation counter instead of Instant for Kani compatibility.)
    pub generation: u64,
}

/// Generic source identity registry with lifecycle tracking.
///
/// K = identity key (SourceId for files, TopicPartition for Kafka)
/// M = metadata for persistence (PathBuf for files, String for Kafka)
///
/// The lifecycle is monotonic: Active → Draining → Committed.
/// `upsert` on a Committed entry re-activates it (file reappears with same fingerprint).
pub struct SourceRegistry<K: Ord, M> {
    entries: BTreeMap<K, SourceEntry<M>>,
    current_generation: u64,
}

impl<K: Ord + Clone, M> SourceRegistry<K, M> {
    /// Create an empty registry.
    pub fn new() -> Self {
        SourceRegistry {
            entries: BTreeMap::new(),
            current_generation: 0,
        }
    }

    /// Register or update a source. Sets state to Active, updates generation.
    ///
    /// If the key already exists (even in Committed state), it's re-activated.
    /// Returns the previous state if the key existed.
    pub fn upsert(&mut self, key: K, metadata: M) -> Option<SourceState> {
        let prev = self.entries.get(&key).map(|e| e.state);
        self.entries.insert(
            key,
            SourceEntry {
                metadata,
                state: SourceState::Active,
                generation: self.current_generation,
            },
        );
        prev
    }

    /// Mark a source as Draining (no more data expected).
    ///
    /// Returns true if the transition succeeded (was Active).
    /// Returns false if not Active (no state change).
    pub fn begin_drain(&mut self, key: &K) -> bool {
        if let Some(entry) = self.entries.get_mut(key)
            && entry.state == SourceState::Active
        {
            entry.state = SourceState::Draining;
            entry.generation = self.current_generation;
            return true;
        }
        false
    }

    /// Mark a source as Committed (all in-flight batches resolved).
    ///
    /// Returns true if the transition succeeded (was Draining).
    /// Returns false if not Draining (no state change).
    pub fn commit(&mut self, key: &K) -> bool {
        if let Some(entry) = self.entries.get_mut(key)
            && entry.state == SourceState::Draining
        {
            entry.state = SourceState::Committed;
            entry.generation = self.current_generation;
            return true;
        }
        false
    }

    /// Look up metadata for a source (e.g., file path for checkpoint persistence).
    pub fn metadata(&self, key: &K) -> Option<&M> {
        self.entries.get(key).map(|e| &e.metadata)
    }

    /// Look up the full entry for a source.
    pub fn get(&self, key: &K) -> Option<&SourceEntry<M>> {
        self.entries.get(key)
    }

    /// Advance the generation clock. Call on each flush cycle.
    pub fn tick(&mut self) {
        self.current_generation += 1;
    }

    /// Current generation (logical clock).
    pub fn generation(&self) -> u64 {
        self.current_generation
    }

    /// Remove Committed entries whose generation is older than `min_generation`.
    ///
    /// SAFETY: Never removes Active or Draining entries, regardless of age.
    /// Returns the number of entries removed.
    pub fn prune_committed(&mut self, min_generation: u64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| {
            !(entry.state == SourceState::Committed && entry.generation < min_generation)
        });
        before - self.entries.len()
    }

    /// Number of entries in the registry.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<K: Ord + Clone, M> Default for SourceRegistry<K, M> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_creates_active_entry() {
        let mut reg = SourceRegistry::<u64, &str>::new();
        let prev = reg.upsert(1, "path_a");
        assert!(prev.is_none());
        assert_eq!(reg.get(&1).unwrap().state, SourceState::Active);
        assert_eq!(*reg.metadata(&1).unwrap(), "path_a");
    }

    #[test]
    fn lifecycle_active_to_draining_to_committed() {
        let mut reg = SourceRegistry::<u64, &str>::new();
        reg.upsert(1, "path");

        assert!(reg.begin_drain(&1));
        assert_eq!(reg.get(&1).unwrap().state, SourceState::Draining);

        assert!(reg.commit(&1));
        assert_eq!(reg.get(&1).unwrap().state, SourceState::Committed);
    }

    #[test]
    fn cannot_skip_states() {
        let mut reg = SourceRegistry::<u64, &str>::new();
        reg.upsert(1, "path");

        // Can't commit from Active (must drain first)
        assert!(!reg.commit(&1));
        assert_eq!(reg.get(&1).unwrap().state, SourceState::Active);

        reg.begin_drain(&1);

        // Can't drain again from Draining
        assert!(!reg.begin_drain(&1));
        assert_eq!(reg.get(&1).unwrap().state, SourceState::Draining);

        reg.commit(&1);

        // Can't drain or commit from Committed
        assert!(!reg.begin_drain(&1));
        assert!(!reg.commit(&1));
    }

    #[test]
    fn upsert_reactivates_committed() {
        let mut reg = SourceRegistry::<u64, &str>::new();
        reg.upsert(1, "path_v1");
        reg.begin_drain(&1);
        reg.commit(&1);
        assert_eq!(reg.get(&1).unwrap().state, SourceState::Committed);

        let prev = reg.upsert(1, "path_v2");
        assert_eq!(prev, Some(SourceState::Committed));
        assert_eq!(reg.get(&1).unwrap().state, SourceState::Active);
        assert_eq!(*reg.metadata(&1).unwrap(), "path_v2");
    }

    #[test]
    fn prune_only_removes_committed() {
        let mut reg = SourceRegistry::<u64, &str>::new();
        reg.upsert(1, "active");
        reg.upsert(2, "draining");
        reg.begin_drain(&2);
        reg.upsert(3, "committed");
        reg.begin_drain(&3);
        reg.commit(&3);

        let removed = reg.prune_committed(u64::MAX);
        assert_eq!(removed, 1);
        assert!(reg.metadata(&1).is_some(), "Active must survive");
        assert!(reg.metadata(&2).is_some(), "Draining must survive");
        assert!(reg.metadata(&3).is_none(), "Committed should be pruned");
    }

    #[test]
    fn prune_respects_generation() {
        let mut reg = SourceRegistry::<u64, &str>::new();

        // Entry at generation 0
        reg.upsert(1, "old");
        reg.begin_drain(&1);
        reg.commit(&1);

        reg.tick(); // generation = 1
        reg.tick(); // generation = 2

        // Entry at generation 2
        reg.upsert(2, "new");
        reg.begin_drain(&2);
        reg.commit(&2);

        // Prune entries older than generation 1 — only entry 1 qualifies
        let removed = reg.prune_committed(1);
        assert_eq!(removed, 1);
        assert!(reg.metadata(&1).is_none());
        assert!(reg.metadata(&2).is_some());
    }

    #[test]
    fn missing_key_operations_are_noop() {
        let mut reg = SourceRegistry::<u64, &str>::new();
        assert!(!reg.begin_drain(&99));
        assert!(!reg.commit(&99));
        assert!(reg.metadata(&99).is_none());
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// State transitions are monotonic: Active → Draining → Committed.
    /// No backwards transitions except via upsert (re-activation).
    #[kani::proof]
    fn state_transitions_monotonic() {
        let mut reg = SourceRegistry::<u64, u64>::new();
        let key: u64 = kani::any();
        let meta: u64 = kani::any();

        reg.upsert(key, meta);
        assert_eq!(reg.get(&key).unwrap().state, SourceState::Active);

        // Can't commit from Active
        assert!(!reg.commit(&key));
        assert_eq!(reg.get(&key).unwrap().state, SourceState::Active);

        // Active → Draining
        assert!(reg.begin_drain(&key));
        assert_eq!(reg.get(&key).unwrap().state, SourceState::Draining);

        // Can't drain again
        assert!(!reg.begin_drain(&key));

        // Draining → Committed
        assert!(reg.commit(&key));
        assert_eq!(reg.get(&key).unwrap().state, SourceState::Committed);

        // Can't drain or commit from Committed
        assert!(!reg.begin_drain(&key));
        assert!(!reg.commit(&key));
    }

    /// prune_committed never removes Active or Draining entries.
    #[kani::proof]
    fn prune_never_removes_active_or_draining() {
        let mut reg = SourceRegistry::<u64, u64>::new();
        let k1: u64 = kani::any();
        let k2: u64 = kani::any();
        let k3: u64 = kani::any();
        kani::assume(k1 != k2 && k2 != k3 && k1 != k3);

        reg.upsert(k1, 0); // Active
        reg.upsert(k2, 0);
        reg.begin_drain(&k2); // Draining
        reg.upsert(k3, 0);
        reg.begin_drain(&k3);
        reg.commit(&k3); // Committed

        reg.prune_committed(u64::MAX);

        // Active and Draining must survive
        assert!(reg.metadata(&k1).is_some());
        assert!(reg.metadata(&k2).is_some());
        // Committed should be pruned
        assert!(reg.metadata(&k3).is_none());
    }

    /// upsert on an existing Committed entry re-activates it.
    #[kani::proof]
    fn upsert_reactivates_committed() {
        let mut reg = SourceRegistry::<u64, u64>::new();
        let key: u64 = kani::any();

        reg.upsert(key, 0);
        reg.begin_drain(&key);
        reg.commit(&key);
        assert_eq!(reg.get(&key).unwrap().state, SourceState::Committed);

        reg.upsert(key, 1);
        assert_eq!(reg.get(&key).unwrap().state, SourceState::Active);
    }

    /// prune_committed respects generation — only prunes old entries.
    #[kani::proof]
    fn prune_respects_generation() {
        let mut reg = SourceRegistry::<u64, u64>::new();
        let k1: u64 = kani::any();
        let k2: u64 = kani::any();
        kani::assume(k1 != k2);

        // k1 committed at generation 0
        reg.upsert(k1, 0);
        reg.begin_drain(&k1);
        reg.commit(&k1);

        reg.tick(); // generation = 1

        // k2 committed at generation 1
        reg.upsert(k2, 0);
        reg.begin_drain(&k2);
        reg.commit(&k2);

        // Prune entries with generation < 1
        reg.prune_committed(1);

        // k1 (gen 0) pruned, k2 (gen 1) survives
        assert!(reg.metadata(&k1).is_none());
        assert!(reg.metadata(&k2).is_some());
    }
}
