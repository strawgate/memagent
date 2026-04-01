//! Pipeline lifecycle state machine — ordered batch completion tracking.
//!
//! The pipeline flows: Starting → Running → Draining → Stopped.
//!
//! Ordered ACK tracking ensures checkpoints are committed only when ALL
//! prior batches for a source are acknowledged (Filebeat registrar pattern).
//! The checkpoint type `C` is opaque — the pipeline stores and forwards
//! checkpoints without interpreting them.

use alloc::collections::BTreeMap;
use core::marker::PhantomData;

use super::{AckReceipt, BatchId, BatchTicket, Queued, Sending, SourceId};

// ---------------------------------------------------------------------------
// Pipeline lifecycle typestate markers
// ---------------------------------------------------------------------------

/// Pipeline is initializing (connecting sinks, starting readers).
pub struct Starting;
/// Pipeline is actively processing batches.
pub struct Running;
/// Shutdown requested — draining in-flight batches.
pub struct Draining;
/// All batches acked, pipeline fully stopped.
pub struct Stopped;

/// Pipeline lifecycle state machine.
///
/// Tracks per-source batch completion and committed checkpoints.
/// The checkpoint type `C` is opaque — input plugins define what it means
/// (byte offset for files, partition offset for Kafka, cursor for journald).
///
/// Batch ordering uses `BatchId` (sequential, gap-free, assigned by the
/// machine). Committed checkpoint advances only when all prior batches
/// for a source are acked.
pub struct PipelineMachine<S, C> {
    /// Monotonically increasing batch ID counter.
    next_batch_id: u64,
    /// Per-source committed checkpoint (from the highest contiguously-acked batch).
    committed: BTreeMap<SourceId, C>,
    /// Per-source in-flight batches awaiting ACK (batch_id → checkpoint).
    in_flight: BTreeMap<SourceId, BTreeMap<BatchId, C>>,
    /// Per-source pending ACKs received out of order (batch_id → checkpoint).
    pending_acks: BTreeMap<SourceId, BTreeMap<BatchId, C>>,
    /// Typestate marker.
    _state: PhantomData<S>,
}

/// Result of applying an AckReceipt to the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAdvance<C> {
    /// The source whose checkpoint may have advanced.
    pub source: SourceId,
    /// Whether the committed checkpoint actually changed.
    pub advanced: bool,
    /// The committed checkpoint after this ack (None if source never committed).
    pub checkpoint: Option<C>,
}

// ---------------------------------------------------------------------------
// Starting → Running
// ---------------------------------------------------------------------------

impl<C> PipelineMachine<Starting, C> {
    /// Create a new pipeline in Starting state.
    pub fn new() -> Self {
        PipelineMachine {
            next_batch_id: 0,
            committed: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            pending_acks: BTreeMap::new(),
            _state: PhantomData,
        }
    }

    /// All sinks connected, begin processing.
    pub fn start(self) -> PipelineMachine<Running, C> {
        PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: self.in_flight,
            pending_acks: self.pending_acks,
            _state: PhantomData,
        }
    }
}

impl<C> Default for PipelineMachine<Starting, C> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Running — batch creation + ACK processing
// ---------------------------------------------------------------------------

impl<C: Clone> PipelineMachine<Running, C> {
    /// Create a new batch ticket for the given source.
    ///
    /// Assigns a `BatchId` but does NOT register the batch in the machine's
    /// tracking. The batch enters in-flight tracking only when
    /// [`begin_send`](Self::begin_send) is called. If the Queued ticket is
    /// dropped (e.g., error during preparation), nothing is orphaned.
    ///
    /// The checkpoint value is opaque — the pipeline stores it and commits
    /// it when all prior batches for this source are acked.
    pub fn create_batch(&mut self, source: SourceId, checkpoint: C) -> BatchTicket<Queued, C> {
        let id = BatchId(self.next_batch_id);
        self.next_batch_id += 1;
        BatchTicket::new(id, source, checkpoint)
    }

    /// Register a batch as in-flight and begin sending.
    ///
    /// This is the point where the pipeline takes ownership — after this
    /// call, the batch MUST be acked, rejected, or failed (retry). Dropping
    /// a Sending ticket is a bug (warned by `#[must_use]`).
    ///
    /// Consumes the Queued ticket, returns a Sending ticket.
    pub fn begin_send(&mut self, ticket: BatchTicket<Queued, C>) -> BatchTicket<Sending, C> {
        self.in_flight
            .entry(ticket.source())
            .or_default()
            .insert(ticket.id(), ticket.checkpoint().clone());

        ticket.begin_send()
    }

    /// Apply an AckReceipt and advance the committed checkpoint if possible.
    ///
    /// Ordered ACK: the checkpoint advances only when all batches with
    /// lower IDs for this source have also been acked. Out-of-order acks
    /// are buffered in `pending_acks`.
    pub fn apply_ack(&mut self, receipt: AckReceipt<C>) -> CommitAdvance<C> {
        record_ack_and_advance(
            &mut self.committed,
            &mut self.in_flight,
            &mut self.pending_acks,
            receipt,
        )
    }

    /// Request graceful shutdown — drain remaining in-flight batches.
    pub fn begin_drain(self) -> PipelineMachine<Draining, C> {
        PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: self.in_flight,
            pending_acks: self.pending_acks,
            _state: PhantomData,
        }
    }

    /// Number of in-flight batches across all sources.
    ///
    /// Includes batches that have been `fail()`ed and requeued — the
    /// machine considers them in-flight until `apply_ack` is called with
    /// a successful `AckReceipt`. Use this for backpressure (e.g. limit
    /// concurrent sends) but note that retrying batches are counted.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().map(BTreeMap::len).sum()
    }

    /// Committed checkpoint for a source, or `None` if no batch for this
    /// source has been committed yet.
    pub fn committed_checkpoint(&self, source: SourceId) -> Option<&C> {
        self.committed.get(&source)
    }
}

// ---------------------------------------------------------------------------
// Shared helper (used by both Running and Draining)
// ---------------------------------------------------------------------------

/// Record an ack receipt: remove from in-flight, add to pending, try advance.
fn record_ack_and_advance<C: Clone>(
    committed: &mut BTreeMap<SourceId, C>,
    in_flight: &mut BTreeMap<SourceId, BTreeMap<BatchId, C>>,
    pending_acks: &mut BTreeMap<SourceId, BTreeMap<BatchId, C>>,
    receipt: AckReceipt<C>,
) -> CommitAdvance<C> {
    let source = receipt.source;
    let batch_id = receipt.batch_id;

    // Remove from in-flight by exact BatchId (O(log n)).
    // If not found, this is a spurious/duplicate ack — return early.
    let removed = if let Some(source_flights) = in_flight.get_mut(&source) {
        if let Some(tracked_checkpoint) = source_flights.remove(&batch_id) {
            pending_acks
                .entry(source)
                .or_default()
                .insert(batch_id, tracked_checkpoint);
            true
        } else {
            false
        }
    } else {
        false
    };

    if !removed {
        return CommitAdvance {
            source,
            advanced: false,
            checkpoint: committed.get(&source).cloned(),
        };
    }

    // Try to advance committed checkpoint by consuming contiguous pending acks.
    let mut advanced = false;
    if let Some(pending) = pending_acks.get_mut(&source) {
        while let Some((&lowest_id, _)) = pending.iter().next() {
            // Check if any in-flight batch has a lower ID
            let has_lower = in_flight
                .get(&source)
                .and_then(|f| f.keys().next().copied())
                .is_some_and(|min_id| min_id < lowest_id);
            if has_lower {
                break;
            }
            // Consume this ack — advance checkpoint
            let cp = pending.remove(&lowest_id).unwrap();
            committed.insert(source, cp);
            advanced = true;
        }
    }

    // Clean up empty inner maps to avoid unbounded accumulation
    // with many ephemeral sources (short-lived file tails, etc.).
    if in_flight.get(&source).is_some_and(BTreeMap::is_empty) {
        in_flight.remove(&source);
    }
    if pending_acks.get(&source).is_some_and(BTreeMap::is_empty) {
        pending_acks.remove(&source);
    }

    CommitAdvance {
        source,
        advanced,
        checkpoint: committed.get(&source).cloned(),
    }
}

// ---------------------------------------------------------------------------
// Draining — can still process acks but no new batches
// ---------------------------------------------------------------------------

impl<C: Clone> PipelineMachine<Draining, C> {
    /// Apply an AckReceipt during drain (same logic as Running).
    pub fn apply_ack(&mut self, receipt: AckReceipt<C>) -> CommitAdvance<C> {
        record_ack_and_advance(
            &mut self.committed,
            &mut self.in_flight,
            &mut self.pending_acks,
            receipt,
        )
    }

    /// Check if all in-flight batches have been acked.
    pub fn is_drained(&self) -> bool {
        self.in_flight.values().all(BTreeMap::is_empty)
            && self.pending_acks.values().all(BTreeMap::is_empty)
    }

    /// All batches drained — transition to Stopped.
    ///
    /// Returns `Err(self)` if there are still in-flight or pending batches,
    /// preventing silent data loss in release builds.
    pub fn stop(self) -> Result<PipelineMachine<Stopped, C>, Self> {
        if !self.is_drained() {
            return Err(self);
        }
        Ok(PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: self.in_flight, // guaranteed empty by is_drained()
            pending_acks: self.pending_acks, // guaranteed empty by is_drained()
            _state: PhantomData,
        })
    }

    /// Number of in-flight batches across all sources.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().map(BTreeMap::len).sum()
    }

    /// Committed checkpoint for a source, or `None` if never committed.
    pub fn committed_checkpoint(&self, source: SourceId) -> Option<&C> {
        self.committed.get(&source)
    }
}

// ---------------------------------------------------------------------------
// Stopped — terminal state, read-only
// ---------------------------------------------------------------------------

impl<C> PipelineMachine<Stopped, C> {
    /// Final committed checkpoints for all sources.
    pub fn final_checkpoints(&self) -> &BTreeMap<SourceId, C> {
        &self.committed
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: create a machine with u64 checkpoints (file byte offsets).
    fn new_running() -> PipelineMachine<Running, u64> {
        PipelineMachine::<Starting, u64>::new().start()
    }

    #[test]
    fn full_lifecycle() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let t2 = running.create_batch(src, 2000u64);
        assert_eq!(running.in_flight_count(), 0); // not yet sent

        let sending1 = running.begin_send(t1);
        assert_eq!(running.in_flight_count(), 1); // t1 now in-flight
        let sending2 = running.begin_send(t2);
        assert_eq!(running.in_flight_count(), 2); // both in-flight

        let advance = running.apply_ack(sending1.ack());
        assert!(advance.advanced);
        assert_eq!(advance.checkpoint, Some(1000));

        let advance = running.apply_ack(sending2.ack());
        assert_eq!(advance.checkpoint, Some(2000));

        assert_eq!(running.in_flight_count(), 0);

        let draining = running.begin_drain();
        assert!(draining.is_drained());
        let stopped = draining.stop().ok().expect("should be drained");
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), 2000);
    }

    #[test]
    fn out_of_order_ack() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let t2 = running.create_batch(src, 2000u64);
        let t3 = running.create_batch(src, 3000u64);

        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);
        let s3 = running.begin_send(t3);

        // Ack batch 3 first — checkpoint should NOT advance
        let advance = running.apply_ack(s3.ack());
        assert!(!advance.advanced);
        assert_eq!(advance.checkpoint, None);

        // Ack batch 1 — advances to 1000 only (batch 2 still in flight)
        let advance = running.apply_ack(s1.ack());
        assert!(advance.advanced);
        assert_eq!(advance.checkpoint, Some(1000));

        // Ack batch 2 — jumps to 3000 (1→2→3 all acked)
        let advance = running.apply_ack(s2.ack());
        assert!(advance.advanced);
        assert_eq!(advance.checkpoint, Some(3000));
    }

    #[test]
    fn retry_then_ack() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let s1 = running.begin_send(t1);
        let requeued = s1.fail();
        assert_eq!(requeued.attempts(), 1);

        let s1_retry = running.begin_send(requeued);
        let advance = running.apply_ack(s1_retry.ack());
        assert_eq!(advance.checkpoint, Some(1000));
    }

    #[test]
    fn multi_source() {
        let mut running = new_running();
        let src_a = SourceId(0);
        let src_b = SourceId(1);

        let ta = running.create_batch(src_a, 500u64);
        let tb = running.create_batch(src_b, 800u64);

        let sa = running.begin_send(ta);
        let sb = running.begin_send(tb);

        // Ack source B first
        let advance_b = running.apply_ack(sb.ack());
        assert_eq!(advance_b.source, src_b);
        assert_eq!(advance_b.checkpoint, Some(800));

        // Source A still uncommitted
        assert_eq!(running.committed_checkpoint(src_a), None);

        let advance_a = running.apply_ack(sa.ack());
        assert_eq!(advance_a.checkpoint, Some(500));
    }

    #[test]
    fn drain_with_pending() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let s1 = running.begin_send(t1);

        let mut draining = running.begin_drain();
        assert!(!draining.is_drained());

        let advance = draining.apply_ack(s1.ack());
        assert_eq!(advance.checkpoint, Some(1000));
        assert!(draining.is_drained());

        let stopped = draining.stop().ok().expect("should be drained");
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), 1000);
    }

    #[test]
    fn reject_advances_checkpoint() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let s1 = running.begin_send(t1);

        let advance = running.apply_ack(s1.reject());
        assert_eq!(advance.checkpoint, Some(1000));
    }

    #[test]
    fn mixed_ack_and_reject() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let t2 = running.create_batch(src, 2000u64);
        let t3 = running.create_batch(src, 3000u64);

        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);
        let s3 = running.begin_send(t3);

        running.apply_ack(s2.reject());
        running.apply_ack(s3.ack());
        let advance = running.apply_ack(s1.ack());

        assert_eq!(advance.checkpoint, Some(3000));
        assert_eq!(running.in_flight_count(), 0);
    }

    #[test]
    fn stop_before_drained_returns_err() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let _s1 = running.begin_send(t1);

        let draining = running.begin_drain();
        assert!(!draining.is_drained());

        let draining = draining.stop().err().expect("should fail: not drained");
        assert_eq!(draining.in_flight_count(), 1);
    }

    #[test]
    fn no_batches_drain_immediate() {
        let running = new_running();

        let draining = running.begin_drain();
        assert!(draining.is_drained());
        assert!(draining.stop().is_ok());
    }

    #[test]
    fn default_impl() {
        let machine: PipelineMachine<Starting, u64> = PipelineMachine::default();
        let running = machine.start();
        assert_eq!(running.in_flight_count(), 0);
        assert_eq!(running.committed_checkpoint(SourceId(0)), None);
    }

    #[test]
    fn high_batch_count_single_source() {
        let mut running = new_running();
        let src = SourceId(0);

        let mut sending = alloc::vec::Vec::new();
        for i in 0..100u64 {
            let t = running.create_batch(src, (i + 1) * 100);
            sending.push(running.begin_send(t));
        }
        assert_eq!(running.in_flight_count(), 100);

        // Ack in reverse order
        for s in sending.into_iter().rev() {
            running.apply_ack(s.ack());
        }

        assert_eq!(running.committed_checkpoint(src), Some(&10000));
        assert_eq!(running.in_flight_count(), 0);
    }

    #[test]
    fn unit_checkpoint_push_source() {
        // Push sources have no checkpoint — use ()
        let mut running: PipelineMachine<Running, ()> =
            PipelineMachine::<Starting, ()>::new().start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, ());
        let s1 = running.begin_send(t1);
        let advance = running.apply_ack(s1.ack());
        assert!(advance.advanced);
        assert_eq!(advance.checkpoint, Some(()));
    }

    #[test]
    fn string_checkpoint_journald() {
        // Journald uses opaque cursor strings
        let mut running: PipelineMachine<Running, &str> =
            PipelineMachine::<Starting, &str>::new().start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, "s=abc;i=1");
        let t2 = running.create_batch(src, "s=abc;i=2");

        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);

        running.apply_ack(s1.ack());
        let advance = running.apply_ack(s2.ack());
        assert_eq!(advance.checkpoint, Some("s=abc;i=2"));
    }

    #[test]
    fn spurious_ack_ignored() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let s1 = running.begin_send(t1);
        running.apply_ack(s1.ack());

        // Fabricate a duplicate ack (same batch_id)
        let duplicate = AckReceipt {
            batch_id: BatchId(0),
            source: src,
            checkpoint: 9999u64,
            delivered: true,
        };
        let advance = running.apply_ack(duplicate);

        // Should not advance — spurious ack ignored
        assert!(!advance.advanced);
        assert_eq!(advance.checkpoint, Some(1000));
    }

    #[test]
    fn dropped_queued_ticket_does_not_wedge() {
        let mut running = new_running();
        let src = SourceId(0);

        // Create a batch but drop the ticket without sending
        let _dropped = running.create_batch(src, 1000u64);
        // Ticket dropped here — not in in_flight, nothing orphaned

        // Create and complete another batch
        let t2 = running.create_batch(src, 2000u64);
        let s2 = running.begin_send(t2);
        running.apply_ack(s2.ack());

        // Pipeline can drain and stop — not wedged
        assert_eq!(running.in_flight_count(), 0);
        let draining = running.begin_drain();
        assert!(draining.is_drained());
        assert!(draining.stop().is_ok());
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// Ordered ACK: after all batches acked, committed checkpoint equals the last batch's.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_ordered_ack() {
        let mut running: PipelineMachine<Running, u64> =
            PipelineMachine::<Starting, u64>::new().start();
        let src = SourceId(0);

        let cp1: u64 = kani::any();
        let cp2: u64 = kani::any();
        let cp3: u64 = kani::any();

        let t1 = running.create_batch(src, cp1);
        let t2 = running.create_batch(src, cp2);
        let t3 = running.create_batch(src, cp3);

        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);
        let s3 = running.begin_send(t3);

        // Ack in arbitrary order (all 6 permutations)
        let order: u8 = kani::any_where(|&o: &u8| o < 6);
        let (r_a, r_b, r_c) = match order {
            0 => (s1.ack(), s2.ack(), s3.ack()),
            1 => (s1.ack(), s3.ack(), s2.ack()),
            2 => (s2.ack(), s1.ack(), s3.ack()),
            3 => (s2.ack(), s3.ack(), s1.ack()),
            4 => (s3.ack(), s1.ack(), s2.ack()),
            _ => (s3.ack(), s2.ack(), s1.ack()),
        };

        running.apply_ack(r_a);
        running.apply_ack(r_b);
        let a3 = running.apply_ack(r_c);

        // After all 3 acked, checkpoint must be cp3 (last batch's checkpoint)
        assert_eq!(a3.checkpoint, Some(cp3));
        assert_eq!(running.in_flight_count(), 0);
    }

    /// After all batches acked, committed checkpoint is the last batch's.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_all_acked_final_checkpoint() {
        let mut running: PipelineMachine<Running, u64> =
            PipelineMachine::<Starting, u64>::new().start();
        let src = SourceId(0);

        let cp1: u64 = kani::any();
        let cp2: u64 = kani::any();

        let t1 = running.create_batch(src, cp1);
        let t2 = running.create_batch(src, cp2);

        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);

        running.apply_ack(s1.ack());
        running.apply_ack(s2.ack());

        assert_eq!(running.committed_checkpoint(src), Some(&cp2));
        assert_eq!(running.in_flight_count(), 0);
    }

    /// Drain → stop is safe when all batches acked.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_drain_to_stop() {
        let mut running: PipelineMachine<Running, u64> =
            PipelineMachine::<Starting, u64>::new().start();
        let src = SourceId(0);

        let cp: u64 = kani::any();
        let t1 = running.create_batch(src, cp);
        let s1 = running.begin_send(t1);

        let mut draining = running.begin_drain();
        draining.apply_ack(s1.ack());

        assert!(draining.is_drained());
        let stopped = draining.stop().ok().expect("should be drained");
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), cp);
    }

    /// Multi-source: sources are independent.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_multi_source_independence() {
        let mut running: PipelineMachine<Running, u64> =
            PipelineMachine::<Starting, u64>::new().start();
        let src_a = SourceId(0);
        let src_b = SourceId(1);

        let cp_a: u64 = kani::any();
        let cp_b: u64 = kani::any();

        let ta = running.create_batch(src_a, cp_a);
        let tb = running.create_batch(src_b, cp_b);
        let sa = running.begin_send(ta);
        let sb = running.begin_send(tb);

        // Ack B only
        running.apply_ack(sb.ack());

        assert_eq!(running.committed_checkpoint(src_a), None);
        assert_eq!(running.committed_checkpoint(src_b), Some(&cp_b));

        running.apply_ack(sa.ack());
        assert_eq!(running.committed_checkpoint(src_a), Some(&cp_a));
    }

    /// Duplicate ack does not corrupt state.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_duplicate_ack_harmless() {
        let mut running: PipelineMachine<Running, u64> =
            PipelineMachine::<Starting, u64>::new().start();
        let src = SourceId(0);

        let cp: u64 = kani::any();
        let t1 = running.create_batch(src, cp);
        let s1 = running.begin_send(t1);
        let receipt = s1.ack();

        let a1 = running.apply_ack(receipt);
        assert!(a1.advanced);
        assert_eq!(a1.checkpoint, Some(cp));

        // Fabricate duplicate
        let duplicate = AckReceipt {
            batch_id: BatchId(0),
            source: src,
            checkpoint: kani::any(),
            delivered: true,
        };
        let a2 = running.apply_ack(duplicate);
        assert!(!a2.advanced);
        assert_eq!(a2.checkpoint, Some(cp));
    }
}

// ---------------------------------------------------------------------------
// Proptest: random event sequences
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum Action {
        Create { source: u32, checkpoint: u64 },
        Ack { source: u32 },
        Fail { source: u32 },
        Reject { source: u32 },
    }

    fn action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            3 => (0..3u32, 1..10000u64).prop_map(|(s, c)| Action::Create { source: s, checkpoint: c }),
            3 => (0..3u32).prop_map(|s| Action::Ack { source: s }),
            1 => (0..3u32).prop_map(|s| Action::Fail { source: s }),
            1 => (0..3u32).prop_map(|s| Action::Reject { source: s }),
        ]
    }

    proptest! {
        /// Random event sequences: in-flight count is always consistent.
        #[test]
        fn in_flight_consistent(actions in proptest::collection::vec(action_strategy(), 1..50)) {
            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();
            let mut sending: alloc::collections::BTreeMap<u32, alloc::vec::Vec<super::super::BatchTicket<super::super::Sending, u64>>> = alloc::collections::BTreeMap::new();

            for action in actions {
                match action {
                    Action::Create { source, checkpoint } => {
                        let ticket = running.create_batch(SourceId(source), checkpoint);
                        let s = running.begin_send(ticket);
                        sending.entry(source).or_default().push(s);
                    }
                    Action::Ack { source } => {
                        if let Some(queue) = sending.get_mut(&source) {
                            if !queue.is_empty() {
                                let s = queue.remove(0);
                                running.apply_ack(s.ack());
                            }
                        }
                    }
                    Action::Fail { source } => {
                        if let Some(queue) = sending.get_mut(&source) {
                            if !queue.is_empty() {
                                let s = queue.remove(0);
                                let requeued = s.fail();
                                queue.push(running.begin_send(requeued));
                            }
                        }
                    }
                    Action::Reject { source } => {
                        if let Some(queue) = sending.get_mut(&source) {
                            if !queue.is_empty() {
                                let s = queue.remove(0);
                                running.apply_ack(s.reject());
                            }
                        }
                    }
                }
            }

            // in_flight_count matches our tracking
            let our_count: usize = sending.values().map(|v| v.len()).sum();
            prop_assert_eq!(running.in_flight_count(), our_count);
        }

        /// After all batches acked, checkpoint equals last batch per source.
        #[test]
        fn all_acked_reaches_final(
            num_batches in 1..20usize,
            checkpoints in proptest::collection::vec(1..5000u64, 1..20),
            ack_order_seed in proptest::collection::vec(0..1000u32, 1..20),
        ) {
            let n = num_batches.min(checkpoints.len()).min(ack_order_seed.len());
            if n == 0 { return Ok(()); }

            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();
            let src = SourceId(0);

            let mut tickets = alloc::vec::Vec::new();
            for i in 0..n {
                let t = running.create_batch(src, checkpoints[i]);
                tickets.push(running.begin_send(t));
            }
            let expected_final = checkpoints[n - 1];

            // Ack in shuffled order
            let mut indices: alloc::vec::Vec<usize> = (0..n).collect();
            for i in (1..indices.len()).rev() {
                let j = ack_order_seed[i % ack_order_seed.len()] as usize % (i + 1);
                indices.swap(i, j);
            }

            let mut ticket_map: alloc::collections::BTreeMap<usize, super::super::BatchTicket<super::super::Sending, u64>> = alloc::collections::BTreeMap::new();
            for (i, t) in tickets.into_iter().enumerate() {
                ticket_map.insert(i, t);
            }

            for &idx in &indices {
                if let Some(t) = ticket_map.remove(&idx) {
                    running.apply_ack(t.ack());
                }
            }

            prop_assert_eq!(running.committed_checkpoint(src), Some(&expected_final));
            prop_assert_eq!(running.in_flight_count(), 0);
        }

        /// Drain completes after all in-flight batches are acked.
        #[test]
        fn drain_completes(
            num_batches in 1..10usize,
            checkpoints in proptest::collection::vec(1..2000u64, 1..10),
        ) {
            let n = num_batches.min(checkpoints.len());
            if n == 0 { return Ok(()); }

            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();
            let src = SourceId(0);

            let mut sending = alloc::vec::Vec::new();
            for i in 0..n {
                let t = running.create_batch(src, checkpoints[i]);
                sending.push(running.begin_send(t));
            }

            let mut draining = running.begin_drain();
            prop_assert!(!draining.is_drained() || sending.is_empty());

            for s in sending {
                draining.apply_ack(s.ack());
            }

            prop_assert!(draining.is_drained());
            let stopped = draining.stop().ok().expect("should be drained");
            prop_assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), checkpoints[n - 1]);
        }

        /// Multi-source simulation with mixed outcomes.
        #[test]
        fn multi_source_simulation(
            num_sources in 1..4usize,
            batches_per_source in 2..8usize,
            checkpoints in proptest::collection::vec(1..5000u64, 1..32),
            outcomes in proptest::collection::vec(0..10u32, 1..32),
        ) {
            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();

            let mut expected_checkpoints: alloc::collections::BTreeMap<u32, u64> = alloc::collections::BTreeMap::new();
            let mut sending_queues: alloc::collections::BTreeMap<u32, alloc::vec::Vec<super::super::BatchTicket<super::super::Sending, u64>>> = alloc::collections::BTreeMap::new();

            let n = batches_per_source.min(checkpoints.len() / num_sources.max(1));
            if n == 0 { return Ok(()); }

            let mut cp_idx = 0;
            for src_id in 0..num_sources as u32 {
                let src = SourceId(src_id);
                for _ in 0..n {
                    let cp = checkpoints[cp_idx % checkpoints.len()];
                    cp_idx += 1;
                    let ticket = running.create_batch(src, cp);
                    let s = running.begin_send(ticket);
                    sending_queues.entry(src_id).or_default().push(s);
                    expected_checkpoints.insert(src_id, cp);
                }
            }

            let mut outcome_idx = 0;
            for src_id in 0..num_sources as u32 {
                let queue = sending_queues.get_mut(&src_id).unwrap();
                while !queue.is_empty() {
                    let s = queue.remove(0);
                    let outcome = outcomes[outcome_idx % outcomes.len()];
                    outcome_idx += 1;

                    match outcome % 10 {
                        0..=6 => { running.apply_ack(s.ack()); }
                        7..=8 => {
                            let requeued = s.fail();
                            let retried = running.begin_send(requeued);
                            running.apply_ack(retried.ack());
                        }
                        _ => { running.apply_ack(s.reject()); }
                    }
                }
            }

            for src_id in 0..num_sources as u32 {
                let expected = expected_checkpoints[&src_id];
                let actual = running.committed_checkpoint(SourceId(src_id));
                prop_assert_eq!(actual, Some(&expected));
            }

            prop_assert_eq!(running.in_flight_count(), 0);
            let draining = running.begin_drain();
            prop_assert!(draining.is_drained());
        }
    }
}
