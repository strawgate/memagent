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
    /// TRUE if `force_stop()` was used to bypass the drain guard.
    /// Models TLA+ `forced` variable. When true, `DrainCompleteness`
    /// does not hold: in-flight batches may exist at Stopped.
    forced: bool,
    /// Typestate marker.
    _state: PhantomData<S>,
}

/// Result of applying an AckReceipt to the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAdvance<C> {
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
            forced: false,
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
            forced: false,
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
    /// the returned `Sending` ticket without calling `ack()`, `reject()`, or
    /// `fail()` leaves the batch permanently in_flight, blocking drain.
    ///
    /// Consumes the Queued ticket, returns a Sending ticket.
    #[must_use = "Sending ticket must be acked, rejected, or failed — dropping blocks drain forever"]
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
    pub fn apply_ack(&mut self, receipt: AckReceipt<C>) -> CheckpointAdvance<C> {
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
            forced: false,
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
) -> CheckpointAdvance<C> {
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
        return CheckpointAdvance {
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
            let Some(cp) = pending.remove(&lowest_id) else {
                break;
            };
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

    CheckpointAdvance {
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
    pub fn apply_ack(&mut self, receipt: AckReceipt<C>) -> CheckpointAdvance<C> {
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
            forced: false,
            _state: PhantomData,
        })
    }

    /// Emergency shutdown — bypass the drain guard, accept data loss.
    ///
    /// Models TLA+ `ForceStop` action. Transitions to Stopped regardless of
    /// in-flight batches. The `was_forced()` flag records that drain was
    /// incomplete — callers should log the in-flight count for diagnostics.
    ///
    /// Use when the drain timeout expires and the pipeline must stop.
    pub fn force_stop(self) -> PipelineMachine<Stopped, C> {
        PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: self.in_flight,
            pending_acks: self.pending_acks,
            forced: true,
            _state: PhantomData,
        }
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

    /// Whether the pipeline was force-stopped (drain incomplete).
    ///
    /// When true, `DrainCompleteness` does not hold: in-flight batches
    /// may have existed at shutdown. Checkpoints reflect the last
    /// successfully committed state, not the latest delivered data.
    pub fn was_forced(&self) -> bool {
        self.forced
    }

    /// Number of in-flight batches that were abandoned by force_stop.
    /// Zero when `was_forced()` is false.
    pub fn abandoned_count(&self) -> usize {
        self.in_flight.values().map(BTreeMap::len).sum()
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
        assert!(!stopped.was_forced());
        assert_eq!(stopped.abandoned_count(), 0);
    }

    #[test]
    fn force_stop_with_in_flight() {
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let t2 = running.create_batch(src, 2000u64);
        let s1 = running.begin_send(t1);
        let _s2 = running.begin_send(t2); // leave in-flight

        // Ack t1 but leave t2
        running.apply_ack(s1.ack());

        let draining = running.begin_drain();
        assert!(!draining.is_drained());
        assert_eq!(draining.in_flight_count(), 1);

        // stop() should refuse
        let draining = match draining.stop() {
            Err(d) => d,
            Ok(_) => panic!("stop() should refuse with in-flight batches"),
        };

        // force_stop() always succeeds
        let stopped = draining.force_stop();
        assert!(stopped.was_forced());
        assert_eq!(stopped.abandoned_count(), 1);
        // Committed checkpoint is 1000 (t1 was acked), NOT 2000
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), 1000);
    }

    #[test]
    fn force_stop_when_already_drained() {
        let running = new_running();
        let draining = running.begin_drain();
        assert!(draining.is_drained());

        // force_stop on an already-drained machine still works
        let stopped = draining.force_stop();
        assert!(stopped.was_forced()); // forced flag is set regardless
        assert_eq!(stopped.abandoned_count(), 0);
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

        let sending: Vec<_> = (0..100u64)
            .map(|i| {
                let t = running.create_batch(src, (i + 1) * 100);
                running.begin_send(t)
            })
            .collect();
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
        let advance = running.apply_ack(s2.ack());

        // Committed advances to t2's checkpoint — the gap at t1 is invisible
        assert!(advance.advanced);
        assert_eq!(advance.checkpoint, Some(2000));

        // Pipeline can drain and stop — not wedged by the dropped ticket
        assert_eq!(running.in_flight_count(), 0);
        let draining = running.begin_drain();
        assert!(draining.is_drained());
        let stopped = draining.stop().ok().expect("should drain");
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), 2000);
    }

    #[test]
    fn dropped_queued_ticket_gap_in_middle() {
        // Drop the middle ticket — sends t1 and t3, skips t2.
        // Committed should advance past the gap to t3's checkpoint.
        let mut running = new_running();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 1000u64);
        let _t2_dropped = running.create_batch(src, 2000u64);
        let t3 = running.create_batch(src, 3000u64);

        let s1 = running.begin_send(t1);
        let s3 = running.begin_send(t3);

        // Ack t1 — t3 still in-flight, so committed = 1000
        let advance = running.apply_ack(s1.ack());
        assert_eq!(advance.checkpoint, Some(1000));

        // Ack t3 — no lower-ID in-flight; gap at t2 is invisible → advances to 3000
        let advance = running.apply_ack(s3.ack());
        assert!(advance.advanced);
        assert_eq!(advance.checkpoint, Some(3000));

        let draining = running.begin_drain();
        assert!(draining.is_drained());
        let stopped = draining.stop().ok().expect("should drain");
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), 3000);
    }

    #[test]
    fn dropped_queued_ticket_only_one_sent() {
        // Drop first ticket, send only the second — gap is at a lower ID.
        // Committed should advance to the sent ticket's checkpoint without waiting
        // for the dropped ticket.
        let mut running = new_running();
        let src = SourceId(0);

        let _t1_dropped = running.create_batch(src, 1000u64);
        let t2 = running.create_batch(src, 2000u64);
        let t3 = running.create_batch(src, 3000u64);
        let _t4_dropped = running.create_batch(src, 4000u64);

        let s2 = running.begin_send(t2);
        let s3 = running.begin_send(t3);

        // Ack out of order: t3 first
        let advance = running.apply_ack(s3.ack());
        assert!(!advance.advanced, "t2 still in-flight blocks t3");

        // Ack t2 — t1 was never sent so no lower in-flight; t3 pending drains too
        let advance = running.apply_ack(s2.ack());
        assert!(advance.advanced);
        assert_eq!(advance.checkpoint, Some(3000));

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

    // Use u8 instead of u64 for symbolic checkpoint values in Kani proofs.
    // The proofs verify state-machine behavior (ordering, drain, advance),
    // not checkpoint arithmetic. u8 gives 256 symbolic values — sufficient
    // for coverage — while avoiding the SAT explosion that u64 causes in
    // BTreeMap reasoning.
    //
    // Individual proofs further bound checkpoints to small ranges (e.g.,
    // <= 4) when the full u8 domain causes solver timeouts. This is safe
    // because the proofs test state-machine transitions, not value ranges.
    //
    // TLA+ no-impact: PipelineMachine.tla models checkpoints as opaque
    // values. This cfg(kani)-only alias narrows only the Rust proof input
    // domain; it does not change lifecycle transitions for batch sequencing,
    // ACK/reject advance, drain, or checkpoint ordering.
    type Cp = u8;

    /// Ordered ACK: after all batches acked, committed checkpoint equals the last batch's.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_ordered_ack() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp1: Cp = kani::any();
        let cp2: Cp = kani::any();
        let cp3: Cp = kani::any();
        kani::assume(cp1 <= cp2);
        kani::assume(cp2 <= cp3);
        kani::cover!(
            cp1 == cp2 || cp2 == cp3,
            "non-strict checkpoint ordering boundary covered"
        );
        kani::cover!(
            cp1 < cp2 && cp2 < cp3,
            "strictly increasing checkpoints covered"
        );

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
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp1: Cp = kani::any();
        let cp2: Cp = kani::any();

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
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp: Cp = kani::any();
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
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src_a = SourceId(0);
        let src_b = SourceId(1);

        let cp_a: Cp = kani::any();
        let cp_b: Cp = kani::any();

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
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp: Cp = kani::any();
        let t1 = running.create_batch(src, cp);
        let s1 = running.begin_send(t1);
        let receipt = s1.ack();

        let a1 = running.apply_ack(receipt);
        assert!(a1.advanced);
        assert_eq!(a1.checkpoint, Some(cp));

        // Fabricate a duplicate with an arbitrary batch_id — any stale or fabricated
        // receipt must be harmless, not just the specific id=0 case.
        let duplicate = AckReceipt {
            batch_id: BatchId(kani::any()),
            source: src,
            checkpoint: kani::any(),
            delivered: true,
        };
        let a2 = running.apply_ack(duplicate);
        assert!(!a2.advanced);
        assert_eq!(a2.checkpoint, Some(cp));
    }

    /// Symbolic-order ACK with 4 batches: all 24 permutations produce
    /// the correct final checkpoint (cp4).
    ///
    /// Extends the 3-batch proof (all 6 permutations) to 4 batches,
    /// symbolically exploring all orderings via kani::any(). Verifies
    /// the BTreeMap-based pending-ACK queue handles arbitrary depth
    /// and ordering correctly.
    #[kani::proof]
    #[kani::unwind(5)]
    fn verify_symbolic_order_ack_4_batches() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        // Bound checkpoints to <= 4 to keep SAT tractable. The proof
        // verifies ordering logic, not value ranges — 5 distinct values
        // cover equal, less-than, and greater-than cases.
        let cp1: Cp = kani::any();
        kani::assume(cp1 <= 4);
        let cp2: Cp = kani::any();
        kani::assume(cp2 <= 4);
        let cp3: Cp = kani::any();
        kani::assume(cp3 <= 4);
        let cp4: Cp = kani::any();
        kani::assume(cp4 <= 4);

        let t1 = running.create_batch(src, cp1);
        let t2 = running.create_batch(src, cp2);
        let t3 = running.create_batch(src, cp3);
        let t4 = running.create_batch(src, cp4);

        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);
        let s3 = running.begin_send(t3);
        let s4 = running.begin_send(t4);

        // Symbolically explore all 24 orderings (s2n-quic pattern)
        let order: u8 = kani::any_where(|&o: &u8| o < 24);
        let (r_a, r_b, r_c, r_d) = match order {
            0 => (s1.ack(), s2.ack(), s3.ack(), s4.ack()),
            1 => (s1.ack(), s2.ack(), s4.ack(), s3.ack()),
            2 => (s1.ack(), s3.ack(), s2.ack(), s4.ack()),
            3 => (s1.ack(), s3.ack(), s4.ack(), s2.ack()),
            4 => (s1.ack(), s4.ack(), s2.ack(), s3.ack()),
            5 => (s1.ack(), s4.ack(), s3.ack(), s2.ack()),
            6 => (s2.ack(), s1.ack(), s3.ack(), s4.ack()),
            7 => (s2.ack(), s1.ack(), s4.ack(), s3.ack()),
            8 => (s2.ack(), s3.ack(), s1.ack(), s4.ack()),
            9 => (s2.ack(), s3.ack(), s4.ack(), s1.ack()),
            10 => (s2.ack(), s4.ack(), s1.ack(), s3.ack()),
            11 => (s2.ack(), s4.ack(), s3.ack(), s1.ack()),
            12 => (s3.ack(), s1.ack(), s2.ack(), s4.ack()),
            13 => (s3.ack(), s1.ack(), s4.ack(), s2.ack()),
            14 => (s3.ack(), s2.ack(), s1.ack(), s4.ack()),
            15 => (s3.ack(), s2.ack(), s4.ack(), s1.ack()),
            16 => (s3.ack(), s4.ack(), s1.ack(), s2.ack()),
            17 => (s3.ack(), s4.ack(), s2.ack(), s1.ack()),
            18 => (s4.ack(), s1.ack(), s2.ack(), s3.ack()),
            19 => (s4.ack(), s1.ack(), s3.ack(), s2.ack()),
            20 => (s4.ack(), s2.ack(), s1.ack(), s3.ack()),
            21 => (s4.ack(), s2.ack(), s3.ack(), s1.ack()),
            22 => (s4.ack(), s3.ack(), s1.ack(), s2.ack()),
            _ => (s4.ack(), s3.ack(), s2.ack(), s1.ack()),
        };

        // Assert progression after each ACK: advanced flag, checkpoint, and in_flight_count.
        // With symbolic ordering, intermediate checkpoint depends on which batch was acked.
        let a1 = running.apply_ack(r_a);
        assert_eq!(running.in_flight_count(), 3);
        // First ack only advances if batch 1 (the oldest) was acked
        if a1.advanced {
            assert!(a1.checkpoint.is_some(), "advanced but no checkpoint");
        }

        let _a2 = running.apply_ack(r_b);
        assert_eq!(running.in_flight_count(), 2);

        let _a3 = running.apply_ack(r_c);
        assert_eq!(running.in_flight_count(), 1);

        let a4 = running.apply_ack(r_d);
        // After all 4 acked in any order, checkpoint must be cp4
        assert!(a4.advanced, "final ack must advance checkpoint");
        assert_eq!(a4.checkpoint, Some(cp4));
        assert_eq!(running.in_flight_count(), 0);

        // Verify interesting orderings are reachable
        kani::cover!(order == 0, "in-order ack");
        kani::cover!(order == 23, "fully reversed ack");
    }

    /// Committed checkpoint for a source never decreases.
    ///
    /// Verifies the monotonicity invariant that makes crash recovery safe:
    /// once a checkpoint is committed, it never regresses. Maps to the TLA+
    /// `CommittedMonotonic` property: `[][committed[s]' >= committed[s]]_vars`.
    ///
    /// Checks all 6 ACK orderings for 3 batches — if any ordering caused a
    /// regression Kani would find the violating permutation.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_committed_monotonic() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp1: Cp = kani::any();
        let cp2: Cp = kani::any();
        let cp3: Cp = kani::any();

        let t1 = running.create_batch(src, cp1);
        let t2 = running.create_batch(src, cp2);
        let t3 = running.create_batch(src, cp3);
        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);
        let s3 = running.begin_send(t3);

        let order: u8 = kani::any_where(|&o: &u8| o < 6);
        let (r_a, r_b, r_c) = match order {
            0 => (s1.ack(), s2.ack(), s3.ack()),
            1 => (s1.ack(), s3.ack(), s2.ack()),
            2 => (s2.ack(), s1.ack(), s3.ack()),
            3 => (s2.ack(), s3.ack(), s1.ack()),
            4 => (s3.ack(), s1.ack(), s2.ack()),
            _ => (s3.ack(), s2.ack(), s1.ack()),
        };

        let adv_a = running.apply_ack(r_a);
        let cp_after_a = adv_a.checkpoint;

        let adv_b = running.apply_ack(r_b);
        let cp_after_b = adv_b.checkpoint;

        let adv_c = running.apply_ack(r_c);
        let cp_after_c = adv_c.checkpoint;

        // Checkpoint must not regress between consecutive acks
        if let (Some(a), Some(b)) = (cp_after_a, cp_after_b) {
            assert!(b >= a, "checkpoint regressed after second ack");
        }
        if let (Some(b), Some(c)) = (cp_after_b, cp_after_c) {
            assert!(c >= b, "checkpoint regressed after third ack");
        }

        kani::cover!(adv_a.advanced, "first ack advances checkpoint");
        kani::cover!(!adv_a.advanced, "first ack does not advance (out-of-order)");
    }

    /// stop() returns Err when batches are still in-flight.
    ///
    /// The drain completeness guarantee: the pipeline cannot be stopped
    /// while unresolved batches remain. Maps to TLA+ `DrainCompleteness`:
    /// `(phase = "Stopped" /\ ~forced) => ∀s: in_flight[s] = {}`.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_stop_blocks_when_in_flight() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp: Cp = kani::any();
        let t1 = running.create_batch(src, cp);
        let _s1 = running.begin_send(t1); // leave it in-flight

        let draining = running.begin_drain();
        assert!(
            !draining.is_drained(),
            "one batch in-flight, should not be drained"
        );

        // stop() must refuse — not all batches resolved
        assert!(
            draining.stop().is_err(),
            "stop() must block while in-flight batches remain"
        );
    }

    /// reject() advances the checkpoint just like ack().
    ///
    /// Permanently-undeliverable data must not stall checkpoint progress
    /// forever — at-least-once delivery is weakened to at-most-once for
    /// rejected batches. Maps to TLA+ `RejectBatch` aliased to `AckBatch`.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_reject_advances_checkpoint() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp: Cp = kani::any();
        let t1 = running.create_batch(src, cp);
        let s1 = running.begin_send(t1);

        let receipt = s1.reject();
        assert!(!receipt.delivered(), "reject marks delivered=false");

        let advance = running.apply_ack(receipt);
        assert!(advance.advanced, "reject must advance committed checkpoint");
        assert_eq!(advance.checkpoint, Some(cp));
        assert_eq!(running.in_flight_count(), 0);

        // Can drain immediately after a reject
        let draining = running.begin_drain();
        assert!(draining.is_drained());
        let stopped = draining.stop().ok().expect("should drain");
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), cp);
    }

    /// BatchIds assigned by create_batch are strictly increasing.
    ///
    /// IDs never collide or wrap within a single pipeline instance.
    /// Required for the ordered-ack invariant: a stale duplicate ack
    /// can never be mistaken for a live in-flight batch.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_batch_id_strictly_monotonic() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0);
        let t2 = running.create_batch(src, 0);
        let t3 = running.create_batch(src, 0);

        assert!(t1.id() < t2.id(), "batch IDs must be strictly increasing");
        assert!(t2.id() < t3.id(), "batch IDs must be strictly increasing");
    }

    /// `is_drained()` returns true if and only if `in_flight_count() == 0`.
    ///
    /// The biconditional that makes `stop()` safe: drained ↔ no in-flight batches.
    /// An empty pipeline is immediately drained; a pipeline with in-flight batches
    /// is not. These are two separate proofs (each direction separately).
    #[kani::proof]
    #[kani::unwind(2)]
    fn verify_is_drained_when_empty() {
        // An empty pipeline (never any batches) must be immediately drained.
        let running: PipelineMachine<Running, Cp> = PipelineMachine::<Starting, Cp>::new().start();
        let draining = running.begin_drain();
        assert!(draining.is_drained());
        assert_eq!(draining.in_flight_count(), 0);
        assert!(draining.stop().is_ok());
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_not_drained_with_in_flight() {
        // A pipeline with exactly one unsent-to-acked batch must NOT be drained.
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(kani::any());
        let cp: Cp = kani::any();
        let t1 = running.create_batch(src, cp);
        let _s1 = running.begin_send(t1); // in-flight, not acked

        let draining = running.begin_drain();
        assert!(!draining.is_drained());
        assert_eq!(draining.in_flight_count(), 1);
        assert!(draining.stop().is_err());

        kani::cover!(true, "in-flight batch present at drain time");
    }

    /// Mixed ack and reject: both resolve in-flight batches and advance checkpoint.
    ///
    /// For a sequence of batches where some are acked and some rejected,
    /// the final checkpoint must equal the last batch's checkpoint — reject
    /// does not stall the ordered-ack queue.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_mixed_ack_reject_advances() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp1: Cp = kani::any();
        let cp2: Cp = kani::any();
        let cp3: Cp = kani::any();

        let t1 = running.create_batch(src, cp1);
        let t2 = running.create_batch(src, cp2);
        let t3 = running.create_batch(src, cp3);
        let s1 = running.begin_send(t1);
        let s2 = running.begin_send(t2);
        let s3 = running.begin_send(t3);

        // Symbolically choose which batches are acked vs rejected
        let pattern: u8 = kani::any_where(|&p: &u8| p < 8); // 3 bits
        let outcome1 = pattern & 1 != 0;
        let outcome2 = pattern & 2 != 0;
        let outcome3 = pattern & 4 != 0;

        // Apply in strict order (t1, t2, t3) to avoid ordering complexity
        let r1 = if outcome1 { s1.ack() } else { s1.reject() };
        let r2 = if outcome2 { s2.ack() } else { s2.reject() };
        let r3 = if outcome3 { s3.ack() } else { s3.reject() };

        running.apply_ack(r1);
        running.apply_ack(r2);
        let final_adv = running.apply_ack(r3);

        // All resolved — checkpoint must equal cp3 regardless of ack/reject mix
        assert_eq!(final_adv.checkpoint, Some(cp3));
        assert_eq!(running.in_flight_count(), 0);
    }

    /// force_stop() transitions to Stopped regardless of in-flight state.
    ///
    /// Models TLA+ `ForceStop`: phase = "Draining" → phase = "Stopped",
    /// forced = TRUE. Unlike stop(), does NOT require is_drained().
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_force_stop_always_succeeds() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp: Cp = kani::any();
        let t1 = running.create_batch(src, cp);
        let _s1 = running.begin_send(t1); // leave in-flight

        let draining = running.begin_drain();
        assert!(!draining.is_drained());

        // stop() should refuse — recover the Draining machine from the Err
        let draining = match draining.stop() {
            Err(d) => d,
            Ok(_) => panic!("stop() should refuse with in-flight batches"),
        };

        // force_stop always succeeds on the same machine that stop() refused
        let stopped = draining.force_stop();
        assert!(stopped.was_forced());
        assert_eq!(stopped.abandoned_count(), 1);

        kani::cover!(stopped.was_forced(), "force_stop sets forced flag");
    }

    /// force_stop() preserves committed checkpoints.
    ///
    /// Even under forced shutdown, committed checkpoints reflect the
    /// last contiguously-acked state — they are valid for restart.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_force_stop_preserves_committed() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp1: Cp = kani::any();
        let cp2: Cp = kani::any();

        let t1 = running.create_batch(src, cp1);
        let t2 = running.create_batch(src, cp2);
        let s1 = running.begin_send(t1);
        let _s2 = running.begin_send(t2); // leave t2 in-flight

        // Ack t1, leave t2 in-flight
        let adv = running.apply_ack(s1.ack());
        assert!(adv.advanced);
        assert_eq!(adv.checkpoint, Some(cp1));

        let draining = running.begin_drain();
        let stopped = draining.force_stop();

        // Committed checkpoint is cp1 (the last contiguously acked),
        // NOT cp2 (which was in-flight and abandoned).
        assert_eq!(*stopped.final_checkpoints().get(&src).unwrap(), cp1);
        assert!(stopped.was_forced());
        assert_eq!(stopped.abandoned_count(), 1);
    }

    /// Dropped Queued ticket (created-but-never-sent) does not block
    /// checkpoint advancement or drain.
    ///
    /// This is the gap scenario that broke the TLA+ TypeOK invariant:
    /// batch 1 created, ticket dropped (scan error), batch 2 sent and acked.
    /// Committed must advance to batch 2's checkpoint, and drain must succeed.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_dropped_ticket_does_not_block() {
        let mut running: PipelineMachine<Running, Cp> =
            PipelineMachine::<Starting, Cp>::new().start();
        let src = SourceId(0);

        let cp1: Cp = kani::any();
        kani::assume(cp1 <= 4);
        let cp2: Cp = kani::any();
        kani::assume(cp2 <= 4);

        // Create batch 1 — then DROP the Queued ticket (simulates scan error)
        let _dropped = running.create_batch(src, cp1);
        // Queued ticket goes out of scope — never sent, not tracked

        // Create batch 2 — this one succeeds
        let t2 = running.create_batch(src, cp2);
        let s2 = running.begin_send(t2);

        // Ack batch 2
        let adv = running.apply_ack(s2.ack());

        // Committed must advance to cp2 (batch 1 was never in_flight,
        // so it doesn't block the ordered-ack watermark)
        assert!(adv.advanced);
        assert_eq!(adv.checkpoint, Some(cp2));

        // Drain must succeed — only batch 2 was ever in_flight, and it's acked
        let draining = running.begin_drain();
        assert!(draining.is_drained());
        assert!(draining.stop().is_ok());

        kani::cover!(adv.advanced, "checkpoint advances past dropped ticket");
    }
}

// ---------------------------------------------------------------------------
// Proptest: random event sequences
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::Config as ProptestConfig;

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
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            .. ProptestConfig::default()
        })]
        /// Random event sequences: in-flight count is always consistent.
        #[test]
        fn in_flight_consistent(actions in proptest::collection::vec(action_strategy(), 1..50)) {
            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();
            let mut sending: BTreeMap<u32, Vec<BatchTicket<Sending, u64>>> = BTreeMap::new();

            for action in actions {
                match action {
                    Action::Create { source, checkpoint } => {
                        let ticket = running.create_batch(SourceId(u64::from(source)), checkpoint);
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
            let our_count: usize = sending.values().map(Vec::len).sum();
            prop_assert_eq!(running.in_flight_count(), our_count);
        }

        /// After all batches acked, checkpoint equals last batch per source.
        #[test]
        fn all_acked_reaches_final(
            num_batches in 1..20usize,
            // Fixed-length vecs (length 20) ensure Fisher-Yates uses a distinct seed per
            // swap position with no modular wrapping for any n <= 20.
            checkpoints in proptest::collection::vec(1..5000u64, 20),
            perm_seed in proptest::collection::vec(0..10000usize, 20),
        ) {
            let n = num_batches;

            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();
            let src = SourceId(0);

            let tickets: Vec<_> = checkpoints
                .iter()
                .take(n)
                .map(|&checkpoint| {
                    let t = running.create_batch(src, checkpoint);
                    running.begin_send(t)
                })
                .collect();
            let expected_final = checkpoints[n - 1];

            // Unbiased Fisher-Yates: perm_seed[i] is the swap index for position i.
            // No wrapping because perm_seed.len() == 20 >= n.
            let mut indices: Vec<usize> = (0..n).collect();
            for i in (1..n).rev() {
                let j = perm_seed[i] % (i + 1);
                indices.swap(i, j);
            }

            let mut ticket_map: BTreeMap<usize, BatchTicket<Sending, u64>> = BTreeMap::new();
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

            let sending: Vec<_> = checkpoints
                .iter()
                .take(n)
                .map(|&checkpoint| {
                    let t = running.create_batch(src, checkpoint);
                    running.begin_send(t)
                })
                .collect();

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
            checkpoints in proptest::collection::vec(1..5000u64, 32),
            // Fixed length 32 >= max(num_sources * batches_per_source) = 3*7=21,
            // so outcome_idx never wraps and each batch gets an independent outcome.
            outcomes in proptest::collection::vec(0..10u32, 32),
        ) {
            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();

            let mut expected_checkpoints: BTreeMap<u32, u64> = BTreeMap::new();
            let mut sending_queues: BTreeMap<u32, Vec<BatchTicket<Sending, u64>>> = BTreeMap::new();

            let n = batches_per_source.min(checkpoints.len() / num_sources.max(1));
            if n == 0 { return Ok(()); }

            let mut cp_idx = 0;
            for src_id in 0..num_sources as u32 {
                let src = SourceId(u64::from(src_id));
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
                let actual = running.committed_checkpoint(SourceId(u64::from(src_id)));
                prop_assert_eq!(actual, Some(&expected));
            }

            prop_assert_eq!(running.in_flight_count(), 0);
            let draining = running.begin_drain();
            prop_assert!(draining.is_drained());
        }

        /// Committed checkpoint never decreases when input checkpoints are non-decreasing.
        ///
        /// The pipeline does NOT enforce monotonic input checkpoints — that is a
        /// caller contract (byte offsets always increase as the file is read).
        /// This test upholds that contract and verifies the pipeline preserves
        /// monotonicity through arbitrary ack/reject/fail sequences.
        ///
        /// Maps to TLA+ `CommittedMonotonic`: `[][committed[s]' >= committed[s]]_vars`.
        #[test]
        fn committed_checkpoint_monotonic(
            actions in proptest::collection::vec(
                prop_oneof![
                    3 => (0..3u32).prop_map(|s| Action::Create { source: s, checkpoint: 0 }),
                    3 => (0..3u32).prop_map(|s| Action::Ack { source: s }),
                    1 => (0..3u32).prop_map(|s| Action::Fail { source: s }),
                    1 => (0..3u32).prop_map(|s| Action::Reject { source: s }),
                ],
                1..50,
            )
        ) {
            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();
            let mut sending: BTreeMap<
                u32,
                Vec<BatchTicket<Sending, u64>>,
            > = BTreeMap::new();
            // Monotonically increasing counter per source (simulates advancing byte offset).
            let mut next_checkpoint: BTreeMap<u32, u64> =
                BTreeMap::new();
            // Maximum committed value seen per source — must never decrease.
            let mut max_committed: BTreeMap<u64, u64> =
                BTreeMap::new();

            for action in actions {
                match action {
                    Action::Create { source, checkpoint: _ } => {
                        let cp_counter = next_checkpoint.entry(source).or_insert(0);
                        *cp_counter += 100; // always increasing
                        let cp = *cp_counter;
                        let ticket = running.create_batch(SourceId(u64::from(source)), cp);
                        let s = running.begin_send(ticket);
                        sending.entry(source).or_default().push(s);
                    }
                    Action::Ack { source } => {
                        if let Some(queue) = sending.get_mut(&source) {
                            if !queue.is_empty() {
                                let s = queue.remove(0);
                                let advance = running.apply_ack(s.ack());
                                if advance.advanced {
                                    if let Some(cp) = advance.checkpoint {
                                        let prev = max_committed
                                            .get(&u64::from(source))
                                            .copied()
                                            .unwrap_or(0);
                                        prop_assert!(
                                            cp >= prev,
                                            "ack: checkpoint regressed src {}: {} < {}",
                                            source, cp, prev
                                        );
                                        max_committed.insert(u64::from(source), cp);
                                    }
                                }
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
                                let advance = running.apply_ack(s.reject());
                                if advance.advanced {
                                    if let Some(cp) = advance.checkpoint {
                                        let prev = max_committed
                                            .get(&u64::from(source))
                                            .copied()
                                            .unwrap_or(0);
                                        prop_assert!(
                                            cp >= prev,
                                            "reject: checkpoint regressed src {}: {} < {}",
                                            source, cp, prev
                                        );
                                        max_committed.insert(u64::from(source), cp);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// Acking in any random order produces the same final checkpoint as in-order.
        ///
        /// Extends the 4-batch Kani exhaustive-permutation proof to proptest
        /// with up to 20 batches and arbitrary shuffles. The ordered-ack BTreeMap
        /// must produce the same result regardless of arrival order.
        #[test]
        fn out_of_order_acks_final_matches_ordered(
            num_batches in 1..20usize,
            // Fixed-length vecs (20) avoid Fisher-Yates modular wrapping for any n <= 20.
            checkpoints in proptest::collection::vec(1..10000u64, 20),
            perm_seed in proptest::collection::vec(0..10000usize, 20),
        ) {
            let n = num_batches;

            let mut running: PipelineMachine<Running, u64> =
                PipelineMachine::<Starting, u64>::new().start();
            let src = SourceId(0);

            let tickets: Vec<_> = checkpoints
                .iter()
                .take(n)
                .map(|&checkpoint| {
                    let t = running.create_batch(src, checkpoint);
                    running.begin_send(t)
                })
                .collect();

            let expected_final = checkpoints[n - 1];

            // Unbiased Fisher-Yates: distinct seed per position, no wrapping.
            let mut indices: Vec<usize> = (0..n).collect();
            for i in (1..n).rev() {
                let j = perm_seed[i] % (i + 1);
                indices.swap(i, j);
            }

            let mut ticket_map: BTreeMap<
                usize,
                BatchTicket<Sending, u64>,
            > = BTreeMap::new();
            for (i, t) in tickets.into_iter().enumerate() {
                ticket_map.insert(i, t);
            }

            for &idx in &indices {
                if let Some(t) = ticket_map.remove(&idx) {
                    running.apply_ack(t.ack());
                }
            }

            prop_assert_eq!(
                running.committed_checkpoint(src),
                Some(&expected_final),
                "out-of-order ack produced wrong final checkpoint (n={})",
                n
            );
            prop_assert_eq!(running.in_flight_count(), 0);
        }
    }
}
