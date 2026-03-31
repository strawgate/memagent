//! Pipeline lifecycle state machine — ordered offset tracking.
//!
//! The pipeline flows: Starting → Running → Draining → Stopped.
//! Ordered ACK tracking ensures offsets advance only when ALL prior
//! batches are acknowledged (Filebeat registrar pattern).

use alloc::collections::BTreeMap;
use core::marker::PhantomData;

use super::{AckReceipt, BatchId, BatchTicket, Queued, SourceId};

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
/// Tracks per-source committed offsets and ensures ordered ACK:
/// offset advances only when all prior batches are acked.
///
/// The type parameter `S` is a typestate marker controlling which
/// operations are available in each lifecycle phase.
pub struct PipelineMachine<S> {
    /// Monotonically increasing batch ID counter.
    next_batch_id: u64,
    /// Per-source committed offset (highest contiguously-acked end_offset).
    committed: BTreeMap<SourceId, u64>,
    /// Per-source in-flight batches awaiting ACK (batch_id → end_offset).
    /// Ordered by batch_id so we can find the lowest un-acked batch.
    in_flight: BTreeMap<SourceId, BTreeMap<BatchId, u64>>,
    /// Per-source pending ACKs received out of order (batch_id → end_offset).
    pending_acks: BTreeMap<SourceId, BTreeMap<BatchId, u64>>,
    /// Typestate marker.
    _state: PhantomData<S>,
}

/// Result of applying an AckReceipt to the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAdvance {
    /// The source whose offset advanced.
    pub source: SourceId,
    /// Previous committed offset.
    pub old_offset: u64,
    /// New committed offset (may equal old if out-of-order ack).
    pub new_offset: u64,
}

// ---------------------------------------------------------------------------
// Starting → Running
// ---------------------------------------------------------------------------

impl PipelineMachine<Starting> {
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
    pub fn start(self) -> PipelineMachine<Running> {
        PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: self.in_flight,
            pending_acks: self.pending_acks,
            _state: PhantomData,
        }
    }
}

impl Default for PipelineMachine<Starting> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Running — batch creation + ACK processing
// ---------------------------------------------------------------------------

impl PipelineMachine<Running> {
    /// Create a new batch ticket for the given source.
    ///
    /// Assigns a unique BatchId and registers it in the in-flight set.
    pub fn create_batch(
        &mut self,
        source: SourceId,
        start_offset: u64,
        end_offset: u64,
    ) -> BatchTicket<Queued> {
        let id = BatchId(self.next_batch_id);
        self.next_batch_id += 1;

        // Register in in-flight tracking
        self.in_flight
            .entry(source)
            .or_default()
            .insert(id, end_offset);

        BatchTicket::new(id, source, start_offset, end_offset)
    }

    /// Apply an AckReceipt and advance the committed offset if possible.
    ///
    /// Ordered ACK: the committed offset advances only when all batches
    /// with lower IDs for this source have also been acked. Out-of-order
    /// acks are buffered in `pending_acks`.
    pub fn apply_ack(&mut self, receipt: AckReceipt) -> CommitAdvance {
        record_ack_and_advance(
            &mut self.committed,
            &mut self.in_flight,
            &mut self.pending_acks,
            receipt,
        )
    }

    /// Request graceful shutdown — drain remaining in-flight batches.
    pub fn begin_drain(self) -> PipelineMachine<Draining> {
        PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: self.in_flight,
            pending_acks: self.pending_acks,
            _state: PhantomData,
        }
    }

    /// Number of in-flight batches across all sources.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().map(|m| m.len()).sum()
    }

    /// Committed offset for a source (0 if never committed).
    pub fn committed_offset(&self, source: SourceId) -> u64 {
        self.committed.get(&source).copied().unwrap_or(0)
    }

}

// ---------------------------------------------------------------------------
// Shared helpers (used by both Running and Draining)
// ---------------------------------------------------------------------------

/// Record an ack receipt: remove from in-flight, add to pending, try advance.
fn record_ack_and_advance(
    committed: &mut BTreeMap<SourceId, u64>,
    in_flight: &mut BTreeMap<SourceId, BTreeMap<BatchId, u64>>,
    pending_acks: &mut BTreeMap<SourceId, BTreeMap<BatchId, u64>>,
    receipt: AckReceipt,
) -> CommitAdvance {
    let source = receipt.source;
    let old_offset = committed.get(&source).copied().unwrap_or(0);

    // Find which batch this ack corresponds to
    let batch_id = in_flight
        .get(&source)
        .and_then(|flights| {
            flights
                .iter()
                .find(|&(_, &off)| off == receipt.end_offset)
                .map(|(&bid, _)| bid)
        });

    if let Some(bid) = batch_id {
        if let Some(source_flights) = in_flight.get_mut(&source) {
            source_flights.remove(&bid);
        }
        pending_acks
            .entry(source)
            .or_default()
            .insert(bid, receipt.end_offset);
    }

    // Try to advance committed offset
    let mut current = old_offset;
    if let Some(pending) = pending_acks.get_mut(&source) {
        while let Some((&lowest_id, &offset)) = pending.iter().next() {
            let has_lower = in_flight
                .get(&source)
                .map(|f| f.keys().any(|&id| id < lowest_id))
                .unwrap_or(false);
            if has_lower {
                break;
            }
            current = offset;
            pending.remove(&lowest_id);
        }
    }
    committed.insert(source, current);

    CommitAdvance {
        source,
        old_offset,
        new_offset: current,
    }
}

// ---------------------------------------------------------------------------
// Draining — can still process acks but no new batches
// ---------------------------------------------------------------------------

impl PipelineMachine<Draining> {
    /// Apply an AckReceipt during drain (same logic as Running).
    pub fn apply_ack(&mut self, receipt: AckReceipt) -> CommitAdvance {
        record_ack_and_advance(
            &mut self.committed,
            &mut self.in_flight,
            &mut self.pending_acks,
            receipt,
        )
    }

    /// Check if all in-flight batches have been acked.
    pub fn is_drained(&self) -> bool {
        self.in_flight.values().all(|m| m.is_empty())
            && self.pending_acks.values().all(|m| m.is_empty())
    }

    /// All batches drained — transition to Stopped.
    ///
    /// Panics if there are still in-flight or pending batches.
    pub fn stop(self) -> PipelineMachine<Stopped> {
        debug_assert!(self.is_drained(), "cannot stop with in-flight batches");
        PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: BTreeMap::new(),
            pending_acks: BTreeMap::new(),
            _state: PhantomData,
        }
    }

    /// Number of in-flight batches across all sources.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().map(|m| m.len()).sum()
    }

    /// Committed offset for a source.
    pub fn committed_offset(&self, source: SourceId) -> u64 {
        self.committed.get(&source).copied().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Stopped — terminal state, read-only
// ---------------------------------------------------------------------------

impl PipelineMachine<Stopped> {
    /// Final committed offsets for all sources.
    pub fn final_offsets(&self) -> &BTreeMap<SourceId, u64> {
        &self.committed
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_lifecycle() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();

        let src = SourceId(0);
        let t1 = running.create_batch(src, 0, 1000);
        let t2 = running.create_batch(src, 1000, 2000);
        assert_eq!(running.in_flight_count(), 2);

        // Send and ack batch 1
        let sending1 = t1.begin_send();
        let receipt1 = sending1.ack();
        let advance = running.apply_ack(receipt1);
        assert_eq!(advance.new_offset, 1000);

        // Send and ack batch 2
        let sending2 = t2.begin_send();
        let receipt2 = sending2.ack();
        let advance = running.apply_ack(receipt2);
        assert_eq!(advance.new_offset, 2000);

        assert_eq!(running.in_flight_count(), 0);

        // Drain and stop
        let draining = running.begin_drain();
        assert!(draining.is_drained());
        let stopped = draining.stop();
        assert_eq!(*stopped.final_offsets().get(&src).unwrap(), 2000);
    }

    #[test]
    fn out_of_order_ack() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000);
        let t2 = running.create_batch(src, 1000, 2000);
        let t3 = running.create_batch(src, 2000, 3000);

        let s1 = t1.begin_send();
        let s2 = t2.begin_send();
        let s3 = t3.begin_send();

        // Ack batch 3 first — offset should NOT advance (batch 1,2 still in flight)
        let advance = running.apply_ack(s3.ack());
        assert_eq!(advance.new_offset, 0);

        // Ack batch 1 — offset advances to 1000 only (batch 2 still in flight)
        let advance = running.apply_ack(s1.ack());
        assert_eq!(advance.new_offset, 1000);

        // Ack batch 2 — offset jumps to 3000 (1→2→3 all acked now)
        let advance = running.apply_ack(s2.ack());
        assert_eq!(advance.new_offset, 3000);
    }

    #[test]
    fn retry_then_ack() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000);
        let s1 = t1.begin_send();
        let requeued = s1.fail(); // Transient failure
        assert_eq!(requeued.attempts, 1);

        // Retry — the batch is still in-flight tracking
        let s1_retry = requeued.begin_send();
        let advance = running.apply_ack(s1_retry.ack());
        assert_eq!(advance.new_offset, 1000);
    }

    #[test]
    fn multi_source() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src_a = SourceId(0);
        let src_b = SourceId(1);

        let ta = running.create_batch(src_a, 0, 500);
        let tb = running.create_batch(src_b, 0, 800);

        let sa = ta.begin_send();
        let sb = tb.begin_send();

        // Ack source B first
        let advance_b = running.apply_ack(sb.ack());
        assert_eq!(advance_b.source, src_b);
        assert_eq!(advance_b.new_offset, 800);

        // Source A still at 0
        assert_eq!(running.committed_offset(src_a), 0);

        let advance_a = running.apply_ack(sa.ack());
        assert_eq!(advance_a.new_offset, 500);
    }

    #[test]
    fn drain_with_pending() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000);
        let s1 = t1.begin_send();

        // Begin drain with 1 in-flight
        let mut draining = running.begin_drain();
        assert!(!draining.is_drained());

        // Ack during drain
        let advance = draining.apply_ack(s1.ack());
        assert_eq!(advance.new_offset, 1000);
        assert!(draining.is_drained());

        let stopped = draining.stop();
        assert_eq!(*stopped.final_offsets().get(&src).unwrap(), 1000);
    }

    #[test]
    fn reject_advances_offset() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000);
        let s1 = t1.begin_send();

        // Reject — offset still advances (we accept data loss for malformed data)
        let advance = running.apply_ack(s1.reject());
        assert_eq!(advance.new_offset, 1000);
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// Ordered ACK: offset never goes backwards.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_offset_monotonic() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        // Create 3 batches with increasing offsets
        let off1: u64 = kani::any_where(|&x: &u64| x > 0 && x <= 1000);
        let off2: u64 = kani::any_where(|&x: &u64| x > off1 && x <= 2000);
        let off3: u64 = kani::any_where(|&x: &u64| x > off2 && x <= 3000);

        let t1 = running.create_batch(src, 0, off1);
        let t2 = running.create_batch(src, off1, off2);
        let t3 = running.create_batch(src, off2, off3);

        let s1 = t1.begin_send();
        let s2 = t2.begin_send();
        let s3 = t3.begin_send();

        // Ack in arbitrary order (all 6 permutations covered by kani::any)
        let order: u8 = kani::any_where(|&o: &u8| o < 6);
        let (r_a, r_b, r_c) = match order {
            0 => (s1.ack(), s2.ack(), s3.ack()),
            1 => (s1.ack(), s3.ack(), s2.ack()),
            2 => (s2.ack(), s1.ack(), s3.ack()),
            3 => (s2.ack(), s3.ack(), s1.ack()),
            4 => (s3.ack(), s1.ack(), s2.ack()),
            _ => (s3.ack(), s2.ack(), s1.ack()),
        };

        let a1 = running.apply_ack(r_a);
        let a2 = running.apply_ack(r_b);
        let a3 = running.apply_ack(r_c);

        // Offset is monotonically non-decreasing
        assert!(a1.new_offset >= a1.old_offset);
        assert!(a2.new_offset >= a2.old_offset);
        assert!(a3.new_offset >= a3.old_offset);
        assert!(a2.new_offset >= a1.new_offset);
        assert!(a3.new_offset >= a2.new_offset);

        // Final offset equals the highest batch end
        assert_eq!(a3.new_offset, off3);
    }

    /// After all batches acked, committed offset equals the last batch end.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_all_acked_final_offset() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let end1: u64 = kani::any_where(|&x: &u64| x > 0 && x <= 500);
        let end2: u64 = kani::any_where(|&x: &u64| x > end1 && x <= 1000);

        let t1 = running.create_batch(src, 0, end1);
        let t2 = running.create_batch(src, end1, end2);

        let s1 = t1.begin_send();
        let s2 = t2.begin_send();

        running.apply_ack(s1.ack());
        running.apply_ack(s2.ack());

        assert_eq!(running.committed_offset(src), end2);
        assert_eq!(running.in_flight_count(), 0);
    }

    /// Drain → stop is safe when all batches acked.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_drain_to_stop() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let end: u64 = kani::any_where(|&x: &u64| x > 0 && x <= 1000);
        let t1 = running.create_batch(src, 0, end);
        let s1 = t1.begin_send();

        let mut draining = running.begin_drain();
        draining.apply_ack(s1.ack());

        assert!(draining.is_drained());
        let stopped = draining.stop();
        assert_eq!(*stopped.final_offsets().get(&src).unwrap(), end);
    }

    /// Multi-source: sources are independent.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_multi_source_independence() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src_a = SourceId(0);
        let src_b = SourceId(1);

        let end_a: u64 = kani::any_where(|&x: &u64| x > 0 && x <= 500);
        let end_b: u64 = kani::any_where(|&x: &u64| x > 0 && x <= 500);

        let ta = running.create_batch(src_a, 0, end_a);
        let tb = running.create_batch(src_b, 0, end_b);
        let sa = ta.begin_send();
        let sb = tb.begin_send();

        // Ack B only
        running.apply_ack(sb.ack());

        // A is unaffected
        assert_eq!(running.committed_offset(src_a), 0);
        assert_eq!(running.committed_offset(src_b), end_b);

        // Now ack A
        running.apply_ack(sa.ack());
        assert_eq!(running.committed_offset(src_a), end_a);
    }
}
