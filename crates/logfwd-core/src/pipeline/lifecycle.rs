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

/// Error returned when creating a batch with invalid offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateBatchError {
    /// The batch range is empty or inverted.
    InvalidRange {
        /// Proposed start offset.
        start_offset: u64,
        /// Proposed end offset.
        end_offset: u64,
    },
    /// The proposed start offset is below the committed offset.
    StartBeforeCommitted {
        /// Source for which the batch was requested.
        source: SourceId,
        /// Proposed start offset.
        start_offset: u64,
        /// Current committed offset for this source.
        committed_offset: u64,
    },
    /// The proposed start offset regresses behind already tracked data.
    StartRegresses {
        /// Source for which the batch was requested.
        source: SourceId,
        /// Proposed start offset.
        start_offset: u64,
        /// Highest known end offset tracked by the machine.
        highest_tracked_end: u64,
    },
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
    ///
    /// Returns `Err` when offsets are invalid or non-monotonic for the source.
    pub fn create_batch(
        &mut self,
        source: SourceId,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<BatchTicket<Queued>, CreateBatchError> {
        if end_offset <= start_offset {
            return Err(CreateBatchError::InvalidRange {
                start_offset,
                end_offset,
            });
        }
        let committed_offset = self.committed.get(&source).copied().unwrap_or(0);
        if start_offset < committed_offset {
            return Err(CreateBatchError::StartBeforeCommitted {
                source,
                start_offset,
                committed_offset,
            });
        }

        let highest_tracked_end = self.highest_tracked_end(source);
        if start_offset < highest_tracked_end {
            return Err(CreateBatchError::StartRegresses {
                source,
                start_offset,
                highest_tracked_end,
            });
        }

        let id = BatchId(self.next_batch_id);
        self.next_batch_id += 1;

        // Register in in-flight tracking
        self.in_flight
            .entry(source)
            .or_default()
            .insert(id, end_offset);

        Ok(BatchTicket::new(id, source, start_offset, end_offset))
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
    ///
    /// Includes batches that have been `fail()`ed and requeued — the
    /// machine considers them in-flight until `apply_ack` is called with
    /// a successful `AckReceipt`. Use this for backpressure (e.g. limit
    /// concurrent sends) but note that retrying batches are counted.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().map(|m| m.len()).sum()
    }

    /// Committed offset for a source (`None` if never committed).
    pub fn committed_offset(&self, source: SourceId) -> Option<u64> {
        self.committed.get(&source).copied()
    }

    fn highest_tracked_end(&self, source: SourceId) -> u64 {
        let committed = self.committed.get(&source).copied().unwrap_or(0);
        let in_flight_max = self
            .in_flight
            .get(&source)
            .and_then(|f| f.values().next_back().copied())
            .unwrap_or(0);
        let pending_max = self
            .pending_acks
            .get(&source)
            .and_then(|p| p.values().next_back().copied())
            .unwrap_or(0);

        committed.max(in_flight_max).max(pending_max)
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
    let batch_id = receipt.batch_id;
    let old_offset = committed.get(&source).copied().unwrap_or(0);

    // Remove from in-flight by exact BatchId (O(log n), no scan needed)
    if let Some(source_flights) = in_flight.get_mut(&source)
        && source_flights.remove(&batch_id).is_some()
    {
        pending_acks
            .entry(source)
            .or_default()
            .insert(batch_id, receipt.end_offset);
    }

    // Try to advance committed offset
    let mut current = old_offset;
    if let Some(pending) = pending_acks.get_mut(&source) {
        while let Some((&lowest_id, &offset)) = pending.iter().next() {
            let has_lower = in_flight
                .get(&source)
                .and_then(|f| f.keys().next().copied())
                .map(|min_id| min_id < lowest_id)
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
    /// Returns `Err(self)` if there are still in-flight or pending batches,
    /// preventing silent data loss in release builds.
    pub fn stop(self) -> Result<PipelineMachine<Stopped>, Self> {
        if !self.is_drained() {
            return Err(self);
        }
        Ok(PipelineMachine {
            next_batch_id: self.next_batch_id,
            committed: self.committed,
            in_flight: BTreeMap::new(),
            pending_acks: BTreeMap::new(),
            _state: PhantomData,
        })
    }

    /// Number of in-flight batches across all sources.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().map(|m| m.len()).sum()
    }

    /// Committed offset for a source (`None` if never committed).
    pub fn committed_offset(&self, source: SourceId) -> Option<u64> {
        self.committed.get(&source).copied()
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
        let t1 = running.create_batch(src, 0, 1000).expect("valid batch");
        let t2 = running.create_batch(src, 1000, 2000).expect("valid batch");
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
        let stopped = draining.stop().ok().expect("should be drained");
        assert_eq!(*stopped.final_offsets().get(&src).unwrap(), 2000);
    }

    #[test]
    fn out_of_order_ack() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000).expect("valid batch");
        let t2 = running.create_batch(src, 1000, 2000).expect("valid batch");
        let t3 = running.create_batch(src, 2000, 3000).expect("valid batch");

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

        let t1 = running.create_batch(src, 0, 1000).expect("valid batch");
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

        let ta = running.create_batch(src_a, 0, 500).expect("valid batch");
        let tb = running.create_batch(src_b, 0, 800).expect("valid batch");

        let sa = ta.begin_send();
        let sb = tb.begin_send();

        // Ack source B first
        let advance_b = running.apply_ack(sb.ack());
        assert_eq!(advance_b.source, src_b);
        assert_eq!(advance_b.new_offset, 800);

        // Source A still at 0
        assert_eq!(running.committed_offset(src_a), None);

        let advance_a = running.apply_ack(sa.ack());
        assert_eq!(advance_a.new_offset, 500);
    }

    #[test]
    fn drain_with_pending() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000).expect("valid batch");
        let s1 = t1.begin_send();

        // Begin drain with 1 in-flight
        let mut draining = running.begin_drain();
        assert!(!draining.is_drained());

        // Ack during drain
        let advance = draining.apply_ack(s1.ack());
        assert_eq!(advance.new_offset, 1000);
        assert!(draining.is_drained());

        let stopped = draining.stop().ok().expect("should be drained");
        assert_eq!(*stopped.final_offsets().get(&src).unwrap(), 1000);
    }

    #[test]
    fn reject_advances_offset() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000).expect("valid batch");
        let s1 = t1.begin_send();

        // Reject — offset still advances (we accept data loss for malformed data)
        let advance = running.apply_ack(s1.reject());
        assert_eq!(advance.new_offset, 1000);
    }

    #[test]
    fn mixed_ack_and_reject() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000).expect("valid batch");
        let t2 = running.create_batch(src, 1000, 2000).expect("valid batch");
        let t3 = running.create_batch(src, 2000, 3000).expect("valid batch");

        let s1 = t1.begin_send();
        let s2 = t2.begin_send();
        let s3 = t3.begin_send();

        // Reject batch 2 (malformed), ack 3, then ack 1
        running.apply_ack(s2.reject());
        running.apply_ack(s3.ack());
        let advance = running.apply_ack(s1.ack());

        // All three resolved — offset should be at 3000
        assert_eq!(advance.new_offset, 3000);
        assert_eq!(running.in_flight_count(), 0);
    }

    #[test]
    fn stop_before_drained_returns_err() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let t1 = running.create_batch(src, 0, 1000).expect("valid batch");
        let _s1 = t1.begin_send();

        let draining = running.begin_drain();
        assert!(!draining.is_drained());

        // stop() returns Err because there's still an in-flight batch
        let draining = draining.stop().err().expect("should fail: not drained");
        assert_eq!(draining.in_flight_count(), 1);
    }

    #[test]
    fn create_batch_rejects_zero_size() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        // end_offset == start_offset should return InvalidRange
        let err = running
            .create_batch(SourceId(0), 100, 100)
            .err()
            .expect("must reject empty range");
        assert_eq!(
            err,
            CreateBatchError::InvalidRange {
                start_offset: 100,
                end_offset: 100
            }
        );
    }

    #[test]
    fn create_batch_rejects_inverted() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        // end_offset < start_offset should return InvalidRange
        let err = running
            .create_batch(SourceId(0), 200, 100)
            .err()
            .expect("must reject inverted range");
        assert_eq!(
            err,
            CreateBatchError::InvalidRange {
                start_offset: 200,
                end_offset: 100
            }
        );
    }

    #[test]
    fn create_batch_rejects_regressing_source_offset() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(42);

        let first = running
            .create_batch(src, 0, 100)
            .expect("first batch must be valid");
        let _ = first.begin_send();

        let err = running
            .create_batch(src, 50, 150)
            .err()
            .expect("must reject regressing source offset");
        assert_eq!(
            err,
            CreateBatchError::StartRegresses {
                source: src,
                start_offset: 50,
                highest_tracked_end: 100
            }
        );
    }

    #[test]
    fn no_batches_drain_immediate() {
        let machine = PipelineMachine::new();
        let running = machine.start();

        // Drain with nothing in flight
        let draining = running.begin_drain();
        assert!(draining.is_drained());
        let _stopped = draining.stop();
    }

    #[test]
    fn default_impl() {
        let machine: PipelineMachine<Starting> = PipelineMachine::default();
        let running = machine.start();
        assert_eq!(running.in_flight_count(), 0);
        assert_eq!(running.committed_offset(SourceId(0)), None);
    }

    #[test]
    fn high_batch_count_single_source() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        // Create 100 batches
        let mut sending = alloc::vec::Vec::new();
        for i in 0..100u64 {
            let t = running
                .create_batch(src, i * 100, (i + 1) * 100)
                .expect("valid batch");
            sending.push(t.begin_send());
        }
        assert_eq!(running.in_flight_count(), 100);

        // Ack in reverse order — offset should not advance until batch 0 is acked
        for s in sending.into_iter().rev() {
            running.apply_ack(s.ack());
        }

        // After all acked, offset should be at 10000
        assert_eq!(running.committed_offset(src), Some(10000));
        assert_eq!(running.in_flight_count(), 0);
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

        let t1 = running.create_batch(src, 0, off1).expect("valid batch");
        let t2 = running.create_batch(src, off1, off2).expect("valid batch");
        let t3 = running.create_batch(src, off2, off3).expect("valid batch");

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

        let t1 = running.create_batch(src, 0, end1).expect("valid batch");
        let t2 = running.create_batch(src, end1, end2).expect("valid batch");

        let s1 = t1.begin_send();
        let s2 = t2.begin_send();

        running.apply_ack(s1.ack());
        running.apply_ack(s2.ack());

        assert_eq!(running.committed_offset(src), Some(end2));
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
        let t1 = running.create_batch(src, 0, end).expect("valid batch");
        let s1 = t1.begin_send();

        let mut draining = running.begin_drain();
        draining.apply_ack(s1.ack());

        assert!(draining.is_drained());
        let stopped = draining.stop().ok().expect("should be drained");
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

        let ta = running.create_batch(src_a, 0, end_a).expect("valid batch");
        let tb = running.create_batch(src_b, 0, end_b).expect("valid batch");
        let sa = ta.begin_send();
        let sb = tb.begin_send();

        // Ack B only
        running.apply_ack(sb.ack());

        // A is unaffected
        assert_eq!(running.committed_offset(src_a), None);
        assert_eq!(running.committed_offset(src_b), Some(end_b));

        // Now ack A
        running.apply_ack(sa.ack());
        assert_eq!(running.committed_offset(src_a), Some(end_a));
    }

    /// Duplicate ack (same batch acked twice) does not regress offset.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_duplicate_ack_harmless() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let end: u64 = kani::any_where(|&x: &u64| x > 0 && x <= 1000);
        let t1 = running.create_batch(src, 0, end).expect("valid batch");
        let s1 = t1.begin_send();
        let receipt = s1.ack();

        // Legitimate ack
        let a1 = running.apply_ack(receipt);
        assert_eq!(a1.new_offset, end);

        // Fabricate a duplicate receipt (same batch_id + source + end_offset)
        let duplicate = AckReceipt {
            batch_id: BatchId(0),
            source: src,
            end_offset: end,
            delivered: true,
        };
        let a2 = running.apply_ack(duplicate);

        // Offset must not regress
        assert!(a2.new_offset >= a1.new_offset);
        // And should still equal end (no spurious advancement)
        assert_eq!(a2.new_offset, end);
    }

    /// stop() on a fully-drained machine preserves offsets.
    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_stop_preserves_offsets() {
        let machine = PipelineMachine::new();
        let mut running = machine.start();
        let src = SourceId(0);

        let end: u64 = kani::any_where(|&x: &u64| x > 0 && x <= 1000);
        let t1 = running.create_batch(src, 0, end).expect("valid batch");
        let s1 = t1.begin_send();
        running.apply_ack(s1.ack());

        let draining = running.begin_drain();
        assert!(draining.is_drained());
        let stopped = draining.stop().ok().expect("should be drained");
        assert_eq!(*stopped.final_offsets().get(&src).unwrap(), end);
    }
}

// ---------------------------------------------------------------------------
// Proptest: random event sequences
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Actions that can happen in a pipeline.
    #[derive(Debug, Clone)]
    enum Action {
        /// Create a new batch on a source with given size.
        Create { source: u32, size: u64 },
        /// Ack the oldest sending batch for a source.
        Ack { source: u32 },
        /// Fail (retry) the oldest sending batch for a source.
        Fail { source: u32 },
        /// Reject the oldest sending batch for a source.
        Reject { source: u32 },
    }

    fn action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            // Bias toward create + ack (common path)
            3 => (0..3u32, 1..10000u64).prop_map(|(s, sz)| Action::Create { source: s, size: sz }),
            3 => (0..3u32).prop_map(|s| Action::Ack { source: s }),
            1 => (0..3u32).prop_map(|s| Action::Fail { source: s }),
            1 => (0..3u32).prop_map(|s| Action::Reject { source: s }),
        ]
    }

    proptest! {
        /// Random event sequences maintain offset monotonicity.
        #[test]
        fn offset_never_decreases(actions in proptest::collection::vec(action_strategy(), 1..50)) {
            let machine = PipelineMachine::new();
            let mut running = machine.start();

            // Track per-source state
            let mut source_offsets: alloc::collections::BTreeMap<u32, u64> = alloc::collections::BTreeMap::new();
            // Batches in Sending state, keyed by source
            let mut sending: alloc::collections::BTreeMap<u32, alloc::vec::Vec<super::super::BatchTicket<super::super::Sending>>> = alloc::collections::BTreeMap::new();

            for action in actions {
                match action {
                    Action::Create { source, size } => {
                        let src = SourceId(source);
                        let start = source_offsets.get(&source).copied().unwrap_or(0);
                        // Use running create_batch offset as the next write position
                        let end = start + size;
                        *source_offsets.entry(source).or_insert(0) = end;
                        let ticket = running.create_batch(src, start, end).expect("valid batch");
                        let s = ticket.begin_send();
                        sending.entry(source).or_default().push(s);
                    }
                    Action::Ack { source } => {
                        if let Some(queue) = sending.get_mut(&source) {
                            if !queue.is_empty() {
                                let s = queue.remove(0);
                                let old = running.committed_offset(SourceId(source)).unwrap_or(0);
                                let advance = running.apply_ack(s.ack());
                                // Offset never decreases
                                prop_assert!(advance.new_offset >= old,
                                    "offset decreased: {} -> {}", old, advance.new_offset);
                            }
                        }
                    }
                    Action::Fail { source } => {
                        if let Some(queue) = sending.get_mut(&source) {
                            if !queue.is_empty() {
                                let s = queue.remove(0);
                                let requeued = s.fail();
                                // Re-send immediately
                                let s2 = requeued.begin_send();
                                queue.push(s2);
                            }
                        }
                    }
                    Action::Reject { source } => {
                        if let Some(queue) = sending.get_mut(&source) {
                            if !queue.is_empty() {
                                let s = queue.remove(0);
                                let old = running.committed_offset(SourceId(source)).unwrap_or(0);
                                let advance = running.apply_ack(s.reject());
                                prop_assert!(advance.new_offset >= old,
                                    "offset decreased on reject: {} -> {}", old, advance.new_offset);
                            }
                        }
                    }
                }
            }
        }

        /// After all batches acked, committed offset equals highest end_offset.
        #[test]
        fn all_acked_reaches_final(
            num_batches in 1..20usize,
            batch_sizes in proptest::collection::vec(1..5000u64, 1..20),
            ack_order_seed in proptest::collection::vec(0..1000u32, 1..20),
        ) {
            let n = num_batches.min(batch_sizes.len()).min(ack_order_seed.len());
            if n == 0 { return Ok(()); }

            let machine = PipelineMachine::new();
            let mut running = machine.start();
            let src = SourceId(0);

            // Create N batches with contiguous offsets
            let mut tickets = alloc::vec::Vec::new();
            let mut offset = 0u64;
            for i in 0..n {
                let end = offset + batch_sizes[i];
                let t = running.create_batch(src, offset, end).expect("valid batch");
                tickets.push(t.begin_send());
                offset = end;
            }
            let expected_final = offset;

            // Ack in shuffled order (use seed to create permutation)
            let mut indices: alloc::vec::Vec<usize> = (0..n).collect();
            // Simple Fisher-Yates with deterministic seeds
            for i in (1..indices.len()).rev() {
                let j = ack_order_seed[i % ack_order_seed.len()] as usize % (i + 1);
                indices.swap(i, j);
            }

            // We need to ack by pulling from the tickets vec in permuted order.
            // Since we can't index-remove efficiently, collect into a map.
            let mut ticket_map: alloc::collections::BTreeMap<usize, super::super::BatchTicket<super::super::Sending>> = alloc::collections::BTreeMap::new();
            for (i, t) in tickets.into_iter().enumerate() {
                ticket_map.insert(i, t);
            }

            for &idx in &indices {
                if let Some(t) = ticket_map.remove(&idx) {
                    running.apply_ack(t.ack());
                }
            }

            prop_assert_eq!(running.committed_offset(src), Some(expected_final),
                "committed {} != expected {} after acking all {} batches",
                running.committed_offset(src).unwrap_or(0), expected_final, n);
            prop_assert_eq!(running.in_flight_count(), 0);
        }

        /// Drain completes after all in-flight batches are acked.
        #[test]
        fn drain_completes(
            num_batches in 1..10usize,
            batch_sizes in proptest::collection::vec(1..2000u64, 1..10),
        ) {
            let n = num_batches.min(batch_sizes.len());
            if n == 0 { return Ok(()); }

            let machine = PipelineMachine::new();
            let mut running = machine.start();
            let src = SourceId(0);

            let mut sending = alloc::vec::Vec::new();
            let mut offset = 0u64;
            for i in 0..n {
                let end = offset + batch_sizes[i];
                let t = running.create_batch(src, offset, end).expect("valid batch");
                sending.push(t.begin_send());
                offset = end;
            }

            let mut draining = running.begin_drain();
            prop_assert!(!draining.is_drained() || sending.is_empty());

            // Ack all during drain
            for s in sending {
                draining.apply_ack(s.ack());
            }

            prop_assert!(draining.is_drained());
            let stopped = draining.stop().ok().expect("should be drained");
            prop_assert_eq!(*stopped.final_offsets().get(&src).unwrap(), offset);
        }

        /// Multi-source pipeline simulation: create, send, mixed outcomes.
        /// Verifies that the state machine correctly tracks independent
        /// sources through a realistic interleaved workload.
        #[test]
        fn multi_source_simulation(
            num_sources in 1..4usize,
            batches_per_source in 2..8usize,
            batch_sizes in proptest::collection::vec(1..5000u64, 1..32),
            outcomes in proptest::collection::vec(0..10u32, 1..32),
        ) {
            let machine = PipelineMachine::new();
            let mut running = machine.start();

            // Track per-source expected final offset
            let mut expected_offsets: alloc::collections::BTreeMap<u32, u64> = alloc::collections::BTreeMap::new();
            let mut sending_queues: alloc::collections::BTreeMap<u32, alloc::vec::Vec<super::super::BatchTicket<super::super::Sending>>> = alloc::collections::BTreeMap::new();

            let n = batches_per_source.min(batch_sizes.len() / num_sources.max(1));
            if n == 0 { return Ok(()); }

            // Phase 1: Create batches across all sources
            let mut size_idx = 0;
            for src_id in 0..num_sources as u32 {
                let src = SourceId(src_id);
                let mut offset = 0u64;
                for _ in 0..n {
                    let sz = batch_sizes[size_idx % batch_sizes.len()];
                    size_idx += 1;
                    let end = offset + sz;
                    let ticket = running.create_batch(src, offset, end).expect("valid batch");
                    sending_queues.entry(src_id).or_default().push(ticket.begin_send());
                    offset = end;
                }
                expected_offsets.insert(src_id, offset);
            }

            // Phase 2: Process outcomes (ack, fail+retry, reject)
            let mut outcome_idx = 0;
            for src_id in 0..num_sources as u32 {
                let queue = sending_queues.get_mut(&src_id).unwrap();
                while !queue.is_empty() {
                    let s = queue.remove(0);
                    let outcome = outcomes[outcome_idx % outcomes.len()];
                    outcome_idx += 1;

                    match outcome % 10 {
                        0..=6 => {
                            // Ack (70% of the time)
                            running.apply_ack(s.ack());
                        }
                        7..=8 => {
                            // Fail and retry (20%)
                            let requeued = s.fail();
                            let retried = requeued.begin_send();
                            running.apply_ack(retried.ack());
                        }
                        _ => {
                            // Reject (10%)
                            running.apply_ack(s.reject());
                        }
                    }
                }
            }

            // Phase 3: Verify all offsets reached expected values
            for src_id in 0..num_sources as u32 {
                let expected = expected_offsets[&src_id];
                let actual = running.committed_offset(SourceId(src_id)).unwrap_or(0);
                prop_assert_eq!(actual, expected,
                    "source {} committed {} != expected {}", src_id, actual, expected);
            }

            // Phase 4: Drain and stop
            prop_assert_eq!(running.in_flight_count(), 0);
            let draining = running.begin_drain();
            prop_assert!(draining.is_drained());
            let stopped = draining.stop().ok().expect("should be drained");

            for src_id in 0..num_sources as u32 {
                let expected = expected_offsets[&src_id];
                prop_assert_eq!(
                    *stopped.final_offsets().get(&SourceId(src_id)).unwrap(),
                    expected
                );
            }
        }
    }
}
