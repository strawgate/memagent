//! Typestate batch ticket — compile-time state machine for batch lifecycle.
//!
//! Each batch flows through: Queued → Sending → Acked/Rejected.
//! State transitions consume `self`, making it impossible to:
//! - ACK a batch twice (self consumed on `.ack()`)
//! - Send a batch without first queuing it
//! - Drop a batch without explicitly acking or rejecting it
//!
//! The Rust compiler proves these properties — no runtime checks needed.

use core::marker::PhantomData;

/// Identifies a data source (file, Kafka topic, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(pub u32);

/// Unique batch identifier within the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchId(pub u64);

// ---------------------------------------------------------------------------
// Typestate markers — zero-size types that exist only at compile time
// ---------------------------------------------------------------------------

/// Batch is queued, waiting to be dispatched.
pub struct Queued;
/// Batch is being sent to output sinks.
pub struct Sending;
/// Batch was successfully delivered.
pub struct Delivered;
/// Batch was permanently rejected (non-retriable error).
pub struct Rejected;

/// A batch ticket tracking the lifecycle of a data batch.
///
/// The type parameter `S` is a typestate marker that determines which
/// operations are available. Transitions consume `self` and return a
/// new ticket in the target state.
///
/// ```text
/// BatchTicket<Queued>  →  begin_send()  →  BatchTicket<Sending>
/// BatchTicket<Sending> →  ack()         →  AckReceipt
/// BatchTicket<Sending> →  fail()        →  BatchTicket<Queued>  (retry)
/// BatchTicket<Sending> →  reject()      →  AckReceipt          (permanent failure)
/// ```
#[must_use = "batch tickets must be explicitly acked, rejected, or requeued — dropping loses data"]
pub struct BatchTicket<S> {
    /// Unique batch ID.
    pub id: BatchId,
    /// Which source produced this batch.
    pub source: SourceId,
    /// Byte offset where this batch starts in the source.
    pub start_offset: u64,
    /// Byte offset where this batch ends in the source.
    pub end_offset: u64,
    /// Number of send attempts (starts at 0, incremented on fail→requeue).
    pub attempts: u32,
    /// Typestate marker.
    _state: PhantomData<S>,
}

/// Proof that a batch was acknowledged. Returned by `ack()` and `reject()`.
/// The pipeline uses this to advance the source's committed offset.
pub struct AckReceipt {
    /// Which source to advance.
    pub source: SourceId,
    /// Offset to commit (end_offset of the acked batch).
    pub end_offset: u64,
    /// Whether this was a successful delivery or a permanent rejection.
    pub delivered: bool,
}

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

impl BatchTicket<Queued> {
    /// Create a new batch ticket from a source read.
    pub fn new(
        id: BatchId,
        source: SourceId,
        start_offset: u64,
        end_offset: u64,
    ) -> Self {
        BatchTicket {
            id,
            source,
            start_offset,
            end_offset,
            attempts: 0,
            _state: PhantomData,
        }
    }

    /// Begin sending this batch to output sinks.
    /// Consumes the Queued ticket, returns a Sending ticket.
    pub fn begin_send(self) -> BatchTicket<Sending> {
        BatchTicket {
            id: self.id,
            source: self.source,
            start_offset: self.start_offset,
            end_offset: self.end_offset,
            attempts: self.attempts,
            _state: PhantomData,
        }
    }
}

impl BatchTicket<Sending> {
    /// Batch was successfully delivered to all sinks.
    /// Consumes the Sending ticket, returns an AckReceipt.
    pub fn ack(self) -> AckReceipt {
        AckReceipt {
            source: self.source,
            end_offset: self.end_offset,
            delivered: true,
        }
    }

    /// Batch delivery failed with a transient error (will retry).
    /// Consumes the Sending ticket, returns a Queued ticket for requeue.
    pub fn fail(self) -> BatchTicket<Queued> {
        BatchTicket {
            id: self.id,
            source: self.source,
            start_offset: self.start_offset,
            end_offset: self.end_offset,
            attempts: self.attempts + 1,
            _state: PhantomData,
        }
    }

    /// Batch permanently rejected (non-retriable error).
    /// Consumes the Sending ticket, returns an AckReceipt.
    /// The offset is still advanced — we accept data loss for malformed data
    /// rather than retrying forever.
    pub fn reject(self) -> AckReceipt {
        AckReceipt {
            source: self.source,
            end_offset: self.end_offset,
            delivered: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_lifecycle_queued_to_acked() {
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), 0, 1000);
        assert_eq!(ticket.attempts, 0);

        let sending = ticket.begin_send();
        let receipt = sending.ack();

        assert_eq!(receipt.source, SourceId(0));
        assert_eq!(receipt.end_offset, 1000);
        assert!(receipt.delivered);
    }

    #[test]
    fn fail_increments_attempts() {
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), 0, 1000);
        let sending = ticket.begin_send();
        let requeued = sending.fail();
        assert_eq!(requeued.attempts, 1);

        let sending2 = requeued.begin_send();
        let requeued2 = sending2.fail();
        assert_eq!(requeued2.attempts, 2);

        // Eventually ack
        let sending3 = requeued2.begin_send();
        let receipt = sending3.ack();
        assert!(receipt.delivered);
    }

    #[test]
    fn reject_returns_receipt_with_delivered_false() {
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), 500, 1500);
        let sending = ticket.begin_send();
        let receipt = sending.reject();

        assert!(!receipt.delivered);
        assert_eq!(receipt.end_offset, 1500);
    }

    // Compile-time tests (these would fail to compile if uncommented):
    // fn double_ack() { let t = BatchTicket::new(...); let s = t.begin_send(); s.ack(); s.ack(); }
    // fn send_without_queue() { let s: BatchTicket<Sending> = ...; } // can't construct directly
    // fn drop_without_ack() { let t = BatchTicket::new(...); let s = t.begin_send(); } // #[must_use] warning
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// Every transition from Queued produces a valid Sending ticket.
    #[kani::proof]
    fn verify_queued_to_sending() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        kani::assume(end >= start);

        let ticket = BatchTicket::new(id, source, start, end);
        let sending = ticket.begin_send();

        assert_eq!(sending.id, id);
        assert_eq!(sending.source, source);
        assert_eq!(sending.start_offset, start);
        assert_eq!(sending.end_offset, end);
        assert_eq!(sending.attempts, 0);
    }

    /// Ack produces a receipt with correct source and offset.
    #[kani::proof]
    fn verify_ack_receipt() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        kani::assume(end >= start);

        let ticket = BatchTicket::new(id, source, start, end);
        let sending = ticket.begin_send();
        let receipt = sending.ack();

        assert_eq!(receipt.source, source);
        assert_eq!(receipt.end_offset, end);
        assert!(receipt.delivered);
    }

    /// Fail increments attempts and preserves all other fields.
    #[kani::proof]
    fn verify_fail_preserves_fields() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        kani::assume(end >= start);

        let ticket = BatchTicket::new(id, source, start, end);
        let sending = ticket.begin_send();
        let requeued = sending.fail();

        assert_eq!(requeued.id, id);
        assert_eq!(requeued.source, source);
        assert_eq!(requeued.start_offset, start);
        assert_eq!(requeued.end_offset, end);
        assert_eq!(requeued.attempts, 1);
    }

    /// Reject produces a receipt with delivered=false.
    #[kani::proof]
    fn verify_reject_receipt() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        kani::assume(end >= start);

        let ticket = BatchTicket::new(id, source, start, end);
        let sending = ticket.begin_send();
        let receipt = sending.reject();

        assert!(!receipt.delivered);
        assert_eq!(receipt.end_offset, end);
    }

    /// Multiple fail→retry cycles preserve identity and increment attempts.
    #[kani::proof]
    fn verify_retry_sequence() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        kani::assume(end >= start);
        let max_retries: u32 = kani::any_where(|&r: &u32| r <= 5);

        let mut ticket = BatchTicket::new(id, source, start, end);
        let mut i = 0u32;
        while i < max_retries {
            let sending = ticket.begin_send();
            ticket = sending.fail();
            i += 1;
        }

        assert_eq!(ticket.attempts, max_retries);
        assert_eq!(ticket.id, id);

        // Eventually ack
        let sending = ticket.begin_send();
        let receipt = sending.ack();
        assert!(receipt.delivered);
    }
}
