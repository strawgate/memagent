//! Typestate batch ticket — compile-time state machine for batch lifecycle.
//!
//! Each batch flows through: Queued → Sending → Acked/Rejected.
//!
//! A Queued ticket is a lightweight token — the pipeline does NOT track it.
//! Dropping a Queued ticket is safe (nothing is orphaned). The pipeline
//! takes ownership at `begin_send`, after which the batch MUST be acked,
//! rejected, or failed. This matches the industry pattern where tracking
//! begins at the point of ownership transfer to the pipeline.
//!
//! State transitions consume `self`, making it impossible to:
//! - ACK a batch twice (self consumed on `.ack()`)
//! - Send a batch without first registering it (`begin_send` on machine)
//!
//! The checkpoint type `C` is opaque to the pipeline — each input source
//! defines what a checkpoint means (byte offset for files, partition offset
//! for Kafka, cursor string for journald, etc.). The pipeline stores and
//! forwards checkpoints without interpreting them.

use core::marker::PhantomData;

/// Identifies a data source (file, Kafka topic, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(pub u64);

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

/// A batch ticket tracking the lifecycle of a data batch.
///
/// The type parameter `S` is a typestate marker that determines which
/// operations are available. Transitions consume `self` and return a
/// new ticket in the target state.
///
/// The type parameter `C` is the checkpoint type — opaque to the pipeline.
/// For file inputs this is typically `u64` (byte offset). For Kafka it
/// might be `i64` (partition offset). For push sources it can be `()`.
///
/// ```text
/// BatchTicket<Queued, C>  →  begin_send()  →  BatchTicket<Sending, C>
/// BatchTicket<Sending, C> →  ack()         →  AckReceipt<C>
/// BatchTicket<Sending, C> →  fail()        →  BatchTicket<Queued, C>  (retry)
/// BatchTicket<Sending, C> →  reject()      →  AckReceipt<C>          (permanent failure)
/// ```
///
/// Dropping a `BatchTicket<Queued, C>` is safe — the pipeline does not track
/// queued tickets. Only `BatchTicket<Sending, C>` must not be dropped:
/// `PipelineMachine::begin_send` is `#[must_use]` to enforce this.
pub struct BatchTicket<S, C> {
    id: BatchId,
    source: SourceId,
    checkpoint: C,
    attempts: u32,
    _state: PhantomData<S>,
}

impl<S, C> BatchTicket<S, C> {
    /// Unique batch ID.
    pub fn id(&self) -> BatchId {
        self.id
    }
    /// Which source produced this batch.
    pub fn source(&self) -> SourceId {
        self.source
    }
    /// The checkpoint value for this batch (opaque to the pipeline).
    pub fn checkpoint(&self) -> &C {
        &self.checkpoint
    }
    /// Number of send attempts (starts at 0, incremented on fail→requeue).
    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

/// Proof that a batch was acknowledged. Returned by `ack()` and `reject()`.
/// Fields are crate-private to prevent fabrication of receipts.
#[must_use = "AckReceipt must be passed to apply_ack to advance the committed checkpoint"]
pub struct AckReceipt<C> {
    pub(crate) batch_id: BatchId,
    pub(crate) source: SourceId,
    pub(crate) checkpoint: C,
    pub(crate) delivered: bool,
}

impl<C> AckReceipt<C> {
    /// Which batch was acked.
    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }
    /// Which source to advance.
    pub fn source(&self) -> SourceId {
        self.source
    }
    /// The checkpoint value for this batch.
    ///
    /// Informational for the caller — `PipelineMachine::apply_ack` commits
    /// the checkpoint recorded at `PipelineMachine::create_batch` time,
    /// not this value.
    pub fn checkpoint(&self) -> &C {
        &self.checkpoint
    }
    /// Whether this was a successful delivery or a permanent rejection.
    pub fn delivered(&self) -> bool {
        self.delivered
    }
}

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

impl<C> BatchTicket<Queued, C> {
    /// Create a new batch ticket.
    ///
    /// Crate-private: only `PipelineMachine::create_batch` should call this.
    pub(crate) fn new(id: BatchId, source: SourceId, checkpoint: C) -> Self {
        BatchTicket {
            id,
            source,
            checkpoint,
            attempts: 0,
            _state: PhantomData,
        }
    }

    /// Begin sending this batch to output sinks.
    /// Consumes the Queued ticket, returns a Sending ticket.
    pub fn begin_send(self) -> BatchTicket<Sending, C> {
        BatchTicket {
            id: self.id,
            source: self.source,
            checkpoint: self.checkpoint,
            attempts: self.attempts,
            _state: PhantomData,
        }
    }
}

impl<C> BatchTicket<Sending, C> {
    /// Batch was successfully delivered to all sinks.
    /// Consumes the Sending ticket, returns an AckReceipt.
    pub fn ack(self) -> AckReceipt<C> {
        AckReceipt {
            batch_id: self.id,
            source: self.source,
            checkpoint: self.checkpoint,
            delivered: true,
        }
    }

    /// Batch delivery failed with a transient error (will retry).
    /// Consumes the `BatchTicket<Sending, C>`, returns `BatchTicket<Queued, C>` for requeue.
    ///
    /// Retry correlation is preserved: `BatchId` is unchanged, so `PipelineMachine`
    /// continues tracking the same logical batch in `in_flight`. Only `attempts`
    /// is incremented for each fail→requeue transition.
    pub fn fail(self) -> BatchTicket<Queued, C> {
        BatchTicket {
            id: self.id,
            source: self.source,
            checkpoint: self.checkpoint,
            attempts: self.attempts + 1,
            _state: PhantomData,
        }
    }

    /// Batch permanently rejected (non-retriable error).
    /// Consumes the Sending ticket, returns an AckReceipt.
    /// The checkpoint is still committed — we accept data loss for malformed
    /// data rather than retrying forever.
    pub fn reject(self) -> AckReceipt<C> {
        AckReceipt {
            batch_id: self.id,
            source: self.source,
            checkpoint: self.checkpoint,
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
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), 1000u64);
        assert_eq!(ticket.attempts, 0);

        let sending = ticket.begin_send();
        let receipt = sending.ack();

        assert_eq!(receipt.source, SourceId(0));
        assert_eq!(receipt.checkpoint, 1000u64);
        assert!(receipt.delivered);
    }

    #[test]
    fn fail_increments_attempts() {
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), 1000u64);
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
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), 1500u64);
        let sending = ticket.begin_send();
        let receipt = sending.reject();

        assert!(!receipt.delivered);
        assert_eq!(receipt.checkpoint, 1500u64);
    }

    #[test]
    fn unit_checkpoint_for_push_sources() {
        // Push sources (OTLP, syslog) have no checkpoint — use ()
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), ());
        let sending = ticket.begin_send();
        let receipt = sending.ack();
        assert_eq!(receipt.checkpoint, ());
        assert!(receipt.delivered);
    }

    #[test]
    fn string_checkpoint_for_journald() {
        // Journald uses opaque cursor strings
        let cursor = "s=abc;i=42;b=def;m=100;t=200;x=300";
        let ticket = BatchTicket::new(BatchId(1), SourceId(0), cursor);
        let sending = ticket.begin_send();
        let receipt = sending.ack();
        assert_eq!(*receipt.checkpoint(), cursor);
    }
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
        let checkpoint: u64 = kani::any();

        let ticket = BatchTicket::new(id, source, checkpoint);
        let sending = ticket.begin_send();

        assert_eq!(sending.id, id);
        assert_eq!(sending.source, source);
        assert_eq!(sending.checkpoint, checkpoint);
        assert_eq!(sending.attempts, 0);
    }

    /// Ack produces a receipt with correct source and checkpoint.
    #[kani::proof]
    fn verify_ack_receipt() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let checkpoint: u64 = kani::any();

        let ticket = BatchTicket::new(id, source, checkpoint);
        let sending = ticket.begin_send();
        let receipt = sending.ack();

        assert_eq!(receipt.batch_id, id);
        assert_eq!(receipt.source, source);
        assert_eq!(receipt.checkpoint, checkpoint);
        assert!(receipt.delivered);
    }

    /// Fail increments attempts and preserves all other fields.
    #[kani::proof]
    fn verify_fail_preserves_fields() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let checkpoint: u64 = kani::any();

        let ticket = BatchTicket::new(id, source, checkpoint);
        let sending = ticket.begin_send();
        let requeued = sending.fail();

        assert_eq!(requeued.id, id);
        assert_eq!(requeued.source, source);
        assert_eq!(requeued.checkpoint, checkpoint);
        assert_eq!(requeued.attempts, 1);
    }

    /// Reject produces a receipt with delivered=false.
    #[kani::proof]
    fn verify_reject_receipt() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let checkpoint: u64 = kani::any();

        let ticket = BatchTicket::new(id, source, checkpoint);
        let sending = ticket.begin_send();
        let receipt = sending.reject();

        assert_eq!(receipt.batch_id, id);
        assert!(!receipt.delivered);
        assert_eq!(receipt.checkpoint, checkpoint);
    }

    /// Multiple fail→retry cycles preserve identity and increment attempts.
    #[kani::proof]
    #[kani::unwind(7)]
    fn verify_retry_sequence() {
        let id = BatchId(kani::any());
        let source = SourceId(kani::any());
        let checkpoint: u64 = kani::any();
        let max_retries: u32 = kani::any_where(|&r: &u32| r <= 5);

        let mut ticket = BatchTicket::new(id, source, checkpoint);
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

        // Guard vacuity: confirm the loop body executed in at least one explored path.
        // If max_retries were always 0, the proof would only verify the trivial case.
        kani::cover!(max_retries > 0, "at least one retry occurred");
        kani::cover!(max_retries == 5, "maximum retry count reached");
    }
}
