//! Worker delivery outcome -> checkpoint policy mapping.
//!
//! This module keeps checkpoint advancement policy explicit at the
//! worker-to-pipeline seam.

use crate::worker_pool::DeliveryOutcome;

/// How pipeline tickets should be finalized for a worker outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TicketDisposition {
    Ack,
    Reject,
    Hold,
}

/// Delivery outcome -> checkpoint policy for `#1520`.
///
/// Policy:
/// - successful delivery ACKs tickets
/// - explicit permanent rejection rejects tickets and advances the checkpoint
/// - all other failures hold tickets unresolved so the checkpoint does not
///   advance past undelivered data
///
/// Hold is intentionally conservative. The current runtime does not yet retain
/// enough batch payload state to requeue these batches in-process, so the
/// immediate effect is "do not advance; replay on restart if shutdown forces a
/// stop while these tickets remain unresolved."
#[must_use]
pub(super) const fn default_ticket_disposition(outcome: &DeliveryOutcome) -> TicketDisposition {
    match outcome {
        DeliveryOutcome::Delivered => TicketDisposition::Ack,
        DeliveryOutcome::Rejected { .. } => TicketDisposition::Reject,
        DeliveryOutcome::RetryExhausted
        | DeliveryOutcome::TimedOut
        | DeliveryOutcome::PoolClosed
        | DeliveryOutcome::WorkerChannelClosed
        | DeliveryOutcome::NoWorkersAvailable
        | DeliveryOutcome::InternalFailure => TicketDisposition::Hold,
    }
}

#[cfg(test)]
mod tests {
    use super::{TicketDisposition, default_ticket_disposition};
    use crate::worker_pool::DeliveryOutcome;
    use proptest::prelude::*;

    #[derive(Clone, Copy, Debug)]
    enum Event {
        Ack,
        Reject,
        Hold,
    }

    fn apply_event(checkpoint: u64, event: Event) -> u64 {
        match event {
            Event::Ack | Event::Reject => checkpoint.saturating_add(1),
            Event::Hold => checkpoint,
        }
    }

    #[test]
    fn delivered_acks_tickets() {
        assert_eq!(
            default_ticket_disposition(&DeliveryOutcome::Delivered),
            TicketDisposition::Ack
        );
    }

    #[test]
    fn explicit_rejects_advance_checkpoints() {
        let rejected = DeliveryOutcome::Rejected {
            reason: "bad request".to_string(),
        };
        assert_eq!(
            default_ticket_disposition(&rejected),
            TicketDisposition::Reject
        );
    }

    #[test]
    fn control_plane_and_retry_failures_hold_tickets() {
        for outcome in [
            DeliveryOutcome::RetryExhausted,
            DeliveryOutcome::TimedOut,
            DeliveryOutcome::PoolClosed,
            DeliveryOutcome::WorkerChannelClosed,
            DeliveryOutcome::NoWorkersAvailable,
            DeliveryOutcome::InternalFailure,
        ] {
            assert_eq!(
                default_ticket_disposition(&outcome),
                TicketDisposition::Hold
            );
        }
    }

    proptest! {
        #[test]
        fn checkpoint_policy_advances_on_ack_or_reject_and_holds_on_hold(events in proptest::collection::vec(0u8..=2, 0..256)) {
            let mut checkpoint = 0u64;
            let mut previous = checkpoint;

            for raw in events {
                let event = match raw {
                    0 => Event::Ack,
                    1 => Event::Reject,
                    _ => Event::Hold,
                };
                checkpoint = apply_event(checkpoint, event);

                prop_assert!(checkpoint >= previous, "checkpoint must be monotonic");

                if matches!(event, Event::Hold) {
                    prop_assert_eq!(
                        checkpoint,
                        previous,
                        "checkpoint must not advance on hold"
                    );
                }

                if matches!(event, Event::Ack | Event::Reject) {
                    prop_assert!(
                        checkpoint > previous || previous == u64::MAX,
                        "checkpoint must advance on ack or reject (unless saturated)"
                    );
                }

                previous = checkpoint;
            }
        }
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::{TicketDisposition, default_ticket_disposition};
    use crate::worker_pool::DeliveryOutcome;

    /// Model: how a checkpoint advances for a given disposition.
    /// Ack and Reject advance by 1 (saturating); Hold is a no-op.
    fn apply_disposition(checkpoint: u64, disposition: TicketDisposition) -> u64 {
        match disposition {
            TicketDisposition::Ack | TicketDisposition::Reject => checkpoint.saturating_add(1),
            TicketDisposition::Hold => checkpoint,
        }
    }

    // -- Outcome mapping proofs (existing 3) ---------------------------------

    #[kani::proof]
    fn verify_default_ticket_disposition_delivered_acks() {
        assert_eq!(
            default_ticket_disposition(&DeliveryOutcome::Delivered),
            TicketDisposition::Ack
        );
    }

    #[kani::proof]
    fn verify_default_ticket_disposition_rejected_advances() {
        let rejected = DeliveryOutcome::Rejected {
            reason: "bad request".to_owned(),
        };
        assert_eq!(
            default_ticket_disposition(&rejected),
            TicketDisposition::Reject
        );
    }

    #[kani::proof]
    fn verify_default_ticket_disposition_non_terminal_failures_hold() {
        for outcome in [
            DeliveryOutcome::RetryExhausted,
            DeliveryOutcome::TimedOut,
            DeliveryOutcome::PoolClosed,
            DeliveryOutcome::WorkerChannelClosed,
            DeliveryOutcome::NoWorkersAvailable,
            DeliveryOutcome::InternalFailure,
        ] {
            assert_eq!(
                default_ticket_disposition(&outcome),
                TicketDisposition::Hold
            );
        }
    }

    // -- Checkpoint advancement model proofs (new 3) -------------------------

    /// Hold disposition must never advance the checkpoint.
    #[kani::proof]
    fn verify_hold_never_advances_checkpoint() {
        let checkpoint: u64 = kani::any();
        let after = apply_disposition(checkpoint, TicketDisposition::Hold);
        assert_eq!(after, checkpoint, "Hold must not change checkpoint");
        kani::cover!(checkpoint == 0, "hold at zero reachable");
        kani::cover!(checkpoint == u64::MAX, "hold at max reachable");
    }

    /// Ack and Reject are equivalent for checkpoint advancement: both advance
    /// by exactly 1 (saturating at `u64::MAX`).
    #[kani::proof]
    fn verify_ack_reject_terminal_equivalence() {
        let checkpoint: u64 = kani::any();
        let after_ack = apply_disposition(checkpoint, TicketDisposition::Ack);
        let after_reject = apply_disposition(checkpoint, TicketDisposition::Reject);
        assert_eq!(
            after_ack, after_reject,
            "Ack and Reject must advance identically"
        );
        // Both advance by exactly 1 unless saturated.
        if checkpoint < u64::MAX {
            assert_eq!(after_ack, checkpoint + 1);
        } else {
            assert_eq!(after_ack, u64::MAX);
        }
        kani::cover!(checkpoint == 0, "terminal equivalence at zero reachable");
        kani::cover!(
            checkpoint == u64::MAX,
            "terminal equivalence at saturation reachable"
        );
    }

    /// Over a bounded mixed sequence of dispositions the checkpoint is
    /// monotonically non-decreasing and never jumps by more than 1 per event.
    #[kani::proof]
    #[kani::unwind(9)]
    fn verify_mixed_sequence_monotonicity_no_gap() {
        // 8 events is enough to cover all interleaving patterns while staying
        // tractable for the bounded model checker.
        const MAX_EVENTS: usize = 8;
        let len: usize = kani::any();
        kani::assume(len <= MAX_EVENTS);

        let mut checkpoint: u64 = 0;
        let mut i: usize = 0;
        while i < len {
            let previous = checkpoint;
            let tag: u8 = kani::any();
            kani::assume(tag < 3);
            let disposition = match tag {
                0 => TicketDisposition::Ack,
                1 => TicketDisposition::Reject,
                _ => TicketDisposition::Hold,
            };
            checkpoint = apply_disposition(checkpoint, disposition);

            // Monotonicity: never decreases.
            assert!(checkpoint >= previous, "checkpoint must be monotonic");
            // No-gap: advances by at most 1 per event.
            assert!(
                checkpoint <= previous.saturating_add(1),
                "checkpoint must not jump by more than 1"
            );

            i += 1;
        }
        kani::cover!(checkpoint > 0, "at least one advance reachable");
        kani::cover!(checkpoint == 0 && len > 0, "all-hold sequence reachable");
    }
}
