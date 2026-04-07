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
}

#[cfg(kani)]
mod kani_proofs {
    use super::{TicketDisposition, default_ticket_disposition};
    use crate::worker_pool::DeliveryOutcome;

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
}
