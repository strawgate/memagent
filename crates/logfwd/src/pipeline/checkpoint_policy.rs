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
}

/// Compatibility policy for `#1520`.
///
/// Current behavior is preserved:
/// - successful delivery ACKs tickets
/// - all non-delivery outcomes reject tickets (checkpoint still advances via
///   `BatchTicket::reject` semantics)
#[must_use]
pub(super) const fn default_ticket_disposition(outcome: &DeliveryOutcome) -> TicketDisposition {
    match outcome {
        DeliveryOutcome::Delivered => TicketDisposition::Ack,
        DeliveryOutcome::Rejected { .. }
        | DeliveryOutcome::RetryExhausted
        | DeliveryOutcome::TimedOut
        | DeliveryOutcome::PoolClosed
        | DeliveryOutcome::WorkerChannelClosed
        | DeliveryOutcome::NoWorkersAvailable
        | DeliveryOutcome::InternalFailure => TicketDisposition::Reject,
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
    fn all_non_delivery_outcomes_reject_tickets_in_compat_mode() {
        let rejected = DeliveryOutcome::Rejected {
            reason: "bad request".to_string(),
        };
        for outcome in [
            rejected,
            DeliveryOutcome::RetryExhausted,
            DeliveryOutcome::TimedOut,
            DeliveryOutcome::PoolClosed,
            DeliveryOutcome::WorkerChannelClosed,
            DeliveryOutcome::NoWorkersAvailable,
            DeliveryOutcome::InternalFailure,
        ] {
            assert_eq!(
                default_ticket_disposition(&outcome),
                TicketDisposition::Reject
            );
        }
    }
}
