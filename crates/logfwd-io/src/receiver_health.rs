//! Pure lifecycle policy for standalone HTTP receivers.
//!
//! This module keeps receiver health semantics explicit and Kani-friendly.

use logfwd_types::diagnostics::ComponentHealth;

/// Lifecycle events that can update standalone receiver health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReceiverHealthEvent {
    /// A request delivered non-empty data into the pipeline successfully.
    DeliveryAccepted,
    /// A request completed successfully but did not deliver any rows.
    DeliveryNoop,
    /// The pipeline or bounded queue is applying backpressure.
    Backpressure,
    /// The receiver hit a fatal runtime condition.
    FatalFailure,
    /// Shutdown has started.
    ShutdownRequested,
    /// Shutdown has completed.
    ShutdownCompleted,
}

/// Reduce a current health value and lifecycle event into the next value.
///
/// Terminal states are sticky: once a standalone receiver is stopping,
/// stopped, or failed, later data-path events do not resurrect it.
#[must_use]
pub(crate) const fn reduce_receiver_health(
    current: ComponentHealth,
    event: ReceiverHealthEvent,
) -> ComponentHealth {
    match event {
        ReceiverHealthEvent::DeliveryAccepted => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => ComponentHealth::Healthy,
        },
        ReceiverHealthEvent::DeliveryNoop => current,
        ReceiverHealthEvent::Backpressure => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => ComponentHealth::Degraded,
        },
        ReceiverHealthEvent::FatalFailure => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped => current,
            _ => ComponentHealth::Failed,
        },
        ReceiverHealthEvent::ShutdownRequested => match current {
            ComponentHealth::Stopped | ComponentHealth::Failed => current,
            _ => ComponentHealth::Stopping,
        },
        ReceiverHealthEvent::ShutdownCompleted => match current {
            ComponentHealth::Failed => ComponentHealth::Failed,
            _ => ComponentHealth::Stopped,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{ReceiverHealthEvent, reduce_receiver_health};
    use logfwd_types::diagnostics::ComponentHealth;
    use proptest::prelude::*;

    #[test]
    fn noop_success_does_not_heal_degraded_or_failed() {
        assert_eq!(
            reduce_receiver_health(ComponentHealth::Degraded, ReceiverHealthEvent::DeliveryNoop),
            ComponentHealth::Degraded
        );
        assert_eq!(
            reduce_receiver_health(ComponentHealth::Failed, ReceiverHealthEvent::DeliveryNoop),
            ComponentHealth::Failed
        );
    }

    #[test]
    fn accepted_delivery_recovers_non_terminal_receivers() {
        assert_eq!(
            reduce_receiver_health(
                ComponentHealth::Degraded,
                ReceiverHealthEvent::DeliveryAccepted
            ),
            ComponentHealth::Healthy
        );
        assert_eq!(
            reduce_receiver_health(
                ComponentHealth::Failed,
                ReceiverHealthEvent::DeliveryAccepted
            ),
            ComponentHealth::Failed
        );
    }

    #[test]
    fn fatal_failure_preserves_drain_phase() {
        assert_eq!(
            reduce_receiver_health(ComponentHealth::Stopping, ReceiverHealthEvent::FatalFailure),
            ComponentHealth::Stopping
        );
        assert_eq!(
            reduce_receiver_health(ComponentHealth::Healthy, ReceiverHealthEvent::FatalFailure),
            ComponentHealth::Failed
        );
    }

    fn apply_events(initial: ComponentHealth, events: &[ReceiverHealthEvent]) -> ComponentHealth {
        events.iter().copied().fold(initial, reduce_receiver_health)
    }

    fn arb_event() -> impl Strategy<Value = ReceiverHealthEvent> {
        prop_oneof![
            Just(ReceiverHealthEvent::DeliveryAccepted),
            Just(ReceiverHealthEvent::DeliveryNoop),
            Just(ReceiverHealthEvent::Backpressure),
            Just(ReceiverHealthEvent::FatalFailure),
            Just(ReceiverHealthEvent::ShutdownRequested),
            Just(ReceiverHealthEvent::ShutdownCompleted),
        ]
    }

    proptest! {
        #[test]
        fn failed_state_is_sticky_across_any_event_sequence(
            events in proptest::collection::vec(arb_event(), 0..16)
        ) {
            let out = apply_events(ComponentHealth::Failed, &events);
            prop_assert_eq!(out, ComponentHealth::Failed);
        }

        #[test]
        fn stopped_state_is_sticky_across_any_event_sequence(
            events in proptest::collection::vec(arb_event(), 0..16)
        ) {
            let out = apply_events(ComponentHealth::Stopped, &events);
            prop_assert_eq!(out, ComponentHealth::Stopped);
        }

        #[test]
        fn stopping_only_advances_to_stopped_on_explicit_shutdown_completion(
            events in proptest::collection::vec(arb_event(), 0..16)
        ) {
            let out = apply_events(ComponentHealth::Stopping, &events);
            if events.contains(&ReceiverHealthEvent::ShutdownCompleted) {
                prop_assert!(matches!(out, ComponentHealth::Stopping | ComponentHealth::Stopped));
            } else {
                prop_assert_eq!(out, ComponentHealth::Stopping);
            }
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::{ReceiverHealthEvent, reduce_receiver_health};
    use logfwd_types::diagnostics::ComponentHealth;

    #[kani::proof]
    fn verify_delivery_noop_preserves_current_health() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_receiver_health(current, ReceiverHealthEvent::DeliveryNoop);
        assert_eq!(out, current);

        kani::cover!(
            current == ComponentHealth::Failed,
            "failed_noop_stays_failed"
        );
        kani::cover!(
            current == ComponentHealth::Degraded,
            "degraded_noop_stays_degraded"
        );
    }

    #[kani::proof]
    fn verify_delivery_accepted_recovers_only_non_terminal_receivers() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_receiver_health(current, ReceiverHealthEvent::DeliveryAccepted);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Healthy),
        }

        kani::cover!(
            current == ComponentHealth::Degraded,
            "degraded_accept_recovers"
        );
        kani::cover!(
            current == ComponentHealth::Failed,
            "failed_accept_stays_failed"
        );
    }

    #[kani::proof]
    fn verify_backpressure_degrades_only_non_terminal_receivers() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_receiver_health(current, ReceiverHealthEvent::Backpressure);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Degraded),
        }

        kani::cover!(
            current == ComponentHealth::Healthy,
            "healthy_backpressure_degrades"
        );
        kani::cover!(
            current == ComponentHealth::Stopped,
            "stopped_backpressure_preserves"
        );
    }

    #[kani::proof]
    fn verify_fatal_failure_preserves_drain_phase() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_receiver_health(current, ReceiverHealthEvent::FatalFailure);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped => assert_eq!(out, current),
            _ => assert_eq!(out, ComponentHealth::Failed),
        }

        kani::cover!(
            current == ComponentHealth::Stopping,
            "stopping_failure_stays_stopping"
        );
        kani::cover!(
            current == ComponentHealth::Healthy,
            "healthy_failure_turns_failed"
        );
    }

    #[kani::proof]
    fn verify_shutdown_requested_only_moves_non_terminal_receivers_to_stopping() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_receiver_health(current, ReceiverHealthEvent::ShutdownRequested);

        match current {
            ComponentHealth::Stopped | ComponentHealth::Failed => assert_eq!(out, current),
            _ => assert_eq!(out, ComponentHealth::Stopping),
        }

        kani::cover!(
            current == ComponentHealth::Healthy,
            "healthy_shutdown_requested_becomes_stopping"
        );
        kani::cover!(
            current == ComponentHealth::Failed,
            "failed_shutdown_requested_stays_failed"
        );
    }

    #[kani::proof]
    fn verify_shutdown_completed_stops_only_non_failed_receivers() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_receiver_health(current, ReceiverHealthEvent::ShutdownCompleted);

        match current {
            ComponentHealth::Failed => assert_eq!(out, ComponentHealth::Failed),
            _ => assert_eq!(out, ComponentHealth::Stopped),
        }

        kani::cover!(
            current == ComponentHealth::Stopping,
            "stopping_shutdown_completed_becomes_stopped"
        );
        kani::cover!(
            current == ComponentHealth::Failed,
            "failed_shutdown_completed_stays_failed"
        );
    }
}
