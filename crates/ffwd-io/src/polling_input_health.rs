//! Pure lifecycle policy for polling-style input sources.
//!
//! These inputs do not own independent startup/shutdown threads the way the
//! standalone HTTP receivers do. Instead, they surface source-origin signals
//! such as backpressure or error-backoff, while the outer pipeline loop owns
//! starting/stopping transitions.

use ffwd_types::diagnostics::ComponentHealth;

/// Events that can update source-origin health for polling inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PollingInputHealthEvent {
    /// The latest poll cycle observed no source-local pressure signal.
    PollHealthy,
    /// The source observed backpressure or shed work locally.
    BackpressureObserved,
    /// The source entered local error-backoff after an error burst.
    ErrorBackoffObserved,
}

/// Reduce a current source-origin health value and event into the next value.
///
/// Terminal states remain sticky so this reducer can be safely reused if a
/// caller stores a stronger lifecycle state in the same slot later.
#[must_use]
pub(crate) const fn reduce_polling_input_health(
    current: ComponentHealth,
    event: PollingInputHealthEvent,
) -> ComponentHealth {
    match event {
        PollingInputHealthEvent::PollHealthy => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => ComponentHealth::Healthy,
        },
        PollingInputHealthEvent::BackpressureObserved
        | PollingInputHealthEvent::ErrorBackoffObserved => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => ComponentHealth::Degraded,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{PollingInputHealthEvent, reduce_polling_input_health};
    use ffwd_types::diagnostics::ComponentHealth;
    use proptest::prelude::*;

    #[test]
    fn poll_healthy_recovers_non_terminal_inputs() {
        assert_eq!(
            reduce_polling_input_health(
                ComponentHealth::Degraded,
                PollingInputHealthEvent::PollHealthy,
            ),
            ComponentHealth::Healthy
        );
        assert_eq!(
            reduce_polling_input_health(
                ComponentHealth::Healthy,
                PollingInputHealthEvent::PollHealthy,
            ),
            ComponentHealth::Healthy
        );
        assert_eq!(
            reduce_polling_input_health(
                ComponentHealth::Failed,
                PollingInputHealthEvent::PollHealthy,
            ),
            ComponentHealth::Failed
        );
    }

    #[test]
    fn degraded_observations_preserve_terminal_states() {
        assert_eq!(
            reduce_polling_input_health(
                ComponentHealth::Stopped,
                PollingInputHealthEvent::BackpressureObserved,
            ),
            ComponentHealth::Stopped
        );
        assert_eq!(
            reduce_polling_input_health(
                ComponentHealth::Stopping,
                PollingInputHealthEvent::ErrorBackoffObserved,
            ),
            ComponentHealth::Stopping
        );
    }

    fn arb_event() -> impl Strategy<Value = PollingInputHealthEvent> {
        prop_oneof![
            Just(PollingInputHealthEvent::PollHealthy),
            Just(PollingInputHealthEvent::BackpressureObserved),
            Just(PollingInputHealthEvent::ErrorBackoffObserved),
        ]
    }

    fn apply_events(
        initial: ComponentHealth,
        events: &[PollingInputHealthEvent],
    ) -> ComponentHealth {
        events
            .iter()
            .copied()
            .fold(initial, reduce_polling_input_health)
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
        fn healthy_sequence_never_escapes_healthy_or_degraded(
            events in proptest::collection::vec(arb_event(), 0..16)
        ) {
            let out = apply_events(ComponentHealth::Healthy, &events);
            prop_assert!(matches!(out, ComponentHealth::Healthy | ComponentHealth::Degraded));
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::{PollingInputHealthEvent, reduce_polling_input_health};
    use ffwd_types::diagnostics::ComponentHealth;

    #[kani::proof]
    fn verify_poll_healthy_recovers_only_non_terminal_inputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_polling_input_health(current, PollingInputHealthEvent::PollHealthy);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Healthy),
        }

        kani::cover!(
            current == ComponentHealth::Degraded,
            "degraded_poll_healthy_recovers"
        );
        kani::cover!(
            current == ComponentHealth::Failed,
            "failed_poll_healthy_stays_failed"
        );
    }

    #[kani::proof]
    fn verify_backpressure_degrades_only_non_terminal_inputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out =
            reduce_polling_input_health(current, PollingInputHealthEvent::BackpressureObserved);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Degraded),
        }

        kani::cover!(
            current == ComponentHealth::Healthy,
            "healthy_backpressure_becomes_degraded"
        );
        kani::cover!(
            current == ComponentHealth::Stopped,
            "stopped_backpressure_stays_stopped"
        );
    }

    #[kani::proof]
    fn verify_error_backoff_degrades_only_non_terminal_inputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out =
            reduce_polling_input_health(current, PollingInputHealthEvent::ErrorBackoffObserved);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Degraded),
        }

        kani::cover!(
            current == ComponentHealth::Healthy,
            "healthy_error_backoff_becomes_degraded"
        );
        kani::cover!(
            current == ComponentHealth::Failed,
            "failed_error_backoff_stays_failed"
        );
    }
}
