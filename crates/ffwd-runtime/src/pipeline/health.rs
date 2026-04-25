//! Pure health transition policy for pipeline input loops.
//!
//! This module keeps deterministic lifecycle mapping out of the async
//! orchestration shell so sync and async paths can share one bounded reducer.

use ffwd_diagnostics::diagnostics::ComponentHealth;

/// Explicit lifecycle events that can update a pipeline component snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HealthTransitionEvent {
    /// Adopt the latest health reported by the component itself.
    Observed(ComponentHealth),
    /// A poll or fetch attempt failed.
    PollFailed,
    /// Shutdown or drain has started.
    ShutdownRequested,
    /// Shutdown or drain has completed.
    ShutdownCompleted,
}

/// Consecutive poll failures allowed while still starting before escalation.
pub(super) const STARTING_POLL_FAILURE_ESCALATION_THRESHOLD: u32 = 3;

/// Reduce a current health value and lifecycle event into the next value.
///
/// Terminal states are sticky: once a component is stopped or failed, later
/// observation events do not resurrect it.
#[must_use]
pub(super) const fn reduce_component_health(
    current: ComponentHealth,
    event: HealthTransitionEvent,
) -> ComponentHealth {
    match event {
        HealthTransitionEvent::Observed(next) => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => next,
        },
        HealthTransitionEvent::PollFailed => match current {
            ComponentHealth::Starting
            | ComponentHealth::Stopping
            | ComponentHealth::Stopped
            | ComponentHealth::Failed => current,
            _ => ComponentHealth::Degraded,
        },
        HealthTransitionEvent::ShutdownRequested => match current {
            ComponentHealth::Stopped | ComponentHealth::Failed => current,
            _ => ComponentHealth::Stopping,
        },
        HealthTransitionEvent::ShutdownCompleted => match current {
            ComponentHealth::Failed => ComponentHealth::Failed,
            _ => ComponentHealth::Stopped,
        },
    }
}

/// Reduce one poll failure with startup-aware bounded escalation.
///
/// While `Starting`, repeated poll failures stay `Starting` until
/// `consecutive_poll_failures` reaches
/// [`STARTING_POLL_FAILURE_ESCALATION_THRESHOLD`], then escalate to
/// `Degraded`. Other states follow [`reduce_component_health`] `PollFailed`
/// semantics.
#[must_use]
pub(super) const fn reduce_component_health_after_poll_failure(
    current: ComponentHealth,
    consecutive_poll_failures: u32,
) -> ComponentHealth {
    match current {
        ComponentHealth::Starting
            if consecutive_poll_failures >= STARTING_POLL_FAILURE_ESCALATION_THRESHOLD =>
        {
            ComponentHealth::Degraded
        }
        _ => reduce_component_health(current, HealthTransitionEvent::PollFailed),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HealthTransitionEvent, STARTING_POLL_FAILURE_ESCALATION_THRESHOLD, reduce_component_health,
        reduce_component_health_after_poll_failure,
    };
    use ffwd_diagnostics::diagnostics::ComponentHealth;
    use proptest::prelude::*;

    #[test]
    fn observed_updates_non_terminal_states() {
        assert_eq!(
            reduce_component_health(
                ComponentHealth::Starting,
                HealthTransitionEvent::Observed(ComponentHealth::Healthy)
            ),
            ComponentHealth::Healthy
        );
        assert_eq!(
            reduce_component_health(
                ComponentHealth::Degraded,
                HealthTransitionEvent::Observed(ComponentHealth::Healthy)
            ),
            ComponentHealth::Healthy
        );
        assert_eq!(
            reduce_component_health(
                ComponentHealth::Healthy,
                HealthTransitionEvent::Observed(ComponentHealth::Degraded)
            ),
            ComponentHealth::Degraded
        );
    }

    #[test]
    fn shutdown_transitions_are_sticky() {
        assert_eq!(
            reduce_component_health(
                ComponentHealth::Healthy,
                HealthTransitionEvent::ShutdownRequested
            ),
            ComponentHealth::Stopping
        );
        assert_eq!(
            reduce_component_health(
                ComponentHealth::Stopping,
                HealthTransitionEvent::ShutdownCompleted
            ),
            ComponentHealth::Stopped
        );
        assert_eq!(
            reduce_component_health(
                ComponentHealth::Failed,
                HealthTransitionEvent::ShutdownRequested
            ),
            ComponentHealth::Failed
        );
    }

    #[test]
    fn poll_failure_degrades_non_terminal_states() {
        assert_eq!(
            reduce_component_health(ComponentHealth::Healthy, HealthTransitionEvent::PollFailed),
            ComponentHealth::Degraded
        );
        assert_eq!(
            reduce_component_health(ComponentHealth::Starting, HealthTransitionEvent::PollFailed),
            ComponentHealth::Starting
        );
        assert_eq!(
            reduce_component_health(ComponentHealth::Stopped, HealthTransitionEvent::PollFailed),
            ComponentHealth::Stopped
        );
    }

    #[test]
    fn starting_poll_failures_escalate_once_threshold_is_reached() {
        assert_eq!(
            reduce_component_health_after_poll_failure(ComponentHealth::Starting, 1),
            ComponentHealth::Starting
        );
        assert_eq!(
            reduce_component_health_after_poll_failure(
                ComponentHealth::Starting,
                STARTING_POLL_FAILURE_ESCALATION_THRESHOLD - 1,
            ),
            ComponentHealth::Starting
        );
        assert_eq!(
            reduce_component_health_after_poll_failure(
                ComponentHealth::Starting,
                STARTING_POLL_FAILURE_ESCALATION_THRESHOLD,
            ),
            ComponentHealth::Degraded
        );
    }

    #[test]
    fn non_starting_poll_failure_semantics_are_unchanged_with_counter() {
        assert_eq!(
            reduce_component_health_after_poll_failure(ComponentHealth::Healthy, 100),
            ComponentHealth::Degraded
        );
        assert_eq!(
            reduce_component_health_after_poll_failure(ComponentHealth::Stopped, 100),
            ComponentHealth::Stopped
        );
    }

    #[test]
    fn post_threshold_starting_poll_failures_stay_degraded_until_observed() {
        let mut current = ComponentHealth::Starting;
        for n in 1..=(STARTING_POLL_FAILURE_ESCALATION_THRESHOLD + 3) {
            current = reduce_component_health_after_poll_failure(current, n);
        }
        assert_eq!(current, ComponentHealth::Degraded);

        current = reduce_component_health_after_poll_failure(current, 99);
        assert_eq!(current, ComponentHealth::Degraded);

        current = reduce_component_health(
            current,
            HealthTransitionEvent::Observed(ComponentHealth::Healthy),
        );
        assert_eq!(current, ComponentHealth::Healthy);
    }

    fn arb_health() -> impl Strategy<Value = ComponentHealth> {
        prop_oneof![
            Just(ComponentHealth::Starting),
            Just(ComponentHealth::Healthy),
            Just(ComponentHealth::Degraded),
            Just(ComponentHealth::Stopping),
            Just(ComponentHealth::Stopped),
            Just(ComponentHealth::Failed),
        ]
    }

    fn arb_event() -> impl Strategy<Value = HealthTransitionEvent> {
        prop_oneof![
            arb_health().prop_map(HealthTransitionEvent::Observed),
            Just(HealthTransitionEvent::PollFailed),
            Just(HealthTransitionEvent::ShutdownRequested),
            Just(HealthTransitionEvent::ShutdownCompleted),
        ]
    }

    fn apply_events(initial: ComponentHealth, events: &[HealthTransitionEvent]) -> ComponentHealth {
        events
            .iter()
            .copied()
            .fold(initial, reduce_component_health)
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
        fn shutdown_requested_never_recovers_to_ready_without_full_restart(
            events in proptest::collection::vec(arb_event(), 0..16)
        ) {
            let out = apply_events(ComponentHealth::Stopping, &events);
            prop_assert!(matches!(out, ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed));
        }

        #[test]
        fn starting_poll_failure_escalation_is_threshold_deterministic(
            failures in 0u32..32
        ) {
            let mut current = ComponentHealth::Starting;
            for n in 1..=failures {
                current = reduce_component_health_after_poll_failure(current, n);
            }

            let expected = if failures >= STARTING_POLL_FAILURE_ESCALATION_THRESHOLD {
                ComponentHealth::Degraded
            } else {
                ComponentHealth::Starting
            };
            prop_assert_eq!(current, expected);
        }

        #[test]
        fn observed_starting_resets_startup_poll_failure_window(
            first_streak in 0u32..16,
            second_streak in 0u32..16
        ) {
            let mut current = ComponentHealth::Starting;
            for n in 1..=first_streak {
                current = reduce_component_health_after_poll_failure(current, n);
            }

            current = reduce_component_health(
                current,
                HealthTransitionEvent::Observed(ComponentHealth::Starting),
            );

            for n in 1..=second_streak {
                current = reduce_component_health_after_poll_failure(current, n);
            }

            let expected = if second_streak >= STARTING_POLL_FAILURE_ESCALATION_THRESHOLD {
                ComponentHealth::Degraded
            } else {
                ComponentHealth::Starting
            };
            prop_assert_eq!(current, expected);
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::{
        HealthTransitionEvent, STARTING_POLL_FAILURE_ESCALATION_THRESHOLD, reduce_component_health,
        reduce_component_health_after_poll_failure,
    };
    use ffwd_diagnostics::diagnostics::ComponentHealth;

    #[kani::proof]
    fn verify_observed_does_not_resurrect_terminal_states() {
        let current = ComponentHealth::from_repr(kani::any());
        let next = ComponentHealth::from_repr(kani::any());
        let out = reduce_component_health(current, HealthTransitionEvent::Observed(next));

        if matches!(
            current,
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed
        ) {
            assert_eq!(out, current);
        }

        kani::cover!(
            current == ComponentHealth::Stopped && next == ComponentHealth::Healthy,
            "stopped ignores healthy observation"
        );
        kani::cover!(
            current == ComponentHealth::Starting && next == ComponentHealth::Healthy,
            "starting accepts healthy observation"
        );
    }

    #[kani::proof]
    fn verify_shutdown_requested_progresses_to_stopping_or_preserves_terminal() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_component_health(current, HealthTransitionEvent::ShutdownRequested);

        match current {
            ComponentHealth::Stopped | ComponentHealth::Failed => assert_eq!(out, current),
            _ => assert_eq!(out, ComponentHealth::Stopping),
        }

        kani::cover!(
            current == ComponentHealth::Healthy,
            "healthy transitions to stopping"
        );
        kani::cover!(
            current == ComponentHealth::Failed,
            "failed preserves on shutdown request"
        );
    }

    #[kani::proof]
    fn verify_shutdown_completed_stops_only_non_failed_components() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_component_health(current, HealthTransitionEvent::ShutdownCompleted);

        match current {
            ComponentHealth::Failed => assert_eq!(out, ComponentHealth::Failed),
            _ => assert_eq!(out, ComponentHealth::Stopped),
        }

        kani::cover!(
            current == ComponentHealth::Stopping,
            "stopping transitions to stopped"
        );
        kani::cover!(
            current == ComponentHealth::Failed,
            "failed preserves on shutdown completed"
        );
    }

    #[kani::proof]
    fn verify_poll_failed_degrades_only_non_terminal_states() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_component_health(current, HealthTransitionEvent::PollFailed);

        match current {
            ComponentHealth::Starting
            | ComponentHealth::Stopping
            | ComponentHealth::Stopped
            | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Degraded),
        }

        kani::cover!(
            current == ComponentHealth::Healthy,
            "healthy degrades on poll failure"
        );
        kani::cover!(
            current == ComponentHealth::Starting,
            "starting preserves on poll failure"
        );
        kani::cover!(
            current == ComponentHealth::Stopped,
            "stopped preserves on poll failure"
        );
    }

    #[kani::proof]
    fn verify_starting_poll_failure_escalates_after_threshold() {
        let consecutive_failures: u32 = kani::any();
        let out = reduce_component_health_after_poll_failure(
            ComponentHealth::Starting,
            consecutive_failures,
        );

        if consecutive_failures >= STARTING_POLL_FAILURE_ESCALATION_THRESHOLD {
            assert_eq!(out, ComponentHealth::Degraded);
        } else {
            assert_eq!(out, ComponentHealth::Starting);
        }

        kani::cover!(consecutive_failures == 0, "zero_failures");
        kani::cover!(
            consecutive_failures >= STARTING_POLL_FAILURE_ESCALATION_THRESHOLD,
            "threshold_reached"
        );
    }

    #[kani::proof]
    fn verify_non_starting_poll_failure_matches_base_reducer() {
        let health_repr = kani::any_where(|repr: &u8| {
            ComponentHealth::from_repr(*repr) != ComponentHealth::Starting
        });
        let current = ComponentHealth::from_repr(health_repr);
        let consecutive_failures: u32 = kani::any();

        let out_with_counter =
            reduce_component_health_after_poll_failure(current, consecutive_failures);
        let out_base = reduce_component_health(current, HealthTransitionEvent::PollFailed);
        assert_eq!(out_with_counter, out_base);

        kani::cover!(current == ComponentHealth::Healthy, "healthy_case");
        kani::cover!(current == ComponentHealth::Stopped, "stopped_case");
    }
}
