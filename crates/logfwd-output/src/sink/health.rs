//! Pure output health policy for sink aggregation and worker-driven status.
//!
//! This module keeps control-plane health semantics out of async sink and
//! worker-pool shells so the rules can be tested and Kani-proved locally.

use logfwd_types::diagnostics::ComponentHealth;

/// Explicit output lifecycle events that can update a health snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputHealthEvent {
    /// A worker is creating or binding the sink.
    StartupRequested,
    /// Sink creation or worker startup completed successfully.
    StartupSucceeded,
    /// A batch was delivered successfully.
    DeliverySucceeded,
    /// A batch hit a retriable failure and the worker is retrying.
    Retrying,
    /// A batch or sink hit a fatal failure and cannot make progress.
    FatalFailure,
    /// Shutdown or drain has started.
    ShutdownRequested,
    /// Shutdown or drain completed cleanly.
    ShutdownCompleted,
}

/// Reduce one output health snapshot and lifecycle event into the next value.
///
/// Terminal states are sticky: once a sink is stopped or failed, later
/// delivery/startup observations do not resurrect it.
#[must_use]
pub const fn reduce_output_health(
    current: ComponentHealth,
    event: OutputHealthEvent,
) -> ComponentHealth {
    match event {
        OutputHealthEvent::StartupRequested => match current {
            ComponentHealth::Healthy
            | ComponentHealth::Degraded
            | ComponentHealth::Stopping
            | ComponentHealth::Stopped
            | ComponentHealth::Failed => current,
            ComponentHealth::Starting => ComponentHealth::Starting,
        },
        OutputHealthEvent::StartupSucceeded => match current {
            ComponentHealth::Degraded => ComponentHealth::Degraded,
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => ComponentHealth::Healthy,
        },
        OutputHealthEvent::DeliverySucceeded => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => ComponentHealth::Healthy,
        },
        OutputHealthEvent::Retrying => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                current
            }
            _ => ComponentHealth::Degraded,
        },
        OutputHealthEvent::FatalFailure => match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped => current,
            _ => ComponentHealth::Failed,
        },
        OutputHealthEvent::ShutdownRequested => match current {
            ComponentHealth::Stopped | ComponentHealth::Failed => current,
            _ => ComponentHealth::Stopping,
        },
        OutputHealthEvent::ShutdownCompleted => match current {
            ComponentHealth::Failed => ComponentHealth::Failed,
            _ => ComponentHealth::Stopped,
        },
    }
}

/// Roll child sink health into one fanout view.
///
/// An empty fanout is treated as healthy so the roll-up remains neutral for
/// empty iterators and callers can decide separately whether an empty fanout is
/// valid configuration.
#[must_use]
pub fn aggregate_fanout_health<I>(states: I) -> ComponentHealth
where
    I: IntoIterator<Item = ComponentHealth>,
{
    states
        .into_iter()
        .fold(ComponentHealth::Healthy, ComponentHealth::combine)
}

#[cfg(test)]
mod tests {
    use super::{OutputHealthEvent, aggregate_fanout_health, reduce_output_health};
    use logfwd_types::diagnostics::ComponentHealth;
    use proptest::prelude::*;

    #[test]
    fn fanout_health_uses_worst_child_state() {
        let health = aggregate_fanout_health([
            ComponentHealth::Healthy,
            ComponentHealth::Degraded,
            ComponentHealth::Healthy,
        ]);
        assert_eq!(health, ComponentHealth::Degraded);
    }

    #[test]
    fn empty_fanout_is_neutral_healthy() {
        let health = aggregate_fanout_health(std::iter::empty::<ComponentHealth>());
        assert_eq!(health, ComponentHealth::Healthy);
    }

    #[test]
    fn delivery_success_recovers_non_terminal_outputs() {
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Starting,
                OutputHealthEvent::DeliverySucceeded
            ),
            ComponentHealth::Healthy
        );
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Degraded,
                OutputHealthEvent::DeliverySucceeded
            ),
            ComponentHealth::Healthy
        );
    }

    #[test]
    fn startup_request_does_not_downgrade_ready_outputs() {
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Healthy,
                OutputHealthEvent::StartupRequested
            ),
            ComponentHealth::Healthy
        );
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Degraded,
                OutputHealthEvent::StartupRequested
            ),
            ComponentHealth::Degraded
        );
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Stopping,
                OutputHealthEvent::StartupRequested
            ),
            ComponentHealth::Stopping
        );
    }

    #[test]
    fn startup_success_does_not_clear_retrying_degradation() {
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Degraded,
                OutputHealthEvent::StartupSucceeded
            ),
            ComponentHealth::Degraded
        );
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Starting,
                OutputHealthEvent::StartupSucceeded
            ),
            ComponentHealth::Healthy
        );
    }

    #[test]
    fn retrying_degrades_non_terminal_outputs() {
        assert_eq!(
            reduce_output_health(ComponentHealth::Healthy, OutputHealthEvent::Retrying),
            ComponentHealth::Degraded
        );
        assert_eq!(
            reduce_output_health(ComponentHealth::Starting, OutputHealthEvent::Retrying),
            ComponentHealth::Degraded
        );
        assert_eq!(
            reduce_output_health(ComponentHealth::Stopped, OutputHealthEvent::Retrying),
            ComponentHealth::Stopped
        );
    }

    #[test]
    fn shutdown_and_failure_are_sticky() {
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Healthy,
                OutputHealthEvent::ShutdownRequested
            ),
            ComponentHealth::Stopping
        );
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Stopping,
                OutputHealthEvent::ShutdownCompleted
            ),
            ComponentHealth::Stopped
        );
        assert_eq!(
            reduce_output_health(
                ComponentHealth::Failed,
                OutputHealthEvent::ShutdownCompleted
            ),
            ComponentHealth::Failed
        );
        assert_eq!(
            reduce_output_health(ComponentHealth::Degraded, OutputHealthEvent::FatalFailure),
            ComponentHealth::Failed
        );
        assert_eq!(
            reduce_output_health(ComponentHealth::Stopping, OutputHealthEvent::FatalFailure),
            ComponentHealth::Stopping
        );
    }

    fn apply_events(initial: ComponentHealth, events: &[OutputHealthEvent]) -> ComponentHealth {
        events.iter().copied().fold(initial, reduce_output_health)
    }

    fn arb_event() -> impl Strategy<Value = OutputHealthEvent> {
        prop_oneof![
            Just(OutputHealthEvent::StartupRequested),
            Just(OutputHealthEvent::StartupSucceeded),
            Just(OutputHealthEvent::DeliverySucceeded),
            Just(OutputHealthEvent::Retrying),
            Just(OutputHealthEvent::FatalFailure),
            Just(OutputHealthEvent::ShutdownRequested),
            Just(OutputHealthEvent::ShutdownCompleted),
        ]
    }

    fn arb_health() -> impl Strategy<Value = ComponentHealth> {
        (0u8..=5).prop_map(ComponentHealth::from_repr)
    }

    proptest! {
        #[test]
        fn failed_output_state_is_sticky_across_any_event_sequence(
            events in proptest::collection::vec(arb_event(), 0..16)
        ) {
            let out = apply_events(ComponentHealth::Failed, &events);
            prop_assert_eq!(out, ComponentHealth::Failed);
        }

        #[test]
        fn stopped_output_state_is_sticky_across_any_event_sequence(
            events in proptest::collection::vec(arb_event(), 0..16)
        ) {
            let out = apply_events(ComponentHealth::Stopped, &events);
            prop_assert_eq!(out, ComponentHealth::Stopped);
        }

        #[test]
        fn fanout_health_matches_maximum_repr_over_all_children(
            children in proptest::collection::vec(arb_health(), 0..16)
        ) {
            let out = aggregate_fanout_health(children.iter().copied());
            let expected = children
                .iter()
                .copied()
                .fold(ComponentHealth::Healthy, ComponentHealth::combine);
            prop_assert_eq!(out, expected);
        }
    }
}

// NOTE: This Kani verification module follows the same per-event-variant proof
// pattern as `crates/logfwd-io/src/receiver_health.rs`. Both verify a
// `reduce_*_health(ComponentHealth, Event) -> ComponentHealth` reducer with one
// proof per event variant. They are NOT merged because they are different
// compilation units with distinct event enums and transition tables.
#[cfg(kani)]
mod verification {
    use super::{OutputHealthEvent, aggregate_fanout_health, reduce_output_health};
    use logfwd_types::diagnostics::ComponentHealth;

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_retrying_degrades_only_non_terminal_outputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::Retrying);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Degraded),
        }

        kani::cover!(
            matches!(current, ComponentHealth::Healthy),
            "healthy_retrying"
        );
        kani::cover!(
            matches!(current, ComponentHealth::Stopped),
            "stopped_retrying"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_shutdown_completed_preserves_failed_outputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::ShutdownCompleted);

        if matches!(current, ComponentHealth::Failed) {
            assert_eq!(out, ComponentHealth::Failed);
        } else {
            assert_eq!(out, ComponentHealth::Stopped);
        }

        kani::cover!(
            matches!(current, ComponentHealth::Failed),
            "failed_shutdown_completed"
        );
        kani::cover!(
            matches!(current, ComponentHealth::Healthy),
            "healthy_shutdown_completed"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_startup_succeeded_preserves_retrying_degradation() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::StartupSucceeded);

        match current {
            ComponentHealth::Degraded => assert_eq!(out, ComponentHealth::Degraded),
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Healthy),
        }

        kani::cover!(
            matches!(current, ComponentHealth::Degraded),
            "degraded_startup_succeeded"
        );
        kani::cover!(
            matches!(current, ComponentHealth::Starting),
            "starting_startup_succeeded"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_delivery_succeeded_recovers_only_non_terminal_outputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::DeliverySucceeded);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Healthy),
        }

        kani::cover!(
            matches!(current, ComponentHealth::Degraded),
            "degraded_delivery_succeeded_recovers"
        );
        kani::cover!(
            matches!(current, ComponentHealth::Failed),
            "failed_delivery_succeeded_stays_failed"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_shutdown_requested_only_moves_non_terminal_outputs_to_stopping() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::ShutdownRequested);

        match current {
            ComponentHealth::Stopped | ComponentHealth::Failed => assert_eq!(out, current),
            _ => assert_eq!(out, ComponentHealth::Stopping),
        }

        kani::cover!(
            matches!(current, ComponentHealth::Healthy),
            "healthy_shutdown_requested_becomes_stopping"
        );
        kani::cover!(
            matches!(current, ComponentHealth::Failed),
            "failed_shutdown_requested_stays_failed"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_fanout_health_is_commutative_for_two_children() {
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        assert_eq!(
            aggregate_fanout_health([a, b]),
            aggregate_fanout_health([b, a])
        );

        kani::cover!(
            a == ComponentHealth::Healthy && b == ComponentHealth::Degraded,
            "healthy_degraded_pair"
        );
        kani::cover!(
            a == ComponentHealth::Stopping && b == ComponentHealth::Healthy,
            "stopping_healthy_pair"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_fatal_failure_preserves_drain_phase() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::FatalFailure);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped => assert_eq!(out, current),
            _ => assert_eq!(out, ComponentHealth::Failed),
        }

        kani::cover!(
            matches!(current, ComponentHealth::Stopping),
            "stopping_fatal_failure"
        );
        kani::cover!(
            matches!(current, ComponentHealth::Healthy),
            "healthy_fatal_failure"
        );
    }
}
