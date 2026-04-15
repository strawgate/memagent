//! Pure health transition policy for pipeline input loops.
//!
//! This module keeps deterministic lifecycle mapping out of the async
//! orchestration shell so sync and async paths can share one bounded reducer.

use logfwd_io::diagnostics::ComponentHealth;

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

#[cfg(test)]
mod tests {
    use super::{HealthTransitionEvent, reduce_component_health};
    use logfwd_io::diagnostics::ComponentHealth;

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
}

#[cfg(kani)]
mod verification {
    use super::{HealthTransitionEvent, reduce_component_health};
    use logfwd_io::diagnostics::ComponentHealth;

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
}
