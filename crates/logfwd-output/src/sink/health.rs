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
            | ComponentHealth::Stopped
            | ComponentHealth::Failed => current,
            _ => ComponentHealth::Starting,
        },
        OutputHealthEvent::StartupSucceeded | OutputHealthEvent::DeliverySucceeded => match current
        {
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
        OutputHealthEvent::FatalFailure => ComponentHealth::Failed,
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
    }
}

#[cfg(kani)]
mod verification {
    use super::{OutputHealthEvent, aggregate_fanout_health, reduce_output_health};
    use logfwd_types::diagnostics::ComponentHealth;

    #[kani::proof]
    fn verify_retrying_degrades_only_non_terminal_outputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::Retrying);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Degraded),
        }
    }

    #[kani::proof]
    fn verify_shutdown_completed_preserves_failed_outputs() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = reduce_output_health(current, OutputHealthEvent::ShutdownCompleted);

        if matches!(current, ComponentHealth::Failed) {
            assert_eq!(out, ComponentHealth::Failed);
        } else {
            assert_eq!(out, ComponentHealth::Stopped);
        }
    }

    #[kani::proof]
    fn verify_fanout_health_is_commutative_for_two_children() {
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        assert_eq!(
            aggregate_fanout_health([a, b]),
            aggregate_fanout_health([b, a])
        );
    }
}
