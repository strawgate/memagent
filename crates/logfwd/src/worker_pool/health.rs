//! Pure health policy for output worker-pool aggregation.
//!
//! This module keeps terminal-state and aggregation semantics out of the
//! runtime shell so they can be Kani-proved directly.

use logfwd_io::diagnostics::ComponentHealth;
use logfwd_output::sink::{OutputHealthEvent, reduce_output_health};

/// Pool-level idle health to use after inserting a worker slot.
///
/// Terminal pool phases stay sticky: a new worker must not resurrect a pool
/// that is already stopping, stopped, or failed.
#[must_use]
pub(super) const fn idle_health_after_worker_insert(current: ComponentHealth) -> ComponentHealth {
    match current {
        ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => current,
        _ => ComponentHealth::Healthy,
    }
}

/// Reduce one worker-local health slot through an output event.
#[must_use]
pub(super) const fn reduce_worker_slot_health(
    current: ComponentHealth,
    event: OutputHealthEvent,
) -> ComponentHealth {
    reduce_output_health(current, event)
}

/// Aggregate the pool-level idle phase with live worker slots.
#[must_use]
pub(super) fn aggregate_output_health<I>(
    idle_health: ComponentHealth,
    worker_slots: I,
) -> ComponentHealth
where
    I: IntoIterator<Item = ComponentHealth>,
{
    worker_slots
        .into_iter()
        .fold(idle_health, ComponentHealth::combine)
}

#[cfg(test)]
mod tests {
    use super::{aggregate_output_health, idle_health_after_worker_insert};
    use logfwd_io::diagnostics::ComponentHealth;

    #[test]
    fn inserting_worker_keeps_terminal_pool_phases() {
        assert_eq!(
            idle_health_after_worker_insert(ComponentHealth::Stopping),
            ComponentHealth::Stopping
        );
        assert_eq!(
            idle_health_after_worker_insert(ComponentHealth::Stopped),
            ComponentHealth::Stopped
        );
        assert_eq!(
            idle_health_after_worker_insert(ComponentHealth::Failed),
            ComponentHealth::Failed
        );
        assert_eq!(
            idle_health_after_worker_insert(ComponentHealth::Degraded),
            ComponentHealth::Healthy
        );
    }

    #[test]
    fn aggregate_output_health_uses_worst_slot() {
        assert_eq!(
            aggregate_output_health(
                ComponentHealth::Healthy,
                [ComponentHealth::Healthy, ComponentHealth::Degraded]
            ),
            ComponentHealth::Degraded
        );
        assert_eq!(
            aggregate_output_health(
                ComponentHealth::Stopping,
                [ComponentHealth::Healthy, ComponentHealth::Healthy]
            ),
            ComponentHealth::Stopping
        );
    }
}

#[cfg(kani)]
mod verification {
    use super::{aggregate_output_health, idle_health_after_worker_insert};
    use logfwd_io::diagnostics::ComponentHealth;

    #[kani::proof]
    fn verify_insert_worker_preserves_terminal_pool_phase() {
        let current = ComponentHealth::from_repr(kani::any());
        let out = idle_health_after_worker_insert(current);

        match current {
            ComponentHealth::Stopping | ComponentHealth::Stopped | ComponentHealth::Failed => {
                assert_eq!(out, current)
            }
            _ => assert_eq!(out, ComponentHealth::Healthy),
        }

        kani::cover!(
            current == ComponentHealth::Failed,
            "failed_pool_phase_stays_failed"
        );
        kani::cover!(
            current == ComponentHealth::Degraded,
            "non_terminal_pool_phase_recovers_to_healthy"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_aggregate_output_health_preserves_idle_terminal_state() {
        let idle = ComponentHealth::from_repr(kani::any());
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        let out = aggregate_output_health(idle, [a, b]);

        if matches!(idle, ComponentHealth::Stopping | ComponentHealth::Stopped) {
            assert_eq!(out, idle);
        }
        if idle == ComponentHealth::Failed {
            assert_eq!(out, ComponentHealth::Failed);
        }

        kani::cover!(
            idle == ComponentHealth::Stopping
                && a == ComponentHealth::Healthy
                && b == ComponentHealth::Degraded,
            "stopping_idle_dominates_children"
        );
        kani::cover!(
            idle == ComponentHealth::Healthy
                && a == ComponentHealth::Healthy
                && b == ComponentHealth::Degraded,
            "degraded_child_surfaces"
        );
    }
}
