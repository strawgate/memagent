//! Pure health policy for output worker-pool aggregation.
//!
//! This module keeps terminal-state and aggregation semantics out of the
//! runtime shell so they can be Kani-proved directly.

use ffwd_diagnostics::diagnostics::ComponentHealth;
use ffwd_output::sink::{OutputHealthEvent, reduce_output_health};

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
    use ffwd_diagnostics::diagnostics::ComponentHealth;
    use proptest::prelude::*;

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

    fn arb_health() -> impl Strategy<Value = ComponentHealth> {
        (0u8..=5).prop_map(ComponentHealth::from_repr)
    }

    proptest! {
        #[test]
        fn aggregate_output_health_matches_maximum_repr_over_all_inputs(
            idle in arb_health(),
            slots in proptest::collection::vec(arb_health(), 0..16)
        ) {
            let out = aggregate_output_health(idle, slots.iter().copied());
            let expected = slots
                .iter()
                .copied()
                .fold(idle, ComponentHealth::combine);
            prop_assert_eq!(out, expected);
            prop_assert!(out.as_repr() >= idle.as_repr());
            for slot in &slots {
                prop_assert!(out.as_repr() >= slot.as_repr());
            }
        }

        #[test]
        fn aggregate_output_health_is_permutation_invariant_for_live_slots(
            idle in arb_health(),
            slots in proptest::collection::vec(arb_health(), 0..8)
        ) {
            let forward = aggregate_output_health(idle, slots.iter().copied());
            let reverse = aggregate_output_health(idle, slots.iter().rev().copied());
            prop_assert_eq!(forward, reverse);
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::{aggregate_output_health, idle_health_after_worker_insert};
    use ffwd_diagnostics::diagnostics::ComponentHealth;

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
    fn verify_aggregate_output_health_returns_worst_of_idle_and_children() {
        let idle = ComponentHealth::from_repr(kani::any());
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        let out = aggregate_output_health(idle, [a, b]);

        assert!(out.as_repr() >= idle.as_repr());
        assert!(out.as_repr() >= a.as_repr());
        assert!(out.as_repr() >= b.as_repr());
        assert!(out == idle || out == a || out == b);

        kani::cover!(
            idle == ComponentHealth::Stopping
                && a == ComponentHealth::Healthy
                && b == ComponentHealth::Degraded,
            "stopping_idle_beats_ready_children"
        );
        kani::cover!(
            idle == ComponentHealth::Healthy
                && a == ComponentHealth::Healthy
                && b == ComponentHealth::Degraded,
            "degraded_child_surfaces"
        );
        kani::cover!(
            idle == ComponentHealth::Stopping
                && a == ComponentHealth::Failed
                && b == ComponentHealth::Healthy,
            "failed_child_beats_stopping_idle"
        );
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_aggregate_output_health_is_associative_over_three_children() {
        let idle = ComponentHealth::from_repr(kani::any());
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        let c = ComponentHealth::from_repr(kani::any());

        let left = aggregate_output_health(idle, [a, b, c]);
        let right = aggregate_output_health(
            idle,
            [aggregate_output_health(ComponentHealth::Healthy, [a, b]), c],
        );
        assert_eq!(left, right);

        kani::cover!(
            idle == ComponentHealth::Healthy
                && a == ComponentHealth::Degraded
                && b == ComponentHealth::Starting
                && c == ComponentHealth::Failed,
            "mixed_three_child_associativity_case"
        );
    }
}
