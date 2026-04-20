// Kani formal proofs — dispatch logic
// ---------------------------------------------------------------------------
//
// The tokio runtime and channel operations cannot be modelled by Kani.
// We extract the pure dispatch *decision* into a standalone function and
// prove its invariants symbolically.  The actual pool code mirrors this
// logic exactly so the proofs transfer.

/// Abstract channel state used in Kani models.
#[cfg_attr(kani, derive(kani::Arbitrary))]
#[cfg(any(test, kani))]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ChannelState {
    /// Channel has space — `try_send` would succeed.
    HasSpace,
    /// Channel is full — `try_send` would return `Full`.
    Full,
    /// Worker has exited — `try_send` would return `Closed`.
    Closed,
}

/// Outcome of one dispatch step over an array of workers.
#[cfg(any(test, kani))]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum DispatchOutcome {
    /// Item sent to worker at this index in the original `states` snapshot.
    SentToIndex(usize),
    /// All workers full/closed but under limit — spawn a new worker.
    SpawnNew,
    /// All workers full, at limit — must async-wait on front worker.
    WaitOnFront,
}

/// Pure dispatch algorithm (no I/O). Proved by Kani.
///
/// Scans `states` front-to-back:
/// - Closed workers are skipped (counted as pruned).
/// - The first worker with `HasSpace` receives the item.
/// - If all non-closed workers are `Full`:
///   - If `active_count < max_workers`, returns `SpawnNew`.
///   - Otherwise returns `WaitOnFront`.
///
/// Preconditions: `max_workers >= 1`, `states.len() <= max_workers`.
#[cfg(any(test, kani))]
pub(crate) fn dispatch_step(states: &[ChannelState], max_workers: usize) -> DispatchOutcome {
    for (i, &state) in states.iter().enumerate() {
        match state {
            ChannelState::Closed => {} // prune; try next
            ChannelState::HasSpace => return DispatchOutcome::SentToIndex(i),
            ChannelState::Full => {} // try next
        }
    }
    // No worker had space. Count active (non-closed) workers.
    let active = states
        .iter()
        .filter(|&&s| s != ChannelState::Closed)
        .count();
    if active < max_workers {
        DispatchOutcome::SpawnNew
    } else {
        DispatchOutcome::WaitOnFront
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;
    use crate::worker_pool::types::DeliveryOutcome;

    // -----------------------------------------------------------------------
    // Proof 1: Consolidated dispatch invariants — item is never dropped,
    // SentToIndex picks the FIRST HasSpace worker, SpawnNew only fires
    // when no HasSpace exists, and WaitOnFront only at capacity.
    //
    // Replaces the former 4 separate proofs:
    // - verify_dispatch_never_drops_item
    // - verify_dispatch_picks_first_available
    // - verify_spawn_only_when_no_space
    // - verify_wait_only_at_capacity
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_dispatch_invariants() {
        const MAX_N: usize = 4;
        let n: usize = kani::any();
        kani::assume(n <= MAX_N);

        let max_workers: usize = kani::any();
        kani::assume(max_workers >= 1);
        kani::assume(max_workers <= 8);
        // Pool invariant: never more workers than max.
        kani::assume(n <= max_workers);

        let states: [ChannelState; MAX_N] = kani::any();
        let has_space = states[..n].iter().any(|&s| s == ChannelState::HasSpace);
        let active = states[..n]
            .iter()
            .filter(|&&s| s != ChannelState::Closed)
            .count();

        let outcome = dispatch_step(&states[..n], max_workers);

        // Guard against vacuous proofs: confirm all three arms are reachable.
        kani::cover!(
            matches!(outcome, DispatchOutcome::SentToIndex(_)),
            "SentToIndex path reachable"
        );
        kani::cover!(
            matches!(outcome, DispatchOutcome::SpawnNew),
            "SpawnNew path reachable"
        );
        kani::cover!(
            matches!(outcome, DispatchOutcome::WaitOnFront),
            "WaitOnFront path reachable"
        );
        kani::cover!(has_space, "has_space=true path exercised");
        kani::cover!(!has_space, "has_space=false path exercised");

        match outcome {
            DispatchOutcome::SentToIndex(i) => {
                // Must be a valid index pointing to a HasSpace slot.
                assert!(i < n);
                assert_eq!(states[i], ChannelState::HasSpace);
                // Must be the FIRST HasSpace worker (MRU-first ordering).
                for j in 0..i {
                    assert_ne!(states[j], ChannelState::HasSpace);
                }
            }
            DispatchOutcome::SpawnNew => {
                // Must be under the limit with no HasSpace worker.
                assert!(active < max_workers);
                assert!(!has_space);
            }
            DispatchOutcome::WaitOnFront => {
                // Must be at capacity with no HasSpace worker.
                assert_eq!(active, max_workers);
                assert!(!has_space);
            }
        }

        // HasSpace → never SpawnNew (subsumed from former proof 3)
        if has_space {
            assert!(!matches!(outcome, DispatchOutcome::SpawnNew));
        }
    }

    // -----------------------------------------------------------------------
    // Proof 5: With max_workers = 1 and a Full channel,
    //          dispatch always returns WaitOnFront (backpressure).
    // -----------------------------------------------------------------------
    #[kani::proof]
    fn verify_single_worker_full_causes_wait() {
        let states = [ChannelState::Full];
        let outcome = dispatch_step(&states, 1);
        assert_eq!(outcome, DispatchOutcome::WaitOnFront);
    }

    // -----------------------------------------------------------------------
    // Proof 6: Empty worker list with max >= 1 always triggers SpawnNew.
    // -----------------------------------------------------------------------
    #[kani::proof]
    fn verify_empty_pool_triggers_spawn() {
        let max_workers: usize = kani::any();
        kani::assume(max_workers >= 1);
        // Guard: ensure max_workers=1 AND max_workers>1 are both reachable.
        kani::cover!(max_workers == 1, "single-worker capacity exercised");
        kani::cover!(max_workers > 1, "multi-worker capacity exercised");
        let outcome = dispatch_step(&[], max_workers);
        assert_eq!(outcome, DispatchOutcome::SpawnNew);
    }

    // -----------------------------------------------------------------------
    // Proof 7: is_delivered matches the enum variant exactly.
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_delivery_outcome_is_delivered_contract() {
        let rejected = DeliveryOutcome::Rejected {
            reason: "bad request".to_owned(),
        };
        assert!(DeliveryOutcome::Delivered.is_delivered());
        for outcome in [
            rejected,
            DeliveryOutcome::RetryExhausted,
            DeliveryOutcome::TimedOut,
            DeliveryOutcome::PoolClosed,
            DeliveryOutcome::WorkerChannelClosed,
            DeliveryOutcome::NoWorkersAvailable,
            DeliveryOutcome::InternalFailure,
        ] {
            kani::cover!(
                matches!(&outcome, DeliveryOutcome::InternalFailure),
                "internal failure outcome reached"
            );
            assert!(!outcome.is_delivered());
        }
    }

    // -----------------------------------------------------------------------
    // Proof 8: only explicit Rejected outcomes count as permanent rejects.
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(8)]
    fn verify_delivery_outcome_is_permanent_reject_contract() {
        let rejected = DeliveryOutcome::Rejected {
            reason: "bad request".to_owned(),
        };
        assert!(rejected.is_permanent_reject());
        for outcome in [
            DeliveryOutcome::Delivered,
            DeliveryOutcome::RetryExhausted,
            DeliveryOutcome::TimedOut,
            DeliveryOutcome::PoolClosed,
            DeliveryOutcome::WorkerChannelClosed,
            DeliveryOutcome::NoWorkersAvailable,
            DeliveryOutcome::InternalFailure,
        ] {
            kani::cover!(
                matches!(&outcome, DeliveryOutcome::WorkerChannelClosed),
                "worker channel closed outcome reached"
            );
            assert!(!outcome.is_permanent_reject());
        }
    }
}

// ---------------------------------------------------------------------------
