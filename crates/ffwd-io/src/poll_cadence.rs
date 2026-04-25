use std::cmp;

/// Source feedback consumed by adaptive poll cadence logic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PollCadenceSignal {
    /// The last poll produced payload bytes.
    pub had_data: bool,
    /// The last poll saturated a bounded per-poll read budget.
    pub hit_read_budget: bool,
}

/// Reusable bounded adaptive polling reducer.
///
/// This keeps the "fast poll burst" behavior in one place so inputs and
/// pipeline loops can share semantics instead of open-coding them.
#[derive(Clone, Debug)]
pub struct AdaptivePollController {
    fast_polls_max: u8,
    fast_polls_remaining: u8,
}

impl AdaptivePollController {
    /// Create a controller with an upper bound on immediate repolls.
    pub const fn new(fast_polls_max: u8) -> Self {
        Self {
            fast_polls_max,
            fast_polls_remaining: 0,
        }
    }

    /// Whether callers should bypass baseline sleep and repoll immediately.
    pub const fn should_fast_poll(&self) -> bool {
        self.fast_polls_remaining > 0
    }

    /// Remaining immediate repolls in the current burst.
    pub const fn get_fast_polls_remaining(&self) -> u8 {
        self.fast_polls_remaining
    }

    /// Reset adaptive state (used when poll errors short-circuit normal flow).
    pub fn reset_fast_polls(&mut self) {
        self.fast_polls_remaining = 0;
    }

    /// Consume the latest source signal and update burst state.
    ///
    /// Policy:
    /// - read budget hit => arm a burst, then count it down on subsequent saturated polls
    /// - data without budget hit => decay burst
    /// - idle poll => disarm burst
    pub fn observe_signal(&mut self, signal: PollCadenceSignal) {
        if signal.hit_read_budget {
            if self.fast_polls_remaining == 0 {
                self.fast_polls_remaining = self.fast_polls_max;
            } else {
                self.fast_polls_remaining = self.fast_polls_remaining.saturating_sub(1);
            }
        } else if signal.had_data {
            self.fast_polls_remaining = self.fast_polls_remaining.saturating_sub(1);
        } else {
            self.fast_polls_remaining = 0;
        }
        self.fast_polls_remaining = cmp::min(self.fast_polls_remaining, self.fast_polls_max);
    }
}

#[cfg(test)]
mod tests {
    use super::{AdaptivePollController, PollCadenceSignal};
    use proptest::prelude::*;

    #[test]
    fn budget_hit_arms_full_burst() {
        let mut controller = AdaptivePollController::new(8);
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: true,
        });
        assert_eq!(controller.get_fast_polls_remaining(), 8);
        assert!(controller.should_fast_poll());
    }

    #[test]
    fn data_without_budget_decays_burst() {
        let mut controller = AdaptivePollController::new(3);
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: true,
        });
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: false,
        });
        assert_eq!(controller.get_fast_polls_remaining(), 2);
    }

    #[test]
    fn idle_disarms_burst() {
        let mut controller = AdaptivePollController::new(3);
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: true,
        });
        controller.observe_signal(PollCadenceSignal {
            had_data: false,
            hit_read_budget: false,
        });
        assert_eq!(controller.get_fast_polls_remaining(), 0);
        assert!(!controller.should_fast_poll());
    }

    #[test]
    fn repeated_budget_hits_count_down_the_burst() {
        let mut controller = AdaptivePollController::new(3);
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: true,
        });
        assert_eq!(controller.get_fast_polls_remaining(), 3);
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: true,
        });
        assert_eq!(controller.get_fast_polls_remaining(), 2);
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: true,
        });
        assert_eq!(controller.get_fast_polls_remaining(), 1);
        controller.observe_signal(PollCadenceSignal {
            had_data: true,
            hit_read_budget: true,
        });
        assert_eq!(controller.get_fast_polls_remaining(), 0);
        assert!(!controller.should_fast_poll());
    }

    proptest! {
        #[test]
        fn burst_state_is_always_bounded(
            max in 0u8..=32,
            signals in proptest::collection::vec((any::<bool>(), any::<bool>()), 0..256),
        ) {
            let mut controller = AdaptivePollController::new(max);
            for (had_data, hit_read_budget) in signals {
                controller.observe_signal(PollCadenceSignal {
                    had_data,
                    hit_read_budget,
                });
                prop_assert!(controller.get_fast_polls_remaining() <= max);
            }
        }
    }
}
