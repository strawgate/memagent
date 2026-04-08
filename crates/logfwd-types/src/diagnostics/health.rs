//! Pure health semantics for diagnostics components.
//!
//! This module intentionally contains no atomics or runtime storage. It exists
//! so control-plane aggregation can be reasoned about and proved separately
//! from the hot-path counters in [`super::ComponentStats`].

/// Coarse runtime health for one pipeline component.
///
/// The numeric representation is stable for lock-free storage and intentionally
/// ordered by readiness so `combine` can keep the less-ready value via `max`.
/// Lower repr = more ready, higher repr = less ready.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ComponentHealth {
    /// Component is healthy and able to participate in the pipeline.
    Healthy = 0,
    /// Component is functioning but degraded (for example, retrying).
    Degraded = 1,
    /// Component exists but is still starting up or binding resources.
    Starting = 2,
    /// Component is shutting down and should no longer be considered ready.
    Stopping = 3,
    /// Component has stopped and is not available for work.
    Stopped = 4,
    /// Component hit a fatal condition and is not able to make progress.
    Failed = 5,
}

impl ComponentHealth {
    /// Stable lowercase string used in diagnostics JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }

    /// Stable wire/storage representation used by lock-free atomics.
    #[must_use]
    pub const fn as_repr(self) -> u8 {
        self as u8
    }

    /// Convert from the stable wire/storage representation.
    ///
    /// Unknown values degrade safely to `Failed`.
    #[must_use]
    pub const fn from_repr(value: u8) -> Self {
        match value {
            0 => Self::Healthy,
            1 => Self::Degraded,
            2 => Self::Starting,
            3 => Self::Stopping,
            4 => Self::Stopped,
            5 => Self::Failed,
            _ => Self::Failed,
        }
    }

    /// Returns `true` when the component should count toward readiness.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }

    /// Combine two component states by keeping the less-ready one.
    #[must_use]
    pub const fn combine(self, other: Self) -> Self {
        if self.as_repr() >= other.as_repr() {
            self
        } else {
            other
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ComponentHealth;

    #[test]
    fn as_str_matches_stable_json_contract() {
        assert_eq!(ComponentHealth::Starting.as_str(), "starting");
        assert_eq!(ComponentHealth::Healthy.as_str(), "healthy");
        assert_eq!(ComponentHealth::Degraded.as_str(), "degraded");
        assert_eq!(ComponentHealth::Stopping.as_str(), "stopping");
        assert_eq!(ComponentHealth::Stopped.as_str(), "stopped");
        assert_eq!(ComponentHealth::Failed.as_str(), "failed");
    }

    #[test]
    fn readiness_matches_expected_states() {
        assert!(!ComponentHealth::Starting.is_ready());
        assert!(ComponentHealth::Healthy.is_ready());
        assert!(ComponentHealth::Degraded.is_ready());
        assert!(!ComponentHealth::Stopping.is_ready());
        assert!(!ComponentHealth::Stopped.is_ready());
        assert!(!ComponentHealth::Failed.is_ready());
    }

    #[test]
    fn combine_keeps_less_ready_state() {
        assert_eq!(
            ComponentHealth::Healthy.combine(ComponentHealth::Degraded),
            ComponentHealth::Degraded
        );
        assert_eq!(
            ComponentHealth::Stopping.combine(ComponentHealth::Healthy),
            ComponentHealth::Stopping
        );
        assert_eq!(
            ComponentHealth::Failed.combine(ComponentHealth::Starting),
            ComponentHealth::Failed
        );
        // Bug #1 regression: Starting is less ready than Healthy.
        assert_eq!(
            ComponentHealth::Healthy.combine(ComponentHealth::Starting),
            ComponentHealth::Starting
        );
    }

    #[test]
    fn repr_roundtrip_is_stable() {
        for health in [
            ComponentHealth::Starting,
            ComponentHealth::Healthy,
            ComponentHealth::Degraded,
            ComponentHealth::Stopping,
            ComponentHealth::Stopped,
            ComponentHealth::Failed,
        ] {
            assert_eq!(ComponentHealth::from_repr(health.as_repr()), health);
        }
    }

    #[test]
    fn invalid_repr_degrades_to_failed() {
        assert_eq!(ComponentHealth::from_repr(255), ComponentHealth::Failed);
    }
}

#[cfg(kani)]
mod verification {
    use super::ComponentHealth;

    #[kani::proof]
    fn verify_combine_is_commutative() {
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        assert_eq!(a.combine(b), b.combine(a));

        kani::cover!(
            a == ComponentHealth::Healthy && b == ComponentHealth::Starting,
            "healthy+starting"
        );
        kani::cover!(
            a == ComponentHealth::Failed && b == ComponentHealth::Healthy,
            "failed+healthy"
        );
    }

    #[kani::proof]
    fn verify_combine_returns_the_less_ready_state() {
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        let out = a.combine(b);

        // Result is at least as less-ready as both inputs.
        assert!(out.as_repr() >= a.as_repr());
        assert!(out.as_repr() >= b.as_repr());

        // Result is one of the two inputs, not some fabricated value.
        assert!(out == a || out == b);

        kani::cover!(matches!(out, ComponentHealth::Healthy), "healthy reachable");
        kani::cover!(matches!(out, ComponentHealth::Failed), "failed reachable");
        kani::cover!(
            a == ComponentHealth::Healthy && b == ComponentHealth::Starting,
            "healthy+starting"
        );
    }

    #[kani::proof]
    fn verify_combine_is_associative() {
        let a = ComponentHealth::from_repr(kani::any());
        let b = ComponentHealth::from_repr(kani::any());
        let c = ComponentHealth::from_repr(kani::any());

        assert_eq!(a.combine(b.combine(c)), a.combine(b).combine(c));

        kani::cover!(
            a == ComponentHealth::Healthy
                && b == ComponentHealth::Starting
                && c == ComponentHealth::Failed,
            "healthy+starting+failed"
        );
    }

    #[kani::proof]
    fn verify_ready_states_are_exact() {
        let h = ComponentHealth::from_repr(kani::any());
        assert_eq!(
            h.is_ready(),
            matches!(h, ComponentHealth::Healthy | ComponentHealth::Degraded)
        );

        kani::cover!(h.is_ready(), "ready branch reachable");
        kani::cover!(!h.is_ready(), "not-ready branch reachable");
    }
}
