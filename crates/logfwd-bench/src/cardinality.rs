pub mod cardinality_helpers {
    //! Reusable helper primitives for realistic synthetic-cardinality modeling.
    //!
    //! The goal here is not toy randomness. These helpers exist so benchmark
    //! generators can model sticky identities, heavy-tail hot keys, short bursts,
    //! and sparse optional fields without reimplementing the same ad hoc RNG logic
    //! in every workload profile.

    use std::ops::RangeInclusive;

    /// Choose a value uniformly from a slice.
    pub fn choose<'a, T>(rng: &mut fastrand::Rng, values: &'a [T]) -> &'a T {
        assert!(!values.is_empty(), "choose() requires a non-empty slice");
        &values[rng.usize(..values.len())]
    }

    /// Choose a value from weighted `(value, weight)` entries.
    ///
    /// This is useful for route-class and status-code mixtures where the profile
    /// wants explicit weights rather than a rank-based decay.
    pub fn choose_weighted<T: Copy>(rng: &mut fastrand::Rng, values: &[(T, u32)]) -> T {
        assert!(!values.is_empty(), "choose_weighted() requires entries");
        let total: u64 = values.iter().map(|(_, w)| *w as u64).sum();
        assert!(total > 0, "choose_weighted() requires positive weights");

        let mut ticket = rng.u64(..total);
        for (value, weight) in values {
            let weight = *weight as u64;
            if ticket < weight {
                return *value;
            }
            ticket -= weight;
        }

        values[values.len() - 1].0
    }

    fn sample_range(rng: &mut fastrand::Rng, range: &RangeInclusive<usize>) -> usize {
        let start = *range.start();
        let end = *range.end();
        if start >= end {
            start
        } else {
            rng.usize(start..=end)
        }
    }

    /// Rank-based skew control for heavy-tail cardinality.
    ///
    /// The first value is hottest, the last is coldest. The skew is intentionally
    /// coarse and deterministic so the same `(count, seed)` pair stays reproducible
    /// across platforms.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum Skew {
        Flat,
        Mild,
        Moderate,
        Heavy,
    }

    /// Pool with deterministic rank-based weighting.
    ///
    /// This is useful for route families, user pools, pod pools, and any other
    /// hot-key space where a small head and long tail are more realistic than
    /// uniform sampling.
    pub struct RankedPool<T> {
        items: Vec<T>,
        cumulative: Vec<u64>,
        total: u64,
    }

    impl<T> RankedPool<T> {
        pub fn new(items: Vec<T>, skew: Skew) -> Self {
            assert!(!items.is_empty(), "RankedPool requires at least one item");

            let mut cumulative = Vec::with_capacity(items.len());
            let mut total = 0u64;

            for (idx, _) in items.iter().enumerate() {
                let rank = (idx + 1) as u64;
                let weight = match skew {
                    Skew::Flat => 1,
                    // Head-heavy, but not absurd.
                    Skew::Mild => (1_024 / rank).max(1),
                    // Roughly Zipf-like over a modest set size.
                    Skew::Moderate => (65_536 / rank.saturating_mul(rank)).max(1),
                    // Strong head concentration for hot identifiers.
                    Skew::Heavy => {
                        (1_048_576 / rank.saturating_mul(rank).saturating_mul(rank)).max(1)
                    }
                };
                total += weight;
                cumulative.push(total);
            }

            Self {
                items,
                cumulative,
                total,
            }
        }

        pub fn sample_index(&self, rng: &mut fastrand::Rng) -> usize {
            let ticket = rng.u64(..self.total);
            match self.cumulative.binary_search(&ticket) {
                Ok(idx) => idx,
                Err(idx) => idx,
            }
        }

        pub fn sample<'a>(&'a self, rng: &mut fastrand::Rng) -> &'a T {
            &self.items[self.sample_index(rng)]
        }
    }

    /// Ranked pool with a lease window: the sampled item stays hot for a streak.
    pub struct LeasedRankedPool<T> {
        pool: RankedPool<T>,
        current: usize,
        remaining: usize,
        tenure: RangeInclusive<usize>,
    }

    impl<T> LeasedRankedPool<T> {
        pub fn new(items: Vec<T>, skew: Skew, tenure: RangeInclusive<usize>) -> Self {
            Self {
                pool: RankedPool::new(items, skew),
                current: 0,
                remaining: 0,
                tenure,
            }
        }

        pub fn next<'a>(&'a mut self, rng: &mut fastrand::Rng) -> &'a T {
            if self.remaining == 0 {
                self.current = self.pool.sample_index(rng);
                self.remaining = sample_range(rng, &self.tenure).max(1);
            }
            self.remaining -= 1;
            &self.pool.items[self.current]
        }
    }

    /// Pool that keeps a chosen identity stable for a sampled tenure, then rotates.
    ///
    /// Use this for sticky client IPs, long-lived users, pod identities, and other
    /// fields that should repeat in streaks rather than reshuffle every row.
    pub struct LeasePool<T> {
        items: Vec<T>,
        current: usize,
        remaining: usize,
        tenure: RangeInclusive<usize>,
    }

    impl<T> LeasePool<T> {
        pub fn new(items: Vec<T>, tenure: RangeInclusive<usize>) -> Self {
            assert!(!items.is_empty(), "LeasePool requires at least one item");
            Self {
                items,
                current: 0,
                remaining: 0,
                tenure,
            }
        }

        fn rotate(&mut self, rng: &mut fastrand::Rng) {
            if self.items.len() == 1 {
                self.current = 0;
            } else {
                let mut next = rng.usize(..self.items.len());
                if next == self.current {
                    next = (next + 1) % self.items.len();
                }
                self.current = next;
            }
            self.remaining = sample_range(rng, &self.tenure).max(1);
        }

        pub fn next<'a>(&'a mut self, rng: &mut fastrand::Rng) -> &'a T {
            if self.remaining == 0 {
                self.rotate(rng);
            }
            self.remaining -= 1;
            &self.items[self.current]
        }
    }

    /// Optional-field gate with an explicit probability.
    ///
    /// The numerator/denominator representation keeps the behavior deterministic
    /// and avoids float drift.
    #[derive(Clone, Copy, Debug)]
    pub struct Sparsity {
        numerator: u32,
        denominator: u32,
    }

    impl Sparsity {
        pub fn new(numerator: u32, denominator: u32) -> Self {
            assert!(denominator > 0, "denominator must be positive");
            assert!(numerator <= denominator, "probability must be <= 1");
            Self {
                numerator,
                denominator,
            }
        }

        pub fn enabled(&self, rng: &mut fastrand::Rng) -> bool {
            rng.u32(..self.denominator) < self.numerator
        }
    }

    /// Phase bucket used by the benchmark generators.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum SamplePhase {
        Hot,
        Warm,
        Cold,
    }

    /// Cardinality controls for the synthetic benchmark generators.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct CardinalityProfile {
        pub cardinality_scale: usize,
        pub phase_hot_tenure: RangeInclusive<usize>,
        pub phase_warm_tenure: RangeInclusive<usize>,
        pub phase_cold_tenure: RangeInclusive<usize>,
        pub session_tenure: RangeInclusive<usize>,
        pub trace_tenure: RangeInclusive<usize>,
        pub span_tenure: RangeInclusive<usize>,
        pub retry_rate_bp: u32,
        pub error_rate_bp: u32,
    }

    impl CardinalityProfile {
        pub fn infra_like() -> Self {
            Self {
                cardinality_scale: 1,
                phase_hot_tenure: 18..=64,
                phase_warm_tenure: 12..=40,
                phase_cold_tenure: 6..=18,
                session_tenure: 8..=32,
                trace_tenure: 4..=18,
                span_tenure: 1..=8,
                retry_rate_bp: 900,
                error_rate_bp: 250,
            }
        }

        pub fn for_scale(scale: usize) -> Self {
            let scale = scale.max(1);
            Self {
                cardinality_scale: scale,
                phase_hot_tenure: 12..=48 + scale * 8,
                phase_warm_tenure: 8..=32 + scale * 6,
                phase_cold_tenure: 4..=16 + scale * 4,
                session_tenure: 4..=24 + scale * 6,
                trace_tenure: 2..=16 + scale * 4,
                span_tenure: 1..=6 + scale * 2,
                retry_rate_bp: (900 + (scale as u32 * 100)).min(5_000),
                error_rate_bp: (250 + (scale as u32 * 50)).min(2_000),
            }
        }
    }

    impl Default for CardinalityProfile {
        fn default() -> Self {
            Self::infra_like()
        }
    }

    /// A single sampled row from the cardinality model.
    pub struct CardinalitySample<'a> {
        pub phase: SamplePhase,
        pub service_idx: usize,
        pub namespace_idx: usize,
        pub path_idx: usize,
        pub user_idx: usize,
        pub cluster: &'a str,
        pub node: &'a str,
        pub pod: &'a str,
        pub container: &'a str,
        pub trace_id: u128,
        pub span_id: u64,
        pub request_id: u64,
        pub session_id: u64,
        pub retry_count: u32,
        pub error_count: u32,
        pub status_code: u16,
    }

    fn sample_status(rng: &mut fastrand::Rng, phase: SamplePhase) -> u16 {
        let statuses: &[(u16, u32)] = match phase {
            SamplePhase::Hot => &[(200, 90), (201, 4), (204, 3), (404, 1), (500, 2)],
            SamplePhase::Warm => &[(200, 78), (201, 5), (204, 4), (404, 6), (500, 7)],
            SamplePhase::Cold => &[
                (200, 54),
                (404, 10),
                (429, 6),
                (500, 18),
                (502, 6),
                (503, 6),
            ],
        };
        choose_weighted(rng, statuses)
    }

    /// Stateful sampler that turns the profile into a correlated row stream.
    pub struct CardinalityState {
        profile: CardinalityProfile,
        phase_pool: LeasedRankedPool<SamplePhase>,
        service_pool: LeasedRankedPool<usize>,
        namespace_pool: LeasedRankedPool<usize>,
        path_pool: LeasedRankedPool<usize>,
        user_pool: LeasedRankedPool<usize>,
        cluster_pool: LeasedRankedPool<usize>,
        node_pool: LeasedRankedPool<usize>,
        pod_pool: LeasedRankedPool<usize>,
        container_pool: LeasedRankedPool<usize>,
        session_pool: LeasePool<u64>,
        trace_pool: LeasePool<u128>,
        span_pool: LeasePool<u64>,
        cluster_names: Vec<String>,
        node_names: Vec<String>,
        pod_names: Vec<String>,
        container_names: Vec<String>,
        request_id: u64,
    }

    impl CardinalityState {
        pub fn new(profile: CardinalityProfile) -> Self {
            let scale = profile.cardinality_scale.max(1);
            let phase_hot_tenure = profile.phase_hot_tenure.clone();
            let session_tenure = profile.session_tenure.clone();
            let trace_tenure = profile.trace_tenure.clone();
            let span_tenure = profile.span_tenure.clone();
            let cluster_count = 4 + scale * 4;
            let node_count = 8 + scale * 8;
            let pod_count = 12 + scale * 10;
            let container_count = 4 + scale * 2;
            let service_count = 5 + scale * 2;
            let namespace_count = 5 + scale * 2;
            let path_count = 8 + scale * 4;
            let user_count = 24 + scale * 16;

            let cluster_names = (0..cluster_count)
                .map(|i| format!("cluster-{i:02}"))
                .collect();
            let node_names = (0..node_count).map(|i| format!("node-{i:02}")).collect();
            let pod_names = (0..pod_count).map(|i| format!("pod-{i:03}")).collect();
            let container_names = (0..container_count)
                .map(|i| match i {
                    0 => "main".to_string(),
                    1 => "sidecar".to_string(),
                    2 => "init".to_string(),
                    3 => "envoy".to_string(),
                    _ => format!("container-{i}"),
                })
                .collect();

            Self {
                profile,
                phase_pool: LeasedRankedPool::new(
                    vec![SamplePhase::Hot, SamplePhase::Warm, SamplePhase::Cold],
                    Skew::Mild,
                    phase_hot_tenure,
                ),
                service_pool: LeasedRankedPool::new(
                    (0..service_count).collect(),
                    Skew::Moderate,
                    8..=28,
                ),
                namespace_pool: LeasedRankedPool::new(
                    (0..namespace_count).collect(),
                    Skew::Moderate,
                    14..=48,
                ),
                path_pool: LeasedRankedPool::new((0..path_count).collect(), Skew::Heavy, 4..=18),
                user_pool: LeasedRankedPool::new((0..user_count).collect(), Skew::Heavy, 6..=24),
                cluster_pool: LeasedRankedPool::new(
                    (0..cluster_count).collect(),
                    Skew::Moderate,
                    12..=48,
                ),
                node_pool: LeasedRankedPool::new(
                    (0..node_count).collect(),
                    Skew::Moderate,
                    16..=64,
                ),
                pod_pool: LeasedRankedPool::new((0..pod_count).collect(), Skew::Heavy, 8..=32),
                container_pool: LeasedRankedPool::new(
                    (0..container_count).collect(),
                    Skew::Flat,
                    32..=128,
                ),
                session_pool: LeasePool::new(
                    (0..(32 * scale))
                        .map(|i| 1_000_000_000_000_000_000u64 + (i as u64 * 97_531))
                        .collect(),
                    session_tenure,
                ),
                trace_pool: LeasePool::new(
                    (0..(64 * scale))
                        .map(|i| {
                            let x = 0x9e3779b97f4a7c15u128.wrapping_mul((i as u128) + 1);
                            x ^ ((x << 17) | (x >> 31))
                        })
                        .collect(),
                    trace_tenure,
                ),
                span_pool: LeasePool::new(
                    (0..(128 * scale))
                        .map(|i| 0xd6e8feb86659fd93u64.wrapping_mul((i as u64) + 1))
                        .collect(),
                    span_tenure,
                ),
                cluster_names,
                node_names,
                pod_names,
                container_names,
                request_id: 0,
            }
        }

        pub fn sample<'a>(&'a mut self, rng: &mut fastrand::Rng) -> CardinalitySample<'a> {
            let phase = *self.phase_pool.next(rng);
            let service_idx = *self.service_pool.next(rng);
            let namespace_idx = *self.namespace_pool.next(rng);
            let path_idx = *self.path_pool.next(rng);
            let user_idx = *self.user_pool.next(rng);
            let cluster_idx = *self.cluster_pool.next(rng);
            let node_idx = *self.node_pool.next(rng);
            let pod_idx = *self.pod_pool.next(rng);
            let container_idx = *self.container_pool.next(rng);
            let session_id = *self.session_pool.next(rng);
            let trace_id = *self.trace_pool.next(rng);
            let span_id = *self.span_pool.next(rng);
            self.request_id = self.request_id.wrapping_add(1);

            let retry_bias_bp = match phase {
                SamplePhase::Hot => self.profile.retry_rate_bp / 4,
                SamplePhase::Warm => self.profile.retry_rate_bp,
                SamplePhase::Cold => (self.profile.retry_rate_bp * 2).min(10_000),
            };
            let error_bias_bp = match phase {
                SamplePhase::Hot => self.profile.error_rate_bp / 4,
                SamplePhase::Warm => self.profile.error_rate_bp,
                SamplePhase::Cold => (self.profile.error_rate_bp * 3).min(10_000),
            };
            let retry_count = if rng.u32(..10_000) < retry_bias_bp {
                1 + rng.u32(..2)
            } else {
                0
            };
            let status_code = sample_status(rng, phase);
            let error_count = u32::from(status_code >= 500 || rng.u32(..10_000) < error_bias_bp);

            CardinalitySample {
                phase,
                service_idx,
                namespace_idx,
                path_idx,
                user_idx,
                cluster: &self.cluster_names[cluster_idx % self.cluster_names.len()],
                node: &self.node_names[node_idx % self.node_names.len()],
                pod: &self.pod_names[pod_idx % self.pod_names.len()],
                container: &self.container_names[container_idx % self.container_names.len()],
                trace_id,
                span_id,
                request_id: self.request_id,
                session_id,
                retry_count,
                error_count,
                status_code,
            }
        }
    }
}
