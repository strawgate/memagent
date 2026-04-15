//! Deterministic data generators for benchmarks.
//!
//! All generators are re-exported from `logfwd-io`. See that crate for
//! implementation details and documentation.

use std::sync::Arc;

use logfwd_output::BatchMetadata;

pub use logfwd_io::generator::cardinality::cardinality_helpers::{
    CardinalityProfile, CardinalityState, SamplePhase,
};
pub use logfwd_io::generator::cri::{
    gen_cri_k8s, gen_narrow, gen_narrow_batch, gen_production_mixed, gen_production_mixed_batch,
};
pub use logfwd_io::generator::envoy::{
    EnvoyAccessProfile, gen_envoy_access, gen_envoy_access_batch,
    gen_envoy_access_batch_with_profile, gen_envoy_access_with_profile,
};
pub use logfwd_io::generator::shared::{
    CloudTrailProfile, CloudTrailRegionMix, CloudTrailServiceMix,
};
pub use logfwd_io::generator::wide::{gen_wide, gen_wide_batch};

pub mod cloudtrail;

/// Create benchmark-standard `BatchMetadata` with typical K8s resource attributes.
pub fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::new(vec![
            ("service.name".into(), "bench-service".into()),
            ("service.version".into(), "1.0.0".into()),
            ("host.name".into(), "bench-node-01".into()),
        ]),
        observed_time_ns: 1_705_312_200_000_000_000, // 2024-01-15T10:30:00Z
    }
}
