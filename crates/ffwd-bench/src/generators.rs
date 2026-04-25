//! Deterministic data generators for benchmarks.
//!
//! Every generator uses a seeded [`fastrand::Rng`] so that identical
//! `(count, seed)` pairs always produce byte-identical output. This ensures
//! reproducible benchmark results across runs and CI.

use std::fmt::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ffwd_output::BatchMetadata;

use crate::cardinality::cardinality_helpers::{CardinalityProfile, CardinalityState, SamplePhase};

pub mod cloudtrail;
pub mod otlp;

include!("generators/shared_profiles.rs");
include!("generators/envoy_access.rs");
include!("generators/cri_mixed_narrow.rs");
include!("generators/wide_metadata.rs");
include!("generators/wide_sparse.rs");

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod generator_tests;
