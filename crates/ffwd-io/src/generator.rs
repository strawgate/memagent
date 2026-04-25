//! Synthetic data generator input source.
//!
//! Produces JSON log lines at a configurable rate. Used for benchmarking
//! and testing pipelines without external data sources.

#![allow(clippy::indexing_slicing)]

include!("generator/types.rs");
include!("generator/logs.rs");
include!("generator/input.rs");
include!("generator/encoding.rs");

#[cfg(test)]
mod tests {
    pub(super) use super::*;

    #[path = "basic.rs"]
    mod basic;
    #[path = "common.rs"]
    mod common;
    #[path = "record_profile.rs"]
    mod record_profile;
    #[path = "timestamps.rs"]
    mod timestamps;
}
