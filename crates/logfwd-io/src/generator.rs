//! Synthetic data generator input source.
//!
//! Produces JSON log lines at a configurable rate. Used for benchmarking
//! and testing pipelines without external data sources.

include!("generator_parts/part_1.rs");
include!("generator_parts/part_2.rs");
include!("generator_parts/part_3.rs");
include!("generator_parts/part_4.rs");

#[cfg(test)]
mod tests {
    include!("generator_parts/tests_1.rs");
    include!("generator_parts/tests_2.rs");
    include!("generator_parts/tests_3.rs");
}
