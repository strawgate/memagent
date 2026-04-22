//! Host metrics input.
//!
//! This source emits Arrow `RecordBatch` rows directly with host metrics
//! (process snapshots, CPU, memory, network stats via sysinfo). It includes a
//! lightweight runtime control plane (optional JSON file reload) and explicit
//! per-platform signal families so we can iterate toward production metrics
//! without routing synthetic JSON through text decoders.

include!("host_metrics_parts/part_1.rs");
include!("host_metrics_parts/part_2.rs");
include!("host_metrics_parts/part_3.rs");
include!("host_metrics_parts/part_4.rs");
include!("host_metrics_parts/part_5.rs");
include!("host_metrics_parts/part_6.rs");

#[cfg(test)]
mod tests {
    include!("host_metrics_parts/tests_1.rs");
    include!("host_metrics_parts/tests_2.rs");
}
