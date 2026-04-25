//! Host metrics input.
//!
//! This source emits Arrow `RecordBatch` rows directly with host metrics
//! (process snapshots, CPU, memory, network stats via sysinfo). It includes a
//! lightweight runtime control plane (optional JSON file reload) and explicit
//! per-platform signal families so we can iterate toward production metrics
//! without routing synthetic JSON through text decoders.

include!("host_metrics/types.rs");
include!("host_metrics/control.rs");
include!("host_metrics/collection.rs");
include!("host_metrics/batch.rs");
include!("host_metrics/input.rs");
include!("host_metrics/schema.rs");

#[cfg(test)]
mod tests {
    include!("host_metrics/tests/control.rs");
    include!("host_metrics/tests/collection.rs");
}
