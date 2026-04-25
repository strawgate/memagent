mod metrics;
mod models;
mod policy;
mod process;
/// HTML rendering helpers for the diagnostics dashboard.
pub(crate) mod render;
mod server;
mod telemetry;

pub use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

pub use metrics::PipelineMetrics;
pub use models::{ActiveBatch, MemoryStats};
pub(crate) use process::process_metrics;
pub use server::{DiagnosticsServer, ServerHandle, redact_config_yaml};
