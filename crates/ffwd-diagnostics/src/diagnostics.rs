mod metrics;
mod models;
mod policy;
mod process;
mod render;
mod server;
mod telemetry;

pub use ffwd_types::diagnostics::{ComponentHealth, ComponentStats};

pub use metrics::PipelineMetrics;
pub use models::{ActiveBatch, MemoryStats};
pub(crate) use process::process_metrics;
pub use server::{DiagnosticsServer, ServerHandle, redact_config_yaml};
