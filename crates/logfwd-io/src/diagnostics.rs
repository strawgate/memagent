mod metrics;
mod models;
mod policy;
mod process;
mod render;
mod server;
mod telemetry;

// Re-export ComponentStats from logfwd-types so existing `logfwd_io::diagnostics::ComponentStats`
// paths keep working.
pub use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

pub use metrics::PipelineMetrics;
pub use models::{ActiveBatch, MemoryStats};
pub(crate) use process::process_metrics;
pub use server::{DiagnosticsServer, ServerHandle};
