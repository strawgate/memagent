mod metrics;
mod models;
mod policy;
mod process;
mod render;
mod server;

// Re-export ComponentStats from logfwd-types so existing `logfwd_io::diagnostics::ComponentStats`
// paths keep working.
pub use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

pub use metrics::PipelineMetrics;
pub use models::{ActiveBatch, MemoryStats};
pub use server::{DiagnosticsServer, ServerHandle};
