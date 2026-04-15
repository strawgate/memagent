// Re-export from logfwd-runtime to avoid duplication.
// When DataFusion is enabled, use the full implementation.
// When DataFusion is disabled, use the passthrough stub.
pub use logfwd_runtime::transform::{QueryAnalyzer, SqlTransform, TransformError};

#[cfg(feature = "datafusion")]
pub mod enrichment {
    pub use logfwd_transform::enrichment::*;
}

#[cfg(feature = "datafusion")]
pub mod udf {
    pub mod geo_lookup {
        pub use logfwd_transform::udf::geo_lookup::MmdbDatabase;
    }
    /// CSV IP-range geo database backend for CLI facade wiring.
    pub use logfwd_transform::udf::CsvRangeDatabase;
}
