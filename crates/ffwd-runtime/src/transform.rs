//! Transform abstraction for pipeline processing.
//!
//! Defines the `Transform` trait that abstracts SQL transform execution.
//! Implementations are DataFusion-based SQL transforms and a passthrough stub.

use arrow::record_batch::RecordBatch;
use ffwd_core::scan_config::ScanConfig;
use ffwd_types::source_metadata::SourceMetadataPlan;

#[cfg(feature = "datafusion")]
pub use ffwd_transform::SqlTransform;

#[cfg(feature = "datafusion")]
pub mod enrichment {
    pub use ffwd_transform::enrichment::*;
}

#[cfg(feature = "datafusion")]
pub mod udf {
    pub mod geo_lookup {
        pub use ffwd_transform::udf::geo_lookup::MmdbDatabase;
    }
    pub use ffwd_transform::udf::CsvRangeDatabase;
}

/// Unified error type for transform operations.
#[derive(Debug, Clone)]
pub struct TransformError {
    message: String,
}

impl TransformError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    #[cfg(feature = "datafusion")]
    pub fn from_df_error(e: ffwd_transform::TransformError) -> Self {
        Self::new(e.to_string())
    }
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TransformError {}

/// Transform trait for pipeline batch processing.
///
/// Implementations handle SQL transform execution against Arrow RecordBatches.
pub trait Transform: Send {
    fn execute_blocking(&mut self, batch: RecordBatch) -> Result<RecordBatch, TransformError>;

    fn scan_config(&self) -> ScanConfig;

    fn source_metadata_plan(&self) -> SourceMetadataPlan;

    fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan;

    fn validate_plan(&mut self) -> Result<(), TransformError>;
}

/// Create a new transform based on the datafusion feature flag.
///
/// When datafusion is enabled, delegates to DataFusion SqlTransform.
/// When disabled, creates a passthrough transform that only accepts `SELECT * FROM logs`.
#[cfg(feature = "datafusion")]
pub fn create_transform(sql: &str) -> Result<Box<dyn Transform>, TransformError> {
    ConfiguredSqlTransform::new(sql)?.build()
}

#[cfg(not(feature = "datafusion"))]
pub fn create_transform(sql: &str) -> Result<Box<dyn Transform>, TransformError> {
    PassthroughTransform::new(sql).map(|t| Box::new(t) as Box<dyn Transform>)
}

/// Builder for DataFusion SqlTransform with enrichment configuration.
///
/// Allows setting geo database and enrichment tables before building
/// a `Box<dyn Transform>` for the pipeline.
#[cfg(feature = "datafusion")]
pub struct ConfiguredSqlTransform {
    inner: SqlTransform,
}

#[cfg(feature = "datafusion")]
impl ConfiguredSqlTransform {
    pub fn new(sql: &str) -> Result<Self, TransformError> {
        SqlTransform::new(sql)
            .map(|inner| Self { inner })
            .map_err(TransformError::from_df_error)
    }

    pub fn set_geo_database(&mut self, db: std::sync::Arc<dyn enrichment::GeoDatabase>) {
        self.inner.set_geo_database(db);
    }

    pub fn add_enrichment_table(
        &mut self,
        table: std::sync::Arc<dyn enrichment::EnrichmentTable>,
    ) -> Result<(), TransformError> {
        self.inner
            .add_enrichment_table(table)
            .map_err(TransformError::from_df_error)
    }

    pub fn scan_config(&self) -> ScanConfig {
        self.inner.scan_config()
    }

    pub fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan {
        self.inner.analyzer().explicit_source_metadata_plan()
    }

    pub fn build(self) -> Result<Box<dyn Transform>, TransformError> {
        Ok(Box::new(self.inner) as Box<dyn Transform>)
    }
}

#[cfg(not(feature = "datafusion"))]
mod passthrough {
    use super::*;

    pub struct PassthroughTransform;

    impl PassthroughTransform {
        pub fn new(sql: &str) -> Result<Self, TransformError> {
            if !is_passthrough_sql(sql) {
                let preview = sql.trim().lines().next().unwrap_or("").trim();
                return Err(TransformError::new(format!(
                    "SQL transforms require DataFusion. Build the full package \
                     (default) or add `--features datafusion` (unsupported SQL: {preview})"
                )));
            }
            Ok(Self)
        }
    }

    impl Transform for PassthroughTransform {
        fn execute_blocking(&mut self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
            Ok(batch)
        }

        fn scan_config(&self) -> ScanConfig {
            ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                line_field_name: None,
                validate_utf8: false,
                row_predicate: None,
            }
        }

        fn source_metadata_plan(&self) -> SourceMetadataPlan {
            SourceMetadataPlan::default()
        }

        fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan {
            SourceMetadataPlan::default()
        }

        fn validate_plan(&mut self) -> Result<(), TransformError> {
            Ok(())
        }
    }

    fn is_passthrough_sql(sql: &str) -> bool {
        let sql = sql.trim();
        if sql.is_empty() {
            return true;
        }
        let sql = sql.trim_end_matches(';').trim();
        let normalized = sql
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();
        normalized == "SELECT * FROM LOGS"
    }
}

#[cfg(feature = "datafusion")]
impl Transform for SqlTransform {
    fn execute_blocking(&mut self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        use ffwd_transform::SqlTransform as DfSqlTransform;
        DfSqlTransform::execute_blocking(self, batch).map_err(TransformError::from_df_error)
    }

    fn scan_config(&self) -> ScanConfig {
        use ffwd_transform::SqlTransform as DfSqlTransform;
        DfSqlTransform::scan_config(self)
    }

    fn source_metadata_plan(&self) -> SourceMetadataPlan {
        self.analyzer().source_metadata_plan()
    }

    fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan {
        self.analyzer().explicit_source_metadata_plan()
    }

    fn validate_plan(&mut self) -> Result<(), TransformError> {
        use ffwd_transform::SqlTransform as DfSqlTransform;
        DfSqlTransform::validate_plan(self).map_err(TransformError::from_df_error)
    }
}

#[cfg(not(feature = "datafusion"))]
pub use passthrough::PassthroughTransform;