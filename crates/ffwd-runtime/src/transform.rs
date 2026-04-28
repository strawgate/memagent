//! Transform abstraction for pipeline processing.
//!
//! Defines the `Transform` trait that abstracts SQL transform execution.
//! Implementations are DataFusion-based SQL transforms and a passthrough stub.

use std::future::Future;
use std::pin::Pin;

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
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransformError {
    #[error("transform error: {0}")]
    Message(String),
    #[cfg(feature = "datafusion")]
    #[error("datafusion error: {0}")]
    DataFusion(String),
}

impl TransformError {
    /// Creates a new error with a message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        TransformError::Message(message.into())
    }

    /// Creates an error from a DataFusion transform error.
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn from_df_error(e: ffwd_transform::TransformError) -> Self {
        TransformError::DataFusion(e.to_string())
    }
}

/// Transform trait for pipeline batch processing.
///
/// Implementations handle SQL transform execution against Arrow RecordBatches.
pub trait Transform: Send {
    /// Execute the transform on a record batch asynchronously.
    ///
    /// This method enables direct `.await` usage in async contexts, avoiding
    /// the overhead of creating a runtime per call in turmoil/simulation.
    fn execute_async(
        &mut self,
        batch: RecordBatch,
    ) -> Pin<Box<dyn futures_util::Future<Output = Result<RecordBatch, TransformError>> + Send + '_>>;

    /// Execute the transform on a record batch synchronously.
    ///
    /// For callers that cannot use async (e.g., plain thread context),
    /// this delegates to [`execute_async`](Self::execute_async) via a runtime.
    fn execute_blocking(&mut self, batch: RecordBatch) -> Result<RecordBatch, TransformError>;

    /// Return the scan configuration for field pushdown.
    fn scan_config(&self) -> ScanConfig;

    /// Return the source metadata columns required by this transform.
    fn source_metadata_plan(&self) -> SourceMetadataPlan;

    /// Return source metadata columns explicitly referenced in the SQL.
    fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan;

    /// Validate the transform plan by executing against a dummy batch.
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

/// Create a new transform based on the datafusion feature flag.
///
/// When datafusion is enabled, delegates to DataFusion SqlTransform.
/// When disabled, creates a passthrough transform that only accepts `SELECT * FROM logs`.
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
    /// Create a new configured transform from SQL.
    pub fn new(sql: &str) -> Result<Self, TransformError> {
        SqlTransform::new(sql)
            .map(|inner| Self { inner })
            .map_err(TransformError::from_df_error)
    }

    /// Set the geo-IP database for the `geo_lookup()` UDF.
    pub fn set_geo_database(&mut self, db: std::sync::Arc<dyn enrichment::GeoDatabase>) {
        self.inner.set_geo_database(db);
    }

    /// Add an enrichment table to be registered alongside `logs`.
    pub fn add_enrichment_table(
        &mut self,
        table: std::sync::Arc<dyn enrichment::EnrichmentTable>,
    ) -> Result<(), TransformError> {
        self.inner
            .add_enrichment_table(table)
            .map_err(TransformError::from_df_error)
    }

    /// Return the scan configuration for field pushdown.
    pub fn scan_config(&self) -> ScanConfig {
        self.inner.scan_config()
    }

    /// Return source metadata columns explicitly referenced in the SQL.
    pub fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan {
        self.inner.analyzer().explicit_source_metadata_plan()
    }

    /// Build into a `Box<dyn Transform>`.
    pub fn build(self) -> Result<Box<dyn Transform>, TransformError> {
        Ok(Box::new(self.inner) as Box<dyn Transform>)
    }
}

#[cfg(not(feature = "datafusion"))]
mod passthrough {
    use super::*;

    /// Passthrough transform that returns batches unchanged.
    ///
    /// Used when datafusion is disabled; only accepts `SELECT * FROM logs`.
    pub struct PassthroughTransform;

    impl PassthroughTransform {
        /// Create a new passthrough transform.
        ///
        /// Returns an error if the SQL is not `SELECT * FROM logs`.
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
        fn execute_async(
            &mut self,
            batch: RecordBatch,
        ) -> Pin<Box<dyn Future<Output = Result<RecordBatch, TransformError>> + Send + '_>>
        {
            Box::pin(std::future::ready(Ok(batch)))
        }

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
    fn execute_async(
        &mut self,
        batch: RecordBatch,
    ) -> Pin<Box<dyn Future<Output = Result<RecordBatch, TransformError>> + Send + '_>> {
        use ffwd_transform::SqlTransform as DfSqlTransform;
        let fut = async move {
            let result = DfSqlTransform::execute(self, batch).await;
            result.map_err(TransformError::from_df_error)
        };
        Box::pin(fut)
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .expect("valid test batch")
    }

    #[test]
    fn transform_error_new_creates_message_variant() {
        let err = TransformError::new("something went wrong");
        assert_eq!(err.to_string(), "transform error: something went wrong");
    }

    #[test]
    fn transform_error_display_formats_correctly() {
        let err = TransformError::Message("test error".to_string());
        assert!(err.to_string().contains("test error"));
    }

    #[tokio::test]
    async fn create_transform_trait_object_executes_async() {
        let mut transform = create_transform("SELECT * FROM logs").expect("create transform");
        let result = transform
            .execute_async(make_test_batch())
            .await
            .expect("execute transform");
        assert_eq!(result.num_rows(), 3);
    }

    /// Tests for the `PassthroughTransform` — only compiled when DataFusion is absent.
    #[cfg(not(feature = "datafusion"))]
    mod passthrough_tests {
        use super::*;

        #[test]
        fn passthrough_accepts_select_star() {
            assert!(PassthroughTransform::new("SELECT * FROM logs").is_ok());
        }

        #[test]
        fn passthrough_accepts_empty_sql() {
            assert!(PassthroughTransform::new("").is_ok());
        }

        #[test]
        fn passthrough_accepts_select_star_with_semicolon() {
            assert!(PassthroughTransform::new("SELECT * FROM logs;").is_ok());
        }

        #[test]
        fn passthrough_rejects_non_passthrough_sql() {
            let err = PassthroughTransform::new("SELECT id FROM logs").unwrap_err();
            assert!(err.to_string().contains("DataFusion"));
        }

        #[test]
        fn passthrough_execute_blocking_passes_batch_through() {
            let mut transform =
                PassthroughTransform::new("SELECT * FROM logs").expect("passthrough sql");
            let result = transform
                .execute_blocking(make_test_batch())
                .expect("execute");
            assert_eq!(result.num_rows(), 3);
        }

        #[test]
        fn passthrough_validate_plan_succeeds() {
            let mut transform =
                PassthroughTransform::new("SELECT * FROM logs").expect("passthrough sql");
            assert!(transform.validate_plan().is_ok());
        }
    }
}
