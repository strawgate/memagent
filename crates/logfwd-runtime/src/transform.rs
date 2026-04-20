#[cfg(feature = "datafusion")]
pub use logfwd_transform::SqlTransform;

#[cfg(feature = "datafusion")]
pub mod enrichment {
    pub use logfwd_transform::enrichment::*;
}

#[cfg(feature = "datafusion")]
pub mod udf {
    pub mod geo_lookup {
        pub use logfwd_transform::udf::geo_lookup::MmdbDatabase;
    }
    /// CSV IP-range geo database backend for runtime pipeline wiring.
    pub use logfwd_transform::udf::CsvRangeDatabase;
}

#[cfg(not(feature = "datafusion"))]
mod passthrough {
    use std::collections::HashSet;
    use std::fmt;

    use arrow::record_batch::RecordBatch;
    use logfwd_core::scan_config::ScanConfig;
    use logfwd_types::source_metadata::SourceMetadataPlan;

    /// Error returned when passthrough transform construction or execution fails.
    #[derive(Debug, Clone)]
    pub struct TransformError {
        message: String,
    }

    impl TransformError {
        fn new(message: impl Into<String>) -> Self {
            Self {
                message: message.into(),
            }
        }
    }

    impl fmt::Display for TransformError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.message)
        }
    }

    impl std::error::Error for TransformError {}

    /// Minimal analyzer surface for passthrough builds without DataFusion support.
    pub struct QueryAnalyzer {
        /// Referenced columns inferred from the parsed query.
        pub referenced_columns: HashSet<String>,
    }

    /// Passthrough SQL transform stub used when the `datafusion` feature is disabled.
    pub struct SqlTransform {
        analyzer: QueryAnalyzer,
    }

    impl SqlTransform {
        pub fn new(sql: &str) -> Result<Self, TransformError> {
            if !is_passthrough_sql(sql) {
                let preview = sql.trim().lines().next().unwrap_or("").trim();
                return Err(TransformError::new(format!(
                    "SQL transforms require DataFusion. Build the full package \
                     (default) or add `--features datafusion` (unsupported SQL: {preview})"
                )));
            }
            Ok(Self {
                analyzer: QueryAnalyzer {
                    referenced_columns: HashSet::new(),
                },
            })
        }

        pub async fn execute(&mut self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
            Ok(batch)
        }

        pub fn execute_blocking(
            &mut self,
            batch: RecordBatch,
        ) -> Result<RecordBatch, TransformError> {
            Ok(batch)
        }

        pub fn scan_config(&self) -> ScanConfig {
            ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                line_field_name: None,
                validate_utf8: false,
            }
        }

        pub fn analyzer(&self) -> &QueryAnalyzer {
            &self.analyzer
        }

        pub fn validate_plan(&mut self) -> Result<(), TransformError> {
            Ok(())
        }
    }

    impl QueryAnalyzer {
        /// Return the metadata columns the passthrough transform needs.
        ///
        /// The passthrough transform accepts only `SELECT * FROM logs`, which
        /// never widens results with source metadata.
        pub fn source_metadata_plan(&self) -> SourceMetadataPlan {
            SourceMetadataPlan::default()
        }

        /// Return metadata columns explicitly referenced by SQL.
        ///
        /// The passthrough analyzer has no SQL expression tree, so it reports
        /// no explicit metadata references.
        pub fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan {
            SourceMetadataPlan::default()
        }

        /// Return whether the transform needs source paths attached.
        pub fn source_path_required(&self) -> bool {
            self.source_metadata_plan().has_source_path()
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

#[cfg(not(feature = "datafusion"))]
pub use passthrough::{QueryAnalyzer, SqlTransform, TransformError};
