//! Per-crate error types for the ffwd-transform layer.

use thiserror::Error;

/// Errors from SQL transforms, enrichment table setup, or DataFusion operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransformError {
    /// SQL parsing, planning, or execution error.
    #[error("SQL error: {0}")]
    Sql(String),
    /// Enrichment table loading or registration error.
    #[error("enrichment error: {0}")]
    Enrichment(String),
    /// Arrow operation error (schema mismatch, batch construction, etc.).
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}
