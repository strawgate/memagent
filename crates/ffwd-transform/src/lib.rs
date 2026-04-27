//! DataFusion SQL transform layer.
//!
//! Takes a user's SQL string, analyzes it at startup, compiles a DataFusion
//! execution plan, and executes it against Arrow RecordBatches from the scanner.

// SQL transform: provably-bounded indexing in UDF helpers and column access.
#![allow(clippy::indexing_slicing, clippy::expect_used)]

pub mod enrichment;
pub mod error;
pub mod udf;

mod cast_udf;
mod query_analyzer;
mod sql_transform;

pub use error::TransformError;
pub use query_analyzer::QueryAnalyzer;
pub use sql_transform::SqlTransform;

#[cfg(test)]
mod tests;
