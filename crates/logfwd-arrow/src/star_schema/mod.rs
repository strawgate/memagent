//! Flat ↔ OTAP star schema conversion for Arrow RecordBatches.
//!
//! logfwd uses a **flat schema**: one `RecordBatch` with all fields as columns.
//! Resource attributes are prefixed with `resource.attributes.*`. This is directly
//! queryable by DuckDB, Polars, DataFusion with zero schema knowledge.
//!
//! OTAP uses a **star schema**: 4 tables with foreign keys (LOGS fact table +
//! RESOURCE_ATTRS / SCOPE_ATTRS / LOG_ATTRS dimension tables). This is
//! optimized for wire efficiency.
//!
//! This module converts between the two representations at the boundary.

mod flat_to_star;
pub(crate) mod helpers;
mod star_to_flat;
#[cfg(test)]
mod tests;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use logfwd_types::field_names;

pub use flat_to_star::flat_to_star;
pub use helpers::chrono_timestamp;
pub use star_to_flat::star_to_flat;

/// OTAP star schema representation for Logs.
///
/// Contains a fact table (`logs`) and three dimension tables for attributes.
pub struct StarSchema {
    /// LOGS fact table: id, resource_id, scope_id, time_unix_nano,
    /// severity_number, severity_text, body_str, trace_id, span_id, flags,
    /// dropped_attributes_count.
    pub logs: RecordBatch,
    /// LOG_ATTRS dimension table: parent_id, key, type, str, int, double,
    /// bool, bytes.
    pub log_attrs: RecordBatch,
    /// RESOURCE_ATTRS dimension table: parent_id, key, type, str, int, double,
    /// bool, bytes.
    pub resource_attrs: RecordBatch,
    /// SCOPE_ATTRS dimension table: parent_id, key, type, str, int, double,
    /// bool, bytes.
    pub scope_attrs: RecordBatch,
}

// ---------------------------------------------------------------------------
// Well-known column name mappings
// ---------------------------------------------------------------------------
// Canonical names and heuristic variants are defined in
// `logfwd_types::field_names` — the single source of truth.  The helpers
// below delegate to `field_names::matches_any` so that adding a new variant
// in one place automatically propagates here.

/// Returns `true` when `name` matches a well-known timestamp column.
// `@timestamp` (Elasticsearch convention) is intentionally included.  Both
// `field_names::TIMESTAMP_VARIANTS` and the Loki/ES sinks recognise it; the
// OTAP conversion must be consistent.  Fixes #1669.
fn is_well_known_timestamp(name: &str) -> bool {
    field_names::matches_any(
        name,
        field_names::TIMESTAMP,
        field_names::TIMESTAMP_VARIANTS,
    )
}

/// Returns `true` when `name` matches a well-known severity column.
fn is_well_known_severity(name: &str) -> bool {
    field_names::matches_any(name, field_names::SEVERITY, field_names::SEVERITY_VARIANTS)
}

/// Returns `true` when `name` matches a well-known body/message column.
fn is_well_known_body(name: &str) -> bool {
    field_names::matches_any(name, field_names::BODY, field_names::BODY_VARIANTS)
}

/// Returns `true` when `name` matches a well-known trace-ID column.
fn is_well_known_trace_id(name: &str) -> bool {
    name == field_names::TRACE_ID
}

/// Returns `true` when `name` matches a well-known span-ID column.
fn is_well_known_span_id(name: &str) -> bool {
    name == field_names::SPAN_ID
}

/// Returns `true` when `name` matches a well-known flags column.
fn is_well_known_flags(name: &str) -> bool {
    field_names::matches_any(
        name,
        field_names::TRACE_FLAGS,
        field_names::TRACE_FLAGS_VARIANTS,
    )
}

/// Canonical resource attribute prefix shared across receivers/sinks.
const RESOURCE_PREFIX: &str = field_names::DEFAULT_RESOURCE_PREFIX;

// ---------------------------------------------------------------------------
// Attribute type tags (stored in the `type` column of attrs tables)
// ---------------------------------------------------------------------------

const ATTR_TYPE_STR: u8 = 0;
const ATTR_TYPE_INT: u8 = 1;
const ATTR_TYPE_DOUBLE: u8 = 2;
const ATTR_TYPE_BOOL: u8 = 3;
const ATTR_TYPE_BYTES: u8 = 4;

// ---------------------------------------------------------------------------
// Schema builders
// ---------------------------------------------------------------------------

fn logs_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("resource_id", DataType::UInt32, false),
        Field::new("scope_id", DataType::UInt32, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body_str", DataType::Utf8, true),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ])
}

/// Schema for dimension tables (LOG_ATTRS, RESOURCE_ATTRS, SCOPE_ATTRS).
pub fn attrs_schema() -> Schema {
    Schema::new(vec![
        Field::new("parent_id", DataType::UInt32, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::UInt8, false),
        Field::new("str", DataType::Utf8, true),
        Field::new("int", DataType::Int64, true),
        Field::new("double", DataType::Float64, true),
        Field::new("bool", DataType::Boolean, true),
        Field::new("bytes", DataType::Binary, true),
    ])
}
