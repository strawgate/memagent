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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use logfwd_types::field_names;

use crate::conflict_schema::{has_conflict_struct_columns, normalize_conflict_columns};

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

// ---------------------------------------------------------------------------
// flat_to_star
// ---------------------------------------------------------------------------

/// Convert logfwd's flat `RecordBatch` to OTAP star schema.
///
/// 1. Normalizes conflict struct columns (e.g. `status: Struct { int, str }`)
///    to flat Utf8 columns so that their values are preserved in LOG_ATTRS.
///    Without this, `build_log_attrs` would see a StructArray, call
///    `str_value_at` which returns `""` for unknown types, and emit a NULL
///    attr row — silently dropping the field value.
/// 2. Scans columns for `resource.attributes.*` prefix, extracts unique resource
///    attribute sets, assigns `resource_id`, builds RESOURCE_ATTRS table.
/// 3. Maps well-known columns to LOGS fact table fields.
/// 4. Remaining columns become LOG_ATTRS rows (column-to-row pivot).
/// 5. Builds SCOPE_ATTRS: empty when real `scope.*` columns exist (they flow
///    through LOG_ATTRS instead), or a default `scope_name="logfwd"` row otherwise.
/// 6. Builds the LOGS fact table with foreign keys.
pub fn flat_to_star(batch: &RecordBatch) -> Result<StarSchema, ArrowError> {
    let num_rows = batch.num_rows();

    if num_rows == 0 {
        return Ok(empty_star_schema());
    }

    // Normalize conflict struct columns to flat Utf8 before any further
    // processing.  Conflict structs arise when the scanner sees a field with
    // mixed types across rows (e.g. status=200 in one row, status="OK" in
    // another).  Without normalization, `build_log_attrs` cannot read their
    // values and silently emits NULL attr rows.
    let normalized;
    let batch = if has_conflict_struct_columns(batch.schema().as_ref()) {
        normalized = normalize_conflict_columns(batch.clone());
        &normalized
    } else {
        batch
    };

    let schema = batch.schema();

    // Classify columns.
    let mut resource_cols: Vec<(String, usize)> = Vec::new(); // (attr_key, col_idx)
    let mut timestamp_col: Option<usize> = None;
    let mut severity_col: Option<usize> = None;
    let mut body_col: Option<usize> = None;
    let mut trace_id_col: Option<usize> = None;
    let mut span_id_col: Option<usize> = None;
    let mut flags_col: Option<usize> = None;
    let mut has_scope_cols = false;
    let mut attr_cols: Vec<(String, usize)> = Vec::new(); // (key, col_idx)

    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name().as_str();

        let resource_key = name.strip_prefix(RESOURCE_PREFIX).map(str::to_string);
        if let Some(resource_key) = resource_key {
            resource_cols.push((resource_key, idx));
            continue;
        }

        if timestamp_col.is_none() && is_well_known_timestamp(name) {
            timestamp_col = Some(idx);
            continue;
        }
        if severity_col.is_none() && is_well_known_severity(name) {
            severity_col = Some(idx);
            continue;
        }
        if is_well_known_body(name) {
            match body_col {
                None => {
                    body_col = Some(idx);
                    continue;
                }
                Some(prev_idx) if name == field_names::BODY => {
                    let prev_name = batch.schema().field(prev_idx).name().clone();
                    if prev_name != field_names::BODY {
                        attr_cols.push((prev_name, prev_idx));
                        body_col = Some(idx);
                        continue;
                    }
                }
                Some(_) => {}
            }
        }
        if trace_id_col.is_none() && is_well_known_trace_id(name) {
            trace_id_col = Some(idx);
            continue;
        }
        if span_id_col.is_none() && is_well_known_span_id(name) {
            span_id_col = Some(idx);
            continue;
        }
        if flags_col.is_none() && is_well_known_flags(name) {
            flags_col = Some(idx);
            continue;
        }
        if name.starts_with("scope.") || name.starts_with("_scope_") {
            has_scope_cols = true;
        }

        attr_cols.push((name.to_string(), idx));
    }

    // --- Resource deduplication ---
    // Build a map of unique resource attribute sets → resource_id.
    // Key preserves NULL vs empty-string distinctions for each attribute.
    let mut resource_id_map: HashMap<Vec<Option<String>>, u32> = HashMap::new();
    let mut row_resource_ids: Vec<u32> = Vec::with_capacity(num_rows);
    let mut resource_template_rows: Vec<usize> = Vec::new();

    for row in 0..num_rows {
        let mut key_parts: Vec<Option<String>> = Vec::with_capacity(resource_cols.len());

        for (attr_key, col_idx) in &resource_cols {
            let arr = batch.column(*col_idx);
            let val = if arr.is_null(row) {
                None
            } else {
                Some(str_value_at(arr.as_ref(), row))
            };
            let _ = attr_key; // key order is fixed by `resource_cols`.
            key_parts.push(val);
        }

        let next_id = resource_id_map.len() as u32;
        let rid = *resource_id_map.entry(key_parts).or_insert_with(|| {
            resource_template_rows.push(row);
            next_id
        });
        row_resource_ids.push(rid);
    }

    // --- Build RESOURCE_ATTRS table ---
    let resource_attrs_batch =
        build_resource_attrs_table(batch, &resource_cols, &resource_template_rows)?;

    // --- Build SCOPE_ATTRS table ---
    // When real scope columns exist in the flat input, they flow through
    // LOG_ATTRS and override scope defaults during star_to_flat round-trip.
    // Only emit the hardcoded "logfwd" default when no scope columns are present.
    let scope_attrs_batch = if has_scope_cols {
        RecordBatch::new_empty(Arc::new(attrs_schema()))
    } else {
        build_scope_attrs()?
    };

    // --- Build LOG_ATTRS table ---
    let log_attrs_batch = build_log_attrs(batch, &attr_cols, num_rows)?;

    // --- Build LOGS fact table ---
    let logs_batch = build_logs_fact(
        batch,
        num_rows,
        &row_resource_ids,
        timestamp_col,
        severity_col,
        body_col,
        trace_id_col,
        span_id_col,
        flags_col,
    )?;

    Ok(StarSchema {
        logs: logs_batch,
        log_attrs: log_attrs_batch,
        resource_attrs: resource_attrs_batch,
        scope_attrs: scope_attrs_batch,
    })
}

// ---------------------------------------------------------------------------
// star_to_flat
// ---------------------------------------------------------------------------

/// Typed column data for `star_to_flat` reconstruction.
///
/// Each variant holds a `Vec` with one slot per output row. The variant is
/// chosen based on the `type` column in the attrs dimension table, so the
/// original Arrow type survives the round-trip.
enum TypedColumn {
    /// UTF-8 string column (ATTR_TYPE_STR or well-known text fields).
    Str(Vec<Option<String>>),
    /// 64-bit signed integer column (ATTR_TYPE_INT).
    Int(Vec<Option<i64>>),
    /// 64-bit float column (ATTR_TYPE_DOUBLE).
    Double(Vec<Option<f64>>),
    /// Boolean column (ATTR_TYPE_BOOL).
    Bool(Vec<Option<bool>>),
    /// Binary column (ATTR_TYPE_BYTES).
    Bytes(Vec<Option<Arc<[u8]>>>),
}

impl TypedColumn {
    fn new_str(n: usize) -> Self {
        Self::Str(vec![None; n])
    }
    fn new_int(n: usize) -> Self {
        Self::Int(vec![None; n])
    }
    fn new_double(n: usize) -> Self {
        Self::Double(vec![None; n])
    }
    fn new_bool(n: usize) -> Self {
        Self::Bool(vec![None; n])
    }
    fn new_bytes(n: usize) -> Self {
        Self::Bytes(vec![None; n])
    }

    fn matches_attr_type(&self, type_tag: u8) -> bool {
        matches!(
            (self, type_tag),
            (Self::Str(_), ATTR_TYPE_STR)
                | (Self::Int(_), ATTR_TYPE_INT)
                | (Self::Double(_), ATTR_TYPE_DOUBLE)
                | (Self::Bool(_), ATTR_TYPE_BOOL)
                | (Self::Bytes(_), ATTR_TYPE_BYTES)
        )
    }

    fn promote_to_str(&mut self) {
        if matches!(self, Self::Str(_)) {
            return;
        }

        let strings = match self {
            Self::Str(_) => unreachable!("handled above"),
            Self::Int(values) => values.iter().map(|v| v.map(|v| v.to_string())).collect(),
            Self::Double(values) => values.iter().map(|v| v.map(|v| v.to_string())).collect(),
            Self::Bool(values) => values.iter().map(|v| v.map(|v| v.to_string())).collect(),
            Self::Bytes(values) => values
                .iter()
                .map(|v| v.as_ref().map(|v| hex_encode_lower(v)))
                .collect(),
        };
        *self = Self::Str(strings);
    }

    /// Build the Arrow array and data type for this column.
    fn to_arrow(&self) -> (DataType, ArrayRef) {
        match self {
            Self::Str(v) => {
                let arr: ArrayRef = Arc::new(StringArray::from(
                    v.iter()
                        .map(|v| v.as_deref())
                        .collect::<Vec<Option<&str>>>(),
                ));
                (DataType::Utf8, arr)
            }
            Self::Int(v) => {
                let arr: ArrayRef = Arc::new(Int64Array::from(v.clone()));
                (DataType::Int64, arr)
            }
            Self::Double(v) => {
                let arr: ArrayRef = Arc::new(Float64Array::from(v.clone()));
                (DataType::Float64, arr)
            }
            Self::Bool(v) => {
                let arr: ArrayRef = Arc::new(BooleanArray::from(v.clone()));
                (DataType::Boolean, arr)
            }
            Self::Bytes(v) => {
                let refs: Vec<Option<&[u8]>> = v.iter().map(|o| o.as_deref()).collect();
                let arr: ArrayRef = Arc::new(BinaryArray::from(refs));
                (DataType::Binary, arr)
            }
        }
    }
}

/// Convert OTAP star schema back to logfwd's flat `RecordBatch`.
///
/// 1. Reads LOG_ATTRS: unpivots rows grouped by parent_id into columns,
///    preserving the original Arrow type (string, int, double, bool).
/// 2. Reads RESOURCE_ATTRS: groups by parent_id, prefixes keys with
///    `resource.attributes.`, scatters to rows via resource_id from the LOGS table.
/// 3. Maps well-known LOGS fields back: time_unix_nano → `_timestamp`,
///    severity_text → `level`, body_str → `message`.
/// 4. Combines into a single flat `RecordBatch`.
pub fn star_to_flat(star: &StarSchema) -> Result<RecordBatch, ArrowError> {
    let num_rows = star.logs.num_rows();

    if num_rows == 0 {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    // --- Extract LOGS fact columns ---
    let logs_schema = star.logs.schema();
    let resource_ids = star
        .logs
        .column(
            logs_schema
                .index_of("resource_id")
                .map_err(|e| ArrowError::SchemaError(format!("missing resource_id: {e}")))?,
        )
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ArrowError::SchemaError("resource_id not UInt32".to_string()))?;
    let scope_ids = star
        .logs
        .column(
            logs_schema
                .index_of("scope_id")
                .map_err(|e| ArrowError::SchemaError(format!("missing scope_id: {e}")))?,
        )
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ArrowError::SchemaError("scope_id not UInt32".to_string()))?;

    // Collect flat columns: name → TypedColumn.
    let mut flat_cols: Vec<(String, TypedColumn)> = Vec::new();
    let mut col_index: HashMap<String, usize> = HashMap::new();
    let unprotected_cols: HashSet<String> = HashSet::new();
    let mut protected_log_fact_cols: HashSet<String> = HashSet::new();

    // Helper to get or create a string column.
    let ensure_str_col = |name: &str,
                          flat_cols: &mut Vec<(String, TypedColumn)>,
                          col_index: &mut HashMap<String, usize>|
     -> usize {
        if let Some(&idx) = col_index.get(name) {
            idx
        } else {
            let idx = flat_cols.len();
            flat_cols.push((name.to_string(), TypedColumn::new_str(num_rows)));
            col_index.insert(name.to_string(), idx);
            idx
        }
    };
    let ensure_int_col = |name: &str,
                          flat_cols: &mut Vec<(String, TypedColumn)>,
                          col_index: &mut HashMap<String, usize>|
     -> usize {
        if let Some(&idx) = col_index.get(name) {
            idx
        } else {
            let idx = flat_cols.len();
            flat_cols.push((name.to_string(), TypedColumn::new_int(num_rows)));
            col_index.insert(name.to_string(), idx);
            idx
        }
    };

    // --- Map well-known LOGS fields (always string) ---
    // _timestamp from time_unix_nano
    if let Ok(ts_idx) = logs_schema.index_of("time_unix_nano") {
        let ts_arr = star.logs.column(ts_idx);
        if !matches!(
            ts_arr.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            return Err(ArrowError::SchemaError(format!(
                "time_unix_nano must be Timestamp(Nanosecond), got {}",
                ts_arr.data_type()
            )));
        }
        let col_pos = ensure_str_col("_timestamp", &mut flat_cols, &mut col_index);
        protected_log_fact_cols.insert("_timestamp".to_string());
        for row in 0..num_rows {
            if !ts_arr.is_null(row) {
                // Timestamp is stored as i64 nanoseconds.
                if let Some(prim) = ts_arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    let ns = prim.value(row);
                    // Format as RFC3339 nanoseconds. Use Euclidean div/rem so
                    // that negative timestamps (pre-1970) yield nanos in
                    // [0, 999_999_999] rather than a negative value that wraps
                    // on the `as u32` cast, which would corrupt the output.
                    let secs = ns.div_euclid(1_000_000_000);
                    let nanos = ns.rem_euclid(1_000_000_000) as u32;
                    if let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1 {
                        v[row] = Some(chrono_timestamp(secs, nanos));
                    }
                }
            }
        }
    }

    // severity_text → level
    if let Ok(sev_idx) = logs_schema.index_of("severity_text") {
        let sev_arr = star.logs.column(sev_idx);
        if !matches!(sev_arr.data_type(), DataType::Utf8 | DataType::Utf8View) {
            return Err(ArrowError::SchemaError(format!(
                "severity_text must be Utf8 or Utf8View, got {}",
                sev_arr.data_type()
            )));
        }
        let col_pos = ensure_str_col("level", &mut flat_cols, &mut col_index);
        protected_log_fact_cols.insert("level".to_string());
        for row in 0..num_rows {
            if !sev_arr.is_null(row) {
                let val = str_from_array(sev_arr.as_ref(), row);
                if !val.is_empty()
                    && let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1
                {
                    v[row] = Some(val);
                }
            }
        }
    }

    // body_str → message
    if let Ok(body_idx) = logs_schema.index_of("body_str") {
        let body_arr = star.logs.column(body_idx);
        if !matches!(body_arr.data_type(), DataType::Utf8 | DataType::Utf8View) {
            return Err(ArrowError::SchemaError(format!(
                "body_str must be Utf8 or Utf8View, got {}",
                body_arr.data_type()
            )));
        }
        let col_pos = ensure_str_col("message", &mut flat_cols, &mut col_index);
        protected_log_fact_cols.insert("message".to_string());
        for row in 0..num_rows {
            if !body_arr.is_null(row) {
                let val = str_from_array(body_arr.as_ref(), row);
                if !val.is_empty()
                    && let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1
                {
                    v[row] = Some(val);
                }
            }
        }
    }

    // severity_number
    if let Ok(sev_num_idx) = logs_schema.index_of("severity_number") {
        let sev_num_arr = star.logs.column(sev_num_idx);
        if !matches!(sev_num_arr.data_type(), DataType::Int32) {
            return Err(ArrowError::SchemaError(format!(
                "severity_number must be Int32, got {}",
                sev_num_arr.data_type()
            )));
        }
        let col_pos = ensure_int_col("severity_number", &mut flat_cols, &mut col_index);
        protected_log_fact_cols.insert("severity_number".to_string());
        let sev_num_arr = sev_num_arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ArrowError::SchemaError("severity_number not Int32".to_string()))?;
        for row in 0..num_rows {
            if !sev_num_arr.is_null(row)
                && let TypedColumn::Int(ref mut v) = flat_cols[col_pos].1
            {
                v[row] = Some(i64::from(sev_num_arr.value(row)));
            }
        }
    }

    // trace_id (16-byte fixed binary) -> lowercase hex string
    if let Ok(trace_idx) = logs_schema.index_of("trace_id") {
        let trace_arr = star.logs.column(trace_idx);
        if !matches!(trace_arr.data_type(), DataType::FixedSizeBinary(16)) {
            return Err(ArrowError::SchemaError(format!(
                "trace_id must be FixedSizeBinary(16), got {}",
                trace_arr.data_type()
            )));
        }
        let col_pos = ensure_str_col("trace_id", &mut flat_cols, &mut col_index);
        protected_log_fact_cols.insert("trace_id".to_string());
        let trace_arr = trace_arr
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .ok_or_else(|| {
                ArrowError::SchemaError("trace_id not FixedSizeBinary(16)".to_string())
            })?;
        for row in 0..num_rows {
            if !trace_arr.is_null(row)
                && let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1
            {
                v[row] = Some(hex_encode_lower(trace_arr.value(row)));
            }
        }
    }

    // span_id (8-byte fixed binary) -> lowercase hex string
    if let Ok(span_idx) = logs_schema.index_of("span_id") {
        let span_arr = star.logs.column(span_idx);
        if !matches!(span_arr.data_type(), DataType::FixedSizeBinary(8)) {
            return Err(ArrowError::SchemaError(format!(
                "span_id must be FixedSizeBinary(8), got {}",
                span_arr.data_type()
            )));
        }
        let col_pos = ensure_str_col("span_id", &mut flat_cols, &mut col_index);
        protected_log_fact_cols.insert("span_id".to_string());
        let span_arr = span_arr
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .ok_or_else(|| ArrowError::SchemaError("span_id not FixedSizeBinary(8)".to_string()))?;
        for row in 0..num_rows {
            if !span_arr.is_null(row)
                && let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1
            {
                v[row] = Some(hex_encode_lower(span_arr.value(row)));
            }
        }
    }

    // flags
    if let Ok(flags_idx) = logs_schema.index_of("flags") {
        let flags_arr = star.logs.column(flags_idx);
        if !matches!(flags_arr.data_type(), DataType::UInt32) {
            return Err(ArrowError::SchemaError(format!(
                "flags must be UInt32, got {}",
                flags_arr.data_type()
            )));
        }
        let col_pos = ensure_int_col("flags", &mut flat_cols, &mut col_index);
        protected_log_fact_cols.insert("flags".to_string());
        let flags_arr = flags_arr
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ArrowError::SchemaError("flags not UInt32".to_string()))?;
        for row in 0..num_rows {
            if !flags_arr.is_null(row)
                && let TypedColumn::Int(ref mut v) = flat_cols[col_pos].1
            {
                v[row] = Some(i64::from(flags_arr.value(row)));
            }
        }
    }

    // --- Unpivot SCOPE_ATTRS ---
    // Populate default scope fields first (e.g., synthetic "logfwd" scope).
    // LOG_ATTRS are unpivoted afterward and may carry row-level scope.*
    // values from the original flat input; those should take precedence.
    unpivot_attrs_to_flat(
        &star.scope_attrs,
        &mut flat_cols,
        &mut col_index,
        num_rows,
        |parent_id| parent_id as usize,
        |key| match key {
            "scope_name" => "scope.name".to_string(),
            "scope_version" => "scope.version".to_string(),
            _ if key.starts_with("scope.") || key.starts_with("_scope_") => key.to_string(),
            _ => format!("scope.{key}"),
        },
        &unprotected_cols,
    )?;

    // --- Unpivot LOG_ATTRS → flat columns ---
    unpivot_attrs_to_flat(
        &star.log_attrs,
        &mut flat_cols,
        &mut col_index,
        num_rows,
        |parent_id| parent_id as usize, // parent_id IS the row index
        str::to_string,                 // no prefix
        &protected_log_fact_cols,
    )?;

    // --- Unpivot RESOURCE_ATTRS → resource.attributes.* columns ---
    // Build resource_id → parent_id mapping: for each row, scatter resource
    // attrs via the LOGS table's resource_id column.
    unpivot_attrs_to_flat(
        &star.resource_attrs,
        &mut flat_cols,
        &mut col_index,
        num_rows,
        |parent_id| {
            // parent_id in RESOURCE_ATTRS is the resource_id.
            // We need to scatter to all rows that have this resource_id.
            // This closure is called per-attr-row, but we handle scattering below.
            parent_id as usize
        },
        |key| format!("{RESOURCE_PREFIX}{key}"),
        &unprotected_cols,
    )?;

    // The unpivot above set values at the resource_id index, but we need to
    // scatter them to all rows that share that resource_id.
    scatter_resource_attrs(&mut flat_cols, &col_index, resource_ids, num_rows);
    // Scope attrs are keyed by scope_id and must be scattered to all rows
    // sharing that scope.
    scatter_scope_attrs(&mut flat_cols, &col_index, scope_ids, num_rows);

    // --- Build the flat RecordBatch ---
    let mut fields: Vec<Field> = Vec::with_capacity(flat_cols.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(flat_cols.len());

    for (name, col) in &flat_cols {
        let (dt, arr) = col.to_arrow();
        fields.push(Field::new(name.as_str(), dt, true));
        arrays.push(arr);
    }

    if fields.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build an empty `StarSchema` with zero rows.
fn empty_star_schema() -> StarSchema {
    StarSchema {
        logs: RecordBatch::new_empty(Arc::new(logs_schema())),
        log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
    }
}

/// Extract a string value from any supported Arrow array at the given row.
fn str_value_at(arr: &dyn Array, row: usize) -> String {
    if arr.is_null(row) {
        return String::new();
    }
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8View => arr
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Binary => arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| {
                let mut hex = String::with_capacity(a.value(row).len() * 2);
                for b in a.value(row) {
                    use std::fmt::Write as _;
                    let _ = write!(&mut hex, "{b:02x}");
                }
                hex
            })
            .unwrap_or_default(),
        DataType::LargeBinary => arr
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|a| {
                let mut hex = String::with_capacity(a.value(row).len() * 2);
                for b in a.value(row) {
                    use std::fmt::Write as _;
                    let _ = write!(&mut hex, "{b:02x}");
                }
                hex
            })
            .unwrap_or_default(),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int8 => arr
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int16 => arr
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt8 => arr
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt16 => arr
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => arr
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => arr
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float32 => arr
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        // Timestamps: normalize the raw i64 to nanoseconds before stringifying so that
        // `parse_timestamp_to_nanos` (which infers units from magnitude) always sees a
        // nanos-scale value. The old code stringified the raw i64 directly, which caused
        // small-epoch timestamps in coarser units (seconds, millis, micros) to be
        // misclassified by the magnitude heuristic.
        DataType::Timestamp(TimeUnit::Second, _) => arr
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|a| a.value(row).saturating_mul(1_000_000_000).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|a| a.value(row).saturating_mul(1_000_000).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| a.value(row).saturating_mul(1_000).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        // Fallback for any other Arrow types (e.g. Date32, Dictionary, List, etc.)
        // delegates to Arrow's display formatting via `array_value_to_string`.
        //
        // Note: `array_value_to_string` allocates an `ArrayFormatter` per call. This is
        // acceptable here because all common hot-path types (Utf8, Int64, Float64,
        // Boolean, Binary, all integer/float variants, and Timestamps) are handled by
        // explicit arms above. Only truly exotic types reach this fallback.
        _ => array_value_to_string(arr, row).unwrap_or_default(),
    }
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Read a string value from a Utf8 or Utf8View array.
fn str_from_array(arr: &dyn Array, row: usize) -> String {
    if arr.is_null(row) {
        return String::new();
    }
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8View => arr
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

/// Determine the attribute type tag for an Arrow `DataType`.
fn attr_type_for(dt: &DataType) -> u8 {
    match dt {
        DataType::Int64 => ATTR_TYPE_INT,
        DataType::Float64 => ATTR_TYPE_DOUBLE,
        DataType::Boolean => ATTR_TYPE_BOOL,
        DataType::Binary | DataType::LargeBinary => ATTR_TYPE_BYTES,
        _ => ATTR_TYPE_STR,
    }
}

/// Build RESOURCE_ATTRS from deduplicated resource template rows.
fn build_resource_attrs_table(
    batch: &RecordBatch,
    resource_cols: &[(String, usize)],
    template_rows: &[usize],
) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(attrs_schema());

    if template_rows.is_empty() || resource_cols.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Upper bound: every resource row has every resource attr.
    let total: usize = template_rows.len() * resource_cols.len();
    if total == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut parent_ids = Vec::with_capacity(total);
    let mut keys = Vec::with_capacity(total);
    let mut types = Vec::with_capacity(total);
    let mut str_vals: Vec<Option<String>> = Vec::with_capacity(total);
    let mut int_vals: Vec<Option<i64>> = Vec::with_capacity(total);
    let mut double_vals: Vec<Option<f64>> = Vec::with_capacity(total);
    let mut bool_vals: Vec<Option<bool>> = Vec::with_capacity(total);
    let mut bytes_vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);

    for (pid, &row) in template_rows.iter().enumerate() {
        for (key, col_idx) in resource_cols {
            let arr = batch.column(*col_idx).as_ref();
            if arr.is_null(row) {
                continue;
            }

            parent_ids.push(pid as u32);
            keys.push(key.clone());

            match attr_type_for(arr.data_type()) {
                ATTR_TYPE_INT => {
                    let value = arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| a.value(row))
                        .ok_or_else(|| {
                            ArrowError::SchemaError("resource attr int not Int64".to_string())
                        })?;
                    types.push(ATTR_TYPE_INT);
                    int_vals.push(Some(value));
                    str_vals.push(None);
                    double_vals.push(None);
                    bool_vals.push(None);
                    bytes_vals.push(None);
                }
                ATTR_TYPE_DOUBLE => {
                    let value = arr
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .map(|a| a.value(row))
                        .ok_or_else(|| {
                            ArrowError::SchemaError("resource attr double not Float64".to_string())
                        })?;
                    types.push(ATTR_TYPE_DOUBLE);
                    double_vals.push(Some(value));
                    str_vals.push(None);
                    int_vals.push(None);
                    bool_vals.push(None);
                    bytes_vals.push(None);
                }
                ATTR_TYPE_BOOL => {
                    let value = arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .map(|a| a.value(row))
                        .ok_or_else(|| {
                            ArrowError::SchemaError("resource attr bool not Boolean".to_string())
                        })?;
                    types.push(ATTR_TYPE_BOOL);
                    bool_vals.push(Some(value));
                    str_vals.push(None);
                    int_vals.push(None);
                    double_vals.push(None);
                    bytes_vals.push(None);
                }
                ATTR_TYPE_BYTES => {
                    let bytes = match arr.data_type() {
                        DataType::Binary => arr
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .map(|a| a.value(row).to_vec()),
                        DataType::LargeBinary => arr
                            .as_any()
                            .downcast_ref::<LargeBinaryArray>()
                            .map(|a| a.value(row).to_vec()),
                        _ => None,
                    }
                    .ok_or_else(|| {
                        ArrowError::SchemaError(
                            "resource attr bytes not Binary/LargeBinary".to_string(),
                        )
                    })?;
                    types.push(ATTR_TYPE_BYTES);
                    bytes_vals.push(Some(bytes));
                    str_vals.push(None);
                    int_vals.push(None);
                    double_vals.push(None);
                    bool_vals.push(None);
                }
                _ => {
                    types.push(ATTR_TYPE_STR);
                    str_vals.push(Some(str_value_at(arr, row)));
                    int_vals.push(None);
                    double_vals.push(None);
                    bool_vals.push(None);
                    bytes_vals.push(None);
                }
            }
        }
    }

    let bytes_refs: Vec<Option<&[u8]>> = bytes_vals.iter().map(|v| v.as_deref()).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(parent_ids)),
        Arc::new(StringArray::from(
            keys.into_iter().map(Some).collect::<Vec<_>>(),
        )),
        Arc::new(UInt8Array::from(types)),
        Arc::new(StringArray::from(str_vals)),
        Arc::new(Int64Array::from(int_vals)),
        Arc::new(Float64Array::from(double_vals)),
        Arc::new(BooleanArray::from(bool_vals)),
        Arc::new(BinaryArray::from(bytes_refs)),
    ];

    RecordBatch::try_new(schema, columns)
}

/// Build the single-row SCOPE_ATTRS table with scope_name="logfwd".
fn build_scope_attrs() -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(attrs_schema());

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(vec![0u32])),
        Arc::new(StringArray::from(vec!["scope_name"])),
        Arc::new(UInt8Array::from(vec![ATTR_TYPE_STR])),
        Arc::new(StringArray::from(vec![Some("logfwd")])),
        Arc::new(Int64Array::from(vec![None as Option<i64>])),
        Arc::new(Float64Array::from(vec![None as Option<f64>])),
        Arc::new(BooleanArray::from(vec![None as Option<bool>])),
        Arc::new(BinaryArray::from(vec![None as Option<&[u8]>])),
    ];

    RecordBatch::try_new(schema, columns)
}

/// Build LOG_ATTRS by pivoting attribute columns to rows.
fn build_log_attrs(
    batch: &RecordBatch,
    attr_cols: &[(String, usize)],
    num_rows: usize,
) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(attrs_schema());

    if attr_cols.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Upper bound on attr rows: num_rows * num_attr_cols.
    // Actual count may be less if there are nulls.
    let capacity = num_rows * attr_cols.len();
    let mut parent_ids = Vec::with_capacity(capacity);
    let mut keys = Vec::with_capacity(capacity);
    let mut types = Vec::with_capacity(capacity);
    let mut str_vals: Vec<Option<String>> = Vec::with_capacity(capacity);
    let mut int_vals: Vec<Option<i64>> = Vec::with_capacity(capacity);
    let mut double_vals: Vec<Option<f64>> = Vec::with_capacity(capacity);
    let mut bool_vals: Vec<Option<bool>> = Vec::with_capacity(capacity);
    let mut bytes_vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(capacity);

    for row in 0..num_rows {
        for (key, col_idx) in attr_cols {
            let arr = batch.column(*col_idx);
            if arr.is_null(row) {
                continue; // Skip null values — no attr row emitted.
            }

            let schema = batch.schema();
            let field = schema.field(*col_idx);
            let type_tag = attr_type_for(field.data_type());

            parent_ids.push(row as u32);
            keys.push(key.clone());
            types.push(type_tag);

            match type_tag {
                ATTR_TYPE_INT => {
                    let a = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Expected Int64Array for column {}, got {}",
                            key,
                            arr.data_type()
                        ))
                    })?;
                    str_vals.push(None);
                    int_vals.push(Some(a.value(row)));
                    double_vals.push(None);
                    bool_vals.push(None);
                    bytes_vals.push(None);
                }
                ATTR_TYPE_DOUBLE => {
                    let a = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Expected Float64Array for column {}, got {}",
                            key,
                            arr.data_type()
                        ))
                    })?;
                    str_vals.push(None);
                    int_vals.push(None);
                    double_vals.push(Some(a.value(row)));
                    bool_vals.push(None);
                    bytes_vals.push(None);
                }
                ATTR_TYPE_BOOL => {
                    let a = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Expected BooleanArray for column {}, got {}",
                            key,
                            arr.data_type()
                        ))
                    })?;
                    str_vals.push(None);
                    int_vals.push(None);
                    double_vals.push(None);
                    bool_vals.push(Some(a.value(row)));
                    bytes_vals.push(None);
                }
                ATTR_TYPE_BYTES => {
                    // attr_type_for() maps both DataType::Binary and DataType::LargeBinary to
                    // ATTR_TYPE_BYTES, so we must handle both array types here.
                    let bytes = match arr.data_type() {
                        DataType::Binary => arr
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .map(|a| a.value(row).to_vec()),
                        DataType::LargeBinary => arr
                            .as_any()
                            .downcast_ref::<LargeBinaryArray>()
                            .map(|a| a.value(row).to_vec()),
                        _ => None,
                    }
                    .ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Expected BinaryArray or LargeBinaryArray for column {}, got {}",
                            key,
                            arr.data_type()
                        ))
                    })?;
                    str_vals.push(None);
                    int_vals.push(None);
                    double_vals.push(None);
                    bool_vals.push(None);
                    bytes_vals.push(Some(bytes));
                }
                _ => {
                    // String (default).
                    let val = str_value_at(arr.as_ref(), row);
                    str_vals.push(Some(val));
                    int_vals.push(None);
                    double_vals.push(None);
                    bool_vals.push(None);
                    bytes_vals.push(None);
                }
            }
        }
    }

    if parent_ids.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(parent_ids)),
        Arc::new(StringArray::from(keys)),
        Arc::new(UInt8Array::from(types)),
        Arc::new(StringArray::from(
            str_vals
                .iter()
                .map(|v| v.as_deref())
                .collect::<Vec<Option<&str>>>(),
        )),
        Arc::new(Int64Array::from(int_vals)),
        Arc::new(Float64Array::from(double_vals)),
        Arc::new(BooleanArray::from(bool_vals)),
        Arc::new(BinaryArray::from(
            bytes_vals
                .iter()
                .map(|v| v.as_deref())
                .collect::<Vec<Option<&[u8]>>>(),
        )),
    ];

    RecordBatch::try_new(schema, columns)
}

/// Build the LOGS fact table.
#[allow(clippy::too_many_arguments)]
fn build_logs_fact(
    batch: &RecordBatch,
    num_rows: usize,
    resource_ids: &[u32],
    timestamp_col: Option<usize>,
    severity_col: Option<usize>,
    body_col: Option<usize>,
    trace_id_col: Option<usize>,
    span_id_col: Option<usize>,
    flags_col: Option<usize>,
) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(logs_schema());

    // id: sequential row index
    let ids: Vec<u32> = (0..num_rows as u32).collect();

    // resource_id: from deduplication
    let rid_arr = UInt32Array::from(resource_ids.to_vec());

    // scope_id: all 0 (single scope)
    let scope_ids: Vec<u32> = vec![0; num_rows];

    // time_unix_nano: parse timestamp strings to i64 nanoseconds
    let timestamps: Vec<Option<i64>> = if let Some(ts_idx) = timestamp_col {
        let arr = batch.column(ts_idx);
        (0..num_rows)
            .map(|row| {
                if arr.is_null(row) {
                    None
                } else {
                    let s = str_value_at(arr.as_ref(), row);
                    parse_timestamp_to_nanos(&s)
                }
            })
            .collect()
    } else {
        vec![None; num_rows]
    };

    // severity_number + severity_text
    let (severity_numbers, severity_texts): (Vec<Option<i32>>, Vec<Option<String>>) =
        if let Some(sev_idx) = severity_col {
            let arr = batch.column(sev_idx);
            (0..num_rows)
                .map(|row| {
                    if arr.is_null(row) {
                        (None, None)
                    } else {
                        let s = str_value_at(arr.as_ref(), row);
                        let num = severity_text_to_number(&s);
                        (Some(num), Some(s))
                    }
                })
                .unzip()
        } else {
            (vec![None; num_rows], vec![None; num_rows])
        };

    // body_str
    let body_strs: Vec<Option<String>> = if let Some(body_idx) = body_col {
        let arr = batch.column(body_idx);
        (0..num_rows)
            .map(|row| {
                if arr.is_null(row) {
                    None
                } else {
                    let s = str_value_at(arr.as_ref(), row);
                    Some(s)
                }
            })
            .collect()
    } else {
        vec![None; num_rows]
    };

    // trace_id: 16-byte fixed-size binary (from hex string)
    let trace_ids: Vec<Option<[u8; 16]>> = if let Some(tid_idx) = trace_id_col {
        let arr = batch.column(tid_idx);
        (0..num_rows)
            .map(|row| {
                if arr.is_null(row) {
                    None
                } else {
                    let s = str_value_at(arr.as_ref(), row);
                    hex_to_fixed::<16>(&s)
                }
            })
            .collect()
    } else {
        vec![None; num_rows]
    };

    // span_id: 8-byte fixed-size binary (from hex string)
    let span_ids: Vec<Option<[u8; 8]>> = if let Some(sid_idx) = span_id_col {
        let arr = batch.column(sid_idx);
        (0..num_rows)
            .map(|row| {
                if arr.is_null(row) {
                    None
                } else {
                    let s = str_value_at(arr.as_ref(), row);
                    hex_to_fixed::<8>(&s)
                }
            })
            .collect()
    } else {
        vec![None; num_rows]
    };

    // flags
    let flags: Vec<Option<u32>> = if let Some(f_idx) = flags_col {
        let arr = batch.column(f_idx);
        (0..num_rows)
            .map(|row| {
                if arr.is_null(row) {
                    None
                } else {
                    let s = str_value_at(arr.as_ref(), row);
                    s.parse::<u32>().ok()
                }
            })
            .collect()
    } else {
        vec![None; num_rows]
    };

    // dropped_attributes_count: always 0
    let dropped: Vec<Option<u32>> = vec![Some(0); num_rows];

    // Build arrays.
    let trace_id_arr = build_fixed_binary_array::<16>(&trace_ids)?;
    let span_id_arr = build_fixed_binary_array::<8>(&span_ids)?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(ids)),
        Arc::new(rid_arr),
        Arc::new(UInt32Array::from(scope_ids)),
        Arc::new(TimestampNanosecondArray::from(timestamps)),
        Arc::new(Int32Array::from(severity_numbers)),
        Arc::new(StringArray::from(
            severity_texts
                .iter()
                .map(|v| v.as_deref())
                .collect::<Vec<Option<&str>>>(),
        )),
        Arc::new(StringArray::from(
            body_strs
                .iter()
                .map(|v| v.as_deref())
                .collect::<Vec<Option<&str>>>(),
        )),
        trace_id_arr,
        span_id_arr,
        Arc::new(UInt32Array::from(flags)),
        Arc::new(UInt32Array::from(dropped)),
    ];

    RecordBatch::try_new(schema, columns)
}

/// Build a `FixedSizeBinary` array from optional fixed-size byte arrays.
fn build_fixed_binary_array<const N: usize>(
    values: &[Option<[u8; N]>],
) -> Result<ArrayRef, ArrowError> {
    let mut builder = arrow::array::FixedSizeBinaryBuilder::new(N as i32);
    for val in values {
        match val {
            Some(bytes) => builder.append_value(bytes)?,
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Parse a hex string to a fixed-size byte array.
fn hex_to_fixed<const N: usize>(hex: &str) -> Option<[u8; N]> {
    let hex = hex.trim();
    if hex.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for i in 0..N {
        let byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
        out[i] = byte;
    }
    Some(out)
}

/// Parse a timestamp string to nanoseconds since epoch.
/// Supports ISO 8601 / RFC 3339 format and bare epoch nanoseconds.
pub(crate) fn parse_timestamp_to_nanos(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Try bare integer nanoseconds first.
    if let Ok(ns) = s.parse::<i64>() {
        // Heuristic for epoch timestamps based on magnitude. We use unsigned_abs() so that
        // pre-1970 (negative) timestamps are classified correctly — for negative values the
        // comparisons against positive thresholds would otherwise always be false.
        // > 1e17: nanoseconds  (1e17 ns = ~3.17 years from 1970)
        // > 1e14: microseconds (1e14 us = ~3.17 years)
        // > 1e11: milliseconds (1e11 ms = ~3.17 years)
        // else: seconds
        let abs = ns.unsigned_abs();
        if abs > 100_000_000_000_000_000 {
            return Some(ns);
        }
        if abs > 100_000_000_000_000 {
            return ns.checked_mul(1_000);
        }
        if abs > 100_000_000_000 {
            return ns.checked_mul(1_000_000);
        }
        return ns.checked_mul(1_000_000_000);
    }
    // Try RFC 3339 parsing.
    // Format: 2024-01-15T10:30:00.123456789Z
    parse_rfc3339_nanos(s)
}

/// Minimal RFC 3339 parser that preserves nanosecond precision.
pub(crate) fn parse_rfc3339_nanos(s: &str) -> Option<i64> {
    // Expected format: YYYY-MM-DDThh:mm:ss[.nnnnnnnnn]Z or +HH:MM
    if s.len() < 19 {
        return None;
    }

    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hour: u32 = s.get(11..13)?.parse().ok()?;
    let min: u32 = s.get(14..16)?.parse().ok()?;
    let sec: u32 = s.get(17..19)?.parse().ok()?;

    if hour >= 24 || min >= 60 || sec > 60 {
        // sec > 60 allows leap seconds (60)
        return None;
    }

    let rest = &s[19..];
    let (frac_nanos, tz_start) = if let Some(dot_rest) = rest.strip_prefix('.') {
        // Parse fractional seconds.
        let frac_end = dot_rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(dot_rest.len());
        let frac_str = &dot_rest[..frac_end];
        // Pad or truncate to 9 digits.
        let mut padded = String::with_capacity(9);
        padded.push_str(frac_str);
        while padded.len() < 9 {
            padded.push('0');
        }
        let nanos: i64 = padded[..9].parse().ok()?;
        (nanos, 1 + frac_end)
    } else {
        (0i64, 0)
    };

    let tz_str = &rest[tz_start..];
    // RFC 3339 requires an explicit timezone designator. Strings without one (e.g.
    // "2024-01-15T10:30:00") are not valid RFC 3339 and we return None rather than
    // silently assuming UTC, which could misinterpret local-time values.
    let tz_offset_secs: i64 = if tz_str == "Z" || tz_str == "z" {
        0
    } else if tz_str.is_empty() {
        return None;
    } else if tz_str.len() >= 6 && (tz_str.starts_with('+') || tz_str.starts_with('-')) {
        let sign: i64 = if tz_str.starts_with('-') { -1 } else { 1 };
        let tz_h: i64 = tz_str[1..3].parse().ok()?;
        let tz_m: i64 = tz_str[4..6].parse().ok()?;
        if tz_h >= 24 || tz_m >= 60 {
            return None;
        }
        sign * (tz_h * 3600 + tz_m * 60)
    } else {
        return None;
    };

    // Convert date to days since epoch using a simplified calculation.
    let days = days_from_civil(year, month, day)?;
    let total_secs = days
        .checked_mul(86400)?
        .checked_add(i64::from(hour) * 3600)?
        .checked_add(i64::from(min) * 60)?
        .checked_add(i64::from(sec))?
        .checked_sub(tz_offset_secs)?;

    total_secs
        .checked_mul(1_000_000_000)?
        .checked_add(frac_nanos)
}

/// Convert a civil date to days since Unix epoch (1970-01-01).
///
/// Validates month/day ranges (including leap year rules) and delegates
/// to the Kani-verified `logfwd_core::otlp::days_from_civil`.
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // More precise day validation
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let max_days = match month {
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap {
                29
            } else {
                28
            }
        }
        _ => 31,
    };
    if day > max_days {
        return None;
    }

    Some(logfwd_core::otlp::days_from_civil(year, month, day))
}

/// Format nanosecond epoch timestamp as RFC 3339 string.
pub fn chrono_timestamp(secs: i64, nanos: u32) -> String {
    // Reverse the days_from_civil calculation.
    let total_days = secs.div_euclid(86400);
    let day_secs = secs.rem_euclid(86400);
    let hour = day_secs / 3600;
    let min = (day_secs % 3600) / 60;
    let sec = day_secs % 60;

    let (year, month, day) = civil_from_days(total_days);

    if nanos == 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
    } else {
        // Trim trailing zeros from nanoseconds.
        let ns_str = format!("{nanos:09}");
        let trimmed = ns_str.trim_end_matches('0');
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{trimmed}Z")
    }
}

/// Reverse of `days_from_civil`: days since epoch → (year, month, day).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Map severity text to OTLP severity number.
fn severity_text_to_number(text: &str) -> i32 {
    match text.to_uppercase().as_str() {
        "TRACE" => 1,
        "TRACE2" => 2,
        "TRACE3" => 3,
        "TRACE4" => 4,
        "DEBUG" => 5,
        "DEBUG2" => 6,
        "DEBUG3" => 7,
        "DEBUG4" => 8,
        "INFO" => 9,
        "INFO2" => 10,
        "INFO3" => 11,
        "INFO4" => 12,
        "WARN" | "WARNING" => 13,
        "WARN2" => 14,
        "WARN3" => 15,
        "WARN4" => 16,
        "ERROR" => 17,
        "ERROR2" => 18,
        "ERROR3" => 19,
        "ERROR4" => 20,
        "FATAL" => 21,
        "FATAL2" => 22,
        "FATAL3" => 23,
        "FATAL4" => 24,
        _ => 0, // UNSPECIFIED
    }
}

/// Unpivot an attrs dimension table into flat typed columns.
///
/// For each row in the attrs table, reads (parent_id, key, typed value) and
/// writes the value into `flat_cols[key][row_for(parent_id)]`, preserving the
/// original type from the `type` column.
///
/// Existing columns listed in `protected_existing_cols` are canonical fact
/// columns; attrs with the same flat name are skipped because flat batches
/// cannot represent duplicate column names.
fn unpivot_attrs_to_flat(
    attrs_batch: &RecordBatch,
    flat_cols: &mut Vec<(String, TypedColumn)>,
    col_index: &mut HashMap<String, usize>,
    num_rows: usize,
    row_for: impl Fn(u32) -> usize,
    map_key: impl Fn(&str) -> String,
    protected_existing_cols: &HashSet<String>,
) -> Result<(), ArrowError> {
    if attrs_batch.num_rows() == 0 {
        return Ok(());
    }

    let a_schema = attrs_batch.schema();
    let parent_id_arr = attrs_batch
        .column(
            a_schema
                .index_of("parent_id")
                .map_err(|e| ArrowError::SchemaError(format!("missing parent_id: {e}")))?,
        )
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ArrowError::SchemaError("parent_id not UInt32".to_string()))?;

    let key_arr = attrs_batch
        .column(
            a_schema
                .index_of("key")
                .map_err(|e| ArrowError::SchemaError(format!("missing key: {e}")))?,
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::SchemaError("key not Utf8".to_string()))?;

    let type_arr = attrs_batch
        .column(
            a_schema
                .index_of("type")
                .map_err(|e| ArrowError::SchemaError(format!("missing type: {e}")))?,
        )
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| ArrowError::SchemaError("type not UInt8".to_string()))?;

    let str_arr = attrs_batch
        .column(
            a_schema
                .index_of("str")
                .map_err(|e| ArrowError::SchemaError(format!("missing str: {e}")))?,
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::SchemaError("str not Utf8".to_string()))?;

    let int_arr = attrs_batch
        .column(
            a_schema
                .index_of("int")
                .map_err(|e| ArrowError::SchemaError(format!("missing int: {e}")))?,
        )
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ArrowError::SchemaError("int not Int64".to_string()))?;

    let double_arr = attrs_batch
        .column(
            a_schema
                .index_of("double")
                .map_err(|e| ArrowError::SchemaError(format!("missing double: {e}")))?,
        )
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ArrowError::SchemaError("double not Float64".to_string()))?;

    let bool_arr = attrs_batch
        .column(
            a_schema
                .index_of("bool")
                .map_err(|e| ArrowError::SchemaError(format!("missing bool: {e}")))?,
        )
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArrowError::SchemaError("bool not Boolean".to_string()))?;

    let bytes_arr = attrs_batch
        .column(
            a_schema
                .index_of("bytes")
                .map_err(|e| ArrowError::SchemaError(format!("missing bytes: {e}")))?,
        )
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ArrowError::SchemaError("bytes not Binary".to_string()))?;

    for row in 0..attrs_batch.num_rows() {
        let parent_id = parent_id_arr.value(row);
        let target_row = row_for(parent_id);
        if target_row >= num_rows {
            continue; // Skip out-of-range rows.
        }

        let key = key_arr.value(row);
        let mapped_key = map_key(key);
        let type_tag = type_arr.value(row);

        // Preserve canonical LOGS fact fields when LOG_ATTRS use the same name.
        if protected_existing_cols.contains(mapped_key.as_str())
            && col_index.contains_key(mapped_key.as_str())
        {
            continue;
        }

        // Get or create the flat column with the correct type.
        let col_pos = if let Some(&idx) = col_index.get(&mapped_key) {
            idx
        } else {
            let idx = flat_cols.len();
            let col = match type_tag {
                ATTR_TYPE_INT => TypedColumn::new_int(num_rows),
                ATTR_TYPE_DOUBLE => TypedColumn::new_double(num_rows),
                ATTR_TYPE_BOOL => TypedColumn::new_bool(num_rows),
                ATTR_TYPE_BYTES => TypedColumn::new_bytes(num_rows),
                _ => TypedColumn::new_str(num_rows),
            };
            flat_cols.push((mapped_key.clone(), col));
            col_index.insert(mapped_key, idx);
            idx
        };

        if !matches!(
            type_tag,
            ATTR_TYPE_STR | ATTR_TYPE_INT | ATTR_TYPE_DOUBLE | ATTR_TYPE_BOOL | ATTR_TYPE_BYTES
        ) {
            return Err(ArrowError::SchemaError(format!(
                "unknown attr type tag {type_tag} for key '{}'",
                key_arr.value(row)
            )));
        }

        if !flat_cols[col_pos].1.matches_attr_type(type_tag) {
            flat_cols[col_pos].1.promote_to_str();
        }

        if let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1 {
            if let Some(value) = attrs_row_value_to_string(
                type_tag, str_arr, int_arr, double_arr, bool_arr, bytes_arr, row,
            ) {
                v[target_row] = Some(value);
            }
            continue;
        }

        // Write the typed value into the column.
        match type_tag {
            ATTR_TYPE_INT => {
                if !int_arr.is_null(row)
                    && let TypedColumn::Int(ref mut v) = flat_cols[col_pos].1
                {
                    v[target_row] = Some(int_arr.value(row));
                }
            }
            ATTR_TYPE_DOUBLE => {
                if !double_arr.is_null(row)
                    && let TypedColumn::Double(ref mut v) = flat_cols[col_pos].1
                {
                    v[target_row] = Some(double_arr.value(row));
                }
            }
            ATTR_TYPE_BOOL => {
                if !bool_arr.is_null(row)
                    && let TypedColumn::Bool(ref mut v) = flat_cols[col_pos].1
                {
                    v[target_row] = Some(bool_arr.value(row));
                }
            }
            ATTR_TYPE_BYTES => {
                // Bytes are stored in the binary column; preserve as raw bytes.
                if !bytes_arr.is_null(row)
                    && let TypedColumn::Bytes(ref mut v) = flat_cols[col_pos].1
                {
                    v[target_row] = Some(Arc::<[u8]>::from(bytes_arr.value(row)));
                }
            }
            ATTR_TYPE_STR => {}
            _ => {
                return Err(ArrowError::SchemaError(format!(
                    "unsupported column type: {}",
                    type_tag
                )));
            }
        }
    }

    Ok(())
}

fn attrs_row_value_to_string(
    type_tag: u8,
    str_arr: &StringArray,
    int_arr: &Int64Array,
    double_arr: &Float64Array,
    bool_arr: &BooleanArray,
    bytes_arr: &BinaryArray,
    row: usize,
) -> Option<String> {
    match type_tag {
        ATTR_TYPE_STR => (!str_arr.is_null(row)).then(|| str_arr.value(row).to_string()),
        ATTR_TYPE_INT => (!int_arr.is_null(row)).then(|| int_arr.value(row).to_string()),
        ATTR_TYPE_DOUBLE => (!double_arr.is_null(row)).then(|| double_arr.value(row).to_string()),
        ATTR_TYPE_BOOL => (!bool_arr.is_null(row)).then(|| bool_arr.value(row).to_string()),
        ATTR_TYPE_BYTES => {
            (!bytes_arr.is_null(row)).then(|| hex_encode_lower(bytes_arr.value(row)))
        }
        _ => None,
    }
}

/// Scatter resource attribute values from the "template" rows (indexed by
/// resource_id / parent_id) to all rows that share the same resource_id.
///
/// After `unpivot_attrs_to_flat`, resource attrs are only populated at the
/// row index matching the parent_id. This function copies those values to
/// every row whose resource_id matches.
fn scatter_resource_attrs(
    flat_cols: &mut [(String, TypedColumn)],
    col_index: &HashMap<String, usize>,
    resource_ids: &UInt32Array,
    num_rows: usize,
) {
    // Find all resource.attributes.* columns.
    let resource_col_indices: Vec<usize> = col_index
        .iter()
        .filter(|(name, _)| name.starts_with(RESOURCE_PREFIX))
        .map(|(_, &idx)| idx)
        .collect();

    if resource_col_indices.is_empty() {
        return;
    }

    // For each resource column, build a map of resource_id → value from
    // the template rows, then scatter to all matching rows.
    // Resource attrs are always strings (from resource.attributes.* flat columns).
    for &col_pos in &resource_col_indices {
        match &flat_cols[col_pos].1 {
            TypedColumn::Str(values) => {
                let rid_to_val = collect_resource_template_values(values, resource_ids, num_rows);
                if let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        let rid = resource_ids.value(row);
                        if let Some(val) = rid_to_val.get(&rid) {
                            *slot = val.clone();
                        }
                    }
                }
            }
            TypedColumn::Int(values) => {
                let rid_to_val = collect_resource_template_values(values, resource_ids, num_rows);
                if let TypedColumn::Int(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        let rid = resource_ids.value(row);
                        if let Some(val) = rid_to_val.get(&rid) {
                            *slot = *val;
                        }
                    }
                }
            }
            TypedColumn::Double(values) => {
                let rid_to_val = collect_resource_template_values(values, resource_ids, num_rows);
                if let TypedColumn::Double(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        let rid = resource_ids.value(row);
                        if let Some(val) = rid_to_val.get(&rid) {
                            *slot = *val;
                        }
                    }
                }
            }
            TypedColumn::Bool(values) => {
                let rid_to_val = collect_resource_template_values(values, resource_ids, num_rows);
                if let TypedColumn::Bool(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        let rid = resource_ids.value(row);
                        if let Some(val) = rid_to_val.get(&rid) {
                            *slot = *val;
                        }
                    }
                }
            }
            TypedColumn::Bytes(values) => {
                let rid_to_val = collect_resource_template_values(values, resource_ids, num_rows);
                if let TypedColumn::Bytes(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        let rid = resource_ids.value(row);
                        if let Some(val) = rid_to_val.get(&rid) {
                            *slot = val.clone();
                        }
                    }
                }
            }
        }
    }
}

fn collect_resource_template_values<T: Clone>(
    values: &[Option<T>],
    resource_ids: &UInt32Array,
    num_rows: usize,
) -> HashMap<u32, Option<T>> {
    let mut map: HashMap<u32, Option<T>> = HashMap::new();
    for row in 0..num_rows {
        let rid = resource_ids.value(row);
        if let std::collections::hash_map::Entry::Vacant(e) = map.entry(rid) {
            let rid_row = rid as usize;
            if rid_row < num_rows {
                e.insert(values[rid_row].clone());
            }
        }
    }
    map
}

/// Scatter scope attribute values from template rows (indexed by scope_id) to
/// all rows that share the same scope_id.
fn scatter_scope_attrs(
    flat_cols: &mut [(String, TypedColumn)],
    col_index: &HashMap<String, usize>,
    scope_ids: &UInt32Array,
    num_rows: usize,
) {
    let scope_col_indices: Vec<usize> = col_index
        .iter()
        .filter(|(name, _)| name.starts_with("scope.") || name.starts_with("_scope_"))
        .map(|(_, &idx)| idx)
        .collect();

    if scope_col_indices.is_empty() {
        return;
    }

    for &col_pos in &scope_col_indices {
        match &flat_cols[col_pos].1 {
            TypedColumn::Str(values) => {
                let sid_to_val = collect_template_values_by_id(values, scope_ids, num_rows);
                if let TypedColumn::Str(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        if slot.is_some() {
                            continue;
                        }
                        let sid = scope_ids.value(row);
                        if let Some(val) = sid_to_val.get(&sid) {
                            *slot = val.clone();
                        }
                    }
                }
            }
            TypedColumn::Int(values) => {
                let sid_to_val = collect_template_values_by_id(values, scope_ids, num_rows);
                if let TypedColumn::Int(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        if slot.is_some() {
                            continue;
                        }
                        let sid = scope_ids.value(row);
                        if let Some(val) = sid_to_val.get(&sid) {
                            *slot = *val;
                        }
                    }
                }
            }
            TypedColumn::Double(values) => {
                let sid_to_val = collect_template_values_by_id(values, scope_ids, num_rows);
                if let TypedColumn::Double(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        if slot.is_some() {
                            continue;
                        }
                        let sid = scope_ids.value(row);
                        if let Some(val) = sid_to_val.get(&sid) {
                            *slot = *val;
                        }
                    }
                }
            }
            TypedColumn::Bool(values) => {
                let sid_to_val = collect_template_values_by_id(values, scope_ids, num_rows);
                if let TypedColumn::Bool(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        if slot.is_some() {
                            continue;
                        }
                        let sid = scope_ids.value(row);
                        if let Some(val) = sid_to_val.get(&sid) {
                            *slot = *val;
                        }
                    }
                }
            }
            TypedColumn::Bytes(values) => {
                let sid_to_val = collect_template_values_by_id(values, scope_ids, num_rows);
                if let TypedColumn::Bytes(ref mut v) = flat_cols[col_pos].1 {
                    for (row, slot) in v.iter_mut().enumerate().take(num_rows) {
                        if slot.is_some() {
                            continue;
                        }
                        let sid = scope_ids.value(row);
                        if let Some(val) = sid_to_val.get(&sid) {
                            *slot = val.clone();
                        }
                    }
                }
            }
        }
    }
}

fn collect_template_values_by_id<T: Clone>(
    values: &[Option<T>],
    ids: &UInt32Array,
    num_rows: usize,
) -> HashMap<u32, Option<T>> {
    let mut map: HashMap<u32, Option<T>> = HashMap::new();
    for row in 0..num_rows {
        let id = ids.value(row);
        if let std::collections::hash_map::Entry::Vacant(e) = map.entry(id) {
            let template_row = id as usize;
            if template_row < values.len() {
                e.insert(values[template_row].clone());
            }
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;

    /// Helper: create a flat RecordBatch with mixed columns.
    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("flags", DataType::Int64, true),
            Field::new("resource.attributes.service_name", DataType::Utf8, true),
            Field::new("resource.attributes.k8s_pod", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T10:30:00.123456789Z"),
                Some("2024-01-15T10:30:01.000000000Z"),
                Some("2024-01-15T10:30:02.500Z"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("INFO"),
                Some("ERROR"),
                Some("WARN"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("request started"),
                Some("connection failed"),
                Some("retry attempt"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("00112233445566778899aabbccddeeff"),
                Some("fedcba98765432100123456789abcdef"),
                Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("0011223344556677"),
                Some("8899aabbccddeeff"),
                Some("1234567890abcdef"),
            ])),
            Arc::new(Int64Array::from(vec![Some(1), Some(3), Some(255)])),
            Arc::new(StringArray::from(vec![
                Some("api-server"),
                Some("api-server"),
                Some("worker"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("pod-abc"),
                Some("pod-abc"),
                Some("pod-xyz"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("host-1"),
                Some("host-2"),
                Some("host-1"),
            ])),
            Arc::new(Int64Array::from(vec![Some(200), Some(500), Some(429)])),
        ];

        RecordBatch::try_new(schema, columns).expect("valid batch")
    }

    #[test]
    fn roundtrip_preserves_data() {
        let flat = make_test_batch();
        let star = flat_to_star(&flat).expect("flat_to_star");

        // Verify star schema structure.
        assert_eq!(star.logs.num_rows(), 3);
        assert!(star.log_attrs.num_rows() > 0, "should have log attrs");
        assert!(
            star.resource_attrs.num_rows() > 0,
            "should have resource attrs"
        );
        assert_eq!(star.scope_attrs.num_rows(), 1);

        // Convert back.
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 3);

        // Verify well-known columns survived.
        let rt_schema = roundtrip.schema();

        // _timestamp
        let ts_idx = rt_schema.index_of("_timestamp").expect("_timestamp col");
        let ts_arr = roundtrip
            .column(ts_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ts string");
        // Timestamps may lose trailing zeros but must parse to the same value.
        assert!(
            ts_arr
                .value(0)
                .starts_with("2024-01-15T10:30:00.123456789Z")
        );
        assert!(ts_arr.value(1).starts_with("2024-01-15T10:30:01Z"));

        // level
        let lvl_idx = rt_schema.index_of("level").expect("level col");
        let lvl_arr = roundtrip
            .column(lvl_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("level string");
        assert_eq!(lvl_arr.value(0), "INFO");
        assert_eq!(lvl_arr.value(1), "ERROR");
        assert_eq!(lvl_arr.value(2), "WARN");

        // message
        let msg_idx = rt_schema.index_of("message").expect("message col");
        let msg_arr = roundtrip
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("msg string");
        assert_eq!(msg_arr.value(0), "request started");
        assert_eq!(msg_arr.value(1), "connection failed");
        assert_eq!(msg_arr.value(2), "retry attempt");

        let trace_idx = rt_schema.index_of("trace_id").expect("trace_id col");
        let trace_arr = roundtrip
            .column(trace_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("trace string");
        assert_eq!(trace_arr.value(0), "00112233445566778899aabbccddeeff");
        assert_eq!(trace_arr.value(1), "fedcba98765432100123456789abcdef");
        assert_eq!(trace_arr.value(2), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let span_idx = rt_schema.index_of("span_id").expect("span_id col");
        let span_arr = roundtrip
            .column(span_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("span string");
        assert_eq!(span_arr.value(0), "0011223344556677");
        assert_eq!(span_arr.value(1), "8899aabbccddeeff");
        assert_eq!(span_arr.value(2), "1234567890abcdef");

        let flags_idx = rt_schema.index_of("flags").expect("flags col");
        let flags_arr = roundtrip
            .column(flags_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("flags i64");
        assert_eq!(flags_arr.value(0), 1);
        assert_eq!(flags_arr.value(1), 3);
        assert_eq!(flags_arr.value(2), 255);

        let sev_num_idx = rt_schema
            .index_of("severity_number")
            .expect("severity_number col");
        let sev_num_arr = roundtrip
            .column(sev_num_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("severity_number i64");
        assert_eq!(sev_num_arr.value(0), 9); // INFO
        assert_eq!(sev_num_arr.value(1), 17); // ERROR
        assert_eq!(sev_num_arr.value(2), 13); // WARN

        let scope_name_idx = rt_schema.index_of("scope.name").expect("scope.name col");
        let scope_name_arr = roundtrip
            .column(scope_name_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("scope.name string");
        assert_eq!(scope_name_arr.value(0), "logfwd");
        assert_eq!(scope_name_arr.value(1), "logfwd");
        assert_eq!(scope_name_arr.value(2), "logfwd");

        let rs_idx = rt_schema
            .index_of("resource.attributes.service_name")
            .expect("resource_service_name col");
        let rs_arr = roundtrip
            .column(rs_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("resource str");
        assert_eq!(rs_arr.value(0), "api-server");
        assert_eq!(rs_arr.value(1), "api-server");
        assert_eq!(rs_arr.value(2), "worker");

        let rp_idx = rt_schema
            .index_of("resource.attributes.k8s_pod")
            .expect("resource_k8s_pod col");
        let rp_arr = roundtrip
            .column(rp_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("resource str");
        assert_eq!(rp_arr.value(0), "pod-abc");
        assert_eq!(rp_arr.value(1), "pod-abc");
        assert_eq!(rp_arr.value(2), "pod-xyz");

        // host (attribute column)
        let host_idx = rt_schema.index_of("host").expect("host col");
        let host_arr = roundtrip
            .column(host_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("host str");
        assert_eq!(host_arr.value(0), "host-1");
        assert_eq!(host_arr.value(1), "host-2");
        assert_eq!(host_arr.value(2), "host-1");

        // status (int attribute — now roundtrips as Int64)
        let st_idx = rt_schema.index_of("status").expect("status col");
        let st_arr = roundtrip
            .column(st_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("status i64");
        assert_eq!(st_arr.value(0), 200);
        assert_eq!(st_arr.value(1), 500);
        assert_eq!(st_arr.value(2), 429);
    }

    #[test]
    fn roundtrip_preserves_existing_scope_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("scope.name", DataType::Utf8, true),
            Field::new("scope.version", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(StringArray::from(vec![Some("otel"), Some("custom")])),
                Arc::new(StringArray::from(vec![Some("1.0.0"), Some("2.0.0")])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let scope_name = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.name")
                    .expect("scope.name idx"),
            )
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("scope.name utf8");
        assert_eq!(scope_name.value(0), "otel");
        assert_eq!(scope_name.value(1), "custom");

        let scope_version = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.version")
                    .expect("scope.version idx"),
            )
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("scope.version utf8");
        assert_eq!(scope_version.value(0), "1.0.0");
        assert_eq!(scope_version.value(1), "2.0.0");
    }

    #[test]
    fn roundtrip_preserves_typed_scope_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("scope.name", DataType::Utf8, true),
            Field::new("scope.enabled", DataType::Boolean, true),
            Field::new("scope.rank", DataType::Int64, true),
            Field::new("scope.ratio", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(StringArray::from(vec![Some("otel"), Some("otel")])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(true)])),
                Arc::new(Int64Array::from(vec![Some(7), Some(7)])),
                Arc::new(Float64Array::from(vec![Some(0.25), Some(0.25)])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let scope_enabled = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.enabled")
                    .expect("scope.enabled idx"),
            )
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("scope.enabled bool");
        assert_eq!(scope_enabled.value(0), true);
        assert_eq!(scope_enabled.value(1), true);

        let scope_rank = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.rank")
                    .expect("scope.rank idx"),
            )
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("scope.rank i64");
        assert_eq!(scope_rank.value(0), 7);
        assert_eq!(scope_rank.value(1), 7);

        let scope_ratio = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.ratio")
                    .expect("scope.ratio idx"),
            )
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("scope.ratio f64");
        assert_eq!(scope_ratio.value(0), 0.25);
        assert_eq!(scope_ratio.value(1), 0.25);
    }

    #[test]
    fn noncanonical_scope_columns_do_not_inject_default_scope_name() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("scope.enabled", DataType::Boolean, true),
            Field::new("scope.rank", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
                Arc::new(Int64Array::from(vec![Some(7), Some(9)])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        assert_eq!(
            star.scope_attrs.num_rows(),
            0,
            "any real scope.* columns should suppress the default scope row"
        );

        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert!(
            roundtrip.column_by_name("scope.name").is_none(),
            "default scope.name must not be synthesized when only noncanonical scope columns exist"
        );

        let scope_enabled = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.enabled")
                    .expect("scope.enabled idx"),
            )
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("scope.enabled bool");
        assert!(scope_enabled.value(0));
        assert!(!scope_enabled.value(1));

        let scope_rank = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.rank")
                    .expect("scope.rank idx"),
            )
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("scope.rank i64");
        assert_eq!(scope_rank.value(0), 7);
        assert_eq!(scope_rank.value(1), 9);
    }

    #[test]
    fn empty_batch_produces_empty_star() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::new_empty(schema);
        let star = flat_to_star(&batch).expect("flat_to_star empty");

        assert_eq!(star.logs.num_rows(), 0);
        assert_eq!(star.log_attrs.num_rows(), 0);
        assert_eq!(star.resource_attrs.num_rows(), 0);
        assert_eq!(star.scope_attrs.num_rows(), 0);

        let roundtrip = star_to_flat(&star).expect("star_to_flat empty");
        assert_eq!(roundtrip.num_rows(), 0);
    }

    #[test]
    fn collect_template_values_prefers_template_row_index() {
        let values = vec![None, None, Some("otel".to_string())];
        let ids = UInt32Array::from(vec![2u32, 2, 2]);

        let collected = collect_template_values_by_id(&values, &ids, 3);

        assert_eq!(collected.get(&2), Some(&Some("otel".to_string())));
    }

    #[test]
    fn collect_template_values_skips_template_ids_outside_column_len() {
        let values = vec![Some("otel".to_string())];
        let ids = UInt32Array::from(vec![0u32, 1]);

        let collected = collect_template_values_by_id(&values, &ids, 2);

        assert_eq!(collected.get(&0), Some(&Some("otel".to_string())));
        assert!(!collected.contains_key(&1));
    }

    #[test]
    fn batch_with_no_resource_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["hello", "world"])),
            Arc::new(StringArray::from(vec!["INFO", "DEBUG"])),
            Arc::new(StringArray::from(vec!["host-a", "host-b"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.logs.num_rows(), 2);
        assert_eq!(star.resource_attrs.num_rows(), 0);
        assert!(star.log_attrs.num_rows() > 0);

        // All rows share the same (empty) resource set → single resource_id.
        let rid_arr = star
            .logs
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("rid");
        assert_eq!(rid_arr.value(0), rid_arr.value(1));

        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 2);

        let msg_idx = roundtrip.schema().index_of("message").expect("msg");
        let msg_arr = roundtrip
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(msg_arr.value(0), "hello");
        assert_eq!(msg_arr.value(1), "world");
    }

    #[test]
    fn flat_to_star_prefers_canonical_body_over_message_alias() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["alias-value"])),
            Arc::new(StringArray::from(vec!["canonical-value"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let body_idx = star.logs.schema().index_of("body_str").expect("body_str");
        let body_arr = star
            .logs
            .column(body_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body_str should be Utf8");
        assert_eq!(
            body_arr.value(0),
            "canonical-value",
            "canonical body should win over alias columns"
        );
    }

    #[test]
    fn flat_to_star_keeps_unselected_body_alias_as_attribute() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["alias-value"])),
            Arc::new(StringArray::from(vec!["canonical-value"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let key_idx = star.log_attrs.schema().index_of("key").expect("key");
        let str_idx = star.log_attrs.schema().index_of("str").expect("str");
        let keys = star
            .log_attrs
            .column(key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key should be Utf8");
        let values = star
            .log_attrs
            .column(str_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str should be Utf8");
        let message_value = keys
            .iter()
            .zip(values.iter())
            .find_map(|(key, value)| if key == Some("message") { value } else { None });

        assert_eq!(message_value, Some("alias-value"));
    }

    #[test]
    fn batch_with_only_resource_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.service", DataType::Utf8, true),
            Field::new("resource.attributes.env", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["api", "worker"])),
            Arc::new(StringArray::from(vec!["prod", "prod"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.logs.num_rows(), 2);
        assert!(star.resource_attrs.num_rows() > 0);
        assert_eq!(star.log_attrs.num_rows(), 0);

        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 2);

        let svc_idx = roundtrip
            .schema()
            .index_of("resource.attributes.service")
            .expect("svc");
        let svc_arr = roundtrip
            .column(svc_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(svc_arr.value(0), "api");
        assert_eq!(svc_arr.value(1), "worker");
    }

    #[test]
    fn legacy_resource_prefix_is_regular_log_attribute() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("_resource_service", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["hello"])),
            Arc::new(StringArray::from(vec!["api"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.resource_attrs.num_rows(), 0);
        assert_eq!(star.log_attrs.num_rows(), 1);

        let key_idx = star.log_attrs.schema().index_of("key").expect("key");
        let keys = star
            .log_attrs
            .column(key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key strings");
        assert_eq!(keys.value(0), "_resource_service");
    }

    #[test]
    fn null_values_handled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
            Arc::new(StringArray::from(vec![None, Some("ERROR"), None])),
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T10:30:00Z"),
                None,
                Some("2024-01-15T10:30:02Z"),
            ])),
            Arc::new(StringArray::from(vec![None, None, Some("extra-val")])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.logs.num_rows(), 3);

        // Verify nulls in body.
        let body_idx = star.logs.schema().index_of("body_str").expect("body_str");
        let body_arr = star
            .logs
            .column(body_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert!(!body_arr.is_null(0));
        assert!(body_arr.is_null(1)); // was None in input
        assert!(!body_arr.is_null(2));

        // Roundtrip.
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 3);

        let msg_idx = roundtrip.schema().index_of("message").expect("msg");
        let msg_arr = roundtrip
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(msg_arr.value(0), "hello");
        assert!(msg_arr.is_null(1));
        assert_eq!(msg_arr.value(2), "world");
    }

    #[test]
    fn resource_deduplication() {
        // Rows 0 and 1 share the same resource attrs, row 2 differs.
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.svc", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["api", "api", "worker"])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        // Should have 2 distinct resource sets.
        assert_eq!(star.resource_attrs.num_rows(), 2);

        // resource_ids: rows 0,1 share one, row 2 has another.
        let rid_arr = star
            .logs
            .column(1) // resource_id
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("rid");
        assert_eq!(rid_arr.value(0), rid_arr.value(1));
        assert_ne!(rid_arr.value(0), rid_arr.value(2));
    }

    #[test]
    fn resource_dedup_distinguishes_null_and_empty_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.svc", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![None, Some(""), Some("")])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let rid_arr = star
            .logs
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("resource_id");
        assert_ne!(
            rid_arr.value(0),
            rid_arr.value(1),
            "NULL and empty-string resource attrs must dedupe separately"
        );
        assert_eq!(rid_arr.value(1), rid_arr.value(2));
    }

    #[test]
    fn severity_mapping() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL",
            ])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        let sev_idx = star.logs.schema().index_of("severity_number").expect("sev");
        let sev_arr = star
            .logs
            .column(sev_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");

        assert_eq!(sev_arr.value(0), 1); // TRACE
        assert_eq!(sev_arr.value(1), 5); // DEBUG
        assert_eq!(sev_arr.value(2), 9); // INFO
        assert_eq!(sev_arr.value(3), 13); // WARN
        assert_eq!(sev_arr.value(4), 17); // ERROR
        assert_eq!(sev_arr.value(5), 21); // FATAL
    }

    #[test]
    fn star_schema_table_schemas() {
        let batch = make_test_batch();
        let star = flat_to_star(&batch).expect("flat_to_star");

        // Verify LOGS schema.
        let logs_schema = star.logs.schema();
        let logs_fields: Vec<&str> = logs_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            logs_fields,
            vec![
                "id",
                "resource_id",
                "scope_id",
                "time_unix_nano",
                "severity_number",
                "severity_text",
                "body_str",
                "trace_id",
                "span_id",
                "flags",
                "dropped_attributes_count"
            ]
        );

        // Verify attrs schema.
        let attrs_schema = star.log_attrs.schema();
        let attrs_fields: Vec<&str> = attrs_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            attrs_fields,
            vec![
                "parent_id",
                "key",
                "type",
                "str",
                "int",
                "double",
                "bool",
                "bytes"
            ]
        );
    }

    #[test]
    fn int_attrs_use_typed_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["hello"])),
            Arc::new(Int64Array::from(vec![Some(200)])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        // LOG_ATTRS should have the status as an int attr.
        let type_arr = star
            .log_attrs
            .column(2) // type
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("u8");

        let key_arr = star
            .log_attrs
            .column(1) // key
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");

        // Find the status attr row.
        let mut found = false;
        for row in 0..star.log_attrs.num_rows() {
            if key_arr.value(row) == "status" {
                assert_eq!(type_arr.value(row), ATTR_TYPE_INT);
                let int_arr = star
                    .log_attrs
                    .column(4) // int
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("i64");
                assert_eq!(int_arr.value(row), 200);
                found = true;
                break;
            }
        }
        assert!(found, "status attr not found in log_attrs");
    }

    #[test]
    fn binary_attrs_roundtrip_as_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("row-0"), Some("row-1")])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0x00_u8, 0x0f_u8, 0xff_u8][..]),
                Some(&[0xab_u8, 0xcd_u8][..]),
            ])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid batch");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let payload_idx = roundtrip.schema().index_of("payload").expect("payload col");
        let payload_arr = roundtrip
            .column(payload_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("payload binary");

        assert_eq!(payload_arr.value(0), &[0x00, 0x0f, 0xff]);
        assert_eq!(payload_arr.value(1), &[0xab, 0xcd]);
    }

    #[test]
    fn mixed_attrs_promote_bytes_first_column_to_string() {
        let logs = RecordBatch::try_new(
            Arc::new(logs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32, 1])),
                Arc::new(UInt32Array::from(vec![0_u32, 0])),
                Arc::new(UInt32Array::from(vec![0_u32, 0])),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>, None])),
                Arc::new(Int32Array::from(vec![None::<i32>, None])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![Some("row-0"), Some("row-1")])),
                build_fixed_binary_array::<16>(&[None, None]).expect("trace ids"),
                build_fixed_binary_array::<8>(&[None, None]).expect("span ids"),
                Arc::new(UInt32Array::from(vec![None::<u32>, None])),
                Arc::new(UInt32Array::from(vec![Some(0_u32), Some(0)])),
            ],
        )
        .expect("valid logs");

        let log_attrs = RecordBatch::try_new(
            Arc::new(attrs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32, 1])),
                Arc::new(StringArray::from(vec!["payload", "payload"])),
                Arc::new(UInt8Array::from(vec![ATTR_TYPE_BYTES, ATTR_TYPE_STR])),
                Arc::new(StringArray::from(vec![None, Some("text")])),
                Arc::new(Int64Array::from(vec![None::<i64>, None])),
                Arc::new(Float64Array::from(vec![None::<f64>, None])),
                Arc::new(BooleanArray::from(vec![None::<bool>, None])),
                Arc::new(BinaryArray::from(vec![Some(&[0xde_u8, 0xad_u8][..]), None])),
            ],
        )
        .expect("valid attrs");

        let star = StarSchema {
            logs,
            log_attrs,
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        let payload_idx = roundtrip.schema().index_of("payload").expect("payload");
        let payload_arr = roundtrip
            .column(payload_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("mixed column promotes to Utf8");

        assert_eq!(payload_arr.value(0), "dead");
        assert_eq!(payload_arr.value(1), "text");
    }

    #[test]
    fn log_attr_collision_does_not_overwrite_fact_column() {
        let logs = RecordBatch::try_new(
            Arc::new(logs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>])),
                Arc::new(Int32Array::from(vec![Some(9_i32)])),
                Arc::new(StringArray::from(vec![Some("ERROR")])),
                Arc::new(StringArray::from(vec![Some("row-0")])),
                build_fixed_binary_array::<16>(&[None]).expect("trace ids"),
                build_fixed_binary_array::<8>(&[None]).expect("span ids"),
                Arc::new(UInt32Array::from(vec![Some(3_u32)])),
                Arc::new(UInt32Array::from(vec![Some(0_u32)])),
            ],
        )
        .expect("valid logs");

        let log_attrs = RecordBatch::try_new(
            Arc::new(attrs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(StringArray::from(vec!["flags"])),
                Arc::new(UInt8Array::from(vec![ATTR_TYPE_STR])),
                Arc::new(StringArray::from(vec![Some("not-flags")])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Float64Array::from(vec![None::<f64>])),
                Arc::new(BooleanArray::from(vec![None::<bool>])),
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
            ],
        )
        .expect("valid attrs");

        let star = StarSchema {
            logs,
            log_attrs,
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        let flags_idx = roundtrip.schema().index_of("flags").expect("flags");
        let flags_arr = roundtrip
            .column(flags_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("fact flags stays int");

        assert_eq!(flags_arr.value(0), 3);
    }

    #[test]
    fn binary_resource_attrs_roundtrip_as_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("resource.attributes.payload", DataType::Binary, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("row-0"),
                Some("row-1"),
                Some("row-2"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0xde_u8, 0xad_u8, 0xbe_u8, 0xef_u8][..]),
                Some(&[0xde_u8, 0xad_u8, 0xbe_u8, 0xef_u8][..]),
                Some(&[0x01_u8, 0x02_u8][..]),
            ])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid batch");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let payload_idx = roundtrip
            .schema()
            .index_of("resource.attributes.payload")
            .expect("resource.attributes.payload col");
        let payload_arr = roundtrip
            .column(payload_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("resource.attributes.payload binary");

        assert_eq!(payload_arr.value(0), &[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(payload_arr.value(1), &[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(payload_arr.value(2), &[0x01, 0x02]);
    }

    #[test]
    fn typed_resource_attrs_scatter_for_duplicate_resource_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.retry_count", DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(9)])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let idx = roundtrip
            .schema()
            .index_of("resource.attributes.retry_count")
            .expect("resource.attributes.retry_count");
        let arr = roundtrip
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(arr.value(0), 5);
        assert_eq!(arr.value(1), 5);
        assert_eq!(arr.value(2), 9);
    }

    #[test]
    fn test_parse_timestamp_to_nanos_integer() {
        // Bug #1028: correctly scale magnitude-based epoch values
        // seconds
        assert_eq!(
            parse_timestamp_to_nanos("1705314600"),
            Some(1_705_314_600_000_000_000)
        );
        // milliseconds
        assert_eq!(
            parse_timestamp_to_nanos("1705314600123"),
            Some(1_705_314_600_123_000_000)
        );
        // microseconds
        assert_eq!(
            parse_timestamp_to_nanos("1705314600123456"),
            Some(1_705_314_600_123_456_000)
        );
        // nanoseconds
        assert_eq!(
            parse_timestamp_to_nanos("1705314600123456789"),
            Some(1_705_314_600_123_456_789)
        );
    }

    #[test]
    fn test_parse_timestamp_to_nanos_overflow() {
        // Bug #1085: integer seconds should not panic on overflow (e.g. year 2300)
        // 10_413_792_000 seconds = ~year 2300.
        // Multiply by 1_000_000_000 -> 10_413_792_000_000_000_000, which overflows i64.
        assert_eq!(parse_timestamp_to_nanos("10413792000"), None);
    }

    #[test]
    fn test_parse_rfc3339_nanos_invalid_time() {
        // Bug #1088: reject invalid time values
        assert_eq!(parse_rfc3339_nanos("2024-01-01T99:99:99Z"), None);
        assert_eq!(parse_rfc3339_nanos("2024-01-01T24:00:00Z"), None);
        assert_eq!(parse_rfc3339_nanos("2024-01-01T23:60:00Z"), None);
        assert_eq!(parse_rfc3339_nanos("2024-01-01T23:59:61Z"), None); // 60 is leap second, 61 is invalid
    }

    #[test]
    fn timestamp_roundtrip_precision() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let ts = "2024-06-15T08:30:45.123456789Z";
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some(ts)])),
            Arc::new(StringArray::from(vec![Some("test")])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let ts_idx = roundtrip.schema().index_of("_timestamp").expect("ts");
        let ts_arr = roundtrip
            .column(ts_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");

        // Verify nanosecond precision is preserved.
        assert_eq!(ts_arr.value(0), ts);
    }

    #[test]
    fn star_to_flat_returns_error_for_invalid_logs_column_types() {
        let logs_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            // Wrong type: should be Timestamp(Nanosecond)
            Field::new("time_unix_nano", DataType::Utf8, true),
            // Wrong type: should be Utf8/Utf8View
            Field::new("severity_text", DataType::Int64, true),
            // Wrong type: should be Utf8/Utf8View
            Field::new("body_str", DataType::Boolean, true),
        ]));

        let logs = RecordBatch::try_new(
            logs_schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(StringArray::from(vec![Some("2024-01-01T00:00:00Z")])),
                Arc::new(Int64Array::from(vec![Some(9i64)])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .expect("valid malformed logs batch");

        let star = StarSchema {
            logs,
            log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };

        let err = star_to_flat(&star).expect_err("invalid schema must return error");
        assert!(
            err.to_string()
                .contains("time_unix_nano must be Timestamp(Nanosecond)"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_optional_logs_column_types() {
        let logs_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            Field::new("severity_number", DataType::Utf8, true),
        ]));
        let logs = RecordBatch::try_new(
            logs_schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(StringArray::from(vec![Some("9")])),
            ],
        )
        .expect("valid malformed logs batch");
        let star = StarSchema {
            logs,
            log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };

        let err = star_to_flat(&star).expect_err("invalid severity_number type must fail");
        assert!(
            err.to_string().contains("severity_number must be Int32"),
            "unexpected error: {err}"
        );
    }

    fn assert_optional_logs_column_type_error(field: Field, column: ArrayRef, expected: &str) {
        let field_name = field.name().clone();
        let logs_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            field,
        ]));
        let logs = RecordBatch::try_new(
            logs_schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                column,
            ],
        )
        .expect("valid malformed logs batch");
        let star = StarSchema {
            logs,
            log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };

        let err = star_to_flat(&star).expect_err("invalid optional LOGS column type must fail");
        let message = err.to_string();
        assert!(
            message.contains(&field_name),
            "expected error to mention {field_name}, got: {message}"
        );
        assert!(
            message.contains(expected),
            "expected error to mention {expected}, got: {message}"
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_trace_id_type() {
        assert_optional_logs_column_type_error(
            Field::new("trace_id", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("not-binary")])) as ArrayRef,
            "FixedSizeBinary(16)",
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_span_id_type() {
        assert_optional_logs_column_type_error(
            Field::new("span_id", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("not-binary")])) as ArrayRef,
            "FixedSizeBinary(8)",
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_flags_type() {
        assert_optional_logs_column_type_error(
            Field::new("flags", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(1i64)])) as ArrayRef,
            "UInt32",
        );
    }

    #[test]
    fn empty_string_log_attr_survives_star_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("user.id", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(StringArray::from(vec![Some(""), Some("alice")])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let idx = roundtrip.schema().index_of("user.id").expect("user.id");
        let values = roundtrip
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert!(!values.is_null(0), "empty string must not become NULL");
        assert_eq!(values.value(0), "");
        assert_eq!(values.value(1), "alice");
    }

    /// Conflict struct columns (e.g. `status: Struct { int, str }`) must be
    /// normalized to flat Utf8 before OTAP encoding.
    ///
    /// Before the fix, `flat_to_star` passed conflict struct columns directly
    /// to `build_log_attrs`, where `attr_type_for(DataType::Struct(...))` returned
    /// `ATTR_TYPE_STR`, `str_value_at` on a StructArray returned `""`, and the
    /// value was pushed as `None`.  The OTAP receiver would see a LOG_ATTR row
    /// with type=STR and str=NULL — the actual field value was silently dropped.
    ///
    /// After the fix, `flat_to_star` calls `normalize_conflict_columns` first,
    /// replacing the struct with a flat Utf8 column, so the value is preserved.
    #[test]
    fn conflict_struct_column_preserved_in_log_attrs() {
        use arrow::array::{Int64Array, StructArray};

        // Build a batch with a conflict struct column (status: Struct { int, str })
        // mirroring what the streaming_builder emits for mixed-type fields.
        let int_arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(200), None]));
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, Some("NOT_FOUND")]));

        let struct_field = Field::new(
            "status",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Field::new("int", DataType::Int64, true),
                Field::new("str", DataType::Utf8, true),
            ])),
            true,
        );
        let struct_col: ArrayRef = Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Field::new("int", DataType::Int64, true),
                Field::new("str", DataType::Utf8, true),
            ]),
            vec![int_arr, str_arr],
            None,
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            struct_field,
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("request ok"),
                    Some("not found"),
                ])),
                struct_col,
            ],
        )
        .expect("valid batch");

        // Confirm the batch actually has a conflict struct column.
        assert!(
            batch.schema().fields().iter().any(|f| {
                if let DataType::Struct(cf) = f.data_type() {
                    !cf.is_empty()
                        && cf
                            .iter()
                            .all(|c| matches!(c.name().as_str(), "int" | "float" | "str" | "bool"))
                } else {
                    false
                }
            }),
            "test batch must contain a conflict struct column"
        );

        let star = flat_to_star(&batch).expect("flat_to_star");

        // The conflict struct column `status` must appear in log_attrs with
        // non-NULL string values ("200" for row 0, "NOT_FOUND" for row 1).
        let log_attrs = &star.log_attrs;
        let key_arr = log_attrs
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let str_arr = log_attrs
            .column_by_name("str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let status_values: Vec<_> = (0..log_attrs.num_rows())
            .filter(|&i| key_arr.value(i) == "status")
            .map(|i| {
                if str_arr.is_null(i) {
                    None
                } else {
                    Some(str_arr.value(i))
                }
            })
            .collect();
        for value in &status_values {
            match value {
                Some("200" | "NOT_FOUND") => {}
                other => panic!("unexpected status value in log_attrs: {:?}", other),
            }
        }
        assert!(
            status_values.iter().any(|value| *value == Some("200")),
            "status=200 (from int child) must appear in log_attrs, got: {:?}",
            status_values
                .iter()
                .map(|value| value.map_or_else(|| "NULL".to_string(), str::to_string))
                .collect::<Vec<_>>()
        );
        assert!(
            status_values
                .iter()
                .any(|value| *value == Some("NOT_FOUND")),
            "status=NOT_FOUND (from str child) must appear in log_attrs"
        );
    }

    /// Regression test for #1669: `@timestamp` (Elasticsearch convention) must
    /// map to `time_unix_nano` in the LOGS fact table, not fall through to
    /// LOG_ATTRS as an unrecognised string attribute.
    #[test]
    fn at_timestamp_col_maps_to_time_unix_nano() {
        let ts_ns: i64 = 1_705_314_600_000_000_000; // 2024-01-15T10:30:00Z
        let schema = Arc::new(Schema::new(vec![
            Field::new("@timestamp", DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![ts_ns])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();

        let star = flat_to_star(&batch).unwrap();

        // time_unix_nano in the LOGS table must be populated.
        let time_col = star
            .logs
            .column_by_name("time_unix_nano")
            .expect("time_unix_nano column must exist");
        assert!(!time_col.is_null(0), "time_unix_nano must not be NULL");

        // @timestamp must NOT appear as a LOG_ATTR.
        let key_col = star
            .log_attrs
            .column_by_name("key")
            .expect("key column must exist")
            .as_string::<i32>();
        let has_at_timestamp_attr = (0..key_col.len()).any(|i| key_col.value(i) == "@timestamp");
        assert!(
            !has_at_timestamp_attr,
            "@timestamp must map to time_unix_nano, not appear as a LOG_ATTR"
        );
    }
}

#[cfg(test)]
mod str_value_at_tests {
    use super::*;

    #[test]
    fn int_types() {
        let i8_arr = Int8Array::from(vec![Some(-1i8)]);
        assert_eq!(str_value_at(&i8_arr, 0), "-1");
        let i16_arr = Int16Array::from(vec![Some(256i16)]);
        assert_eq!(str_value_at(&i16_arr, 0), "256");
        let i32_arr = Int32Array::from(vec![Some(100_000i32)]);
        assert_eq!(str_value_at(&i32_arr, 0), "100000");
        let i64_arr = Int64Array::from(vec![Some(42i64)]);
        assert_eq!(str_value_at(&i64_arr, 0), "42");
    }

    #[test]
    fn uint_types() {
        let u8_arr = UInt8Array::from(vec![Some(255u8)]);
        assert_eq!(str_value_at(&u8_arr, 0), "255");
        let u16_arr = UInt16Array::from(vec![Some(65535u16)]);
        assert_eq!(str_value_at(&u16_arr, 0), "65535");
        let u32_arr = UInt32Array::from(vec![Some(123456u32)]);
        assert_eq!(str_value_at(&u32_arr, 0), "123456");
        let u64_arr = UInt64Array::from(vec![Some(u64::MAX)]);
        assert_eq!(str_value_at(&u64_arr, 0), u64::MAX.to_string());
    }

    #[test]
    fn float_types() {
        let f32_arr = Float32Array::from(vec![Some(3.14f32)]);
        let val = str_value_at(&f32_arr, 0);
        assert!(val.starts_with("3.14"), "got: {val}");
        let f64_arr = Float64Array::from(vec![Some(2.718f64)]);
        let val = str_value_at(&f64_arr, 0);
        assert!(val.starts_with("2.718"), "got: {val}");
    }

    #[test]
    fn large_utf8() {
        let arr = LargeStringArray::from(vec![Some("hello large")]);
        assert_eq!(str_value_at(&arr, 0), "hello large");
    }

    #[test]
    fn null_returns_empty() {
        let arr = Int32Array::from(vec![None::<i32>]);
        assert_eq!(str_value_at(&arr, 0), "");
    }

    #[test]
    fn timestamp_nanos_normalized() {
        // TimestampNanosecondArray: raw value is already in nanos, no scaling needed.
        let arr = TimestampNanosecondArray::from(vec![Some(1_000_000_000i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
        // Verify parse_timestamp_to_nanos recognizes this as nanos (magnitude > 1e17
        // is not met here but > 1e8 so it's treated as seconds then multiplied -- but
        // we actually want to check the full round-trip).
        let nanos = parse_timestamp_to_nanos(&str_value_at(&arr, 0));
        assert_eq!(nanos, Some(1_000_000_000_000_000_000i64));
    }

    #[test]
    fn timestamp_seconds_normalized_to_nanos() {
        // TimestampSecondArray with value 1 (= 1 second) should produce "1000000000"
        // (1e9 nanos), not "1" which parse_timestamp_to_nanos would misclassify.
        let arr = TimestampSecondArray::from(vec![Some(1i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
    }

    #[test]
    fn timestamp_millis_normalized_to_nanos() {
        // TimestampMillisecondArray with value 1000 (= 1 second) should produce "1000000000".
        let arr = TimestampMillisecondArray::from(vec![Some(1000i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
    }

    #[test]
    fn timestamp_micros_normalized_to_nanos() {
        // TimestampMicrosecondArray with value 1_000_000 (= 1 second) should produce "1000000000".
        let arr = TimestampMicrosecondArray::from(vec![Some(1_000_000i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
    }

    #[test]
    fn fallback_exotic_type() {
        // Date32 is not handled by any explicit match arm, so it exercises the
        // `_ => array_value_to_string(...)` fallback.
        let arr = arrow::array::Date32Array::from(vec![Some(0i32)]); // 1970-01-01
        let val = str_value_at(&arr, 0);
        assert!(
            val.contains("1970-01-01"),
            "expected Date32 fallback to produce a date string, got: {val}"
        );
    }
}
