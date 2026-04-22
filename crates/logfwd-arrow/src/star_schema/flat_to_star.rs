//! Flat RecordBatch to OTAP star schema conversion.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    StringArray, TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use logfwd_types::field_names;

use crate::conflict_schema::{has_conflict_struct_columns, normalize_conflict_columns};

use super::helpers::{
    attr_type_for, build_fixed_binary_array, hex_to_fixed, parse_timestamp_to_nanos,
    severity_text_to_number, str_value_at,
};
use super::{
    ATTR_TYPE_BOOL, ATTR_TYPE_BYTES, ATTR_TYPE_DOUBLE, ATTR_TYPE_INT, ATTR_TYPE_STR,
    RESOURCE_PREFIX, StarSchema, attrs_schema, is_well_known_body, is_well_known_flags,
    is_well_known_severity, is_well_known_span_id, is_well_known_timestamp, is_well_known_trace_id,
    logs_schema,
};

/// Build an empty `StarSchema` with zero rows.
pub(super) fn empty_star_schema() -> StarSchema {
    StarSchema {
        logs: RecordBatch::new_empty(Arc::new(logs_schema())),
        log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
    }
}

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

        if field_names::is_internal_column(name) {
            continue;
        }

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
// Builder helpers
// ---------------------------------------------------------------------------

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
