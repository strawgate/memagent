//! OTAP star schema to flat RecordBatch conversion.
// xtask-verify: allow(pub_module_needs_tests) // Tested via star_schema/tests.rs

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    UInt8Array, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use super::helpers::{
    attrs_row_value_to_string, chrono_timestamp, hex_encode_lower, str_from_array,
};
use super::{
    ATTR_TYPE_BOOL, ATTR_TYPE_BYTES, ATTR_TYPE_DOUBLE, ATTR_TYPE_INT, ATTR_TYPE_STR,
    RESOURCE_PREFIX, StarSchema,
};

use arrow::array::TimestampNanosecondArray;

// ---------------------------------------------------------------------------
// TypedColumn
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

/// Convert OTAP star schema back to ffwd's flat `RecordBatch`.
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
    // Populate default scope fields first (e.g., synthetic "ffwd" scope).
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
// Unpivot and scatter helpers
// ---------------------------------------------------------------------------

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
                    "unsupported column type: {type_tag}"
                )));
            }
        }
    }

    Ok(())
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

pub(super) fn collect_template_values_by_id<T: Clone>(
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
