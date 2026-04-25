//! Column resolution and downcast types for OTLP encoding.
//!
//! Scans a RecordBatch schema once to classify columns into well-known OTLP roles
//! (timestamp, severity, body, trace context) vs. generic attributes, and downcasts
//! Arrow arrays so the per-row encoding loop avoids repeated schema lookups.

use arrow::array::{
    Array, AsArray, BinaryArray, LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray,
    StringViewArray,
};
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use ffwd_core::otlp::{self, bytes_field_size, encode_bytes_field};
use ffwd_types::field_names;

use super::types::{FlagsArray, ResourceValueRef};

/// Pre-downcast array variant for an attribute column.
#[derive(Clone, Copy)]
pub(super) enum OtlpStrCol<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
    LargeUtf8(&'a LargeStringArray),
}

impl OtlpStrCol<'_> {
    #[inline(always)]
    pub(super) fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(row),
            Self::Utf8View(arr) => arr.is_null(row),
            Self::LargeUtf8(arr) => arr.is_null(row),
        }
    }

    #[inline(always)]
    pub(super) fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(arr) => arr.value(row),
            Self::Utf8View(arr) => arr.value(row),
            Self::LargeUtf8(arr) => arr.value(row),
        }
    }
}

pub(super) enum AttrArray<'a> {
    Str(OtlpStrCol<'a>),
    OtherStr(&'a dyn Array),
    PreformattedStr(Vec<Option<String>>),
    Bytes(&'a BinaryArray),
    LargeBytes(&'a LargeBinaryArray),
    Int(&'a PrimitiveArray<Int64Type>),
    UInt(&'a PrimitiveArray<UInt64Type>),
    Float(&'a PrimitiveArray<Float64Type>),
    Bool(&'a arrow::array::BooleanArray),
}

impl AttrArray<'_> {
    #[inline(always)]
    pub(super) fn value_ref(&self, row: usize, field_name: &str) -> Option<ResourceValueRef<'_>> {
        match self {
            Self::Str(arr) => (!arr.is_null(row)).then(|| ResourceValueRef::Str(arr.value(row))),
            Self::OtherStr(arr) => {
                if !arr.is_null(row) {
                    tracing::warn!(
                        column = field_name,
                        row,
                        "skipping OTLP resource attribute: unsupported non-string column was not preformatted"
                    );
                }
                None
            }
            Self::PreformattedStr(values) => values
                .get(row)
                .and_then(|value| value.as_deref())
                .map(ResourceValueRef::Str),
            Self::Bytes(arr) => {
                (!arr.is_null(row)).then(|| ResourceValueRef::Bytes(arr.value(row)))
            }
            Self::LargeBytes(arr) => {
                (!arr.is_null(row)).then(|| ResourceValueRef::Bytes(arr.value(row)))
            }
            Self::Int(arr) => (!arr.is_null(row)).then(|| ResourceValueRef::Int(arr.value(row))),
            Self::UInt(arr) => {
                if arr.is_null(row) {
                    return None;
                }
                let value = arr.value(row);
                match i64::try_from(value) {
                    Ok(value) => Some(ResourceValueRef::Int(value)),
                    Err(_) => {
                        tracing::warn!(
                            column = field_name,
                            row,
                            value,
                            "skipping OTLP resource attribute: UInt64 value exceeds AnyValue.int_value range"
                        );
                        None
                    }
                }
            }
            Self::Float(arr) => {
                (!arr.is_null(row)).then(|| ResourceValueRef::Float(arr.value(row).to_bits()))
            }
            Self::Bool(arr) => (!arr.is_null(row)).then(|| ResourceValueRef::Bool(arr.value(row))),
        }
    }
}

/// Per-column attribute metadata, resolved once per batch.
///
/// Carries pre-encoded key bytes alongside the downcast array so the per-row
/// encoding loop can skip recomputing the key tag + varint + bytes on every
/// iteration.
pub(super) struct ColAttr<'a> {
    /// Original field name used for error messages and fallback formatting.
    pub(super) name: String,
    /// Pre-encoded `encode_bytes_field(KEY_VALUE_KEY, name_bytes)` — constant
    /// for every row in the batch.  Written with a single `extend_from_slice`
    /// instead of three separate encode_tag / encode_varint / extend calls.
    pub(super) key_encoding: Vec<u8>,
    /// `bytes_field_size(KEY_VALUE_KEY, name.len())` — constant per column.
    /// Used directly when computing outer KV message size on each row.
    pub(super) kv_key_cost: usize,
    /// Pre-computed `kv_key_cost + 4` used by the short-string fast path.
    ///
    /// When `string_value.len() <= 125` all intermediate length varints fit in
    /// one byte, so the outer KV length is simply `short_kv_inner_base +
    /// value.len()` — two additions replace four `varint_len` calls.
    pub(super) short_kv_inner_base: usize,
    /// True when `array.null_count() > 0`.  When false the per-row `is_null`
    /// check is skipped entirely, eliminating a null-bitmap pointer fetch and
    /// branch on every row for columns that never contain nulls.
    pub(super) has_nulls: bool,
    /// The downcast Arrow array.
    pub(super) array: AttrArray<'a>,
}

/// Pre-resolved column roles and downcast arrays for one RecordBatch.
///
/// Built once in `encode_batch` before the per-row loop to avoid
/// re-scanning the schema and re-downcasting arrays on every row.
pub(super) struct BatchColumns<'a> {
    /// Downcast array for the timestamp column (e.g. "2024-01-15T10:30:00Z").
    pub(super) timestamp_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Raw array for Int64/UInt64/Timestamp timestamp columns (e.g. time_unix_nano from OTLP
    /// receiver). Used when the column is not a string type so `timestamp_col` cannot be set.
    pub(super) timestamp_num_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the level/severity column (e.g. "ERROR").
    pub(super) level_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the primary message/body column.
    pub(super) body_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the `trace_id` column (32 hex chars → 16-byte OTLP field 9).
    pub(super) trace_id_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the `span_id` column (16 hex chars → 8-byte OTLP field 10).
    pub(super) span_id_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Downcast array for the `flags` / `trace_flags` column (uint32, OTLP field 8).
    pub(super) flags_col: Option<(usize, FlagsArray<'a>)>,
    /// Severity number column (Int64, OTLP severity_number).
    pub(super) severity_num_col: Option<(usize, &'a PrimitiveArray<Int64Type>)>,
    /// Observed timestamp column (Int64, nanoseconds).
    pub(super) observed_ts_col: Option<(usize, &'a dyn Array)>,
    /// Scope name column.
    pub(super) scope_name_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Scope version column.
    pub(super) scope_version_col: Option<(usize, OtlpStrCol<'a>)>,
    /// Non-special attribute columns with pre-encoded key bytes.
    pub(super) attribute_cols: Vec<ColAttr<'a>>,
    /// Resource attribute columns promoted to OTLP Resource attributes.
    pub(super) resource_cols: Vec<(String, AttrArray<'a>)>,
}

pub(super) fn resolve_otlp_str_col(col: &dyn Array) -> Option<OtlpStrCol<'_>> {
    match col.data_type() {
        DataType::Utf8 => Some(OtlpStrCol::Utf8(col.as_string::<i32>())),
        DataType::Utf8View => Some(OtlpStrCol::Utf8View(col.as_string_view())),
        DataType::LargeUtf8 => Some(OtlpStrCol::LargeUtf8(col.as_string::<i64>())),
        _ => None,
    }
}

/// Scan the batch schema once and resolve column roles and downcast arrays.
pub(super) fn resolve_batch_columns<'a>(
    batch: &'a RecordBatch,
    message_field: &str,
) -> BatchColumns<'a> {
    let schema = batch.schema();
    let mut timestamp_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut timestamp_num_col: Option<(usize, &dyn Array)> = None;
    let mut level_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut body_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut trace_id_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut span_id_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut flags_col: Option<(usize, FlagsArray<'_>)> = None;
    // Column indices to exclude from attributes.
    let mut excluded = vec![false; schema.fields().len()];

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_name = field.name().as_str();
        if field_names::is_internal_column(col_name) {
            excluded[idx] = true;
            continue;
        }
        let field_name = col_name;
        match field_name {
            name if field_names::matches_any(
                name,
                field_names::TIMESTAMP,
                field_names::TIMESTAMP_VARIANTS,
            ) && timestamp_col.is_none()
                && timestamp_num_col.is_none() =>
            {
                if let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref()) {
                    timestamp_col = Some((idx, arr));
                    excluded[idx] = true;
                } else if matches!(
                    field.data_type(),
                    DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _)
                ) {
                    timestamp_num_col = Some((idx, batch.column(idx).as_ref()));
                    excluded[idx] = true;
                }
            }
            name if field_names::matches_any(
                name,
                field_names::SEVERITY,
                field_names::SEVERITY_VARIANTS,
            ) =>
            {
                if level_col.is_none()
                    && let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref())
                {
                    level_col = Some((idx, arr));
                    excluded[idx] = true;
                }
            }
            field_names::TRACE_ID => {
                if trace_id_col.is_none()
                    && let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref())
                {
                    trace_id_col = Some((idx, arr));
                    excluded[idx] = true;
                }
            }
            field_names::SPAN_ID => {
                if span_id_col.is_none()
                    && let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref())
                {
                    span_id_col = Some((idx, arr));
                    excluded[idx] = true;
                }
            }
            name if field_names::matches_any(
                name,
                field_names::TRACE_FLAGS,
                field_names::TRACE_FLAGS_VARIANTS,
            ) && flags_col.is_none() =>
            {
                let col = batch.column(idx);
                let resolved = match field.data_type() {
                    DataType::Int64 => Some(FlagsArray::Int64(col.as_primitive::<Int64Type>())),
                    DataType::UInt32 => Some(FlagsArray::UInt32(
                        col.as_primitive::<arrow::datatypes::UInt32Type>(),
                    )),
                    DataType::UInt64 => Some(FlagsArray::UInt64(col.as_primitive::<UInt64Type>())),
                    _ => None,
                };
                if let Some(flags_arr) = resolved {
                    flags_col = Some((idx, flags_arr));
                    excluded[idx] = true;
                }
            }
            name if name == message_field
                || field_names::matches_any(
                    name,
                    field_names::BODY,
                    field_names::BODY_VARIANTS,
                ) =>
            {
                if let Some(arr) = resolve_otlp_str_col(batch.column(idx).as_ref()) {
                    // Canonical "body" always wins; among aliases, first match wins.
                    if body_col.is_none() || name == field_names::BODY {
                        body_col = Some((idx, arr));
                    }
                }
            }
            _ => {}
        }
    }
    if let Some((idx, _)) = body_col {
        excluded[idx] = true;
    }

    // --- Second pass: new well-known columns and attribute/resource classification ---
    let mut severity_num_col: Option<(usize, &PrimitiveArray<Int64Type>)> = None;
    let mut observed_ts_col: Option<(usize, &dyn Array)> = None;
    let mut scope_name_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut scope_version_col: Option<(usize, OtlpStrCol<'_>)> = None;
    let mut attribute_cols: Vec<ColAttr<'_>> = Vec::new();
    let mut resource_cols: Vec<(String, AttrArray<'_>)> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if excluded[idx] {
            continue;
        }
        let field_name = field.name().as_str();

        // Severity number — use directly instead of deriving from text.
        if field_name == field_names::SEVERITY_NUMBER
            && severity_num_col.is_none()
            && matches!(field.data_type(), DataType::Int64)
        {
            severity_num_col = Some((idx, batch.column(idx).as_primitive::<Int64Type>()));
            continue;
        }

        // Observed timestamp.
        if field_name == field_names::OBSERVED_TIMESTAMP
            && observed_ts_col.is_none()
            && matches!(
                field.data_type(),
                DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _)
            )
        {
            observed_ts_col = Some((idx, batch.column(idx).as_ref()));
            continue;
        }

        // Scope metadata.
        if field_name == field_names::SCOPE_NAME && scope_name_col.is_none() {
            scope_name_col = resolve_otlp_str_col(batch.column(idx).as_ref()).map(|arr| (idx, arr));
            if scope_name_col.is_some() {
                continue;
            }
        }
        if field_name == field_names::SCOPE_VERSION && scope_version_col.is_none() {
            scope_version_col =
                resolve_otlp_str_col(batch.column(idx).as_ref()).map(|arr| (idx, arr));
            if scope_version_col.is_some() {
                continue;
            }
        }

        let stripped_resource_key = field_name.strip_prefix(field_names::DEFAULT_RESOURCE_PREFIX);
        if matches!(stripped_resource_key, Some("")) {
            tracing::warn!(
                column = field_name,
                resource_prefix = field_names::DEFAULT_RESOURCE_PREFIX,
                "dropping resource column with empty OTLP key after prefix stripping"
            );
            continue;
        }
        if let Some(resource_key) = stripped_resource_key.map(str::to_string) {
            let resource_attr = match field.data_type() {
                DataType::Int64 => AttrArray::Int(batch.column(idx).as_primitive::<Int64Type>()),
                DataType::UInt64 => AttrArray::UInt(batch.column(idx).as_primitive::<UInt64Type>()),
                DataType::Float64 => {
                    AttrArray::Float(batch.column(idx).as_primitive::<Float64Type>())
                }
                DataType::Boolean => AttrArray::Bool(batch.column(idx).as_boolean()),
                DataType::Binary => {
                    let Some(arr) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() else {
                        continue;
                    };
                    AttrArray::Bytes(arr)
                }
                DataType::LargeBinary => {
                    let Some(arr) = batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                    else {
                        continue;
                    };
                    AttrArray::LargeBytes(arr)
                }
                DataType::Struct(_) => {
                    tracing::warn!(
                        column = field_name,
                        data_type = ?field.data_type(),
                        "dropping struct resource column; OTLP Resource attributes only support scalar values"
                    );
                    continue;
                }
                _ => resolve_otlp_str_col(batch.column(idx).as_ref()).map_or_else(
                    || {
                        AttrArray::PreformattedStr(preformat_non_string_attr_column(
                            batch.column(idx).as_ref(),
                            field_name,
                        ))
                    },
                    AttrArray::Str,
                ),
            };
            resource_cols.push((resource_key, resource_attr));
            continue;
        }
        // Dispatch on the actual Arrow DataType, not the column name suffix.
        // A SQL transform may produce a column whose name suffix disagrees with
        // its real type (e.g. `SELECT level_str AS count_int`); using
        // `field.data_type()` avoids an `as_primitive` panic in that case.
        let attr = match field.data_type() {
            DataType::Int64 => AttrArray::Int(batch.column(idx).as_primitive::<Int64Type>()),
            DataType::UInt64 => AttrArray::UInt(batch.column(idx).as_primitive::<UInt64Type>()),
            DataType::Float64 => AttrArray::Float(batch.column(idx).as_primitive::<Float64Type>()),
            DataType::Boolean => AttrArray::Bool(batch.column(idx).as_boolean()),
            DataType::Binary => {
                let Some(arr) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() else {
                    continue;
                };
                AttrArray::Bytes(arr)
            }
            DataType::LargeBinary => {
                let Some(arr) = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                else {
                    continue;
                };
                AttrArray::LargeBytes(arr)
            }
            // Non-conflict struct columns (e.g. nested objects not produced by the
            // type-conflict builder) cannot be encoded as a single typed OTLP attribute.
            // Conflict structs (Struct { int, str, float, bool }) are already normalized
            // to flat Utf8 by `encode_batch` before this function is called.
            DataType::Struct(_) => {
                tracing::warn!(
                    column = field.name().as_str(),
                    "non-conflict struct column cannot be encoded as OTLP attribute, skipping"
                );
                continue;
            }
            _ => resolve_otlp_str_col(batch.column(idx).as_ref()).map_or_else(
                || AttrArray::OtherStr(batch.column(idx).as_ref()),
                AttrArray::Str,
            ),
        };
        let name = field_name.to_string();
        let mut key_encoding = Vec::with_capacity(2 + name.len());
        encode_bytes_field(&mut key_encoding, otlp::KEY_VALUE_KEY, name.as_bytes());
        let kv_key_cost = bytes_field_size(otlp::KEY_VALUE_KEY, name.len());
        let short_kv_inner_base = kv_key_cost + 4;
        let has_nulls = batch.column(idx).null_count() > 0;
        attribute_cols.push(ColAttr {
            name,
            key_encoding,
            kv_key_cost,
            short_kv_inner_base,
            has_nulls,
            array: attr,
        });
    }

    BatchColumns {
        timestamp_col,
        timestamp_num_col,
        level_col,
        body_col,
        trace_id_col,
        span_id_col,
        flags_col,
        severity_num_col,
        observed_ts_col,
        scope_name_col,
        scope_version_col,
        attribute_cols,
        resource_cols,
    }
}

pub(super) fn format_non_string_attr_value(
    arr: &dyn Array,
    row: usize,
    field_name: &str,
) -> Option<String> {
    if arr.is_null(row) {
        return None;
    }
    match array_value_to_string(arr, row) {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::warn!(
                column = field_name,
                row,
                error = %err,
                "skipping OTLP attribute value: failed to format non-string Arrow value"
            );
            None
        }
    }
}

pub(super) fn preformat_non_string_attr_column(
    arr: &dyn Array,
    field_name: &str,
) -> Vec<Option<String>> {
    (0..arr.len())
        .map(|row| format_non_string_attr_value(arr, row, field_name))
        .collect()
}

pub(super) fn numeric_timestamp_ns(arr: &dyn Array, row: usize) -> Option<u64> {
    if arr.is_null(row) {
        return None;
    }

    match arr.data_type() {
        DataType::Int64 => {
            let v = arr.as_primitive::<Int64Type>().value(row);
            match u64::try_from(v) {
                Ok(ns) => Some(ns),
                Err(_) => {
                    tracing::debug!(
                        "timestamp parse fallback: negative i64 timestamp cannot convert to u64"
                    );
                    None
                }
            }
        }
        DataType::UInt64 => Some(arr.as_primitive::<UInt64Type>().value(row)),
        DataType::Timestamp(unit, _) => {
            use arrow::datatypes::{
                TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
                TimestampNanosecondType, TimestampSecondType,
            };
            let raw_ns = match unit {
                TimeUnit::Nanosecond => {
                    Some(arr.as_primitive::<TimestampNanosecondType>().value(row))
                }
                TimeUnit::Microsecond => arr
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row)
                    .checked_mul(1_000),
                TimeUnit::Millisecond => arr
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row)
                    .checked_mul(1_000_000),
                TimeUnit::Second => arr
                    .as_primitive::<TimestampSecondType>()
                    .value(row)
                    .checked_mul(1_000_000_000),
            };
            match raw_ns.and_then(|ns| u64::try_from(ns).ok()) {
                Some(ns) => Some(ns),
                None => {
                    tracing::debug!(
                        "timestamp parse fallback: numeric timestamp overflow or negative value"
                    );
                    None
                }
            }
        }
        _ => None,
    }
}
