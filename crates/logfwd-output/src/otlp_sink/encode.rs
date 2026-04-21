//! OTLP protobuf encoding: batch-level and row-level encoding logic.

use std::io;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use arrow::array::Array;
use logfwd_arrow::conflict_schema::normalize_conflict_columns;
use logfwd_core::otlp::{
    self, Severity, bytes_field_size, encode_bytes_field, encode_fixed64, encode_varint_field,
    hex_decode, parse_severity, parse_timestamp_nanos, varint_len,
};
// These are also re-exported as `pub(super)` so the generated encoder
// (included via `mod generated { include!(...) }`) can reach them through `super::`.
pub(super) use logfwd_core::otlp::{encode_fixed32, encode_tag, encode_varint};

use super::columns::{
    AttrArray, BatchColumns, ColAttr, format_non_string_attr_value, numeric_timestamp_ns,
    resolve_batch_columns,
};
use super::types::{OtlpSink, ResourceValueRef, SCOPE_NAME, SCOPE_VERSION};
use crate::BatchMetadata;

mod generated {
    include!("../generated/otlp_log_record_fast_v1.rs");
}

impl OtlpSink {
    /// Encode a full ExportLogsServiceRequest from a RecordBatch.
    /// Returns the raw protobuf bytes in `self.encoder_buf`.
    pub fn encode_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) {
        self.encode_batch_with_row_encoder(batch, metadata, encode_row_as_log_record);
    }

    /// Benchmark/reference path: encode using the generated fast-row encoder.
    pub fn encode_batch_generated_fast(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) {
        self.encode_batch_with_row_encoder(
            batch,
            metadata,
            generated::encode_row_as_log_record_fast_v1,
        );
    }

    /// Benchmark/reference path: encode only the concatenated LogRecord payloads using the
    /// handwritten row encoder.
    pub fn encode_rows_only_for_bench(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) {
        self.encode_rows_only_with_row_encoder(batch, metadata, encode_row_as_log_record);
    }

    /// Benchmark/reference path: encode only the concatenated LogRecord payloads using the
    /// generated fast row encoder.
    pub fn encode_rows_only_generated_fast_for_bench(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) {
        self.encode_rows_only_with_row_encoder(
            batch,
            metadata,
            generated::encode_row_as_log_record_fast_v1,
        );
    }

    fn encode_batch_with_row_encoder(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        mut encode_row: impl FnMut(&BatchColumns<'_>, usize, &BatchMetadata, &mut Vec<u8>),
    ) {
        self.encoder_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        // Normalize any conflict struct columns (e.g. `status: Struct { int, str }`)
        // to flat Utf8 columns before encoding. Without this, struct columns would be
        // silently dropped, causing data loss when no SQL transform is applied upstream.
        let normalized;
        let batch = if batch
            .schema()
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Struct(_)))
        {
            normalized = normalize_conflict_columns(batch.clone());
            &normalized
        } else {
            batch
        };

        // Resolve column roles and downcast arrays once for the whole batch.
        let columns = resolve_batch_columns(batch, self.message_field.as_str());

        // Phase 1: encode all LogRecords and assign each row to a resource group.
        let mut records_buf: Vec<u8> =
            Vec::with_capacity(estimate_records_buf_capacity(num_rows, &columns));
        let mut grouped_ranges: Vec<super::types::ResourceGroup<'_>> = Vec::new();

        // Fast path: when there are no resource or scope columns every row belongs to
        // the same single group.  This is the overwhelmingly common case for file / CRI
        // log forwarding and avoids a per-row HashMap lookup, key Vec allocation, and
        // scope-column null check.
        if columns.resource_cols.is_empty()
            && columns.scope_name_col.is_none()
            && columns.scope_version_col.is_none()
        {
            let mut record_ranges: Vec<(usize, usize)> = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let start = records_buf.len();
                encode_row(&columns, row, metadata, &mut records_buf);
                record_ranges.push((start, records_buf.len()));
            }
            grouped_ranges.push((Vec::new(), (None, None), record_ranges));
        } else {
            let mut group_index_by_key: std::collections::HashMap<
                (super::types::ResourceKey<'_>, super::types::ScopeKey<'_>),
                usize,
            > = std::collections::HashMap::new();

            for row in 0..num_rows {
                let mut key: super::types::ResourceKey<'_> =
                    Vec::with_capacity(columns.resource_cols.len());
                for (field_name, attr) in &columns.resource_cols {
                    key.push(attr.value_ref(row, field_name));
                }
                let scope_name = columns.scope_name_col.as_ref().and_then(|(_, arr)| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                });
                let scope_version = columns.scope_version_col.as_ref().and_then(|(_, arr)| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                });
                let scope_key = (scope_name, scope_version);

                let group_idx = match group_index_by_key.entry((key, scope_key)) {
                    std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let idx = grouped_ranges.len();
                        let (group_key, group_scope) = entry.key();
                        grouped_ranges.push((group_key.clone(), *group_scope, Vec::new()));
                        entry.insert(idx);
                        idx
                    }
                };

                let start = records_buf.len();
                encode_row(&columns, row, metadata, &mut records_buf);
                grouped_ranges[group_idx].2.push((start, records_buf.len()));
            }
        }

        // Phase 2: compute sizes bottom-up per resource group.
        let mut grouped_resource_msgs: Vec<Vec<u8>> = Vec::with_capacity(grouped_ranges.len());
        let mut grouped_resource_inner_sizes: Vec<usize> = Vec::with_capacity(grouped_ranges.len());
        let mut grouped_scope_inner_sizes: Vec<usize> = Vec::with_capacity(grouped_ranges.len());
        let mut grouped_scope_values: Vec<(Vec<u8>, Vec<u8>)> =
            Vec::with_capacity(grouped_ranges.len());
        let mut request_size = 0usize;

        for (key, scope_key, record_ranges) in &grouped_ranges {
            let scope_name = scope_key
                .0
                .unwrap_or_else(|| std::str::from_utf8(SCOPE_NAME).unwrap_or("logfwd"));
            let scope_version = scope_key
                .1
                .unwrap_or_else(|| std::str::from_utf8(SCOPE_VERSION).unwrap_or(""));
            let scope_name_bytes = scope_name.as_bytes().to_vec();
            let scope_version_bytes = scope_version.as_bytes().to_vec();
            let instrumentation_scope_inner_size =
                bytes_field_size(otlp::INSTRUMENTATION_SCOPE_NAME, scope_name_bytes.len())
                    + bytes_field_size(
                        otlp::INSTRUMENTATION_SCOPE_VERSION,
                        scope_version_bytes.len(),
                    );
            let mut scope_logs_inner_size =
                bytes_field_size(otlp::SCOPE_LOGS_SCOPE, instrumentation_scope_inner_size);
            for &(start, end) in record_ranges {
                scope_logs_inner_size +=
                    bytes_field_size(otlp::SCOPE_LOGS_LOG_RECORDS, end - start);
            }

            let mut resource_msg: Vec<u8> = Vec::new();
            for (k, v) in metadata.resource_attrs.as_ref() {
                encode_key_value_string(
                    &mut resource_msg,
                    otlp::RESOURCE_ATTRIBUTES,
                    k.as_bytes(),
                    v.as_bytes(),
                );
            }
            for ((key_name, _), value) in columns.resource_cols.iter().zip(key.iter()) {
                if let Some(v) = value {
                    match v {
                        ResourceValueRef::Str(v) => encode_key_value_string(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            v.as_bytes(),
                        ),
                        ResourceValueRef::Bytes(v) => encode_key_value_bytes(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            v,
                        ),
                        ResourceValueRef::Int(v) => encode_key_value_int(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            *v,
                        ),
                        ResourceValueRef::Float(v) => encode_key_value_double(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            f64::from_bits(*v),
                        ),
                        ResourceValueRef::Bool(v) => encode_key_value_bool(
                            &mut resource_msg,
                            otlp::RESOURCE_ATTRIBUTES,
                            key_name.as_bytes(),
                            *v,
                        ),
                    }
                }
            }

            let mut resource_inner_size =
                bytes_field_size(otlp::RESOURCE_LOGS_SCOPE_LOGS, scope_logs_inner_size);
            if !resource_msg.is_empty() {
                resource_inner_size +=
                    bytes_field_size(otlp::RESOURCE_LOGS_RESOURCE, resource_msg.len());
            }
            request_size +=
                bytes_field_size(otlp::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, resource_inner_size);

            grouped_resource_msgs.push(resource_msg);
            grouped_resource_inner_sizes.push(resource_inner_size);
            grouped_scope_inner_sizes.push(scope_logs_inner_size);
            grouped_scope_values.push((scope_name_bytes, scope_version_bytes));
        }

        // Phase 3: write final protobuf with one ResourceLogs per group.
        self.encoder_buf.reserve(request_size + 16);
        for (group_idx, (_key, _scope_key, record_ranges)) in grouped_ranges.iter().enumerate() {
            let (scope_name, scope_version) = &grouped_scope_values[group_idx];
            let instrumentation_scope_inner_size =
                bytes_field_size(otlp::INSTRUMENTATION_SCOPE_NAME, scope_name.len())
                    + bytes_field_size(otlp::INSTRUMENTATION_SCOPE_VERSION, scope_version.len());
            encode_tag(
                &mut self.encoder_buf,
                otlp::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
                otlp::WIRE_TYPE_LEN,
            );
            encode_varint(
                &mut self.encoder_buf,
                grouped_resource_inner_sizes[group_idx] as u64,
            );

            let resource_msg = &grouped_resource_msgs[group_idx];
            if !resource_msg.is_empty() {
                encode_bytes_field(
                    &mut self.encoder_buf,
                    otlp::RESOURCE_LOGS_RESOURCE,
                    resource_msg,
                );
            }

            encode_tag(
                &mut self.encoder_buf,
                otlp::RESOURCE_LOGS_SCOPE_LOGS,
                otlp::WIRE_TYPE_LEN,
            );
            encode_varint(
                &mut self.encoder_buf,
                grouped_scope_inner_sizes[group_idx] as u64,
            );

            encode_tag(
                &mut self.encoder_buf,
                otlp::SCOPE_LOGS_SCOPE,
                otlp::WIRE_TYPE_LEN,
            );
            encode_varint(
                &mut self.encoder_buf,
                instrumentation_scope_inner_size as u64,
            );
            encode_bytes_field(
                &mut self.encoder_buf,
                otlp::INSTRUMENTATION_SCOPE_NAME,
                scope_name,
            );
            encode_bytes_field(
                &mut self.encoder_buf,
                otlp::INSTRUMENTATION_SCOPE_VERSION,
                scope_version,
            );

            for &(start, end) in record_ranges {
                encode_bytes_field(
                    &mut self.encoder_buf,
                    otlp::SCOPE_LOGS_LOG_RECORDS,
                    &records_buf[start..end],
                );
            }
        }
    }

    fn encode_rows_only_with_row_encoder(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        mut encode_row: impl FnMut(&BatchColumns<'_>, usize, &BatchMetadata, &mut Vec<u8>),
    ) {
        self.encoder_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        let normalized;
        let batch = if batch
            .schema()
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Struct(_)))
        {
            normalized = normalize_conflict_columns(batch.clone());
            &normalized
        } else {
            batch
        };

        let columns = resolve_batch_columns(batch, self.message_field.as_str());
        self.encoder_buf.reserve(num_rows * 128);

        for row in 0..num_rows {
            encode_row(&columns, row, metadata, &mut self.encoder_buf);
        }
    }
}

pub(super) fn estimate_records_buf_capacity(num_rows: usize, columns: &BatchColumns<'_>) -> usize {
    const MIN_RECORD_BYTES: usize = 128;
    // 64 bytes/attr covers typical string attribute values (~30 bytes) plus
    // protobuf framing overhead (~27 bytes key+tag+varint) with some margin.
    // Wide batches with 27 attrs need ~14.5 MB for 10K rows; at 48 bytes/attr
    // the estimate was 13.9 MB — slightly short, causing one realloc per batch.
    const ATTR_BYTES_HINT: usize = 64;
    const MAX_INITIAL_RECORDS_BUF: usize = 64 * 1024 * 1024;

    let hinted_attrs = columns.attribute_cols.len().min(64);
    let bytes_per_row = MIN_RECORD_BYTES.saturating_add(hinted_attrs * ATTR_BYTES_HINT);
    num_rows
        .saturating_mul(bytes_per_row)
        .min(MAX_INITIAL_RECORDS_BUF)
}

/// Encode a single RecordBatch row as an OTLP LogRecord using pre-resolved columns.
pub(super) fn encode_row_as_log_record(
    columns: &BatchColumns<'_>,
    row: usize,
    metadata: &BatchMetadata,
    buf: &mut Vec<u8>,
) {
    // --- Read per-row values from pre-resolved columns ---

    let timestamp_ns: Option<u64> = if let Some((_, arr)) = columns.timestamp_num_col.as_ref() {
        numeric_timestamp_ns(*arr, row)
    } else if let Some((_, arr)) = columns.timestamp_col.as_ref() {
        if arr.is_null(row) {
            None
        } else {
            match parse_timestamp_nanos(arr.value(row).as_bytes()) {
                Some(ts) => Some(ts),
                None => {
                    tracing::debug!(
                        "timestamp parse fallback: event_time omitted for unparsable value"
                    );
                    None
                }
            }
        }
    } else {
        None
    };

    let (severity_num, severity_text): (Severity, &[u8]) =
        if let Some((_, arr)) = columns.level_col.as_ref() {
            if arr.is_null(row) {
                (Severity::Unspecified, b"")
            } else {
                parse_severity(arr.value(row).as_bytes())
            }
        } else {
            (Severity::Unspecified, b"")
        };
    let severity_number = if let Some((_, arr)) = columns.severity_num_col.as_ref() {
        if arr.is_null(row) {
            severity_num as u64
        } else {
            u64::try_from(arr.value(row)).unwrap_or(0)
        }
    } else {
        severity_num as u64
    };

    let body: &str = match columns.body_col.as_ref() {
        Some((_, body)) if !body.is_null(row) => body.value(row),
        _ => "",
    };
    let body_bytes = body.as_bytes();

    // --- Write protobuf fields ---

    // LogRecord.time_unix_nano (fixed64)
    if let Some(timestamp_ns) = timestamp_ns {
        encode_fixed64(buf, otlp::LOG_RECORD_TIME_UNIX_NANO, timestamp_ns);
    }

    // LogRecord.severity_number (varint)
    if severity_number > 0 {
        encode_varint_field(buf, otlp::LOG_RECORD_SEVERITY_NUMBER, severity_number);
    }

    // LogRecord.severity_text (string)
    if !severity_text.is_empty() {
        encode_bytes_field(buf, otlp::LOG_RECORD_SEVERITY_TEXT, severity_text);
    }

    // LogRecord.body (AnyValue { string_value })
    if !body_bytes.is_empty() {
        let anyvalue_inner_size = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, body_bytes.len());
        encode_tag(buf, otlp::LOG_RECORD_BODY, otlp::WIRE_TYPE_LEN);
        encode_varint(buf, anyvalue_inner_size as u64);
        encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, body_bytes);
    }

    // LogRecord.attributes — pre-resolved attribute columns
    for col in &columns.attribute_cols {
        encode_col_attr(buf, otlp::LOG_RECORD_ATTRIBUTES, col, row);
    }

    // LogRecord.flags (fixed32) — W3C trace flags.
    // Filter to u32 range: negative or >u32::MAX values are invalid per the
    // W3C Trace Context spec (only 8 bits are defined). (#1121)
    if let Some((_, arr)) = columns.flags_col
        && let Some(flags) = arr.value_u32(row)
    {
        encode_fixed32(buf, otlp::LOG_RECORD_FLAGS, flags);
    }

    // LogRecord.trace_id (bytes, 16 bytes) — hex-decoded from 32-char string column
    if let Some((_, arr)) = columns.trace_id_col
        && !arr.is_null(row)
    {
        let hex = arr.value(row);
        let mut decoded = [0u8; 16];
        if hex_decode(hex.as_bytes(), &mut decoded) {
            encode_bytes_field(buf, otlp::LOG_RECORD_TRACE_ID, &decoded);
        }
    }

    // LogRecord.span_id (bytes, 8 bytes) — hex-decoded from 16-char string column
    if let Some((_, arr)) = columns.span_id_col
        && !arr.is_null(row)
    {
        let hex = arr.value(row);
        let mut decoded = [0u8; 8];
        if hex_decode(hex.as_bytes(), &mut decoded) {
            encode_bytes_field(buf, otlp::LOG_RECORD_SPAN_ID, &decoded);
        }
    }

    // LogRecord.observed_time_unix_nano (fixed64)
    let observed_ns = columns
        .observed_ts_col
        .as_ref()
        .and_then(|(_, arr)| numeric_timestamp_ns(*arr, row))
        .unwrap_or(metadata.observed_time_ns);
    encode_fixed64(buf, otlp::LOG_RECORD_OBSERVED_TIME_UNIX_NANO, observed_ns);
}

/// Encode a KeyValue with string AnyValue as an attribute.
/// `field_number` is the protobuf field tag of the parent message's `attributes` repeated field
/// (e.g. `LOG_RECORD_ATTRIBUTES` for LogRecord, `RESOURCE_ATTRIBUTES` for Resource).
/// KeyValue: { key (string), value (AnyValue { string_value }) }
pub(super) fn encode_key_value_string(
    buf: &mut Vec<u8>,
    field_number: u32,
    key: &[u8],
    value: &[u8],
) {
    let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, value.len());
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, value);
}

/// Encode a KeyValue with bytes AnyValue (`AnyValue.bytes_value`).
pub(super) fn encode_key_value_bytes(
    buf: &mut Vec<u8>,
    field_number: u32,
    key: &[u8],
    value: &[u8],
) {
    let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_BYTES_VALUE, value.len());
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_bytes_field(buf, otlp::ANY_VALUE_BYTES_VALUE, value);
}

/// Encode a KeyValue with int AnyValue (`AnyValue.int_value`).
pub(super) fn encode_key_value_int(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: i64) {
    let anyvalue_inner = 1 + varint_len(value as u64); // tag(1 byte) + varint
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_varint_field(buf, otlp::ANY_VALUE_INT_VALUE, value as u64);
}

/// Encode one attribute column into `buf` using pre-computed key encoding.
///
/// Avoids recomputing the
/// key tag + varint + key bytes on every row by using `col.key_encoding` and
/// `col.kv_key_cost` which were computed once in `resolve_batch_columns`.
#[inline(always)]
pub(super) fn encode_col_attr(buf: &mut Vec<u8>, field_number: u32, col: &ColAttr<'_>, row: usize) {
    // Shared helper for string-value KV pairs.
    //
    // When `value.len() <= 125` every intermediate length fits in a single
    // varint byte, so the outer KV byte-count reduces to two additions instead
    // of four `varint_len`/`bytes_field_size` calls.
    #[inline(always)]
    fn encode_str_value(buf: &mut Vec<u8>, field_number: u32, col: &ColAttr<'_>, value: &[u8]) {
        if value.len() <= 125 {
            // anyvalue_inner = tag(1) + len_varint(1) + data
            let anyvalue_inner = 2 + value.len();
            let kv_inner = col.short_kv_inner_base + value.len();
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, value);
        } else {
            let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, value.len());
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, value);
        }
    }

    match &col.array {
        AttrArray::Int(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = 1 + varint_len(value as u64);
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_varint_field(buf, otlp::ANY_VALUE_INT_VALUE, value as u64);
        }
        AttrArray::UInt(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let Ok(value) = i64::try_from(value) else {
                tracing::warn!(
                    column = col.name.as_str(),
                    row,
                    value,
                    "skipping OTLP attribute: UInt64 value exceeds AnyValue.int_value range"
                );
                return;
            };
            let anyvalue_inner = 1 + varint_len(value as u64);
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_varint_field(buf, otlp::ANY_VALUE_INT_VALUE, value as u64);
        }
        AttrArray::Float(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = 1 + 8; // tag(1) + fixed64(8)
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_fixed64(buf, otlp::ANY_VALUE_DOUBLE_VALUE, value.to_bits());
        }
        AttrArray::Bool(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = 1 + 1; // tag(1) + varint(bool)
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_varint_field(buf, otlp::ANY_VALUE_BOOL_VALUE, u64::from(value));
        }
        AttrArray::Str(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            encode_str_value(buf, field_number, col, arr.value(row).as_bytes());
        }
        AttrArray::PreformattedStr(values) => {
            if let Some(Some(value)) = values.get(row) {
                encode_str_value(buf, field_number, col, value.as_bytes());
            }
        }
        AttrArray::Bytes(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_BYTES_VALUE, value.len());
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_BYTES_VALUE, value);
        }
        AttrArray::LargeBytes(arr) => {
            if col.has_nulls && arr.is_null(row) {
                return;
            }
            let value = arr.value(row);
            let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_BYTES_VALUE, value.len());
            let kv_inner =
                col.kv_key_cost + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
            encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, kv_inner as u64);
            buf.extend_from_slice(&col.key_encoding);
            encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
            encode_varint(buf, anyvalue_inner as u64);
            encode_bytes_field(buf, otlp::ANY_VALUE_BYTES_VALUE, value);
        }
        AttrArray::OtherStr(arr) => {
            if let Some(value) = format_non_string_attr_value(*arr, row, &col.name) {
                encode_str_value(buf, field_number, col, value.as_bytes());
            }
        }
    }
}

/// Encode a KeyValue with double AnyValue (`AnyValue.double_value`).
pub(super) fn encode_key_value_double(
    buf: &mut Vec<u8>,
    field_number: u32,
    key: &[u8],
    value: f64,
) {
    let anyvalue_inner = 1 + 8; // tag(1 byte) + fixed64
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_fixed64(buf, otlp::ANY_VALUE_DOUBLE_VALUE, value.to_bits());
}

/// Encode a KeyValue with boolean AnyValue (`AnyValue.bool_value`).
pub(super) fn encode_key_value_bool(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: bool) {
    let anyvalue_inner = 1 + 1; // tag(1 byte) + varint(1 byte)
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_varint_field(buf, otlp::ANY_VALUE_BOOL_VALUE, u64::from(value));
}

/// Write a gRPC length-prefixed message frame into `buf`.
///
/// gRPC wire format (per the [gRPC over HTTP/2 specification](https://grpc.io/docs/what-is-grpc/core-concepts/)):
/// ```text
/// [1 byte: compressed flag (0 = not compressed, 1 = compressed)]
/// [4 bytes: big-endian message length]
/// [N bytes: protobuf message]
/// ```
pub(super) fn write_grpc_frame(
    buf: &mut Vec<u8>,
    payload: &[u8],
    compressed: bool,
) -> io::Result<()> {
    let len = u32::try_from(payload.len()).map_err(|_e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "gRPC message payload must be < 4 GiB",
        )
    })?;
    buf.clear();
    buf.push(u8::from(compressed));
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
    Ok(())
}
