use std::borrow::Cow;
use std::io;
use std::io::Read as _;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use ffwd_arrow::Scanner;
use ffwd_core::scan_config::ScanConfig;
use ffwd_types::field_names;
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;

use crate::InputError;

#[cfg(any(feature = "otlp-research", test))]
use super::OtlpProtobufDecodeMode;
use super::convert::{
    convert_request_to_batch, decode_protojson_bytes, hex, parse_protojson_f64,
    parse_protojson_i64, parse_protojson_u64, write_f64_to_buf, write_i64_to_buf, write_json_key,
    write_json_string_field, write_u64_to_buf,
};
#[cfg(any(feature = "otlp-research", test))]
use super::projection::ProjectionError;

pub(super) fn decompress_zstd(body: &[u8], max_body_size: usize) -> Result<Vec<u8>, InputError> {
    let decoder = zstd::Decoder::new(body)
        .map_err(|_e| InputError::Receiver("zstd decompression failed".to_string()))?;
    read_decompressed_body(
        decoder,
        body.len(),
        max_body_size,
        "zstd decompression failed",
    )
}

pub(super) fn decompress_gzip(body: &[u8], max_body_size: usize) -> Result<Vec<u8>, InputError> {
    let decoder = GzDecoder::new(body);
    read_decompressed_body(
        decoder,
        body.len(),
        max_body_size,
        "gzip decompression failed",
    )
}

pub(super) fn read_decompressed_body(
    reader: impl io::Read,
    compressed_len: usize,
    max_body_size: usize,
    error_label: &str,
) -> Result<Vec<u8>, InputError> {
    let mut decompressed = Vec::with_capacity(compressed_len.min(max_body_size));
    match reader
        .take(max_body_size as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > max_body_size => Err(InputError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "payload too large",
        ))),
        Ok(_) => Ok(decompressed),
        Err(_) => Err(InputError::Receiver(error_label.to_string())),
    }
}

/// Decode an OTLP ExportLogsServiceRequest from protobuf and produce a
/// structured Arrow RecordBatch.
pub(super) fn decode_otlp_protobuf(
    body: &[u8],
    resource_prefix: &str,
) -> Result<RecordBatch, InputError> {
    decode_otlp_protobuf_with_prost(body, resource_prefix)
}

#[cfg(any(feature = "otlp-research", test))]
pub(super) fn decode_otlp_protobuf_bytes_with_mode(
    body: Bytes,
    resource_prefix: &str,
    mode: OtlpProtobufDecodeMode,
) -> Result<RecordBatch, InputError> {
    if body.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }

    match mode {
        OtlpProtobufDecodeMode::Prost => decode_otlp_protobuf_with_prost(&body, resource_prefix),
        #[cfg(any(feature = "otlp-research", test))]
        OtlpProtobufDecodeMode::ProjectedFallback => {
            match super::projection::classify_projected_fallback_support(&body) {
                Ok(()) => {}
                Err(ProjectionError::Unsupported(_)) => {
                    return decode_otlp_protobuf_with_prost(&body, resource_prefix);
                }
                Err(err) => return Err(InputError::Receiver(err.to_string())),
            }
            match super::projection::decode_projected_otlp_logs_view_bytes(
                body.clone(),
                resource_prefix,
            ) {
                Ok(batch) => Ok(batch),
                Err(ProjectionError::Unsupported(_)) => {
                    decode_otlp_protobuf_with_prost(&body, resource_prefix)
                }
                Err(err) => Err(InputError::Receiver(err.to_string())),
            }
        }
        #[cfg(any(feature = "otlp-research", test))]
        OtlpProtobufDecodeMode::ProjectedOnly => {
            super::projection::decode_projected_otlp_logs_view_bytes(body, resource_prefix)
                .map_err(|err| InputError::Receiver(err.to_string()))
        }
    }
}

pub(super) fn decode_otlp_protobuf_with_prost(
    body: &[u8],
    resource_prefix: &str,
) -> Result<RecordBatch, InputError> {
    if body.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }

    let request = ExportLogsServiceRequest::decode(body)
        .map_err(|e| InputError::Receiver(format!("invalid protobuf: {e}")))?;

    convert_request_to_batch(&request, resource_prefix)
}

/// Decode OTLP protobuf bytes into a structured Arrow RecordBatch using the
/// default resource attribute prefix.
pub(super) fn decode_otlp_logs_to_batch(body: &[u8]) -> Result<RecordBatch, InputError> {
    decode_otlp_protobuf(body, field_names::DEFAULT_RESOURCE_PREFIX)
}

/// Decode an OTLP ExportLogsServiceRequest from JSON and produce a structured
/// Arrow RecordBatch. Parses the OTLP JSON structure directly, converts to
/// newline-delimited JSON lines, then scans into a batch.
pub(super) fn decode_otlp_json(
    body: &[u8],
    resource_prefix: &str,
) -> Result<RecordBatch, InputError> {
    let json_lines = decode_otlp_logs_json(body, resource_prefix)?;
    if json_lines.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }
    let mut scanner = Scanner::new(ScanConfig::default());
    scanner
        .scan(Bytes::from(json_lines))
        .map_err(|e| InputError::Receiver(format!("structured OTLP JSON scan error: {e}")))
}

/// Parse an OTLP JSON ExportLogsServiceRequest and produce newline-delimited
/// JSON lines. Used internally by [`decode_otlp_json`] to feed the scanner.
fn decode_otlp_logs_json(body: &[u8], resource_prefix: &str) -> Result<Vec<u8>, InputError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let root: serde_json::Value = sonic_rs::from_slice(body)
        .map_err(|e| InputError::Receiver(format!("invalid JSON: {e}")))?;

    let Some(resource_logs) = root.get("resourceLogs").and_then(|v| v.as_array()) else {
        return Err(InputError::Receiver(
            "missing required field 'resourceLogs' in OTLP JSON payload".to_string(),
        ));
    };

    let mut out = Vec::new();
    // Scratch buffer reused across resource attribute keys to avoid one
    // `format!()` heap allocation per attribute.
    let mut key_buf = String::with_capacity(128);

    for rl in resource_logs {
        // Collect resource attributes. Keys are built by concatenating
        // `resource_prefix` + original key into the reusable `key_buf`,
        // then cloned into the vec. This avoids `format!()` macro overhead
        // (argument parsing, Display trait dispatch) while still producing
        // the required owned Strings for the inner loop.
        let mut resource_attrs: Vec<(String, &serde_json::Value)> = Vec::new();
        if let Some(attrs) = rl
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|a| a.as_array())
        {
            for kv in attrs {
                let Some(key) = kv.get("key").and_then(|k| k.as_str()) else {
                    continue;
                };
                let Some(value) = kv.get("value") else {
                    continue;
                };
                key_buf.clear();
                key_buf.push_str(resource_prefix);
                key_buf.push_str(key);
                resource_attrs.push((key_buf.clone(), value));
            }
        }

        let scope_logs = match rl.get("scopeLogs").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => continue,
        };

        for sl in scope_logs {
            let scope_name = sl
                .get("scope")
                .and_then(|scope| scope.get("name"))
                .and_then(|name| name.as_str());
            let scope_version = sl
                .get("scope")
                .and_then(|scope| scope.get("version"))
                .and_then(|version| version.as_str());

            let records = match sl.get("logRecords").and_then(|v| v.as_array()) {
                Some(arr) => arr,
                None => continue,
            };

            for record in records {
                out.push(b'{');

                // timestamp: prefer timeUnixNano, fall back to observedTimeUnixNano
                // when event time is unknown (fixes #1690).
                let mut ts_val = 0u64;
                if let Some(ts) = record.get("timeUnixNano") {
                    ts_val = parse_protojson_u64(ts).ok_or_else(|| {
                        InputError::Receiver(
                            "invalid OTLP JSON timeUnixNano: not a valid uint64".into(),
                        )
                    })?;
                }
                if ts_val == 0
                    && let Some(obs) = record.get("observedTimeUnixNano")
                {
                    ts_val = parse_protojson_u64(obs).ok_or_else(|| {
                        InputError::Receiver(
                            "invalid OTLP JSON observedTimeUnixNano: not a valid uint64".into(),
                        )
                    })?;
                }
                if ts_val > 0 {
                    write_json_key(&mut out, field_names::TIMESTAMP);
                    write_u64_to_buf(&mut out, ts_val);
                    out.push(b',');
                }

                if let Some(obs) = record.get("observedTimeUnixNano") {
                    let obs_val = parse_protojson_u64(obs).ok_or_else(|| {
                        InputError::Receiver(
                            "invalid OTLP JSON observedTimeUnixNano: not a valid uint64".into(),
                        )
                    })?;
                    if obs_val > 0 {
                        write_json_key(&mut out, field_names::OBSERVED_TIMESTAMP);
                        write_u64_to_buf(&mut out, obs_val);
                        out.push(b',');
                    }
                }

                if let Some(sev) = record.get("severityText").and_then(|v| v.as_str())
                    && !sev.is_empty()
                {
                    write_json_string_field(&mut out, field_names::SEVERITY, sev);
                    out.push(b',');
                }

                if let Some(sev_num) = record.get("severityNumber") {
                    let severity_number = parse_protojson_i64(sev_num).ok_or_else(|| {
                        InputError::Receiver(
                            "invalid OTLP JSON severityNumber: not a valid int64".into(),
                        )
                    })?;
                    if severity_number > 0 {
                        write_json_key(&mut out, field_names::SEVERITY_NUMBER);
                        write_i64_to_buf(&mut out, severity_number);
                        out.push(b',');
                    }
                }

                if let Some(body_val) = record.get("body")
                    && let Some(body_str) = json_any_value_to_string(body_val)?
                {
                    write_json_string_field(&mut out, field_names::BODY, &body_str);
                    out.push(b',');
                }

                for (key, value) in &resource_attrs {
                    if write_json_any_value_field_from_json(&mut out, key, value)? {
                        out.push(b',');
                    }
                }

                // Write protocol fields BEFORE log record attributes so that
                // first-write-wins semantics prevent attributes from shadowing
                // trace_id, span_id, flags, scope.name, or scope.version.
                if let Some(tid) = record.get("traceId").and_then(|v| v.as_str())
                    && !tid.is_empty()
                {
                    let normalized_trace_id = normalize_otlp_hex_id(tid, 32, "traceId")?;
                    write_json_string_field(&mut out, field_names::TRACE_ID, &normalized_trace_id);
                    out.push(b',');
                }
                if let Some(sid) = record.get("spanId").and_then(|v| v.as_str())
                    && !sid.is_empty()
                {
                    let normalized_span_id = normalize_otlp_hex_id(sid, 16, "spanId")?;
                    write_json_string_field(&mut out, field_names::SPAN_ID, &normalized_span_id);
                    out.push(b',');
                }

                if let Some(flags) = record.get("flags") {
                    let parsed_flags = parse_protojson_i64(flags).ok_or_else(|| {
                        InputError::Receiver("invalid OTLP JSON flags: not a valid int64".into())
                    })?;
                    if parsed_flags < 0 || parsed_flags > i64::from(u32::MAX) {
                        return Err(InputError::Receiver(
                            "invalid OTLP JSON flags: must be a uint32".into(),
                        ));
                    }
                    if parsed_flags > 0 {
                        write_json_key(&mut out, field_names::FLAGS);
                        write_i64_to_buf(&mut out, parsed_flags);
                        out.push(b',');
                    }
                }

                if let Some(name) = scope_name
                    && !name.is_empty()
                {
                    write_json_string_field(&mut out, field_names::SCOPE_NAME, name);
                    out.push(b',');
                }
                if let Some(version) = scope_version
                    && !version.is_empty()
                {
                    write_json_string_field(&mut out, field_names::SCOPE_VERSION, version);
                    out.push(b',');
                }

                if let Some(attrs) = record.get("attributes").and_then(|v| v.as_array()) {
                    for kv in attrs {
                        if let (Some(key), Some(val)) =
                            (kv.get("key").and_then(|k| k.as_str()), kv.get("value"))
                            && write_json_any_value_field_from_json(&mut out, key, val)?
                        {
                            out.push(b',');
                        }
                    }
                }

                if out.last() == Some(&b',') {
                    out.pop();
                }
                out.extend_from_slice(b"}\n");
            }
        }
    }

    Ok(out)
}

fn normalize_otlp_hex_id<'a>(
    raw: &'a str,
    expected_len: usize,
    field_name: &str,
) -> Result<Cow<'a, str>, InputError> {
    if raw.len() != expected_len {
        return Err(InputError::Receiver(format!(
            "invalid OTLP JSON {field_name}: expected {expected_len} hex chars"
        )));
    }
    if !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(InputError::Receiver(format!(
            "invalid OTLP JSON {field_name}: contains non-hex characters"
        )));
    }
    if raw.bytes().all(|b| !b.is_ascii_uppercase()) {
        Ok(Cow::Borrowed(raw))
    } else {
        Ok(Cow::Owned(raw.to_ascii_lowercase()))
    }
}

/// Extract a scalar OTLP JSON AnyValue as an owned string.
/// Returns `Ok(None)` when the value is not a supported scalar string representation.
fn json_any_value_to_string(v: &serde_json::Value) -> Result<Option<String>, InputError> {
    if let Some(s) = v.get("stringValue").and_then(|v| v.as_str()) {
        return Ok(Some(s.to_string()));
    }
    if let Some(i) = v.get("intValue") {
        let parsed = parse_protojson_i64(i)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON intValue".into()))?;
        let mut buf = itoa::Buffer::new();
        return Ok(Some(buf.format(parsed).to_string()));
    }
    if let Some(dv) = v.get("doubleValue") {
        let parsed = parse_protojson_f64(dv)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON doubleValue".into()))?;
        let mut buf = ryu::Buffer::new();
        return Ok(Some(buf.format(parsed).to_string()));
    }
    if let Some(b) = v.get("boolValue").and_then(serde_json::Value::as_bool) {
        return Ok(Some(if b { "true" } else { "false" }.to_string()));
    }
    if let Some(bytes) = v.get("bytesValue").and_then(|v| v.as_str()) {
        let decoded = decode_protojson_bytes(bytes)
            .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON bytesValue: {e}")))?;
        return Ok(Some(hex::encode(&decoded)));
    }
    // Structured types: serialize as JSON strings to match the protobuf decode path.
    if let Some(arr) = v.get("arrayValue") {
        return Ok(Some(json_any_array_to_string(arr)?));
    }
    if let Some(kvlist) = v.get("kvlistValue") {
        return Ok(Some(json_any_kvlist_to_string(kvlist)?));
    }
    Ok(None)
}

fn json_any_array_to_string(array_value: &serde_json::Value) -> Result<String, InputError> {
    // Per protobuf JSON mapping: a missing "values" key means an empty
    // repeated field (valid). A present-but-non-array "values" is invalid.
    let values = match array_value.get("values") {
        None => return Ok("[]".to_string()),
        Some(v) => v.as_array().ok_or_else(|| {
            InputError::Receiver("invalid OTLP JSON arrayValue: 'values' is not an array".into())
        })?,
    };

    let mut out = Vec::with_capacity(values.len());
    for item in values {
        out.push(json_any_value_to_json_literal(item)?.unwrap_or(serde_json::Value::Null));
    }

    serde_json::to_string(&out)
        .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON arrayValue: {e}")))
}

fn json_any_kvlist_to_string(kvlist_value: &serde_json::Value) -> Result<String, InputError> {
    // Per protobuf JSON mapping: a missing "values" key means an empty
    // repeated field (valid). A present-but-non-array "values" is invalid.
    let values = match kvlist_value.get("values") {
        None => return Ok("[]".to_string()),
        Some(v) => v.as_array().ok_or_else(|| {
            InputError::Receiver("invalid OTLP JSON kvlistValue: 'values' is not an array".into())
        })?,
    };

    let mut out = Vec::with_capacity(values.len());
    for entry in values {
        let key = entry
            .get("key")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON kvlistValue entry".into()))?;
        let value = entry
            .get("value")
            .map(json_any_value_to_json_literal)
            .transpose()?
            .flatten()
            .unwrap_or(serde_json::Value::Null);
        out.push(serde_json::json!({ "k": key, "v": value }));
    }

    serde_json::to_string(&out)
        .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON kvlistValue: {e}")))
}

fn json_any_value_to_json_literal(
    v: &serde_json::Value,
) -> Result<Option<serde_json::Value>, InputError> {
    if let Some(s) = v.get("stringValue").and_then(|v| v.as_str()) {
        return Ok(Some(serde_json::Value::String(s.to_string())));
    }
    if let Some(i) = v.get("intValue") {
        let parsed = parse_protojson_i64(i)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON intValue".into()))?;
        return Ok(Some(serde_json::Value::Number(parsed.into())));
    }
    if let Some(dv) = v.get("doubleValue") {
        let parsed = parse_protojson_f64(dv)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON doubleValue".into()))?;
        return Ok(Some(match serde_json::Number::from_f64(parsed) {
            Some(number) => serde_json::Value::Number(number),
            None => serde_json::Value::String(parsed.to_string()),
        }));
    }
    if let Some(b) = v.get("boolValue").and_then(serde_json::Value::as_bool) {
        return Ok(Some(serde_json::Value::Bool(b)));
    }
    if let Some(bytes) = v.get("bytesValue").and_then(|v| v.as_str()) {
        let decoded = decode_protojson_bytes(bytes)
            .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON bytesValue: {e}")))?;
        return Ok(Some(serde_json::Value::String(hex::encode(&decoded))));
    }
    if let Some(arr) = v.get("arrayValue") {
        return Ok(Some(
            serde_json::from_str(&json_any_array_to_string(arr)?)
                .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON arrayValue: {e}")))?,
        ));
    }
    if let Some(kvlist) = v.get("kvlistValue") {
        return Ok(Some(
            serde_json::from_str(&json_any_kvlist_to_string(kvlist)?)
                .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON kvlistValue: {e}")))?,
        ));
    }
    Ok(None)
}

/// Write an OTLP JSON AnyValue as a JSON field preserving primitive types.
fn write_json_any_value_field_from_json(
    out: &mut Vec<u8>,
    key: &str,
    value: &serde_json::Value,
) -> Result<bool, InputError> {
    if let Some(i) = value.get("intValue") {
        let parsed = parse_protojson_i64(i).ok_or_else(|| {
            InputError::Receiver(format!(
                "invalid OTLP JSON intValue for key {key}: not a valid integer"
            ))
        })?;
        write_json_key(out, key);
        write_i64_to_buf(out, parsed);
        return Ok(true);
    }

    if let Some(dv) = value.get("doubleValue") {
        let parsed = parse_protojson_f64(dv).ok_or_else(|| {
            InputError::Receiver(format!(
                "invalid OTLP JSON doubleValue for key {key}: not a valid float"
            ))
        })?;
        write_json_key(out, key);
        write_f64_to_buf(out, parsed);
        return Ok(true);
    }

    if let Some(b) = value.get("boolValue").and_then(serde_json::Value::as_bool) {
        write_json_key(out, key);
        if b {
            out.extend_from_slice(b"true");
        } else {
            out.extend_from_slice(b"false");
        }
        return Ok(true);
    }

    if let Some(s) = json_any_value_to_string(value)? {
        write_json_string_field(out, key, &s);
        return Ok(true);
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::Write as _;

    use super::*;

    #[test]
    fn gzip_decompression_rejects_expansion_past_limit() {
        let raw = vec![b'a'; 4096];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(&raw)
            .expect("test gzip encoder should accept payload");
        let compressed = encoder
            .finish()
            .expect("test gzip encoder should flush payload");

        let err = decompress_gzip(&compressed, 128).expect_err("oversized gzip must fail");
        match err {
            InputError::Io(io_err) => {
                assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
                assert!(io_err.to_string().contains("payload too large"));
            }
            other => panic!("expected InvalidData payload-too-large error, got {other:?}"),
        }
    }

    #[test]
    fn zstd_decompression_rejects_expansion_past_limit() {
        let raw = vec![b'b'; 4096];
        let compressed = zstd::stream::encode_all(raw.as_slice(), 1)
            .expect("test zstd encoder should produce payload");

        let err = decompress_zstd(&compressed, 128).expect_err("oversized zstd must fail");
        match err {
            InputError::Io(io_err) => {
                assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
                assert!(io_err.to_string().contains("payload too large"));
            }
            other => panic!("expected InvalidData payload-too-large error, got {other:?}"),
        }
    }

    #[test]
    fn normalize_otlp_hex_id_borrows_lowercase_ids() {
        let id = normalize_otlp_hex_id("0123456789abcdef0123456789abcdef", 32, "traceId")
            .expect("lowercase id is valid");

        assert!(matches!(id, Cow::Borrowed(_)));
    }

    #[test]
    fn normalize_otlp_hex_id_allocates_only_when_case_changes() {
        let id = normalize_otlp_hex_id("0123456789ABCDEF0123456789ABCDEF", 32, "traceId")
            .expect("uppercase id is valid");

        assert_eq!(id.as_ref(), "0123456789abcdef0123456789abcdef");
        assert!(matches!(id, Cow::Owned(_)));
    }

    #[test]
    fn empty_array_value_without_values_key() {
        let v: serde_json::Value = serde_json::json!({});
        let result = json_any_array_to_string(&v).expect("empty arrayValue must succeed");
        assert_eq!(result, "[]");
    }

    #[test]
    fn empty_kvlist_value_without_values_key() {
        let v: serde_json::Value = serde_json::json!({});
        let result = json_any_kvlist_to_string(&v).expect("empty kvlistValue must succeed");
        assert_eq!(result, "[]");
    }

    #[test]
    fn any_value_empty_array_value() {
        let v: serde_json::Value = serde_json::json!({"arrayValue": {}});
        let result = json_any_value_to_string(&v)
            .expect("must succeed")
            .expect("must produce Some");
        assert_eq!(result, "[]");
    }

    #[test]
    fn any_value_empty_kvlist_value() {
        let v: serde_json::Value = serde_json::json!({"kvlistValue": {}});
        let result = json_any_value_to_string(&v)
            .expect("must succeed")
            .expect("must produce Some");
        assert_eq!(result, "[]");
    }

    #[test]
    fn json_literal_empty_array_value() {
        let v: serde_json::Value = serde_json::json!({"arrayValue": {}});
        let result = json_any_value_to_json_literal(&v)
            .expect("must succeed")
            .expect("must produce Some");
        assert_eq!(result, serde_json::json!([]));
    }

    #[test]
    fn json_literal_empty_kvlist_value() {
        let v: serde_json::Value = serde_json::json!({"kvlistValue": {}});
        let result = json_any_value_to_json_literal(&v)
            .expect("must succeed")
            .expect("must produce Some");
        assert_eq!(result, serde_json::json!([]));
    }

    #[test]
    fn protocol_fields_not_shadowed_by_attributes() {
        use arrow::array::{Array, StringArray, StringViewArray};
        use arrow::datatypes::DataType;

        fn str_val(col: &dyn Array, row: usize) -> String {
            match col.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8")
                    .value(row)
                    .to_string(),
                DataType::Utf8View => col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Utf8View")
                    .value(row)
                    .to_string(),
                other => panic!("expected string column, got {other}"),
            }
        }

        let json_body = br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "scope": { "name": "real-scope", "version": "1.0" },
                    "logRecords": [{
                        "body": { "stringValue": "test" },
                        "traceId": "0123456789abcdef0123456789abcdef",
                        "spanId": "0123456789abcdef",
                        "flags": 1,
                        "attributes": [
                            { "key": "trace_id", "value": { "stringValue": "fake-trace" } },
                            { "key": "span_id", "value": { "stringValue": "fake-span" } },
                            { "key": "flags", "value": { "intValue": "999" } },
                            { "key": "scope.name", "value": { "stringValue": "fake-scope" } },
                            { "key": "scope.version", "value": { "stringValue": "fake-ver" } }
                        ]
                    }]
                }]
            }]
        }"#;

        let batch = decode_otlp_json(json_body, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect("decode must succeed");

        assert_eq!(batch.num_rows(), 1);

        let trace_col = batch
            .column_by_name(field_names::TRACE_ID)
            .expect("trace_id column");
        assert_eq!(
            str_val(trace_col.as_ref(), 0),
            "0123456789abcdef0123456789abcdef",
            "trace_id must be the protocol field, not the attribute"
        );

        let span_col = batch
            .column_by_name(field_names::SPAN_ID)
            .expect("span_id column");
        assert_eq!(
            str_val(span_col.as_ref(), 0),
            "0123456789abcdef",
            "span_id must be the protocol field, not the attribute"
        );

        let scope_col = batch
            .column_by_name(field_names::SCOPE_NAME)
            .expect("scope.name column");
        assert_eq!(
            str_val(scope_col.as_ref(), 0),
            "real-scope",
            "scope.name must be the protocol field, not the attribute"
        );
    }
}
