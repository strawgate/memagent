use std::io;
use std::io::Read as _;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use flate2::read::GzDecoder;
use logfwd_arrow::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_types::field_names;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;

use crate::InputError;

use super::convert::{
    convert_request_to_batch, decode_protojson_bytes, hex, parse_protojson_f64,
    parse_protojson_i64, parse_protojson_u64, write_f64_to_buf, write_i64_to_buf, write_json_key,
    write_json_string_field, write_u64_to_buf,
};

/// Maximum request body size: 10 MB.
pub(super) const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

pub(super) fn decompress_zstd(body: &[u8]) -> Result<Vec<u8>, InputError> {
    let decoder = zstd::Decoder::new(body)
        .map_err(|_| InputError::Receiver("zstd decompression failed".to_string()))?;
    read_decompressed_body(decoder, body.len(), "zstd decompression failed")
}

pub(super) fn decompress_gzip(body: &[u8]) -> Result<Vec<u8>, InputError> {
    let decoder = GzDecoder::new(body);
    read_decompressed_body(decoder, body.len(), "gzip decompression failed")
}

pub(super) fn read_decompressed_body(
    reader: impl io::Read,
    compressed_len: usize,
    error_label: &str,
) -> Result<Vec<u8>, InputError> {
    let mut decompressed = Vec::with_capacity(compressed_len.min(MAX_BODY_SIZE));
    match reader
        .take(MAX_BODY_SIZE as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > MAX_BODY_SIZE => Err(InputError::Io(io::Error::new(
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

    let resource_logs = match root.get("resourceLogs").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => {
            return Err(InputError::Receiver(
                "missing required field 'resourceLogs' in OTLP JSON payload".to_string(),
            ));
        }
    };

    let mut out = Vec::new();

    for rl in resource_logs {
        // Collect resource attributes.
        let mut resource_attrs: Vec<(String, String)> = Vec::new();
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
                if let Some(value) = json_any_value_to_string(value)? {
                    resource_attrs.push((format!("{resource_prefix}{key}"), value));
                }
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
                if ts_val == 0 {
                    if let Some(obs) = record.get("observedTimeUnixNano") {
                        ts_val = parse_protojson_u64(obs).ok_or_else(|| {
                            InputError::Receiver(
                                "invalid OTLP JSON observedTimeUnixNano: not a valid uint64".into(),
                            )
                        })?;
                    }
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
                    write_json_string_field(&mut out, key, value);
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

                if let Some(tid) = record.get("traceId").and_then(|v| v.as_str()) {
                    if !tid.is_empty() {
                        write_json_string_field(&mut out, field_names::TRACE_ID, tid);
                        out.push(b',');
                    }
                }
                if let Some(sid) = record.get("spanId").and_then(|v| v.as_str()) {
                    if !sid.is_empty() {
                        write_json_string_field(&mut out, field_names::SPAN_ID, sid);
                        out.push(b',');
                    }
                }

                if let Some(flags) = record.get("flags") {
                    let parsed_flags = parse_protojson_i64(flags).ok_or_else(|| {
                        InputError::Receiver("invalid OTLP JSON flags: not a valid int64".into())
                    })?;
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

                if out.last() == Some(&b',') {
                    out.pop();
                }
                out.extend_from_slice(b"}\n");
            }
        }
    }

    Ok(out)
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
        return Ok(Some(parsed.to_string()));
    }
    if let Some(dv) = v.get("doubleValue") {
        let parsed = parse_protojson_f64(dv)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON doubleValue".into()))?;
        return Ok(Some(parsed.to_string()));
    }
    if let Some(b) = v.get("boolValue").and_then(serde_json::Value::as_bool) {
        return Ok(Some(if b { "true" } else { "false" }.to_string()));
    }
    if let Some(bytes) = v.get("bytesValue").and_then(|v| v.as_str()) {
        let decoded = decode_protojson_bytes(bytes)
            .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON bytesValue: {e}")))?;
        return Ok(Some(hex::encode(&decoded)));
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
