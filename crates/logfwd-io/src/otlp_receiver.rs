//! OTLP HTTP receiver input source.
//!
//! Listens for OTLP ExportLogsServiceRequest via HTTP POST, decodes the
//! protobuf, and produces JSON lines that the scanner can process.
//!
//! Endpoint: POST /v1/logs (protobuf or JSON)
//!
//! This replaces the hand-rolled `--blackhole` with a proper pipeline input.

use std::io;
use std::io::Read as _;
use std::sync::mpsc;

use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use prost::Message;

use crate::input::{InputEvent, InputSource};

/// Maximum request body size: 10 MB.
const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Bounded channel capacity — limits memory when the pipeline falls behind.
const CHANNEL_BOUND: usize = 4096;

/// OTLP receiver that listens for log exports via HTTP.
pub struct OtlpReceiverInput {
    name: String,
    rx: mpsc::Receiver<Vec<u8>>,
    /// The address the HTTP server is bound to.
    addr: std::net::SocketAddr,
    /// Keep the server thread handle alive.
    _handle: std::thread::JoinHandle<()>,
}

impl OtlpReceiverInput {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4318").
    /// Spawns a background thread to handle requests.
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity(name, addr, CHANNEL_BOUND)
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    fn new_with_capacity(name: impl Into<String>, addr: &str, capacity: usize) -> io::Result<Self> {
        let server = tiny_http::Server::http(addr)
            .map_err(|e| io::Error::other(format!("OTLP receiver bind {addr}: {e}")))?;

        let bound_addr = match server.server_addr() {
            tiny_http::ListenAddr::IP(a) => a,
            tiny_http::ListenAddr::Unix(_) => {
                return Err(io::Error::other("OTLP receiver: unexpected listen addr"));
            }
        };

        let (tx, rx) = mpsc::sync_channel(capacity);

        let handle = std::thread::Builder::new()
            .name("otlp-receiver".into())
            .spawn(move || {
                for mut request in server.incoming_requests() {
                    let url = request.url().to_string();

                    // Accept POST to /v1/logs (standard OTLP endpoint).
                    if request.method() != &tiny_http::Method::Post || !url.starts_with("/v1/logs")
                    {
                        let _ = request.respond(
                            tiny_http::Response::from_string("not found").with_status_code(404),
                        );
                        continue;
                    }

                    // Reject bodies that declare a size over the limit.
                    if request.body_length().unwrap_or(0) > MAX_BODY_SIZE {
                        let _ = request.respond(
                            tiny_http::Response::from_string("payload too large")
                                .with_status_code(413),
                        );
                        continue;
                    }

                    // Read body with a hard cap.
                    let mut body =
                        Vec::with_capacity(request.body_length().unwrap_or(0).min(MAX_BODY_SIZE));
                    match request
                        .as_reader()
                        .take(MAX_BODY_SIZE as u64 + 1)
                        .read_to_end(&mut body)
                    {
                        Ok(n) if n > MAX_BODY_SIZE => {
                            let _ = request.respond(
                                tiny_http::Response::from_string("payload too large")
                                    .with_status_code(413),
                            );
                            continue;
                        }
                        Err(_) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string("read error")
                                    .with_status_code(400),
                            );
                            continue;
                        }
                        Ok(_) => {}
                    }

                    // Decompress if Content-Encoding is set.
                    let content_encoding = request
                        .headers()
                        .iter()
                        .find(|h| h.field.equiv("Content-Encoding"))
                        .map(|h| h.value.as_str().to_lowercase());

                    let body = match content_encoding.as_deref() {
                        Some("zstd") => {
                            let decoder = match zstd::Decoder::new(&body[..]) {
                                Ok(d) => d,
                                Err(_) => {
                                    let _ = request.respond(
                                        tiny_http::Response::from_string(
                                            "zstd decompression failed",
                                        )
                                        .with_status_code(400),
                                    );
                                    continue;
                                }
                            };
                            let mut decompressed =
                                Vec::with_capacity(body.len().min(MAX_BODY_SIZE));
                            match decoder
                                .take(MAX_BODY_SIZE as u64 + 1)
                                .read_to_end(&mut decompressed)
                            {
                                Ok(n) if n > MAX_BODY_SIZE => {
                                    let _ = request.respond(
                                        tiny_http::Response::from_string("payload too large")
                                            .with_status_code(413),
                                    );
                                    continue;
                                }
                                Ok(_) => decompressed,
                                Err(_) => {
                                    let _ = request.respond(
                                        tiny_http::Response::from_string(
                                            "zstd decompression failed",
                                        )
                                        .with_status_code(400),
                                    );
                                    continue;
                                }
                            }
                        }
                        None | Some("identity") => body,
                        Some(other) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(format!(
                                    "unsupported content-encoding: {other}"
                                ))
                                .with_status_code(415),
                            );
                            continue;
                        }
                    };

                    // Determine content type — accept protobuf and JSON.
                    let content_type = request
                        .headers()
                        .iter()
                        .find(|h| h.field.equiv("Content-Type"))
                        .map_or("application/x-protobuf", |h| h.value.as_str());

                    let is_json = content_type.contains("application/json");

                    // Decode and convert to JSON lines.
                    let json_lines = if is_json {
                        decode_otlp_logs_json(&body)
                    } else {
                        decode_otlp_logs(&body)
                    };

                    let json_lines = match json_lines {
                        Ok(lines) => lines,
                        Err(msg) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(msg).with_status_code(400),
                            );
                            continue;
                        }
                    };

                    let send_result = if json_lines.is_empty() {
                        Ok(())
                    } else {
                        tx.try_send(json_lines)
                    };

                    match send_result {
                        Ok(()) => {
                            // Return standard OTLP success response with Content-Type header.
                            let response = tiny_http::Response::from_string("{}")
                                .with_header(
                                    "Content-Type: application/json"
                                        .parse::<tiny_http::Header>()
                                        .unwrap(),
                                )
                                .with_status_code(200);
                            let _ = request.respond(response);
                        }
                        Err(mpsc::TrySendError::Full(_)) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "too many requests: pipeline backpressure",
                                )
                                .with_status_code(429),
                            );
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "service unavailable: pipeline disconnected",
                                )
                                .with_status_code(503),
                            );
                        }
                    }
                }
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name: name.into(),
            rx,
            addr: bound_addr,
            _handle: handle,
        })
    }

    /// Returns the local address the HTTP server is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }
}

impl InputSource for OtlpReceiverInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut all = Vec::new();

        // Drain all available decoded batches.
        while let Ok(data) = self.rx.try_recv() {
            all.extend_from_slice(&data);
        }

        if all.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![InputEvent::Data { bytes: all }])
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Decode an ExportLogsServiceRequest from JSON body and produce
/// newline-delimited JSON lines. Parses the OTLP JSON structure directly
/// since the protobuf types don't derive serde traits.
fn decode_otlp_logs_json(body: &[u8]) -> Result<Vec<u8>, String> {
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let root: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;

    let resource_logs = match root.get("resourceLogs").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };

    let mut out = Vec::new();

    for rl in resource_logs {
        // Collect resource attributes.
        let resource_attrs: Vec<(&str, String)> = rl
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|a| a.as_array())
            .map(|attrs| {
                attrs
                    .iter()
                    .filter_map(|kv| {
                        let key = kv.get("key")?.as_str()?;
                        let val = json_any_value_to_string(kv.get("value")?);
                        Some((key, val))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let scope_logs = match rl.get("scopeLogs").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => continue,
        };

        for sl in scope_logs {
            let records = match sl.get("logRecords").and_then(|v| v.as_array()) {
                Some(arr) => arr,
                None => continue,
            };

            for record in records {
                out.push(b'{');

                if let Some(ts) = record.get("timeUnixNano").and_then(|v| v.as_str()) {
                    write_json_field(&mut out, "timestamp_int", ts);
                    out.push(b',');
                }

                if let Some(sev) = record.get("severityText").and_then(|v| v.as_str()) {
                    if !sev.is_empty() {
                        write_json_string_field(&mut out, "level", sev);
                        out.push(b',');
                    }
                }

                if let Some(body_val) = record.get("body") {
                    let body_str = json_any_value_to_string(body_val);
                    if !body_str.is_empty() {
                        write_json_string_field(&mut out, "message", &body_str);
                        out.push(b',');
                    }
                }

                if let Some(attrs) = record.get("attributes").and_then(|v| v.as_array()) {
                    for kv in attrs {
                        if let (Some(key), Some(val)) =
                            (kv.get("key").and_then(|k| k.as_str()), kv.get("value"))
                        {
                            let s = json_any_value_to_string(val);
                            write_json_string_field(&mut out, key, &s);
                            out.push(b',');
                        }
                    }
                }

                for (key, value) in &resource_attrs {
                    write_json_string_field(&mut out, key, value);
                    out.push(b',');
                }

                if let Some(tid) = record.get("traceId").and_then(|v| v.as_str()) {
                    if !tid.is_empty() {
                        write_json_string_field(&mut out, "trace_id", tid);
                        out.push(b',');
                    }
                }
                if let Some(sid) = record.get("spanId").and_then(|v| v.as_str()) {
                    if !sid.is_empty() {
                        write_json_string_field(&mut out, "span_id", sid);
                        out.push(b',');
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

/// Extract a string from an OTLP JSON AnyValue object.
fn json_any_value_to_string(v: &serde_json::Value) -> String {
    if let Some(s) = v.get("stringValue").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    if let Some(i) = v.get("intValue") {
        // OTLP JSON encodes int64 as string
        return i.as_str().unwrap_or("0").to_string();
    }
    if let Some(d) = v.get("doubleValue").and_then(serde_json::Value::as_f64) {
        return d.to_string();
    }
    if let Some(b) = v.get("boolValue").and_then(serde_json::Value::as_bool) {
        return b.to_string();
    }
    String::new()
}

/// Decode an ExportLogsServiceRequest protobuf and produce newline-delimited
/// JSON. Each LogRecord becomes one JSON line with fields that the scanner
/// can extract into Arrow columns.
fn decode_otlp_logs(body: &[u8]) -> Result<Vec<u8>, String> {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

    if body.is_empty() {
        return Ok(Vec::new());
    }

    let request =
        ExportLogsServiceRequest::decode(body).map_err(|e| format!("invalid protobuf: {e}"))?;

    Ok(convert_request_to_json_lines(&request))
}

/// Shared conversion: ExportLogsServiceRequest -> newline-delimited JSON bytes.
fn convert_request_to_json_lines(
    request: &opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest,
) -> Vec<u8> {
    let mut out = Vec::new();

    for resource_logs in &request.resource_logs {
        // Extract resource attributes (e.g., service.name).
        let mut resource_attrs: Vec<(&str, String)> = Vec::new();
        if let Some(ref resource) = resource_logs.resource {
            for attr in &resource.attributes {
                if let Some(ref value) = attr.value {
                    resource_attrs.push((&attr.key, any_value_to_string(value)));
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                out.push(b'{');

                // timestamp
                if record.time_unix_nano > 0 {
                    write_json_field(
                        &mut out,
                        "timestamp_int",
                        &record.time_unix_nano.to_string(),
                    );
                    out.push(b',');
                }

                // severity
                if !record.severity_text.is_empty() {
                    write_json_string_field(&mut out, "level", &record.severity_text);
                    out.push(b',');
                }

                // body
                if let Some(ref body_val) = record.body {
                    let body_str = any_value_to_string(body_val);
                    write_json_string_field(&mut out, "message", &body_str);
                    out.push(b',');
                }

                // log record attributes
                for attr in &record.attributes {
                    if let Some(ref value) = attr.value {
                        write_json_any_value(&mut out, &attr.key, value);
                        out.push(b',');
                    }
                }

                // resource attributes
                for (key, value) in &resource_attrs {
                    write_json_string_field(&mut out, key, value);
                    out.push(b',');
                }

                // trace context
                if !record.trace_id.is_empty() {
                    write_json_string_field(&mut out, "trace_id", &hex::encode(&record.trace_id));
                    out.push(b',');
                }
                if !record.span_id.is_empty() {
                    write_json_string_field(&mut out, "span_id", &hex::encode(&record.span_id));
                    out.push(b',');
                }

                // Remove trailing comma.
                if out.last() == Some(&b',') {
                    out.pop();
                }

                out.extend_from_slice(b"}\n");
            }
        }
    }

    out
}

fn any_value_to_string(v: &AnyValue) -> String {
    match &v.value {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BoolValue(b)) => b.to_string(),
        Some(Value::BytesValue(b)) => hex::encode(b),
        _ => String::new(),
    }
}

fn write_json_any_value(out: &mut Vec<u8>, key: &str, v: &AnyValue) {
    match &v.value {
        Some(Value::IntValue(i)) => write_json_field(out, key, &i.to_string()),
        Some(Value::DoubleValue(d)) => write_json_field(out, key, &d.to_string()),
        Some(Value::BoolValue(b)) => write_json_field(out, key, &b.to_string()),
        Some(Value::StringValue(s)) => write_json_string_field(out, key, s),
        _ => {}
    }
}

fn write_json_field(out: &mut Vec<u8>, key: &str, value: &str) {
    out.push(b'"');
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(b"\":");
    out.extend_from_slice(value.as_bytes());
}

fn write_json_string_field(out: &mut Vec<u8>, key: &str, value: &str) {
    out.push(b'"');
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(b"\":\"");
    // JSON escape per RFC 8259: all control chars (0x00-0x1f) must be escaped.
    for &b in value.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x00..=0x1f => {
                // Escape remaining control chars as \u00XX.
                out.extend_from_slice(b"\\u00");
                out.push(HEX_DIGITS[(b >> 4) as usize]);
                out.push(HEX_DIGITS[(b & 0x0f) as usize]);
            }
            _ => out.push(b),
        }
    }
    out.push(b'"');
}

const HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";

/// Minimal hex encoding (avoid adding the `hex` crate).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
            s.push(char::from_digit((b & 0xf) as u32, 16).unwrap_or('0'));
        }
        s
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(5)]
    fn hex_encode_matches_format() {
        let len: usize = kani::any();
        kani::assume(len <= 4);
        let mut bytes = [0u8; 4];
        for i in 0..len {
            bytes[i] = kani::any();
        }
        let result = hex::encode(&bytes[..len]);
        assert_eq!(result.len(), len * 2);
        // Each char is a valid hex digit
        for c in result.chars() {
            assert!(c.is_ascii_hexdigit());
        }
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn json_string_escaping_produces_valid_json() {
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let mut bytes = [0u8; 8];
        for i in 0..len {
            bytes[i] = kani::any();
        }
        if let Ok(s) = std::str::from_utf8(&bytes[..len]) {
            let mut out = Vec::new();
            write_json_string_field(&mut out, "k", s);
            // Output must start with "k":" and end with "
            assert!(out.starts_with(b"\"k\":\""));
            assert!(out.ends_with(b"\""));
            // No unescaped control chars, quotes, or backslashes in the value
            let value = &out[5..out.len() - 1]; // strip "k":"..."
            let mut i = 0;
            while i < value.len() {
                if value[i] == b'\\' {
                    i += 2; // skip escaped char
                } else {
                    assert!(value[i] != b'"');
                    assert!(value[i] != b'\\');
                    // No raw control bytes (0x00-0x1f) per RFC 8259.
                    assert!(value[i] >= 0x20);
                    i += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    };
    use prost::Message;

    fn make_test_request() -> Vec<u8> {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        LogRecord {
                            time_unix_nano: 1705314600_000_000_000,
                            severity_text: "INFO".into(),
                            body: Some(AnyValue {
                                value: Some(Value::StringValue("hello world".into())),
                            }),
                            attributes: vec![KeyValue {
                                key: "service".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("myapp".into())),
                                }),
                            }],
                            ..Default::default()
                        },
                        LogRecord {
                            severity_text: "ERROR".into(),
                            body: Some(AnyValue {
                                value: Some(Value::StringValue("something broke".into())),
                            }),
                            attributes: vec![KeyValue {
                                key: "status".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::IntValue(500)),
                                }),
                            }],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        request.encode_to_vec()
    }

    #[test]
    fn decodes_otlp_to_json_lines() {
        let body = make_test_request();
        let json = decode_otlp_logs(&body).unwrap();
        let text = String::from_utf8(json).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();

        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"level\":\"INFO\""), "got: {}", lines[0]);
        assert!(
            lines[0].contains("\"message\":\"hello world\""),
            "got: {}",
            lines[0]
        );
        assert!(
            lines[0].contains("\"service\":\"myapp\""),
            "got: {}",
            lines[0]
        );
        assert!(
            lines[1].contains("\"level\":\"ERROR\""),
            "got: {}",
            lines[1]
        );
        assert!(lines[1].contains("\"status\":500"), "got: {}", lines[1]);
    }

    #[test]
    fn handles_invalid_protobuf() {
        let result = decode_otlp_logs(b"not valid protobuf");
        assert!(result.is_err());
    }

    #[test]
    fn handles_empty_body() {
        let json = decode_otlp_logs(b"").unwrap();
        assert!(json.is_empty());
    }

    #[test]
    fn handles_request_with_no_log_records() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let body = request.encode_to_vec();
        let json = decode_otlp_logs(&body).unwrap();
        assert!(json.is_empty());
    }

    #[test]
    fn handles_record_with_no_body() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        severity_text: "WARN".into(),
                        body: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let body = request.encode_to_vec();
        let json = decode_otlp_logs(&body).unwrap();
        let text = String::from_utf8(json).unwrap();
        assert!(text.contains("\"level\":\"WARN\""));
        assert!(!text.contains("\"message\""));
    }

    #[test]
    fn hex_encode_empty() {
        assert_eq!(hex::encode(&[]), "");
    }

    #[test]
    fn json_escaping_control_chars() {
        // Build a string with all control chars that are valid single-byte UTF-8 (0x00-0x1f all are).
        let ctrl: String = (0u8..=0x1f).map(|b| b as char).collect();
        let mut out = Vec::new();
        write_json_string_field(&mut out, "k", &ctrl);
        let text = String::from_utf8(out).unwrap();

        // No raw control bytes should appear in the output.
        for b in text.as_bytes() {
            // The only bytes < 0x20 allowed are the literal `"` delimiters… but `"` is 0x22.
            // So nothing < 0x20 should appear at all.
            assert!(
                *b >= 0x20,
                "raw control byte 0x{:02x} found in output: {text}",
                b
            );
        }

        // Spot-check specific escapes.
        assert!(text.contains(r"\u0000"), "NUL not escaped: {text}");
        assert!(text.contains(r"\u0001"), "SOH not escaped: {text}");
        assert!(text.contains(r"\u0008"), "BS not escaped: {text}");
        assert!(text.contains(r"\t"), "TAB not escaped: {text}");
        assert!(text.contains(r"\n"), "LF not escaped: {text}");
        assert!(text.contains(r"\r"), "CR not escaped: {text}");
        assert!(text.contains(r"\u000c"), "FF not escaped: {text}");
    }

    #[test]
    fn json_escaping_unicode() {
        // Multi-byte UTF-8 should pass through unchanged.
        let input = "hello \u{00e9}\u{1f600} world \u{4e16}\u{754c}";
        let mut out = Vec::new();
        write_json_string_field(&mut out, "k", input);
        let text = String::from_utf8(out).unwrap();

        // The multi-byte chars should appear literally (not \u-escaped).
        assert!(text.contains('\u{00e9}'), "e-acute missing: {text}");
        assert!(text.contains('\u{1f600}'), "emoji missing: {text}");
        assert!(text.contains('\u{4e16}'), "CJK char missing: {text}");

        // Verify the whole thing is valid JSON.
        let json_str = format!("{{{text}}}");
        serde_json::from_str::<serde_json::Value>(&json_str)
            .unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json_str}"));
    }

    /// Regression test: when the pipeline channel is full the receiver must
    /// return 429 rather than silently dropping the payload and returning 200.
    #[test]
    fn returns_429_when_channel_full_not_200() {
        // Use a tiny channel so it fills up after 2 sends.
        let mut receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 2).unwrap();
        let addr = receiver.local_addr();
        let url = format!("http://{addr}/v1/logs");

        let body = serde_json::json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{"body": {"stringValue": "x"}}]
                }]
            }]
        })
        .to_string();

        // Fill the channel (capacity = 2 so two sends succeed).
        for i in 0..2 {
            let resp = ureq::post(&url)
                .header("content-type", "application/json")
                .send(body.as_bytes())
                .unwrap_or_else(|e| panic!("request {i} failed: {e}"));
            assert_eq!(
                resp.status(),
                200,
                "expected 200 while channel has capacity (request {i})"
            );
        }

        // The channel is now full; the next request must not return 200.
        let result = ureq::post(&url)
            .header("content-type", "application/json")
            .send(body.as_bytes());

        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_ne!(
            status, 200,
            "channel-full request must not return 200 (got {status})"
        );
        assert!(
            status == 429 || status == 503,
            "expected 429 or 503 for backpressure, got {status}"
        );

        // Drain the two buffered entries so the receiver is valid.
        let _ = receiver.poll().unwrap();
    }
}
