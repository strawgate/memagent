//! OTLP HTTP receiver input source.
//!
//! Listens for OTLP ExportLogsServiceRequest via HTTP POST, decodes the
//! protobuf, and produces JSON lines that the scanner can process.
//!
//! Endpoint: POST /v1/logs (protobuf or JSON)
//!
//! This replaces the hand-rolled `--blackhole` with a proper pipeline input.

use std::io;
use std::sync::mpsc;

use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use prost::Message;

use crate::input::{InputEvent, InputSource};

/// OTLP receiver that listens for log exports via HTTP.
pub struct OtlpReceiverInput {
    name: String,
    rx: mpsc::Receiver<Vec<u8>>,
    /// Keep the server thread handle alive.
    _handle: std::thread::JoinHandle<()>,
}

impl OtlpReceiverInput {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4318").
    /// Spawns a background thread to handle requests.
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        let server = tiny_http::Server::http(addr)
            .map_err(|e| io::Error::other(format!("OTLP receiver bind {addr}: {e}")))?;

        let (tx, rx) = mpsc::channel();

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

                    // Read body.
                    let mut body = Vec::with_capacity(
                        request.body_length().unwrap_or(0).min(64 * 1024 * 1024),
                    );
                    if request.as_reader().read_to_end(&mut body).is_err() {
                        let _ = request.respond(
                            tiny_http::Response::from_string("read error").with_status_code(400),
                        );
                        continue;
                    }

                    // Decode and convert to JSON lines.
                    let json_lines = decode_otlp_logs(&body);

                    if !json_lines.is_empty() {
                        // Send to the pipeline. If the channel is full/closed, drop.
                        let _ = tx.send(json_lines);
                    }

                    // Return standard OTLP success response.
                    let _ = request.respond(tiny_http::Response::from_string("{}"));
                }
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name: name.into(),
            rx,
            _handle: handle,
        })
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

/// Decode an ExportLogsServiceRequest protobuf and produce newline-delimited
/// JSON. Each LogRecord becomes one JSON line with fields that the scanner
/// can extract into Arrow columns.
fn decode_otlp_logs(body: &[u8]) -> Vec<u8> {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

    let request = match ExportLogsServiceRequest::decode(body) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::with_capacity(body.len());

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
    // Simple JSON escape.
    for &b in value.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            _ => out.push(b),
        }
    }
    out.push(b'"');
}

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
        let json = decode_otlp_logs(&body);
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
        let json = decode_otlp_logs(b"not valid protobuf");
        assert!(json.is_empty());
    }
}
