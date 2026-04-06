use base64::Engine as _;
use logfwd_types::field_names;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, any_value::Value},
};

const OTLP_TIMESTAMP_FIELD: &str = "timestamp_int";

/// Encode a byte slice as a lowercase hex string (used for trace/span IDs).
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn body_string_from_any(value: &AnyValue) -> Option<String> {
    match &value.value {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::BytesValue(bytes)) => Some(hex_encode(bytes)),
        _ => None,
    }
}

fn attribute_json_from_any(value: &AnyValue) -> Option<serde_json::Value> {
    match &value.value {
        Some(Value::StringValue(s)) => Some(serde_json::Value::String(s.clone())),
        Some(Value::IntValue(i)) => Some(serde_json::Value::Number((*i).into())),
        Some(Value::DoubleValue(d)) => normalized_json_number(*d).map(serde_json::Value::Number),
        Some(Value::BoolValue(b)) => Some(serde_json::Value::Bool(*b)),
        Some(Value::BytesValue(bytes)) => Some(serde_json::Value::String(hex_encode(bytes))),
        _ => None,
    }
}

/// Build the canonical expected JSON row from an OTLP `ExportLogsServiceRequest` oracle.
///
/// Extracts the first log record from the request and maps its fields to the
/// same JSON key names that the logfwd OTLP receiver writes when forwarding.
pub fn expected_single_row_from_request(request: &ExportLogsServiceRequest) -> serde_json::Value {
    let resource_logs = request
        .resource_logs
        .first()
        .expect("oracle request must contain ResourceLogs");
    let scope_logs = resource_logs
        .scope_logs
        .first()
        .expect("oracle request must contain ScopeLogs");
    let record = scope_logs
        .log_records
        .first()
        .expect("oracle request must contain LogRecord");

    let mut object = serde_json::Map::new();

    if record.time_unix_nano > 0 {
        object.insert(
            OTLP_TIMESTAMP_FIELD.into(),
            serde_json::Value::Number(record.time_unix_nano.into()),
        );
    }
    if !record.severity_text.is_empty() {
        object.insert(
            field_names::SEVERITY.into(),
            serde_json::Value::String(record.severity_text.clone()),
        );
    }
    if let Some(body) = record.body.as_ref().and_then(body_string_from_any) {
        object.insert(field_names::BODY.into(), serde_json::Value::String(body));
    }
    for attr in &record.attributes {
        if let Some(value) = attr.value.as_ref().and_then(attribute_json_from_any) {
            object.insert(attr.key.clone(), value);
        }
    }
    if let Some(resource) = &resource_logs.resource {
        for attr in &resource.attributes {
            if let Some(value) = attr.value.as_ref().and_then(body_string_from_any) {
                object.insert(attr.key.clone(), serde_json::Value::String(value));
            }
        }
    }
    if !record.trace_id.is_empty() {
        object.insert(
            field_names::TRACE_ID.into(),
            serde_json::Value::String(hex_encode(&record.trace_id)),
        );
    }
    if !record.span_id.is_empty() {
        object.insert(
            field_names::SPAN_ID.into(),
            serde_json::Value::String(hex_encode(&record.span_id)),
        );
    }

    serde_json::Value::Object(object)
}

/// Build the canonical actual JSON row from an OTLP Collector JSON export.
///
/// Extracts the first log record from the collector's captured export and maps
/// its OTLP JSON fields to the same key names as [`expected_single_row_from_request`].
pub fn emitted_single_row_from_otlp_json(export: &serde_json::Value) -> serde_json::Value {
    let resource_logs = export
        .get("resourceLogs")
        .and_then(serde_json::Value::as_array)
        .and_then(|logs| logs.first())
        .expect("collector export must contain resourceLogs[0]");
    let scope_logs = resource_logs
        .get("scopeLogs")
        .and_then(serde_json::Value::as_array)
        .and_then(|logs| logs.first())
        .expect("collector export must contain scopeLogs[0]");
    let record = scope_logs
        .get("logRecords")
        .and_then(serde_json::Value::as_array)
        .and_then(|records| records.first())
        .expect("collector export must contain logRecords[0]");

    let mut object = serde_json::Map::new();

    if let Some(timestamp) = record.get("timeUnixNano").and_then(parse_u64_from_json) {
        object.insert(
            OTLP_TIMESTAMP_FIELD.into(),
            serde_json::Value::Number(timestamp.into()),
        );
    }
    if let Some(level) = record
        .get("severityText")
        .and_then(serde_json::Value::as_str)
    {
        object.insert(
            field_names::SEVERITY.into(),
            serde_json::Value::String(level.to_string()),
        );
    }
    if let Some(body) = record.get("body").and_then(otlp_json_any_to_string) {
        object.insert(field_names::BODY.into(), serde_json::Value::String(body));
    }
    for attr in record
        .get("attributes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(key) = attr.get("key").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(value) = attr.get("value").and_then(otlp_json_any_to_attr_value) else {
            continue;
        };
        object.insert(key.to_string(), value);
    }
    for attr in resource_logs
        .get("resource")
        .and_then(|resource| resource.get("attributes"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(key) = attr.get("key").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(value) = attr.get("value").and_then(otlp_json_any_to_string) else {
            continue;
        };
        object.insert(key.to_string(), serde_json::Value::String(value));
    }
    if let Some(trace_id) = record.get("traceId").and_then(serde_json::Value::as_str)
        && !trace_id.is_empty()
    {
        object.insert(
            field_names::TRACE_ID.into(),
            serde_json::Value::String(trace_id.to_string()),
        );
    }
    if let Some(span_id) = record.get("spanId").and_then(serde_json::Value::as_str)
        && !span_id.is_empty()
    {
        object.insert(
            field_names::SPAN_ID.into(),
            serde_json::Value::String(span_id.to_string()),
        );
    }

    serde_json::Value::Object(object)
}

fn otlp_json_any_to_string(value: &serde_json::Value) -> Option<String> {
    if let Some(s) = value.get("stringValue").and_then(serde_json::Value::as_str) {
        return Some(s.to_string());
    }
    if let Some(i) = value.get("intValue").and_then(parse_i64_from_json) {
        return Some(i.to_string());
    }
    if let Some(d) = value.get("doubleValue").and_then(serde_json::Value::as_f64) {
        return Some(d.to_string());
    }
    if let Some(b) = value.get("boolValue").and_then(serde_json::Value::as_bool) {
        return Some(b.to_string());
    }
    if let Some(bytes) = value.get("bytesValue").and_then(serde_json::Value::as_str) {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(bytes)
            .ok()?;
        return Some(hex_encode(&decoded));
    }
    None
}

fn otlp_json_any_to_attr_value(value: &serde_json::Value) -> Option<serde_json::Value> {
    if let Some(s) = value.get("stringValue").and_then(serde_json::Value::as_str) {
        return Some(serde_json::Value::String(s.to_string()));
    }
    if let Some(i) = value.get("intValue").and_then(parse_i64_from_json) {
        return Some(serde_json::Value::Number(i.into()));
    }
    if let Some(d) = value.get("doubleValue").and_then(serde_json::Value::as_f64) {
        return normalized_json_number(d).map(serde_json::Value::Number);
    }
    if let Some(b) = value.get("boolValue").and_then(serde_json::Value::as_bool) {
        return Some(serde_json::Value::Bool(b));
    }
    if let Some(bytes) = value.get("bytesValue").and_then(serde_json::Value::as_str) {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(bytes)
            .ok()?;
        return Some(serde_json::Value::String(hex_encode(&decoded)));
    }
    None
}

fn normalized_json_number(value: f64) -> Option<serde_json::Number> {
    if value.fract() == 0.0
        && value.is_finite()
        && value >= i64::MIN as f64
        && value < (i64::MAX as f64 + 1.0)
    {
        return Some((value as i64).into());
    }

    serde_json::Number::from_f64(value)
}

fn parse_i64_from_json(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        serde_json::Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

fn parse_u64_from_json(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        _ => None,
    }
}
