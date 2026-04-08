use logfwd_types::field_names;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, any_value::Value},
};

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
            field_names::TIMESTAMP.into(),
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
