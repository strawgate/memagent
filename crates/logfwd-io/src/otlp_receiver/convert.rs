use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use bytes::Bytes;
use logfwd_arrow::StreamingBuilder;
use logfwd_types::field_names;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;

use crate::InputError;

/// Shared conversion: ExportLogsServiceRequest -> newline-delimited JSON bytes.
pub(super) fn convert_request_to_json_lines(request: &ExportLogsServiceRequest) -> Vec<u8> {
    let mut out = Vec::new();

    for resource_logs in &request.resource_logs {
        // Extract resource attributes (e.g., service.name).
        let mut resource_attrs: Vec<(&str, String)> = Vec::new();
        if let Some(ref resource) = resource_logs.resource {
            for attr in &resource.attributes {
                if let Some(ref value) = attr.value {
                    if let Some(value) = any_value_to_string(value) {
                        resource_attrs.push((&attr.key, value));
                    }
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                out.push(b'{');

                // timestamp: prefer time_unix_nano, fall back to observed_time_unix_nano
                // when event time is unknown (fixes #1690).
                let ts = if record.time_unix_nano > 0 {
                    record.time_unix_nano
                } else {
                    record.observed_time_unix_nano
                };
                if ts > 0 {
                    out.push(b'"');
                    out.extend_from_slice(field_names::TIMESTAMP.as_bytes());
                    out.extend_from_slice(b"\":");
                    write_u64_to_buf(&mut out, ts);
                    out.push(b',');
                }

                // severity
                if !record.severity_text.is_empty() {
                    write_json_string_field(&mut out, field_names::SEVERITY, &record.severity_text);
                    out.push(b',');
                }

                // body
                if let Some(ref body_val) = record.body {
                    if let Some(body_str) = any_value_to_string(body_val) {
                        write_json_string_field(&mut out, field_names::BODY, &body_str);
                        out.push(b',');
                    }
                }

                // resource attributes
                for (key, value) in &resource_attrs {
                    write_json_string_field(&mut out, key, value);
                    out.push(b',');
                }

                // log record attributes
                for attr in &record.attributes {
                    if let Some(ref value) = attr.value {
                        if write_json_any_value(&mut out, &attr.key, value) {
                            out.push(b',');
                        }
                    }
                }

                // trace context (write hex directly to avoid allocation)
                if !record.trace_id.is_empty() {
                    out.push(b'"');
                    out.extend_from_slice(field_names::TRACE_ID.as_bytes());
                    out.extend_from_slice(b"\":\"");
                    write_hex_to_buf(&mut out, &record.trace_id);
                    out.extend_from_slice(b"\",");
                }
                if !record.span_id.is_empty() {
                    out.push(b'"');
                    out.extend_from_slice(field_names::SPAN_ID.as_bytes());
                    out.extend_from_slice(b"\":\"");
                    write_hex_to_buf(&mut out, &record.span_id);
                    out.extend_from_slice(b"\",");
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

pub(super) fn convert_request_to_batch(
    request: &ExportLogsServiceRequest,
) -> Result<RecordBatch, InputError> {
    let mut builder = StreamingBuilder::new(false);
    builder.begin_batch(Bytes::new());

    let timestamp_idx = builder.resolve_field(field_names::TIMESTAMP.as_bytes());
    let severity_idx = builder.resolve_field(field_names::SEVERITY.as_bytes());
    let body_idx = builder.resolve_field(field_names::BODY.as_bytes());
    let trace_id_idx = builder.resolve_field(field_names::TRACE_ID.as_bytes());
    let span_id_idx = builder.resolve_field(field_names::SPAN_ID.as_bytes());
    let mut hex_buf = Vec::with_capacity(64);

    for resource_logs in &request.resource_logs {
        let mut resource_attrs = Vec::new();
        if let Some(ref resource) = resource_logs.resource {
            for attr in &resource.attributes {
                if let Some(ref value) = attr.value {
                    resource_attrs.push((&attr.key, value));
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                builder.begin_row();

                // timestamp: prefer time_unix_nano, fall back to
                // observed_time_unix_nano when event time is unknown (#1690).
                let ts_raw = if record.time_unix_nano > 0 {
                    record.time_unix_nano
                } else {
                    record.observed_time_unix_nano
                };
                if let Ok(ts) = i64::try_from(ts_raw) {
                    if ts > 0 {
                        builder.append_i64_value_by_idx(timestamp_idx, ts);
                    }
                }

                if !record.severity_text.is_empty() {
                    builder
                        .append_decoded_str_by_idx(severity_idx, record.severity_text.as_bytes());
                }

                if let Some(ref body_val) = record.body {
                    append_any_value_as_string(&mut builder, body_idx, body_val, &mut hex_buf);
                }

                for attr in &record.attributes {
                    if let Some(ref value) = attr.value {
                        append_attribute_value(&mut builder, &attr.key, value, &mut hex_buf);
                    }
                }

                for (key, value) in &resource_attrs {
                    append_attribute_value(&mut builder, key, value, &mut hex_buf);
                }

                if !record.trace_id.is_empty() {
                    append_hex_field(&mut builder, trace_id_idx, &record.trace_id, &mut hex_buf);
                }
                if !record.span_id.is_empty() {
                    append_hex_field(&mut builder, span_id_idx, &record.span_id, &mut hex_buf);
                }

                builder.end_row();
            }
        }
    }

    builder
        .finish_batch_detached()
        .map_err(|e| InputError::Receiver(format!("structured OTLP batch build error: {e}")))
}

fn append_attribute_value(
    builder: &mut StreamingBuilder,
    key: &str,
    value: &AnyValue,
    hex_buf: &mut Vec<u8>,
) {
    let idx = builder.resolve_field(key.as_bytes());
    match &value.value {
        Some(Value::IntValue(v)) => builder.append_i64_value_by_idx(idx, *v),
        Some(Value::DoubleValue(v)) => builder.append_f64_value_by_idx(idx, *v),
        Some(Value::BoolValue(v)) => builder.append_bool_by_idx(idx, *v),
        Some(Value::StringValue(v)) => builder.append_decoded_str_by_idx(idx, v.as_bytes()),
        Some(Value::BytesValue(v)) => append_hex_field(builder, idx, v, hex_buf),
        _ => {}
    }
}

fn append_any_value_as_string(
    builder: &mut StreamingBuilder,
    idx: usize,
    value: &AnyValue,
    hex_buf: &mut Vec<u8>,
) {
    match &value.value {
        Some(Value::StringValue(v)) => builder.append_decoded_str_by_idx(idx, v.as_bytes()),
        Some(Value::IntValue(v)) => {
            let value = v.to_string();
            builder.append_decoded_str_by_idx(idx, value.as_bytes());
        }
        Some(Value::DoubleValue(v)) => {
            let value = v.to_string();
            builder.append_decoded_str_by_idx(idx, value.as_bytes());
        }
        Some(Value::BoolValue(v)) => {
            builder.append_decoded_str_by_idx(idx, if *v { b"true" } else { b"false" });
        }
        Some(Value::BytesValue(v)) => append_hex_field(builder, idx, v, hex_buf),
        _ => {}
    }
}

fn append_hex_field(
    builder: &mut StreamingBuilder,
    idx: usize,
    value: &[u8],
    hex_buf: &mut Vec<u8>,
) {
    hex_buf.clear();
    hex_buf.reserve(value.len() * 2);
    write_hex_to_buf(hex_buf, value);
    builder.append_decoded_str_by_idx(idx, hex_buf);
}

pub(super) fn any_value_to_string(v: &AnyValue) -> Option<String> {
    match &v.value {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::BytesValue(b)) => Some(hex::encode(b)),
        _ => None,
    }
}

pub(super) fn parse_protojson_i64(value: &serde_json::Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    if let Some(n) = value.as_u64() {
        return i64::try_from(n).ok();
    }
    if let Some(n) = value.as_number() {
        return parse_protojson_i64_str(&n.to_string());
    }
    if let Some(s) = value.as_str() {
        return parse_protojson_i64_str(s);
    }
    None
}

pub(super) fn parse_protojson_u64(value: &serde_json::Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    if let Some(s) = value.as_str() {
        return parse_protojson_u64_str(s);
    }
    if let Some(n) = value.as_number() {
        return parse_protojson_u64_str(&n.to_string());
    }
    None
}

pub(super) fn parse_protojson_f64(value: &serde_json::Value) -> Option<f64> {
    if let Some(n) = value.as_f64() {
        return Some(n);
    }
    if let Some(s) = value.as_str() {
        return match s {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => s.parse::<f64>().ok(),
        };
    }
    None
}

fn parse_protojson_i64_str(s: &str) -> Option<i64> {
    let (negative, digits) = normalize_protojson_integral_digits(s)?;
    let magnitude = digits.parse::<u64>().ok()?;
    if negative {
        let signed = i128::from(magnitude).checked_neg()?;
        i64::try_from(signed).ok()
    } else {
        i64::try_from(magnitude).ok()
    }
}

fn parse_protojson_u64_str(s: &str) -> Option<u64> {
    let (negative, digits) = normalize_protojson_integral_digits(s)?;
    if negative {
        return None;
    }
    digits.parse::<u64>().ok()
}

pub(super) fn normalize_protojson_integral_digits(s: &str) -> Option<(bool, String)> {
    const MAX_INTEGER_DECIMAL_DIGITS: usize = 20;

    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (negative, unsigned) = match s.as_bytes()[0] {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    if unsigned.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(idx) => (&unsigned[..idx], unsigned[idx + 1..].parse::<i32>().ok()?),
        None => (unsigned, 0),
    };

    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    if digits.is_empty() {
        return None;
    }
    if digits.bytes().all(|b| b == b'0') {
        return Some((false, "0".to_string()));
    }

    let fractional_digits = i32::try_from(frac_part.len()).ok()?;
    let effective_exponent = exponent.checked_sub(fractional_digits)?;

    if effective_exponent >= 0 {
        let zeros = usize::try_from(effective_exponent).ok()?;
        let total_len = digits.len().checked_add(zeros)?;
        if total_len > MAX_INTEGER_DECIMAL_DIGITS {
            return None;
        }
        digits.extend(std::iter::repeat_n('0', zeros));
    } else {
        let trim = usize::try_from(effective_exponent.checked_neg()?).ok()?;
        if trim > digits.len() {
            return None;
        }
        if !digits[digits.len() - trim..].bytes().all(|b| b == b'0') {
            return None;
        }
        digits.truncate(digits.len() - trim);
    }

    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some((false, "0".to_string()));
    }

    Some((negative, digits.to_string()))
}

pub(super) fn decode_protojson_bytes(value: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};

    STANDARD
        .decode(value)
        .or_else(|_| STANDARD_NO_PAD.decode(value))
        .or_else(|_| URL_SAFE.decode(value))
        .or_else(|_| URL_SAFE_NO_PAD.decode(value))
}

pub(super) fn write_json_any_value(out: &mut Vec<u8>, key: &str, v: &AnyValue) -> bool {
    match &v.value {
        Some(Value::IntValue(i)) => {
            write_json_key(out, key);
            write_i64_to_buf(out, *i);
            true
        }
        Some(Value::DoubleValue(d)) => {
            write_json_key(out, key);
            write_f64_to_buf(out, *d);
            true
        }
        Some(Value::BoolValue(b)) => {
            write_json_key(out, key);
            out.extend_from_slice(if *b { b"true" } else { b"false" });
            true
        }
        Some(Value::StringValue(s)) => {
            write_json_string_field(out, key, s);
            true
        }
        Some(Value::BytesValue(bytes)) => {
            write_json_key(out, key);
            out.push(b'"');
            write_hex_to_buf(out, bytes);
            out.push(b'"');
            true
        }
        _ => false,
    }
}

/// Write i64 to buffer without allocation using itoa algorithm.
#[inline]
pub(super) fn write_i64_to_buf(out: &mut Vec<u8>, mut n: i64) {
    if n == 0 {
        out.push(b'0');
        return;
    }

    if n < 0 {
        out.push(b'-');
        // Handle i64::MIN specially to avoid overflow
        if n == i64::MIN {
            out.extend_from_slice(b"9223372036854775808");
            return;
        }
        n = -n;
    }

    // Count digits
    let mut temp = n;
    let mut digits = 0;
    while temp > 0 {
        temp /= 10;
        digits += 1;
    }

    // Reserve space and write digits in reverse
    let start = out.len();
    out.resize(start + digits, 0);
    let mut pos = start + digits - 1;
    while n > 0 {
        out[pos] = b'0' + (n % 10) as u8;
        n /= 10;
        // `pos` may wrap on the final iteration, but the loop exits immediately
        // after `n` reaches zero and `pos` is never read again.
        pos = pos.wrapping_sub(1);
    }
}

/// Write u64 to buffer without allocation using itoa algorithm.
#[inline]
pub(super) fn write_u64_to_buf(out: &mut Vec<u8>, mut n: u64) {
    if n == 0 {
        out.push(b'0');
        return;
    }

    // Count digits
    let mut temp = n;
    let mut digits = 0;
    while temp > 0 {
        temp /= 10;
        digits += 1;
    }

    // Reserve space and write digits in reverse
    let start = out.len();
    out.resize(start + digits, 0);
    let mut pos = start + digits - 1;
    while n > 0 {
        out[pos] = b'0' + (n % 10) as u8;
        n /= 10;
        pos = pos.wrapping_sub(1);
    }
}

/// Write f64 to buffer without allocation using ryu algorithm.
#[inline]
pub(super) fn write_f64_to_buf(out: &mut Vec<u8>, d: f64) {
    use std::io::Write;
    if !d.is_finite() {
        out.extend_from_slice(b"null");
        return;
    }
    // Use ryu for optimal float formatting (available in std)
    let _ = write!(out, "{}", d);
}

pub(super) fn write_json_string_field(out: &mut Vec<u8>, key: &str, value: &str) {
    write_json_key(out, key);
    write_json_quoted_string(out, value);
}

const HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";

pub(super) fn write_json_key(out: &mut Vec<u8>, key: &str) {
    write_json_quoted_string(out, key);
    out.push(b':');
}

pub(super) fn write_json_quoted_string(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    write_json_escaped_string_contents(out, value);
    out.push(b'"');
}

pub(super) fn write_json_escaped_string_contents(out: &mut Vec<u8>, value: &str) {
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
}

/// Write hex-encoded bytes directly to output buffer (zero allocation).
pub(super) fn write_hex_to_buf(out: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        out.push(HEX_DIGITS[(b >> 4) as usize]);
        out.push(HEX_DIGITS[(b & 0xf) as usize]);
    }
}

#[cfg(test)]
pub(super) fn write_i64_to_buf_simple(out: &mut Vec<u8>, n: i64) {
    out.extend_from_slice(n.to_string().as_bytes());
}

#[cfg(test)]
pub(super) fn write_u64_to_buf_simple(out: &mut Vec<u8>, n: u64) {
    out.extend_from_slice(n.to_string().as_bytes());
}

#[cfg(test)]
pub(super) fn write_f64_to_buf_simple(out: &mut Vec<u8>, d: f64) {
    if !d.is_finite() {
        out.extend_from_slice(b"null");
    } else {
        out.extend_from_slice(d.to_string().as_bytes());
    }
}

#[cfg(test)]
pub(super) fn write_hex_to_buf_simple(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(hex::encode(bytes).as_bytes());
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(5)]
    fn verify_write_hex_to_buf_lower_hex_pairs() {
        let bytes: [u8; 2] = kani::any();
        let mut out = Vec::new();
        write_hex_to_buf(&mut out, &bytes);

        assert_eq!(out.len(), bytes.len() * 2);
        assert_eq!(
            out.as_slice(),
            &[
                HEX_DIGITS[(bytes[0] >> 4) as usize],
                HEX_DIGITS[(bytes[0] & 0x0f) as usize],
                HEX_DIGITS[(bytes[1] >> 4) as usize],
                HEX_DIGITS[(bytes[1] & 0x0f) as usize],
            ]
        );
        assert!(
            out.iter()
                .all(|&b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        kani::cover!(bytes[0] == 0, "hex encoding covers low nibble zeros");
        kani::cover!(bytes[1] == u8::MAX, "hex encoding covers ff");
    }
}

/// Minimal hex encoding (avoid adding the `hex` crate).
/// Only used in any_value_to_string for BytesValue (rare case).
pub(super) mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_TABLE[(b >> 4) as usize] as char);
            s.push(HEX_TABLE[(b & 0xf) as usize] as char);
        }
        s
    }
}
