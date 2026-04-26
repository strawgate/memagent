//! OTLP protobuf encoding helpers and log-field parsers.
//!
//! Provides protobuf wire format primitives (`encode_varint`, `encode_tag`,
//! etc.), severity parsing, and timestamp parsing. The actual OTLP
//! LogRecord encoding from RecordBatch columns lives in
//! `crates/ffwd-output/src/otlp_sink.rs`.
//!
//! The OTLP LogRecord protobuf layout (field numbers from opentelemetry/proto/logs/v1/logs.proto):
//!   1: time_unix_nano (fixed64)
//!   2: severity_number (int32 enum)
//!   3: severity_text (string)
//!   5: body (AnyValue message containing string_value)
//!   6: attributes (repeated KeyValue)
//!   8: flags (fixed32) — W3C trace flags
//!   9: trace_id (bytes, 16 bytes) — hex-decoded from string column
//!  10: span_id (bytes, 8 bytes) — hex-decoded from string column
//!  11: observed_time_unix_nano (fixed64)
//!
//! Wire format: each field = tag_varint + value
//!   tag = (field_number << 3) | wire_type
//!   wire_type: 0=varint, 1=64-bit fixed, 2=length-delimited, 5=32-bit fixed

// --- Protobuf field number constants ---
//
// Extracted from opentelemetry/proto/logs/v1/logs.proto so that the encoder
// uses named constants instead of magic numbers. Kani tripwire proofs
// (in the verification module below) verify these match the proto spec.

// --- Wire types ---

/// Protobuf wire type 0: varint.
pub const WIRE_TYPE_VARINT: u8 = 0;
/// Protobuf wire type 1: 64-bit fixed.
pub const WIRE_TYPE_FIXED64: u8 = 1;
/// Protobuf wire type 2: length-delimited.
pub const WIRE_TYPE_LEN: u8 = 2;
/// Protobuf wire type 5: 32-bit fixed.
pub const WIRE_TYPE_FIXED32: u8 = 5;

// --- ExportLogsServiceRequest ---

/// `ExportLogsServiceRequest.resource_logs` (repeated ResourceLogs).
pub const EXPORT_LOGS_REQUEST_RESOURCE_LOGS: u32 = 1;

// --- ResourceLogs ---

/// `ResourceLogs.resource` (Resource message).
pub const RESOURCE_LOGS_RESOURCE: u32 = 1;
/// `ResourceLogs.scope_logs` (repeated ScopeLogs).
pub const RESOURCE_LOGS_SCOPE_LOGS: u32 = 2;

// --- Resource ---

/// `Resource.attributes` (repeated KeyValue).
pub const RESOURCE_ATTRIBUTES: u32 = 1;

// --- ScopeLogs ---

/// `ScopeLogs.scope` (InstrumentationScope message).
pub const SCOPE_LOGS_SCOPE: u32 = 1;
/// `ScopeLogs.log_records` (repeated LogRecord).
pub const SCOPE_LOGS_LOG_RECORDS: u32 = 2;

// --- InstrumentationScope ---

/// `InstrumentationScope.name` (string).
pub const INSTRUMENTATION_SCOPE_NAME: u32 = 1;
/// `InstrumentationScope.version` (string).
pub const INSTRUMENTATION_SCOPE_VERSION: u32 = 2;

// --- LogRecord field numbers (logs.proto) ---

/// `LogRecord.time_unix_nano` (fixed64).
pub const LOG_RECORD_TIME_UNIX_NANO: u32 = 1;
/// `LogRecord.severity_number` (SeverityNumber enum, varint).
pub const LOG_RECORD_SEVERITY_NUMBER: u32 = 2;
/// `LogRecord.severity_text` (string).
pub const LOG_RECORD_SEVERITY_TEXT: u32 = 3;
/// `LogRecord.body` (AnyValue message).
pub const LOG_RECORD_BODY: u32 = 5;
/// `LogRecord.attributes` (repeated KeyValue).
pub const LOG_RECORD_ATTRIBUTES: u32 = 6;
/// `LogRecord.flags` (fixed32, W3C trace flags).
pub const LOG_RECORD_FLAGS: u32 = 8;
/// `LogRecord.trace_id` (bytes, 16 bytes).
pub const LOG_RECORD_TRACE_ID: u32 = 9;
/// `LogRecord.span_id` (bytes, 8 bytes).
pub const LOG_RECORD_SPAN_ID: u32 = 10;
/// `LogRecord.observed_time_unix_nano` (fixed64).
pub const LOG_RECORD_OBSERVED_TIME_UNIX_NANO: u32 = 11;

// --- AnyValue field numbers (common.proto) ---

/// `AnyValue.string_value` (string).
pub const ANY_VALUE_STRING_VALUE: u32 = 1;
/// `AnyValue.bool_value` (bool).
pub const ANY_VALUE_BOOL_VALUE: u32 = 2;
/// `AnyValue.int_value` (int64).
pub const ANY_VALUE_INT_VALUE: u32 = 3;
/// `AnyValue.double_value` (double/fixed64).
pub const ANY_VALUE_DOUBLE_VALUE: u32 = 4;
/// `AnyValue.array_value` (ArrayValue message).
pub const ANY_VALUE_ARRAY_VALUE: u32 = 5;
/// `AnyValue.kvlist_value` (KeyValueList message).
pub const ANY_VALUE_KVLIST_VALUE: u32 = 6;
/// `AnyValue.bytes_value` (bytes).
pub const ANY_VALUE_BYTES_VALUE: u32 = 7;

// --- KeyValue field numbers (common.proto) ---

/// `KeyValue.key` (string).
pub const KEY_VALUE_KEY: u32 = 1;
/// `KeyValue.value` (AnyValue message).
pub const KEY_VALUE_VALUE: u32 = 2;

// --- Protobuf wire format helpers ---

use alloc::vec::Vec;
use ffwd_lint_attrs::{allow_unproven, trust_boundary, verified};

/// Encode a varint into buf at offset, return new offset.
#[inline(always)]
#[verified(kani = "verify_varint_len_matches_encode")]
pub fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        if value < 0x80 {
            buf.push(value as u8);
            return;
        }
        buf.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
}

/// Compute encoded varint length without writing.
#[inline(always)]
#[allow(clippy::match_overlapping_arm)]
#[verified(kani = "verify_varint_len_matches_encode")]
pub const fn varint_len(value: u64) -> usize {
    match value {
        0..=0x7F => 1,
        0..=0x3FFF => 2,
        0..=0x1FFFFF => 3,
        0..=0xFFFFFFF => 4,
        0..=0x7FFFFFFFF => 5,
        0..=0x3FFFFFFFFFF => 6,
        0..=0x1FFFFFFFFFFFF => 7,
        0..=0xFFFFFFFFFFFFFF => 8,
        0..=0x7FFFFFFFFFFFFFFF => 9,
        _ => 10,
    }
}

/// Write a protobuf tag (field_number + wire_type).
#[inline(always)]
#[verified(kani = "verify_encode_tag")]
pub fn encode_tag(buf: &mut Vec<u8>, field_number: u32, wire_type: u8) {
    encode_varint(buf, ((field_number as u64) << 3) | wire_type as u64);
}

/// Write a fixed64 field (tag + 8 bytes little-endian).
#[inline(always)]
#[verified(kani = "verify_encode_fixed64")]
pub fn encode_fixed64(buf: &mut Vec<u8>, field_number: u32, value: u64) {
    encode_tag(buf, field_number, 1); // wire type 1 = 64-bit
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a varint field (tag + varint value).
#[inline(always)]
#[verified(kani = "verify_encode_varint_field")]
pub fn encode_varint_field(buf: &mut Vec<u8>, field_number: u32, value: u64) {
    encode_tag(buf, field_number, 0); // wire type 0 = varint
    encode_varint(buf, value);
}

/// Write a length-delimited field (tag + length + bytes).
#[inline(always)]
#[verified(kani = "verify_encode_bytes_field_content")]
pub fn encode_bytes_field(buf: &mut Vec<u8>, field_number: u32, data: &[u8]) {
    encode_tag(buf, field_number, 2); // wire type 2 = length-delimited
    encode_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

/// Compute the encoded size of a protobuf tag (`field_number << 3 | 2`).
///
/// Wire type 2 = length-delimited. Used as a building block by
/// `bytes_field_total_size`.
#[inline(always)]
#[verified(kani = "verify_tag_size_vs_oracle")]
pub const fn tag_size(field_number: u32) -> usize {
    varint_len(((field_number as u64) << 3) | 2)
}

/// Compute the total encoded size of a length-delimited field
/// (tag varint + length varint + data), without writing anything.
#[inline(always)]
#[verified(kani = "verify_bytes_field_total_size_vs_oracle")]
pub const fn bytes_field_total_size(field_number: u32, data_len: usize) -> usize {
    tag_size(field_number) + varint_len(data_len as u64) + data_len
}

/// Compute the encoded size of a length-delimited field (without writing).
#[inline(always)]
#[verified(kani = "verify_bytes_field_size")]
pub const fn bytes_field_size(field_number: u32, data_len: usize) -> usize {
    bytes_field_total_size(field_number, data_len)
}

/// Write a fixed32 field (tag + 4 bytes little-endian).
/// Used for LogRecord field 8 (`flags`), wire type 5.
#[inline(always)]
#[allow_unproven]
pub fn encode_fixed32(buf: &mut Vec<u8>, field_number: u32, value: u32) {
    encode_tag(buf, field_number, 5); // wire type 5 = 32-bit fixed
    buf.extend_from_slice(&value.to_le_bytes());
}

// --- Protobuf wire format decode helpers ---

/// Decode a varint from `buf` starting at `pos`.
///
/// Returns `(value, new_pos)` or an error string if the input is truncated
/// or the varint exceeds 10 bytes. The 10th byte must carry a payload
/// of at most 1 to avoid u64 overflow.
#[verified(kani = "verify_decode_varint_vs_oracle")]
#[trust_boundary]
pub fn decode_varint(buf: &[u8], pos: usize) -> Result<(u64, usize), &'static str> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut i = pos;
    loop {
        if i >= buf.len() {
            return Err("varint: unexpected end of input");
        }
        let byte = buf[i];
        i += 1;
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i));
        }
        shift += 7;
        if shift >= 64 || (shift == 63 && byte & 0x7F > 1) {
            return Err("varint: too many bytes");
        }
    }
}

/// Decode a protobuf tag into `(field_number, wire_type, new_pos)`.
#[allow_unproven]
#[trust_boundary]
pub fn decode_tag(buf: &[u8], pos: usize) -> Result<(u32, u8, usize), &'static str> {
    let (tag, new_pos) = decode_varint(buf, pos)?;
    let field_number = (tag >> 3) as u32;
    let wire_type = (tag & 0x07) as u8;
    Ok((field_number, wire_type, new_pos))
}

/// Skip a protobuf field value based on its wire type. Returns the new
/// position after the field value.
#[allow_unproven]
#[trust_boundary]
pub fn skip_field(buf: &[u8], wire_type: u8, pos: usize) -> Result<usize, &'static str> {
    match wire_type {
        0 => {
            // Varint.
            let (_, new_pos) = decode_varint(buf, pos)?;
            Ok(new_pos)
        }
        1 => {
            // 64-bit fixed.
            if pos + 8 > buf.len() {
                return Err("skip: truncated fixed64");
            }
            Ok(pos + 8)
        }
        2 => {
            // Length-delimited.
            let (len, new_pos) = decode_varint(buf, pos)?;
            let len_usize = usize::try_from(len).map_err(|_e| "skip: length overflow")?;
            let end = new_pos
                .checked_add(len_usize)
                .ok_or("skip: length-delimited overflow")?;
            if end > buf.len() {
                return Err("skip: length-delimited overflow");
            }
            Ok(end)
        }
        5 => {
            // 32-bit fixed.
            if pos + 4 > buf.len() {
                return Err("skip: truncated fixed32");
            }
            Ok(pos + 4)
        }
        _ => Err("skip: unsupported wire type"),
    }
}

/// Decode a hex-encoded byte slice into `out`.
///
/// Returns `true` when `hex_bytes.len() == out.len() * 2` and every character
/// is a valid lowercase or uppercase hex digit. On failure `out` is left in
/// an unspecified state (partial write).
///
/// Designed for zero-allocation decoding of `trace_id` (32 hex chars → 16 bytes)
/// and `span_id` (16 hex chars → 8 bytes) on the hot encoding path.
#[verified(kani = "verify_hex_decode_roundtrip")]
pub fn hex_decode(hex_bytes: &[u8], out: &mut [u8]) -> bool {
    if hex_bytes.len() != out.len() * 2 {
        return false;
    }
    // Validate all nibbles first: collect the bitwise OR of all LUT values.
    // Invalid bytes produce 0xFF; valid nibbles produce 0x00–0x0F.  The high
    // nibble of the OR will be set (≥ 0x10) iff any invalid byte was present.
    // Doing validation as a single pass over the input lets the compiler
    // vectorise the check without a per-pair early-return branch.
    let mut any_invalid = 0u8;
    for &b in hex_bytes {
        any_invalid |= HEX_NIBBLE_LUT[b as usize];
    }
    if any_invalid & 0xF0 != 0 {
        return false;
    }
    // All nibbles are valid; decode without any per-pair branch.
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = (HEX_NIBBLE_LUT[hex_bytes[i * 2] as usize] << 4)
            | HEX_NIBBLE_LUT[hex_bytes[i * 2 + 1] as usize];
    }
    true
}

/// 256-entry lookup table for hex nibble decoding.
///
/// Valid ASCII hex digits map to their 0x00–0x0F value; everything else maps
/// to the sentinel 0xFF used by callers to signal an invalid character.
/// Using a LUT replaces the three-branch match with a single indexed load.
/// The table is 256 bytes and stays hot in L1 cache during batch decode loops.
const HEX_NIBBLE_LUT: [u8; 256] = {
    let mut lut = [0xFF_u8; 256];
    let mut i = 0u16;
    while i < 256 {
        let c = i as u8;
        lut[c as usize] = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => 0xFF,
        };
        i += 1;
    }
    lut
};

// --- OTLP Severity mapping ---

/// OTLP SeverityNumber enum values.
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Severity {
    /// No severity specified.
    Unspecified = 0,
    /// TRACE level.
    Trace = 1,
    /// DEBUG level.
    Debug = 5,
    /// INFO level.
    Info = 9,
    /// WARN level.
    Warn = 13,
    /// ERROR level.
    Error = 17,
    /// FATAL level.
    Fatal = 21,
}

/// Fast severity lookup from first byte + length. No string comparison needed.
#[inline(always)]
#[verified(kani = "verify_parse_severity_no_false_positives")]
pub fn parse_severity(text: &[u8]) -> (Severity, &[u8]) {
    // Exact case-insensitive match against the 6 standard severity strings
    // plus common aliases used by syslog and application logs.
    // Previous version used prefix matching (e.g., any string starting with
    // "I" matched INFO) which caused false positives like "INVALID" → Info.
    let sev = match text.len() {
        3 if eq_ignore_case_3(text, b"ERR") => Severity::Error,
        4 if eq_ignore_case_4(text, b"INFO") => Severity::Info,
        4 if eq_ignore_case_4(text, b"WARN") => Severity::Warn,
        5 if eq_ignore_case_5(text, b"DEBUG") => Severity::Debug,
        5 if eq_ignore_case_5(text, b"TRACE") => Severity::Trace,
        5 if eq_ignore_case_5(text, b"ERROR") => Severity::Error,
        5 if eq_ignore_case_5(text, b"FATAL") => Severity::Fatal,
        6 if eq_ignore_case_6(text, b"NOTICE") => Severity::Info,
        7 if eq_ignore_case_7(text, b"WARNING") => Severity::Warn,
        8 if eq_ignore_case_8(text, b"CRITICAL") => Severity::Fatal,
        _ => Severity::Unspecified,
    };
    if matches!(sev, Severity::Unspecified) {
        (Severity::Unspecified, text)
    } else {
        (sev, text)
    }
}

/// Case-insensitive variable-length comparison for ASCII letters.
/// Used by Kani proofs to verify parse_severity only matches standard levels.
#[cfg(kani)]
#[allow_unproven]
fn eq_ignore_case_match(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x | 0x20 == y | 0x20)
}

/// Case-insensitive 3-byte comparison.
#[inline(always)]
#[allow_unproven]
fn eq_ignore_case_3(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20 && a[1] | 0x20 == b[1] | 0x20 && a[2] | 0x20 == b[2] | 0x20
}

/// Case-insensitive 4-byte comparison. Uses `|0x20` which maps uppercase
/// ASCII letters to lowercase. This is NOT a general case-fold — it has
/// collisions for non-letters (e.g., `@` |0x20 = `` ` ``). Safe here because
/// the comparison targets ("INFO", "WARN") are all ASCII letters.
#[inline(always)]
#[verified(kani = "verify_eq_ignore_case_4_no_false_positives_info")]
fn eq_ignore_case_4(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20
        && a[1] | 0x20 == b[1] | 0x20
        && a[2] | 0x20 == b[2] | 0x20
        && a[3] | 0x20 == b[3] | 0x20
}

/// Case-insensitive 5-byte comparison.
#[inline(always)]
#[verified(kani = "verify_eq_ignore_case_5_no_false_positives_error")]
fn eq_ignore_case_5(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20
        && a[1] | 0x20 == b[1] | 0x20
        && a[2] | 0x20 == b[2] | 0x20
        && a[3] | 0x20 == b[3] | 0x20
        && a[4] | 0x20 == b[4] | 0x20
}

/// Case-insensitive 6-byte comparison.
#[inline(always)]
#[verified(kani = "verify_eq_ignore_case_6_no_false_positives_notice")]
fn eq_ignore_case_6(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20
        && a[1] | 0x20 == b[1] | 0x20
        && a[2] | 0x20 == b[2] | 0x20
        && a[3] | 0x20 == b[3] | 0x20
        && a[4] | 0x20 == b[4] | 0x20
        && a[5] | 0x20 == b[5] | 0x20
}

/// Case-insensitive 7-byte comparison.
#[inline(always)]
#[verified(kani = "verify_eq_ignore_case_7_no_false_positives_warning")]
fn eq_ignore_case_7(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20
        && a[1] | 0x20 == b[1] | 0x20
        && a[2] | 0x20 == b[2] | 0x20
        && a[3] | 0x20 == b[3] | 0x20
        && a[4] | 0x20 == b[4] | 0x20
        && a[5] | 0x20 == b[5] | 0x20
        && a[6] | 0x20 == b[6] | 0x20
}

/// Case-insensitive 8-byte comparison.
#[inline(always)]
#[verified(kani = "verify_eq_ignore_case_8_no_false_positives_critical")]
fn eq_ignore_case_8(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20
        && a[1] | 0x20 == b[1] | 0x20
        && a[2] | 0x20 == b[2] | 0x20
        && a[3] | 0x20 == b[3] | 0x20
        && a[4] | 0x20 == b[4] | 0x20
        && a[5] | 0x20 == b[5] | 0x20
        && a[6] | 0x20 == b[6] | 0x20
        && a[7] | 0x20 == b[7] | 0x20
}

// JSON field extraction (extract_json_fields, JsonFields, key_eq_ignore_case)
// removed — the scanner + Arrow pipeline extracts fields into RecordBatch
// columns. OTLP encoding reads from RecordBatch via otlp_sink.rs. See #357.

// --- Timestamp parsing ---

/// Parse an ISO 8601 / RFC 3339 timestamp to nanoseconds since Unix epoch.
/// Hand-rolled for speed — no chrono, no strptime. Handles:
///   2024-01-15T10:30:00Z
///   2024-01-15T10:30:00.123Z
///   2024-01-15T10:30:00.123456789Z
///   2024-01-15 10:30:00Z (space separator)
///
/// Returns `None` on parse failure. Callers typically fall back to
/// `observed_time_ns`. Note: `Some(0)` is a valid result for exactly
/// 1970-01-01T00:00:00Z (Unix epoch).
///
/// Fractional seconds beyond 9 digits (nanosecond precision) are
/// truncated — this is intentional as OTLP uses nanoseconds.
#[verified(kani = "verify_parse_timestamp_compositional")]
pub fn parse_timestamp_nanos(ts: &[u8]) -> Option<u64> {
    if ts.len() < 19 {
        return None;
    }

    // Validate separator characters first: YYYY-MM-DDThh:mm:ss
    // Do this before digit parsing so invalid formats fail fast.
    if ts[4] != b'-'
        || ts[7] != b'-'
        || (ts[10] != b'T' && ts[10] != b't' && ts[10] != b' ')
        || ts[13] != b':'
        || ts[16] != b':'
    {
        return None;
    }

    // Parse and validate all digit fields in one pass each.
    // parse_Ndigits_checked uses wrapping_sub so each digit is validated and
    // converted in a single subtract+compare — no double-checking.
    let year = parse_4digits_checked(ts, 0)? as i64;
    let month = parse_2digits_checked(ts, 5)? as u32;
    let day = parse_2digits_checked(ts, 8)? as u32;
    let hour = parse_2digits_checked(ts, 11)? as u64;
    let min = parse_2digits_checked(ts, 14)? as u64;
    let sec = parse_2digits_checked(ts, 17)? as u64;

    if year == 0 || month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }

    // Month-specific day validation (#1874). Reject invalid dates like Feb 31.
    let max_day = days_in_month(year, month);
    if day > max_day {
        return None;
    }

    if hour > 23 || min > 59 || sec > 60 {
        return None;
    }

    // Reject years that would overflow u64 nanos (year > ~584 from epoch).
    // Year 2554 can exceed u64::MAX nanos; 2553-12-31T23:59:59.999999999Z fits.
    if year > 2553 {
        return None;
    }

    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
    }

    // Parse timezone designator: 'Z' or ±HH:MM.
    let mut tz_start = 19usize;
    let mut frac_nanos = 0u64;

    // Parse fractional seconds if present.
    if ts.len() > 19 && ts[19] == b'.' {
        let frac_start = 20;
        let mut frac_end = frac_start;
        while frac_end < ts.len() && ts[frac_end].is_ascii_digit() {
            frac_end += 1;
        }
        let frac_digits = frac_end - frac_start;
        if frac_digits > 0 {
            let mut frac_val = 0u64;
            for &b in &ts[frac_start..frac_end.min(frac_start + 9)] {
                frac_val = frac_val * 10 + (b - b'0') as u64;
            }
            // Pad or truncate to 9 digits (nanoseconds).
            for _ in frac_digits..9 {
                frac_val *= 10;
            }
            frac_nanos = frac_val;
        }
        tz_start = frac_end;
    }

    if tz_start >= ts.len() {
        return None;
    }

    let tz_offset_secs: i64 = match ts[tz_start] {
        b'Z' | b'z' if tz_start + 1 == ts.len() => 0,
        b'+' | b'-' if tz_start + 6 == ts.len() => {
            if ts[tz_start + 3] != b':' {
                return None;
            }
            let tz_h = parse_2digits_checked(ts, tz_start + 1)? as i64;
            let tz_m = parse_2digits_checked(ts, tz_start + 4)? as i64;
            if tz_h >= 24 || tz_m >= 60 {
                return None;
            }
            let sign = if ts[tz_start] == b'-' { -1 } else { 1 };
            sign * (tz_h * 3600 + tz_m * 60)
        }
        _ => return None,
    };

    let total_secs = days
        .checked_mul(86_400)?
        .checked_add(hour as i64 * 3600)?
        .checked_add(min as i64 * 60)?
        .checked_add(sec as i64)?
        .checked_sub(tz_offset_secs)?;
    if total_secs < 0 {
        return None;
    }

    let nanos = (total_secs as u64)
        .checked_mul(1_000_000_000)?
        .checked_add(frac_nanos)?;
    Some(nanos)
}

/// Parse 4 ASCII digits at offset. Returns 0 on non-digit.
// Only used by Kani proof harnesses; production paths use parse_4digits_checked.
#[allow(dead_code)]
#[inline(always)]
#[cfg_attr(kani, kani::ensures(|result: &u16| *result <= 9999))]
#[verified(kani = "verify_parse_4digits_contract")]
fn parse_4digits(s: &[u8], off: usize) -> u16 {
    if off + 4 > s.len() {
        return 0;
    }
    let (a, b, c, d) = (s[off], s[off + 1], s[off + 2], s[off + 3]);
    if !a.is_ascii_digit() || !b.is_ascii_digit() || !c.is_ascii_digit() || !d.is_ascii_digit() {
        return 0;
    }
    (a - b'0') as u16 * 1000 + (b - b'0') as u16 * 100 + (c - b'0') as u16 * 10 + (d - b'0') as u16
}

/// Parse 2 ASCII digits at offset. Returns 0 on non-digit.
// Only used by Kani proof harnesses; production paths use parse_2digits_checked.
#[allow(dead_code)]
#[inline(always)]
#[cfg_attr(kani, kani::ensures(|result: &u8| *result <= 99))]
#[verified(kani = "verify_parse_2digits_contract")]
fn parse_2digits(s: &[u8], off: usize) -> u8 {
    if off + 2 > s.len() {
        return 0;
    }
    let (a, b) = (s[off], s[off + 1]);
    if !a.is_ascii_digit() || !b.is_ascii_digit() {
        return 0;
    }
    (a - b'0') * 10 + (b - b'0')
}

/// Parse 4 ASCII digits at `s[off..off+4]`, returning `None` if any byte is not
/// a digit. Uses `wrapping_sub` to validate and convert in a single pass —
/// avoids the double-check that `parse_4digits` + external `is_ascii_digit`
/// calls would perform.
#[inline(always)]
#[allow_unproven]
fn parse_4digits_checked(s: &[u8], off: usize) -> Option<u16> {
    let (a, b, c, d) = (
        s[off].wrapping_sub(b'0'),
        s[off + 1].wrapping_sub(b'0'),
        s[off + 2].wrapping_sub(b'0'),
        s[off + 3].wrapping_sub(b'0'),
    );
    if a > 9 || b > 9 || c > 9 || d > 9 {
        return None;
    }
    Some(a as u16 * 1000 + b as u16 * 100 + c as u16 * 10 + d as u16)
}

/// Parse 2 ASCII digits at `s[off..off+2]`, returning `None` if either byte is
/// not a digit. Single-pass: one `wrapping_sub` + compare per digit instead of
/// a `is_ascii_digit` check followed by a separate subtraction.
#[inline(always)]
#[allow_unproven]
fn parse_2digits_checked(s: &[u8], off: usize) -> Option<u8> {
    let a = s[off].wrapping_sub(b'0');
    let b = s[off + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some(a * 10 + b)
}

/// Returns `true` if `year` is a leap year (Gregorian calendar).
#[inline(always)]
#[allow_unproven]
fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Returns the number of days in the given month (1-12) for the given year.
#[inline(always)]
#[allow_unproven]
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days from 1970-01-01 to the given civil date. Algorithm from Howard Hinnant.
#[cfg_attr(kani, kani::requires(
    year >= 1 && year <= 2553 && month >= 1 && month <= 12 && day >= 1 && day <= 31
))]
#[cfg_attr(kani, kani::ensures(|result: &i64|
    // Pre-epoch dates (year < 1970) are always negative; epoch-1970-01-01 = day 0.
    (year >= 1970 || *result < 0)
    // Post-1970 lower bound: at most one leap-year worth before epoch boundary.
    && (year < 1970 || *result >= -366)
    // Upper bound: days_from_civil(2553, 12, 31) ≈ 213_301; 213_400 gives margin.
    // Required so `stub_verified(days_from_civil)` keeps nanos arithmetic within u64.
    && *result <= 213_400
))]
#[verified(kani = "verify_days_from_civil_contract")]
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let m = if month <= 2 {
        month as i64 + 9
    } else {
        month as i64 - 3
    };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

// OTLP LogRecord encoding from RecordBatch columns lives in
// crates/ffwd-output/src/otlp_sink.rs. Raw-line encoding was
// removed in #357.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_field_length_overflow_rejected() {
        // wire_type 2 (length-delimited) with a varint that encodes a length
        // large enough that new_pos + len wraps usize — must return Err, not Ok.
        use alloc::vec;
        let mut buf = vec![0u8; 16];
        // Encode varint for usize::MAX - 1 at position 0.
        let big: u64 = (usize::MAX as u64) - 1;
        let mut tmp = Vec::new();
        encode_varint(&mut tmp, big);
        buf[..tmp.len()].copy_from_slice(&tmp);
        // new_pos = tmp.len() (~10); len = usize::MAX-1 → overflows
        let result = skip_field(&buf, 2, 0);
        assert!(result.is_err(), "expected overflow error, got {:?}", result);
    }

    #[test]
    fn test_parse_timestamp() {
        let ts = b"2024-01-15T10:30:00Z";
        let nanos = parse_timestamp_nanos(ts).unwrap();
        // 2024-01-15 10:30:00 UTC
        // Expected: 1705314600 seconds * 1e9
        assert_eq!(nanos, 1_705_314_600_000_000_000);
    }

    #[test]
    fn test_parse_timestamp_fractional() {
        let ts = b"2024-01-15T10:30:00.123Z";
        let nanos = parse_timestamp_nanos(ts).unwrap();
        assert_eq!(nanos, 1_705_314_600_123_000_000);
    }

    #[test]
    fn test_parse_timestamp_nanos_precision() {
        let ts = b"2024-01-15T10:30:00.123456789Z";
        let nanos = parse_timestamp_nanos(ts).unwrap();
        assert_eq!(nanos, 1_705_314_600_123_456_789);
    }

    #[test]
    fn test_parse_timestamp_with_timezone_offset() {
        let plus = b"2024-01-15T10:30:00+02:30";
        let minus = b"2024-01-15T10:30:00-02:30";
        assert_eq!(parse_timestamp_nanos(plus), Some(1_705_305_600_000_000_000));
        assert_eq!(
            parse_timestamp_nanos(minus),
            Some(1_705_323_600_000_000_000)
        );
    }

    #[test]
    fn test_parse_timestamp_rejects_invalid_timezone_suffix() {
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:30:00+0230"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:30:00+02:30Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:30:00"), None);
    }

    #[test]
    fn test_parse_severity() {
        assert!(matches!(parse_severity(b"INFO").0, Severity::Info));
        assert!(matches!(parse_severity(b"info").0, Severity::Info));
        assert!(matches!(parse_severity(b"WARN").0, Severity::Warn));
        assert!(matches!(parse_severity(b"ERROR").0, Severity::Error));
        assert!(matches!(parse_severity(b"DEBUG").0, Severity::Debug));
        assert!(matches!(parse_severity(b"TRACE").0, Severity::Trace));
        assert!(matches!(parse_severity(b"FATAL").0, Severity::Fatal));
        assert!(matches!(
            parse_severity(b"unknown").0,
            Severity::Unspecified
        ));
    }

    #[test]
    fn test_parse_severity_aliases() {
        // WARNING -> Warn (#1866)
        assert!(matches!(parse_severity(b"WARNING").0, Severity::Warn));
        assert!(matches!(parse_severity(b"warning").0, Severity::Warn));
        assert!(matches!(parse_severity(b"Warning").0, Severity::Warn));
        // ERR -> Error (#1912)
        assert!(matches!(parse_severity(b"ERR").0, Severity::Error));
        assert!(matches!(parse_severity(b"err").0, Severity::Error));
        assert!(matches!(parse_severity(b"Err").0, Severity::Error));
        // NOTICE -> Info (#1912)
        assert!(matches!(parse_severity(b"NOTICE").0, Severity::Info));
        assert!(matches!(parse_severity(b"notice").0, Severity::Info));
        assert!(matches!(parse_severity(b"Notice").0, Severity::Info));
        // CRITICAL -> Fatal (#1912)
        assert!(matches!(parse_severity(b"CRITICAL").0, Severity::Fatal));
        assert!(matches!(parse_severity(b"critical").0, Severity::Fatal));
        assert!(matches!(parse_severity(b"Critical").0, Severity::Fatal));
    }

    #[test]
    fn wire_format_varint_known_values() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, 0);
        assert_eq!(buf, [0x00]);

        buf.clear();
        encode_varint(&mut buf, 1);
        assert_eq!(buf, [0x01]);

        buf.clear();
        encode_varint(&mut buf, 300);
        assert_eq!(buf, [0xAC, 0x02]); // protobuf varint for 300

        buf.clear();
        encode_varint(&mut buf, u64::MAX);
        assert_eq!(buf.len(), 10); // max varint is 10 bytes
    }

    #[test]
    fn wire_format_tag_encoding() {
        let mut buf = Vec::new();
        // field 1, wire type 0 (varint) = (1 << 3) | 0 = 8
        encode_tag(&mut buf, 1, 0);
        assert_eq!(buf, [0x08]);

        buf.clear();
        // field 2, wire type 2 (length-delimited) = (2 << 3) | 2 = 18
        encode_tag(&mut buf, 2, 2);
        assert_eq!(buf, [0x12]);
    }

    #[test]
    fn wire_format_bytes_field() {
        let mut buf = Vec::new();
        encode_bytes_field(&mut buf, 1, b"hello");
        // tag(field 1, wire 2) = 0x0A, length = 5, data = "hello"
        assert_eq!(&buf[0..1], &[0x0A]);
        assert_eq!(&buf[1..2], &[0x05]);
        assert_eq!(&buf[2..], b"hello");
    }

    #[test]
    fn days_from_civil_matches_chrono() {
        use chrono::NaiveDate;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        // Test a spread of dates across the valid range
        for (y, m, d) in [
            (1970, 1, 1),
            (1970, 1, 2),
            (1970, 12, 31),
            (2000, 2, 29), // leap day
            (2024, 1, 15),
            (2024, 6, 30),
            (2100, 12, 31),
        ] {
            let our_days = days_from_civil(y, m, d);
            let chrono_days = (NaiveDate::from_ymd_opt(y as i32, m, d).unwrap() - epoch).num_days();
            assert_eq!(
                our_days, chrono_days,
                "mismatch for {y}-{m:02}-{d:02}: ours={our_days}, chrono={chrono_days}"
            );
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config {
            failure_persistence: None,
            ..proptest::test_runner::Config::default()
        })]
        /// `days_from_civil` (Hinnant algorithm) must agree with chrono for
        /// all valid calendar dates in the range 1970–2099.
        #[test]
        fn proptest_days_from_civil_matches_chrono(
            year in 1970i64..=2099,
            month in 1u32..=12,
            day in 1u32..=31,
        ) {
            use chrono::NaiveDate;
            let max_day = days_in_month(year, month);
            proptest::prop_assume!(day <= max_day);

            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = NaiveDate::from_ymd_opt(year as i32, month, day);
            if let Some(d) = date {
                let our = days_from_civil(year, month, day);
                let chrono_days = (d - epoch).num_days();
                proptest::prop_assert_eq!(our, chrono_days,
                    "days_from_civil mismatch");
            }
        }

        /// `parse_timestamp_nanos` must produce the same result as chrono for
        /// valid UTC timestamps in the range 1970–2099.
        #[test]
        fn proptest_parse_timestamp_nanos_matches_chrono(
            year in 1970u32..=2099,
            month in 1u32..=12,
            day in 1u32..=28, // cap at 28 to always be a valid day
            hour in 0u32..=23,
            min in 0u32..=59,
            sec in 0u32..=59,
        ) {
            use chrono::NaiveDateTime;
            let s = alloc::format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z"
            );
            let our = parse_timestamp_nanos(s.as_bytes());
            let chrono = NaiveDateTime::parse_from_str(
                    s.trim_end_matches('Z'), "%Y-%m-%dT%H:%M:%S"
                )
                .ok()
                .and_then(|dt| dt.and_utc().timestamp_nanos_opt())
                .map(|n| n as u64);
            proptest::prop_assert_eq!(our, chrono,
                "mismatch for timestamp");
        }
    }

    #[test]
    fn parse_timestamp_matches_chrono() {
        use chrono::NaiveDateTime;
        let cases = [
            b"2024-01-15T10:30:00Z" as &[u8],
            b"2000-02-29T00:00:00Z",
            b"2099-12-31T23:59:59Z",
        ];
        for ts in cases {
            let our_nanos = parse_timestamp_nanos(ts).unwrap();
            let s = core::str::from_utf8(ts).unwrap().trim_end_matches('Z');
            let chrono_nanos = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                .unwrap()
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap() as u64;
            assert_eq!(
                our_nanos, chrono_nanos,
                "timestamp mismatch for {s}: ours={our_nanos}, chrono={chrono_nanos}"
            );
        }
    }

    #[test]
    fn parse_timestamp_epoch_returns_zero() {
        // Unix epoch (1970-01-01T00:00:00Z) returns 0, which is also
        // the sentinel for "parse failed". This is a known limitation
        // documented in the audit — callers use observed_time as fallback.
        assert_eq!(parse_timestamp_nanos(b"1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn parse_timestamp_rejects_non_digit_timezone() {
        // Regression: non-digit bytes in timezone offset must be rejected (#1467).
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:30:00+0a:30"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:30:00+02:3x"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:30:00+ab:cd"), None);
        // Valid offsets should still work.
        assert!(parse_timestamp_nanos(b"2024-01-15T10:30:00+05:30").is_some());
        assert!(parse_timestamp_nanos(b"2024-01-15T10:30:00-08:00").is_some());
    }

    #[test]
    fn parse_timestamp_invalid_returns_zero() {
        assert_eq!(parse_timestamp_nanos(b"not a timestamp"), None);
        assert_eq!(parse_timestamp_nanos(b""), None);
        assert_eq!(parse_timestamp_nanos(b"2024"), None);
    }

    #[test]
    fn test_parse_timestamp_nanos_invalid_time() {
        // Bug #1047: Invalid time of day should return None
        assert_eq!(parse_timestamp_nanos(b"2024-01-01T99:99:99Z"), None);
        // Invalid hour
        assert_eq!(parse_timestamp_nanos(b"2024-01-01T24:00:00Z"), None);
        // Invalid minute
        assert_eq!(parse_timestamp_nanos(b"2024-01-01T23:60:00Z"), None);
        // Invalid second (leap second 60 is allowed, 61 is not)
        assert_eq!(parse_timestamp_nanos(b"2024-01-01T23:59:61Z"), None);
    }

    #[test]
    fn parse_timestamp_rejects_invalid_calendar_dates() {
        assert_eq!(parse_timestamp_nanos(b"2024-02-31T00:00:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-02-30T00:00:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2023-02-29T00:00:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-04-31T00:00:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-06-31T00:00:00Z"), None);
        assert!(parse_timestamp_nanos(b"2024-02-29T00:00:00Z").is_some());
        assert_eq!(parse_timestamp_nanos(b"1900-02-29T00:00:00Z"), None);
        assert!(parse_timestamp_nanos(b"2000-02-29T00:00:00Z").is_some());
    }

    #[test]
    fn parse_timestamp_rejects_non_digit_time_fields() {
        assert_eq!(parse_timestamp_nanos(b"2024-01-15TXX:30:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:XX:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10:30:XXZ"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-XX-15T10:30:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-XXT10:30:00Z"), None);
    }

    #[test]
    fn parse_timestamp_rejects_bad_separators() {
        assert_eq!(parse_timestamp_nanos(b"2024/01/15T10:30:00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15T10-30-00Z"), None);
        assert_eq!(parse_timestamp_nanos(b"2024-01-15X10:30:00Z"), None);
    }

    #[test]
    fn wire_format_fixed32() {
        let mut buf = Vec::new();
        // field 8, wire type 5 = (8 << 3) | 5 = 0x45
        encode_fixed32(&mut buf, 8, 0x01000000);
        assert_eq!(buf[0], 0x45); // tag
        assert_eq!(&buf[1..], &[0x00, 0x00, 0x00, 0x01]); // little-endian
    }

    #[test]
    fn hex_decode_trace_id() {
        // 32 hex chars → 16 bytes
        let hex = b"0102030405060708090a0b0c0d0e0f10";
        let mut out = [0u8; 16];
        assert!(hex_decode(hex, &mut out));
        assert_eq!(
            out,
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10
            ]
        );
    }

    #[test]
    fn hex_decode_span_id() {
        // 16 hex chars → 8 bytes
        let hex = b"0102030405060708";
        let mut out = [0u8; 8];
        assert!(hex_decode(hex, &mut out));
        assert_eq!(out, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn hex_decode_uppercase() {
        let hex = b"AABBCCDDEEFF0011";
        let mut out = [0u8; 8];
        assert!(hex_decode(hex, &mut out));
        assert_eq!(out, [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11]);
    }

    #[test]
    fn hex_decode_wrong_length_fails() {
        let mut out = [0u8; 8];
        assert!(!hex_decode(b"0102", &mut out)); // too short
        assert!(!hex_decode(b"010203040506070809", &mut out)); // too long
    }

    #[test]
    fn hex_decode_invalid_char_fails() {
        let mut out = [0u8; 4];
        assert!(!hex_decode(b"0102030G", &mut out)); // 'G' is invalid
        assert!(!hex_decode(b"01 20304", &mut out)); // space is invalid
    }

    /// Oracle: the reference (branch-based) nibble decoder used to verify the
    /// LUT implementation. Delegates to the centralized oracle in ffwd-kani.
    fn hex_nibble_oracle(c: u8) -> u8 {
        ffwd_kani::hex::hex_nibble_oracle(c)
    }

    /// Oracle: reference `hex_decode` implementation. Delegates to the
    /// centralized oracle in ffwd-kani.
    fn hex_decode_oracle(hex_bytes: &[u8], out: &mut [u8]) -> bool {
        ffwd_kani::hex::hex_decode_oracle(hex_bytes, out)
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config {
            failure_persistence: None,
            ..proptest::test_runner::Config::default()
        })]
        /// LUT nibble lookup must agree with the branch-based oracle for every
        /// possible byte value (0x00–0xFF).
        #[test]
        fn proptest_hex_nibble_lut_matches_oracle(b: u8) {
            proptest::prop_assert_eq!(
                HEX_NIBBLE_LUT[b as usize],
                hex_nibble_oracle(b),
                "LUT[{:#04x}] != oracle({:#04x})",
                b, b,
            );
        }

        /// LUT-based `hex_decode` must agree with the oracle on every possible
        /// input byte sequence of lengths 0, 2, 4, 8, 16, and 32.
        ///
        /// Covers: valid hex, invalid chars, mixed case, and arbitrary garbage.
        #[test]
        fn proptest_hex_decode_lut_matches_oracle(
            input in proptest::collection::vec(proptest::num::u8::ANY, 0..=64),
        ) {
            // Test all output sizes that are representable as hex strings of
            // the given input length.
            for out_len in [0usize, 1, 2, 4, 8, 16] {
                let expected_input_len = out_len * 2;
                if input.len() < expected_input_len {
                    continue;
                }
                let hex = &input[..expected_input_len];
                let mut got = alloc::vec![0u8; out_len];
                let mut expected = alloc::vec![0u8; out_len];
                let got_ok = hex_decode(hex, &mut got);
                let expected_ok = hex_decode_oracle(hex, &mut expected);
                proptest::prop_assert_eq!(got_ok, expected_ok,
                    "return value mismatch for input {:?}", hex);
                if got_ok {
                    proptest::prop_assert_eq!(&got, &expected,
                        "output mismatch for input {:?}", hex);
                }
            }
        }
    }

    /// Spot-check protobuf field number constants against the OTLP proto spec.
    /// Catches drift without requiring the Kani toolchain.
    #[test]
    fn field_constants_spot_check() {
        // Wire types (protobuf spec).
        assert_eq!(WIRE_TYPE_VARINT, 0);
        assert_eq!(WIRE_TYPE_FIXED64, 1);
        assert_eq!(WIRE_TYPE_LEN, 2);
        assert_eq!(WIRE_TYPE_FIXED32, 5);

        // LogRecord fields (logs.proto).
        assert_eq!(LOG_RECORD_TIME_UNIX_NANO, 1);
        assert_eq!(LOG_RECORD_SEVERITY_NUMBER, 2);
        assert_eq!(LOG_RECORD_SEVERITY_TEXT, 3);
        assert_eq!(LOG_RECORD_BODY, 5);
        assert_eq!(LOG_RECORD_ATTRIBUTES, 6);
        assert_eq!(LOG_RECORD_FLAGS, 8);
        assert_eq!(LOG_RECORD_TRACE_ID, 9);
        assert_eq!(LOG_RECORD_SPAN_ID, 10);
        assert_eq!(LOG_RECORD_OBSERVED_TIME_UNIX_NANO, 11);

        // AnyValue fields (common.proto).
        assert_eq!(ANY_VALUE_STRING_VALUE, 1);
        assert_eq!(ANY_VALUE_BOOL_VALUE, 2);
        assert_eq!(ANY_VALUE_INT_VALUE, 3);
        assert_eq!(ANY_VALUE_DOUBLE_VALUE, 4);
        assert_eq!(ANY_VALUE_ARRAY_VALUE, 5);
        assert_eq!(ANY_VALUE_KVLIST_VALUE, 6);
        assert_eq!(ANY_VALUE_BYTES_VALUE, 7);

        // KeyValue fields (common.proto).
        assert_eq!(KEY_VALUE_KEY, 1);
        assert_eq!(KEY_VALUE_VALUE, 2);

        // Message nesting fields.
        assert_eq!(EXPORT_LOGS_REQUEST_RESOURCE_LOGS, 1);
        assert_eq!(RESOURCE_LOGS_RESOURCE, 1);
        assert_eq!(RESOURCE_LOGS_SCOPE_LOGS, 2);
        assert_eq!(RESOURCE_ATTRIBUTES, 1);
        assert_eq!(SCOPE_LOGS_SCOPE, 1);
        assert_eq!(SCOPE_LOGS_LOG_RECORDS, 2);
        assert_eq!(INSTRUMENTATION_SCOPE_NAME, 1);
        assert_eq!(INSTRUMENTATION_SCOPE_VERSION, 2);
    }
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;
    use alloc::{vec, vec::Vec};
    use ffwd_kani::proto::decode_varint_oracle;

    // NOTE: encode_varint and encode_tag take `&mut Vec<u8>` and return `()`.
    // In our current Kani version/configuration used in CI, the contract system
    // (requires/ensures/modifies) requires the modified type to implement
    // `kani::Arbitrary` for stub_verified, but `Vec<u8>` does not in that setup.
    // Function contracts for these wire format helpers are therefore deferred for
    // now, and the correctness of these functions is instead proven
    // exhaustively by the verify_varint_* and verify_encode_tag proofs below.

    /// Prove varint_len matches encode_varint output length for ALL u64 values.
    ///
    /// This is the foundational wire format proof — if these disagree,
    /// protobuf message size calculations are wrong and payloads are corrupt.
    #[kani::proof]
    #[kani::unwind(12)] // varint loop: max 10 iterations + overhead
    #[kani::solver(kissat)]
    pub(super) fn verify_varint_len_matches_encode() {
        let value: u64 = kani::any();
        let mut buf = Vec::with_capacity(10); // varint max 10 bytes — no realloc paths
        encode_varint(&mut buf, value);
        assert!(
            buf.len() == varint_len(value),
            "varint_len disagrees with encode_varint"
        );
    }

    /// Oracle equivalence: varint_len matches varint_len_oracle for ALL u64 values.
    ///
    /// Two different algorithms are compared: the production `varint_len` uses
    /// a match-with-ranges, while `varint_len_oracle` uses a count-the-shifts loop.
    /// This proof guarantees they produce identical results for all 2^64 inputs.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::solver(kissat)]
    pub(super) fn verify_varint_len_vs_oracle() {
        let value: u64 = kani::any();
        assert_eq!(
            varint_len(value),
            ffwd_kani::proto::varint_len_oracle(value),
            "varint_len disagrees with varint_len_oracle"
        );
    }

    /// Oracle equivalence: tag_size matches tag_size_oracle for all field numbers.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::solver(kissat)]
    pub(super) fn verify_tag_size_vs_oracle() {
        let field_number: u32 = kani::any();
        kani::assume(field_number > 0 && field_number <= 0x1FFFFFFF);
        kani::cover!(field_number == 1, "min valid protobuf field number");
        kani::cover!(
            field_number == 0x1FFFFFFF,
            "max valid protobuf field number"
        );
        assert_eq!(
            tag_size(field_number),
            ffwd_kani::proto::tag_size_oracle(field_number),
            "tag_size disagrees with tag_size_oracle"
        );
    }

    /// Oracle equivalence: bytes_field_total_size matches bytes_field_total_size_oracle.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::solver(kissat)]
    pub(super) fn verify_bytes_field_total_size_vs_oracle() {
        let field_number: u32 = kani::any();
        let data_len: usize = kani::any();
        kani::assume(field_number > 0 && field_number <= 0x1FFFFFFF);
        kani::cover!(data_len == 0, "empty payload reachable");
        kani::cover!(data_len > 0, "non-empty payload reachable");
        assert_eq!(
            bytes_field_total_size(field_number, data_len),
            ffwd_kani::proto::bytes_field_total_size_oracle(field_number, data_len),
            "bytes_field_total_size disagrees with oracle"
        );
    }

    /// Prove encode_varint produces valid protobuf varint format for ALL u64.
    ///
    /// Properties:
    /// 1. Length is 1-10 bytes
    /// 2. All bytes except the last have the continuation bit (0x80) set
    /// 3. The last byte does NOT have the continuation bit set
    /// 4. Decoding the varint gives back the original value
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::solver(kissat)]
    fn verify_varint_format_and_roundtrip() {
        let value: u64 = kani::any();
        let mut buf = Vec::with_capacity(10); // varint max 10 bytes — no realloc paths
        encode_varint(&mut buf, value);

        let len = buf.len();

        // Property 1: length is 1-10
        assert!(len >= 1 && len <= 10, "varint length out of range");

        // Property 2: all bytes except last have continuation bit
        let mut i = 0;
        while i < len - 1 {
            assert!(buf[i] & 0x80 != 0, "non-last byte missing continuation bit");
            i += 1;
        }

        // Property 3: last byte has no continuation bit
        assert!(buf[len - 1] & 0x80 == 0, "last byte has continuation bit");

        // Property 4: decode roundtrip
        let mut decoded: u64 = 0;
        let mut shift: u32 = 0;
        let mut j = 0;
        while j < len {
            let byte = buf[j] as u64;
            decoded |= (byte & 0x7F) << shift;
            shift += 7;
            j += 1;
        }
        assert!(decoded == value, "varint roundtrip mismatch");
    }

    /// Prove encode_varint handles the boundary values where encoded length changes.
    ///
    /// The exhaustive all-`u64` behavior is already covered by
    /// `verify_varint_len_matches_encode` and `verify_varint_format_and_roundtrip`.
    #[kani::proof]
    #[kani::solver(kissat)] // arithmetic-heavy varint bit ops: kissat outperforms cadical
    fn verify_varint_no_panic() {
        for value in [0, 1, 0x7F, 0x80, 0x3FFF, 0x4000, u64::MAX] {
            let mut buf = Vec::with_capacity(10); // varint max 10 bytes — no realloc paths
            encode_varint(&mut buf, value);
            assert!(buf.len() <= 10);
        }
    }

    /// Prove encode_tag produces correct field_number and wire_type encoding.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::solver(kissat)] // arithmetic-heavy varint bit ops: kissat outperforms cadical
    pub(super) fn verify_encode_tag() {
        let field_number: u32 = kani::any();
        let wire_type: u8 = kani::any();
        kani::assume(field_number > 0);
        kani::assume(field_number <= 0x1FFFFFFF); // max protobuf field number
        kani::assume(wire_type <= 5); // valid wire types: 0-5

        let mut buf = Vec::with_capacity(10); // tag varint max 10 bytes — no realloc paths
        encode_tag(&mut buf, field_number, wire_type);

        // Decode the tag varint
        let mut tag_value: u64 = 0;
        let mut shift: u32 = 0;
        let mut i = 0;
        while i < buf.len() {
            let byte = buf[i] as u64;
            tag_value |= (byte & 0x7F) << shift;
            shift += 7;
            i += 1;
        }

        // Verify field_number and wire_type
        let decoded_wire = (tag_value & 0x7) as u8;
        let decoded_field = (tag_value >> 3) as u32;
        assert!(decoded_wire == wire_type, "wire type mismatch");
        assert!(decoded_field == field_number, "field number mismatch");

        // Confirm both extremes of the constrained space are reachable
        kani::cover!(field_number == 1 && wire_type == 0);
        kani::cover!(field_number == 0x1FFFFFFF && wire_type == 5);
    }

    /// Oracle proof: days_from_civil matches a closed-form Julian Day
    /// Number calculation for bounded date components in [1970, 2100].
    ///
    /// Uses the Fliegel-Van Flandern JDN formula (a completely different
    /// algorithm from the Hinnant formula in days_from_civil) as the
    /// Kani-compatible oracle. Both are pure arithmetic with no loops,
    /// so no unwind bound is needed.
    ///
    /// Note: this proof range includes non-calendar-valid day-of-month
    /// combinations (e.g. Feb 31); the oracle and implementation agree
    /// on all such inputs regardless.
    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_days_from_civil_oracle() {
        let year: i64 = kani::any();
        let month: u32 = kani::any();
        let day: u32 = kani::any();

        kani::assume(year >= 1970 && year <= 2100);
        kani::assume(month >= 1 && month <= 12);
        kani::assume(day >= 1 && day <= 31);

        kani::cover!(year == 1970 && month == 1 && day == 1, "epoch start");
        kani::cover!(year == 2100 && month == 12 && day == 31, "upper boundary");
        kani::cover!(year == 2000 && month == 2 && day == 29, "leap-year Feb 29");
        kani::cover!(year == 1971 && month == 6 && day == 15, "typical mid-range");

        let result = days_from_civil(year, month, day);
        let oracle = ffwd_kani::datetime::jdn_days_from_epoch(year, month, day);
        assert!(
            result == oracle,
            "days_from_civil disagrees with JDN oracle"
        );
    }

    /// Prove bytes_field_size matches actual encode_bytes_field output for the
    /// tag-length and length-varint boundary classes that determine the size.
    #[kani::proof]
    #[kani::solver(kissat)]
    pub(super) fn verify_bytes_field_size() {
        // Fixed array sliced to data_len — no dynamic allocation, no realloc paths.
        // Output buf pre-sized to tag varint (10) + length varint (10) + data (256) = 276 max.
        let data = [0u8; 256];
        for (field_number, data_len) in [
            (1, 0),
            (15, 127),
            (16, 128),
            (2_047, 255),
            (2_048, 256),
            (262_143, 1),
            (262_144, 127),
            (33_554_431, 128),
            (0x1FFF_FFFF, 0),
        ] {
            let predicted = bytes_field_size(field_number, data_len);
            let mut buf = Vec::with_capacity(276);
            encode_bytes_field(&mut buf, field_number, &data[..data_len]);

            assert!(
                buf.len() == predicted,
                "bytes_field_size disagrees with encode_bytes_field"
            );
        }
    }

    // NOTE: parse_timestamp_nanos proofs deferred — Kani has trouble with
    // div_euclid/rem_euclid in days_from_civil and large u64 multiplications
    // (nanos *= 1_000_000_000). The timestamp functions are better verified
    // with proptest oracle against chrono. Tracked in #268.

    /// Prove parse_severity never panics for any 8-byte input and
    /// returns correct severity for known level strings.
    #[kani::proof]
    fn verify_parse_severity_no_panic() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let _ = parse_severity(&bytes[..len]);
    }

    /// Prove parse_severity correctly classifies all standard level strings.
    #[kani::proof]
    fn verify_parse_severity_known_values() {
        // Uppercase
        assert!(matches!(parse_severity(b"INFO").0, Severity::Info));
        assert!(matches!(parse_severity(b"WARN").0, Severity::Warn));
        assert!(matches!(parse_severity(b"ERROR").0, Severity::Error));
        assert!(matches!(parse_severity(b"DEBUG").0, Severity::Debug));
        assert!(matches!(parse_severity(b"TRACE").0, Severity::Trace));
        assert!(matches!(parse_severity(b"FATAL").0, Severity::Fatal));

        // Lowercase
        assert!(matches!(parse_severity(b"info").0, Severity::Info));
        assert!(matches!(parse_severity(b"warn").0, Severity::Warn));
        assert!(matches!(parse_severity(b"error").0, Severity::Error));
        assert!(matches!(parse_severity(b"debug").0, Severity::Debug));
        assert!(matches!(parse_severity(b"trace").0, Severity::Trace));
        assert!(matches!(parse_severity(b"fatal").0, Severity::Fatal));

        // Aliases -- uppercase and lowercase (#1866, #1912)
        assert!(matches!(parse_severity(b"ERR").0, Severity::Error));
        assert!(matches!(parse_severity(b"err").0, Severity::Error));
        assert!(matches!(parse_severity(b"NOTICE").0, Severity::Info));
        assert!(matches!(parse_severity(b"notice").0, Severity::Info));
        assert!(matches!(parse_severity(b"WARNING").0, Severity::Warn));
        assert!(matches!(parse_severity(b"warning").0, Severity::Warn));
        assert!(matches!(parse_severity(b"CRITICAL").0, Severity::Fatal));
        assert!(matches!(parse_severity(b"critical").0, Severity::Fatal));

        // Empty / unknown
        assert!(matches!(parse_severity(b"").0, Severity::Unspecified));
        assert!(matches!(parse_severity(b"X").0, Severity::Unspecified));
    }

    /// Prove parse_severity handles mixed case and rejects false positives.
    #[kani::proof]
    fn verify_parse_severity_mixed_case() {
        // Mixed case matches (|0x20 folds to lowercase)
        assert!(matches!(parse_severity(b"Info").0, Severity::Info));
        assert!(matches!(parse_severity(b"Warn").0, Severity::Warn));
        assert!(matches!(parse_severity(b"Error").0, Severity::Error));
        assert!(matches!(parse_severity(b"Debug").0, Severity::Debug));
        assert!(matches!(parse_severity(b"Trace").0, Severity::Trace));
        assert!(matches!(parse_severity(b"Fatal").0, Severity::Fatal));

        // Mixed case aliases
        assert!(matches!(parse_severity(b"Err").0, Severity::Error));
        assert!(matches!(parse_severity(b"Notice").0, Severity::Info));
        assert!(matches!(parse_severity(b"Warning").0, Severity::Warn));
        assert!(matches!(parse_severity(b"Critical").0, Severity::Fatal));

        // Exact length required — no prefix matching
        assert!(matches!(
            parse_severity(b"INFORMATION").0,
            Severity::Unspecified
        ));
        assert!(matches!(parse_severity(b"TRAMP").0, Severity::Unspecified));
        assert!(matches!(parse_severity(b"INF").0, Severity::Unspecified));
    }

    /// Prove parse_severity ONLY returns non-Unspecified for the 10
    /// recognized level strings (6 standard + 4 aliases, any case).
    /// No false positives.
    #[kani::proof]
    #[kani::unwind(9)] // eq_ignore_case_match: Zip over ≤8-byte targets + 1 terminator
    pub(super) fn verify_parse_severity_no_false_positives() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= 8);
        let text = &bytes[..len];
        let (sev, _) = parse_severity(text);

        if !matches!(sev, Severity::Unspecified) {
            // Must be exactly one of the 10 recognized levels
            assert!(
                eq_ignore_case_match(text, b"TRACE")
                    || eq_ignore_case_match(text, b"DEBUG")
                    || eq_ignore_case_match(text, b"INFO")
                    || eq_ignore_case_match(text, b"WARN")
                    || eq_ignore_case_match(text, b"ERROR")
                    || eq_ignore_case_match(text, b"FATAL")
                    || eq_ignore_case_match(text, b"ERR")
                    || eq_ignore_case_match(text, b"NOTICE")
                    || eq_ignore_case_match(text, b"WARNING")
                    || eq_ignore_case_match(text, b"CRITICAL"),
                "matched a non-standard level"
            );
        }
    }

    /// Prove parse_2digits and parse_4digits never panic for any input.
    #[kani::proof]
    fn verify_digit_parsers_no_panic() {
        let bytes: [u8; 8] = kani::any();
        let off: usize = kani::any();
        kani::assume(off <= 6);

        let _ = parse_2digits(&bytes, off);
        let _ = parse_4digits(&bytes, off);
    }

    /// Prove parse_2digits returns correct value for valid digit pairs.
    #[kani::proof]
    fn verify_parse_2digits_correct() {
        let a: u8 = kani::any();
        let b: u8 = kani::any();
        kani::assume(a >= b'0' && a <= b'9');
        kani::assume(b >= b'0' && b <= b'9');
        let bytes = [a, b];
        let result = parse_2digits(&bytes, 0);
        let expected = (a - b'0') * 10 + (b - b'0');
        assert!(result == expected, "parse_2digits value mismatch");
    }

    /// Prove parse_4digits returns correct value for valid digit quads.
    #[kani::proof]
    fn verify_parse_4digits_correct() {
        let a: u8 = kani::any();
        let b: u8 = kani::any();
        let c: u8 = kani::any();
        let d: u8 = kani::any();
        kani::assume(a >= b'0' && a <= b'9');
        kani::assume(b >= b'0' && b <= b'9');
        kani::assume(c >= b'0' && c <= b'9');
        kani::assume(d >= b'0' && d <= b'9');
        let bytes = [a, b, c, d];
        let result = parse_4digits(&bytes, 0);
        let expected = (a - b'0') as u16 * 1000
            + (b - b'0') as u16 * 100
            + (c - b'0') as u16 * 10
            + (d - b'0') as u16;
        assert!(result == expected, "parse_4digits value mismatch");
    }

    /// Prove encode_fixed64 produces exactly tag + 8 LE bytes.
    #[kani::proof]
    #[kani::unwind(12)]
    pub(super) fn verify_encode_fixed64() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::new();
        encode_fixed64(&mut buf, field_number, value);

        // Tag + 8 bytes
        let tag_val = ((field_number as u64) << 3) | 1;
        let tag_len = varint_len(tag_val);
        assert!(buf.len() == tag_len + 8, "fixed64 size wrong");

        // Verify tag bytes encode correct field_number + wire_type
        let mut tag_buf = Vec::new();
        encode_varint(&mut tag_buf, tag_val);
        let mut i = 0;
        while i < tag_len {
            assert!(buf[i] == tag_buf[i], "tag byte mismatch");
            i += 1;
        }

        // Last 8 bytes are the value in little-endian
        let val_bytes = &buf[tag_len..];
        let decoded = u64::from_le_bytes(val_bytes.try_into().unwrap());
        assert!(decoded == value, "fixed64 value mismatch");
    }

    /// Prove encode_varint_field produces tag + varint value of the predicted size.
    ///
    /// Byte-level correctness of the individual tag and value encodings is already
    /// established by verify_encode_tag and verify_varint_format_and_roundtrip;
    /// this proof checks the compositional size property and uses a single
    /// Vec with pre-allocated capacity to keep VCC counts under budget.
    ///
    /// NOTE: Do NOT add roundtrip decode loops here — two nested symbolic varint
    /// decode loops over a full u64 value cause SAT solver timeouts on CI runners
    /// (>60 min). The roundtrip property is already proven by the component proofs.
    #[kani::solver(kissat)]
    #[kani::proof]
    #[kani::unwind(12)]
    pub(super) fn verify_encode_varint_field() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::with_capacity(20); // tag max 5 bytes + value max 10 bytes + margin
        encode_varint_field(&mut buf, field_number, value);

        let tag_len = varint_len(((field_number as u64) << 3) | 0);
        let val_len = varint_len(value);
        assert!(buf.len() == tag_len + val_len, "varint_field size wrong");
    }

    /// Prove parse_timestamp_nanos never panics for any 32-byte input.
    ///
    /// Uses stub_verified for proven sub-functions to avoid re-verifying
    /// digit parsing and calendar arithmetic inline.
    #[kani::proof]
    #[kani::stub_verified(parse_4digits)]
    #[kani::stub_verified(parse_2digits)]
    #[kani::stub_verified(days_from_civil)]
    #[kani::unwind(14)] // fractional while loop: up to 12 iters (len=32, frac_start=20) + 2 margin
    #[kani::solver(kissat)]
    fn verify_parse_timestamp_no_panic() {
        let bytes: [u8; 32] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= 32);
        let _ = parse_timestamp_nanos(&bytes[..len]);
    }

    /// Prove parse_timestamp_nanos returns correct values for known dates.
    #[kani::proof]
    fn verify_parse_timestamp_known_dates() {
        // 2024-01-15T10:30:00Z = 1705314600 seconds
        let ts = b"2024-01-15T10:30:00Z____extra___";
        let nanos = parse_timestamp_nanos(&ts[..20]);
        assert!(nanos == Some(1_705_314_600_000_000_000));

        // Unix epoch returns 0 (sentinel — documented limitation)
        let epoch = b"1970-01-01T00:00:00Z____________";
        assert!(parse_timestamp_nanos(&epoch[..20]) == Some(0));

        // Pre-epoch returns 0
        let pre = b"1969-12-31T23:59:59Z____________";
        assert!(parse_timestamp_nanos(&pre[..20]) == None);
    }

    /// Prove encode_bytes_field content correctness: tag + length + exact data.
    #[kani::solver(kissat)]
    #[kani::proof]
    #[kani::unwind(12)]
    pub(super) fn verify_encode_bytes_field_content() {
        let field_number: u32 = kani::any();
        kani::assume(field_number > 0 && field_number <= 100);
        let data_len: usize = kani::any_where(|&l: &usize| l <= 8);
        let data: [u8; 8] = kani::any();

        let mut buf = Vec::with_capacity(30); // tag ≤5 + length varint ≤2 + data ≤8 + margin
        encode_bytes_field(&mut buf, field_number, &data[..data_len]);

        // Size must match prediction
        assert!(buf.len() == bytes_field_size(field_number, data_len));

        // Last data_len bytes must be the exact input data
        let payload = &buf[buf.len() - data_len..];
        let mut i = 0;
        while i < data_len {
            assert!(payload[i] == data[i], "data mismatch at byte");
            i += 1;
        }
    }

    /// Verify parse_4digits contract: output ≤ 9999.
    #[kani::proof_for_contract(parse_4digits)]
    #[kani::unwind(5)]
    pub(super) fn verify_parse_4digits_contract() {
        let s: [u8; 8] = kani::any();
        let off: usize = kani::any_where(|&o: &usize| o <= 4);
        parse_4digits(&s, off);
    }

    /// Verify parse_2digits contract: output ≤ 99.
    #[kani::proof_for_contract(parse_2digits)]
    #[kani::unwind(3)]
    pub(super) fn verify_parse_2digits_contract() {
        let s: [u8; 8] = kani::any();
        let off: usize = kani::any_where(|&o: &usize| o <= 6);
        parse_2digits(&s, off);
    }

    /// Verify days_from_civil contract over the full input domain declared by requires:
    /// year ∈ [1, 2553], month ∈ [1, 12], day ∈ [1, 31].
    /// Covers both pre-1970 (result < 0) and post-1970 (result ∈ [-366, 213_400]).
    #[kani::proof_for_contract(days_from_civil)]
    pub(super) fn verify_days_from_civil_contract() {
        let year: i64 = kani::any();
        let month: u32 = kani::any();
        let day: u32 = kani::any();
        // Kani uses the function's #[kani::requires] to constrain inputs automatically;
        // no manual assumes needed.
        days_from_civil(year, month, day);
    }

    /// Compositional proof: parse_timestamp_nanos using proven sub-functions.
    /// Instead of re-verifying digit parsing and calendar arithmetic,
    /// Kani trusts their contracts (already proven above) and focuses on
    /// the composition logic: field extraction, validation, and nanos math.
    #[kani::proof]
    #[kani::stub_verified(parse_4digits)]
    #[kani::stub_verified(parse_2digits)]
    #[kani::stub_verified(days_from_civil)]
    #[kani::solver(kissat)]
    #[kani::unwind(12)]
    pub(super) fn verify_parse_timestamp_compositional() {
        let ts: [u8; 24] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l >= 19 && l <= 24);
        let result = parse_timestamp_nanos(&ts[..len]);

        // If it parsed successfully, the result must be bounded.
        // Upper bound: days ≤ 213_400 (contract), hour/min/sec ≤ 99 (parse_2digits contract),
        // frac < 1_000_000_000. parse_timestamp_nanos does not validate hour/min/sec ranges.
        // Overflow safety is checked automatically by Kani's arithmetic instrumentation.
        if let Some(nanos) = result {
            const MAX_NANOS: u64 = 18_438_122_439_999_999_999;
            assert!(nanos <= MAX_NANOS);
        }
    }

    /// Prove parse_timestamp_nanos validates month/day ranges.
    #[kani::proof]
    fn verify_parse_timestamp_rejects_invalid_dates() {
        // Month 0
        let ts = b"2024-00-15T10:30:00Z";
        assert!(parse_timestamp_nanos(ts) == None);

        // Month 13
        let ts = b"2024-13-15T10:30:00Z";
        assert!(parse_timestamp_nanos(ts) == None);

        // Day 0
        let ts = b"2024-01-00T10:30:00Z";
        assert!(parse_timestamp_nanos(ts) == None);

        // Day 32
        let ts = b"2024-01-32T10:30:00Z";
        assert!(parse_timestamp_nanos(ts) == None);
    }

    /// Prove eq_ignore_case_4 agrees with eq_ignore_case_match for INFO.
    ///
    /// **Oracle limitation:** eq_ignore_case_match also uses `|0x20`, so this
    /// proof cannot detect non-letter collisions (e.g., `@` vs `` ` ``).
    /// The exhaustive parse_severity proof (verify_parse_severity_no_false_positives)
    /// already covers the real-world safety of eq_ignore_case_4 by checking
    /// that only valid severity strings produce a match. This proof is kept
    /// as a structural consistency check between eq_ignore_case_4 and the oracle.
    #[kani::proof]
    #[kani::unwind(5)] // eq_ignore_case_match: Zip over 4-byte slice + 1 terminator
    pub(super) fn verify_eq_ignore_case_4_no_false_positives_info() {
        let input: [u8; 4] = kani::any();
        let target = b"INFO";
        if eq_ignore_case_4(&input, target) {
            assert!(eq_ignore_case_match(&input, target));
        }
    }

    /// Same for 5-byte targets — proves eq_ignore_case_5 agrees with oracle.
    ///
    /// **Oracle limitation:** same as above — eq_ignore_case_match uses `|0x20`
    /// and cannot detect non-letter collisions. Redundant with
    /// verify_parse_severity_no_false_positives which exhaustively validates
    /// that parse_severity only matches valid severity level strings.
    #[kani::proof]
    #[kani::unwind(6)] // eq_ignore_case_match: Zip over 5-byte slice + 1 terminator
    pub(super) fn verify_eq_ignore_case_5_no_false_positives_error() {
        let input: [u8; 5] = kani::any();
        let target = b"ERROR";
        if eq_ignore_case_5(&input, target) {
            assert!(eq_ignore_case_match(&input, target));
        }
    }

    /// Prove eq_ignore_case_6 agrees with eq_ignore_case_match for NOTICE.
    #[kani::proof]
    #[kani::unwind(7)] // eq_ignore_case_match: Zip over 6-byte slice + 1 terminator
    pub(super) fn verify_eq_ignore_case_6_no_false_positives_notice() {
        let input: [u8; 6] = kani::any();
        let target = b"NOTICE";
        if eq_ignore_case_6(&input, target) {
            assert!(eq_ignore_case_match(&input, target));
        }
    }

    /// Prove eq_ignore_case_7 agrees with eq_ignore_case_match for WARNING.
    #[kani::proof]
    #[kani::unwind(8)] // eq_ignore_case_match: Zip over 7-byte slice + 1 terminator
    pub(super) fn verify_eq_ignore_case_7_no_false_positives_warning() {
        let input: [u8; 7] = kani::any();
        let target = b"WARNING";
        if eq_ignore_case_7(&input, target) {
            assert!(eq_ignore_case_match(&input, target));
        }
    }

    /// Prove eq_ignore_case_8 agrees with eq_ignore_case_match for CRITICAL.
    #[kani::proof]
    #[kani::unwind(9)] // eq_ignore_case_match: Zip over 8-byte slice + 1 terminator
    pub(super) fn verify_eq_ignore_case_8_no_false_positives_critical() {
        let input: [u8; 8] = kani::any();
        let target = b"CRITICAL";
        if eq_ignore_case_8(&input, target) {
            assert!(eq_ignore_case_match(&input, target));
        }
    }

    /// Prove hex_decode roundtrip: for any 16-byte array, hex-encoding then
    /// decoding yields the original bytes.
    #[kani::proof]
    #[kani::unwind(17)] // 16 bytes + 1
    pub(super) fn verify_hex_decode_roundtrip() {
        let original: [u8; 16] = kani::any();
        // Hex-encode
        let mut hex = [0u8; 32];
        for i in 0..16 {
            let hi = original[i] >> 4;
            let lo = original[i] & 0x0F;
            hex[2 * i] = if hi < 10 { b'0' + hi } else { b'a' + hi - 10 };
            hex[2 * i + 1] = if lo < 10 { b'0' + lo } else { b'a' + lo - 10 };
        }
        // Decode back
        let mut decoded = [0u8; 16];
        assert!(hex_decode(&hex, &mut decoded));
        assert_eq!(original, decoded);
    }

    /// Prove HEX_NIBBLE_LUT returns the correct value for all 256 byte inputs:
    /// valid hex digits map to 0x00..=0x0F, everything else maps to 0xFF.
    #[kani::proof]
    fn verify_hex_nibble_valid_range() {
        let b: u8 = kani::any();
        let result = HEX_NIBBLE_LUT[b as usize];
        if result <= 0x0F {
            // Valid hex digit — verify correctness
            match b {
                b'0'..=b'9' => assert_eq!(result, b - b'0'),
                b'a'..=b'f' => assert_eq!(result, b - b'a' + 10),
                b'A'..=b'F' => assert_eq!(result, b - b'A' + 10),
                _ => unreachable!(),
            }
        } else {
            // Invalid — must be the sentinel 0xFF
            assert_eq!(result, 0xFF);
        }
    }

    /// Prove hex_decode rejects inputs where hex length != 2 * output length.
    #[kani::proof]
    fn verify_hex_decode_rejects_wrong_length() {
        // Any length mismatch should return false
        let hex_len: usize = kani::any_where(|&l: &usize| l <= 34 && l != 32);
        let hex = vec![b'a'; hex_len];
        let mut out = [0u8; 16];
        assert!(!hex_decode(&hex, &mut out));
    }

    // -----------------------------------------------------------------------
    // Field number constant tripwire proofs
    //
    // These verify that the named constants in this module match the OTLP
    // proto spec. If someone changes a constant, Kani will catch the drift.
    // The values come from:
    //   opentelemetry/proto/logs/v1/logs.proto
    //   opentelemetry/proto/common/v1/common.proto
    //   opentelemetry/proto/resource/v1/resource.proto
    // -----------------------------------------------------------------------

    /// Verify LogRecord field numbers match logs.proto.
    #[kani::proof]
    fn verify_log_record_field_numbers() {
        assert!(LOG_RECORD_TIME_UNIX_NANO == 1);
        assert!(LOG_RECORD_SEVERITY_NUMBER == 2);
        assert!(LOG_RECORD_SEVERITY_TEXT == 3);
        // field 4 is dropped_attributes_count (not used)
        assert!(LOG_RECORD_BODY == 5);
        assert!(LOG_RECORD_ATTRIBUTES == 6);
        // field 7 is dropped_attributes_count (not used)
        assert!(LOG_RECORD_FLAGS == 8);
        assert!(LOG_RECORD_TRACE_ID == 9);
        assert!(LOG_RECORD_SPAN_ID == 10);
        assert!(LOG_RECORD_OBSERVED_TIME_UNIX_NANO == 11);

        // Cover: all constants exercised
        kani::cover!(LOG_RECORD_TIME_UNIX_NANO == 1, "time_unix_nano is 1");
        kani::cover!(
            LOG_RECORD_OBSERVED_TIME_UNIX_NANO == 11,
            "observed_time is 11"
        );
    }

    /// Verify AnyValue field numbers match common.proto.
    #[kani::proof]
    fn verify_any_value_field_numbers() {
        assert!(ANY_VALUE_STRING_VALUE == 1);
        assert!(ANY_VALUE_BOOL_VALUE == 2);
        assert!(ANY_VALUE_INT_VALUE == 3);
        assert!(ANY_VALUE_DOUBLE_VALUE == 4);
        assert!(ANY_VALUE_ARRAY_VALUE == 5);
        assert!(ANY_VALUE_KVLIST_VALUE == 6);
        assert!(ANY_VALUE_BYTES_VALUE == 7);

        kani::cover!(ANY_VALUE_STRING_VALUE == 1, "string_value is 1");
        kani::cover!(ANY_VALUE_DOUBLE_VALUE == 4, "double_value is 4");
        kani::cover!(ANY_VALUE_ARRAY_VALUE == 5, "array_value is 5");
        kani::cover!(ANY_VALUE_KVLIST_VALUE == 6, "kvlist_value is 6");
        kani::cover!(ANY_VALUE_BYTES_VALUE == 7, "bytes_value is 7");
    }

    /// Verify KeyValue field numbers match common.proto.
    #[kani::proof]
    fn verify_key_value_field_numbers() {
        assert!(KEY_VALUE_KEY == 1);
        assert!(KEY_VALUE_VALUE == 2);

        kani::cover!(KEY_VALUE_KEY == 1, "key is 1");
        kani::cover!(KEY_VALUE_VALUE == 2, "value is 2");
    }

    /// Verify message nesting field numbers match the OTLP spec.
    #[kani::proof]
    fn verify_message_nesting_field_numbers() {
        // ExportLogsServiceRequest
        assert!(EXPORT_LOGS_REQUEST_RESOURCE_LOGS == 1);
        // ResourceLogs
        assert!(RESOURCE_LOGS_RESOURCE == 1);
        assert!(RESOURCE_LOGS_SCOPE_LOGS == 2);
        // Resource
        assert!(RESOURCE_ATTRIBUTES == 1);
        // ScopeLogs
        assert!(SCOPE_LOGS_SCOPE == 1);
        assert!(SCOPE_LOGS_LOG_RECORDS == 2);
        // InstrumentationScope
        assert!(INSTRUMENTATION_SCOPE_NAME == 1);
        assert!(INSTRUMENTATION_SCOPE_VERSION == 2);

        kani::cover!(EXPORT_LOGS_REQUEST_RESOURCE_LOGS == 1, "request field 1");
        kani::cover!(SCOPE_LOGS_LOG_RECORDS == 2, "log_records is 2");
    }

    /// Verify wire type constants match protobuf spec.
    #[kani::proof]
    fn verify_wire_type_constants() {
        assert!(WIRE_TYPE_VARINT == 0);
        assert!(WIRE_TYPE_FIXED64 == 1);
        assert!(WIRE_TYPE_LEN == 2);
        assert!(WIRE_TYPE_FIXED32 == 5);

        kani::cover!(WIRE_TYPE_VARINT == 0, "varint is 0");
        kani::cover!(WIRE_TYPE_FIXED32 == 5, "fixed32 is 5");
    }

    // -----------------------------------------------------------------------
    // Manual encode_varint stub for compositional proofs
    //
    // encode_varint takes `&mut Vec<u8>` which doesn't implement
    // `kani::Arbitrary`, so standard `#[kani::ensures]` contracts can't be
    // used with `stub_verified`. Instead we use a manual `#[kani::stub]`
    // that models encode_varint's proven behavior: appends 1-10 bytes.
    // The ground-truth correctness is already established by the
    // verify_varint_* proofs above; these compositional variants focus on
    // the *caller's* logic with encode_varint abstracted away.
    // -----------------------------------------------------------------------

    /// Manual stub modelling `encode_varint`'s proven output contract:
    /// appends between 1 and 10 arbitrary bytes to `buf`.
    fn encode_varint_stub(buf: &mut Vec<u8>, _value: u64) {
        let len: usize = kani::any();
        kani::assume(len >= 1 && len <= 10);
        let mut i = 0;
        while i < len {
            buf.push(kani::any());
            i += 1;
        }
    }

    /// Compositional proof: encode_tag with encode_varint stubbed out.
    ///
    /// encode_tag computes `(field_number << 3) | wire_type` and passes it
    /// to encode_varint. With the stub, we verify the caller logic produces
    /// a buffer of 1-10 bytes (the tag varint) for any valid inputs.
    #[kani::proof]
    #[kani::stub(encode_varint, encode_varint_stub)]
    #[kani::unwind(12)]
    fn verify_encode_tag_compositional() {
        let field_number: u32 = kani::any();
        let wire_type: u8 = kani::any();
        kani::assume(field_number > 0);
        kani::assume(field_number <= 0x1FFFFFFF);
        kani::assume(wire_type <= 5);

        let mut buf = Vec::new();
        encode_tag(&mut buf, field_number, wire_type);

        // encode_tag calls encode_varint once, so output is 1-10 bytes
        assert!(buf.len() >= 1 && buf.len() <= 10);

        kani::cover!(buf.len() == 1, "single-byte tag");
        kani::cover!(buf.len() > 1, "multi-byte tag");
    }

    /// Compositional proof: encode_fixed64 with encode_varint stubbed out.
    ///
    /// encode_fixed64 calls encode_tag (which calls encode_varint) then
    /// appends 8 LE bytes. With the stub, we verify the total output is
    /// tag (1-10 bytes) + 8 fixed bytes = 9-18 bytes.
    #[kani::proof]
    #[kani::stub(encode_varint, encode_varint_stub)]
    #[kani::unwind(12)]
    fn verify_encode_fixed64_compositional() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::new();
        encode_fixed64(&mut buf, field_number, value);

        // Tag (1-10 bytes via stub) + 8 fixed bytes
        assert!(buf.len() >= 9 && buf.len() <= 18);

        // Last 8 bytes are the value in little-endian
        let tail = &buf[buf.len() - 8..];
        let decoded = u64::from_le_bytes(tail.try_into().unwrap());
        assert!(decoded == value, "fixed64 value mismatch");

        kani::cover!(buf.len() == 9, "single-byte tag + 8 value bytes");
    }

    /// Compositional proof: encode_varint_field with encode_varint stubbed.
    ///
    /// encode_varint_field calls encode_tag (1 encode_varint for the tag)
    /// then encode_varint again for the value. With the stub, each call
    /// appends 1-10 bytes, so total is 2-20 bytes.
    #[kani::proof]
    #[kani::stub(encode_varint, encode_varint_stub)]
    #[kani::unwind(12)]
    fn verify_encode_varint_field_compositional() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::new();
        encode_varint_field(&mut buf, field_number, value);

        // Two encode_varint calls: tag (1-10) + value (1-10)
        assert!(buf.len() >= 2 && buf.len() <= 20);

        kani::cover!(buf.len() == 2, "minimal encoding");
        kani::cover!(buf.len() > 10, "large encoding");
    }

    /// Compositional proof: encode_bytes_field with encode_varint stubbed.
    ///
    /// encode_bytes_field calls encode_tag (1 encode_varint for the tag),
    /// then encode_varint for the length, then extends with the data slice.
    /// With the stub, tag and length each append 1-10 bytes, plus data_len
    /// bytes of payload.
    #[kani::proof]
    #[kani::stub(encode_varint, encode_varint_stub)]
    #[kani::unwind(12)]
    fn verify_encode_bytes_field_content_compositional() {
        let field_number: u32 = kani::any();
        kani::assume(field_number > 0 && field_number <= 100);
        let data_len: usize = kani::any_where(|&l: &usize| l <= 8);
        let data: [u8; 8] = kani::any();

        let mut buf = Vec::new();
        encode_bytes_field(&mut buf, field_number, &data[..data_len]);

        // Tag (1-10) + length varint (1-10) + data (0-8) = 2-28 bytes
        assert!(buf.len() >= 2 + data_len && buf.len() <= 20 + data_len);

        // Last data_len bytes must be the exact input data
        let payload = &buf[buf.len() - data_len..];
        let mut i = 0;
        while i < data_len {
            assert!(payload[i] == data[i], "data mismatch at byte");
            i += 1;
        }

        kani::cover!(data_len == 0, "empty payload");
        kani::cover!(data_len > 0, "non-empty payload");
    }

    /// Oracle equivalence: `decode_varint` matches `ffwd_kani::proto::decode_varint_oracle`
    /// for all bounded byte inputs (up to 10 bytes).
    ///
    /// Both return `None`/`Err` for malformed varints and `Some`/`Ok` with the same
    /// `(value, new_pos)` for well-formed varints.
    #[kani::proof]
    #[kani::unwind(22)]
    pub(super) fn verify_decode_varint_vs_oracle() {
        let data: [u8; 10] = kani::any();
        let pos: usize = kani::any_where(|&p| p <= 10);

        let ora_result = decode_varint_oracle(&data[pos..]);
        let prod_result = decode_varint(&data, pos);

        match ora_result {
            None => {
                assert!(prod_result.is_err(), "oracle None → prod Err");
            }
            Some((ora_val, ora_pos)) => {
                assert!(prod_result.is_ok(), "oracle Some → prod Ok");
                let (prod_val, prod_pos) = prod_result.unwrap();
                assert_eq!(prod_val, ora_val, "value mismatch");
                // prod_pos is absolute (from original data), ora_pos is relative to sliced data
                assert_eq!(prod_pos, pos + ora_pos, "pos mismatch");
                kani::cover!(ora_val < 128, "single-byte result");
                kani::cover!(ora_val >= 128, "multi-byte result");
            }
        }
    }
}
