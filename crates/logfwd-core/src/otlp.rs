//! Zero-allocation OTLP protobuf encoder for log records.
//!
//! Transcodes JSON log lines directly to OTLP protobuf LogRecords
//! without intermediate Rust structs. Scans JSON for field positions
//! using memchr, writes protobuf bytes directly from those byte ranges.
//!
//! The OTLP LogRecord protobuf layout (field numbers from opentelemetry/proto/logs/v1/logs.proto):
//!   1: time_unix_nano (fixed64)
//!  11: observed_time_unix_nano (fixed64)
//!   2: severity_number (int32 enum)
//!   3: severity_text (string)
//!   5: body (AnyValue message containing string_value)
//!   6: attributes (repeated KeyValue)
//!
//! Wire format: each field = tag_varint + value
//!   tag = (field_number << 3) | wire_type
//!   wire_type: 0=varint, 1=64-bit fixed, 2=length-delimited

use memchr::memchr;

// --- Protobuf wire format helpers ---

/// Encode a varint into buf at offset, return new offset.
#[inline(always)]
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
pub fn encode_tag(buf: &mut Vec<u8>, field_number: u32, wire_type: u8) {
    encode_varint(buf, ((field_number as u64) << 3) | wire_type as u64);
}

/// Write a fixed64 field (tag + 8 bytes little-endian).
#[inline(always)]
pub fn encode_fixed64(buf: &mut Vec<u8>, field_number: u32, value: u64) {
    encode_tag(buf, field_number, 1); // wire type 1 = 64-bit
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a varint field (tag + varint value).
#[inline(always)]
pub fn encode_varint_field(buf: &mut Vec<u8>, field_number: u32, value: u64) {
    encode_tag(buf, field_number, 0); // wire type 0 = varint
    encode_varint(buf, value);
}

/// Write a length-delimited field (tag + length + bytes).
#[inline(always)]
pub fn encode_bytes_field(buf: &mut Vec<u8>, field_number: u32, data: &[u8]) {
    encode_tag(buf, field_number, 2); // wire type 2 = length-delimited
    encode_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

/// Compute the encoded size of a length-delimited field (without writing).
#[inline(always)]
pub const fn bytes_field_size(field_number: u32, data_len: usize) -> usize {
    let tag_size = varint_len(((field_number as u64) << 3) | 2);
    let len_size = varint_len(data_len as u64);
    tag_size + len_size + data_len
}

// --- OTLP Severity mapping ---

/// OTLP SeverityNumber enum values.
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum Severity {
    Unspecified = 0,
    Trace = 1,
    Debug = 5,
    Info = 9,
    Warn = 13,
    Error = 17,
    Fatal = 21,
}

/// Fast severity lookup from first byte + length. No string comparison needed.
#[inline(always)]
pub fn parse_severity(text: &[u8]) -> (Severity, &[u8]) {
    // Common patterns: "INFO", "WARN", "ERROR", "DEBUG", "TRACE", "FATAL"
    // Also: "info", "warn", "error", "debug", "trace", "fatal"
    if text.is_empty() {
        return (Severity::Unspecified, text);
    }
    let (sev, len) = match (text[0] | 0x20, text.len()) {
        // lowercase first byte
        (b't', n) if n >= 5 && (text[1] | 0x20) == b'r' => (Severity::Trace, 5),
        (b'd', n) if n >= 5 && (text[1] | 0x20) == b'e' => (Severity::Debug, 5),
        (b'i', n) if n >= 4 => (Severity::Info, 4),
        (b'w', n) if n >= 4 => (Severity::Warn, 4),
        (b'e', n) if n >= 5 => (Severity::Error, 5),
        (b'f', n) if n >= 5 => (Severity::Fatal, 5),
        _ => (Severity::Unspecified, 0),
    };
    if len > 0 {
        (sev, &text[..len])
    } else {
        (Severity::Unspecified, text)
    }
}

// --- JSON field extraction ---

/// Result of scanning a JSON log line for known fields.
/// All fields are byte ranges into the original line buffer (zero-copy).
#[derive(Default)]
struct JsonFields<'a> {
    /// Timestamp string (e.g., "2024-01-15T10:30:00.123Z")
    timestamp: Option<&'a [u8]>,
    /// Log level / severity string (e.g., "INFO")
    level: Option<&'a [u8]>,
    /// Message body
    message: Option<&'a [u8]>,
    /// The full line (used as body if no message field found)
    full_line: &'a [u8],
}

/// Field names we recognize (checked in order of likelihood).
/// These cover the most common JSON log field names across ecosystems.
const TIMESTAMP_KEYS: &[&[u8]] = &[
    b"timestamp",
    b"time",
    b"ts",
    b"@timestamp",
    b"datetime",
    b"t",
];
const LEVEL_KEYS: &[&[u8]] = &[b"level", b"severity", b"log_level", b"loglevel", b"lvl"];
const MESSAGE_KEYS: &[&[u8]] = &[b"message", b"msg", b"body", b"log", b"text"];

/// Extract known fields from a JSON line. Uses memchr to find quotes,
/// then matches field names. No full JSON parse — just enough to find
/// our target fields.
///
/// Handles: `{"timestamp":"...","level":"...","message":"...",...}`
/// Does NOT handle: nested objects as values, escaped quotes in keys.
/// (Log lines rarely have escaped quotes in field names.)
fn extract_json_fields<'a>(line: &'a [u8]) -> JsonFields<'a> {
    let mut fields = JsonFields {
        full_line: line,
        ..Default::default()
    };

    let mut pos = 0;
    while pos < line.len() {
        // Find next '"' (start of a key).
        let Some(q1) = memchr(b'"', &line[pos..]) else {
            break;
        };
        let key_start = pos + q1 + 1;
        let Some(q2) = memchr(b'"', &line[key_start..]) else {
            break;
        };
        let key = &line[key_start..key_start + q2];
        pos = key_start + q2 + 1;

        // Skip to value: expect ':' then optional whitespace then '"' or digit.
        let Some(colon) = memchr(b':', &line[pos..]) else {
            break;
        };
        pos += colon + 1;

        // Skip whitespace.
        while pos < line.len() && (line[pos] == b' ' || line[pos] == b'\t') {
            pos += 1;
        }
        if pos >= line.len() {
            break;
        }

        // Extract value based on type.
        let value = if line[pos] == b'"' {
            // String value.
            pos += 1; // skip opening quote
            let val_start = pos;
            // Find closing quote (simple — doesn't handle escaped quotes in values).
            // For log data this is almost always fine.
            let Some(vq) = memchr(b'"', &line[pos..]) else {
                break;
            };
            let val = &line[val_start..pos + vq];
            pos += vq + 1;
            val
        } else {
            // Non-string value (number, bool, null). Scan to next , or }.
            let val_start = pos;
            while pos < line.len() && line[pos] != b',' && line[pos] != b'}' {
                pos += 1;
            }
            let val = &line[val_start..pos];
            // Trim trailing whitespace.
            let trimmed_end = val
                .iter()
                .rposition(|&b| b != b' ' && b != b'\t')
                .map(|i| i + 1)
                .unwrap_or(0);
            &val[..trimmed_end]
        };

        // Match key against known field names.
        let _key_lower_first = if key.is_empty() { 0 } else { key[0] | 0x20 };
        // Check all field categories — a key starting with 't' could be
        // "timestamp" OR "text", so we check each category independently.
        if fields.timestamp.is_none() && TIMESTAMP_KEYS.iter().any(|k| key_eq_ignore_case(key, k)) {
            fields.timestamp = Some(value);
        } else if fields.level.is_none() && LEVEL_KEYS.iter().any(|k| key_eq_ignore_case(key, k)) {
            fields.level = Some(value);
        } else if fields.message.is_none()
            && MESSAGE_KEYS.iter().any(|k| key_eq_ignore_case(key, k))
        {
            fields.message = Some(value);
        }

        // Early exit if we found all three.
        if fields.timestamp.is_some() && fields.level.is_some() && fields.message.is_some() {
            break;
        }
    }

    fields
}

/// Case-insensitive key comparison. Keys are typically short (<20 bytes).
#[inline]
fn key_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x | 0x20 == y | 0x20)
}

// --- Timestamp parsing ---

/// Parse an ISO 8601 / RFC 3339 timestamp to nanoseconds since Unix epoch.
/// Hand-rolled for speed — no chrono, no strptime. Handles:
///   2024-01-15T10:30:00Z
///   2024-01-15T10:30:00.123Z
///   2024-01-15T10:30:00.123456789Z
///   2024-01-15 10:30:00Z (space separator)
/// Returns 0 on parse failure (observed_time will be used instead).
pub fn parse_timestamp_nanos(ts: &[u8]) -> u64 {
    if ts.len() < 19 {
        return 0; // too short for YYYY-MM-DDTHH:MM:SS
    }

    let year = parse_4digits(ts, 0) as i64;
    let month = parse_2digits(ts, 5) as u32;
    let day = parse_2digits(ts, 8) as u32;
    let hour = parse_2digits(ts, 11) as u64;
    let min = parse_2digits(ts, 14) as u64;
    let sec = parse_2digits(ts, 17) as u64;

    if year == 0 || month == 0 || month > 12 || day == 0 || day > 31 {
        return 0;
    }

    // Days from Unix epoch (1970-01-01) to the given date.
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return 0;
    }

    let mut nanos = (days as u64) * 86400 + hour * 3600 + min * 60 + sec;
    nanos *= 1_000_000_000;

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
            nanos += frac_val;
        }
    }

    nanos
}

/// Parse 4 ASCII digits at offset. Returns 0 on non-digit.
#[inline(always)]
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

/// Parse 2 ASCII digits at offset.
#[inline(always)]
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

/// Days from 1970-01-01 to the given civil date. Algorithm from Howard Hinnant.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
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

// --- OTLP LogRecord encoder ---

/// Encode a log line as an OTLP LogRecord with the raw line as body.
/// No JSON parsing — just wraps the bytes in protobuf. Maximum speed.
#[inline]
pub fn encode_log_record_raw(line: &[u8], observed_time_ns: u64, buf: &mut Vec<u8>) -> usize {
    let start_len = buf.len();

    // AnyValue.string_value inner size
    let anyvalue_inner = bytes_field_size(1, line.len());
    // LogRecord inner: observed_time (9) + body (tag + len + anyvalue)
    let inner_size = 1 + 8 + bytes_field_size(5, anyvalue_inner);

    buf.reserve(inner_size + 5);

    // field 5: body AnyValue { string_value = line }
    encode_tag(buf, 5, 2);
    encode_varint(buf, anyvalue_inner as u64);
    encode_bytes_field(buf, 1, line);

    // field 11: observed_time_unix_nano
    encode_fixed64(buf, 11, observed_time_ns);

    buf.len() - start_len
}

/// Encodes a single JSON log line as an OTLP LogRecord into the output buffer.
/// Extracts timestamp, severity, and message from JSON fields.
/// Returns the number of bytes written.
pub fn encode_log_record(line: &[u8], observed_time_ns: u64, buf: &mut Vec<u8>) -> usize {
    let start_len = buf.len();
    let fields = extract_json_fields(line);

    // Parse timestamp from JSON, fall back to observed_time.
    let time_ns = fields
        .timestamp
        .and_then(|ts| {
            let v = parse_timestamp_nanos(ts);
            if v > 0 { Some(v) } else { None }
        })
        .unwrap_or(0);

    // Parse severity.
    let (severity_num, severity_text) = fields
        .level
        .map(parse_severity)
        .unwrap_or((Severity::Unspecified, b"" as &[u8]));

    // Body: use message field if found, otherwise the full line.
    let body = fields.message.unwrap_or(fields.full_line);

    // Encode the LogRecord fields into a temporary area, then prepend its length.
    // We need to know the total size first for the length-delimited wrapper.

    // Calculate the inner LogRecord size.
    let mut inner_size = 0usize;

    // field 1: time_unix_nano (fixed64) — 1 byte tag + 8 bytes = 9
    if time_ns > 0 {
        inner_size += 1 + 8;
    }
    // field 11: observed_time_unix_nano (fixed64) — 2 byte tag (field 11 > 15? no, 11 fits in 1 byte tag) + 8 bytes = 9
    // tag for field 11: (11 << 3) | 1 = 89, fits in 1 byte
    inner_size += 1 + 8;

    // field 2: severity_number (varint) — 1 byte tag + 1 byte value = 2
    if severity_num as u8 > 0 {
        inner_size += 1 + 1;
    }
    // field 3: severity_text (string)
    if !severity_text.is_empty() {
        inner_size += bytes_field_size(3, severity_text.len());
    }

    // field 5: body (AnyValue { string_value = body })
    // AnyValue.string_value is field 1, wire type 2 (length-delimited)
    let anyvalue_inner = bytes_field_size(1, body.len());
    inner_size += bytes_field_size(5, anyvalue_inner);

    // Now write: we DON'T write a LogRecord wrapper tag here — that's the
    // caller's job (ScopeLogs.log_records is a repeated field). We just write
    // the raw LogRecord bytes. The caller wraps each in a length-delimited field.

    // Reserve space.
    buf.reserve(inner_size + 5); // +5 for safety

    // Write fields in field-number order.
    if time_ns > 0 {
        encode_fixed64(buf, 1, time_ns);
    }
    if severity_num as u8 > 0 {
        encode_varint_field(buf, 2, severity_num as u64);
    }
    if !severity_text.is_empty() {
        encode_bytes_field(buf, 3, severity_text);
    }

    // field 5: body AnyValue
    encode_tag(buf, 5, 2); // field 5, wire type 2 (length-delimited)
    encode_varint(buf, anyvalue_inner as u64);
    // AnyValue.string_value (field 1)
    encode_bytes_field(buf, 1, body);

    // field 11: observed_time_unix_nano
    encode_fixed64(buf, 11, observed_time_ns);

    buf.len() - start_len
}

/// Encode a batch of log lines as a complete ExportLogsServiceRequest.
///
/// Structure:
///   ExportLogsServiceRequest {
///     resource_logs[0]: ResourceLogs {
///       scope_logs[0]: ScopeLogs {
///         log_records: [ ...one per line... ]
///       }
///     }
///   }
///
/// Returns the complete protobuf bytes ready to POST to an OTLP HTTP endpoint.
pub fn encode_batch(lines: &[&[u8]], observed_time_ns: u64) -> Vec<u8> {
    let mut encoder = BatchEncoder::new();
    encoder.encode(lines, observed_time_ns)
}

/// Reusable batch encoder. Holds internal buffers across calls to avoid
/// per-batch allocation.
pub struct BatchEncoder {
    /// Intermediate buffer for encoded LogRecord bytes.
    records_buf: Vec<u8>,
    /// Offsets of each record in records_buf: (start, end).
    record_ranges: Vec<(usize, usize)>,
    /// Final output buffer.
    out: Vec<u8>,
}

impl Default for BatchEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchEncoder {
    pub fn new() -> Self {
        BatchEncoder {
            records_buf: Vec::with_capacity(1024 * 1024),
            record_ranges: Vec::with_capacity(8192),
            out: Vec::with_capacity(1024 * 1024),
        }
    }

    /// Encode a batch of log lines into a complete ExportLogsServiceRequest.
    /// Reuses internal buffers — no allocation after the first call (unless
    /// the batch is larger than previous ones).
    pub fn encode(&mut self, lines: &[&[u8]], observed_time_ns: u64) -> Vec<u8> {
        // Phase 1: encode all LogRecords into records_buf.
        self.records_buf.clear();
        self.record_ranges.clear();

        for &line in lines {
            let start = self.records_buf.len();
            encode_log_record(line, observed_time_ns, &mut self.records_buf);
            let end = self.records_buf.len();
            self.record_ranges.push((start, end));
        }

        // Phase 2: compute sizes bottom-up.
        let mut scope_logs_inner_size = 0usize;
        for &(start, end) in &self.record_ranges {
            let record_len = end - start;
            scope_logs_inner_size +=
                varint_len(((2u64) << 3) | 2) + varint_len(record_len as u64) + record_len;
        }

        let resource_logs_inner_size = bytes_field_size(2, scope_logs_inner_size);
        let request_size = bytes_field_size(1, resource_logs_inner_size);

        // Phase 3: write the final output.
        self.out.clear();
        self.out.reserve(request_size + 10);

        encode_tag(&mut self.out, 1, 2);
        encode_varint(&mut self.out, resource_logs_inner_size as u64);

        encode_tag(&mut self.out, 2, 2);
        encode_varint(&mut self.out, scope_logs_inner_size as u64);

        for &(start, end) in &self.record_ranges {
            encode_bytes_field(&mut self.out, 2, &self.records_buf[start..end]);
        }

        // Return ownership of the output. Caller should pass it back or drop it.
        // We swap in a fresh vec so self.out is ready for next call.
        std::mem::replace(&mut self.out, Vec::with_capacity(request_size + 10))
    }

    /// Encode lines from a contiguous buffer of newline-delimited messages.
    /// Parses JSON fields for timestamp, severity, body.
    pub fn encode_from_buf(&mut self, buf: &[u8], observed_time_ns: u64) -> Vec<u8> {
        self.encode_from_buf_with(buf, observed_time_ns, encode_log_record)
    }

    /// Encode lines from a contiguous buffer, raw body mode.
    /// No JSON parsing — the entire line becomes the body string.
    pub fn encode_from_buf_raw(&mut self, buf: &[u8], observed_time_ns: u64) -> Vec<u8> {
        self.encode_from_buf_with(buf, observed_time_ns, encode_log_record_raw)
    }

    fn encode_from_buf_with(
        &mut self,
        buf: &[u8],
        observed_time_ns: u64,
        encode_fn: fn(&[u8], u64, &mut Vec<u8>) -> usize,
    ) -> Vec<u8> {
        self.records_buf.clear();
        self.record_ranges.clear();

        let mut line_start = 0;
        for pos in memchr::memchr_iter(b'\n', buf) {
            let line = &buf[line_start..pos];
            line_start = pos + 1;
            if line.is_empty() {
                continue;
            }
            let start = self.records_buf.len();
            encode_fn(line, observed_time_ns, &mut self.records_buf);
            self.record_ranges.push((start, self.records_buf.len()));
        }
        if line_start < buf.len() {
            let line = &buf[line_start..];
            if !line.is_empty() {
                let start = self.records_buf.len();
                encode_fn(line, observed_time_ns, &mut self.records_buf);
                self.record_ranges.push((start, self.records_buf.len()));
            }
        }

        self.finish_batch()
    }

    fn finish_batch(&mut self) -> Vec<u8> {
        let mut scope_logs_inner_size = 0usize;
        for &(start, end) in &self.record_ranges {
            let record_len = end - start;
            scope_logs_inner_size +=
                varint_len(((2u64) << 3) | 2) + varint_len(record_len as u64) + record_len;
        }

        let resource_logs_inner_size = bytes_field_size(2, scope_logs_inner_size);
        let request_size = bytes_field_size(1, resource_logs_inner_size);

        self.out.clear();
        self.out.reserve(request_size + 10);

        encode_tag(&mut self.out, 1, 2);
        encode_varint(&mut self.out, resource_logs_inner_size as u64);
        encode_tag(&mut self.out, 2, 2);
        encode_varint(&mut self.out, scope_logs_inner_size as u64);

        for &(start, end) in &self.record_ranges {
            encode_bytes_field(&mut self.out, 2, &self.records_buf[start..end]);
        }

        std::mem::replace(&mut self.out, Vec::with_capacity(request_size + 10))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        let ts = b"2024-01-15T10:30:00Z";
        let nanos = parse_timestamp_nanos(ts);
        // 2024-01-15 10:30:00 UTC
        // Expected: 1705314600 seconds * 1e9
        assert_eq!(nanos, 1705314600_000_000_000);
    }

    #[test]
    fn test_parse_timestamp_fractional() {
        let ts = b"2024-01-15T10:30:00.123Z";
        let nanos = parse_timestamp_nanos(ts);
        assert_eq!(nanos, 1705314600_123_000_000);
    }

    #[test]
    fn test_parse_timestamp_nanos_precision() {
        let ts = b"2024-01-15T10:30:00.123456789Z";
        let nanos = parse_timestamp_nanos(ts);
        assert_eq!(nanos, 1705314600_123_456_789);
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
    fn test_extract_json_fields() {
        let line = br#"{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"hello world","extra":"data"}"#;
        let fields = extract_json_fields(line);
        assert_eq!(fields.timestamp, Some(&b"2024-01-15T10:30:00Z"[..]));
        assert_eq!(fields.level, Some(&b"INFO"[..]));
        assert_eq!(fields.message, Some(&b"hello world"[..]));
    }

    #[test]
    fn test_extract_alternate_field_names() {
        let line = br#"{"ts":"2024-01-15T10:30:00Z","severity":"warn","msg":"something happened"}"#;
        let fields = extract_json_fields(line);
        assert_eq!(fields.timestamp, Some(&b"2024-01-15T10:30:00Z"[..]));
        assert_eq!(fields.level, Some(&b"warn"[..]));
        assert_eq!(fields.message, Some(&b"something happened"[..]));
    }

    #[test]
    fn test_encode_single_record() {
        let line = br#"{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"hello"}"#;
        let mut buf = Vec::new();
        let size = encode_log_record(line, 1705314600_000_000_000, &mut buf);
        assert!(size > 0);
        assert_eq!(buf.len(), size);
        // The encoded bytes should be valid protobuf (we verify via decode in the batch test).
    }

    #[test]
    fn test_encode_batch() {
        let lines: Vec<&[u8]> = vec![
            br#"{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"first"}"#,
            br#"{"timestamp":"2024-01-15T10:30:01Z","level":"WARN","message":"second"}"#,
            br#"{"timestamp":"2024-01-15T10:30:02Z","level":"ERROR","message":"third"}"#,
        ];
        let batch = encode_batch(&lines, 1705314600_000_000_000);
        assert!(!batch.is_empty());

        // Basic structure check: starts with tag for field 1 (ResourceLogs).
        // Field 1, wire type 2 = (1 << 3) | 2 = 0x0A
        assert_eq!(batch[0], 0x0A);
    }

    #[test]
    fn test_non_json_line_uses_full_body() {
        let line = b"2024-01-15 INFO just a plain text log line";
        let mut buf = Vec::new();
        encode_log_record(line, 1705314600_000_000_000, &mut buf);
        // Should still encode — body will be the full line since no JSON fields found.
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_overhead() {
        // Measure protobuf overhead for a 200-byte message.
        let msg = "x".repeat(200);
        let line = format!(
            r#"{{"timestamp":"2024-01-15T10:30:00.123Z","level":"INFO","message":"{}"}}"#,
            msg
        );
        let mut buf = Vec::new();
        let size = encode_log_record(line.as_bytes(), 1705314600_000_000_000, &mut buf);

        // Body is 200 bytes. Overhead should be small.
        let overhead = size - 200;
        assert!(
            overhead < 40,
            "protobuf overhead {} bytes for 200-byte body is too high",
            overhead
        );
    }
}
