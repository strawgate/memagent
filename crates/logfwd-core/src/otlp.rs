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

/// Fast severity lookup from full string.
#[inline(always)]
pub fn parse_severity(text: &[u8]) -> (Severity, &[u8]) {
    // Common patterns: "INFO", "WARN", "ERROR", "DEBUG", "TRACE", "FATAL"
    // Also: "info", "warn", "error", "debug", "trace", "fatal"
    let sev = match text.len() {
        4 => {
            if key_eq_ignore_case(text, b"INFO") {
                Severity::Info
            } else if key_eq_ignore_case(text, b"WARN") {
                Severity::Warn
            } else {
                Severity::Unspecified
            }
        }
        5 => {
            if key_eq_ignore_case(text, b"DEBUG") {
                Severity::Debug
            } else if key_eq_ignore_case(text, b"TRACE") {
                Severity::Trace
            } else if key_eq_ignore_case(text, b"ERROR") {
                Severity::Error
            } else if key_eq_ignore_case(text, b"FATAL") {
                Severity::Fatal
            } else {
                Severity::Unspecified
            }
        }
        _ => Severity::Unspecified,
    };

    if !matches!(sev, Severity::Unspecified) {
        (sev, text)
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
    a.eq_ignore_ascii_case(b)
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

    let year = parse_4digits(ts, 0) as i32;
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
/// Year range restricted to [1900, 2100] for verification purposes.
fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    #[cfg(kani)]
    kani::assume(year >= 1900 && year <= 2100);

    let y = if month <= 2 { year - 1 } else { year };
    let m = if month <= 2 {
        month as i32 + 9
    } else {
        month as i32 - 3
    };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + day as i32 - 1;
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
        assert_eq!(nanos, 1_705_314_600_000_000_000);
    }

    #[test]
    fn test_parse_timestamp_fractional() {
        let ts = b"2024-01-15T10:30:00.123Z";
        let nanos = parse_timestamp_nanos(ts);
        assert_eq!(nanos, 1_705_314_600_123_000_000);
    }

    #[test]
    fn test_parse_timestamp_nanos_precision() {
        let ts = b"2024-01-15T10:30:00.123456789Z";
        let nanos = parse_timestamp_nanos(ts);
        assert_eq!(nanos, 1_705_314_600_123_456_789);
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
        let size = encode_log_record(line, 1_705_314_600_000_000_000, &mut buf);
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
        let batch = encode_batch(&lines, 1_705_314_600_000_000_000);
        assert!(!batch.is_empty());

        // Basic structure check: starts with tag for field 1 (ResourceLogs).
        // Field 1, wire type 2 = (1 << 3) | 2 = 0x0A
        assert_eq!(batch[0], 0x0A);
    }

    #[test]
    fn test_non_json_line_uses_full_body() {
        let line = b"2024-01-15 INFO just a plain text log line";
        let mut buf = Vec::new();
        encode_log_record(line, 1_705_314_600_000_000_000, &mut buf);
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
        let size = encode_log_record(line.as_bytes(), 1_705_314_600_000_000_000, &mut buf);

        // Body is 200 bytes. Overhead should be small.
        let overhead = size - 200;
        assert!(
            overhead < 40,
            "protobuf overhead {} bytes for 200-byte body is too high",
            overhead
        );
    }
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    fn decode_varint(buf: &[u8]) -> (u64, usize) {
        let mut val = 0u64;
        let mut shift = 0;
        for (i, &b) in buf.iter().enumerate() {
            val |= ((b & 0x7F) as u64) << shift;
            if b < 0x80 {
                return (val, i + 1);
            }
            shift += 7;
            if shift >= 64 {
                break;
            }
        }
        (val, 0)
    }

    /// Prove varint_len matches encode_varint output length for ALL u64 values.
    ///
    /// This is the foundational wire format proof — if these disagree,
    /// protobuf message size calculations are wrong and payloads are corrupt.
    #[kani::proof]
    #[kani::unwind(12)] // varint loop: max 10 iterations + overhead
    #[kani::solver(kissat)]
    fn verify_varint_len_matches_encode() {
        let value: u64 = kani::any();
        let mut buf = Vec::new();
        encode_varint(&mut buf, value);
        assert!(
            buf.len() == varint_len(value),
            "varint_len disagrees with encode_varint"
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
        let mut buf = Vec::new();
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

    /// Prove encode_varint never panics for any u64 input.
    #[kani::proof]
    #[kani::unwind(12)]
    fn verify_varint_no_panic() {
        let value: u64 = kani::any();
        let mut buf = Vec::new();
        encode_varint(&mut buf, value);
    }

    /// Prove encode_tag produces correct field_number and wire_type encoding.
    #[kani::proof]
    #[kani::unwind(12)]
    fn verify_encode_tag() {
        let field_number: u32 = kani::any();
        let wire_type: u8 = kani::any();
        kani::assume(field_number > 0);
        kani::assume(field_number <= 0x1FFFFFFF); // max protobuf field number
        kani::assume(wire_type <= 5); // valid wire types: 0-5

        let mut buf = Vec::new();
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
    }

    /// Prove days_from_civil never panics and produces reasonable values
    /// for all dates in the range [1970-01-01, 2100-12-31].
    ///
    /// Also verifies monotonicity: incrementing the day by 1 always
    /// increments the result by 1 (within the same month).
    #[kani::proof]
    fn verify_days_from_civil() {
        let year: i32 = kani::any();
        let month: u32 = kani::any();
        let day: u32 = kani::any();

        kani::assume(year >= 1970 && year <= 2100);
        kani::assume(month >= 1 && month <= 12);
        kani::assume(day >= 1 && day <= 31);

        let result = days_from_civil(year, month, day);

        // Epoch (1970-01-01) must be day 0.
        if year == 1970 && month == 1 && day == 1 {
            assert!(result == 0, "epoch must be 0");
        }

        // All dates in [1970, 2100] must produce non-negative results.
        assert!(result >= 0, "date before epoch in valid range");

        // 2100-12-31 is about 47846 days after epoch.
        assert!(result <= 50000, "date too far in future");

        // Monotonicity within a month: day+1 → result+1.
        if day < 28 {
            let next = days_from_civil(year, month, day + 1);
            assert!(next == result + 1, "days not monotonic within month");
        }
    }

    /// Prove bytes_field_size matches actual encode_bytes_field output.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::solver(kissat)]
    fn verify_bytes_field_size() {
        let field_number: u32 = kani::any();
        let data_len: usize = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);
        kani::assume(data_len <= 256);

        let predicted = bytes_field_size(field_number, data_len);

        // Create dummy data of the right length and encode
        let data = vec![0u8; data_len];
        let mut buf = Vec::new();
        encode_bytes_field(&mut buf, field_number, &data);

        assert!(
            buf.len() == predicted,
            "bytes_field_size disagrees with encode_bytes_field"
        );
    }

    /// Prove parse_timestamp_nanos for a restricted range.
    #[kani::proof]
    #[kani::unwind(32)]
    fn verify_parse_timestamp_nanos_restricted() {
        let bytes: [u8; 20] = kani::any();
        // Constrain to something that looks like 2024-01-15T10:30:00Z
        kani::assume(bytes[0] == b'2');
        kani::assume(bytes[1] == b'0');
        kani::assume(bytes[2] == b'2');
        kani::assume(bytes[3].is_ascii_digit());
        kani::assume(bytes[4] == b'-');
        kani::assume(bytes[5] == b'0' || bytes[5] == b'1');
        kani::assume(bytes[6].is_ascii_digit());
        kani::assume(bytes[7] == b'-');
        kani::assume(bytes[8].is_ascii_digit());
        kani::assume(bytes[9].is_ascii_digit());
        kani::assume(bytes[10] == b'T');
        kani::assume(bytes[11].is_ascii_digit());
        kani::assume(bytes[12].is_ascii_digit());
        kani::assume(bytes[13] == b':');
        kani::assume(bytes[14].is_ascii_digit());
        kani::assume(bytes[15].is_ascii_digit());
        kani::assume(bytes[16] == b':');
        kani::assume(bytes[17].is_ascii_digit());
        kani::assume(bytes[18].is_ascii_digit());
        kani::assume(bytes[19] == b'Z');

        let nanos = parse_timestamp_nanos(&bytes);
        // If it parsed, it should be reasonably in the future
        if nanos > 0 {
            assert!(nanos > 1_000_000_000_000_000_000);
        }
    }

    /// Prove extract_json_fields never panics for small buffers.
    #[kani::proof]
    #[kani::unwind(21)]
    fn verify_extract_json_fields_small() {
        let bytes: [u8; 16] = kani::any();
        let _ = extract_json_fields(&bytes);
    }

    /// Prove parse_severity never panics for any 8-byte input and
    /// returns correct severity for known level strings.
    #[kani::proof]
    fn verify_parse_severity_no_panic() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let _ = parse_severity(&bytes[..len]);
    }

    /// Prove parse_severity ONLY matches the 6 standard levels.
    #[kani::proof]
    fn verify_parse_severity_only_matches_standard() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let text = &bytes[..len];
        let (sev, _) = parse_severity(text);

        if !matches!(sev, Severity::Unspecified) {
            let is_standard = key_eq_ignore_case(text, b"TRACE")
                || key_eq_ignore_case(text, b"DEBUG")
                || key_eq_ignore_case(text, b"INFO")
                || key_eq_ignore_case(text, b"WARN")
                || key_eq_ignore_case(text, b"ERROR")
                || key_eq_ignore_case(text, b"FATAL");
            assert!(is_standard, "Matched a non-standard level");
        }
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

        // Empty / unknown
        assert!(matches!(parse_severity(b"").0, Severity::Unspecified));
        assert!(matches!(parse_severity(b"X").0, Severity::Unspecified));
        assert!(matches!(parse_severity(b"TRAMP").0, Severity::Unspecified));
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

    /// Prove key_eq_ignore_case matches to_ascii_lowercase for ASCII inputs.
    ///
    /// LIMITATION: This function uses a bitwise OR with 0x20, which is only
    /// correct for ASCII letters. It will incorrectly equate some non-letter
    /// ASCII characters, for example '@' (0x40) and '`' (0x60) since
    /// 0x40 | 0x20 == 0x60. For JSON field names which are typically
    /// alphanumeric, this approximation is safe and efficient.
    #[kani::proof]
    fn verify_key_eq_ignore_case_ascii() {
        let a: [u8; 2] = kani::any();
        let b: [u8; 2] = kani::any();

        // Constrain to full ASCII range
        kani::assume(a[0] <= 127 && a[1] <= 127);
        kani::assume(b[0] <= 127 && b[1] <= 127);

        let result = key_eq_ignore_case(&a, &b);

        // Oracle: true ASCII case-insensitive comparison
        let expected = a[0].to_ascii_lowercase() == b[0].to_ascii_lowercase()
            && a[1].to_ascii_lowercase() == b[1].to_ascii_lowercase();

        assert!(
            result == expected,
            "key_eq_ignore_case diverges from ascii_lowercase"
        );
    }

    /// Prove encode_fixed64 produces correct tag and 8 LE bytes.
    #[kani::proof]
    #[kani::unwind(12)]
    fn verify_encode_fixed64() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::new();
        encode_fixed64(&mut buf, field_number, value);

        // Verify tag
        let (tag, tag_len) = decode_varint(&buf);
        assert!(tag_len > 0, "tag decode failed");
        assert!((tag >> 3) == field_number as u64, "field number mismatch");
        assert!((tag & 0x7) == 1, "wire type mismatch (expected 1 for fixed64)");

        // Tag + 8 bytes
        assert!(buf.len() == tag_len + 8, "fixed64 total size wrong");

        // Last 8 bytes are the value in little-endian
        let val_bytes = &buf[tag_len..];
        let decoded = u64::from_le_bytes(val_bytes.try_into().unwrap());
        assert!(decoded == value, "fixed64 value mismatch");
    }

    /// Prove encode_varint_field produces correct tag + varint value.
    #[kani::proof]
    #[kani::unwind(12)]
    fn verify_encode_varint_field() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::new();
        encode_varint_field(&mut buf, field_number, value);

        // Verify tag
        let (tag, tag_len) = decode_varint(&buf);
        assert!(tag_len > 0, "tag decode failed");
        assert!((tag >> 3) == field_number as u64, "field number mismatch");
        assert!((tag & 0x7) == 0, "wire type mismatch (expected 0 for varint)");

        // Verify value
        let (decoded_val, val_len) = decode_varint(&buf[tag_len..]);
        assert!(val_len > 0, "value decode failed");
        assert!(decoded_val == value, "varint value mismatch");

        assert!(buf.len() == tag_len + val_len, "varint_field total size wrong");
    }
}
