//! OTLP protobuf encoding helpers and log-field parsers.
//!
//! Provides protobuf wire format primitives (`encode_varint`, `encode_tag`,
//! etc.), severity parsing, and timestamp parsing. The actual OTLP
//! LogRecord encoding from RecordBatch columns lives in
//! `crates/logfwd-output/src/otlp_sink.rs`.
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

// OTLP LogRecord encoding from RecordBatch columns lives in
// crates/logfwd-output/src/otlp_sink.rs. Raw-line encoding was
// removed in #357.

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
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

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
        let year: i64 = kani::any();
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

        // Empty / unknown
        assert!(matches!(parse_severity(b"").0, Severity::Unspecified));
        assert!(matches!(parse_severity(b"X").0, Severity::Unspecified));
    }

    /// Prove parse_severity handles mixed case via |0x20 on first two bytes.
    #[kani::proof]
    fn verify_parse_severity_mixed_case() {
        // Mixed case should match (|0x20 folds to lowercase)
        assert!(matches!(parse_severity(b"Info").0, Severity::Info));
        assert!(matches!(parse_severity(b"Warn").0, Severity::Warn));
        assert!(matches!(parse_severity(b"Error").0, Severity::Error));
        assert!(matches!(parse_severity(b"Debug").0, Severity::Debug));
        assert!(matches!(parse_severity(b"Trace").0, Severity::Trace));
        assert!(matches!(parse_severity(b"Fatal").0, Severity::Fatal));

        // Severity text slice should be the correct length
        assert_eq!(parse_severity(b"INFO extra").1, b"INFO");
        assert_eq!(parse_severity(b"error stuff").1, b"error");
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
    fn verify_encode_fixed64() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::new();
        encode_fixed64(&mut buf, field_number, value);

        // Tag + 8 bytes
        let tag_len = varint_len(((field_number as u64) << 3) | 1);
        assert!(buf.len() == tag_len + 8, "fixed64 size wrong");

        // Last 8 bytes are the value in little-endian
        let val_bytes = &buf[tag_len..];
        let decoded = u64::from_le_bytes(val_bytes.try_into().unwrap());
        assert!(decoded == value, "fixed64 value mismatch");
    }

    /// Prove encode_varint_field produces tag + varint value.
    #[kani::proof]
    #[kani::unwind(12)]
    fn verify_encode_varint_field() {
        let field_number: u32 = kani::any();
        let value: u64 = kani::any();
        kani::assume(field_number > 0 && field_number <= 1000);

        let mut buf = Vec::new();
        encode_varint_field(&mut buf, field_number, value);

        let tag_len = varint_len(((field_number as u64) << 3) | 0);
        let val_len = varint_len(value);
        assert!(buf.len() == tag_len + val_len, "varint_field size wrong");
    }
}
