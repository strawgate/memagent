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

use alloc::vec::Vec;

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
pub fn parse_severity(text: &[u8]) -> (Severity, &[u8]) {
    // Exact case-insensitive match against the 6 standard severity strings.
    // Previous version used prefix matching (e.g., any string starting with
    // "I" matched INFO) which caused false positives like "INVALID" → Info.
    let sev = match text.len() {
        4 if eq_ignore_case_4(text, b"INFO") => Severity::Info,
        4 if eq_ignore_case_4(text, b"WARN") => Severity::Warn,
        5 if eq_ignore_case_5(text, b"DEBUG") => Severity::Debug,
        5 if eq_ignore_case_5(text, b"TRACE") => Severity::Trace,
        5 if eq_ignore_case_5(text, b"ERROR") => Severity::Error,
        5 if eq_ignore_case_5(text, b"FATAL") => Severity::Fatal,
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
fn eq_ignore_case_match(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x | 0x20 == y | 0x20)
}

/// Case-insensitive 4-byte comparison. Uses `|0x20` which maps uppercase
/// ASCII letters to lowercase. This is NOT a general case-fold — it has
/// collisions for non-letters (e.g., `@` |0x20 = `` ` ``). Safe here because
/// the comparison targets ("INFO", "WARN") are all ASCII letters.
#[inline(always)]
fn eq_ignore_case_4(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20
        && a[1] | 0x20 == b[1] | 0x20
        && a[2] | 0x20 == b[2] | 0x20
        && a[3] | 0x20 == b[3] | 0x20
}

/// Case-insensitive 5-byte comparison.
#[inline(always)]
fn eq_ignore_case_5(a: &[u8], b: &[u8]) -> bool {
    a[0] | 0x20 == b[0] | 0x20
        && a[1] | 0x20 == b[1] | 0x20
        && a[2] | 0x20 == b[2] | 0x20
        && a[3] | 0x20 == b[3] | 0x20
        && a[4] | 0x20 == b[4] | 0x20
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
pub fn parse_timestamp_nanos(ts: &[u8]) -> Option<u64> {
    if ts.len() < 19 {
        return None;
    }

    let year = parse_4digits(ts, 0) as i64;
    let month = parse_2digits(ts, 5) as u32;
    let day = parse_2digits(ts, 8) as u32;
    let hour = parse_2digits(ts, 11) as u64;
    let min = parse_2digits(ts, 14) as u64;
    let sec = parse_2digits(ts, 17) as u64;

    if year == 0 || month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }

    // Reject years that would overflow u64 nanos (year > ~584 from epoch)
    if year > 2554 {
        return None;
    }

    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
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

    Some(nanos)
}

/// Parse 4 ASCII digits at offset. Returns 0 on non-digit.
#[inline(always)]
#[cfg_attr(kani, kani::ensures(|result: &u16| *result <= 9999))]
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
#[cfg_attr(kani, kani::ensures(|result: &u8| *result <= 99))]
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
#[cfg_attr(kani, kani::ensures(|result: &i64| year < 1970 || *result >= -366))]
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
    fn parse_timestamp_invalid_returns_zero() {
        assert_eq!(parse_timestamp_nanos(b"not a timestamp"), None);
        assert_eq!(parse_timestamp_nanos(b""), None);
        assert_eq!(parse_timestamp_nanos(b"2024"), None);
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
    /// Oracle proof: days_from_civil matches a naive year/month
    /// iteration for all valid dates in [1970, 2100].
    ///
    /// Uses a completely different algorithm (cumulative day counting)
    /// from the Hinnant formula. Kani can't use chrono, so this naive
    /// oracle serves as the Kani-compatible reference. A chrono-based
    /// oracle test below covers the same property in test mode.
    #[kani::proof]
    fn verify_days_from_civil_oracle() {
        let year: i64 = kani::any();
        let month: u32 = kani::any();
        let day: u32 = kani::any();

        kani::assume(year >= 1970 && year <= 2100);
        kani::assume(month >= 1 && month <= 12);
        kani::assume(day >= 1 && day <= 31);

        let result = days_from_civil(year, month, day);

        let oracle = naive_days_from_epoch(year, month, day);
        assert!(
            result == oracle,
            "days_from_civil disagrees with naive oracle"
        );
    }

    fn naive_days_from_epoch(year: i64, month: u32, day: u32) -> i64 {
        let mut days: i64 = 0;
        let mut y = 1970i64;
        while y < year {
            days += if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                366
            } else {
                365
            };
            y += 1;
        }
        let month_days: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let mut m = 1u32;
        while m < month {
            let mut d = month_days[(m - 1) as usize];
            if m == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                d += 1;
            }
            days += d;
            m += 1;
        }
        days + day as i64 - 1
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

        // Exact length required — no prefix matching
        assert!(matches!(
            parse_severity(b"INFORMATION").0,
            Severity::Unspecified
        ));
        assert!(matches!(
            parse_severity(b"WARNING").0,
            Severity::Unspecified
        ));
        assert!(matches!(parse_severity(b"TRAMP").0, Severity::Unspecified));
        assert!(matches!(parse_severity(b"INF").0, Severity::Unspecified));
    }

    /// Prove parse_severity ONLY returns non-Unspecified for the 6
    /// standard level strings (any case). No false positives.
    #[kani::proof]
    fn verify_parse_severity_no_false_positives() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= 8);
        let text = &bytes[..len];
        let (sev, _) = parse_severity(text);

        if !matches!(sev, Severity::Unspecified) {
            // Must be exactly one of the 6 standard levels
            assert!(
                eq_ignore_case_match(text, b"TRACE")
                    || eq_ignore_case_match(text, b"DEBUG")
                    || eq_ignore_case_match(text, b"INFO")
                    || eq_ignore_case_match(text, b"WARN")
                    || eq_ignore_case_match(text, b"ERROR")
                    || eq_ignore_case_match(text, b"FATAL"),
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
    fn verify_encode_fixed64() {
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

        // Verify tag bytes
        let mut tag_buf = Vec::new();
        encode_varint(&mut tag_buf, ((field_number as u64) << 3) | 0);
        let mut i = 0;
        while i < tag_len {
            assert!(buf[i] == tag_buf[i], "varint_field tag mismatch");
            i += 1;
        }

        // Verify value bytes
        let mut val_buf = Vec::new();
        encode_varint(&mut val_buf, value);
        i = 0;
        while i < val_len {
            assert!(
                buf[tag_len + i] == val_buf[i],
                "varint_field value mismatch"
            );
            i += 1;
        }
    }

    /// Prove parse_timestamp_nanos never panics for any 32-byte input.
    #[kani::proof]
    #[kani::unwind(12)]
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
    #[kani::proof]
    #[kani::unwind(12)]
    fn verify_encode_bytes_field_content() {
        let field_number: u32 = kani::any();
        kani::assume(field_number > 0 && field_number <= 100);
        let data_len: usize = kani::any_where(|&l: &usize| l <= 8);
        let data: [u8; 8] = kani::any();

        let mut buf = Vec::new();
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
    fn verify_parse_4digits_contract() {
        let s: [u8; 8] = kani::any();
        let off: usize = kani::any_where(|&o: &usize| o <= 4);
        parse_4digits(&s, off);
    }

    /// Verify parse_2digits contract: output ≤ 99.
    #[kani::proof_for_contract(parse_2digits)]
    #[kani::unwind(3)]
    fn verify_parse_2digits_contract() {
        let s: [u8; 8] = kani::any();
        let off: usize = kani::any_where(|&o: &usize| o <= 6);
        parse_2digits(&s, off);
    }

    /// Verify days_from_civil contract: year ≥ 1970 implies result ≥ -366.
    #[kani::proof_for_contract(days_from_civil)]
    fn verify_days_from_civil_contract() {
        let year: i64 = kani::any();
        let month: u32 = kani::any();
        let day: u32 = kani::any();
        kani::assume(year >= 1970 && year <= 2200);
        kani::assume(month >= 1 && month <= 12);
        kani::assume(day >= 1 && day <= 31);
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
    #[kani::unwind(12)]
    fn verify_parse_timestamp_compositional() {
        let ts: [u8; 24] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l >= 19 && l <= 24);
        let result = parse_timestamp_nanos(&ts[..len]);

        // If it parsed successfully, the result must be bounded
        if let Some(nanos) = result {
            // Year 2554 upper bound: force u64 to avoid intermediate overflow.
            const MAX_NANOS: u64 = 80_763_609_600_000_000_000;
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
}
