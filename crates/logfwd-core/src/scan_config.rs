// scanner.rs — Scan configuration types for JSON field extraction.
//
// Defines ScanConfig and FieldSpec, used by all scanner implementations
// (SimdScanner, StreamingSimdScanner) and the SQL transform layer.

/// Specification for a single field to extract.
pub struct FieldSpec {
    pub name: String,
    pub aliases: Vec<String>,
}

/// Controls which fields to extract and whether to keep _raw.
pub struct ScanConfig {
    /// Fields to extract. Empty = extract all (SELECT *).
    pub wanted_fields: Vec<FieldSpec>,
    /// True if the query uses SELECT * (extract everything).
    pub extract_all: bool,
    /// True if _raw column is needed.
    pub keep_raw: bool,
}

impl Default for ScanConfig {
    fn default() -> Self {
        ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
        }
    }
}

impl ScanConfig {
    /// Is this field name wanted?
    #[inline]
    pub fn is_wanted(&self, key: &[u8]) -> bool {
        if self.extract_all {
            return true;
        }
        for fs in &self.wanted_fields {
            if key == fs.name.as_bytes() {
                return true;
            }
            for a in &fs.aliases {
                if key == a.as_bytes() {
                    return true;
                }
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Fast number parsers — used by all builders
// ---------------------------------------------------------------------------

/// Parse a byte slice as a signed 64-bit integer.
/// Returns None on overflow or non-digit bytes.
#[inline(always)]
pub fn parse_int_fast(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };
    if start >= bytes.len() {
        return None;
    }
    let mut acc: i64 = 0;
    if neg {
        for &b in &bytes[start..] {
            if !b.is_ascii_digit() {
                return None;
            }
            acc = acc.checked_mul(10)?;
            acc = acc.checked_sub((b - b'0') as i64)?;
        }
        Some(acc)
    } else {
        for &b in &bytes[start..] {
            if !b.is_ascii_digit() {
                return None;
            }
            acc = acc.checked_mul(10)?;
            acc = acc.checked_add((b - b'0') as i64)?;
        }
        Some(acc)
    }
}

/// Parse a byte slice as f64 using the standard library.
#[inline(always)]
pub fn parse_float_fast(bytes: &[u8]) -> Option<f64> {
    // SAFETY: We only call this on bytes that look like a JSON number,
    // which is always valid ASCII.
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_int_fast() {
        assert_eq!(parse_int_fast(b"0"), Some(0));
        assert_eq!(parse_int_fast(b"42"), Some(42));
        assert_eq!(parse_int_fast(b"-7"), Some(-7));
        assert_eq!(parse_int_fast(b""), None);
        assert_eq!(parse_int_fast(b"-"), None);
        assert_eq!(parse_int_fast(b"3.14"), None);
        assert_eq!(parse_int_fast(b"abc"), None);
        assert_eq!(parse_int_fast(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_int_fast(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_int_fast(b"9223372036854775808"), None); // overflow
        assert_eq!(parse_int_fast(b"-9223372036854775809"), None); // underflow
    }

    #[test]
    fn test_parse_float_fast() {
        assert!((parse_float_fast(b"3.14").unwrap() - 3.14).abs() < 1e-10);
        assert_eq!(parse_float_fast(b"abc"), None);
    }
}
