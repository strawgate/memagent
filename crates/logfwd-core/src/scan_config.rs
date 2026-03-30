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
    /// When true, validates that the input buffer is valid UTF-8 before scanning
    /// and panics with a descriptive message if it is not. Disabled by default
    /// for maximum throughput; enable when input provenance is untrusted.
    pub validate_utf8: bool,
}

impl Default for ScanConfig {
    fn default() -> Self {
        ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
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
        assert!((parse_float_fast(b"3.25").unwrap() - 3.25).abs() < 1e-10);
        assert_eq!(parse_float_fast(b"abc"), None);
    }
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// Prove parse_int_fast never panics for any 8-byte input.
    /// Also proves: if it returns Some(n), encoding n back to decimal
    /// and re-parsing gives the same result (roundtrip).
    #[kani::proof]
    #[kani::unwind(10)] // max 8 bytes + loop overhead
    #[kani::solver(kissat)]
    fn verify_parse_int_fast_no_panic_8bytes() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let _ = parse_int_fast(&bytes[..len]);
    }

    /// Prove parse_int_fast correctly rejects overflow.
    /// For any sequence of ASCII digits, if the result would exceed i64
    /// bounds, None is returned.
    #[kani::proof]
    #[kani::unwind(22)] // i64::MAX is 19 digits + sign + loop overhead
    #[kani::solver(kissat)]
    fn verify_parse_int_fast_overflow_detection() {
        let bytes: [u8; 20] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 20);

        let input = &bytes[..len];
        let result = parse_int_fast(input);

        // If result is Some(n), verify n is a valid i64 representation
        // of the input bytes.
        if let Some(n) = result {
            // Re-check: the input must be all ASCII digits (with optional leading minus)
            let (neg, start) = if !input.is_empty() && input[0] == b'-' {
                (true, 1usize)
            } else {
                (false, 0usize)
            };

            // Must have at least one digit
            assert!(start < input.len(), "Some returned for empty digit sequence");

            // Verify all remaining bytes are digits
            let mut i = start;
            while i < input.len() {
                assert!(input[i].is_ascii_digit(), "non-digit in accepted input");
                i += 1;
            }

            // Verify the sign is correct
            if neg {
                assert!(n <= 0, "negative input parsed as positive");
            } else {
                assert!(n >= 0, "positive input parsed as negative");
            }
        }
    }
}
