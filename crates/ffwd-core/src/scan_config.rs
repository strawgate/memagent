// scan_config.rs — Scan configuration types for JSON field extraction.
//
// Defines ScanConfig and FieldSpec, used by the Scanner and the SQL
// transform layer.

use crate::scan_predicate::ScanPredicate;
use alloc::{string::String, vec, vec::Vec};
/// Specification for a single field to extract.
pub struct FieldSpec {
    /// Primary field name.
    pub name: String,
    /// Alternative names that map to this field.
    pub aliases: Vec<String>,
}

/// Controls which fields to extract and whether to capture each full input line.
pub struct ScanConfig {
    /// Fields to extract. Empty = extract all (SELECT *).
    pub wanted_fields: Vec<FieldSpec>,
    /// True if the query uses SELECT * (extract everything).
    pub extract_all: bool,
    /// Optional output field name that receives the full unparsed line.
    ///
    /// When `Some(name)`, the scanner appends the original line bytes into
    /// column `name` for every row.
    pub line_field_name: Option<String>,
    /// When true, validates that the input buffer is valid UTF-8 before scanning.
    /// Invalid bytes are reported as a descriptive scanner error before internal
    /// builder paths can reach `finish_batch()` UTF-8 assumptions. Disabled by
    /// default for maximum throughput; enable when input provenance is untrusted.
    pub validate_utf8: bool,
    /// Optional row-level predicate evaluated during scanning.
    ///
    /// When set, the scanner extracts predicate fields, evaluates the predicate,
    /// and skips rows that don't match (no `begin_row`/`end_row` calls).
    /// This is advisory — the SQL transform still applies all predicates for
    /// correctness.
    pub row_predicate: Option<ScanPredicate>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        }
    }
}

impl ScanConfig {
    /// Returns true when scanner line capture is enabled.
    #[inline]
    pub fn captures_line(&self) -> bool {
        self.line_field_name.is_some()
    }

    /// Is this field name wanted?
    #[inline]
    pub fn is_wanted(&self, key: &[u8]) -> bool {
        if self.extract_all {
            return true;
        }
        for fs in &self.wanted_fields {
            if key.eq_ignore_ascii_case(fs.name.as_bytes()) {
                return true;
            }
            for a in &fs.aliases {
                if key.eq_ignore_ascii_case(a.as_bytes()) {
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
///
/// No Kani contract needed: the function handles all inputs gracefully via
/// checked_mul/checked_add. The only precondition would be "bytes is not
/// empty", but returning None on empty is the intended behavior — expressing
/// this as a `#[requires]` would force every caller to guard against empty
/// input even though the function already does the right thing.
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
///
/// No Kani contract needed: returns None on invalid UTF-8 or unparsable content.
/// The function is total over byte slices (always produces a result) — no
/// meaningful precondition exists that would reduce proof burden.
#[inline(always)]
pub fn parse_float_fast(bytes: &[u8]) -> Option<f64> {
    let s = core::str::from_utf8(bytes).ok()?;
    s.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

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

    /// Regression test: is_wanted must return true when the JSON key differs
    /// only in ASCII case from the configured field name.
    ///
    /// This covers the scenario where a SQL query references `Level` but the
    /// JSON log line contains the key `level` (or any other case variant).
    #[test]
    fn test_is_wanted_case_insensitive() {
        let config = ScanConfig {
            wanted_fields: vec![FieldSpec {
                name: "level".to_string(),
                aliases: vec!["severity".to_string()],
            }],
            extract_all: false,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };

        // Exact match still works.
        assert!(config.is_wanted(b"level"));
        // Case-mismatched key must also match (the regression case).
        assert!(config.is_wanted(b"Level"));
        assert!(config.is_wanted(b"LEVEL"));
        assert!(config.is_wanted(b"LeVeL"));
        // Alias case-insensitive match.
        assert!(config.is_wanted(b"Severity"));
        assert!(config.is_wanted(b"SEVERITY"));
        // Unrelated key must not match.
        assert!(!config.is_wanted(b"message"));
    }
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;
    use alloc::string::ToString;

    /// Prove parse_int_fast never panics for any 8-byte input.
    /// NOTE: The roundtrip check (to_string + re-parse) is intentionally
    /// omitted here — it adds 500s+ of solver time modeling the allocator
    /// and digit-formatting logic. The oracle proof below covers correctness.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::solver(kissat)]
    fn verify_parse_int_fast_no_panic_8bytes() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let input = &bytes[..len];
        let _ = parse_int_fast(input);

        // Cover both parsing outcomes
        kani::cover!(parse_int_fast(input).is_some(), "valid integer parsed");
        kani::cover!(parse_int_fast(input).is_none(), "invalid input rejected");
    }

    /// Prove parse_int_fast matches a reference i128 oracle.
    ///
    /// The oracle independently parses the input as i128, then checks:
    /// - If the input is valid digits (optional minus + ASCII digits)
    ///   AND the value fits in i64 → parse_int_fast must return Some(value)
    /// - If the input is invalid or overflows i64 → parse_int_fast must
    ///   return None
    ///
    /// Bounded to 10 bytes to keep solver tractable (~30s vs 534s at 20).
    /// 10 bytes covers all i32 values and most i64 (up to 9,999,999,999).
    /// See verify_parse_int_fast_overflow_boundary for i64 overflow coverage.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::solver(kissat)]
    fn verify_parse_int_fast_vs_oracle() {
        let bytes: [u8; 10] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 10);

        let input = &bytes[..len];
        let result = parse_int_fast(input);

        let expected = ffwd_kani::numeric::parse_int_oracle(input);

        assert!(result == expected, "parse_int_fast disagrees with oracle");
    }

    /// Targeted proof for i64 overflow boundary in parse_int_fast.
    ///
    /// The main oracle proof (verify_parse_int_fast_vs_oracle) is bounded to
    /// 10 bytes for solver tractability, which doesn't reach i64 overflow
    /// (19+ digits). This proof specifically covers the overflow region by
    /// constraining inputs to pure ASCII digits of length 19-20 and an
    /// optional leading minus sign, then verifying against the oracle.
    ///
    /// By restricting to digit-only inputs, the solver state space is
    /// dramatically smaller than fully symbolic 20-byte arrays.
    ///
    /// Gated behind `kani-slow`: 20-byte constrained array + overflow math ~30-60s.
    #[cfg(feature = "kani-slow")]
    #[kani::proof]
    #[kani::unwind(22)]
    #[kani::solver(kissat)]
    fn verify_parse_int_fast_overflow_boundary() {
        // 20 bytes: optional minus + up to 20 digits covers the i64 boundary.
        // i64::MAX = 9223372036854775807 (19 digits)
        // i64::MIN = -9223372036854775808 (20 chars with minus)
        let len: usize = kani::any();
        kani::assume(len >= 18 && len <= 20);

        let has_minus: bool = kani::any();
        let start = if has_minus { 1 } else { 0 };
        // Ensure we have at least 1 digit after optional minus
        kani::assume(len > start);

        let mut bytes = [0u8; 20];
        if has_minus {
            bytes[0] = b'-';
        }
        // Fill digits as constrained symbolic values (b'0'..=b'9' only)
        let mut i = start;
        while i < len {
            let d: u8 = kani::any();
            kani::assume(d >= b'0' && d <= b'9');
            bytes[i] = d;
            i += 1;
        }

        let input = &bytes[..len];
        let result = parse_int_fast(input);
        let expected = ffwd_kani::numeric::parse_int_oracle(input);

        assert!(
            result == expected,
            "overflow boundary: disagrees with oracle"
        );

        kani::cover!(result.is_none() && len >= 19, "overflow returns None");
        kani::cover!(result.is_some() && len >= 19, "large valid i64");
    }
}
