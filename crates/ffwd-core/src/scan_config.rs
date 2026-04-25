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
/// this as a #[requires] would force every caller to guard against empty
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
/// No Kani contract needed: returns None on invalid UTF-8 or unparseable content.
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
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::solver(kissat)]
    fn verify_parse_int_fast_no_panic_8bytes() {
        let bytes: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let input = &bytes[..len];
        let _ = parse_int_fast(input);

        // If parser accepts input, decimal re-encode/re-parse must roundtrip.
        if let Some(n) = parse_int_fast(input) {
            let decimal = n.to_string();
            assert!(
                parse_int_fast(decimal.as_bytes()) == Some(n),
                "roundtrip parse mismatch"
            );
        }
    }

    /// Prove parse_int_fast matches a reference i128 oracle for all
    /// inputs up to 20 bytes.
    ///
    /// The oracle independently parses the input as i128, then checks:
    /// - If the input is valid digits (optional minus + ASCII digits)
    ///   AND the value fits in i64 → parse_int_fast must return Some(value)
    /// - If the input is invalid or overflows i64 → parse_int_fast must
    ///   return None
    ///
    /// This is a full behavioral equivalence proof, not just overflow
    /// detection. WARNING: takes ~4 minutes (i128 oracle adds solver
    /// complexity).
    #[kani::proof]
    #[kani::unwind(22)]
    #[kani::solver(kissat)]
    fn verify_parse_int_fast_vs_oracle() {
        let bytes: [u8; 20] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 20);

        let input = &bytes[..len];
        let result = parse_int_fast(input);

        let expected = ffwd_kani::numeric::parse_int_oracle(input);

        assert!(result == expected, "parse_int_fast disagrees with oracle");
    }
}
