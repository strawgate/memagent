// scanner.rs — Scan configuration types for JSON field extraction.
//
// Defines ScanConfig and FieldSpec, used by the Scanner and the SQL
// transform layer.

use alloc::{string::String, vec, vec::Vec};
/// Specification for a single field to extract.
pub struct FieldSpec {
    /// Primary field name.
    pub name: String,
    /// Alternative names that map to this field.
    pub aliases: Vec<String>,
}

/// A simple predicate that can be evaluated during JSON scanning to skip
/// entire rows before building Arrow columns.
///
/// These are **advisory** — the SQL transform still applies all predicates,
/// so correctness doesn't depend on pushdown. Only top-level AND-chain
/// predicates are extracted; OR branches are left for DataFusion.
///
/// The scanner uses a `u64` bitmask to track matches, so at most 64
/// predicates are supported. If more are provided, the scanner passes all
/// rows through and lets DataFusion handle filtering.
///
/// See `dev-docs/PREDICATE_PUSHDOWN.md` for the design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowPredicate {
    /// Field value must equal this byte string exactly.
    /// Extracted from `WHERE field = 'literal'`.
    Eq {
        /// JSON field name to match.
        field: Vec<u8>,
        /// Expected value (raw bytes, no JSON escaping).
        value: Vec<u8>,
    },

    /// Field value must be one of these byte strings.
    /// Extracted from `WHERE field IN ('a', 'b', 'c')`.
    In {
        /// JSON field name to match.
        field: Vec<u8>,
        /// Set of acceptable values.
        values: Vec<Vec<u8>>,
    },

    /// Field value must NOT equal this byte string.
    /// Extracted from `WHERE field != 'literal'` or `WHERE field <> 'literal'`.
    NotEq {
        /// JSON field name to match.
        field: Vec<u8>,
        /// Value that must NOT appear.
        value: Vec<u8>,
    },
}

impl RowPredicate {
    /// The field name this predicate applies to.
    #[inline]
    pub fn field_name(&self) -> &[u8] {
        match self {
            Self::Eq { field, .. } | Self::In { field, .. } | Self::NotEq { field, .. } => field,
        }
    }

    /// Check whether a raw (unescaped) JSON string value satisfies this predicate.
    #[inline]
    pub fn matches_bytes(&self, value: &[u8]) -> bool {
        match self {
            Self::Eq {
                value: expected, ..
            } => value == expected.as_slice(),
            Self::In { values, .. } => values.iter().any(|v| value == v.as_slice()),
            Self::NotEq {
                value: expected, ..
            } => value != expected.as_slice(),
        }
    }
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
    /// Row-level predicates pushed down from the SQL WHERE clause.
    ///
    /// When non-empty, the scanner probes each JSON line for these predicates
    /// *before* materializing Arrow columns. Lines that fail any predicate are
    /// skipped entirely — no `begin_row`/`end_row`, no field extraction.
    ///
    /// All predicates are AND'd: a row must satisfy every predicate to pass.
    pub row_predicates: Vec<RowPredicate>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
            row_predicates: vec![],
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

    /// True if any row predicates are configured for scanner-level filtering.
    #[inline]
    pub fn has_row_predicates(&self) -> bool {
        !self.row_predicates.is_empty()
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
    let s = core::str::from_utf8(bytes).ok()?;
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

        // Reference oracle: parse as i128 to detect i64 overflow
        let expected = parse_int_oracle(input);

        assert!(result == expected, "parse_int_fast disagrees with oracle");
    }

    /// Reference parser using i128 to avoid overflow.
    /// Returns the same result parse_int_fast should return.
    fn parse_int_oracle(bytes: &[u8]) -> Option<i64> {
        if bytes.is_empty() {
            return None;
        }
        let (neg, start) = if bytes[0] == b'-' {
            (true, 1usize)
        } else {
            (false, 0usize)
        };
        if start >= bytes.len() {
            return None;
        }
        let mut acc: i128 = 0;
        let mut i = start;
        while i < bytes.len() {
            let b = bytes[i];
            if !b.is_ascii_digit() {
                return None;
            }
            acc = acc * 10 + (b - b'0') as i128;
            i += 1;
        }
        if neg {
            acc = -acc;
        }
        // Check i64 bounds
        if acc < i64::MIN as i128 || acc > i64::MAX as i128 {
            None
        } else {
            Some(acc as i64)
        }
    }
}
