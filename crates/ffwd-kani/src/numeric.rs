#![allow(clippy::indexing_slicing)]
//! Reference numeric parsing implementations.

/// Reference integer parser using `i128` accumulator to detect overflow.
///
/// Parses an optional leading `-` followed by ASCII digits. Returns `None`
/// for empty input, non-digit bytes, lone `-`, or values outside `i64` range.
///
/// Input length is capped at 20 bytes (max `i64` digit count + sign) to
/// prevent `i128` accumulator overflow.
pub fn parse_int_oracle(bytes: &[u8]) -> Option<i64> {
    // i64::MIN is "-9223372036854775808" = 20 chars.
    // Beyond 20 digits the i128 accumulator could silently wrap.
    if bytes.is_empty() || bytes.len() > 20 {
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
        if !(b'0'..=b'9').contains(&b) {
            return None;
        }
        acc = acc * 10 + (b - b'0') as i128;
        i += 1;
    }
    if neg {
        acc = -acc;
    }
    if acc < i64::MIN as i128 || acc > i64::MAX as i128 {
        None
    } else {
        Some(acc as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_int_basic() {
        assert_eq!(parse_int_oracle(b"0"), Some(0));
        assert_eq!(parse_int_oracle(b"42"), Some(42));
        assert_eq!(parse_int_oracle(b"-1"), Some(-1));
        assert_eq!(parse_int_oracle(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_int_oracle(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn parse_int_invalid() {
        assert_eq!(parse_int_oracle(b""), None);
        assert_eq!(parse_int_oracle(b"-"), None);
        assert_eq!(parse_int_oracle(b"abc"), None);
        assert_eq!(parse_int_oracle(b"12x"), None);
    }

    #[test]
    fn parse_int_overflow() {
        assert_eq!(parse_int_oracle(b"9223372036854775808"), None);
        assert_eq!(parse_int_oracle(b"-9223372036854775809"), None);
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_parse_int_oracle_no_panic() {
        let input: [u8; 4] = kani::any();
        let _ = parse_int_oracle(&input);
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_parse_int_oracle_sign_consistency() {
        let input: [u8; 4] = kani::any();
        if let Some(val) = parse_int_oracle(&input) {
            if input[0] == b'-' {
                assert!(val <= 0);
            } else {
                assert!(val >= 0);
            }
        }
    }
}
