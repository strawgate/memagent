//! Byte-level comparison and bitmask helpers for Kani proofs.

/// Bounded byte-by-byte equality assertion suitable for Kani proofs.
///
/// Unlike `assert_eq!` on slices (which may use formatting that Kani cannot
/// handle), this function compares element-by-element with bounded unwind.
pub fn assert_bytes_eq(actual: &[u8], expected: &[u8]) {
    assert_eq!(actual.len(), expected.len());
    let mut i = 0;
    while i < expected.len() {
        assert_eq!(actual[i], expected[i]);
        i += 1;
    }
}

/// Case-insensitive variable-length comparison for ASCII letters.
///
/// Uses proper case-folding (only ASCII letters are folded).
#[cfg_attr(kani, kani::ensures(|result: &bool| {
    !*result || a.len() == b.len()
}))]
pub fn eq_ignore_case_match(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        let x = if a[i].is_ascii_uppercase() {
            a[i] | 0x20
        } else {
            a[i]
        };
        let y = if b[i].is_ascii_uppercase() {
            b[i] | 0x20
        } else {
            b[i]
        };
        if x != y {
            return false;
        }
        i += 1;
    }
    true
}

/// Returns true if the byte is a printable ASCII character (32–126).
#[inline(always)]
#[cfg_attr(kani, kani::ensures(|result: &bool| {
    !*result || (b >= 32 && b <= 126)
}))]
pub fn is_ascii_printable(b: u8) -> bool {
    (32..=126).contains(&b)
}

/// Oracle for `compute_real_quotes`: computes unescaped quote positions
/// from quote and backslash bitmasks.
///
/// Iterates through backslash bits to identify which quotes are escaped.
/// Carries state between blocks via `prev_odd_backslash`.
///
/// Contract: result is always a submask of `quote_bits`.
#[cfg_attr(kani, kani::ensures(|result: &u64| *result & !quote_bits == 0))]
pub fn compute_real_quotes_oracle(
    quote_bits: u64,
    bs_bits: u64,
    prev_odd_backslash: &mut u64,
) -> u64 {
    if bs_bits == 0 && *prev_odd_backslash == 0 {
        return quote_bits;
    }

    let mut escaped: u64 = 0;
    let mut b = bs_bits;

    if *prev_odd_backslash != 0 {
        escaped |= 1;
        b &= !1;
    }

    while b != 0 {
        let pos = b.trailing_zeros() as u64;
        b &= !(1u64 << pos);
        let next_pos = pos + 1;
        if next_pos < 64 {
            escaped |= 1u64 << next_pos;
            b &= !(1u64 << next_pos);
        }
    }

    let last_is_bs = (bs_bits >> 63) & 1 == 1;
    let last_is_escaped = (escaped >> 63) & 1 == 1;
    *prev_odd_backslash = u64::from(last_is_bs && !last_is_escaped);

    quote_bits & !escaped
}

/// Oracle for `prefix_xor`: running XOR that toggles at each set bit.
///
/// Used to compute string interior mask from quote positions.
#[inline(always)]
pub fn prefix_xor_oracle(mut bitmask: u64) -> u64 {
    bitmask ^= bitmask << 1;
    bitmask ^= bitmask << 2;
    bitmask ^= bitmask << 4;
    bitmask ^= bitmask << 8;
    bitmask ^= bitmask << 16;
    bitmask ^= bitmask << 32;
    bitmask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_ignore_case_ascii() {
        assert!(eq_ignore_case_match(b"Hello", b"hello"));
        assert!(eq_ignore_case_match(b"INFO", b"info"));
        assert!(!eq_ignore_case_match(b"abc", b"abz"));
        assert!(!eq_ignore_case_match(b"ab", b"abc"));
    }

    #[test]
    fn is_printable() {
        assert!(is_ascii_printable(b'A'));
        assert!(is_ascii_printable(b' '));
        assert!(is_ascii_printable(b'~'));
        assert!(!is_ascii_printable(0));
        assert!(!is_ascii_printable(127));
    }

    #[test]
    fn compute_real_quotes_no_backslashes() {
        let mut carry = 0u64;
        let result = compute_real_quotes_oracle(0b1010, 0, &mut carry);
        assert_eq!(result, 0b1010);
    }

    #[test]
    fn prefix_xor_basic() {
        assert_eq!(prefix_xor_oracle(0), 0);
        assert_eq!(prefix_xor_oracle(1), u64::MAX);
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof_for_contract(eq_ignore_case_match)]
    fn verify_eq_ignore_case_match_contract() {
        let a: [u8; 4] = kani::any();
        let b: [u8; 4] = kani::any();
        let len_a: usize = kani::any();
        let len_b: usize = kani::any();
        kani::assume(len_a <= 4 && len_b <= 4);
        let res = eq_ignore_case_match(&a[..len_a], &b[..len_b]);
        kani::cover!(res, "match reachable");
        kani::cover!(!res && len_a == len_b, "mismatch same length reachable");
    }

    #[kani::proof_for_contract(is_ascii_printable)]
    fn verify_is_ascii_printable_contract() {
        let b: u8 = kani::any();
        let res = is_ascii_printable(b);
        kani::cover!(res, "printable reachable");
        kani::cover!(!res, "non-printable reachable");
    }

    #[kani::proof_for_contract(compute_real_quotes_oracle)]
    #[kani::unwind(65)]
    #[kani::solver(kissat)]
    fn verify_compute_real_quotes_oracle_contract() {
        let quote_bits: u64 = kani::any();
        let bs_bits: u64 = kani::any();
        let mut carry: u64 = kani::any();
        kani::assume(carry <= 1);
        let res = compute_real_quotes_oracle(quote_bits, bs_bits, &mut carry);
        kani::cover!(res != quote_bits, "escaped quotes masked");
        kani::cover!(res == quote_bits && quote_bits != 0, "no quotes masked");
    }

    #[kani::proof]
    #[kani::unwind(65)]
    #[kani::solver(kissat)]
    fn verify_prefix_xor_oracle_no_panic() {
        let bitmask: u64 = kani::any();
        let res = prefix_xor_oracle(bitmask);
        kani::cover!(res != 0 && bitmask != 0, "non-zero result");
        kani::cover!(res == 0 && bitmask == 0, "zero result");
    }
}
