//! Kani formal verification proofs for the Elasticsearch timestamp utilities.
#![cfg(kani)]

use super::*;
use crate::elasticsearch::timestamp::{is_leap_year, write_ts_suffix};

/// Prove is_leap_year satisfies all four cases of the Gregorian calendar rule
/// exhaustively for every possible u32 year value.
#[kani::proof]
fn verify_is_leap_year_gregorian_rules() {
    let y: u32 = kani::any();

    kani::cover!(is_leap_year(y), "at least one leap year found");
    kani::cover!(!is_leap_year(y), "at least one non-leap year found");

    // Rule 1: divisible by 400 → always leap
    if y % 400 == 0 {
        assert!(is_leap_year(y), "div by 400 must be leap: {y}");
    }
    // Rule 2: divisible by 100 but not 400 → not leap
    if y % 100 == 0 && y % 400 != 0 {
        assert!(
            !is_leap_year(y),
            "div by 100 but not 400 must not be leap: {y}"
        );
    }
    // Rule 3: divisible by 4 but not 100 → leap
    if y % 4 == 0 && y % 100 != 0 {
        assert!(is_leap_year(y), "div by 4 but not 100 must be leap: {y}");
    }
    // Rule 4: not divisible by 4 → not leap
    if y % 4 != 0 {
        assert!(!is_leap_year(y), "not div by 4 must not be leap: {y}");
    }
}

/// Prove write_ts_suffix produces valid ASCII for any representative
/// timestamp in the first 16 years of the epoch (bounded for solver speed).
#[kani::proof]
fn verify_write_ts_suffix_ascii() {
    // Restrict to first ~16 years (0..504921600) to keep the solver tractable.
    let secs: u64 = kani::any();
    kani::assume(secs < 504_921_600); // 1970-01-01 to 1985-12-31
    let frac: u64 = kani::any();
    kani::assume(frac < 1_000_000_000);

    let mut buf = [0u8; 47];
    write_ts_suffix(&mut buf, secs, frac);

    // Every byte must be printable ASCII (32..=126).
    for &b in &buf {
        assert!(b >= 32 && b <= 126, "non-printable byte in ts_suffix");
    }
    // Structural checks: leading comma, fixed string markers.
    assert_eq!(buf[0], b',');
    assert_eq!(buf[1], b'"');
    assert_eq!(buf[34], b'.');
    assert_eq!(buf[44], b'Z');
    assert_eq!(buf[45], b'"');
    assert_eq!(buf[46], b'}');
}
