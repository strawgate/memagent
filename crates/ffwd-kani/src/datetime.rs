//! Reference date/time implementations for verification.

/// Julian Day Number oracle: days since Unix epoch (1970-01-01).
///
/// Uses the Fliegel–Van Flandern formula (pure integer arithmetic,
/// no branches, no loops). This serves as the reference implementation
/// for verifying the production `days_from_civil` function.
pub fn jdn_days_from_epoch(year: i64, month: u32, day: u32) -> i64 {
    let m = month as i64;
    let d = day as i64;
    let a = (14 - m) / 12;
    let y = year + 4800 - a;
    let mo = m + 12 * a - 3;
    let jdn = d + (153 * mo + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    // JDN of 1970-01-01 is 2440588.
    jdn - 2_440_588
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_start() {
        assert_eq!(jdn_days_from_epoch(1970, 1, 1), 0);
    }

    #[test]
    fn known_dates() {
        assert_eq!(jdn_days_from_epoch(2000, 1, 1), 10957);
        assert_eq!(jdn_days_from_epoch(2000, 2, 29), 11016); // leap year
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_jdn_epoch_is_zero() {
        assert_eq!(jdn_days_from_epoch(1970, 1, 1), 0);
    }

    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_jdn_monotonic_within_month() {
        let year: i64 = kani::any();
        let month: u32 = kani::any();
        let day1: u32 = kani::any();
        let day2: u32 = kani::any();

        kani::assume(year >= 1970 && year <= 2100);
        kani::assume(month >= 1 && month <= 12);
        kani::assume(day1 >= 1 && day1 <= 30);
        kani::assume(day2 >= 1 && day2 <= 30);
        kani::assume(day2 > day1);

        let d1 = jdn_days_from_epoch(year, month, day1);
        let d2 = jdn_days_from_epoch(year, month, day2);
        assert!(d2 > d1, "later day must produce larger epoch offset");
    }
}
