// Date/time utilities for the generator.
//
// Pure date math functions for converting between epoch milliseconds and
// civil (calendar) date-time components.
// xtask-verify: allow(pub_module_needs_tests) // public helpers tested via generator/tests/record_profile.rs and generator/tests/timestamps.rs

/// Compute the number of days from 1970-01-01 to the given civil (Gregorian) date.
///
/// Uses the Euclidean algorithm from Howard Hinnant's date algorithms paper.
pub fn compute_days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year } as i64;
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

/// Compute the civil (Gregorian) calendar date `(year, month, day)` from days since 1970-01-01.
pub fn compute_civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = (y + i64::from(month <= 2)) as i32;
    (year, month, day)
}

/// Convert epoch milliseconds to calendar/time components `(year, month, day, hour, min, sec, ms)`.
pub fn convert_epoch_ms_to_parts(epoch_ms: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    let day_count = epoch_ms.div_euclid(86_400_000);
    let day_ms = epoch_ms.rem_euclid(86_400_000) as u32;
    let (year, month, day) = compute_civil_from_days(day_count);
    let hour = day_ms / 3_600_000;
    let min = (day_ms % 3_600_000) / 60_000;
    let sec = (day_ms % 60_000) / 1000;
    let ms = day_ms % 1000;
    (year, month, day, hour, min, sec, ms)
}

/// Parse `YYYY-MM-DDTHH:MM:SSZ` to epoch milliseconds.
pub fn parse_iso8601_to_epoch_ms(s: &str) -> Result<i64, String> {
    let b = s.as_bytes();
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return Err(format!("expected YYYY-MM-DDTHH:MM:SSZ format, got {s:?}"));
    }
    let year = parse_digits(b, 0, 4).ok_or("invalid year")? as i32;
    let month = parse_digits(b, 5, 2).ok_or("invalid month")? as u32;
    let day = parse_digits(b, 8, 2).ok_or("invalid day")? as u32;
    let hour = parse_digits(b, 11, 2).ok_or("invalid hour")? as u32;
    let min = parse_digits(b, 14, 2).ok_or("invalid minute")? as u32;
    let sec = parse_digits(b, 17, 2).ok_or("invalid second")? as u32;

    if !(1..=12).contains(&month) || day < 1 || hour > 23 || min > 59 || sec > 59 {
        return Err(format!("date/time component out of range in {s:?}"));
    }
    let max_day = max_days_in_month(year, month);
    if day > max_day {
        return Err(format!(
            "day {day} out of range for {year:04}-{month:02} (max {max_day}) in {s:?}"
        ));
    }

    let days = compute_days_from_civil(year, month, day);
    let ms = days * 86_400_000 + hour as i64 * 3_600_000 + min as i64 * 60_000 + sec as i64 * 1000;
    Ok(ms)
}

fn max_days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn parse_digits(b: &[u8], offset: usize, count: usize) -> Option<u64> {
    let mut v = 0u64;
    for i in 0..count {
        let c = b[offset + i];
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
    }
    Some(v)
}