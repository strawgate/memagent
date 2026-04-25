//! UTC timestamp formatting for Elasticsearch bulk documents (no chrono dependency).

// ---------------------------------------------------------------------------
// UTC timestamp formatting (no chrono dependency)
// ---------------------------------------------------------------------------

/// Write a Unix timestamp (seconds) as `YYYY-MM-DDTHH:MM:SS` directly into
/// `buf[0..19]`, zero-allocation.
///
/// Handles dates from 1970 to 9999 correctly via the Gregorian calendar.
/// Timestamps beyond year 9999 are clamped to 9999-12-31T23:59:59 to fit
/// the 19-byte ASCII buffer and comply with ISO 8601 4-digit year limit.
pub(crate) fn write_unix_timestamp_utc_into(buf: &mut [u8; 19], mut secs: u64) {
    const MAX_SECS: u64 = 253402300799; // 9999-12-31T23:59:59
    if secs > MAX_SECS {
        secs = MAX_SECS;
    }

    let days = secs / 86400;
    let time = secs % 86400;
    let h = time / 3600;
    let m = (time % 3600) / 60;
    let s = time % 60;

    let mut year = 1970u32;
    let mut remaining = days;

    // Jump in 400-year blocks (146097 days) to ensure O(1) performance
    // regardless of how large the timestamp is.
    let blocks400 = remaining / 146097;
    year += blocks400 as u32 * 400;
    remaining %= 146097;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }
    let leap = is_leap_year(year);
    let month_days: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1u32;
    for &md in &month_days {
        if remaining < md as u64 {
            break;
        }
        remaining -= md as u64;
        month += 1;
    }
    let day = (remaining + 1) as u32;

    // Write YYYY-MM-DDTHH:MM:SS into the 19-byte buffer.
    debug_assert!(year <= 9999, "year must be <= 9999, got {year}");
    buf[0] = b'0' + (year / 1000) as u8;
    buf[1] = b'0' + (year / 100 % 10) as u8;
    buf[2] = b'0' + (year / 10 % 10) as u8;
    buf[3] = b'0' + (year % 10) as u8;
    buf[4] = b'-';
    buf[5] = b'0' + (month / 10) as u8;
    buf[6] = b'0' + (month % 10) as u8;
    buf[7] = b'-';
    buf[8] = b'0' + (day / 10) as u8;
    buf[9] = b'0' + (day % 10) as u8;
    buf[10] = b'T';
    buf[11] = b'0' + (h / 10) as u8;
    buf[12] = b'0' + (h % 10) as u8;
    buf[13] = b':';
    buf[14] = b'0' + (m / 10) as u8;
    buf[15] = b'0' + (m % 10) as u8;
    buf[16] = b':';
    buf[17] = b'0' + (s / 10) as u8;
    buf[18] = b'0' + (s % 10) as u8;
}

/// Fill `out[0..47]` with the full `@timestamp` suffix used in bulk documents:
///
/// ```text
/// ,"@timestamp":"YYYY-MM-DDTHH:MM:SS.fffffffffZ"}
/// ```
///
/// The leading `,` is at index 0; callers that need the no-comma form
/// (empty-document case) can simply slice `&out[1..]`.
///
/// Zero-allocation: all work happens on the stack-allocated `out` buffer.
pub(crate) fn write_ts_suffix(out: &mut [u8; 47], secs: u64, frac: u64) {
    debug_assert!(frac < 1_000_000_000, "frac must be < 1e9, got {frac}");
    out[0] = b',';
    out[1..15].copy_from_slice(b"\"@timestamp\":\"");
    let mut dt = [0u8; 19];
    write_unix_timestamp_utc_into(&mut dt, secs);
    out[15..34].copy_from_slice(&dt);
    out[34] = b'.';
    // Write 9-digit zero-padded nanosecond fraction.
    let mut f = frac;
    for i in (35..44).rev() {
        out[i] = b'0' + (f % 10) as u8;
        f /= 10;
    }
    out[44] = b'Z';
    out[45] = b'"';
    out[46] = b'}';
}

/// Format a Unix timestamp (seconds) as `YYYY-MM-DDTHH:MM:SS` in UTC.
///
/// Wraps [`write_unix_timestamp_utc_into`] for use in unit tests.
/// Production code uses [`write_ts_suffix`] directly to avoid the String allocation.
#[cfg(test)]
pub(super) fn format_unix_timestamp_utc(secs: u64) -> String {
    let mut buf = [0u8; 19];
    write_unix_timestamp_utc_into(&mut buf, secs);
    // `[u8; 19]` is Copy; Vec::from avoids the extra clone that to_vec() would do.
    // write_unix_timestamp_utc_into only writes ASCII digits and punctuation.
    String::from_utf8(Vec::from(buf)).expect("timestamp bytes are valid UTF-8")
}

#[cfg(test)]
pub(super) fn write_ts_suffix_simple(secs: u64, frac: u64) -> Vec<u8> {
    let secs = secs.min(253402300799); // 9999-12-31T23:59:59
    let ts = format_unix_timestamp_utc(secs);
    format!(",\"@timestamp\":\"{ts}.{frac:09}Z\"}}").into_bytes()
}

pub(crate) fn is_leap_year(y: u32) -> bool {
    (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400)
}
