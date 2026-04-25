// Log event generation helpers.
// xtask-verify: allow(pub_module_needs_tests) // parse_iso8601_to_epoch_ms tested via generator/tests/timestamps.rs

/// Write the message field value (without key or quotes) into a buffer.
///
/// Used by both JSON serialization and Arrow field extraction.
pub(crate) fn write_message_value(buf: &mut Vec<u8>, fields: &LogEventFields<'_>) {
    if let Some(escaped) = fields.message_template {
        buf.extend_from_slice(escaped);
    } else {
        // Avoid `write!()` formatting overhead — build from parts directly.
        buf.extend_from_slice(fields.method.as_bytes());
        buf.push(b' ');
        buf.extend_from_slice(fields.path.as_bytes());
        buf.push(b'/');
        let mut itoa_buf = itoa::Buffer::new();
        buf.extend_from_slice(itoa_buf.format(fields.id).as_bytes());
        buf.push(b' ');
        buf.extend_from_slice(itoa_buf.format(fields.status).as_bytes());
    }
}

/// Write a u64 as 16 zero-padded lowercase hex digits into a fixed buffer.
fn write_hex16(out: &mut [u8; 16], val: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut v = val;
    // Fill right-to-left for natural zero-padding.
    let mut i = 15_i32;
    while i >= 0 {
        out[i as usize] = HEX[(v & 0xf) as usize];
        v >>= 4;
        i -= 1;
    }
}

/// Serialize a `LogEventFields` as a single JSON object into `buf`.
fn write_log_fields_json(buf: &mut Vec<u8>, fields: &LogEventFields<'_>) {
    let ts = &fields.timestamp;

    // Common prefix: timestamp, level, message
    buf.extend_from_slice(b"{\"timestamp\":\"");
    ts.write_iso8601_into(buf);
    buf.extend_from_slice(b"\",\"level\":\"");
    buf.extend_from_slice(fields.level.as_bytes());
    buf.extend_from_slice(b"\",\"message\":\"");
    write_message_value(buf, fields);
    buf.push(b'"');

    // Shared numeric/hex helpers
    let mut itoa_buf = itoa::Buffer::new();
    let mut hex_buf = [0u8; 16];

    match &fields.complexity {
        ComplexityFields::Simple => {
            write_simple_suffix(buf, fields, &mut itoa_buf, &mut hex_buf);
        }
        ComplexityFields::Complex {
            bytes_in,
            bytes_out,
            variant,
        } => {
            buf.extend_from_slice(b",\"duration_ms\":");
            buf.extend_from_slice(itoa_buf.format(fields.duration_ms).as_bytes());
            buf.extend_from_slice(b",\"request_id\":\"");
            write_hex16(&mut hex_buf, fields.request_id);
            buf.extend_from_slice(&hex_buf);
            buf.extend_from_slice(b"\",\"service\":\"");
            buf.extend_from_slice(fields.service.as_bytes());
            buf.extend_from_slice(b"\",\"status\":");
            buf.extend_from_slice(itoa_buf.format(fields.status).as_bytes());
            buf.extend_from_slice(b",\"bytes_in\":");
            buf.extend_from_slice(itoa_buf.format(*bytes_in).as_bytes());
            buf.extend_from_slice(b",\"bytes_out\":");
            buf.extend_from_slice(itoa_buf.format(*bytes_out).as_bytes());

            match variant {
                ComplexVariant::WithHeadersAndTags => {
                    buf.extend_from_slice(
                        b",\"headers\":{\"content-type\":\"application/json\",\"x-request-id\":\"",
                    );
                    buf.extend_from_slice(&hex_buf);
                    buf.extend_from_slice(b"\"},\"tags\":[\"web\",\"");
                    buf.extend_from_slice(fields.service.as_bytes());
                    buf.extend_from_slice(b"\",\"");
                    buf.extend_from_slice(fields.level.as_bytes());
                    buf.extend_from_slice(b"\"]}");
                }
                ComplexVariant::WithUpstream { upstream_ms } => {
                    buf.extend_from_slice(b",\"upstream\":[{\"host\":\"10.0.0.1\",\"latency_ms\":");
                    buf.extend_from_slice(itoa_buf.format(*upstream_ms).as_bytes());
                    buf.extend_from_slice(b"},{\"host\":\"10.0.0.2\",\"latency_ms\":");
                    buf.extend_from_slice(itoa_buf.format(fields.duration_ms).as_bytes());
                    buf.extend_from_slice(b"}]}");
                }
                ComplexVariant::Basic => {
                    buf.push(b'}');
                }
            }
        }
    }
}

/// Write the Simple suffix: `,"duration_ms":N,"request_id":"...","service":"...","status":N}`
fn write_simple_suffix(
    buf: &mut Vec<u8>,
    fields: &LogEventFields<'_>,
    itoa_buf: &mut itoa::Buffer,
    hex_buf: &mut [u8; 16],
) {
    buf.extend_from_slice(b",\"duration_ms\":");
    buf.extend_from_slice(itoa_buf.format(fields.duration_ms).as_bytes());
    buf.extend_from_slice(b",\"request_id\":\"");
    write_hex16(hex_buf, fields.request_id);
    buf.extend_from_slice(hex_buf.as_slice());
    buf.extend_from_slice(b"\",\"service\":\"");
    buf.extend_from_slice(fields.service.as_bytes());
    buf.extend_from_slice(b"\",\"status\":");
    buf.extend_from_slice(itoa_buf.format(fields.status).as_bytes());
    buf.push(b'}');
}

// ---------------------------------------------------------------------------
// Date math helpers (Howard Hinnant algorithms)
// ---------------------------------------------------------------------------

/// Days from 1970-01-01 to the given civil date.
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
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

/// Civil date from days since 1970-01-01.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
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

/// Decompose epoch milliseconds into `(year, month, day, hour, min, sec, ms)`.
fn epoch_ms_to_parts(epoch_ms: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    let day_count = epoch_ms.div_euclid(86_400_000);
    let day_ms = epoch_ms.rem_euclid(86_400_000) as u32;
    let (year, month, day) = civil_from_days(day_count);
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

    let days = days_from_civil(year, month, day);
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
