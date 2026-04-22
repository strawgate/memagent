//! Synthetic JSON log data generation for testing and benchmarking.

use std::io::{self, Write};

use crate::cli::{bold, green, reset};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimestampParts {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u64,
    pub(crate) minute: u64,
    pub(crate) second: u64,
    pub(crate) millisecond: u64,
}

pub(crate) fn timestamp_parts_for_generated_log(offset_ms: u64) -> TimestampParts {
    // Base timestamp for generated logs: 2024-01-15T10:00:00.000Z.
    const BASE_HOUR_MS: u64 = 10 * 60 * 60 * 1000;
    const MILLIS_PER_DAY: u64 = 24 * 60 * 60 * 1000;

    let total_ms = BASE_HOUR_MS + offset_ms;
    let day_offset = (total_ms / MILLIS_PER_DAY) as i64;
    let ms_in_day = total_ms % MILLIS_PER_DAY;
    let hour = ms_in_day / (60 * 60 * 1000);
    let minute = (ms_in_day / (60 * 1000)) % 60;
    let second = (ms_in_day / 1000) % 60;
    let millisecond = ms_in_day % 1000;

    let base_days = days_from_civil(2024, 1, 15);
    let (year, month, day) = civil_from_days(base_days + day_offset);

    TimestampParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        millisecond,
    }
}

// Howard Hinnant civil-date conversion helpers:
// https://howardhinnant.github.io/date_algorithms.html
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = year - i32::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::from(era) * 146_097 + i64::from(doe) - 719_468
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i32 + (era as i32) * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = mp + if mp < 10 { 3 } else { -9 }; // [1, 12]
    let year = y + i32::from(month <= 2);
    (year, month as u32, day as u32)
}

pub(crate) fn generate_json_log_file(num_lines: usize, output: &str) -> io::Result<()> {
    use std::io::BufWriter;

    eprintln!(
        "Generating {}{num_lines}{} JSON log lines to {}{output}{}...",
        bold(),
        reset(),
        bold(),
        reset(),
    );

    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v2/products",
        "/health",
        "/api/v1/auth",
    ];

    for i in 0..num_lines {
        let seq = i as u64;
        let level = levels[i % 4];
        let path = paths[i % 5];
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", seq.wrapping_mul(0x517cc1b727220a95));
        let status = [200, 201, 400, 404, 500, 503][i % 6];
        let ts = timestamp_parts_for_generated_log(seq);

        write!(
            writer,
            r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millisecond:03}Z","level":"{level}","message":"request handled GET {path}/{id}","duration_ms":{dur},"request_id":"{rid}","service":"myapp","status":{status}}}"#,
            year = ts.year,
            month = ts.month,
            day = ts.day,
            hour = ts.hour,
            minute = ts.minute,
            second = ts.second,
            millisecond = ts.millisecond,
            level = level,
            path = path,
            id = id,
            dur = dur,
            rid = rid,
            status = status,
        )?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    let size = std::fs::metadata(output)?.len();
    if num_lines == 0 {
        eprintln!("{}done{}: 0 lines, 0 bytes", green(), reset());
    } else {
        eprintln!(
            "{}done{}: {:.1} MB, avg {:.0} bytes/line",
            green(),
            reset(),
            size as f64 / (1024.0 * 1024.0),
            size as f64 / num_lines as f64,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_timestamp_carries_hour_and_day() {
        // 10:59:59.999 -> 11:00:00.000
        let before_hour = timestamp_parts_for_generated_log(3_599_999);
        let after_hour = timestamp_parts_for_generated_log(3_600_000);
        assert_eq!(
            (
                before_hour.year,
                before_hour.month,
                before_hour.day,
                before_hour.hour,
                before_hour.minute,
                before_hour.second,
                before_hour.millisecond
            ),
            (2024, 1, 15, 10, 59, 59, 999)
        );
        assert_eq!(
            (
                after_hour.year,
                after_hour.month,
                after_hour.day,
                after_hour.hour,
                after_hour.minute,
                after_hour.second,
                after_hour.millisecond
            ),
            (2024, 1, 15, 11, 0, 0, 0)
        );

        // 2024-01-15T23:59:59.999 -> 2024-01-16T00:00:00.000
        // Base is 10:00:00, so day rollover happens at +14h.
        let before_day = timestamp_parts_for_generated_log(50_399_999);
        let after_day = timestamp_parts_for_generated_log(50_400_000);
        assert_eq!(
            (
                before_day.year,
                before_day.month,
                before_day.day,
                before_day.hour,
                before_day.minute,
                before_day.second,
                before_day.millisecond
            ),
            (2024, 1, 15, 23, 59, 59, 999)
        );
        assert_eq!(
            (
                after_day.year,
                after_day.month,
                after_day.day,
                after_day.hour,
                after_day.minute,
                after_day.second,
                after_day.millisecond
            ),
            (2024, 1, 16, 0, 0, 0, 0)
        );
    }

    /// generate_json_log_file must produce monotonically increasing timestamps
    /// for files larger than 1000 lines.  Before the fix, line 1000 had the
    /// same millisecond component as line 0 (both .000Z), making the timestamp
    /// go backward between lines 999 and 1000.
    #[test]
    fn generate_json_timestamps_are_monotonic_across_1000_line_boundary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("logs.json");
        generate_json_log_file(1002, path.to_str().unwrap()).expect("generate");

        let content = std::fs::read_to_string(&path).expect("read");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1002);

        let ts_at = |idx: usize| -> String {
            let obj: serde_json::Value = serde_json::from_str(lines[idx]).expect("parse json");
            obj["timestamp"]
                .as_str()
                .expect("timestamp field")
                .to_owned()
        };

        let ts_999 = ts_at(999);
        let ts_1000 = ts_at(1000);

        assert!(
            ts_1000 > ts_999,
            "timestamp at line 1000 ({ts_1000:?}) must be greater than at line 999 ({ts_999:?}); \
             timestamps must not wrap at the 1000-line boundary"
        );
    }
}
