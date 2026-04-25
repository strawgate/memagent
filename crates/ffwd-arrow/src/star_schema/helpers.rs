#![allow(clippy::indexing_slicing)]

//! Helper functions for Arrow array value extraction, hex encoding,
//! timestamp parsing, severity mapping, and attribute type tagging.
// xtask-verify: allow(pub_module_needs_tests) // Utility module - functions tested via star_schema/tests.rs

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use arrow::util::display::array_value_to_string;

use super::{ATTR_TYPE_BOOL, ATTR_TYPE_BYTES, ATTR_TYPE_DOUBLE, ATTR_TYPE_INT, ATTR_TYPE_STR};

/// Extract a string value from any supported Arrow array at the given row.
pub(super) fn str_value_at(arr: &dyn Array, row: usize) -> String {
    if arr.is_null(row) {
        return String::new();
    }
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8View => arr
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Binary => arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| {
                let mut hex = String::with_capacity(a.value(row).len() * 2);
                for b in a.value(row) {
                    use std::fmt::Write as _;
                    let _ = write!(&mut hex, "{b:02x}");
                }
                hex
            })
            .unwrap_or_default(),
        DataType::LargeBinary => arr
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|a| {
                let mut hex = String::with_capacity(a.value(row).len() * 2);
                for b in a.value(row) {
                    use std::fmt::Write as _;
                    let _ = write!(&mut hex, "{b:02x}");
                }
                hex
            })
            .unwrap_or_default(),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int8 => arr
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int16 => arr
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt8 => arr
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt16 => arr
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => arr
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => arr
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float32 => arr
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        // Timestamps: normalize the raw i64 to nanoseconds before stringifying so that
        // `parse_timestamp_to_nanos` (which infers units from magnitude) always sees a
        // nanos-scale value. The old code stringified the raw i64 directly, which caused
        // small-epoch timestamps in coarser units (seconds, millis, micros) to be
        // misclassified by the magnitude heuristic.
        DataType::Timestamp(TimeUnit::Second, _) => arr
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|a| a.value(row).saturating_mul(1_000_000_000).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|a| a.value(row).saturating_mul(1_000_000).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| a.value(row).saturating_mul(1_000).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        // Fallback for any other Arrow types (e.g. Date32, Dictionary, List, etc.)
        // delegates to Arrow's display formatting via `array_value_to_string`.
        //
        // Note: `array_value_to_string` allocates an `ArrayFormatter` per call. This is
        // acceptable here because all common hot-path types (Utf8, Int64, Float64,
        // Boolean, Binary, all integer/float variants, and Timestamps) are handled by
        // explicit arms above. Only truly exotic types reach this fallback.
        _ => array_value_to_string(arr, row).unwrap_or_default(),
    }
}

pub(super) fn hex_encode_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Read a string value from a Utf8 or Utf8View array.
pub(super) fn str_from_array(arr: &dyn Array, row: usize) -> String {
    if arr.is_null(row) {
        return String::new();
    }
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8View => arr
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

/// Determine the attribute type tag for an Arrow `DataType`.
pub(super) fn attr_type_for(dt: &DataType) -> u8 {
    match dt {
        DataType::Int64 => ATTR_TYPE_INT,
        DataType::Float64 => ATTR_TYPE_DOUBLE,
        DataType::Boolean => ATTR_TYPE_BOOL,
        DataType::Binary | DataType::LargeBinary => ATTR_TYPE_BYTES,
        _ => ATTR_TYPE_STR,
    }
}

/// Build a `FixedSizeBinary` array from optional fixed-size byte arrays.
pub(super) fn build_fixed_binary_array<const N: usize>(
    values: &[Option<[u8; N]>],
) -> Result<ArrayRef, ArrowError> {
    let mut builder = arrow::array::FixedSizeBinaryBuilder::new(N as i32);
    for val in values {
        match val {
            Some(bytes) => builder.append_value(bytes)?,
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Parse a hex string to a fixed-size byte array.
pub(super) fn hex_to_fixed<const N: usize>(hex: &str) -> Option<[u8; N]> {
    let hex = hex.trim();
    if hex.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for i in 0..N {
        let byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
        out[i] = byte;
    }
    Some(out)
}

/// Parse a timestamp string to nanoseconds since epoch.
/// Supports ISO 8601 / RFC 3339 format and bare epoch nanoseconds.
pub(crate) fn parse_timestamp_to_nanos(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Try bare integer nanoseconds first.
    if let Ok(ns) = s.parse::<i64>() {
        // Heuristic for epoch timestamps based on magnitude. We use unsigned_abs() so that
        // pre-1970 (negative) timestamps are classified correctly — for negative values the
        // comparisons against positive thresholds would otherwise always be false.
        // > 1e17: nanoseconds  (1e17 ns = ~3.17 years from 1970)
        // > 1e14: microseconds (1e14 us = ~3.17 years)
        // > 1e11: milliseconds (1e11 ms = ~3.17 years)
        // else: seconds
        let abs = ns.unsigned_abs();
        if abs > 100_000_000_000_000_000 {
            return Some(ns);
        }
        if abs > 100_000_000_000_000 {
            return ns.checked_mul(1_000);
        }
        if abs > 100_000_000_000 {
            return ns.checked_mul(1_000_000);
        }
        return ns.checked_mul(1_000_000_000);
    }
    // Try RFC 3339 parsing.
    // Format: 2024-01-15T10:30:00.123456789Z
    parse_rfc3339_nanos(s)
}

/// Minimal RFC 3339 parser that preserves nanosecond precision.
pub(crate) fn parse_rfc3339_nanos(s: &str) -> Option<i64> {
    // Expected format: YYYY-MM-DDThh:mm:ss[.nnnnnnnnn]Z or +HH:MM
    if s.len() < 19 {
        return None;
    }

    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hour: u32 = s.get(11..13)?.parse().ok()?;
    let min: u32 = s.get(14..16)?.parse().ok()?;
    let sec: u32 = s.get(17..19)?.parse().ok()?;

    if hour >= 24 || min >= 60 || sec > 60 {
        // sec > 60 allows leap seconds (60)
        return None;
    }

    let rest = &s[19..];
    let (frac_nanos, tz_start) = if let Some(dot_rest) = rest.strip_prefix('.') {
        // Parse fractional seconds.
        let frac_end = dot_rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(dot_rest.len());
        let frac_str = &dot_rest[..frac_end];
        // Pad or truncate to 9 digits.
        let mut padded = String::with_capacity(9);
        padded.push_str(frac_str);
        while padded.len() < 9 {
            padded.push('0');
        }
        let nanos: i64 = padded[..9].parse().ok()?;
        (nanos, 1 + frac_end)
    } else {
        (0i64, 0)
    };

    let tz_str = &rest[tz_start..];
    // RFC 3339 requires an explicit timezone designator. Strings without one (e.g.
    // "2024-01-15T10:30:00") are not valid RFC 3339 and we return None rather than
    // silently assuming UTC, which could misinterpret local-time values.
    let tz_offset_secs: i64 = if tz_str == "Z" || tz_str == "z" {
        0
    } else if tz_str.is_empty() {
        return None;
    } else if tz_str.len() >= 6 && (tz_str.starts_with('+') || tz_str.starts_with('-')) {
        let sign: i64 = if tz_str.starts_with('-') { -1 } else { 1 };
        let tz_h: i64 = tz_str[1..3].parse().ok()?;
        let tz_m: i64 = tz_str[4..6].parse().ok()?;
        if tz_h >= 24 || tz_m >= 60 {
            return None;
        }
        sign * (tz_h * 3600 + tz_m * 60)
    } else {
        return None;
    };

    // Convert date to days since epoch using a simplified calculation.
    let days = days_from_civil(year, month, day)?;
    let total_secs = days
        .checked_mul(86400)?
        .checked_add(i64::from(hour) * 3600)?
        .checked_add(i64::from(min) * 60)?
        .checked_add(i64::from(sec))?
        .checked_sub(tz_offset_secs)?;

    total_secs
        .checked_mul(1_000_000_000)?
        .checked_add(frac_nanos)
}

/// Convert a civil date to days since Unix epoch (1970-01-01).
///
/// Validates month/day ranges (including leap year rules) and delegates
/// to the Kani-verified `ffwd_core::otlp::days_from_civil`.
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // More precise day validation
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let max_days = match month {
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap {
                29
            } else {
                28
            }
        }
        _ => 31,
    };
    if day > max_days {
        return None;
    }

    Some(ffwd_core::otlp::days_from_civil(year, month, day))
}

/// Format nanosecond epoch timestamp as RFC 3339 string.
pub fn chrono_timestamp(secs: i64, nanos: u32) -> String {
    // Reverse the days_from_civil calculation.
    let total_days = secs.div_euclid(86400);
    let day_secs = secs.rem_euclid(86400);
    let hour = day_secs / 3600;
    let min = (day_secs % 3600) / 60;
    let sec = day_secs % 60;

    let (year, month, day) = civil_from_days(total_days);

    if nanos == 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
    } else {
        // Trim trailing zeros from nanoseconds.
        let ns_str = format!("{nanos:09}");
        let trimmed = ns_str.trim_end_matches('0');
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{trimmed}Z")
    }
}

/// Reverse of `days_from_civil`: days since epoch → (year, month, day).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Map severity text to OTLP severity number.
pub(super) fn severity_text_to_number(text: &str) -> i32 {
    match text.to_uppercase().as_str() {
        "TRACE" => 1,
        "TRACE2" => 2,
        "TRACE3" => 3,
        "TRACE4" => 4,
        "DEBUG" => 5,
        "DEBUG2" => 6,
        "DEBUG3" => 7,
        "DEBUG4" => 8,
        "INFO" => 9,
        "INFO2" => 10,
        "INFO3" => 11,
        "INFO4" => 12,
        "WARN" | "WARNING" => 13,
        "WARN2" => 14,
        "WARN3" => 15,
        "WARN4" => 16,
        "ERROR" => 17,
        "ERROR2" => 18,
        "ERROR3" => 19,
        "ERROR4" => 20,
        "FATAL" => 21,
        "FATAL2" => 22,
        "FATAL3" => 23,
        "FATAL4" => 24,
        _ => 0, // UNSPECIFIED
    }
}

/// Convert a single attrs row's typed value to a String representation.
pub(super) fn attrs_row_value_to_string(
    type_tag: u8,
    str_arr: &StringArray,
    int_arr: &Int64Array,
    double_arr: &Float64Array,
    bool_arr: &BooleanArray,
    bytes_arr: &BinaryArray,
    row: usize,
) -> Option<String> {
    match type_tag {
        ATTR_TYPE_STR => (!str_arr.is_null(row)).then(|| str_arr.value(row).to_string()),
        ATTR_TYPE_INT => (!int_arr.is_null(row)).then(|| int_arr.value(row).to_string()),
        ATTR_TYPE_DOUBLE => (!double_arr.is_null(row)).then(|| double_arr.value(row).to_string()),
        ATTR_TYPE_BOOL => (!bool_arr.is_null(row)).then(|| bool_arr.value(row).to_string()),
        ATTR_TYPE_BYTES => {
            (!bytes_arr.is_null(row)).then(|| hex_encode_lower(bytes_arr.value(row)))
        }
        _ => None,
    }
}
