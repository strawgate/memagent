//! Synthetic log and record event writers for the `Logs` and `Record` generator profiles.

use std::io::Write as _;

use super::{GeneratorComplexity, GeneratorTimestamp, RecordFields};

const LEVELS: [&str; 4] = ["INFO", "DEBUG", "WARN", "ERROR"];
const PATHS: [&str; 5] = [
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v2/products",
    "/health",
    "/api/v1/auth",
];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const SERVICES: [&str; 3] = ["myapp", "gateway", "auth-svc"];

/// Write a single synthetic log event into `buf`.
///
/// Sets `*done = true` and returns without writing if the counter would overflow.
pub(super) fn write_logs_event(
    buf: &mut Vec<u8>,
    counter: u64,
    complexity: GeneratorComplexity,
    timestamp: &GeneratorTimestamp,
    message_template: Option<&str>,
    field_count: Option<usize>,
    done: &mut bool,
) {
    let seq = counter;
    let level = LEVELS[(seq % LEVELS.len() as u64) as usize];
    let path = PATHS[(seq % PATHS.len() as u64) as usize];
    let method = METHODS[(seq % METHODS.len() as u64) as usize];
    let service = SERVICES[(seq % SERVICES.len() as u64) as usize];
    let id = 10000 + seq.wrapping_mul(7) % 90000;
    let dur = 1 + seq.wrapping_mul(13) % 500;
    let rid = seq.wrapping_mul(0x517c_c1b7_2722_0a95);
    let status = match seq % 20 {
        0 => 500,
        1 | 2 => 404,
        3 => 429,
        _ => 200,
    };
    let Ok(counter_i64) = i64::try_from(counter) else {
        *done = true;
        return;
    };
    let Some(event_ms) = counter_i64
        .checked_mul(timestamp.step_ms)
        .and_then(|offset| timestamp.start_epoch_ms.checked_add(offset))
    else {
        *done = true;
        return;
    };
    let (year, month, day, hour, min, sec, msec) = super::epoch_ms_to_parts(event_ms);

    let mut msg_buf = String::new();
    if let Some(template) = message_template {
        // Safe to unwrap because json_escape_bytes returns valid UTF-8 if given valid UTF-8.
        let mut tmp_buf = Vec::with_capacity(template.len());
        logfwd_core::cri::json_escape_bytes(template.as_bytes(), &mut tmp_buf);
        msg_buf.push_str(std::str::from_utf8(&tmp_buf).unwrap());
    } else {
        use std::fmt::Write;
        let _ = write!(&mut msg_buf, "{method} {path}/{id} {status}");
    };
    let msg_str = &msg_buf;

    let mut synthetic_fields = String::new();
    if let Some(count) = field_count {
        use std::fmt::Write;
        for i in 0..count {
            let _ = write!(&mut synthetic_fields, r#","field_{}":"val_{}""#, i, i);
        }
    }

    match complexity {
        GeneratorComplexity::Simple => {
            let _ = write!(
                buf,
                r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{msg_str}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status}{synthetic_fields}}}"#,
            );
        }
        GeneratorComplexity::Complex => {
            let bytes_in = 128 + seq.wrapping_mul(17) % 8192;
            let bytes_out = 64 + seq.wrapping_mul(31) % 4096;
            if seq.is_multiple_of(5) {
                let _ = write!(
                    buf,
                    r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{msg_str}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status},"bytes_in":{bytes_in},"bytes_out":{bytes_out},"headers":{{"content-type":"application/json","x-request-id":"{rid:016x}"}},"tags":["web","{service}","{level}"]{synthetic_fields}}}"#,
                );
            } else if seq.is_multiple_of(7) {
                let upstream_ms = 1 + seq.wrapping_mul(19) % 200;
                let _ = write!(
                    buf,
                    r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{msg_str}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status},"bytes_in":{bytes_in},"bytes_out":{bytes_out},"upstream":[{{"host":"10.0.0.1","latency_ms":{upstream_ms}}},{{"host":"10.0.0.2","latency_ms":{dur}}}]{synthetic_fields}}}"#,
                );
            } else {
                let _ = write!(
                    buf,
                    r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{msg_str}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status},"bytes_in":{bytes_in},"bytes_out":{bytes_out}{synthetic_fields}}}"#,
                );
            }
        }
    }
}

/// Write a single record event into `buf`.
///
/// Sets `*done = true` and returns without writing if the sequence counter would overflow.
pub(super) fn write_record_event(
    buf: &mut Vec<u8>,
    record_fields: &RecordFields,
    counter: u64,
    done: &mut bool,
    event_created_unix_nano: Option<u128>,
) {
    buf.push(b'{');
    let mut first = true;
    for encoded_field in &record_fields.attributes {
        if !first {
            buf.push(b',');
        }
        first = false;
        buf.extend_from_slice(encoded_field);
    }
    if let Some(sequence) = &record_fields.sequence {
        let Some(value) = sequence.start.checked_add(counter) else {
            *done = true;
            return;
        };
        super::write_json_u64_field(buf, &sequence.field, value, &mut first);
    }
    if let (Some(field), Some(event_created_unix_nano)) = (
        &record_fields.event_created_unix_nano_field,
        event_created_unix_nano,
    ) {
        super::write_json_u128_field(buf, field, event_created_unix_nano, &mut first);
    }
    buf.push(b'}');
}
