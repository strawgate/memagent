// Arrow batch generation for benchmarking.
// xtask-verify: allow(pub_module_needs_tests) // generate_arrow_batch tested via generator/tests/basic.rs

use arrow::array::{Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Build a `RecordBatch` directly from computed field values, bypassing JSON
/// serialization and scanning entirely.
///
/// Produces the exact same column names, types, and values as
/// `GeneratorInput` → `FramedInput` → `Scanner` would for the `logs` profile
/// with `Simple` complexity. This is verified by the `arrow_matches_json_path`
/// equivalence test.
///
/// # Arguments
/// * `n` — number of rows to generate
/// * `start_counter` — first event counter value (rows use `start_counter..start_counter+n`)
/// * `timestamp_config` — timestamp base and step
/// * `message_template` — raw (un-escaped) message template as a `&str`, or
///   `None` for default `"{method} {path}/{id} {status}"`. When the JSON
///   generator uses a pre-escaped template for embedding in JSON strings, this
///   function expects the **decoded** form — the value the scanner would extract
///   after un-escaping the JSON string.
pub fn generate_arrow_batch(
    n: usize,
    start_counter: u64,
    timestamp_config: &GeneratorTimestamp,
    message_template: Option<&str>,
) -> RecordBatch {
    let mut ts_builder = StringBuilder::with_capacity(n, n * 24);
    let mut level_builder = StringBuilder::with_capacity(n, n * 5);
    let mut message_builder = StringBuilder::with_capacity(n, n * 40);
    let mut duration_builder = Int64Builder::with_capacity(n);
    let mut rid_builder = StringBuilder::with_capacity(n, n * 16);
    let mut service_builder = StringBuilder::with_capacity(n, n * 8);
    let mut status_builder = Int64Builder::with_capacity(n);

    let mut ts_buf = [0u8; 24];
    let mut hex_buf = [0u8; 16];
    let mut msg_buf = Vec::with_capacity(64);

    for i in 0..n {
        let counter = start_counter.wrapping_add(i as u64);
        let Some(fields) =
            compute_log_fields(counter, timestamp_config, GeneratorComplexity::Simple, None)
        else {
            break;
        };

        // Timestamp as ISO 8601 string
        fields.timestamp.write_iso8601(&mut ts_buf);
        append_utf8_or_null(&mut ts_builder, &ts_buf);

        level_builder.append_value(fields.level);

        // Message — reuse buffer
        msg_buf.clear();
        if let Some(tmpl) = message_template {
            msg_buf.extend_from_slice(tmpl.as_bytes());
        } else {
            write_message_value(&mut msg_buf, &fields);
        }
        append_utf8_or_null(&mut message_builder, &msg_buf);

        // Integer fields as Int64
        duration_builder.append_value(fields.duration_ms as i64);

        // request_id: stack-based hex formatting
        write_hex16_into(&mut hex_buf, fields.request_id);
        append_utf8_or_null(&mut rid_builder, &hex_buf);

        service_builder.append_value(fields.service);
        status_builder.append_value(fields.status as i64);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("duration_ms", DataType::Int64, true),
        Field::new("request_id", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts_builder.finish()),
            Arc::new(level_builder.finish()),
            Arc::new(message_builder.finish()),
            Arc::new(duration_builder.finish()),
            Arc::new(rid_builder.finish()),
            Arc::new(service_builder.finish()),
            Arc::new(status_builder.finish()),
        ],
    )
    .expect("schema matches builders")
}

#[inline]
fn append_utf8_or_null(builder: &mut StringBuilder, bytes: &[u8]) {
    match std::str::from_utf8(bytes) {
        Ok(value) => builder.append_value(value),
        Err(_) => builder.append_null(),
    }
}

/// Write a u64 as 16 zero-padded lowercase hex digits into a fixed buffer.
fn write_hex16_into(out: &mut [u8; 16], val: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut v = val;
    let mut i = 15_i32;
    while i >= 0 {
        out[i as usize] = HEX[(v & 0xf) as usize];
        v >>= 4;
        i -= 1;
    }
}