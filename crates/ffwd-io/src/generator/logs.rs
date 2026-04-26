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


