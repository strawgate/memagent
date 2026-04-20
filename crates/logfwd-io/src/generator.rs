//! Synthetic data generator input source.
//!
//! Produces JSON log lines at a configurable rate. Used for benchmarking
//! and testing pipelines without external data sources.

use std::collections::HashMap;
use std::io;
use std::io::Write;

use crate::input::{InputEvent, InputSource};
use logfwd_types::diagnostics::ComponentHealth;

/// Controls the complexity/size of generated lines.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum GeneratorComplexity {
    /// Flat JSON object, ~200 bytes per line.
    #[default]
    Simple,
    /// Includes occasional nested objects and arrays, ~400-800 bytes.
    Complex,
}

/// Named generator output profiles.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum GeneratorProfile {
    /// Synthetic request-like JSON logs.
    #[default]
    Logs,
    /// Flat JSON records built from static attributes and generated fields.
    Record,
}

/// Monotonic generated field configuration.
pub struct GeneratorGeneratedField {
    /// Output field name for the generated sequence in record rows.
    pub field: String,
    /// Initial monotonic sequence value.
    pub start: u64,
}

/// Static scalar attribute value written into generated `record` rows.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum GeneratorAttributeValue {
    /// UTF-8 text scalar.
    String(String),
    /// Signed 64-bit integer scalar.
    Integer(i64),
    /// 64-bit floating point scalar.
    Float(f64),
    /// Boolean scalar.
    Bool(bool),
    /// JSON null scalar.
    Null,
}

/// Resolved timestamp configuration for the `logs` profile.
///
/// Controls the base timestamp and per-event step for generated log lines.
/// Resolved at pipeline build time — `"now"` is converted to an epoch ms value.
#[derive(Debug, Clone, Copy)]
pub struct GeneratorTimestamp {
    /// Base timestamp in milliseconds since Unix epoch.
    pub start_epoch_ms: i64,
    /// Milliseconds between events. Negative = events go backward in time.
    pub step_ms: i64,
}

/// Default: 2024-01-15T00:00:00Z, +1ms per event.
impl Default for GeneratorTimestamp {
    fn default() -> Self {
        Self {
            start_epoch_ms: 1_705_276_800_000, // 2024-01-15T00:00:00Z
            step_ms: 1,
        }
    }
}

/// Configuration for the generator input.
pub struct GeneratorConfig {
    /// Target events per second. 0 = unlimited (as fast as possible).
    pub events_per_sec: u64,
    /// Number of events per batch (per poll() call).
    pub batch_size: usize,
    /// Total events to generate. 0 = infinite.
    pub total_events: u64,
    /// Controls the size and shape of generated JSON lines.
    pub complexity: GeneratorComplexity,
    /// Which event shape to emit.
    pub profile: GeneratorProfile,
    /// Static scalar attributes written into generated rows.
    pub attributes: HashMap<String, GeneratorAttributeValue>,
    /// Monotonic sequence field for `record` rows.
    pub sequence: Option<GeneratorGeneratedField>,
    /// Source-created timestamp field for `record` rows.
    pub event_created_unix_nano_field: Option<String>,
    /// Timestamp configuration for the `logs` profile.
    pub timestamp: GeneratorTimestamp,
    /// Optional template string used as the `message` field value in generated
    /// log events. When `None`, the default synthetic message is used.
    /// The string is JSON-escaped before embedding so special characters
    /// (quotes, backslashes, newlines, etc.) produce valid JSON.
    pub message_template: Option<String>,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            events_per_sec: 0,
            batch_size: 1000,
            total_events: 0,
            complexity: GeneratorComplexity::default(),
            profile: GeneratorProfile::default(),
            attributes: HashMap::new(),
            sequence: None,
            event_created_unix_nano_field: None,
            timestamp: GeneratorTimestamp::default(),
            message_template: None,
        }
    }
}

/// Input source that generates synthetic JSON log lines.
pub struct GeneratorInput {
    name: String,
    config: GeneratorConfig,
    counter: u64,
    buf: Vec<u8>,
    done: bool,
    last_refill: std::time::Instant,
    rate_credit_events: f64,
    record_fields: RecordFields,
    /// Pre-escaped `message_template` bytes (the inner JSON string content,
    /// without surrounding quotes). `None` means use the default message.
    message_template_escaped: Option<Vec<u8>>,
}

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

// ---------------------------------------------------------------------------
// Shared event field computation
// ---------------------------------------------------------------------------

/// Computed field values for a single `logs` profile event.
///
/// Pure function of counter + config — shared between JSON and Arrow generators.
pub(crate) struct LogEventFields<'a> {
    pub timestamp: TimestampParts,
    pub level: &'static str,
    pub message_template: Option<&'a [u8]>,
    pub method: &'static str,
    pub path: &'static str,
    pub id: u64,
    pub duration_ms: u64,
    pub request_id: u64,
    pub service: &'static str,
    pub status: u32,
    pub complexity: ComplexityFields,
}

/// Pre-decomposed timestamp for both JSON formatting and Arrow string building.
pub(crate) struct TimestampParts {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub min: u32,
    pub sec: u32,
    pub ms: u32,
}

impl TimestampParts {
    /// Format as `YYYY-MM-DDTHH:MM:SS.mmmZ` into a stack buffer.
    pub fn write_iso8601(&self, out: &mut [u8; 24]) {
        // "2024-01-15T00:00:00.000Z"
        let y = self.year as u32;
        out[0] = b'0' + (y / 1000 % 10) as u8;
        out[1] = b'0' + (y / 100 % 10) as u8;
        out[2] = b'0' + (y / 10 % 10) as u8;
        out[3] = b'0' + (y % 10) as u8;
        out[4] = b'-';
        out[5] = b'0' + (self.month / 10) as u8;
        out[6] = b'0' + (self.month % 10) as u8;
        out[7] = b'-';
        out[8] = b'0' + (self.day / 10) as u8;
        out[9] = b'0' + (self.day % 10) as u8;
        out[10] = b'T';
        out[11] = b'0' + (self.hour / 10) as u8;
        out[12] = b'0' + (self.hour % 10) as u8;
        out[13] = b':';
        out[14] = b'0' + (self.min / 10) as u8;
        out[15] = b'0' + (self.min % 10) as u8;
        out[16] = b':';
        out[17] = b'0' + (self.sec / 10) as u8;
        out[18] = b'0' + (self.sec % 10) as u8;
        out[19] = b'.';
        out[20] = b'0' + (self.ms / 100) as u8;
        out[21] = b'0' + (self.ms / 10 % 10) as u8;
        out[22] = b'0' + (self.ms % 10) as u8;
        out[23] = b'Z';
    }

    /// Append `YYYY-MM-DDTHH:MM:SS.mmmZ` to `buf` (avoids a separate stack buffer).
    pub fn write_iso8601_into(&self, buf: &mut Vec<u8>) {
        let mut tmp = [0u8; 24];
        self.write_iso8601(&mut tmp);
        buf.extend_from_slice(&tmp);
    }
}

/// Extra fields present only in `Complex` events.
pub(crate) enum ComplexityFields {
    /// `Simple` profile — no extra fields.
    Simple,
    /// `Complex` profile with varying extra fields per event.
    Complex {
        bytes_in: u64,
        bytes_out: u64,
        variant: ComplexVariant,
    },
}

/// Which structural variant a complex event takes.
pub(crate) enum ComplexVariant {
    /// Includes `headers` map and `tags` array.
    WithHeadersAndTags,
    /// Includes `upstream` array with nested objects.
    WithUpstream { upstream_ms: u64 },
    /// Only `bytes_in`/`bytes_out` extras.
    Basic,
}

/// Compute the field values for one `logs` event from the counter and config.
///
/// Returns `None` if the counter overflows timestamp arithmetic (signals done).
pub(crate) fn compute_log_fields<'a>(
    counter: u64,
    timestamp_config: &GeneratorTimestamp,
    complexity: GeneratorComplexity,
    message_template_escaped: Option<&'a [u8]>,
) -> Option<LogEventFields<'a>> {
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

    let counter_i64 = i64::try_from(counter).ok()?;
    let event_ms = counter_i64
        .checked_mul(timestamp_config.step_ms)
        .and_then(|offset| timestamp_config.start_epoch_ms.checked_add(offset))?;
    let (year, month, day, hour, min, sec, ms) = epoch_ms_to_parts(event_ms);

    let complexity_fields = match complexity {
        GeneratorComplexity::Simple => ComplexityFields::Simple,
        GeneratorComplexity::Complex => {
            let bytes_in = 128 + seq.wrapping_mul(17) % 8192;
            let bytes_out = 64 + seq.wrapping_mul(31) % 4096;
            let variant = if seq.is_multiple_of(5) {
                ComplexVariant::WithHeadersAndTags
            } else if seq.is_multiple_of(7) {
                ComplexVariant::WithUpstream {
                    upstream_ms: 1 + seq.wrapping_mul(19) % 200,
                }
            } else {
                ComplexVariant::Basic
            };
            ComplexityFields::Complex {
                bytes_in,
                bytes_out,
                variant,
            }
        }
    };

    Some(LogEventFields {
        timestamp: TimestampParts {
            year,
            month,
            day,
            hour,
            min,
            sec,
            ms,
        },
        level,
        message_template: message_template_escaped,
        method,
        path,
        id,
        duration_ms: dur,
        request_id: rid,
        service,
        status,
        complexity: complexity_fields,
    })
}

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

#[derive(Debug, Clone)]
struct RecordFields {
    attributes: Vec<Vec<u8>>,
    sequence: Option<GeneratorGeneratedFieldState>,
    event_created_unix_nano_field: Option<String>,
}

#[derive(Debug, Clone)]
struct GeneratorGeneratedFieldState {
    field: String,
    start: u64,
}

impl GeneratorInput {
    pub fn new(name: impl Into<String>, config: GeneratorConfig) -> Self {
        let name = name.into();
        // The generator treats a zero batch size as the smallest useful batch.
        // User-facing config validation should reject zero before construction.
        let batch_size = config.batch_size.max(1);
        let initial_rate_credit_events = if config.events_per_sec > 0 {
            batch_size as f64
        } else {
            0.0
        };
        let mut config = config;
        config.batch_size = batch_size;
        let mut attributes: Vec<(&String, &GeneratorAttributeValue)> =
            config.attributes.iter().collect();
        attributes.sort_by(|a, b| a.0.cmp(b.0));
        let record_fields = RecordFields {
            attributes: attributes
                .into_iter()
                .map(|(key, value)| encode_static_field(key, value))
                .collect(),
            sequence: config
                .sequence
                .as_ref()
                .map(|seq| GeneratorGeneratedFieldState {
                    field: seq.field.clone(),
                    start: seq.start,
                }),
            event_created_unix_nano_field: config.event_created_unix_nano_field.clone(),
        };
        // Pre-escape the message_template so we don't re-escape on every event.
        let message_template_escaped = config.message_template.as_deref().map(|tmpl| {
            let mut escaped = Vec::with_capacity(tmpl.len() + 4);
            write_json_escaped_string_contents(&mut escaped, tmpl);
            escaped
        });
        Self {
            name,
            buf: Vec::with_capacity(batch_size * 512),
            config,
            counter: 0,
            done: false,
            record_fields,
            last_refill: std::time::Instant::now(),
            // Seed one batch worth of credits so the first poll() can emit
            // immediately in rate-limited mode.
            rate_credit_events: initial_rate_credit_events,
            message_template_escaped,
        }
    }

    /// Return the total number of events generated so far.
    pub fn events_generated(&self) -> u64 {
        self.counter
    }

    fn generate_batch(&mut self, n: usize) {
        self.buf.clear();
        let batch_created_unix_nano = self
            .record_fields
            .event_created_unix_nano_field
            .as_ref()
            .map(|_| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            });
        for batch_offset in 0_u128..n as u128 {
            if self.config.total_events > 0 && self.counter >= self.config.total_events {
                self.done = true;
                break;
            }
            let event_created_unix_nano = batch_created_unix_nano.map(|base| base + batch_offset);
            let len_before = self.buf.len();
            self.write_event(event_created_unix_nano);
            if self.done {
                self.buf.truncate(len_before);
                break;
            }
            self.buf.push(b'\n');
            self.counter += 1;
        }
    }

    fn write_event(&mut self, event_created_unix_nano: Option<u128>) {
        match self.config.profile {
            GeneratorProfile::Logs => self.write_logs_event(),
            GeneratorProfile::Record => self.write_record_event(event_created_unix_nano),
        }
    }

    fn write_logs_event(&mut self) {
        let Some(fields) = compute_log_fields(
            self.counter,
            &self.config.timestamp,
            self.config.complexity,
            self.message_template_escaped.as_deref(),
        ) else {
            self.done = true;
            return;
        };
        write_log_fields_json(&mut self.buf, &fields);
    }

    fn write_record_event(&mut self, event_created_unix_nano: Option<u128>) {
        self.buf.push(b'{');
        let mut first = true;
        for encoded_field in &self.record_fields.attributes {
            if !first {
                self.buf.push(b',');
            }
            first = false;
            self.buf.extend_from_slice(encoded_field);
        }
        if let Some(sequence) = &self.record_fields.sequence {
            let Some(value) = sequence.start.checked_add(self.counter) else {
                self.done = true;
                return;
            };
            write_json_u64_field(&mut self.buf, &sequence.field, value, &mut first);
        }
        if let (Some(field), Some(event_created_unix_nano)) = (
            &self.record_fields.event_created_unix_nano_field,
            event_created_unix_nano,
        ) {
            write_json_u128_field(&mut self.buf, field, event_created_unix_nano, &mut first);
        }
        self.buf.push(b'}');
    }
}

impl InputSource for GeneratorInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        if self.done {
            return Ok(vec![]);
        }

        if self.config.total_events > 0 && self.counter >= self.config.total_events {
            self.done = true;
            return Ok(vec![]);
        }

        let mut events_to_emit = self.config.batch_size as u64;
        if self.config.events_per_sec > 0 {
            let batch_size = self.config.batch_size as u64;
            let now = std::time::Instant::now();
            let elapsed_sec = now
                .checked_duration_since(self.last_refill)
                .unwrap_or_default()
                .as_secs_f64();
            self.last_refill = now;
            self.rate_credit_events += elapsed_sec * self.config.events_per_sec as f64;

            // Bound carried credits to one poll burst window so scheduler stalls
            // do not turn into an arbitrarily large catch-up burst.
            let burst_cap = self.config.events_per_sec.max(batch_size);
            self.rate_credit_events = self.rate_credit_events.min(burst_cap as f64);
            let available = self.rate_credit_events.floor() as u64;
            let remaining_total = if self.config.total_events > 0 {
                self.config.total_events.saturating_sub(self.counter)
            } else {
                u64::MAX
            };
            let full_batches_available = available / batch_size;
            if full_batches_available == 0 {
                // Preserve full-batch cadence, but allow a final partial batch
                // when the finite total_events tail is smaller than batch_size.
                if self.config.total_events > 0
                    && remaining_total < batch_size
                    && available >= remaining_total
                {
                    events_to_emit = remaining_total;
                } else {
                    return Ok(vec![]);
                }
            } else {
                // Preserve the legacy "full batches only" behavior in rate-limited
                // mode while still allowing multiple batches per poll at high EPS.
                let max_full_batches = burst_cap / batch_size;
                events_to_emit = full_batches_available.min(max_full_batches) * batch_size;
            }
        }

        if self.config.total_events > 0 {
            let remaining = self.config.total_events.saturating_sub(self.counter);
            events_to_emit = events_to_emit.min(remaining);
        }

        if events_to_emit == 0 {
            return Ok(vec![]);
        }

        if self.config.events_per_sec > 0 {
            self.rate_credit_events -= events_to_emit as f64;
        }

        let expected_batches = events_to_emit.div_ceil(self.config.batch_size as u64) as usize;
        let mut out_events = Vec::with_capacity(expected_batches);
        let out_capacity = self.buf.capacity().max(self.config.batch_size * 512);
        let mut remaining = events_to_emit;
        while remaining > 0 {
            let chunk = remaining.min(self.config.batch_size as u64) as usize;
            self.generate_batch(chunk);
            if self.buf.is_empty() {
                break;
            }
            // Transfer batch ownership without copying, while keeping a
            // full-capacity hot buffer for the next generator write.
            let mut out = Vec::with_capacity(out_capacity);
            std::mem::swap(&mut self.buf, &mut out);
            let accounted_bytes = out.len() as u64;
            out_events.push(InputEvent::Data {
                bytes: out,
                source_id: None,
                accounted_bytes,
                cri_metadata: None,
            });
            remaining = remaining.saturating_sub(chunk as u64);
            if self.done {
                break;
            }
        }
        Ok(out_events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        // Generator input has no independent bind/startup/shutdown lifecycle.
        // It is either idle or emitting synthetic events under pipeline control.
        ComponentHealth::Healthy
    }
}

fn write_json_u64_field(out: &mut Vec<u8>, key: &str, value: u64, first: &mut bool) {
    if !*first {
        out.push(b',');
    }
    *first = false;
    write_json_key(out, key);
    let _ = write!(out, ":{value}");
}

fn write_json_u128_field(out: &mut Vec<u8>, key: &str, value: u128, first: &mut bool) {
    if !*first {
        out.push(b',');
    }
    *first = false;
    write_json_key(out, key);
    let _ = write!(out, ":{value}");
}

fn write_json_key(out: &mut Vec<u8>, key: &str) {
    write_json_quoted_string(out, key);
}

fn write_json_quoted_string(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    write_json_escaped_string_contents(out, value);
    out.push(b'"');
}

fn write_json_escaped_string_contents(out: &mut Vec<u8>, value: &str) {
    for ch in value.chars() {
        match ch {
            '"' => out.extend_from_slice(br#"\""#),
            '\\' => out.extend_from_slice(br"\\"),
            '\n' => out.extend_from_slice(br"\n"),
            '\r' => out.extend_from_slice(br"\r"),
            '\t' => out.extend_from_slice(br"\t"),
            c if c <= '\u{1F}' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
}

fn encode_static_field(key: &str, value: &GeneratorAttributeValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_json_key(&mut out, key);
    out.push(b':');
    match value {
        GeneratorAttributeValue::String(value) => {
            out.push(b'"');
            write_json_escaped_string_contents(&mut out, value);
            out.push(b'"');
        }
        GeneratorAttributeValue::Integer(value) => {
            let _ = write!(&mut out, "{value}");
        }
        GeneratorAttributeValue::Float(value) => {
            if value.is_finite() {
                let rendered = serde_json::to_string(value).expect("finite float serializes");
                out.extend_from_slice(rendered.as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        GeneratorAttributeValue::Bool(value) => {
            out.extend_from_slice(if *value { b"true" } else { b"false" });
        }
        GeneratorAttributeValue::Null => {
            out.extend_from_slice(b"null");
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Arrow-direct batch generation
// ---------------------------------------------------------------------------

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
        // Pass None as template to compute_log_fields so the message is
        // built via write_message_value's default formatting path. When
        // the caller provides a raw template, we write it directly.
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

        // Integer fields as Int64 (matching scanner's append_int_by_idx → Int64Array)
        duration_builder.append_value(fields.duration_ms as i64);

        // request_id: stack-based hex formatting (avoids format!() heap alloc)
        write_hex16(&mut hex_buf, fields.request_id);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_health_is_explicitly_healthy() {
        let input = GeneratorInput::new("test", GeneratorConfig::default());
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    /// Prove that the Arrow-direct generator produces identical data to the
    /// JSON → Scanner path across randomized inputs.
    ///
    /// Proptest varies counter offset, batch size, timestamp config, and
    /// message template. For each combination the test:
    /// 1. Generates N events via `generate_arrow_batch()`
    /// 2. Generates the same events as JSON via `write_log_fields_json()`
    /// 3. Scans the JSON through `Scanner::scan_detached()`
    /// 4. Asserts both RecordBatches are column-for-column identical
    /// 5. Asserts the JSON batch has no extra columns the Arrow batch lacks
    #[test]
    fn arrow_matches_json_path() {
        use arrow::array::AsArray;
        use logfwd_arrow::Scanner;
        use logfwd_core::scan_config::ScanConfig;
        use proptest::prelude::*;

        fn assert_batches_identical(
            arrow_batch: &RecordBatch,
            json_batch: &RecordBatch,
            label: &str,
        ) {
            assert_eq!(
                arrow_batch.num_rows(),
                json_batch.num_rows(),
                "{label}: row count mismatch"
            );
            let n = arrow_batch.num_rows();

            // Every column in the Arrow batch must exist and match in the JSON batch.
            let string_cols = ["timestamp", "level", "message", "request_id", "service"];
            let int_cols = ["duration_ms", "status"];

            for col_name in &string_cols {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow batch missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json batch missing {col_name}"));

                let arrow_strs = arrow_col.as_string::<i32>();
                let json_strs = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not StringArray"));

                for row in 0..n {
                    assert_eq!(
                        arrow_strs.value(row),
                        json_strs.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }

            for col_name in &int_cols {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow batch missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json batch missing {col_name}"));

                let arrow_ints = arrow_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in arrow"));
                let json_ints = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in json"));

                for row in 0..n {
                    assert_eq!(
                        arrow_ints.value(row),
                        json_ints.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }

            // The JSON batch must not have extra columns beyond what Arrow produces.
            let arrow_schema = arrow_batch.schema();
            let arrow_names: std::collections::HashSet<&str> = arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            for field in json_batch.schema().fields() {
                assert!(
                    arrow_names.contains(field.name().as_str()),
                    "{label}: json batch has extra column '{}' not in arrow batch",
                    field.name()
                );
            }
        }

        /// Compare Arrow-direct output against manually-constructed JSON→Scanner output.
        fn run_comparison(
            start: u64,
            n: usize,
            ts_config: &GeneratorTimestamp,
            raw_template: Option<&str>,
            label: &str,
        ) {
            // Arrow path: raw (un-escaped) template
            let arrow_batch = generate_arrow_batch(n, start, ts_config, raw_template);

            // JSON path: needs pre-escaped template for JSON embedding
            let escaped_template: Option<Vec<u8>> = raw_template.map(|raw| {
                let mut buf = Vec::with_capacity(raw.len() + 4);
                write_json_escaped_string_contents(&mut buf, raw);
                buf
            });
            let escaped_slice = escaped_template.as_deref();

            let mut json_buf = Vec::with_capacity(n * 256);
            let mut json_count = 0;
            for i in 0..n {
                let counter = start + i as u64;
                let Some(fields) = compute_log_fields(
                    counter,
                    ts_config,
                    GeneratorComplexity::Simple,
                    escaped_slice,
                ) else {
                    break;
                };
                write_log_fields_json(&mut json_buf, &fields);
                json_buf.push(b'\n');
                json_count += 1;
            }

            if json_count == 0 {
                assert_eq!(
                    arrow_batch.num_rows(),
                    0,
                    "{label}: expected 0 rows when fields overflow"
                );
                return;
            }

            let config = ScanConfig {
                extract_all: true,
                ..Default::default()
            };
            let mut scanner = Scanner::new(config);
            let json_batch = scanner
                .scan_detached(bytes::Bytes::from(json_buf))
                .expect("valid JSON");

            assert_batches_identical(&arrow_batch, &json_batch, label);
        }

        // Proptest: randomized counter, timestamp config, and message template.
        proptest!(ProptestConfig::with_cases(200), |(
            start in 0u64..1_000_000,
            n in 1usize..500,
            start_epoch_ms in 0i64..2_000_000_000_000i64,
            step_ms in -100i64..100,
            use_template in proptest::bool::ANY,
        )| {
            let ts_config = GeneratorTimestamp { start_epoch_ms, step_ms };
            let template_str = "GET /api/test 200";
            let template = if use_template { Some(template_str) } else { None };
            let label = format!("proptest(start={start},n={n},epoch={start_epoch_ms},step={step_ms},tmpl={use_template})");
            run_comparison(start, n, &ts_config, template, &label);
        });

        // Also run deterministic edge cases: counter 0, max rotation coverage,
        // negative timestamp step, and large offsets.
        for (start, n, label) in [
            (0, 420, "all_rotations"), // LCM(4,5,4,3)=60 × 7 = covers all combos
            (u64::MAX - 10, 5, "near_u64_max"),
            (999_999, 200, "high_counter"),
        ] {
            run_comparison(start, n, &GeneratorTimestamp::default(), None, label);
        }

        // Negative step timestamp
        run_comparison(
            0,
            100,
            &GeneratorTimestamp {
                start_epoch_ms: 1_705_276_800_000,
                step_ms: -1,
            },
            None,
            "negative_step",
        );

        // Template with JSON-special characters (quotes, backslashes, newlines)
        run_comparison(
            0,
            50,
            &GeneratorTimestamp::default(),
            Some("hello \"world\" \n\t\\end"),
            "special_chars_template",
        );
    }

    /// Oracle test: use the real `GeneratorInput::poll()` → `Scanner` pipeline
    /// as ground truth, and verify `generate_arrow_batch()` matches it exactly.
    ///
    /// Unlike `arrow_matches_json_path` which reconstructs JSON via
    /// `write_log_fields_json()`, this test exercises the actual production
    /// JSON code path (`GeneratorInput::generate_batch` → `write_logs_event`).
    #[test]
    fn arrow_matches_real_generator_oracle() {
        use arrow::array::AsArray;
        use logfwd_arrow::Scanner;
        use logfwd_core::scan_config::ScanConfig;

        for (batch_size, total, label) in [
            (100, 100, "single_batch"),
            (50, 200, "multi_batch"),
            (1, 10, "single_event_batches"),
            (500, 500, "large_batch"),
        ] {
            let ts_config = GeneratorTimestamp::default();
            let config = GeneratorConfig {
                batch_size,
                total_events: total,
                events_per_sec: 0, // unlimited
                timestamp: ts_config,
                profile: GeneratorProfile::Logs,
                complexity: GeneratorComplexity::Simple,
                message_template: None,
                ..Default::default()
            };
            let mut generator = GeneratorInput::new("oracle-test", config);

            // Collect all JSON bytes from the real generator
            let mut all_json = Vec::new();
            loop {
                let events = generator.poll().expect("poll succeeded");
                if events.is_empty() {
                    break;
                }
                for event in events {
                    if let InputEvent::Data { bytes, .. } = event {
                        all_json.extend_from_slice(&bytes);
                    }
                }
            }
            assert!(!all_json.is_empty(), "{label}: generator produced no bytes");

            // Scan JSON through the real scanner
            let scan_config = ScanConfig {
                extract_all: true,
                ..Default::default()
            };
            let mut scanner = Scanner::new(scan_config);
            let json_batch = scanner
                .scan_detached(bytes::Bytes::from(all_json))
                .expect("valid JSON");

            // Generate Arrow-direct batch for the same events
            let arrow_batch = generate_arrow_batch(total as usize, 0, &ts_config, None);

            assert_eq!(
                arrow_batch.num_rows(),
                json_batch.num_rows(),
                "{label}: row count mismatch (arrow={} json={})",
                arrow_batch.num_rows(),
                json_batch.num_rows(),
            );

            let n = arrow_batch.num_rows();

            // Compare all string columns
            for col_name in &["timestamp", "level", "message", "request_id", "service"] {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json missing {col_name}"));

                let arrow_strs = arrow_col.as_string::<i32>();
                let json_strs = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not StringArray"));

                for row in 0..n {
                    assert_eq!(
                        arrow_strs.value(row),
                        json_strs.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }

            // Compare all int columns
            for col_name in &["duration_ms", "status"] {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json missing {col_name}"));

                let arrow_ints = arrow_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in arrow"));
                let json_ints = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in json"));

                for row in 0..n {
                    assert_eq!(
                        arrow_ints.value(row),
                        json_ints.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }
        }
    }

    #[test]
    fn generates_valid_json_lines() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 20,
                total_events: 20,
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().lines().collect();
            assert_eq!(lines.len(), 20);
            // Every line must parse as valid JSON.
            for (i, line) in lines.iter().enumerate() {
                assert!(
                    serde_json::from_str::<serde_json::Value>(line).is_ok(),
                    "line {i} is not valid JSON: {line}"
                );
            }
            assert!(lines[0].contains("\"level\":\"INFO\""));
            assert!(lines[0].contains("\"service\":"));
        } else {
            panic!("expected Data event");
        }

        // Should be done after total_events.
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn complex_generates_valid_json_lines() {
        let mut input = GeneratorInput::new(
            "test-complex",
            GeneratorConfig {
                batch_size: 50,
                total_events: 50,
                complexity: GeneratorComplexity::Complex,
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().lines().collect();
            assert_eq!(lines.len(), 50);
            let mut saw_nested = false;
            for (i, line) in lines.iter().enumerate() {
                let val: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("line {i} invalid JSON: {e}\n{line}"));
                if val.get("headers").is_some() || val.get("upstream").is_some() {
                    saw_nested = true;
                }
            }
            assert!(saw_nested, "complex mode should produce nested objects");
        } else {
            panic!("expected Data event");
        }
    }

    #[test]
    fn rate_limited_returns_empty_when_called_too_soon() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 10,
                events_per_sec: 1, // 1 event/sec => ~10s per batch of 10
                total_events: 0,
                ..Default::default()
            },
        );

        // First call succeeds.
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        // Immediate second call should return empty (not block).
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn rate_limited_can_emit_multiple_batches_per_poll() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 1_000,
                events_per_sec: 200_000,
                total_events: 5_000,
                ..Default::default()
            },
        );

        // Initial seeded credits emit one batch immediately.
        let first = input.poll().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(input.events_generated(), 1_000);

        // Simulate a 20ms scheduler interval: 4k events should be due.
        input.last_refill = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(20))
            .unwrap_or_else(std::time::Instant::now);

        let second = input.poll().unwrap();
        let emitted_rows: usize = second
            .iter()
            .map(|event| match event {
                InputEvent::Data { bytes, .. } => memchr::memchr_iter(b'\n', bytes).count(),
                _ => 0,
            })
            .sum();

        assert!(
            second.len() >= 2,
            "expected multiple batches in one poll at high EPS, got {}",
            second.len()
        );
        assert_eq!(emitted_rows, 4_000);
        assert_eq!(input.events_generated(), 5_000);
        assert!(input.poll().unwrap().is_empty());
    }

    #[test]
    fn rate_limited_waits_for_a_full_batch_of_credit() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 1_000,
                events_per_sec: 500,
                total_events: 0,
                ..Default::default()
            },
        );

        let first = input.poll().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(input.events_generated(), 1_000);

        input.last_refill = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(500))
            .unwrap_or_else(std::time::Instant::now);
        assert!(input.poll().unwrap().is_empty());
        assert_eq!(input.events_generated(), 1_000);

        input.last_refill = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(2))
            .unwrap_or_else(std::time::Instant::now);
        let second = input.poll().unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(input.events_generated(), 2_000);
    }

    #[test]
    fn rate_limited_discards_credit_beyond_burst_cap() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 1_000,
                events_per_sec: 5_000,
                total_events: 20_000,
                ..Default::default()
            },
        );

        let first = input.poll().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(input.events_generated(), 1_000);

        input.last_refill = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(10))
            .unwrap_or_else(std::time::Instant::now);
        let second = input.poll().unwrap();
        let emitted_rows: usize = second
            .iter()
            .map(|event| match event {
                InputEvent::Data { bytes, .. } => memchr::memchr_iter(b'\n', bytes).count(),
                _ => 0,
            })
            .sum();
        assert_eq!(second.len(), 5);
        assert_eq!(emitted_rows, 5_000);
        assert_eq!(input.events_generated(), 6_000);

        assert!(input.poll().unwrap().is_empty());
        assert_eq!(input.events_generated(), 6_000);
    }

    #[test]
    fn rate_limited_allows_final_partial_batch_for_finite_total_events() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 1_000,
                events_per_sec: 100,
                total_events: 1_500,
                ..Default::default()
            },
        );

        let first = input.poll().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(input.events_generated(), 1_000);

        input.last_refill = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(5))
            .unwrap_or_else(std::time::Instant::now);
        let second = input.poll().unwrap();
        assert_eq!(second.len(), 1);
        let emitted_rows: usize = second
            .iter()
            .map(|event| match event {
                InputEvent::Data { bytes, .. } => memchr::memchr_iter(b'\n', bytes).count(),
                _ => 0,
            })
            .sum();
        assert_eq!(emitted_rows, 500);
        assert_eq!(input.events_generated(), 1_500);
        assert!(input.poll().unwrap().is_empty());
    }

    #[test]
    fn unlimited_keeps_going() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 100,
                total_events: 0, // infinite
                ..Default::default()
            },
        );

        for _ in 0..5 {
            let events = input.poll().unwrap();
            assert_eq!(events.len(), 1);
        }
    }

    fn extract_timestamp(line: &str) -> &str {
        let start = line.find("\"timestamp\":\"").unwrap() + 13;
        let end = start + line[start..].find('"').unwrap();
        &line[start..end]
    }

    #[test]
    fn timestamps_are_monotonically_increasing() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 2000,
                total_events: 2000,
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.trim().lines().collect();
        assert_eq!(lines.len(), 2000);

        let mut prev = String::new();
        for (i, line) in lines.iter().enumerate() {
            let ts = extract_timestamp(line);
            if i > 0 {
                assert!(
                    ts > prev.as_str(),
                    "timestamp at event {i} ({ts}) must be > previous ({prev})"
                );
            }
            prev = ts.to_string();
        }
    }

    #[test]
    fn negative_step_produces_decreasing_timestamps() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 100,
                total_events: 100,
                timestamp: GeneratorTimestamp {
                    start_epoch_ms: 1_705_276_800_000,
                    step_ms: -1,
                },
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.trim().lines().collect();

        let first_ts = extract_timestamp(lines[0]);
        let last_ts = extract_timestamp(lines[99]);
        assert!(
            first_ts > last_ts,
            "negative step should decrease: {first_ts} vs {last_ts}"
        );
    }

    #[test]
    fn custom_start_timestamp() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 1,
                total_events: 1,
                timestamp: GeneratorTimestamp {
                    start_epoch_ms: 0,
                    step_ms: 1,
                },
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let text = String::from_utf8_lossy(bytes);
        let line = text.trim().lines().next().unwrap();
        let ts = extract_timestamp(line);
        assert_eq!(ts, "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn epoch_ms_to_parts_known_values() {
        let (y, mo, d, h, mi, s, ms) = epoch_ms_to_parts(1_705_276_800_000);
        assert_eq!((y, mo, d, h, mi, s, ms), (2024, 1, 15, 0, 0, 0, 0));

        let (y, mo, d, h, mi, s, ms) = epoch_ms_to_parts(0);
        assert_eq!((y, mo, d, h, mi, s, ms), (1970, 1, 1, 0, 0, 0, 0));

        let leap_ms = days_from_civil(2000, 2, 29) * 86_400_000
            + 23 * 3_600_000
            + 59 * 60_000
            + 59 * 1000
            + 999;
        let (y, mo, d, h, mi, s, ms) = epoch_ms_to_parts(leap_ms);
        assert_eq!((y, mo, d, h, mi, s, ms), (2000, 2, 29, 23, 59, 59, 999));
    }

    #[test]
    fn parse_iso8601_roundtrip() {
        let ms = parse_iso8601_to_epoch_ms("2024-01-15T00:00:00Z").unwrap();
        assert_eq!(ms, 1_705_276_800_000);

        let ms = parse_iso8601_to_epoch_ms("1970-01-01T00:00:00Z").unwrap();
        assert_eq!(ms, 0);

        assert!(parse_iso8601_to_epoch_ms("not-a-date").is_err());
        assert!(parse_iso8601_to_epoch_ms("2024-13-01T00:00:00Z").is_err());
        assert!(parse_iso8601_to_epoch_ms("2024-02-31T00:00:00Z").is_err());
        assert!(parse_iso8601_to_epoch_ms("2023-02-29T00:00:00Z").is_err());
        assert!(parse_iso8601_to_epoch_ms("2024-02-29T00:00:00Z").is_ok());
    }

    #[test]
    fn proptest_generated_json_always_valid() {
        // Inline proptest runner: validate JSON for a range of counter offsets
        // by skipping past initial batches to reach different counter values.
        use proptest::prelude::*;

        proptest!(|(offset in 0u64..1000)| {
            // We generate (offset + 1) events and check the last one.
            let total = offset + 1;
            let mut generator = GeneratorInput::new(
                "test",
                GeneratorConfig {
                    batch_size: total as usize,
                    total_events: total,
                    ..Default::default()
                },
            );
            let events = generator.poll().unwrap();
            assert_eq!(events.len(), 1, "poll() must produce exactly one Data event (offset={offset})");
            match &events[0] {
                InputEvent::Data { bytes, .. } => {
                    assert!(!bytes.is_empty(), "generator produced empty data (offset={offset})");
                    let text = String::from_utf8(bytes.clone()).unwrap();
                    let line_count = text.trim().lines().count();
                    assert!(line_count >= 1, "expected at least 1 JSON line, got 0 (offset={offset})");
                    for (i, line) in text.trim().lines().enumerate() {
                        serde_json::from_str::<serde_json::Value>(line)
                            .unwrap_or_else(|e| panic!("invalid JSON at event {i} (offset={offset}): {e}\n{line}"));
                    }
                }
                _ => panic!("unexpected event variant"),
            }
        });
    }

    #[test]
    fn proptest_complex_json_always_valid() {
        use proptest::prelude::*;

        proptest!(|(offset in 0u64..500)| {
            let total = offset + 1;
            let mut generator = GeneratorInput::new(
                "test",
                GeneratorConfig {
                    batch_size: total as usize,
                    total_events: total,
                    complexity: GeneratorComplexity::Complex,
                    ..Default::default()
                },
            );
            let events = generator.poll().unwrap();
            assert_eq!(events.len(), 1, "poll() must produce exactly one Data event (offset={offset})");
            match &events[0] {
                InputEvent::Data { bytes, .. } => {
                    assert!(!bytes.is_empty(), "generator produced empty data (offset={offset})");
                    let text = String::from_utf8(bytes.clone()).unwrap();
                    let line_count = text.trim().lines().count();
                    assert!(line_count >= 1, "expected at least 1 JSON line, got 0 (offset={offset})");
                    for (i, line) in text.trim().lines().enumerate() {
                        serde_json::from_str::<serde_json::Value>(line)
                            .unwrap_or_else(|e| panic!("invalid JSON at event {i} (offset={offset}): {e}\n{line}"));
                    }
                }
                _ => panic!("unexpected event variant"),
            }
        });
    }

    #[test]
    fn message_template_with_special_chars_produces_valid_json() {
        // Regression test: message_template with quotes, backslashes, newlines,
        // and control chars must still produce valid JSON.
        let templates = [
            r#"quotes "here" and 'there'"#,
            "backslash \\ and tab \t inside",
            "newline\ninside template",
            "carriage\r\nreturn",
            "control\x01chars\x1f",
            r#"mixed "all" \types\ of 'special' chars"#,
        ];
        for tmpl in &templates {
            for complexity in [GeneratorComplexity::Simple, GeneratorComplexity::Complex] {
                let mut generator = GeneratorInput::new(
                    "test",
                    GeneratorConfig {
                        batch_size: 10,
                        total_events: 10,
                        complexity,
                        message_template: Some(tmpl.to_string()),
                        ..Default::default()
                    },
                );
                let events = generator.poll().unwrap();
                let InputEvent::Data { bytes, .. } = &events[0] else {
                    panic!("expected Data event");
                };
                let text = String::from_utf8(bytes.clone()).unwrap();
                for (i, line) in text.trim().lines().enumerate() {
                    let parsed: serde_json::Value = serde_json::from_str(line).unwrap_or_else(
                        |e| {
                            panic!(
                                "invalid JSON at event {i} with template {tmpl:?} ({complexity:?}): {e}\n{line}"
                            )
                        },
                    );
                    let msg = parsed["message"]
                        .as_str()
                        .expect("message field should be a string");
                    assert_eq!(msg, *tmpl, "message content should match template");
                }
            }
        }
    }

    #[test]
    fn events_generated_counter() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 10,
                total_events: 25,
                ..Default::default()
            },
        );
        assert_eq!(input.events_generated(), 0);
        let _ = input.poll().unwrap();
        assert_eq!(input.events_generated(), 10);
        let _ = input.poll().unwrap();
        assert_eq!(input.events_generated(), 20);
        let _ = input.poll().unwrap();
        assert_eq!(input.events_generated(), 25);
    }

    #[test]
    fn generator_respects_total_events() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 7, // not a divisor of 50 — exercises partial-batch logic
                total_events: 50,
                ..Default::default()
            },
        );

        let mut total_lines = 0u64;
        loop {
            let events = input.poll().unwrap();
            if events.is_empty() {
                break;
            }
            for event in &events {
                if let InputEvent::Data { bytes, .. } = event {
                    let text = String::from_utf8_lossy(bytes);
                    total_lines += text.trim().lines().count() as u64;
                }
            }
        }

        assert_eq!(
            total_lines, 50,
            "expected exactly 50 events, got {total_lines}"
        );
        assert_eq!(input.events_generated(), 50);

        // Subsequent polls must return empty.
        let events = input.poll().unwrap();
        assert!(events.is_empty(), "poll after completion must be empty");
    }

    #[test]
    fn record_profile_emits_attributes_and_generated_fields() {
        let mut input = GeneratorInput::new(
            "bench",
            GeneratorConfig {
                batch_size: 3,
                total_events: 3,
                profile: GeneratorProfile::Record,
                attributes: HashMap::from([
                    (
                        "benchmark_id".to_string(),
                        GeneratorAttributeValue::String("run-123".to_string()),
                    ),
                    (
                        "pod_name".to_string(),
                        GeneratorAttributeValue::String("emitter-0".to_string()),
                    ),
                    (
                        "stream_id".to_string(),
                        GeneratorAttributeValue::String("emitter-0".to_string()),
                    ),
                    (
                        "service".to_string(),
                        GeneratorAttributeValue::String("bench-emitter".to_string()),
                    ),
                    ("status".to_string(), GeneratorAttributeValue::Integer(200)),
                    ("sampled".to_string(), GeneratorAttributeValue::Bool(true)),
                ]),
                sequence: Some(GeneratorGeneratedField {
                    field: "seq".to_string(),
                    start: 1,
                }),
                event_created_unix_nano_field: Some("event_created_unix_nano".to_string()),
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["benchmark_id"], "run-123");
        assert_eq!(first["pod_name"], "emitter-0");
        assert_eq!(first["stream_id"], "emitter-0");
        assert_eq!(first["seq"], 1);
        assert_eq!(first["service"], "bench-emitter");
        assert_eq!(first["status"], 200);
        assert_eq!(first["sampled"], true);
        assert!(first.get("event_created_unix_nano").is_some());
    }

    #[test]
    fn record_profile_supports_custom_sequence_start() {
        let mut input = GeneratorInput::new(
            "bench-input",
            GeneratorConfig {
                batch_size: 2,
                total_events: 2,
                profile: GeneratorProfile::Record,
                sequence: Some(GeneratorGeneratedField {
                    field: "seq".to_string(),
                    start: 10,
                }),
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let rows: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(rows[0]["seq"], 10);
        assert_eq!(rows[1]["seq"], 11);
    }

    #[test]
    fn record_profile_stops_on_sequence_overflow() {
        let mut input = GeneratorInput::new(
            "bench-input",
            GeneratorConfig {
                batch_size: 2,
                total_events: 2,
                profile: GeneratorProfile::Record,
                sequence: Some(GeneratorGeneratedField {
                    field: "seq".to_string(),
                    start: u64::MAX,
                }),
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let rows: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["seq"], u64::MAX);
        assert!(input.poll().unwrap().is_empty());
    }

    #[test]
    fn record_profile_escapes_string_fields() {
        let mut input = GeneratorInput::new(
            "bench",
            GeneratorConfig {
                batch_size: 1,
                total_events: 1,
                profile: GeneratorProfile::Record,
                attributes: HashMap::from([
                    (
                        "benchmark_id".to_string(),
                        GeneratorAttributeValue::String("run-\"quoted\"\nline".to_string()),
                    ),
                    (
                        "pod_name".to_string(),
                        GeneratorAttributeValue::String("pod-\\name".to_string()),
                    ),
                    (
                        "stream_id".to_string(),
                        GeneratorAttributeValue::String("stream-\"id\"".to_string()),
                    ),
                    (
                        "service".to_string(),
                        GeneratorAttributeValue::String("svc\tname".to_string()),
                    ),
                    ("ratio".to_string(), GeneratorAttributeValue::Float(1.25)),
                    ("sampled".to_string(), GeneratorAttributeValue::Bool(false)),
                ]),
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let row: serde_json::Value =
            serde_json::from_slice(bytes.split(|b| *b == b'\n').next().unwrap()).unwrap();
        assert_eq!(row["benchmark_id"], "run-\"quoted\"\nline");
        assert_eq!(row["pod_name"], "pod-\\name");
        assert_eq!(row["stream_id"], "stream-\"id\"");
        assert_eq!(row["service"], "svc\tname");
        assert_eq!(row["ratio"], 1.25);
        assert_eq!(row["sampled"], false);
    }

    #[test]
    fn encode_static_field_serializes_floats_as_json_numbers() {
        let encoded = encode_static_field("ratio", &GeneratorAttributeValue::Float(1.0));
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), "\"ratio\":1.0");
    }

    #[test]
    fn encode_static_field_serializes_non_finite_floats_as_null() {
        let encoded = encode_static_field("ratio", &GeneratorAttributeValue::Float(f64::NAN));
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), "\"ratio\":null");
    }

    #[test]
    fn encode_static_field_serializes_null_attribute() {
        let encoded = encode_static_field("deleted_at", &GeneratorAttributeValue::Null);
        assert_eq!(
            std::str::from_utf8(&encoded).unwrap(),
            "\"deleted_at\":null"
        );
    }

    // -----------------------------------------------------------------------
    // Oracle & property-based tests for date math
    // -----------------------------------------------------------------------

    #[test]
    fn epoch_ms_to_parts_matches_chrono() {
        use chrono::{DateTime, Datelike, Timelike};
        use proptest::prelude::*;

        let max_ms = 7_258_118_400_000_i64;
        proptest!(|(ms in 0_i64..max_ms)| {
            let (year, month, day, hour, min, sec, millis) = epoch_ms_to_parts(ms);
            let dt = DateTime::from_timestamp_millis(ms).unwrap();
            prop_assert_eq!(year, dt.year());
            prop_assert_eq!(month, dt.month());
            prop_assert_eq!(day, dt.day());
            prop_assert_eq!(hour, dt.hour());
            prop_assert_eq!(min, dt.minute());
            prop_assert_eq!(sec, dt.second());
            prop_assert_eq!(millis, dt.nanosecond() / 1_000_000);
        });
    }

    #[test]
    fn epoch_ms_to_parts_negative_matches_chrono() {
        use chrono::{DateTime, Datelike, Timelike};
        use proptest::prelude::*;

        let min_ms = -2_208_988_800_000_i64;
        proptest!(|(ms in min_ms..0_i64)| {
            let (year, month, day, hour, min, sec, millis) = epoch_ms_to_parts(ms);
            let dt = DateTime::from_timestamp_millis(ms).unwrap();
            prop_assert_eq!(year, dt.year());
            prop_assert_eq!(month, dt.month());
            prop_assert_eq!(day, dt.day());
            prop_assert_eq!(hour, dt.hour());
            prop_assert_eq!(min, dt.minute());
            prop_assert_eq!(sec, dt.second());
            prop_assert_eq!(millis, dt.nanosecond() / 1_000_000);
        });
    }

    #[test]
    fn civil_days_roundtrip() {
        use proptest::prelude::*;

        proptest!(|(y in 1_i32..3000, m in 1_u32..=12, d in 1_u32..=28)| {
            let days = days_from_civil(y, m, d);
            let (y2, m2, d2) = civil_from_days(days);
            prop_assert_eq!((y, m, d), (y2, m2, d2));
        });
    }

    #[test]
    fn parse_iso8601_roundtrip_proptest() {
        use proptest::prelude::*;

        proptest!(|(
            y in 1970_i32..2200,
            m in 1_u32..=12,
            d in 1_u32..=28,
            h in 0_u32..=23,
            mi in 0_u32..=59,
            s in 0_u32..=59,
        )| {
            let input = format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z");
            let ms = parse_iso8601_to_epoch_ms(&input).unwrap();
            let (y2, m2, d2, h2, mi2, s2, ms2) = epoch_ms_to_parts(ms);
            prop_assert_eq!((y, m, d, h, mi, s, 0_u32), (y2, m2, d2, h2, mi2, s2, ms2));
        });
    }

    #[test]
    fn timestamp_boundary_crossings() {
        use chrono::{DateTime, Datelike, Timelike};

        struct Case {
            label: &'static str,
            start: &'static str,
            step_ms: i64,
            count: usize,
            checks: Vec<(usize, &'static str)>,
        }

        let cases = [
            Case {
                label: "midnight crossing",
                start: "2024-01-15T23:59:59Z",
                step_ms: 1000,
                count: 5,
                checks: vec![(0, "2024-01-15T23:59:59"), (1, "2024-01-16T00:00:00")],
            },
            Case {
                label: "leap day crossing (2024)",
                start: "2024-02-28T23:59:58Z",
                step_ms: 1000,
                count: 5,
                checks: vec![(0, "2024-02-28T23:59:58"), (2, "2024-02-29T00:00:00")],
            },
            Case {
                label: "leap day to March",
                start: "2024-02-29T23:59:58Z",
                step_ms: 1000,
                count: 5,
                checks: vec![(0, "2024-02-29T23:59:58"), (2, "2024-03-01T00:00:00")],
            },
            Case {
                label: "non-leap Feb to March",
                start: "2023-02-28T23:59:58Z",
                step_ms: 1000,
                count: 5,
                checks: vec![(0, "2023-02-28T23:59:58"), (2, "2023-03-01T00:00:00")],
            },
            Case {
                label: "year-end crossing",
                start: "2024-12-31T23:59:58Z",
                step_ms: 1000,
                count: 5,
                checks: vec![(0, "2024-12-31T23:59:58"), (2, "2025-01-01T00:00:00")],
            },
            Case {
                label: "backward across midnight",
                start: "2024-01-16T00:00:02Z",
                step_ms: -1000,
                count: 5,
                checks: vec![(0, "2024-01-16T00:00:02"), (3, "2024-01-15T23:59:59")],
            },
        ];

        for case in &cases {
            let start_ms = parse_iso8601_to_epoch_ms(case.start).unwrap();
            let mut input = GeneratorInput::new(
                "boundary",
                GeneratorConfig {
                    batch_size: case.count,
                    total_events: case.count as u64,
                    timestamp: GeneratorTimestamp {
                        start_epoch_ms: start_ms,
                        step_ms: case.step_ms,
                    },
                    ..Default::default()
                },
            );
            let events = input.poll().unwrap();
            let InputEvent::Data { bytes, .. } = &events[0] else {
                panic!("{}: expected Data event", case.label);
            };
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().lines().collect();

            for &(idx, expected_prefix) in &case.checks {
                let ts = extract_timestamp(lines[idx]);
                assert!(
                    ts.starts_with(expected_prefix),
                    "{}: event {idx} timestamp {ts} does not start with {expected_prefix}",
                    case.label
                );
            }

            // Verify every event against chrono as oracle
            for (i, line) in lines.iter().enumerate() {
                let ts = extract_timestamp(line);
                let event_ms = start_ms + (i as i64) * case.step_ms;
                let dt = DateTime::from_timestamp_millis(event_ms).unwrap();
                let expected = format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour(),
                    dt.minute(),
                    dt.second(),
                    dt.nanosecond() / 1_000_000,
                );
                assert_eq!(ts, expected, "{}: event {i} mismatch", case.label);
            }
        }
    }

    #[test]
    fn prop_positive_step_always_monotonic() {
        use proptest::prelude::*;

        proptest!(|(
            start_ms in 0_i64..4_102_444_800_000_i64,
            step_ms in 1_i64..10_000,
        )| {
            let mut input = GeneratorInput::new(
                "prop",
                GeneratorConfig {
                    batch_size: 100,
                    total_events: 100,
                    timestamp: GeneratorTimestamp { start_epoch_ms: start_ms, step_ms },
                    ..Default::default()
                },
            );
            let events = input.poll().unwrap();
            let InputEvent::Data { bytes, .. } = &events[0] else { panic!("expected Data"); };
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().lines().collect();
            let timestamps: Vec<&str> = lines.iter().map(|l| extract_timestamp(l)).collect();
            for i in 1..timestamps.len() {
                prop_assert!(timestamps[i] > timestamps[i - 1]);
            }
        });
    }

    #[test]
    fn prop_negative_step_always_decreasing() {
        use proptest::prelude::*;

        proptest!(|(
            start_ms in 1_000_000_000_i64..4_102_444_800_000_i64,
            step_ms in -10_000_i64..=-1,
        )| {
            let mut input = GeneratorInput::new(
                "prop",
                GeneratorConfig {
                    batch_size: 100,
                    total_events: 100,
                    timestamp: GeneratorTimestamp { start_epoch_ms: start_ms, step_ms },
                    ..Default::default()
                },
            );
            let events = input.poll().unwrap();
            let InputEvent::Data { bytes, .. } = &events[0] else { panic!("expected Data"); };
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().lines().collect();
            let timestamps: Vec<&str> = lines.iter().map(|l| extract_timestamp(l)).collect();
            for i in 1..timestamps.len() {
                prop_assert!(timestamps[i] < timestamps[i - 1]);
            }
        });
    }

    #[test]
    fn prop_json_valid_with_any_timestamp_config() {
        use proptest::prelude::*;

        proptest!(|(
            start_ms in 0_i64..4_102_444_800_000_i64,
            step_ms in -1000_i64..1000,
            count in 1_u64..50,
        )| {
            prop_assume!(step_ms != 0);
            let mut generator = GeneratorInput::new(
                "prop",
                GeneratorConfig {
                    batch_size: count as usize,
                    total_events: count,
                    timestamp: GeneratorTimestamp { start_epoch_ms: start_ms, step_ms },
                    ..Default::default()
                },
            );
            let events = generator.poll().unwrap();
            prop_assert_eq!(events.len(), 1);
            let InputEvent::Data { bytes, .. } = &events[0] else { panic!("expected Data"); };
            let text = String::from_utf8(bytes.clone()).unwrap();
            for (i, line) in text.trim().lines().enumerate() {
                prop_assert!(
                    serde_json::from_str::<serde_json::Value>(line).is_ok(),
                    "invalid JSON at event {i}: {line}"
                );
            }
        });
    }

    /// Throughput comparison: Arrow-direct vs JSON→Scanner.
    ///
    /// Not a Criterion bench — just a quick sanity check that the Arrow path
    /// is significantly faster. Run with `--release --nocapture -- --ignored` to see numbers.
    ///
    /// Ignored by default: wall-clock assertions are inherently flaky under CI
    /// load. Run manually to verify performance claims.
    #[test]
    #[ignore = "wall-clock speedup assertion is flaky under CI load"]
    fn arrow_vs_json_throughput() {
        use logfwd_arrow::Scanner;
        use logfwd_core::scan_config::ScanConfig;
        use std::time::Instant;

        let ts_config = GeneratorTimestamp::default();
        let message_template: Option<&str> = None;
        let n = 10_000;
        let rounds = 5;

        // Arrow-direct path
        let start = Instant::now();
        for r in 0..rounds {
            let batch = generate_arrow_batch(n, (r * n) as u64, &ts_config, message_template);
            std::hint::black_box(&batch);
        }
        let arrow_elapsed = start.elapsed();

        // JSON→Scanner path
        let config = ScanConfig {
            extract_all: true,
            ..Default::default()
        };
        let mut scanner = Scanner::new(config);
        let start = Instant::now();
        for r in 0..rounds {
            let mut json_buf = Vec::with_capacity(n * 256);
            for i in 0..n {
                let counter = (r * n) as u64 + i as u64;
                let fields = compute_log_fields(
                    counter,
                    &ts_config,
                    GeneratorComplexity::Simple,
                    message_template.map(str::as_bytes),
                )
                .expect("valid counter");
                write_log_fields_json(&mut json_buf, &fields);
                json_buf.push(b'\n');
            }
            let batch = scanner
                .scan_detached(bytes::Bytes::from(json_buf))
                .expect("valid JSON");
            std::hint::black_box(&batch);
        }
        let json_elapsed = start.elapsed();

        let total = (n * rounds) as f64;
        let arrow_eps = total / arrow_elapsed.as_secs_f64();
        let json_eps = total / json_elapsed.as_secs_f64();
        let speedup = arrow_eps / json_eps;

        eprintln!(
            "\n  Arrow direct: {:.2}M EPS ({:.1}ms)\n  JSON→Scanner: {:.2}M EPS ({:.1}ms)\n  Speedup:      {:.1}x\n",
            arrow_eps / 1e6,
            arrow_elapsed.as_secs_f64() * 1000.0,
            json_eps / 1e6,
            json_elapsed.as_secs_f64() * 1000.0,
            speedup,
        );

        // Arrow should be at least 2x faster in debug, much more in release
        assert!(
            speedup > 1.5,
            "Arrow path should be significantly faster (got {speedup:.1}x)"
        );
    }
}
