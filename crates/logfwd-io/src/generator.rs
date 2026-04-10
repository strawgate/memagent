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
    last_batch: std::time::Instant,
    record_fields: RecordFields,
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
        Self {
            name,
            buf: Vec::with_capacity(config.batch_size * 512),
            config,
            counter: 0,
            done: false,
            record_fields,
            // Use a time far in the past so the first poll() always succeeds.
            last_batch: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(3600))
                .unwrap_or_else(std::time::Instant::now),
        }
    }

    /// Return the total number of events generated so far.
    pub fn events_generated(&self) -> u64 {
        self.counter
    }

    fn generate_batch(&mut self) {
        self.buf.clear();
        let n = self.config.batch_size;
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
        let seq = self.counter;
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
        let Ok(counter_i64) = i64::try_from(self.counter) else {
            self.done = true;
            return;
        };
        let Some(event_ms) = counter_i64
            .checked_mul(self.config.timestamp.step_ms)
            .and_then(|offset| self.config.timestamp.start_epoch_ms.checked_add(offset))
        else {
            self.done = true;
            return;
        };
        let (year, month, day, hour, min, sec, msec) = epoch_ms_to_parts(event_ms);

        match self.config.complexity {
            GeneratorComplexity::Simple => {
                let _ = write!(
                    self.buf,
                    r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{method} {path}/{id} {status}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status}}}"#,
                );
            }
            GeneratorComplexity::Complex => {
                let bytes_in = 128 + seq.wrapping_mul(17) % 8192;
                let bytes_out = 64 + seq.wrapping_mul(31) % 4096;
                if seq % 5 == 0 {
                    let _ = write!(
                        self.buf,
                        r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{method} {path}/{id} {status}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status},"bytes_in":{bytes_in},"bytes_out":{bytes_out},"headers":{{"content-type":"application/json","x-request-id":"{rid:016x}"}},"tags":["web","{service}","{level}"]}}"#,
                    );
                } else if seq % 7 == 0 {
                    let upstream_ms = 1 + seq.wrapping_mul(19) % 200;
                    let _ = write!(
                        self.buf,
                        r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{method} {path}/{id} {status}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status},"bytes_in":{bytes_in},"bytes_out":{bytes_out},"upstream":[{{"host":"10.0.0.1","latency_ms":{upstream_ms}}},{{"host":"10.0.0.2","latency_ms":{dur}}}]}}"#,
                    );
                } else {
                    let _ = write!(
                        self.buf,
                        r#"{{"timestamp":"{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{msec:03}Z","level":"{level}","message":"{method} {path}/{id} {status}","duration_ms":{dur},"request_id":"{rid:016x}","service":"{service}","status":{status},"bytes_in":{bytes_in},"bytes_out":{bytes_out}}}"#,
                    );
                }
            }
        }
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

        // Rate limiting: if events_per_sec > 0, return empty if called too
        // soon rather than blocking the thread. The caller drives the poll
        // loop and can decide how to wait.
        if self.config.events_per_sec > 0 {
            let target_interval = std::time::Duration::from_secs_f64(
                self.config.batch_size as f64 / self.config.events_per_sec as f64,
            );
            let elapsed = self.last_batch.elapsed();
            if elapsed < target_interval {
                return Ok(vec![]);
            }
        }

        self.last_batch = std::time::Instant::now();
        self.generate_batch();

        if self.buf.is_empty() {
            return Ok(vec![]);
        }

        // Swap buffers to preserve capacity (avoid realloc every batch).
        let mut out = Vec::with_capacity(self.config.batch_size * 512);
        std::mem::swap(&mut self.buf, &mut out);
        let accounted_bytes = out.len() as u64;
        Ok(vec![InputEvent::Data {
            bytes: out,
            source_id: None,
            accounted_bytes,
        }])
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_health_is_explicitly_healthy() {
        let input = GeneratorInput::new("test", GeneratorConfig::default());
        assert_eq!(input.health(), ComponentHealth::Healthy);
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
}
