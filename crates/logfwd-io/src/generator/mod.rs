//! Synthetic data generator input source.
//!
//! Produces JSON log lines at a configurable rate. Used for benchmarking
//! and testing pipelines without external data sources.

use std::collections::HashMap;
use std::io;
use std::io::Write;

use crate::input::{InputEvent, InputSource};
use logfwd_types::diagnostics::ComponentHealth;

pub mod cardinality;
pub mod cloudtrail;
pub mod cri;
pub mod envoy;
mod logs;
pub mod shared;
pub mod wide;

#[cfg(test)]
mod generator_tests;
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod test_support;

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
    /// Realistic Envoy edge-proxy access logs (~420 bytes/line).
    Envoy,
    /// CRI-formatted Kubernetes container logs (~350 bytes/line).
    CriK8s,
    /// Wide structured logs with 20+ fields (~600 bytes/line).
    Wide,
    /// Narrow JSON logs with 5 fields (~120 bytes/line).
    Narrow,
    /// CloudTrail-like AWS audit log events (~1400 bytes/line).
    CloudTrail,
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
    /// Template for message string.
    pub message_template: Option<String>,
    /// Number of extra synthetic fields.
    pub field_count: Option<usize>,
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
    /// Seed for deterministic RNG in profiles that use randomness.
    pub seed: u64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            events_per_sec: 0,
            batch_size: 1000,
            total_events: 0,
            message_template: None,
            field_count: None,
            complexity: GeneratorComplexity::default(),
            profile: GeneratorProfile::default(),
            attributes: HashMap::new(),
            sequence: None,
            event_created_unix_nano_field: None,
            timestamp: GeneratorTimestamp::default(),
            seed: 42,
        }
    }
}

/// Per-profile runtime state for streaming event generation.
enum ProfileState {
    /// No extra state needed (uses counter only).
    Logs,
    /// No extra state needed (uses counter and record_fields).
    Record,
    /// Shared RNG for `CriK8s` and `Narrow` profiles.
    SimpleRng(fastrand::Rng),
    /// Wide profile: RNG + cardinality state (boxed to reduce variant size).
    Wide(
        Box<(
            fastrand::Rng,
            cardinality::cardinality_helpers::CardinalityState,
        )>,
    ),
    /// Envoy profile: RNG + access profile + burst state.
    Envoy(
        fastrand::Rng,
        envoy::EnvoyAccessProfile,
        envoy::EnvoyStreamState,
    ),
    /// CloudTrail profile: RNG + profile + pre-computed pools (boxed to reduce variant size).
    CloudTrail(
        Box<(
            fastrand::Rng,
            shared::CloudTrailProfile,
            cloudtrail::CloudTrailState,
        )>,
    ),
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
    profile_state: ProfileState,
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
pub(super) fn epoch_ms_to_parts(epoch_ms: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
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
    /// Create a new `GeneratorInput` from the given configuration.
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
        let profile_state = Self::init_profile_state(config.profile, config.seed);
        Self {
            name,
            buf: Vec::with_capacity(config.batch_size * 512),
            config,
            counter: 0,
            done: false,
            record_fields,
            last_refill: std::time::Instant::now(),
            rate_credit_events: initial_rate_credit_events,
            profile_state,
        }
    }

    fn init_profile_state(profile: GeneratorProfile, seed: u64) -> ProfileState {
        match profile {
            GeneratorProfile::Logs => ProfileState::Logs,
            GeneratorProfile::Record => ProfileState::Record,
            GeneratorProfile::Envoy => ProfileState::Envoy(
                fastrand::Rng::with_seed(seed),
                envoy::EnvoyAccessProfile::benchmark(),
                envoy::EnvoyStreamState::new(),
            ),
            GeneratorProfile::CriK8s | GeneratorProfile::Narrow => {
                ProfileState::SimpleRng(fastrand::Rng::with_seed(seed))
            }
            GeneratorProfile::Wide => ProfileState::Wide(Box::new((
                fastrand::Rng::with_seed(seed),
                cardinality::cardinality_helpers::CardinalityState::new(
                    cardinality::cardinality_helpers::CardinalityProfile::infra_like(),
                ),
            ))),
            GeneratorProfile::CloudTrail => {
                let ct_profile = shared::CloudTrailProfile::benchmark_default();
                let ct_state = cloudtrail::CloudTrailState::new(ct_profile);
                ProfileState::CloudTrail(Box::new((
                    fastrand::Rng::with_seed(seed),
                    ct_profile,
                    ct_state,
                )))
            }
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
        // Copy the profile discriminant to release the borrow on `self.config`
        // before taking mutable borrows on `self.buf`, `self.done`, etc.
        let profile = self.config.profile;
        match profile {
            GeneratorProfile::Logs => {
                logs::write_logs_event(
                    &mut self.buf,
                    self.counter,
                    self.config.complexity,
                    &self.config.timestamp,
                    self.config.message_template.as_deref(),
                    self.config.field_count,
                    &mut self.done,
                );
            }
            GeneratorProfile::Record => {
                logs::write_record_event(
                    &mut self.buf,
                    &self.record_fields,
                    self.counter,
                    &mut self.done,
                    event_created_unix_nano,
                );
            }
            GeneratorProfile::Envoy => {
                if let ProfileState::Envoy(ref mut rng, ref ep, ref mut stream_state) =
                    self.profile_state
                {
                    envoy::write_envoy_line(
                        &mut self.buf,
                        rng,
                        ep,
                        self.counter as usize,
                        stream_state,
                    );
                }
            }
            GeneratorProfile::CriK8s => {
                if let ProfileState::SimpleRng(ref mut rng) = self.profile_state {
                    cri::write_cri_line(&mut self.buf, rng, self.counter as usize);
                }
            }
            GeneratorProfile::Wide => {
                if let ProfileState::Wide(ref mut state) = self.profile_state {
                    let (ref mut rng, ref mut card) = **state;
                    wide::write_wide_line(&mut self.buf, rng, card, self.counter as usize);
                }
            }
            GeneratorProfile::Narrow => {
                if let ProfileState::SimpleRng(ref mut rng) = self.profile_state {
                    cri::write_narrow_line(&mut self.buf, rng, self.counter as usize);
                }
            }
            GeneratorProfile::CloudTrail => {
                if let ProfileState::CloudTrail(ref mut boxed) = self.profile_state {
                    let (ref mut rng, ref ct_profile, ref ct_state) = **boxed;
                    cloudtrail::write_cloudtrail_line(
                        &mut self.buf,
                        rng,
                        ct_profile,
                        ct_state,
                        self.counter as usize,
                    );
                }
            }
        }
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

pub(super) fn write_json_u64_field(out: &mut Vec<u8>, key: &str, value: u64, first: &mut bool) {
    if !*first {
        out.push(b',');
    }
    *first = false;
    write_json_key(out, key);
    let _ = write!(out, ":{value}");
}

pub(super) fn write_json_u128_field(out: &mut Vec<u8>, key: &str, value: u128, first: &mut bool) {
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

        // Simulate a huge scheduler stall: 1000 seconds of credits.
        // The burst cap is max(events_per_sec, batch_size) = 5_000 events.
        // Use checked_sub since Instant can't go before the monotonic epoch;
        // fall back to a shorter but still sufficient stall.
        input.last_refill = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(1000))
            .or_else(|| std::time::Instant::now().checked_sub(std::time::Duration::from_secs(10)))
            .expect("system uptime too short for rate-limit test");

        let second = input.poll().unwrap();
        let emitted_rows: usize = second
            .iter()
            .map(|event| match event {
                InputEvent::Data { bytes, .. } => memchr::memchr_iter(b'\n', bytes).count(),
                _ => 0,
            })
            .sum();

        // Should emit at most burst_cap events = 5_000.
        assert!(
            emitted_rows <= 5_000,
            "expected at most 5000 events after burst cap, got {emitted_rows}"
        );
        assert!(emitted_rows >= 1_000, "expected at least one batch");
    }

    #[test]
    fn finite_generates_exact_count_in_unlimited_mode() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 1_000,
                total_events: 3_500,
                ..Default::default()
            },
        );

        let mut total = 0;
        let mut iterations = 0;
        loop {
            let events = input.poll().unwrap();
            if events.is_empty() {
                break;
            }
            for event in &events {
                if let InputEvent::Data { bytes, .. } = event {
                    total += memchr::memchr_iter(b'\n', bytes).count();
                }
            }
            iterations += 1;
            if iterations > 100 {
                break;
            }
        }
        assert_eq!(total, 3_500);
    }

    #[test]
    fn record_profile_generates_attributes() {
        use super::GeneratorAttributeValue;

        let mut attributes = HashMap::new();
        attributes.insert(
            "host".to_string(),
            GeneratorAttributeValue::String("srv1".to_string()),
        );
        attributes.insert(
            "region".to_string(),
            GeneratorAttributeValue::String("us-west".to_string()),
        );

        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 5,
                total_events: 5,
                profile: GeneratorProfile::Record,
                attributes,
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("expected Data event");
        };
        let text = String::from_utf8_lossy(bytes);
        for line in text.lines() {
            let val: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
            assert_eq!(val["host"], "srv1");
            assert_eq!(val["region"], "us-west");
        }
    }

    #[test]
    fn envoy_profile_generates_valid_json() {
        let mut input = GeneratorInput::new(
            "test-envoy",
            GeneratorConfig {
                batch_size: 50,
                total_events: 50,
                profile: GeneratorProfile::Envoy,
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            for (i, line) in text.lines().enumerate() {
                assert!(
                    serde_json::from_str::<serde_json::Value>(line).is_ok(),
                    "line {i} is not valid JSON: {line}"
                );
            }
        } else {
            panic!("expected Data event");
        }
    }

    #[test]
    fn cri_profile_generates_cri_format() {
        let mut input = GeneratorInput::new(
            "test-cri",
            GeneratorConfig {
                batch_size: 50,
                total_events: 50,
                profile: GeneratorProfile::CriK8s,
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            for (i, line) in text.lines().enumerate() {
                let parts: Vec<&str> = line.splitn(4, ' ').collect();
                assert_eq!(parts.len(), 4, "line {i} must have 4 CRI parts: {line}");
                assert!(
                    parts[2] == "P" || parts[2] == "F",
                    "line {i} flag must be P or F: {line}"
                );
            }
        } else {
            panic!("expected Data event");
        }
    }

    #[test]
    fn narrow_profile_generates_valid_json() {
        let mut input = GeneratorInput::new(
            "test-narrow",
            GeneratorConfig {
                batch_size: 30,
                total_events: 30,
                profile: GeneratorProfile::Narrow,
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            for (i, line) in text.lines().enumerate() {
                assert!(
                    serde_json::from_str::<serde_json::Value>(line).is_ok(),
                    "line {i} is not valid JSON: {line}"
                );
            }
        } else {
            panic!("expected Data event");
        }
    }

    #[test]
    fn wide_profile_generates_valid_json() {
        let mut input = GeneratorInput::new(
            "test-wide",
            GeneratorConfig {
                batch_size: 20,
                total_events: 20,
                profile: GeneratorProfile::Wide,
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            for (i, line) in text.lines().enumerate() {
                let val: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("line {i} invalid JSON: {e}\n{line}"));
                // Wide profile must have many fields
                assert!(
                    val.as_object().map(|o| o.len()).unwrap_or(0) >= 10,
                    "line {i} expected wide JSON (10+ fields): {line}"
                );
            }
        } else {
            panic!("expected Data event");
        }
    }

    #[test]
    fn cloudtrail_profile_generates_valid_json() {
        let mut input = GeneratorInput::new(
            "test-cloudtrail",
            GeneratorConfig {
                batch_size: 20,
                total_events: 20,
                profile: GeneratorProfile::CloudTrail,
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            for (i, line) in text.lines().enumerate() {
                assert!(
                    serde_json::from_str::<serde_json::Value>(line).is_ok(),
                    "line {i} is not valid JSON: {line}"
                );
            }
        } else {
            panic!("expected Data event");
        }
    }
}

#[cfg(test)]
mod tests_custom_generator {
    use super::*;

    #[test]
    fn custom_message_template() {
        let mut input = GeneratorInput::new(
            "test-template",
            GeneratorConfig {
                batch_size: 1,
                total_events: 1,
                message_template: Some("custom static message".to_string()),
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            let val: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(val["message"], "custom static message");
        } else {
            panic!("expected Data event");
        }
    }

    #[test]
    fn custom_field_count() {
        let mut input = GeneratorInput::new(
            "test-field-count",
            GeneratorConfig {
                batch_size: 1,
                total_events: 1,
                field_count: Some(2),
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            let val: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(val["field_0"], "val_0");
            assert_eq!(val["field_1"], "val_1");
            assert_eq!(val.get("field_2"), None);
        } else {
            panic!("expected Data event");
        }
    }
}
