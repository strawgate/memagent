// Configuration types for the generator input.

use std::collections::HashMap;
use std::io;
use std::io::Write;

use bytes::Bytes;

use crate::input::{InputSource, SourceEvent};
use ffwd_types::diagnostics::ComponentHealth;

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
#[derive(Debug)]
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