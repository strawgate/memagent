//! Shared configuration types used across inputs and outputs.
//!
//! Today this is just the client/server TLS structs. Per-component retry,
//! batch, network, and compression knobs live on each input/output's own
//! typed config (see `OutputConfigV2` and the input types in `types.rs`).

use crate::serde_helpers::{
    PositiveMillis, PositiveSecs, deserialize_from_string_or_value,
    deserialize_option_from_string_or_value, deserialize_option_strict_string,
};
use serde::Deserialize;

// ── TLS ────────────────────────────────────────────────────────────────

/// Client-side TLS for outbound connections (outputs connecting to servers).
///
/// Used by HTTP, OTLP, Elasticsearch, Loki, and other output sinks that
/// establish TLS connections to a remote endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsClientConfig {
    /// Path to CA certificate file for server verification.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub ca_file: Option<String>,
    /// Path to client certificate file (mTLS).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub cert_file: Option<String>,
    /// Path to client private key file (mTLS).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub key_file: Option<String>,
    /// Skip server certificate verification. Default: false.
    #[serde(default, deserialize_with = "deserialize_from_string_or_value")]
    pub insecure_skip_verify: bool,
}

/// Server-side TLS for inbound connections (inputs accepting connections).
///
/// Used by TCP, OTLP, and other input sources that listen on a port and
/// accept TLS connections from clients.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsServerConfig {
    /// Path to server certificate file.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub cert_file: Option<String>,
    /// Path to server private key file.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub key_file: Option<String>,
    /// Path to CA certificate file for client verification (mTLS).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub client_ca_file: Option<String>,
    /// Require client certificate authentication. Default: false.
    #[serde(default, deserialize_with = "deserialize_from_string_or_value")]
    pub require_client_auth: bool,
}

// ── Retry ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_attempts: Option<i32>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub initial_backoff_secs: Option<PositiveSecs>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_backoff_secs: Option<PositiveSecs>,
}

// ── Batch ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_bytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_events: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub timeout_secs: Option<PositiveSecs>,
}

// ── Rotation ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
pub struct RotationConfig {
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_megabytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_days: Option<PositiveSecs>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_backups: Option<usize>,
}

// ── Multiline ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MultilineConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub start_pattern: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub mode: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub timeout_ms: Option<PositiveMillis>,
}

// ── Network ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub keepalive_enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub keepalive_time_secs: Option<PositiveSecs>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub timeout_secs: Option<PositiveSecs>,
}
