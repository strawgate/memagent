//! Shared configuration types used across inputs and outputs.
//!
//! These types provide a uniform config surface so every input and output uses
//! the same structs and naming for cross-cutting concerns like TLS, retries,
//! batching, compression, and network settings.

use crate::serde_helpers::{
    deserialize_from_string_or_value, deserialize_option_from_string_or_value,
    deserialize_option_strict_string,
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

/// Retry configuration for transient failures.
///
/// Provides exponential backoff with jitter. When all fields are `None` the
/// output falls back to its built-in defaults.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Max retry attempts. Use -1 for infinite. Default: 3.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_attempts: Option<i32>,
    /// Initial backoff delay in seconds. Default: 1.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub initial_backoff_secs: Option<u64>,
    /// Maximum backoff delay in seconds. Default: 60.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_backoff_secs: Option<u64>,
}

// ── Batch ──────────────────────────────────────────────────────────────

/// Batch configuration for aggregating events before sending.
///
/// Outputs collect events into batches and flush when *any* limit is reached.
/// When all fields are `None` the output falls back to its built-in defaults.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    /// Maximum events per batch.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_events: Option<usize>,
    /// Maximum bytes per batch.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_bytes: Option<usize>,
    /// Max time before flushing a partial batch, in seconds.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub timeout_secs: Option<u64>,
}

// ── Compression ────────────────────────────────────────────────────────

/// Compression algorithm for output payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Zstd,
    Snappy,
    Lz4,
}

// ── Network ──────────────────────────────────────────────────────────

/// Network and connection configuration shared across inputs and outputs.
///
/// Provides uniform timeout and connection limit fields. When all fields
/// are `None` the component falls back to its built-in defaults.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    /// Request or send timeout in seconds.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub timeout_secs: Option<u64>,
    /// Connection establishment timeout in seconds.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub connection_timeout_secs: Option<u64>,
    /// Maximum concurrent connections.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_connections: Option<usize>,
    /// Maximum incoming message or packet size in bytes.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_message_size_bytes: Option<usize>,
}
