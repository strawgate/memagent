//! Shared configuration types used across inputs and outputs.
//!
//! These types provide a uniform config surface so every input and output uses
//! the same structs and naming for cross-cutting concerns like TLS, retries,
//! batching, compression, and network settings.

use serde::Deserialize;

// ── TLS ────────────────────────────────────────────────────────────────

/// Client-side TLS for outbound connections (outputs connecting to servers).
///
/// Used by HTTP, OTLP, Elasticsearch, Loki, and other output sinks that
/// establish TLS connections to a remote endpoint.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsClientConfig {
    /// Path to CA certificate file for server verification.
    pub ca_file: Option<String>,
    /// Path to client certificate file (mTLS).
    pub cert_file: Option<String>,
    /// Path to client private key file (mTLS).
    pub key_file: Option<String>,
    /// Skip server certificate verification. Default: false.
    #[serde(default)]
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
    pub cert_file: Option<String>,
    /// Path to server private key file.
    pub key_file: Option<String>,
    /// Path to CA certificate file for client verification (mTLS).
    pub client_ca_file: Option<String>,
    /// Require client certificate authentication. Default: false.
    #[serde(default)]
    pub require_client_auth: bool,
}

/// Backward-compatible alias: [`TlsInputConfig`] is now [`TlsServerConfig`].
pub type TlsInputConfig = TlsServerConfig;

// ── Retry ──────────────────────────────────────────────────────────────

/// Retry configuration for transient failures.
///
/// Provides exponential backoff with jitter. When all fields are `None` the
/// output falls back to its built-in defaults.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Max retry attempts. Use -1 for infinite. Default: 3.
    pub max_attempts: Option<i32>,
    /// Initial backoff delay in seconds. Default: 1.
    pub initial_backoff_secs: Option<u64>,
    /// Maximum backoff delay in seconds. Default: 60.
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
    pub max_events: Option<usize>,
    /// Maximum bytes per batch.
    pub max_bytes: Option<usize>,
    /// Max time before flushing a partial batch, in seconds.
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
    pub timeout_secs: Option<u64>,
    /// Connection establishment timeout in seconds.
    pub connection_timeout_secs: Option<u64>,
    /// Maximum concurrent connections.
    pub max_connections: Option<usize>,
    /// Maximum incoming message or packet size in bytes.
    pub max_message_size_bytes: Option<usize>,
}
