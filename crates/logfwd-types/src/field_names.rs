//! Canonical OTLP log field names and heuristic variants.
//!
//! This module is the **single source of truth** for column names written by
//! receivers and read by sinks.  Both sides import from here so a rename in
//! one place cannot silently break the other.
//!
//! # Design
//!
//! Each semantic role has a **canonical** constant — the name our own receivers
//! produce — and an optional set of **heuristic variants** that sinks also
//! accept when consuming batches from external sources (e.g. SQL transforms or
//! user-defined schemas).
//!
//! Sinks MUST check the canonical name first, then fall through to variants.
// xtask-verify: allow(pub_module_needs_tests) reason: constant definitions validated by sink/receiver integration tests

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

/// Canonical timestamp column — unix epoch nanoseconds (Int64 or Utf8).
///
/// Written by the OTLP receiver for `timeUnixNano`.
pub const TIMESTAMP: &str = "timestamp";

/// Additional names sinks accept for timestamps from external sources.
/// Includes Elasticsearch-style `@timestamp` and pipeline-style `_timestamp`.
pub const TIMESTAMP_VARIANTS: &[&str] = &["time", "ts", "@timestamp", "_timestamp"];

/// Elasticsearch-style timestamp field (`@timestamp`).
pub const TIMESTAMP_AT: &str = "@timestamp";

/// Alternative timestamp column used by some pipelines.
pub const TIMESTAMP_UNDERSCORE: &str = "_timestamp";

/// CRI stream column (`stdout` / `stderr`) attached by the input pipeline.
///
/// # Examples
///
/// ```
/// use logfwd_types::field_names;
///
/// assert_eq!(field_names::CRI_STREAM, "_stream");
/// ```
pub const CRI_STREAM: &str = "_stream";

// ---------------------------------------------------------------------------
// Severity / Level
// ---------------------------------------------------------------------------

/// Canonical severity column — severity text string (e.g. "INFO", "ERROR").
///
/// Written by the OTLP receiver for `severityText`.
pub const SEVERITY: &str = "level";

/// Additional names sinks accept for severity from external sources.
pub const SEVERITY_VARIANTS: &[&str] = &["severity", "log_level", "loglevel", "lvl"];

// ---------------------------------------------------------------------------
// Body / Message
// ---------------------------------------------------------------------------

/// Canonical body column — the log message text.
///
/// Written by the OTLP receiver for the log record `body`.
pub const BODY: &str = "body";

/// Additional names sinks accept for body from external sources.
pub const BODY_VARIANTS: &[&str] = &["message", "msg", "_msg"];

// ---------------------------------------------------------------------------
// Trace context
// ---------------------------------------------------------------------------

/// Canonical trace ID column — hex-encoded 32-char string.
pub const TRACE_ID: &str = "trace_id";

/// Canonical span ID column — hex-encoded 16-char string.
pub const SPAN_ID: &str = "span_id";

/// Canonical trace flags column — Int64.
pub const TRACE_FLAGS: &str = "trace_flags";

/// Additional names sinks accept for trace flags.
pub const TRACE_FLAGS_VARIANTS: &[&str] = &["flags"];

// ---------------------------------------------------------------------------
// Observed timestamp
// ---------------------------------------------------------------------------

/// Observed timestamp column — collector-set receive time in nanoseconds.
///
/// Written by the OTLP receiver for `observedTimeUnixNano`.
pub const OBSERVED_TIMESTAMP: &str = "observed_timestamp";

// ---------------------------------------------------------------------------
// Severity number
// ---------------------------------------------------------------------------

/// Numeric severity column — OTLP severity number (1-24).
///
/// Written by the OTLP receiver for `severityNumber`.
pub const SEVERITY_NUMBER: &str = "severity_number";

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

/// Log record flags column — W3C trace flags (UInt32).
///
/// Written by the OTLP receiver for `flags`.
pub const FLAGS: &str = "flags";

// ---------------------------------------------------------------------------
// Instrumentation Scope
// ---------------------------------------------------------------------------

/// Instrumentation scope name column.
///
/// Written by the OTLP receiver for `InstrumentationScope.name`.
pub const SCOPE_NAME: &str = "scope.name";

/// Instrumentation scope version column.
///
/// Written by the OTLP receiver for `InstrumentationScope.version`.
pub const SCOPE_VERSION: &str = "scope.version";

// ---------------------------------------------------------------------------
// Resource attribute prefix
// ---------------------------------------------------------------------------

/// Default prefix for resource attribute columns.
///
/// The OTLP receiver prefixes resource attribute keys with this string so
/// output sinks can distinguish resource-level from log-level attributes.
/// Example: OTLP `service.name` → column `resource.attributes.service.name`.
pub const DEFAULT_RESOURCE_PREFIX: &str = "resource.attributes.";

/// Legacy resource attribute prefix used before the `resource.attributes.`
/// convention. Sinks check this as a fallback for backwards compatibility
/// with older batches and config-level `resource_attrs`.
pub const LEGACY_RESOURCE_PREFIX: &str = "_resource_";

// ---------------------------------------------------------------------------
// Source metadata columns
// ---------------------------------------------------------------------------

/// FastForward row-level source identity assigned by the input layer.
pub const SOURCE_ID: &str = "__source_id";

/// ECS/Beats-style source file path column.
pub const ECS_FILE_PATH: &str = "file.path";

/// OpenTelemetry log semantic convention source file path column.
pub const OTEL_LOG_FILE_PATH: &str = "log.file.path";

/// Vector legacy file-source path column.
pub const VECTOR_FILE: &str = "file";

// ---------------------------------------------------------------------------
// Type-conflict struct children (Arrow schema)
// ---------------------------------------------------------------------------

/// Field names used inside StructArrays that represent type-conflict
/// resolution (e.g. a column with mixed int/string values).
pub const CONFLICT_CHILDREN: &[&str] = &["int", "float", "str", "bool"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check whether `name` matches the canonical constant or any variant.
///
/// ```
/// # use logfwd_types::field_names;
/// assert!(field_names::matches_any("ts", field_names::TIMESTAMP, field_names::TIMESTAMP_VARIANTS));
/// assert!(field_names::matches_any("timestamp", field_names::TIMESTAMP, field_names::TIMESTAMP_VARIANTS));
/// assert!(!field_names::matches_any("date", field_names::TIMESTAMP, field_names::TIMESTAMP_VARIANTS));
/// ```
pub fn matches_any(name: &str, canonical: &str, variants: &[&str]) -> bool {
    name == canonical || variants.contains(&name)
}

/// Return true when `name` is owned by FastForward internals.
///
/// This intentionally matches only known internal columns. User payloads can
/// legitimately contain double-underscore fields such as GraphQL `__typename`,
/// and output filtering must not drop them by prefix.
pub fn is_internal_column(name: &str) -> bool {
    matches!(name, SOURCE_ID)
}
