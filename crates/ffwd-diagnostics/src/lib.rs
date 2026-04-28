// Diagnostic utilities use indexed access on internals with provable bounds.
#![allow(clippy::indexing_slicing, clippy::expect_used)]

pub(crate) mod background_http_task;
/// Diagnostics HTTP server, readiness policy, and OTLP JSON telemetry shaping.
pub mod diagnostics;
/// Tiered in-memory metric history buffers used by diagnostics endpoints.
pub mod metric_history;
/// In-process OpenTelemetry span buffering for diagnostics views.
pub mod span_exporter;
/// Stderr capture and ANSI-stripping utilities for diagnostics logs.
pub mod stderr_capture;
pub(crate) mod telemetry_buffer;
