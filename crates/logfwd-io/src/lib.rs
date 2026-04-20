pub mod atomic_write;
pub(crate) mod background_http_task;
/// Fixed-worker blocking stages for crate-internal CPU work.
pub(crate) mod blocking_stage;
pub mod error;
pub use error::InputError;

pub mod arrow_ipc_receiver;
pub mod checkpoint;
pub mod compress;
pub mod filter_hints;
pub mod format;
pub mod framed;
pub mod generator;
/// Host metrics inputs backed by periodic system snapshots.
pub mod host_metrics;
/// HTTP NDJSON input source.
pub mod http_input;
pub mod input;
/// FFI bindings for `libsystemd.so.0` `sd_journal` API (runtime dlopen).
pub mod journal_ffi;
/// Journald (systemd journal) input — native API with subprocess fallback.
pub mod journald_input;
pub mod otap_receiver;
pub mod otlp_receiver;
/// eBPF-based platform sensor input (Linux only).
#[cfg(target_os = "linux")]
pub mod platform_sensor;
#[cfg(any(target_os = "linux", test))]
pub(crate) mod platform_sensor_filter;
/// Adaptive polling primitives shared by file-tail and runtime input loops.
pub mod poll_cadence;
pub(crate) mod polling_input_health;
pub(crate) mod receiver_health;
pub(crate) mod receiver_http;
/// High-performance S3 (and S3-compatible) object storage input.
#[cfg(feature = "s3")]
pub mod s3_input;
/// Checkpoint segment file format, writer, reader, and recovery.
pub mod segment;
pub mod tail;
pub mod tcp_input;
pub mod udp_input;
