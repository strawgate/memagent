pub mod atomic_write;
pub(crate) mod background_http_task;
pub mod error;
pub use error::InputError;

pub mod arrow_ipc_receiver;
pub mod checkpoint;
pub mod compress;
pub mod filter_hints;
pub mod format;
pub mod framed;
pub mod generator;
/// HTTP NDJSON input source.
pub mod http_input;
pub mod input;
/// FFI bindings for `libsystemd.so.0` `sd_journal` API (runtime dlopen).
pub mod journal_ffi;
/// Journald (systemd journal) input — native API with subprocess fallback.
pub mod journald_input;
pub mod otap_receiver;
pub mod otlp_receiver;
/// Platform sensor inputs and Arrow-native control/sample event emission.
pub mod platform_sensor;
/// Adaptive polling primitives shared by file-tail and runtime input loops.
pub mod poll_cadence;
pub(crate) mod polling_input_health;
pub(crate) mod receiver_health;
pub(crate) mod receiver_http;
/// Checkpoint segment file format, writer, reader, and recovery.
pub mod segment;
pub mod tail;
pub mod tcp_input;
pub mod udp_input;
