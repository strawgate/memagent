//! Pure pipeline state machine — formally verified batch lifecycle.
//!
//! Separates pipeline decision logic from IO/async. The Rust compiler
//! enforces state transitions via typestate pattern (illegal transitions
//! are compile errors). Kani proves business invariants.
//!
//! # Architecture
//!
//! Two typestate machines, generic over checkpoint type `C`:
//!
//! - `BatchTicket<S, C>`: Per-batch lifecycle (Queued → Sending → Acked/Rejected).
//!   `#[must_use]` + self-consuming transitions prevent double-ack and silent drops.
//!   Fields are private; only `PipelineMachine` can create tickets.
//!
//! - `PipelineMachine<S, C>`: Pipeline lifecycle (Starting → Running → Draining → Stopped).
//!   Ordered ACK: committed checkpoint advances only when ALL prior batches
//!   for a source are acked (Filebeat registrar pattern).
//!
//! The checkpoint type `C` is **opaque** to the pipeline. Each input source
//! defines what a checkpoint means:
//! - File tail: `u64` byte offset
//! - Kafka: `i64` partition offset
//! - Journald: `String` cursor
//! - Push sources (OTLP, syslog): `()` (no checkpoint)

mod batch;
mod lifecycle;
mod registry;

// Batch ticket types
pub use batch::{AckReceipt, BatchId, BatchTicket, Queued, Sending, SourceId};

// Pipeline lifecycle types
pub use lifecycle::{CheckpointAdvance, Draining, PipelineMachine, Running, Starting, Stopped};

// Source registry types
pub use registry::{SourceEntry, SourceRegistry, SourceState};
