//! Pure pipeline state machine — formally verified batch lifecycle.
//!
//! Separates pipeline decision logic from IO/async. The Rust compiler
//! enforces state transitions via typestate pattern (illegal transitions
//! are compile errors). Kani proves business invariants; TLA+ liveness
//! modeling is planned in Phase 7.
//!
//! # Architecture
//!
//! Two components:
//! - [`BatchTicket`]: Per-batch lifecycle via typestate (Queued → Sending → Acked/Rejected)
//! - [`PipelineMachine`]: Pipeline lifecycle + ordered offset tracking

mod batch;
mod lifecycle;

// Batch ticket types
pub use batch::{AckReceipt, BatchId, BatchTicket, Queued, Sending, SourceId};

// Pipeline lifecycle types
pub use lifecycle::{
    CommitAdvance, CreateBatchError, Draining, PipelineMachine, Running, Starting, Stopped,
};
