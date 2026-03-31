//! Pure pipeline state machine — formally verified batch lifecycle.
//!
//! Separates pipeline decision logic from IO/async. The Rust compiler
//! enforces state transitions via typestate pattern (illegal transitions
//! are compile errors). Kani proves business invariants. TLA+ proves
//! liveness.
//!
//! # Architecture
//!
//! Two components:
//! - [`BatchTicket`]: Per-batch lifecycle via typestate (Queued → Sending → Acked)
//! - [`PipelineMachine`]: Pipeline lifecycle + ordered offset tracking

mod batch;
mod lifecycle;

pub use batch::*;
pub use lifecycle::*;
