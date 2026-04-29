//! OpAMP client integration for ffwd.
//!
//! This crate implements the [OpAMP protocol](https://opentelemetry.io/docs/specs/opamp/)
//! for remote agent management. It allows a central OpAMP server to:
//!
//! - Push configuration changes to ffwd instances
//! - Monitor agent health and effective configuration
//! - Issue restart/reconfigure commands
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐         ┌──────────────┐         ┌─────────────┐
//! │ OpAMP Server│◄────────│  OpampClient │────────►│  bootstrap  │
//! │ (Go ref srv)│  HTTP   │ (this crate) │ reload  │ reload loop │
//! └─────────────┘         └──────────────┘  tx     └─────────────┘
//! ```
//!
//! The [`OpampClient`] runs as a background task and communicates with the
//! bootstrap reload loop via a `tokio::sync::mpsc` channel (same channel used
//! by SIGHUP, `--watch-config`, and `POST /api/v1/reload`).

mod client;
mod error;
mod identity;

pub use client::OpampClient;
pub use client::OpampStateHandle;
pub use error::OpampError;
pub use ffwd_config::OpampConfig;
pub use identity::AgentIdentity;
