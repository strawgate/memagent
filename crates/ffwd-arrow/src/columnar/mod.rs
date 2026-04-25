//! Shared columnar construction engine.
//!
//! This module houses the planning and lifecycle types for the shared
//! `ColumnarBatchBuilder` direction:
//!
//! - `row_protocol::RowLifecycle` — batch/row state machine
//! - `plan::BatchPlan` — field registry and handle allocator
//! - `plan::FieldHandle` — stable column reference
//! - `plan::FieldKind` — protocol-agnostic column type
//!
//! See `dev-docs/research/columnar-batch-builder.md` for the design intent.

pub mod accumulator;
pub mod builder;
pub mod plan;
pub(crate) mod row_protocol;
