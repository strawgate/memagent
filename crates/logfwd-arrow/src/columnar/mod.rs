//! Shared columnar construction engine.
//!
//! This module will eventually house the `ColumnarBatchBuilder` and its
//! supporting types (`BatchPlan`, `FieldHandle`, `FieldKind`).  For now it
//! contains only the extracted row lifecycle protocol — the state machine
//! that enforces the begin_batch → begin_row → end_row → finish_batch
//! call sequence shared by all columnar builders.

pub(crate) mod row_protocol;
