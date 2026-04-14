// builder.rs — ColumnarBatchBuilder: shared column construction engine.
//
// Drives BatchPlan + FieldHandle → typed writes → Arrow materialization.
// Uses ColumnAccumulator for per-field storage (typed enum, no dead vecs).
//
// Scope:
//   - Planned fields: single-type columns, zero conflict overhead
//   - Dynamic fields: multi-type, conflict detection at finalization
//   - Detached string materialization via shared string buffer
//   - Sparse padding with deferred (row, value) facts
//   - StringViewArray zero-copy when given input buffer
//
// StreamingBuilder remains the scanner-facing ScanBuilder adapter.
// ColumnarBatchBuilder is for structured producers (OTLP, CSV, etc).

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringViewArray};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use logfwd_core::scanner::BuilderState;

use super::accumulator::{ColumnAccumulator, FinalizationMode, MaterializeError, StringRef};
use super::plan::{BatchPlan, FieldHandle, FieldKind, FieldSchemaMode, PlanError};
use super::row_protocol::RowLifecycle;
use crate::check_dup_bits;

// ---------------------------------------------------------------------------
// BuilderError
// ---------------------------------------------------------------------------

/// Errors from `ColumnarBatchBuilder` operations.
#[derive(Debug)]
pub enum BuilderError {
    /// Arrow error during RecordBatch construction.
    Arrow(ArrowError),
    /// String buffer exceeded u32 addressable range.
    StringBufferOverflow { buffer_len: usize, value_len: usize },
    /// Column materialization failed (invalid StringRef, bad UTF-8, etc).
    Materialize(MaterializeError),
}

impl std::fmt::Display for BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuilderError::Arrow(e) => write!(f, "Arrow error: {e}"),
            BuilderError::StringBufferOverflow {
                buffer_len,
                value_len,
            } => write!(
                f,
                "string buffer overflow: buffer_len={buffer_len}, value_len={value_len}, \
                 exceeds u32::MAX"
            ),
            BuilderError::Materialize(e) => write!(f, "materialize error: {e}"),
        }
    }
}

impl std::error::Error for BuilderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BuilderError::Arrow(e) => Some(e),
            BuilderError::Materialize(e) => Some(e),
            BuilderError::StringBufferOverflow { .. } => None,
        }
    }
}

impl From<ArrowError> for BuilderError {
    fn from(e: ArrowError) -> Self {
        BuilderError::Arrow(e)
    }
}

impl From<MaterializeError> for BuilderError {
    fn from(e: MaterializeError) -> Self {
        BuilderError::Materialize(e)
    }
}

// ---------------------------------------------------------------------------
// ColumnarBatchBuilder
// ---------------------------------------------------------------------------

/// Shared columnar batch builder driven by `BatchPlan` + `FieldHandle`.
///
/// Producers declare fields up front (planned) or discover them dynamically,
/// then write typed values via stable handles. At `finish_batch`, deferred
/// facts are materialized into an Arrow `RecordBatch`.
///
/// String values are appended to an internal buffer and referenced via
/// `StringRef`. At finalization, the buffer is passed to `FinalizationMode`
/// which controls whether strings are copied (Detached) or zero-copy (View).
///
/// # Design
///
/// - Each field gets a `ColumnAccumulator` matched to its schema mode:
///   planned Int64 → `ColumnAccumulator::Int64` (one vec, no conflict overhead),
///   dynamic → `ColumnAccumulator::Dynamic` (all vecs, conflict detection).
/// - Dedup uses the same bitmask approach as `RowLifecycle::written_bits`.
/// - `finish_batch` iterates fields and calls `accumulator.materialize()`.
pub struct ColumnarBatchBuilder {
    plan: BatchPlan,
    lifecycle: RowLifecycle,
    columns: Vec<ColumnAccumulator>,
    /// Optional original input buffer for zero-copy string references.
    /// `write_str_ref` offsets below `original_buf.len()` point here.
    original_buf: Vec<u8>,
    /// Generated buffer for string values written via `write_str`.
    /// StringRef offsets at or above `original_buf.len()` point here
    /// (shifted by original_buf.len()).
    string_buf: Vec<u8>,
    /// When false, skip per-field dedup checks (producers that guarantee
    /// at most one write per field per row can disable for throughput).
    dedup_enabled: bool,
}

impl ColumnarBatchBuilder {
    /// Create a builder from a plan.
    ///
    /// Allocates per-field storage matched to each field's schema mode.
    pub fn new(plan: BatchPlan) -> Self {
        let columns: Vec<ColumnAccumulator> = plan
            .fields()
            .map(|(_handle, _name, mode)| match mode {
                FieldSchemaMode::Planned(kind) => ColumnAccumulator::for_planned(*kind),
                FieldSchemaMode::Dynamic { .. } => ColumnAccumulator::dynamic(),
            })
            .collect();
        ColumnarBatchBuilder {
            plan,
            lifecycle: RowLifecycle::new(),
            columns,
            original_buf: Vec::new(),
            string_buf: Vec::new(),
            dedup_enabled: true,
        }
    }

    /// Disable per-field dedup checks.
    ///
    /// Producers that guarantee at most one write per field per row (e.g., OTLP
    /// decoders, CSV parsers) can disable dedup for ~8% throughput improvement.
    /// If a field is written twice in the same row with dedup disabled, both
    /// values are recorded and the second silently wins at materialization.
    pub fn set_dedup_enabled(&mut self, enabled: bool) {
        self.dedup_enabled = enabled;
    }

    /// Start a new batch.
    pub fn begin_batch(&mut self) {
        self.lifecycle.begin_batch();
        for col in &mut self.columns {
            col.clear();
        }
        self.original_buf.clear();
        self.string_buf.clear();
    }

    /// Set the original input buffer for zero-copy string references.
    ///
    /// Call after `begin_batch` and before any writes. `write_str_ref` with
    /// offsets below `original_buf.len()` will reference this buffer at
    /// materialization time.
    pub fn set_original_buffer(&mut self, buf: &[u8]) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InBatch);
        debug_assert!(
            self.original_buf.is_empty(),
            "set_original_buffer called twice in the same batch"
        );
        self.original_buf.extend_from_slice(buf);
    }

    /// Start a new row.
    #[inline]
    pub fn begin_row(&mut self) {
        self.lifecycle.begin_row();
    }

    /// Finish the current row.
    #[inline]
    pub fn end_row(&mut self) {
        self.lifecycle.end_row();
    }

    /// Resolve a dynamic field by name and observed kind.
    ///
    /// This is the runtime field-discovery path for JSON/CRI-style producers.
    /// Returns a stable handle that can be used for writes in the current row.
    pub fn resolve_dynamic(
        &mut self,
        name: &str,
        kind: FieldKind,
    ) -> Result<FieldHandle, PlanError> {
        let handle = self.plan.resolve_dynamic(name, kind)?;
        // Ensure storage exists for newly resolved fields.
        while self.columns.len() <= handle.index() {
            self.columns.push(ColumnAccumulator::dynamic());
        }
        Ok(handle)
    }

    // -----------------------------------------------------------------------
    // Dedup helper
    // -----------------------------------------------------------------------

    /// Check if a write to `handle` in the current row is a duplicate.
    ///
    /// Returns `true` if the write should be suppressed (duplicate).
    /// For fields 0–63, uses the bitmask. For fields ≥64, uses last_row.
    #[inline]
    fn is_duplicate(&mut self, handle: FieldHandle) -> bool {
        let idx = handle.index();
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return true;
        }
        if idx >= 64 {
            let col = &mut self.columns[idx];
            if col.last_row() == self.lifecycle.row_count() {
                return true;
            }
            *col.last_row_mut() = self.lifecycle.row_count();
        }
        false
    }

    // -----------------------------------------------------------------------
    // Typed write methods
    // -----------------------------------------------------------------------

    /// Write an i64 value for the given field in the current row.
    #[inline]
    pub fn write_i64(&mut self, handle: FieldHandle, value: i64) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if self.dedup_enabled && self.is_duplicate(handle) {
            return;
        }
        self.columns[handle.index()].push_i64(self.lifecycle.row_count(), value);
    }

    /// Write an f64 value for the given field in the current row.
    #[inline]
    pub fn write_f64(&mut self, handle: FieldHandle, value: f64) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if self.dedup_enabled && self.is_duplicate(handle) {
            return;
        }
        self.columns[handle.index()].push_f64(self.lifecycle.row_count(), value);
    }

    /// Write a bool value for the given field in the current row.
    #[inline]
    pub fn write_bool(&mut self, handle: FieldHandle, value: bool) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if self.dedup_enabled && self.is_duplicate(handle) {
            return;
        }
        self.columns[handle.index()].push_bool(self.lifecycle.row_count(), value);
    }

    /// Write a string value for the given field in the current row.
    ///
    /// The string is appended to the builder's internal buffer and
    /// referenced via `StringRef`. At finalization, the buffer is
    /// passed to `FinalizationMode` to produce the appropriate
    /// Arrow string type.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the string buffer would exceed u32 addressable range.
    #[inline]
    pub fn write_str(&mut self, handle: FieldHandle, value: &str) -> Result<(), BuilderError> {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if self.dedup_enabled && self.is_duplicate(handle) {
            return Ok(());
        }
        // Generated string offsets are shifted by original_buf.len() so that
        // the 2-buffer model can distinguish original vs generated at
        // materialization time.
        let raw_offset = self.original_buf.len() + self.string_buf.len();
        let offset = u32::try_from(raw_offset).map_err(|_| BuilderError::StringBufferOverflow {
            buffer_len: self.string_buf.len(),
            value_len: value.len(),
        })?;
        let len = u32::try_from(value.len()).map_err(|_| BuilderError::StringBufferOverflow {
            buffer_len: self.string_buf.len(),
            value_len: value.len(),
        })?;
        self.string_buf.extend_from_slice(value.as_bytes());
        let sref = StringRef { offset, len };
        self.columns[handle.index()].push_str(self.lifecycle.row_count(), sref);
        Ok(())
    }

    /// Write a `StringRef` directly (for zero-copy producers that already
    /// have offsets into an input buffer).
    #[inline]
    pub fn write_str_ref(&mut self, handle: FieldHandle, sref: StringRef) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if self.dedup_enabled && self.is_duplicate(handle) {
            return;
        }
        self.columns[handle.index()].push_str(self.lifecycle.row_count(), sref);
    }

    /// Write a null for the given field in the current row.
    ///
    /// Sparse fields are null by default; this just marks the field as
    /// written for dedup purposes.
    #[inline]
    pub fn write_null(&mut self, handle: FieldHandle) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if self.dedup_enabled {
            let _ = self.is_duplicate(handle);
        }
    }

    // -----------------------------------------------------------------------
    // Finalization
    // -----------------------------------------------------------------------

    /// Materialize all deferred facts into an Arrow `RecordBatch`.
    ///
    /// When utf8_trusted (the default), produces `StringViewArray` columns that
    /// reference the source buffers directly — zero per-string byte copying.
    /// The builder's internal buffers are transferred to Arrow (O(1) for reuse).
    pub fn finish_batch(&mut self) -> Result<RecordBatch, BuilderError> {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InBatch);
        let num_rows = self.lifecycle.row_count() as usize;

        // Copy to aligned Arrow Buffers, then clear (preserving Vec capacity
        // so the next batch avoids reallocation).
        let original = Buffer::from(self.original_buf.as_slice());
        let generated = Buffer::from(self.string_buf.as_slice());
        self.original_buf.clear();
        self.string_buf.clear();

        // utf8_trusted: true because write_str takes &str (Rust guarantees UTF-8)
        // and write_str_ref is only used with scanner-validated buffers.
        let mode = FinalizationMode::Detached {
            original_buf: original,
            generated_buf: generated,
            utf8_trusted: true,
        };

        let result = self.materialize_all(num_rows, mode)?;
        self.lifecycle.finish_batch();
        Ok(result)
    }

    /// Materialize with zero-copy StringViewArray.
    ///
    /// `original` is the input buffer (e.g., protobuf wire bytes).
    /// `original_len` is the length of the original buffer.
    /// Strings with `offset < original_len` reference the original buffer;
    /// strings with `offset >= original_len` reference the generated buffer.
    pub fn finish_batch_view(
        &mut self,
        original: Buffer,
        original_len: u32,
    ) -> Result<RecordBatch, BuilderError> {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InBatch);
        let num_rows = self.lifecycle.row_count() as usize;

        let generated = if self.string_buf.is_empty() {
            None
        } else {
            Some(Buffer::from(self.string_buf.as_slice()))
        };
        let mode = FinalizationMode::View {
            original,
            original_len,
            generated,
        };

        let result = self.materialize_all(num_rows, mode)?;
        self.lifecycle.finish_batch();
        Ok(result)
    }

    /// Core materialization shared by both detached and view paths.
    fn materialize_all(
        &self,
        num_rows: usize,
        mode: FinalizationMode,
    ) -> Result<RecordBatch, BuilderError> {
        let mut schema_fields = Vec::with_capacity(self.plan.len());
        let mut arrays = Vec::with_capacity(self.plan.len());

        for (handle, name, field_mode) in self.plan.fields() {
            let col = &self.columns[handle.index()];
            match col.materialize(name, num_rows, mode.clone())? {
                Some((field, array)) => {
                    schema_fields.push(field);
                    arrays.push(array);
                }
                None => {
                    // Planned fields always appear in the schema (all-null if
                    // no values were written). Dynamic fields with no data are
                    // omitted — this matches StreamingBuilder behavior.
                    if let FieldSchemaMode::Planned(kind) = field_mode {
                        let (field, array) = null_column(name, *kind, num_rows);
                        schema_fields.push(field);
                        arrays.push(array);
                    }
                }
            }
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        Ok(RecordBatch::try_new_with_options(schema, arrays, &opts)?)
    }

    /// Number of completed rows in the current batch.
    pub fn row_count(&self) -> u32 {
        self.lifecycle.row_count()
    }

    /// Reference to the underlying plan.
    pub fn plan(&self) -> &BatchPlan {
        &self.plan
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create an all-null column of the appropriate Arrow type for a planned field.
fn null_column(name: &str, kind: FieldKind, num_rows: usize) -> (Field, ArrayRef) {
    match kind {
        FieldKind::Int64 => {
            let nulls = NullBuffer::new_null(num_rows);
            let arr = Int64Array::new(vec![0i64; num_rows].into(), Some(nulls));
            (Field::new(name, DataType::Int64, true), Arc::new(arr))
        }
        FieldKind::Float64 => {
            let nulls = NullBuffer::new_null(num_rows);
            let arr = Float64Array::new(vec![0.0f64; num_rows].into(), Some(nulls));
            (Field::new(name, DataType::Float64, true), Arc::new(arr))
        }
        FieldKind::Bool => {
            let nulls = NullBuffer::new_null(num_rows);
            let arr = BooleanArray::new(vec![false; num_rows].into(), Some(nulls));
            (Field::new(name, DataType::Boolean, true), Arc::new(arr))
        }
        FieldKind::Utf8View => {
            let arr = StringViewArray::new_null(num_rows);
            (Field::new(name, DataType::Utf8View, true), Arc::new(arr))
        }
        FieldKind::BinaryView | FieldKind::FixedBinary(_) => {
            let arr = NullArray::new(num_rows);
            (Field::new(name, DataType::Null, true), Arc::new(arr))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Int64Type;

    use super::*;

    fn make_planned_builder() -> (ColumnarBatchBuilder, FieldHandle, FieldHandle, FieldHandle) {
        let mut plan = BatchPlan::new();
        let ts = plan.declare_planned("timestamp", FieldKind::Int64).unwrap();
        let sev = plan
            .declare_planned("severity", FieldKind::Utf8View)
            .unwrap();
        let val = plan.declare_planned("value", FieldKind::Float64).unwrap();
        let builder = ColumnarBatchBuilder::new(plan);
        (builder, ts, sev, val)
    }

    // -----------------------------------------------------------------------
    // Planned field tests
    // -----------------------------------------------------------------------

    #[test]
    fn planned_int64_single_row() {
        let (mut b, ts, _sev, _val) = make_planned_builder();
        b.begin_batch();
        b.begin_row();
        b.write_i64(ts, 1_000_000);
        b.end_row();
        let batch = b.finish_batch().unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column_by_name("timestamp").unwrap();
        let arr = col.as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 1_000_000);
        assert!(!arr.is_null(0));
    }

    #[test]
    fn planned_sparse_fields_produce_nulls() {
        let (mut b, ts, sev, val) = make_planned_builder();
        b.begin_batch();

        // Row 0: only timestamp
        b.begin_row();
        b.write_i64(ts, 100);
        b.end_row();

        // Row 1: only severity
        b.begin_row();
        b.write_str(sev, "INFO").unwrap();
        b.end_row();

        // Row 2: only value
        b.begin_row();
        b.write_f64(val, 3.14);
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 3);

        // timestamp: [100, null, null]
        let ts_col = batch.column_by_name("timestamp").unwrap();
        let ts_arr = ts_col.as_primitive::<Int64Type>();
        assert!(!ts_arr.is_null(0));
        assert!(ts_arr.is_null(1));
        assert!(ts_arr.is_null(2));
        assert_eq!(ts_arr.value(0), 100);

        // severity: [null, "INFO", null]
        let sev_col = batch.column_by_name("severity").unwrap();
        assert!(sev_col.is_null(0));
        assert!(!sev_col.is_null(1));
        assert!(sev_col.is_null(2));

        // value: [null, null, 3.14]
        let val_col = batch.column_by_name("value").unwrap();
        assert!(val_col.is_null(0));
        assert!(val_col.is_null(1));
        assert!(!val_col.is_null(2));
    }

    #[test]
    fn planned_bool_field() {
        let mut plan = BatchPlan::new();
        let flag = plan.declare_planned("active", FieldKind::Bool).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.write_bool(flag, true);
        b.end_row();
        b.begin_row();
        b.write_bool(flag, false);
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let col = batch.column_by_name("active").unwrap();
        let arr = col.as_boolean();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn empty_batch_produces_zero_rows() {
        let (mut b, _ts, _sev, _val) = make_planned_builder();
        b.begin_batch();
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn planned_dedup_first_write_wins() {
        let mut plan = BatchPlan::new();
        let h = plan.declare_planned("x", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.write_i64(h, 1);
        b.write_i64(h, 2); // should be ignored (dedup)
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("x").unwrap();
        let arr = col.as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 1);
    }

    #[test]
    fn batch_reuse_resets_state() {
        let (mut b, ts, _sev, _val) = make_planned_builder();

        // First batch
        b.begin_batch();
        b.begin_row();
        b.write_i64(ts, 100);
        b.end_row();
        let batch1 = b.finish_batch().unwrap();
        assert_eq!(batch1.num_rows(), 1);

        // Second batch (reuse builder)
        b.begin_batch();
        b.begin_row();
        b.write_i64(ts, 200);
        b.end_row();
        b.begin_row();
        b.write_i64(ts, 300);
        b.end_row();
        let batch2 = b.finish_batch().unwrap();
        assert_eq!(batch2.num_rows(), 2);

        let arr = batch2
            .column_by_name("timestamp")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 200);
        assert_eq!(arr.value(1), 300);
    }

    // -----------------------------------------------------------------------
    // Dynamic field tests
    // -----------------------------------------------------------------------

    #[test]
    fn dynamic_single_type_flat_column() {
        let plan = BatchPlan::new();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        let h = b.resolve_dynamic("level", FieldKind::Utf8View).unwrap();
        b.write_str(h, "INFO").unwrap();
        b.end_row();

        b.begin_row();
        let h2 = b.resolve_dynamic("level", FieldKind::Utf8View).unwrap();
        assert_eq!(h, h2);
        b.write_str(h2, "ERROR").unwrap();
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let col = batch.column_by_name("level").unwrap();
        assert!(!col.is_null(0));
        assert!(!col.is_null(1));
    }

    #[test]
    fn dynamic_conflict_produces_struct() {
        let plan = BatchPlan::new();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();

        // Row 0: "status" as int
        b.begin_row();
        let h = b.resolve_dynamic("status", FieldKind::Int64).unwrap();
        b.write_i64(h, 200);
        b.end_row();

        // Row 1: "status" as string
        b.begin_row();
        let h2 = b.resolve_dynamic("status", FieldKind::Utf8View).unwrap();
        assert_eq!(h, h2);
        b.write_str(h2, "OK").unwrap();
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let col = batch.column_by_name("status").unwrap();
        // Should be a StructArray with "int" and "str" children.
        let struct_arr = col.as_struct();
        assert_eq!(struct_arr.num_columns(), 2);

        let int_child = struct_arr.column_by_name("int").unwrap();
        let str_child = struct_arr.column_by_name("str").unwrap();

        // Row 0: int=200, str=null
        assert!(!int_child.is_null(0));
        assert!(str_child.is_null(0));
        assert_eq!(int_child.as_primitive::<Int64Type>().value(0), 200);

        // Row 1: int=null, str="OK"
        assert!(int_child.is_null(1));
        assert!(!str_child.is_null(1));
    }

    #[test]
    fn mixed_planned_and_dynamic_fields() {
        let mut plan = BatchPlan::new();
        let ts = plan.declare_planned("timestamp", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();

        b.begin_row();
        b.write_i64(ts, 1000);
        let msg = b.resolve_dynamic("message", FieldKind::Utf8View).unwrap();
        b.write_str(msg, "hello").unwrap();
        b.end_row();

        b.begin_row();
        b.write_i64(ts, 2000);
        let msg2 = b.resolve_dynamic("message", FieldKind::Utf8View).unwrap();
        b.write_str(msg2, "world").unwrap();
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let ts_arr = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(ts_arr.value(0), 1000);
        assert_eq!(ts_arr.value(1), 2000);
    }

    #[test]
    fn write_null_marks_dedup_only() {
        let mut plan = BatchPlan::new();
        let h = plan.declare_planned("x", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.write_null(h);
        b.write_i64(h, 42); // should be ignored (dedup — null was first)
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("x").unwrap();
        let arr = col.as_primitive::<Int64Type>();
        // The field was written as null first, so i64 write was suppressed.
        assert!(arr.is_null(0));
    }

    #[test]
    fn many_rows_sparse_fields() {
        let mut plan = BatchPlan::new();
        let a = plan.declare_planned("a", FieldKind::Int64).unwrap();
        let b_handle = plan.declare_planned("b", FieldKind::Int64).unwrap();
        let mut builder = ColumnarBatchBuilder::new(plan);

        builder.begin_batch();
        for i in 0..100u32 {
            builder.begin_row();
            if i % 2 == 0 {
                builder.write_i64(a, i as i64);
            }
            if i % 3 == 0 {
                builder.write_i64(b_handle, (i * 10) as i64);
            }
            builder.end_row();
        }

        let batch = builder.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 100);

        let a_arr = batch
            .column_by_name("a")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(!a_arr.is_null(0));
        assert!(a_arr.is_null(1));
        assert!(!a_arr.is_null(2));
        assert_eq!(a_arr.value(0), 0);
        assert_eq!(a_arr.value(2), 2);

        let b_arr = batch
            .column_by_name("b")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(!b_arr.is_null(0));
        assert!(b_arr.is_null(1));
        assert!(b_arr.is_null(2));
        assert!(!b_arr.is_null(3));
        assert_eq!(b_arr.value(0), 0);
        assert_eq!(b_arr.value(3), 30);
    }

    #[test]
    fn column_order_matches_declaration_order() {
        let mut plan = BatchPlan::new();
        plan.declare_planned("z_last", FieldKind::Int64).unwrap();
        plan.declare_planned("a_first", FieldKind::Int64).unwrap();
        plan.declare_planned("m_middle", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.end_row();
        let batch = b.finish_batch().unwrap();

        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["z_last", "a_first", "m_middle"]);
    }

    // -----------------------------------------------------------------------
    // Planned-field type safety: wrong-type writes are benign
    // -----------------------------------------------------------------------

    #[test]
    fn planned_wrong_type_write_stored_but_planned_kind_selects_output() {
        // If a producer writes a string to a planned Int64 field, the string
        // is stored but the planned kind selects int_values at materialization.
        // The result is an all-null column for that field.
        let mut plan = BatchPlan::new();
        let h = plan.declare_planned("x", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.write_str(h, "not an int").unwrap();
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("x").unwrap();
        let arr = col.as_primitive::<Int64Type>();
        // Planned kind is Int64, but we only wrote a string → int column is all-null.
        assert!(arr.is_null(0));
    }

    // -----------------------------------------------------------------------
    // View mode finalization
    // -----------------------------------------------------------------------

    #[test]
    fn finish_batch_view_produces_string_view_array() {
        let mut plan = BatchPlan::new();
        let ts = plan.declare_planned("ts", FieldKind::Int64).unwrap();
        let msg = plan.declare_planned("msg", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        let input = b"hello world";
        let arrow_buf = Buffer::from(&input[..]);

        b.begin_batch();
        b.begin_row();
        b.write_i64(ts, 1000);
        // Use str_ref pointing into the original buffer
        b.write_str_ref(
            msg,
            super::super::accumulator::StringRef { offset: 0, len: 5 },
        );
        b.end_row();
        b.begin_row();
        b.write_i64(ts, 2000);
        b.write_str_ref(
            msg,
            super::super::accumulator::StringRef { offset: 6, len: 5 },
        );
        b.end_row();

        let batch = b.finish_batch_view(arrow_buf, input.len() as u32).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let msg_col = batch.column_by_name("msg").unwrap();
        let arr = msg_col.as_string_view();
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "world");
    }

    // -----------------------------------------------------------------------
    // Error handling
    // -----------------------------------------------------------------------

    #[test]
    fn finish_batch_propagates_materialize_error() {
        let mut plan = BatchPlan::new();
        let msg = plan.declare_planned("msg", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        // Write a str_ref pointing beyond any valid buffer
        b.write_str_ref(
            msg,
            super::super::accumulator::StringRef {
                offset: 9999,
                len: 10,
            },
        );
        b.end_row();

        let result = b.finish_batch();
        assert!(
            result.is_err(),
            "finish_batch should fail for out-of-bounds StringRef"
        );
    }

    #[test]
    fn zero_copy_original_buffer_round_trip() {
        let input = b"helloworld";
        let mut plan = BatchPlan::new();
        let a = plan.declare_planned("a", FieldKind::Utf8View).unwrap();
        let b_h = plan.declare_planned("b", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.set_original_buffer(input);
        b.begin_row();
        b.write_str_ref(a, StringRef { offset: 0, len: 5 });
        b.write_str_ref(b_h, StringRef { offset: 5, len: 5 });
        b.end_row();
        b.begin_row();
        b.write_str_ref(a, StringRef { offset: 5, len: 5 });
        b.write_str_ref(b_h, StringRef { offset: 0, len: 5 });
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let col_a = batch.column_by_name("a").unwrap().as_string_view();
        let col_b = batch.column_by_name("b").unwrap().as_string_view();
        assert_eq!(col_a.value(0), "hello");
        assert_eq!(col_a.value(1), "world");
        assert_eq!(col_b.value(0), "world");
        assert_eq!(col_b.value(1), "hello");
    }

    #[test]
    fn mixed_zero_copy_and_generated_strings() {
        let input = b"from-input";
        let mut plan = BatchPlan::new();
        let ref_field = plan
            .declare_planned("ref_field", FieldKind::Utf8View)
            .unwrap();
        let gen_field = plan
            .declare_planned("gen_field", FieldKind::Utf8View)
            .unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.set_original_buffer(input);
        b.begin_row();
        b.write_str_ref(ref_field, StringRef { offset: 0, len: 10 });
        b.write_str(gen_field, "generated-value").unwrap();
        b.end_row();
        b.begin_row();
        b.write_str(gen_field, "another-gen").unwrap();
        b.write_str_ref(ref_field, StringRef { offset: 5, len: 5 });
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let r = batch.column_by_name("ref_field").unwrap().as_string_view();
        let g = batch.column_by_name("gen_field").unwrap().as_string_view();
        assert_eq!(r.value(0), "from-input");
        assert_eq!(r.value(1), "input");
        assert_eq!(g.value(0), "generated-value");
        assert_eq!(g.value(1), "another-gen");
    }

    // -----------------------------------------------------------------------
    // Performance validation: planned fields at scale
    // -----------------------------------------------------------------------

    /// OTLP-like workload: 15 planned fields, 1000 rows per batch, 10 batches.
    /// Validates correctness at scale and prints timing for manual inspection.
    #[test]
    fn planned_fields_at_scale() {
        let mut plan = BatchPlan::new();
        let ts = plan
            .declare_planned("timestamp_ns", FieldKind::Int64)
            .unwrap();
        let sev_num = plan
            .declare_planned("severity_number", FieldKind::Int64)
            .unwrap();
        let sev_text = plan
            .declare_planned("severity_text", FieldKind::Utf8View)
            .unwrap();
        let body = plan.declare_planned("body", FieldKind::Utf8View).unwrap();
        let flags = plan.declare_planned("flags", FieldKind::Int64).unwrap();
        let trace_id = plan
            .declare_planned("trace_id", FieldKind::Utf8View)
            .unwrap();
        let span_id = plan
            .declare_planned("span_id", FieldKind::Utf8View)
            .unwrap();
        let res_svc = plan
            .declare_planned("resource.service.name", FieldKind::Utf8View)
            .unwrap();
        let res_host = plan
            .declare_planned("resource.host.name", FieldKind::Utf8View)
            .unwrap();
        let scope_name = plan
            .declare_planned("scope.name", FieldKind::Utf8View)
            .unwrap();
        let scope_ver = plan
            .declare_planned("scope.version", FieldKind::Utf8View)
            .unwrap();
        let attr_method = plan
            .declare_planned("attributes.http.method", FieldKind::Utf8View)
            .unwrap();
        let attr_status = plan
            .declare_planned("attributes.http.status_code", FieldKind::Int64)
            .unwrap();
        let attr_dur = plan
            .declare_planned("attributes.duration_ms", FieldKind::Float64)
            .unwrap();
        let attr_path = plan
            .declare_planned("attributes.http.path", FieldKind::Utf8View)
            .unwrap();

        let handles_int = [ts, sev_num, flags, attr_status];
        let handles_str = [
            sev_text,
            body,
            trace_id,
            span_id,
            res_svc,
            res_host,
            scope_name,
            scope_ver,
            attr_method,
            attr_path,
        ];
        let handles_float = [attr_dur];

        let mut b = ColumnarBatchBuilder::new(plan);
        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 10;

        let start = std::time::Instant::now();

        for batch_idx in 0..num_batches {
            b.begin_batch();
            for row in 0..rows_per_batch {
                b.begin_row();
                let base = (batch_idx as i64) * (rows_per_batch as i64) + row as i64;
                for (i, &h) in handles_int.iter().enumerate() {
                    b.write_i64(h, base + i as i64 * 1000);
                }
                for &h in &handles_str {
                    b.write_str(h, "example-value-0123456789").unwrap();
                }
                for &h in &handles_float {
                    b.write_f64(h, base as f64 * 0.001);
                }
                b.end_row();
            }
            let batch = b.finish_batch().unwrap();
            assert_eq!(batch.num_rows(), rows_per_batch as usize);
            assert_eq!(batch.num_columns(), 15);
        }

        let elapsed = start.elapsed();
        let total_rows = u64::from(rows_per_batch) * num_batches;
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();

        eprintln!(
            "\n  planned_fields_at_scale: {} rows in {:.1?} ({:.0} rows/sec, {:.0} ns/row)\n",
            total_rows,
            elapsed,
            rows_per_sec,
            elapsed.as_nanos() as f64 / total_rows as f64,
        );
    }

    /// Mixed planned + dynamic workload: OTLP canonical + arbitrary JSON attrs.
    #[test]
    fn mixed_planned_dynamic_at_scale() {
        let mut plan = BatchPlan::new();
        let ts = plan
            .declare_planned("timestamp_ns", FieldKind::Int64)
            .unwrap();
        let sev = plan
            .declare_planned("severity_text", FieldKind::Utf8View)
            .unwrap();
        let body = plan.declare_planned("body", FieldKind::Utf8View).unwrap();

        let mut b = ColumnarBatchBuilder::new(plan);
        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 10;

        let start = std::time::Instant::now();

        for batch_idx in 0..num_batches {
            b.begin_batch();
            for row in 0..rows_per_batch {
                b.begin_row();
                let base = (batch_idx as i64) * (rows_per_batch as i64) + row as i64;
                b.write_i64(ts, base);
                b.write_str(sev, "INFO").unwrap();
                b.write_str(body, "example log message for testing")
                    .unwrap();

                // Dynamic attributes (vary by row)
                let attr_key = match row % 5 {
                    0 => "http.method",
                    1 => "http.path",
                    2 => "user.id",
                    3 => "region",
                    _ => "custom.tag",
                };
                let h = b.resolve_dynamic(attr_key, FieldKind::Utf8View).unwrap();
                b.write_str(h, "dynamic-value-here").unwrap();

                if row % 3 == 0 {
                    let h2 = b
                        .resolve_dynamic("http.status_code", FieldKind::Int64)
                        .unwrap();
                    b.write_i64(h2, 200);
                }

                b.end_row();
            }
            let batch = b.finish_batch().unwrap();
            assert_eq!(batch.num_rows(), rows_per_batch as usize);
            // 3 planned + up to 6 dynamic fields
            assert!(batch.num_columns() >= 3);
        }

        let elapsed = start.elapsed();
        let total_rows = u64::from(rows_per_batch) * num_batches;
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();

        eprintln!(
            "\n  mixed_planned_dynamic_at_scale: {} rows in {:.1?} ({:.0} rows/sec, {:.0} ns/row)\n",
            total_rows,
            elapsed,
            rows_per_sec,
            elapsed.as_nanos() as f64 / total_rows as f64,
        );
    }

    /// Baseline: StreamingBuilder doing the same 15-field OTLP-like workload.
    /// Uses detached path for apples-to-apples comparison.
    #[test]
    fn streaming_builder_baseline_at_scale() {
        use crate::streaming_builder::StreamingBuilder;

        let field_names: Vec<&str> = vec![
            "timestamp_ns",
            "severity_number",
            "severity_text",
            "body",
            "flags",
            "trace_id",
            "span_id",
            "resource.service.name",
            "resource.host.name",
            "scope.name",
            "scope.version",
            "attributes.http.method",
            "attributes.http.status_code",
            "attributes.duration_ms",
            "attributes.http.path",
        ];

        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 10;

        let mut sb = StreamingBuilder::new(None);
        let start = std::time::Instant::now();

        for _batch_idx in 0..num_batches {
            // StreamingBuilder needs a Bytes buffer for begin_batch
            let dummy_buf = bytes::Bytes::from_static(b"");
            sb.begin_batch(dummy_buf);

            // Pre-resolve field indices
            let indices: Vec<usize> = field_names
                .iter()
                .map(|name| sb.resolve_field(name.as_bytes()))
                .collect();

            for row in 0..rows_per_batch {
                sb.begin_row();
                // int fields: timestamp_ns, severity_number, flags, status_code
                for &idx in &[indices[0], indices[1], indices[4], indices[12]] {
                    sb.append_i64_value_by_idx(idx, row as i64);
                }
                // str fields (10 of them) — use decoded path since strings
                // aren't subslices of the input buffer (fair comparison)
                for &idx in &[
                    indices[2],
                    indices[3],
                    indices[5],
                    indices[6],
                    indices[7],
                    indices[8],
                    indices[9],
                    indices[10],
                    indices[11],
                    indices[14],
                ] {
                    sb.append_decoded_str_by_idx(idx, b"example-value-0123456789");
                }
                // float field: duration_ms
                sb.append_f64_value_by_idx(indices[13], row as f64 * 0.001);
                sb.end_row();
            }
            let batch = sb.finish_batch_detached().unwrap();
            assert_eq!(batch.num_rows(), rows_per_batch as usize);
        }

        let elapsed = start.elapsed();
        let total_rows = u64::from(rows_per_batch) * num_batches;
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();

        eprintln!(
            "\n  streaming_builder_baseline: {} rows in {:.1?} ({:.0} rows/sec, {:.0} ns/row)\n",
            total_rows,
            elapsed,
            rows_per_sec,
            elapsed.as_nanos() as f64 / total_rows as f64,
        );
    }

    /// ColumnarBatchBuilder view finalization: same 15-field workload.
    #[test]
    fn planned_fields_view_at_scale() {
        let mut plan = BatchPlan::new();
        let ts = plan
            .declare_planned("timestamp_ns", FieldKind::Int64)
            .unwrap();
        let sev_num = plan
            .declare_planned("severity_number", FieldKind::Int64)
            .unwrap();
        let sev_text = plan
            .declare_planned("severity_text", FieldKind::Utf8View)
            .unwrap();
        let body = plan.declare_planned("body", FieldKind::Utf8View).unwrap();
        let flags = plan.declare_planned("flags", FieldKind::Int64).unwrap();
        let trace_id = plan
            .declare_planned("trace_id", FieldKind::Utf8View)
            .unwrap();
        let span_id = plan
            .declare_planned("span_id", FieldKind::Utf8View)
            .unwrap();
        let res_svc = plan
            .declare_planned("resource.service.name", FieldKind::Utf8View)
            .unwrap();
        let res_host = plan
            .declare_planned("resource.host.name", FieldKind::Utf8View)
            .unwrap();
        let scope_name = plan
            .declare_planned("scope.name", FieldKind::Utf8View)
            .unwrap();
        let scope_ver = plan
            .declare_planned("scope.version", FieldKind::Utf8View)
            .unwrap();
        let attr_method = plan
            .declare_planned("attributes.http.method", FieldKind::Utf8View)
            .unwrap();
        let attr_status = plan
            .declare_planned("attributes.http.status_code", FieldKind::Int64)
            .unwrap();
        let attr_dur = plan
            .declare_planned("attributes.duration_ms", FieldKind::Float64)
            .unwrap();
        let attr_path = plan
            .declare_planned("attributes.http.path", FieldKind::Utf8View)
            .unwrap();

        let handles_int = [ts, sev_num, flags, attr_status];
        let handles_str = [
            sev_text,
            body,
            trace_id,
            span_id,
            res_svc,
            res_host,
            scope_name,
            scope_ver,
            attr_method,
            attr_path,
        ];
        let handles_float = [attr_dur];

        let mut b = ColumnarBatchBuilder::new(plan);
        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 10;

        let start = std::time::Instant::now();

        for batch_idx in 0..num_batches {
            b.begin_batch();
            for row in 0..rows_per_batch {
                b.begin_row();
                let base = (batch_idx as i64) * (rows_per_batch as i64) + row as i64;
                for (i, &h) in handles_int.iter().enumerate() {
                    b.write_i64(h, base + i as i64 * 1000);
                }
                for &h in &handles_str {
                    b.write_str(h, "example-value-0123456789").unwrap();
                }
                for &h in &handles_float {
                    b.write_f64(h, base as f64 * 0.001);
                }
                b.end_row();
            }
            let batch = b
                .finish_batch_view(arrow::buffer::Buffer::from(b"" as &[u8]), 0)
                .unwrap();
            assert_eq!(batch.num_rows(), rows_per_batch as usize);
            assert_eq!(batch.num_columns(), 15);
        }

        let elapsed = start.elapsed();
        let total_rows = u64::from(rows_per_batch) * num_batches;
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();

        eprintln!(
            "\n  planned_fields_view: {} rows in {:.1?} ({:.0} rows/sec, {:.0} ns/row)\n",
            total_rows,
            elapsed,
            rows_per_sec,
            elapsed.as_nanos() as f64 / total_rows as f64,
        );
    }

    /// StreamingBuilder view baseline: same 15-field workload.
    #[test]
    fn streaming_builder_view_baseline_at_scale() {
        use crate::streaming_builder::StreamingBuilder;

        let field_names: Vec<&str> = vec![
            "timestamp_ns",
            "severity_number",
            "severity_text",
            "body",
            "flags",
            "trace_id",
            "span_id",
            "resource.service.name",
            "resource.host.name",
            "scope.name",
            "scope.version",
            "attributes.http.method",
            "attributes.http.status_code",
            "attributes.duration_ms",
            "attributes.http.path",
        ];

        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 10;

        let mut sb = StreamingBuilder::new(None);
        let start = std::time::Instant::now();

        for _batch_idx in 0..num_batches {
            let dummy_buf = bytes::Bytes::from_static(b"");
            sb.begin_batch(dummy_buf);

            let indices: Vec<usize> = field_names
                .iter()
                .map(|name| sb.resolve_field(name.as_bytes()))
                .collect();

            for row in 0..rows_per_batch {
                sb.begin_row();
                for &idx in &[indices[0], indices[1], indices[4], indices[12]] {
                    sb.append_i64_value_by_idx(idx, row as i64);
                }
                for &idx in &[
                    indices[2],
                    indices[3],
                    indices[5],
                    indices[6],
                    indices[7],
                    indices[8],
                    indices[9],
                    indices[10],
                    indices[11],
                    indices[14],
                ] {
                    sb.append_decoded_str_by_idx(idx, b"example-value-0123456789");
                }
                sb.append_f64_value_by_idx(indices[13], row as f64 * 0.001);
                sb.end_row();
            }
            let batch = sb.finish_batch().unwrap();
            assert_eq!(batch.num_rows(), rows_per_batch as usize);
        }

        let elapsed = start.elapsed();
        let total_rows = u64::from(rows_per_batch) * num_batches;
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();

        eprintln!(
            "\n  streaming_builder_view_baseline: {} rows in {:.1?} ({:.0} rows/sec, {:.0} ns/row)\n",
            total_rows,
            elapsed,
            rows_per_sec,
            elapsed.as_nanos() as f64 / total_rows as f64,
        );
    }

    /// Report type sizes for performance-critical structures.
    #[test]
    fn type_sizes() {
        use super::super::accumulator::ColumnAccumulator;

        eprintln!("\n  Type sizes:");
        eprintln!(
            "    ColumnAccumulator: {} bytes",
            size_of::<ColumnAccumulator>()
        );
        eprintln!(
            "    ColumnarBatchBuilder: {} bytes",
            size_of::<ColumnarBatchBuilder>()
        );
        eprintln!("    FieldHandle: {} bytes", size_of::<FieldHandle>());
        eprintln!("    FieldKind: {} bytes", size_of::<FieldKind>());
        eprintln!("    StringRef: {} bytes", size_of::<StringRef>());
        eprintln!("    BatchPlan: {} bytes\n", size_of::<BatchPlan>());
    }

    /// CPU breakdown: write phase vs materialization phase.
    #[test]
    fn cpu_breakdown_write_vs_materialize() {
        let mut plan = BatchPlan::new();
        let ts = plan
            .declare_planned("timestamp_ns", FieldKind::Int64)
            .unwrap();
        let sev_num = plan
            .declare_planned("severity_number", FieldKind::Int64)
            .unwrap();
        let sev_text = plan
            .declare_planned("severity_text", FieldKind::Utf8View)
            .unwrap();
        let body = plan.declare_planned("body", FieldKind::Utf8View).unwrap();
        let flags = plan.declare_planned("flags", FieldKind::Int64).unwrap();
        let trace_id = plan
            .declare_planned("trace_id", FieldKind::Utf8View)
            .unwrap();
        let span_id = plan
            .declare_planned("span_id", FieldKind::Utf8View)
            .unwrap();
        let res_svc = plan
            .declare_planned("resource.service.name", FieldKind::Utf8View)
            .unwrap();
        let res_host = plan
            .declare_planned("resource.host.name", FieldKind::Utf8View)
            .unwrap();
        let scope_name = plan
            .declare_planned("scope.name", FieldKind::Utf8View)
            .unwrap();
        let scope_ver = plan
            .declare_planned("scope.version", FieldKind::Utf8View)
            .unwrap();
        let attr_method = plan
            .declare_planned("attributes.http.method", FieldKind::Utf8View)
            .unwrap();
        let attr_status = plan
            .declare_planned("attributes.http.status_code", FieldKind::Int64)
            .unwrap();
        let attr_dur = plan
            .declare_planned("attributes.duration_ms", FieldKind::Float64)
            .unwrap();
        let attr_path = plan
            .declare_planned("attributes.http.path", FieldKind::Utf8View)
            .unwrap();

        let handles_int = [ts, sev_num, flags, attr_status];
        let handles_str = [
            sev_text,
            body,
            trace_id,
            span_id,
            res_svc,
            res_host,
            scope_name,
            scope_ver,
            attr_method,
            attr_path,
        ];
        let handles_float = [attr_dur];

        let mut b = ColumnarBatchBuilder::new(plan);
        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 20;

        // Warmup
        b.begin_batch();
        for row in 0..rows_per_batch {
            b.begin_row();
            for (i, &h) in handles_int.iter().enumerate() {
                b.write_i64(h, row as i64 + i as i64);
            }
            for &h in &handles_str {
                b.write_str(h, "warmup-string-value").unwrap();
            }
            for &h in &handles_float {
                b.write_f64(h, row as f64);
            }
            b.end_row();
        }
        let _ = b.finish_batch().unwrap();

        let mut total_write = std::time::Duration::ZERO;
        let mut total_materialize = std::time::Duration::ZERO;

        for batch_idx in 0..num_batches {
            b.begin_batch();

            let write_start = std::time::Instant::now();
            for row in 0..rows_per_batch {
                b.begin_row();
                let base = (batch_idx as i64) * (rows_per_batch as i64) + row as i64;
                for (i, &h) in handles_int.iter().enumerate() {
                    b.write_i64(h, base + i as i64 * 1000);
                }
                for &h in &handles_str {
                    b.write_str(h, "example-value-0123456789").unwrap();
                }
                for &h in &handles_float {
                    b.write_f64(h, base as f64 * 0.001);
                }
                b.end_row();
            }
            total_write += write_start.elapsed();

            let mat_start = std::time::Instant::now();
            let batch = b.finish_batch().unwrap();
            total_materialize += mat_start.elapsed();

            assert_eq!(batch.num_rows(), rows_per_batch as usize);
        }

        let total_rows = u64::from(rows_per_batch) * num_batches;
        let total = total_write + total_materialize;
        let write_pct = total_write.as_nanos() as f64 / total.as_nanos() as f64 * 100.0;
        let mat_pct = total_materialize.as_nanos() as f64 / total.as_nanos() as f64 * 100.0;

        eprintln!("\n  CPU breakdown ({total_rows} rows, {num_batches} batches):");
        eprintln!(
            "    write phase:        {:.1?}  ({:.0} ns/row, {:.0}%)",
            total_write,
            total_write.as_nanos() as f64 / total_rows as f64,
            write_pct,
        );
        eprintln!(
            "    materialize phase:  {:.1?}  ({:.0} ns/row, {:.0}%)",
            total_materialize,
            total_materialize.as_nanos() as f64 / total_rows as f64,
            mat_pct,
        );
        eprintln!(
            "    total:              {:.1?}  ({:.0} ns/row)\n",
            total,
            total.as_nanos() as f64 / total_rows as f64,
        );
    }
}
