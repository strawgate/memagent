// builder.rs — ColumnarBatchBuilder: shared column construction engine.
//
// Drives BatchPlan + FieldHandle → typed writes → Arrow materialization.
// Uses ColumnAccumulator for per-field storage.
//
// Scope:
//   - Planned fields: stable handles and typed write calls
//   - Current storage: dynamic accumulators for planned and dynamic fields
//   - Future optimization: planned fields can move to single-type accumulators
//   - Detached string materialization via shared string buffer
//   - Sparse padding with deferred (row, value) facts
//   - StringViewArray zero-copy when given input buffer
//
// StreamingBuilder remains the scanner-facing ScanBuilder adapter.
// ColumnarBatchBuilder is for structured producers (OTLP, CSV, etc).

use std::sync::Arc;

use arrow::buffer::Buffer;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use ffwd_core::scanner::BuilderState;

use super::accumulator::{ColumnAccumulator, FinalizationMode, MaterializeError, StringRef};
use super::plan::{BatchPlan, FieldHandle, FieldKind, PlanError};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StringBufferOverflow {
    buffer_len: usize,
    value_len: usize,
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

impl From<StringBufferOverflow> for BuilderError {
    fn from(e: StringBufferOverflow) -> Self {
        BuilderError::StringBufferOverflow {
            buffer_len: e.buffer_len,
            value_len: e.value_len,
        }
    }
}

#[inline]
fn current_buffer_len(
    original_len: usize,
    generated_len: usize,
    value_len: usize,
) -> Result<usize, StringBufferOverflow> {
    original_len
        .checked_add(generated_len)
        .ok_or(StringBufferOverflow {
            buffer_len: usize::MAX,
            value_len,
        })
}

#[inline]
fn checked_generated_append_len(
    generated_len: usize,
    value_len: usize,
    buffer_len: usize,
) -> Result<(), StringBufferOverflow> {
    generated_len
        .checked_add(value_len)
        .ok_or(StringBufferOverflow {
            buffer_len,
            value_len,
        })?;
    Ok(())
}

#[inline]
fn string_ref_at_buffer_len(
    buffer_len: usize,
    value_len: usize,
) -> Result<StringRef, StringBufferOverflow> {
    let offset = u32::try_from(buffer_len).map_err(|_e| StringBufferOverflow {
        buffer_len,
        value_len,
    })?;
    let len = u32::try_from(value_len).map_err(|_e| StringBufferOverflow {
        buffer_len,
        value_len,
    })?;
    Ok(StringRef { offset, len })
}

#[inline]
fn checked_hex_encoded_len(
    input_len: usize,
    buffer_len: usize,
) -> Result<usize, StringBufferOverflow> {
    input_len.checked_mul(2).ok_or(StringBufferOverflow {
        buffer_len,
        value_len: usize::MAX,
    })
}

#[inline]
fn encode_hex_lower_into(out: &mut Vec<u8>, value: &[u8], encoded_len: usize) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let old_len = out.len();
    out.resize(old_len + encoded_len, 0);
    let encoded = &mut out[old_len..old_len + encoded_len];
    for (i, &byte) in value.iter().enumerate() {
        encoded[i * 2] = HEX[(byte >> 4) as usize];
        encoded[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
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
/// - Each field currently gets a dynamic `ColumnAccumulator`, regardless of
///   schema mode. Planned fields still use stable handles and typed write calls;
///   single-type planned accumulators are a future optimization.
/// - Dedup uses the same bitmask approach as `RowLifecycle::written_bits`.
/// - `finish_batch` iterates fields and calls `accumulator.materialize()`.
pub struct ColumnarBatchBuilder {
    plan: BatchPlan,
    lifecycle: RowLifecycle,
    columns: Vec<ColumnAccumulator>,
    /// Optional original input buffer for zero-copy string references.
    /// `write_str_ref` offsets below `original_buf.len()` point here.
    /// Stored as an Arrow `Buffer` (ref-counted) — O(1) to pass through
    /// to StringViewArray without copying.
    original_buf: Buffer,
    /// Generated buffer for string values written via `write_str`.
    /// StringRef offsets at or above `original_buf.len()` point here
    /// (shifted by original_buf.len()).
    string_buf: Vec<u8>,
    /// When false, skip per-field dedup checks (producers that guarantee
    /// at most one write per field per row can disable for throughput).
    dedup_enabled: bool,
    /// When true (default), string materialization produces zero-copy
    /// `StringViewArray` using `new_unchecked`. When false, produces
    /// validated `StringArray` with full UTF-8 checking.
    utf8_trusted: bool,
    /// Capacity hint for `string_buf` based on the previous batch's usage.
    /// `finish_batch` takes the Vec (Arrow owns the allocation), so the
    /// next batch starts at capacity 0. This hint avoids the 0→1→2→…→N
    /// doubling chain on every batch.
    string_buf_hint: usize,
}

impl ColumnarBatchBuilder {
    /// Create a builder from a plan.
    ///
    /// All fields (planned and dynamic) use Dynamic accumulators, which
    /// accept all types uniformly. Single-type fields are still materialized
    /// as flat columns; multi-type fields become conflict structs.
    pub fn new(plan: BatchPlan) -> Self {
        let columns: Vec<ColumnAccumulator> = plan
            .fields()
            .map(|(_handle, _name, _mode)| ColumnAccumulator::dynamic())
            .collect();
        ColumnarBatchBuilder {
            plan,
            lifecycle: RowLifecycle::new(),
            columns,
            original_buf: Buffer::from(Vec::<u8>::new()),
            string_buf: Vec::new(),
            dedup_enabled: true,
            utf8_trusted: true,
            string_buf_hint: 0,
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

    /// Control UTF-8 trust level for string materialization.
    ///
    /// When `true` (the default), string columns are materialized as zero-copy
    /// `StringViewArray` using `new_unchecked`. This is safe when all string
    /// data comes from trusted boundaries:
    /// - `write_str(&str)` — Rust's type system guarantees UTF-8.
    /// - `write_str_ref` — caller guarantees the referenced buffer is valid
    ///   UTF-8 (e.g., scanner-validated input, OTLP decoder output).
    ///
    /// When `false`, strings are copied into a validated `StringArray` with
    /// full UTF-8 checking. Use this for untrusted producers where the
    /// referenced buffer may contain arbitrary bytes.
    pub fn set_utf8_trusted(&mut self, trusted: bool) {
        self.utf8_trusted = trusted;
    }

    /// Start a new batch.
    ///
    /// Clears all column data, resets dynamic fields (they must be
    /// re-resolved from scratch each batch), and resets string buffers.
    pub fn begin_batch(&mut self) {
        self.lifecycle.begin_batch();
        // Drop dynamic columns — they are re-resolved from scratch each batch.
        let num_planned = self.plan.num_planned();
        self.columns.truncate(num_planned);
        // Clear planned columns in place (capacity retained).
        for col in &mut self.columns {
            col.clear();
        }
        // Reset dynamic field names so the schema doesn't grow unboundedly.
        self.plan.reset_dynamic();
        self.original_buf = Buffer::from(Vec::<u8>::new());
        self.string_buf.clear();
        // Pre-reserve based on previous batch's string usage to avoid doubling.
        if self.string_buf.capacity() == 0 && self.string_buf_hint > 0 {
            self.string_buf.reserve(self.string_buf_hint);
        }
    }

    /// Set the original input buffer for zero-copy string references.
    ///
    /// Call after `begin_batch` and before any writes. `write_str_ref` with
    /// offsets below `original_buf.len()` will reference this buffer at
    /// materialization time. The buffer is ref-counted (Arrow `Buffer`) —
    /// no copy on set, and O(1) pass-through to `StringViewArray`.
    pub fn set_original_buffer(&mut self, buf: Buffer) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InBatch);
        debug_assert!(
            self.original_buf.is_empty(),
            "set_original_buffer called twice in the same batch"
        );
        self.original_buf = buf;
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
    /// Returns a stable handle that can be used for writes in the current batch.
    ///
    /// Dynamic fields are reset on `begin_batch` — handles from a previous
    /// batch are invalidated. This matches `StreamingBuilder`'s per-batch
    /// field resolution and prevents unbounded schema growth.
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
    /// For fields 0–63, uses the O(1) bitmask in `RowLifecycle`.
    /// For fields ≥64, falls back to per-column `last_row` tracking.
    /// Both paths are correct; the bitmask path is faster for common cases.
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
    ///
    /// If the column's planned kind does not accept i64, the write is
    /// silently ignored and the dedup slot is NOT consumed — a subsequent
    /// correct-type write to the same field will still succeed.
    #[inline]
    pub fn write_i64(&mut self, handle: FieldHandle, value: i64) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if !self.columns[handle.index()].accepts_i64() {
            return;
        }
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
        if !self.columns[handle.index()].accepts_f64() {
            return;
        }
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
        if !self.columns[handle.index()].accepts_bool() {
            return;
        }
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
        if !self.columns[handle.index()].accepts_str() {
            return Ok(());
        }
        if self.dedup_enabled && self.is_duplicate(handle) {
            return Ok(());
        }
        let buffer_len =
            current_buffer_len(self.original_buf.len(), self.string_buf.len(), value.len())
                .map_err(BuilderError::from)?;
        checked_generated_append_len(self.string_buf.len(), value.len(), buffer_len)
            .map_err(BuilderError::from)?;
        let sref = string_ref_at_buffer_len(buffer_len, value.len()).map_err(BuilderError::from)?;
        self.string_buf.extend_from_slice(value.as_bytes());
        self.columns[handle.index()].push_str(self.lifecycle.row_count(), sref);
        Ok(())
    }

    /// Write a `StringRef` directly (for zero-copy producers that already
    /// have offsets into an input buffer).
    #[inline]
    pub fn write_str_ref(&mut self, handle: FieldHandle, sref: StringRef) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if !self.columns[handle.index()].accepts_str() {
            return;
        }
        if self.dedup_enabled && self.is_duplicate(handle) {
            return;
        }
        self.columns[handle.index()].push_str(self.lifecycle.row_count(), sref);
    }

    /// Write pre-validated UTF-8 bytes to the generated string buffer.
    ///
    /// Like [`write_str`](Self::write_str), but accepts `&[u8]` that the
    /// caller guarantees is valid UTF-8. This avoids a redundant UTF-8
    /// validation round-trip for producers (like protobuf decoders) that
    /// already know their bytes are valid.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the string buffer would exceed u32 addressable range.
    #[inline]
    pub fn write_str_bytes(
        &mut self,
        handle: FieldHandle,
        value: &[u8],
    ) -> Result<(), BuilderError> {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if !self.columns[handle.index()].accepts_str() {
            return Ok(());
        }
        if self.dedup_enabled && self.is_duplicate(handle) {
            return Ok(());
        }
        let buffer_len =
            current_buffer_len(self.original_buf.len(), self.string_buf.len(), value.len())
                .map_err(BuilderError::from)?;
        checked_generated_append_len(self.string_buf.len(), value.len(), buffer_len)
            .map_err(BuilderError::from)?;
        let sref = string_ref_at_buffer_len(buffer_len, value.len()).map_err(BuilderError::from)?;
        self.string_buf.extend_from_slice(value);
        self.columns[handle.index()].push_str(self.lifecycle.row_count(), sref);
        Ok(())
    }

    /// Write lowercase hexadecimal text for `value` into the generated string buffer.
    ///
    /// This avoids a temporary scratch `Vec<u8>` for producers that need hex
    /// strings but already have the raw bytes.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the encoded hex string or its resulting offset would
    /// exceed `u32::MAX`, or if extending the generated string buffer would
    /// overflow addressable `usize` space.
    #[inline]
    pub fn write_hex_bytes_lower(
        &mut self,
        handle: FieldHandle,
        value: &[u8],
    ) -> Result<(), BuilderError> {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if !self.columns[handle.index()].accepts_str() {
            return Ok(());
        }
        if self.dedup_enabled && self.is_duplicate(handle) {
            return Ok(());
        }

        let buffer_len =
            current_buffer_len(self.original_buf.len(), self.string_buf.len(), value.len())
                .map_err(BuilderError::from)?;
        let encoded_len =
            checked_hex_encoded_len(value.len(), buffer_len).map_err(BuilderError::from)?;
        checked_generated_append_len(self.string_buf.len(), encoded_len, buffer_len)
            .map_err(BuilderError::from)?;
        let sref = string_ref_at_buffer_len(buffer_len, encoded_len).map_err(BuilderError::from)?;
        encode_hex_lower_into(&mut self.string_buf, value, encoded_len);
        self.columns[handle.index()].push_str(self.lifecycle.row_count(), sref);
        Ok(())
    }

    /// Write a zero-copy reference to a subslice of the original input buffer.
    ///
    /// The `value` must be a subslice of the buffer previously set via
    /// [`set_original_buffer`](Self::set_original_buffer). The offset is
    /// computed via pointer arithmetic.
    ///
    /// Returns `BuilderError::StringBufferOverflow` if the value is not within
    /// the original buffer bounds or the offset exceeds `u32::MAX`.
    #[inline]
    pub fn write_input_ref(
        &mut self,
        handle: FieldHandle,
        value: &[u8],
    ) -> Result<(), BuilderError> {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        debug_assert!(handle.index() < self.columns.len());
        if !self.columns[handle.index()].accepts_str() {
            return Ok(());
        }
        if self.dedup_enabled && self.is_duplicate(handle) {
            return Ok(());
        }
        let base = self.original_buf.as_ptr() as usize;
        let ptr = value.as_ptr() as usize;
        let value_end = ptr
            .checked_add(value.len())
            .ok_or(BuilderError::StringBufferOverflow {
                buffer_len: self.original_buf.len(),
                value_len: value.len(),
            })?;
        let buf_end = base.checked_add(self.original_buf.len()).ok_or(
            BuilderError::StringBufferOverflow {
                buffer_len: self.original_buf.len(),
                value_len: value.len(),
            },
        )?;
        if ptr < base || value_end > buf_end {
            return Err(BuilderError::StringBufferOverflow {
                buffer_len: self.original_buf.len(),
                value_len: value.len(),
            });
        }
        let raw_offset = ptr - base;
        let offset =
            u32::try_from(raw_offset).map_err(|_e| BuilderError::StringBufferOverflow {
                buffer_len: self.original_buf.len(),
                value_len: value.len(),
            })?;
        let len = u32::try_from(value.len()).map_err(|_e| BuilderError::StringBufferOverflow {
            buffer_len: self.original_buf.len(),
            value_len: value.len(),
        })?;
        let sref = StringRef { offset, len };
        self.columns[handle.index()].push_str(self.lifecycle.row_count(), sref);
        Ok(())
    }

    /// Record that a field is explicitly null for the current row.
    ///
    /// Sparse fields default to null for rows with no writes, so this is only
    /// needed to consume the dedup slot (preventing a later non-null write from
    /// being recorded). When dedup is disabled, this is a no-op — the field
    /// remains whatever it was before (null if unwritten, or the prior value
    /// if already written in this row).
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

        // Transfer buffers to Arrow — O(1) for the ref-counted original,
        // O(1) move for the generated Vec.
        let original = std::mem::replace(&mut self.original_buf, Buffer::from(Vec::<u8>::new()));
        self.string_buf_hint = self.string_buf.len();
        let generated = Buffer::from_vec(std::mem::take(&mut self.string_buf));

        let mode = FinalizationMode {
            original_buf: original,
            generated_buf: generated,
            utf8_trusted: self.utf8_trusted,
        };

        let result = self.materialize_all(num_rows, mode)?;
        self.lifecycle.finish_batch();
        Ok(result)
    }

    /// Discard an in-progress batch, resetting the builder to idle state.
    ///
    /// Use when a decode error occurs mid-batch and the builder will be
    /// reused for a subsequent request. Accepts any state (including mid-row).
    pub fn discard_batch(&mut self) {
        self.lifecycle.discard();
        self.original_buf = Buffer::from(Vec::<u8>::new());
        self.string_buf.clear();
    }

    /// Core materialization — shared by all finalization paths.
    fn materialize_all(
        &self,
        num_rows: usize,
        mode: FinalizationMode,
    ) -> Result<RecordBatch, BuilderError> {
        let mut schema_fields = Vec::with_capacity(self.plan.len());
        let mut arrays = Vec::with_capacity(self.plan.len());

        for (handle, name, _field_mode) in self.plan.fields() {
            let col = &self.columns[handle.index()];
            match col.materialize(name, num_rows, mode.clone(), self.dedup_enabled)? {
                Some((field, array)) => {
                    schema_fields.push(field);
                    arrays.push(array);
                }
                None => {
                    // All unwritten fields are omitted from the schema,
                    // matching StreamingBuilder behavior.
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

    /// OTLP-like 15-field plan for scale tests (4 int, 10 str, 1 float).
    struct OtlpHandles {
        int_handles: [FieldHandle; 4],
        str_handles: [FieldHandle; 10],
        float_handles: [FieldHandle; 1],
    }

    fn make_otlp_plan() -> (ColumnarBatchBuilder, OtlpHandles) {
        let mut plan = BatchPlan::new();
        let int_handles: [FieldHandle; 4] = [
            plan.declare_planned("timestamp_ns", FieldKind::Int64)
                .unwrap(),
            plan.declare_planned("severity_number", FieldKind::Int64)
                .unwrap(),
            plan.declare_planned("flags", FieldKind::Int64).unwrap(),
            plan.declare_planned("attributes.http.status_code", FieldKind::Int64)
                .unwrap(),
        ];
        let str_handles: [FieldHandle; 10] = [
            plan.declare_planned("severity_text", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("body", FieldKind::Utf8View).unwrap(),
            plan.declare_planned("trace_id", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("span_id", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("resource.service.name", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("resource.host.name", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("scope.name", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("scope.version", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("attributes.http.method", FieldKind::Utf8View)
                .unwrap(),
            plan.declare_planned("attributes.http.path", FieldKind::Utf8View)
                .unwrap(),
        ];
        let float_handles: [FieldHandle; 1] = [plan
            .declare_planned("attributes.duration_ms", FieldKind::Float64)
            .unwrap()];

        (
            ColumnarBatchBuilder::new(plan),
            OtlpHandles {
                int_handles,
                str_handles,
                float_handles,
            },
        )
    }

    fn write_otlp_row(b: &mut ColumnarBatchBuilder, h: &OtlpHandles, base: i64) {
        b.begin_row();
        for (i, &handle) in h.int_handles.iter().enumerate() {
            b.write_i64(handle, base + i as i64 * 1000);
        }
        for &handle in &h.str_handles {
            b.write_str(handle, "example-value-0123456789").unwrap();
        }
        for &handle in &h.float_handles {
            b.write_f64(handle, base as f64 * 0.001);
        }
        b.end_row();
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
        // write_null marks the dedup bit but writes no facts, so the
        // Dynamic accumulator has no data and the column is omitted.
        assert!(
            batch.column_by_name("x").is_none(),
            "null-only column should be omitted"
        );
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
        let z = plan.declare_planned("z_last", FieldKind::Int64).unwrap();
        let a = plan.declare_planned("a_first", FieldKind::Int64).unwrap();
        let m = plan.declare_planned("m_middle", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.write_i64(z, 1);
        b.write_i64(a, 2);
        b.write_i64(m, 3);
        b.end_row();
        let batch = b.finish_batch().unwrap();

        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["z_last", "a_first", "m_middle"]);
    }

    // -----------------------------------------------------------------------
    // Dynamic accumulator: all types accepted, first-write-wins via dedup
    // -----------------------------------------------------------------------

    #[test]
    fn all_types_accepted_by_declared_field() {
        // With Dynamic accumulators, a declared Int64 field still accepts
        // string writes. The actual type is determined by what's written,
        // not the declared hint.
        let mut plan = BatchPlan::new();
        let h = plan.declare_planned("x", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.write_str(h, "hello").unwrap();
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("x").unwrap();
        // Dynamic accumulator stores the string; single-type → flat column.
        assert_eq!(col.data_type(), &arrow::datatypes::DataType::Utf8View);
    }

    #[test]
    fn first_write_wins_with_dedup() {
        // When dedup is enabled, the first write to a field in a row wins.
        // The second write (even if correct type) is suppressed.
        let mut plan = BatchPlan::new();
        let h = plan.declare_planned("x", FieldKind::Int64).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.begin_row();
        b.write_i64(h, 42);
        b.write_str(h, "ignored").unwrap(); // dedup suppresses
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("x").unwrap();
        let arr = col.as_primitive::<Int64Type>();
        assert_eq!(arr.value(0), 42);
    }

    // -----------------------------------------------------------------------
    // Zero-copy finalization via set_original_buffer
    // -----------------------------------------------------------------------

    #[test]
    fn original_buffer_produces_string_view_array() {
        let mut plan = BatchPlan::new();
        let ts = plan.declare_planned("ts", FieldKind::Int64).unwrap();
        let msg = plan.declare_planned("msg", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        let input = b"hello world";

        b.begin_batch();
        b.set_original_buffer(Buffer::from(input.as_slice()));
        b.begin_row();
        b.write_i64(ts, 1000);
        b.write_str_ref(msg, StringRef { offset: 0, len: 5 });
        b.end_row();
        b.begin_row();
        b.write_i64(ts, 2000);
        b.write_str_ref(msg, StringRef { offset: 6, len: 5 });
        b.end_row();

        let batch = b.finish_batch().unwrap();
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
            StringRef {
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

    /// Regression: write_str_bytes with invalid UTF-8 must fail when
    /// utf8_trusted is false, even when no original buffer is set.
    #[test]
    fn write_str_bytes_invalid_utf8_untrusted_fails() {
        let mut plan = BatchPlan::new();
        let msg = plan.declare_planned("msg", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        b.set_utf8_trusted(false);

        b.begin_batch();
        b.begin_row();
        // Accept error from either write_str_bytes or finish_batch — the
        // invariant is that invalid bytes cannot materialize successfully.
        let write_result = b.write_str_bytes(msg, &[0xFF, 0xFE]);

        if write_result.is_ok() {
            b.end_row();

            let result = b.finish_batch();
            assert!(
                result.is_err(),
                "invalid UTF-8 via write_str_bytes must be caught when utf8_trusted=false"
            );
        }
    }

    #[test]
    fn zero_copy_original_buffer_round_trip() {
        let input = b"helloworld";
        let mut plan = BatchPlan::new();
        let a = plan.declare_planned("a", FieldKind::Utf8View).unwrap();
        let b_h = plan.declare_planned("b", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);

        b.begin_batch();
        b.set_original_buffer(Buffer::from(input.as_slice()));
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
        b.set_original_buffer(Buffer::from(input.as_slice()));
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
        let (mut b, h) = make_otlp_plan();
        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 10;

        let start = std::time::Instant::now();

        for batch_idx in 0..num_batches {
            b.begin_batch();
            for row in 0..rows_per_batch {
                let base = (batch_idx as i64) * (rows_per_batch as i64) + row as i64;
                write_otlp_row(&mut b, &h, base);
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
        let (mut b, h) = make_otlp_plan();
        let rows_per_batch: u32 = 1000;
        let num_batches: u64 = 20;

        // Warmup
        b.begin_batch();
        for row in 0..rows_per_batch {
            write_otlp_row(&mut b, &h, row as i64);
        }
        let _ = b.finish_batch().unwrap();

        let mut total_write = std::time::Duration::ZERO;
        let mut total_materialize = std::time::Duration::ZERO;

        for batch_idx in 0..num_batches {
            b.begin_batch();

            let write_start = std::time::Instant::now();
            for row in 0..rows_per_batch {
                let base = (batch_idx as i64) * (rows_per_batch as i64) + row as i64;
                write_otlp_row(&mut b, &h, base);
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

    #[test]
    fn dynamic_fields_reset_between_batches() {
        let plan = BatchPlan::new();
        let mut b = ColumnarBatchBuilder::new(plan);

        // Batch 1: fields "x" and "y", both written.
        b.begin_batch();
        let x = b.resolve_dynamic("x", FieldKind::Int64).unwrap();
        let y = b.resolve_dynamic("y", FieldKind::Utf8View).unwrap();
        b.begin_row();
        b.write_i64(x, 1);
        b.write_str(y, "hello").unwrap();
        b.end_row();
        let batch1 = b.finish_batch().unwrap();
        assert_eq!(batch1.num_columns(), 2);
        let names1: Vec<String> = batch1
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(names1.contains(&"x".to_string()));
        assert!(names1.contains(&"y".to_string()));

        // Batch 2: field "z" only — "x" and "y" should be gone.
        b.begin_batch();
        assert!(b.plan().lookup("x").is_none(), "x should be reset");
        assert!(b.plan().lookup("y").is_none(), "y should be reset");
        let z = b.resolve_dynamic("z", FieldKind::Float64).unwrap();
        b.begin_row();
        b.write_f64(z, 3.14);
        b.end_row();
        let batch2 = b.finish_batch().unwrap();
        // Only field "z" — no stale columns.
        assert_eq!(batch2.num_columns(), 1);
        assert_eq!(batch2.schema().field(0).name(), "z");
    }

    #[test]
    fn write_str_bytes_produces_correct_string() {
        let mut plan = BatchPlan::new();
        let name = plan.declare_planned("name", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        b.begin_batch();
        b.begin_row();
        b.write_str_bytes(name, b"hello world").unwrap();
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column(0).as_string_view();
        assert_eq!(col.value(0), "hello world");
    }

    #[test]
    fn write_str_bytes_multiple_values() {
        let mut plan = BatchPlan::new();
        let msg = plan.declare_planned("msg", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        b.begin_batch();
        for i in 0..5 {
            b.begin_row();
            let val = format!("message-{i}");
            b.write_str_bytes(msg, val.as_bytes()).unwrap();
            b.end_row();
        }
        let batch = b.finish_batch().unwrap();
        let col = batch.column(0).as_string_view();
        for i in 0..5 {
            assert_eq!(col.value(i), format!("message-{i}"));
        }
    }

    #[test]
    fn write_input_ref_zero_copy() {
        let mut plan = BatchPlan::new();
        let body = plan.declare_planned("body", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        let input = b"hello world from input buffer";
        let buf = Buffer::from_vec(input.to_vec());
        b.begin_batch();
        b.set_original_buffer(buf.clone());
        b.begin_row();
        // Reference the subslice "world" at offset 6..11.
        b.write_input_ref(body, &buf[6..11]).unwrap();
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column(0).as_string_view();
        assert_eq!(col.value(0), "world");
    }

    #[test]
    fn write_input_ref_multiple_subslices() {
        let mut plan = BatchPlan::new();
        let f = plan.declare_planned("field", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        let input = b"aaa|bbb|ccc";
        let buf = Buffer::from_vec(input.to_vec());
        b.begin_batch();
        b.set_original_buffer(buf.clone());
        b.begin_row();
        b.write_input_ref(f, &buf[0..3]).unwrap(); // "aaa"
        b.end_row();
        b.begin_row();
        b.write_input_ref(f, &buf[4..7]).unwrap(); // "bbb"
        b.end_row();
        b.begin_row();
        b.write_input_ref(f, &buf[8..11]).unwrap(); // "ccc"
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column(0).as_string_view();
        assert_eq!(col.value(0), "aaa");
        assert_eq!(col.value(1), "bbb");
        assert_eq!(col.value(2), "ccc");
    }

    #[test]
    fn write_str_bytes_and_input_ref_coexist() {
        let mut plan = BatchPlan::new();
        let f = plan.declare_planned("field", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        let input = b"original-data";
        let buf = Buffer::from_vec(input.to_vec());
        b.begin_batch();
        b.set_original_buffer(buf.clone());
        // Row 0: input ref
        b.begin_row();
        b.write_input_ref(f, &buf[0..8]).unwrap(); // "original"
        b.end_row();
        // Row 1: generated string
        b.begin_row();
        b.write_str_bytes(f, b"generated").unwrap();
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column(0).as_string_view();
        assert_eq!(col.value(0), "original");
        assert_eq!(col.value(1), "generated");
    }

    #[test]
    fn write_hex_bytes_lower_produces_correct_string() {
        let mut plan = BatchPlan::new();
        let name = plan.declare_planned("name", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        b.begin_batch();
        b.begin_row();
        b.write_hex_bytes_lower(name, &[0xde, 0xad, 0xbe, 0xef])
            .unwrap();
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column(0).as_string_view();
        assert_eq!(col.value(0), "deadbeef");
    }

    #[test]
    fn write_hex_bytes_lower_respects_dedup() {
        let mut plan = BatchPlan::new();
        let name = plan.declare_planned("name", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        b.begin_batch();
        b.begin_row();
        b.write_hex_bytes_lower(name, &[0xde, 0xad]).unwrap();
        b.write_hex_bytes_lower(name, &[0xbe, 0xef]).unwrap();
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column(0).as_string_view();
        assert_eq!(col.value(0), "dead");
    }

    #[test]
    fn write_input_ref_returns_error_on_oob() {
        let mut plan = BatchPlan::new();
        let f = plan.declare_planned("field", FieldKind::Utf8View).unwrap();
        let mut b = ColumnarBatchBuilder::new(plan);
        let buf = Buffer::from_vec(b"short".to_vec());
        b.begin_batch();
        b.set_original_buffer(buf);
        b.begin_row();
        // Not a subslice of original buffer — should return error.
        let result = b.write_input_ref(f, b"external");
        assert!(result.is_err());
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    fn hex_oracle(bytes: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8; bytes.len() * 2];
        assert!(ffwd_kani::hex::hex_encode_oracle(bytes, &mut out));
        out
    }

    #[kani::proof]
    fn verify_current_buffer_len_matches_checked_add() {
        let original_len: usize = kani::any();
        let generated_len: usize = kani::any();
        let value_len: usize = kani::any();

        let result = current_buffer_len(original_len, generated_len, value_len);
        match original_len.checked_add(generated_len) {
            Some(sum) => {
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), sum);
            }
            None => {
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert_eq!(err.buffer_len, usize::MAX);
                assert_eq!(err.value_len, value_len);
            }
        }

        kani::cover!(original_len == 0, "zero original buffer");
        kani::cover!(generated_len == 0, "zero generated buffer");
        kani::cover!(
            original_len.checked_add(generated_len).is_none(),
            "combined buffer length overflow"
        );
    }

    #[kani::proof]
    fn verify_checked_generated_append_len_matches_checked_add() {
        let generated_len: usize = kani::any();
        let value_len: usize = kani::any();
        let buffer_len: usize = kani::any();

        let result = checked_generated_append_len(generated_len, value_len, buffer_len);
        match generated_len.checked_add(value_len) {
            Some(_sum) => assert!(result.is_ok()),
            None => {
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert_eq!(err.buffer_len, buffer_len);
                assert_eq!(err.value_len, value_len);
            }
        }

        kani::cover!(generated_len == 0, "empty generated buffer");
        kani::cover!(value_len == 0, "empty append");
        kani::cover!(
            generated_len.checked_add(value_len).is_none(),
            "generated append length overflow"
        );
    }

    #[kani::proof]
    fn verify_string_ref_at_buffer_len_matches_u32_conversions() {
        let buffer_len: usize = kani::any();
        let value_len: usize = kani::any();

        let result = string_ref_at_buffer_len(buffer_len, value_len);
        match (u32::try_from(buffer_len), u32::try_from(value_len)) {
            (Ok(offset), Ok(len)) => {
                assert!(result.is_ok());
                let sref = result.unwrap();
                assert_eq!(sref.offset, offset);
                assert_eq!(sref.len, len);
            }
            _ => {
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert_eq!(err.buffer_len, buffer_len);
                assert_eq!(err.value_len, value_len);
            }
        }

        kani::cover!(buffer_len <= u32::MAX as usize, "offset fits in u32");
        kani::cover!(value_len <= u32::MAX as usize, "length fits in u32");
        kani::cover!(buffer_len > u32::MAX as usize, "offset overflow");
        kani::cover!(value_len > u32::MAX as usize, "length overflow");
    }

    #[kani::proof]
    fn verify_checked_hex_encoded_len_matches_doubling() {
        let input_len: usize = kani::any();
        let buffer_len: usize = kani::any();

        let result = checked_hex_encoded_len(input_len, buffer_len);
        match input_len.checked_mul(2) {
            Some(encoded_len) => {
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), encoded_len);
            }
            None => {
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert_eq!(err.buffer_len, buffer_len);
                assert_eq!(err.value_len, usize::MAX);
            }
        }

        kani::cover!(input_len == 0, "empty hex input");
        kani::cover!(input_len == 1, "single-byte hex input");
        kani::cover!(input_len.checked_mul(2).is_none(), "hex length overflow");
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_encode_hex_lower_into_matches_oracle() {
        let prefix: [u8; 2] = kani::any();
        let bytes: [u8; 2] = kani::any();

        let mut out = prefix.to_vec();
        let old_len = out.len();
        let expected = hex_oracle(&bytes);
        encode_hex_lower_into(&mut out, &bytes, expected.len());

        assert_eq!(&out[..old_len], &prefix);
        assert_eq!(&out[old_len..], expected.as_slice());
        assert!(
            out[old_len..]
                .iter()
                .all(|&b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );

        kani::cover!(bytes[0] == 0x00, "hex oracle covers 00");
        kani::cover!(bytes[1] == 0xff, "hex oracle covers ff");
    }
}

// ---------------------------------------------------------------------------
// Proptest — end-to-end builder verification
// ---------------------------------------------------------------------------
#[cfg(test)]
mod proptests {
    use super::*;
    use crate::columnar::plan::{BatchPlan, FieldHandle, FieldKind};
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringViewArray};
    use proptest::prelude::*;

    proptest! {
        /// End-to-end builder: random planned fields, random writes, verify
        /// RecordBatch invariants (row count, null counts, value correctness).
        #[test]
        fn builder_e2e_planned_fields(
            num_fields in 1usize..=8,
            num_rows in 1usize..=16,
            seed in any::<u64>(),
        ) {
            // Deterministic kind selection from seed.
            let kinds: Vec<FieldKind> = (0..num_fields)
                .map(|i| {
                    match (seed.wrapping_mul(i as u64 + 1)) % 6 {
                        0 => FieldKind::Int64,
                        1 => FieldKind::Float64,
                        2 => FieldKind::Bool,
                        3 => FieldKind::Utf8View,
                        4 => FieldKind::BinaryView,
                        _ => FieldKind::FixedBinary(16),
                    }
                })
                .collect();

            let mut plan = BatchPlan::new();
            let handles: Vec<FieldHandle> = kinds
                .iter()
                .enumerate()
                .map(|(i, &k)| {
                    plan.declare_planned(&format!("f{i}"), k).unwrap()
                })
                .collect();

            let mut b = ColumnarBatchBuilder::new(plan);
            b.begin_batch();

            // Track expected values for oracle comparison.
            let mut expected_i64: Vec<Vec<Option<i64>>> =
                vec![vec![None; num_rows]; num_fields];
            let mut expected_f64: Vec<Vec<Option<f64>>> =
                vec![vec![None; num_rows]; num_fields];
            let mut expected_bool: Vec<Vec<Option<bool>>> =
                vec![vec![None; num_rows]; num_fields];
            let mut expected_str: Vec<Vec<Option<String>>> =
                vec![vec![None; num_rows]; num_fields];

            for row in 0..num_rows {
                b.begin_row();
                for (fi, (&h, &kind)) in handles.iter().zip(kinds.iter()).enumerate() {
                    // Deterministic skip pattern: some rows have gaps.
                    let write_it = (seed.wrapping_mul((row * num_fields + fi) as u64 + 7)) % 5 != 0;
                    if !write_it {
                        continue;
                    }
                    match kind {
                        FieldKind::Int64 => {
                            let val = (row * 100 + fi) as i64;
                            b.write_i64(h, val);
                            expected_i64[fi][row] = Some(val);
                        }
                        FieldKind::Float64 => {
                            let val = (row * 100 + fi) as f64 * 0.5;
                            b.write_f64(h, val);
                            expected_f64[fi][row] = Some(val);
                        }
                        FieldKind::Bool => {
                            let val = (row + fi) % 2 == 0;
                            b.write_bool(h, val);
                            expected_bool[fi][row] = Some(val);
                        }
                        FieldKind::Utf8View => {
                            let val = format!("r{row}f{fi}");
                            b.write_str(h, &val).unwrap();
                            expected_str[fi][row] = Some(val);
                        }
                        // No typed write methods for binary kinds yet;
                        // all values remain null, verified below.
                        FieldKind::BinaryView | FieldKind::FixedBinary(_) => {}
                    }
                }
                b.end_row();
            }

            let batch = b.finish_batch().unwrap();

            // Verify RecordBatch invariants.
            prop_assert_eq!(batch.num_rows(), num_rows);

            // Fields with no write methods (BinaryView, FixedBinary) are omitted
            // since Dynamic accumulators skip empty columns.
            let written_fields: usize = kinds.iter().filter(|k| {
                !matches!(k, FieldKind::BinaryView | FieldKind::FixedBinary(_))
            }).count();
            // Some written fields may be all-skipped (sparse pattern), so the
            // actual column count may be less. Just verify it doesn't exceed.
            prop_assert!(batch.num_columns() <= written_fields,
                "too many columns: {} > {}", batch.num_columns(), written_fields);

            // Verify each column matches the oracle (by name lookup).
            for (fi, &kind) in kinds.iter().enumerate() {
                let col_name = format!("f{fi}");
                let col = match batch.column_by_name(&col_name) {
                    Some(c) => c,
                    None => {
                        // Column was omitted — verify all expected values are None.
                        match kind {
                            FieldKind::Int64 => {
                                for row in 0..num_rows {
                                    prop_assert!(expected_i64[fi][row].is_none(),
                                        "field {} row {} expected data but column omitted", fi, row);
                                }
                            }
                            FieldKind::Float64 => {
                                for row in 0..num_rows {
                                    prop_assert!(expected_f64[fi][row].is_none(),
                                        "field {} row {} expected data but column omitted", fi, row);
                                }
                            }
                            FieldKind::Bool => {
                                for row in 0..num_rows {
                                    prop_assert!(expected_bool[fi][row].is_none(),
                                        "field {} row {} expected data but column omitted", fi, row);
                                }
                            }
                            FieldKind::Utf8View => {
                                for row in 0..num_rows {
                                    prop_assert!(expected_str[fi][row].is_none(),
                                        "field {} row {} expected data but column omitted", fi, row);
                                }
                            }
                            FieldKind::BinaryView | FieldKind::FixedBinary(_) => {}
                        }
                        continue;
                    }
                };
                prop_assert_eq!(col.len(), num_rows, "column {} length mismatch", fi);

                match kind {
                    FieldKind::Int64 => {
                        let typed = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        for row in 0..num_rows {
                            match expected_i64[fi][row] {
                                Some(v) => {
                                    prop_assert!(!typed.is_null(row),
                                        "field {} row {} should not be null", fi, row);
                                    prop_assert_eq!(typed.value(row), v);
                                }
                                None => {
                                    prop_assert!(typed.is_null(row),
                                        "field {} row {} should be null", fi, row);
                                }
                            }
                        }
                    }
                    FieldKind::Float64 => {
                        let typed = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        for row in 0..num_rows {
                            match expected_f64[fi][row] {
                                Some(v) => {
                                    prop_assert!(!typed.is_null(row));
                                    prop_assert_eq!(typed.value(row).to_bits(), v.to_bits());
                                }
                                None => {
                                    prop_assert!(typed.is_null(row));
                                }
                            }
                        }
                    }
                    FieldKind::Bool => {
                        let typed = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                        for row in 0..num_rows {
                            match expected_bool[fi][row] {
                                Some(v) => {
                                    prop_assert!(!typed.is_null(row));
                                    prop_assert_eq!(typed.value(row), v);
                                }
                                None => {
                                    prop_assert!(typed.is_null(row));
                                }
                            }
                        }
                    }
                    FieldKind::Utf8View => {
                        let typed = col.as_any().downcast_ref::<StringViewArray>().unwrap();
                        for row in 0..num_rows {
                            match &expected_str[fi][row] {
                                Some(v) => {
                                    prop_assert!(!typed.is_null(row));
                                    prop_assert_eq!(typed.value(row), v.as_str());
                                }
                                None => {
                                    prop_assert!(typed.is_null(row));
                                }
                            }
                        }
                    }
                    // Binary kinds have no write methods yet; columns are
                    // omitted (handled by the None branch above).
                    FieldKind::BinaryView | FieldKind::FixedBinary(_) => {}
                }
            }
        }

        /// Multi-batch builder: planned fields persist, values reset between batches.
        #[test]
        fn builder_e2e_multi_batch(
            num_rows_per_batch in 1usize..=8,
            num_batches in 2usize..=4,
        ) {
            let mut plan = BatchPlan::new();
            let h_int = plan.declare_planned("count", FieldKind::Int64).unwrap();
            let h_str = plan.declare_planned("label", FieldKind::Utf8View).unwrap();

            let mut b = ColumnarBatchBuilder::new(plan);

            for batch_idx in 0..num_batches {
                b.begin_batch();
                for row in 0..num_rows_per_batch {
                    b.begin_row();
                    let val = (batch_idx * 1000 + row) as i64;
                    b.write_i64(h_int, val);
                    let label = format!("b{batch_idx}r{row}");
                    b.write_str(h_str, &label).unwrap();
                    b.end_row();
                }
                let batch = b.finish_batch().unwrap();

                prop_assert_eq!(batch.num_rows(), num_rows_per_batch,
                    "batch {} row count", batch_idx);
                prop_assert_eq!(batch.num_columns(), 2,
                    "batch {} column count", batch_idx);

                let ints = batch.column(0).as_any()
                    .downcast_ref::<Int64Array>().unwrap();
                let strs = batch.column(1).as_any()
                    .downcast_ref::<StringViewArray>().unwrap();

                for row in 0..num_rows_per_batch {
                    let expected_val = (batch_idx * 1000 + row) as i64;
                    let expected_label = format!("b{batch_idx}r{row}");
                    prop_assert_eq!(ints.value(row), expected_val);
                    prop_assert_eq!(strs.value(row), expected_label.as_str());
                }
            }
        }

        #[test]
        fn write_hex_bytes_lower_matches_oracle(
            bytes in proptest::collection::vec(any::<u8>(), 0..128),
        ) {
            let mut plan = BatchPlan::new();
            let field = plan.declare_planned("hex", FieldKind::Utf8View).unwrap();
            let mut builder = ColumnarBatchBuilder::new(plan);

            builder.begin_batch();
            builder.begin_row();
            builder.write_hex_bytes_lower(field, &bytes).unwrap();
            builder.end_row();

            let batch = builder.finish_batch().unwrap();
            let col = batch.column(0).as_any().downcast_ref::<StringViewArray>().unwrap();
            let expected = bytes
                .iter()
                .flat_map(|byte| [b"0123456789abcdef"[(byte >> 4) as usize] as char, b"0123456789abcdef"[(byte & 0x0f) as usize] as char])
                .collect::<String>();
            prop_assert_eq!(col.value(0), expected);
        }
    }
}
