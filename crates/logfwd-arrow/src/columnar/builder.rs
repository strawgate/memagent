// builder.rs — ColumnarBatchBuilder prototype.
//
// Validates the BatchPlan + FieldHandle → typed writes → Arrow materialization
// path end-to-end.  This spike informs the design of #1844-#1847 before
// committing to a full extraction from StreamingBuilder.
//
// Current scope:
//   - Planned fields: fixed-kind, single-type columns, no conflict overhead
//   - Dynamic fields: multi-type, conflict detection at finalization
//   - Detached string materialization (copies into StringBuilder)
//   - Sparse padding with deferred (row, value) facts
//
// Out of scope (future #1844):
//   - Zero-copy StringViewArray via block store
//   - ScanBuilder trait adapter (StreamingBuilder keeps that role)
//   - Resource attributes, line capture (StreamingBuilder concerns)

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringBuilder, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use logfwd_core::scanner::BuilderState;

use super::plan::{BatchPlan, FieldHandle, FieldKind, FieldSchemaMode, PlanError};
use super::row_protocol::RowLifecycle;
use crate::check_dup_bits;

// ---------------------------------------------------------------------------
// Per-field column storage (deferred facts)
// ---------------------------------------------------------------------------

/// Deferred (row, value) facts for a single field.
///
/// Same approach as `StreamingBuilder::FieldColumns`: store facts during row
/// writes, materialize into Arrow arrays at `finish_batch` time.  Sparse rows
/// become null via the validity bitmap.
struct ColumnStorage {
    str_values: Vec<(u32, String)>,
    int_values: Vec<(u32, i64)>,
    float_values: Vec<(u32, f64)>,
    bool_values: Vec<(u32, bool)>,
    has_str: bool,
    has_int: bool,
    has_float: bool,
    has_bool: bool,
    /// Last row written, for dedup when field index >= 64.
    last_row: u32,
}

impl ColumnStorage {
    fn new() -> Self {
        ColumnStorage {
            str_values: Vec::new(),
            int_values: Vec::new(),
            float_values: Vec::new(),
            bool_values: Vec::new(),
            has_str: false,
            has_int: false,
            has_float: false,
            has_bool: false,
            last_row: u32::MAX,
        }
    }

    fn clear(&mut self) {
        self.str_values.clear();
        self.int_values.clear();
        self.float_values.clear();
        self.bool_values.clear();
        self.has_str = false;
        self.has_int = false;
        self.has_float = false;
        self.has_bool = false;
        self.last_row = u32::MAX;
    }
}

// ---------------------------------------------------------------------------
// ColumnarBatchBuilder
// ---------------------------------------------------------------------------

/// Prototype shared columnar batch builder driven by `BatchPlan` + `FieldHandle`.
///
/// Producers declare fields up front (planned) or discover them dynamically,
/// then write typed values via stable handles.  At `finish_batch`, deferred
/// facts are materialized into an Arrow `RecordBatch`.
///
/// # Design findings (spike)
///
/// - Planned fields bypass conflict detection entirely: the handle's kind
///   determines the output column type.
/// - Dynamic fields reuse the `has_*` flag + conflict struct pattern from
///   `StreamingBuilder`.
/// - The `BatchPlan` provides the schema metadata; storage is handle-indexed.
/// - Dedup uses the same bitmask approach as `RowLifecycle::written_bits`.
/// - String storage is detached (owned copies) in this prototype; the
///   block store (#1844) adds zero-copy view-backed paths later.
pub(crate) struct ColumnarBatchBuilder {
    plan: BatchPlan,
    lifecycle: RowLifecycle,
    columns: Vec<ColumnStorage>,
}

impl ColumnarBatchBuilder {
    /// Create a builder from a plan.
    ///
    /// Allocates per-field storage for all declared fields.
    pub(crate) fn new(plan: BatchPlan) -> Self {
        let num_fields = plan.len();
        let mut columns = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            columns.push(ColumnStorage::new());
        }
        ColumnarBatchBuilder {
            plan,
            lifecycle: RowLifecycle::new(),
            columns,
        }
    }

    /// Start a new batch.
    pub(crate) fn begin_batch(&mut self) {
        self.lifecycle.begin_batch();
        for col in &mut self.columns {
            col.clear();
        }
    }

    /// Start a new row.
    #[inline(always)]
    pub(crate) fn begin_row(&mut self) {
        self.lifecycle.begin_row();
    }

    /// Finish the current row.
    #[inline(always)]
    pub(crate) fn end_row(&mut self) {
        self.lifecycle.end_row();
    }

    /// Resolve a dynamic field by name and observed kind.
    ///
    /// This is the runtime field-discovery path for JSON/CRI-style producers.
    /// Returns a stable handle that can be used for writes in the current row.
    pub(crate) fn resolve_dynamic(
        &mut self,
        name: &str,
        kind: FieldKind,
    ) -> Result<FieldHandle, PlanError> {
        let handle = self.plan.resolve_dynamic(name, kind)?;
        // Ensure storage exists for newly resolved fields.
        while self.columns.len() <= handle.index() {
            self.columns.push(ColumnStorage::new());
        }
        Ok(handle)
    }

    // -----------------------------------------------------------------------
    // Typed write methods
    // -----------------------------------------------------------------------

    /// Write an i64 value for the given field in the current row.
    #[inline(always)]
    pub(crate) fn write_i64(&mut self, handle: FieldHandle, value: i64) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        let idx = handle.index();
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        if idx >= 64 && self.columns[idx].last_row == self.lifecycle.row_count() {
            return;
        }
        if idx >= 64 {
            self.columns[idx].last_row = self.lifecycle.row_count();
        }
        let col = &mut self.columns[idx];
        col.has_int = true;
        col.int_values.push((self.lifecycle.row_count(), value));
    }

    /// Write an f64 value for the given field in the current row.
    #[inline(always)]
    pub(crate) fn write_f64(&mut self, handle: FieldHandle, value: f64) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        let idx = handle.index();
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        if idx >= 64 && self.columns[idx].last_row == self.lifecycle.row_count() {
            return;
        }
        if idx >= 64 {
            self.columns[idx].last_row = self.lifecycle.row_count();
        }
        let col = &mut self.columns[idx];
        col.has_float = true;
        col.float_values.push((self.lifecycle.row_count(), value));
    }

    /// Write a bool value for the given field in the current row.
    #[inline(always)]
    pub(crate) fn write_bool(&mut self, handle: FieldHandle, value: bool) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        let idx = handle.index();
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        if idx >= 64 && self.columns[idx].last_row == self.lifecycle.row_count() {
            return;
        }
        if idx >= 64 {
            self.columns[idx].last_row = self.lifecycle.row_count();
        }
        let col = &mut self.columns[idx];
        col.has_bool = true;
        col.bool_values.push((self.lifecycle.row_count(), value));
    }

    /// Write a string value for the given field in the current row.
    ///
    /// This is the detached (copying) path.  The block store (#1844) will add
    /// a zero-copy view-backed alternative.
    #[inline(always)]
    pub(crate) fn write_str(&mut self, handle: FieldHandle, value: &str) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        let idx = handle.index();
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        if idx >= 64 && self.columns[idx].last_row == self.lifecycle.row_count() {
            return;
        }
        if idx >= 64 {
            self.columns[idx].last_row = self.lifecycle.row_count();
        }
        let col = &mut self.columns[idx];
        col.has_str = true;
        col.str_values
            .push((self.lifecycle.row_count(), value.to_string()));
    }

    /// Write a null for the given field in the current row.
    ///
    /// Sparse fields are null by default; this just marks the field as
    /// written for dedup purposes.
    #[inline(always)]
    pub(crate) fn write_null(&mut self, handle: FieldHandle) {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InRow);
        let idx = handle.index();
        let _ = check_dup_bits(self.lifecycle.written_bits_mut(), idx);
        if idx >= 64 {
            self.columns[idx].last_row = self.lifecycle.row_count();
        }
    }

    // -----------------------------------------------------------------------
    // Finalization
    // -----------------------------------------------------------------------

    /// Materialize all deferred facts into an Arrow `RecordBatch`.
    ///
    /// Planned fields produce single-type columns (no conflict check).
    /// Dynamic fields check for type conflicts and emit `StructArray` when
    /// multiple types were observed.
    pub(crate) fn finish_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        debug_assert_eq!(self.lifecycle.state(), BuilderState::InBatch);
        let num_rows = self.lifecycle.row_count() as usize;

        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.plan.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.plan.len());

        for (handle, name, mode) in self.plan.fields() {
            let col = &self.columns[handle.index()];

            match mode {
                FieldSchemaMode::Planned(kind) => {
                    Self::materialize_planned(
                        col,
                        name,
                        *kind,
                        num_rows,
                        &mut schema_fields,
                        &mut arrays,
                    );
                }
                FieldSchemaMode::Dynamic { .. } => {
                    Self::materialize_dynamic(
                        col,
                        name,
                        num_rows,
                        &mut schema_fields,
                        &mut arrays,
                    );
                }
            }
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let result = RecordBatch::try_new_with_options(schema, arrays, &opts);
        self.lifecycle.finish_batch();
        result
    }

    /// Number of completed rows in the current batch.
    pub(crate) fn row_count(&self) -> u32 {
        self.lifecycle.row_count()
    }

    /// Reference to the underlying plan.
    pub(crate) fn plan(&self) -> &BatchPlan {
        &self.plan
    }

    // -----------------------------------------------------------------------
    // Materialization helpers
    // -----------------------------------------------------------------------

    fn materialize_planned(
        col: &ColumnStorage,
        name: &str,
        kind: FieldKind,
        num_rows: usize,
        schema_fields: &mut Vec<Field>,
        arrays: &mut Vec<ArrayRef>,
    ) {
        match kind {
            FieldKind::Int64 => {
                let (array, dt) = Self::build_int64_column(&col.int_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            }
            FieldKind::Float64 => {
                let (array, dt) = Self::build_float64_column(&col.float_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            }
            FieldKind::Bool => {
                let (array, dt) = Self::build_bool_column(&col.bool_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            }
            FieldKind::Utf8View => {
                let (array, dt) = Self::build_string_column(&col.str_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            }
            FieldKind::BinaryView | FieldKind::FixedBinary(_) => {
                // Not yet implemented in prototype; emit null Utf8 column as placeholder.
                let (array, dt) = Self::build_string_column(&col.str_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            }
        }
    }

    fn materialize_dynamic(
        col: &ColumnStorage,
        name: &str,
        num_rows: usize,
        schema_fields: &mut Vec<Field>,
        arrays: &mut Vec<ArrayRef>,
    ) {
        let type_count = col.has_int as u8
            + col.has_float as u8
            + col.has_str as u8
            + col.has_bool as u8;

        if type_count > 1 {
            // Conflict: emit StructArray with typed children.
            let mut child_fields: Vec<Arc<Field>> = Vec::new();
            let mut child_arrays: Vec<ArrayRef> = Vec::new();

            if col.has_int {
                let (array, _) = Self::build_int64_column(&col.int_values, num_rows);
                child_fields.push(Arc::new(Field::new("int", DataType::Int64, true)));
                child_arrays.push(array);
            }
            if col.has_float {
                let (array, _) = Self::build_float64_column(&col.float_values, num_rows);
                child_fields.push(Arc::new(Field::new("float", DataType::Float64, true)));
                child_arrays.push(array);
            }
            if col.has_str {
                let (array, _) = Self::build_string_column(&col.str_values, num_rows);
                child_fields.push(Arc::new(Field::new("str", DataType::Utf8, true)));
                child_arrays.push(array);
            }
            if col.has_bool {
                let (array, _) = Self::build_bool_column(&col.bool_values, num_rows);
                child_fields.push(Arc::new(Field::new("bool", DataType::Boolean, true)));
                child_arrays.push(array);
            }

            let struct_validity: Vec<bool> = (0..num_rows)
                .map(|i| child_arrays.iter().any(|arr| !arr.is_null(i)))
                .collect();

            let fields = Fields::from(child_fields);
            let struct_arr = StructArray::new(
                fields.clone(),
                child_arrays,
                Some(NullBuffer::from(struct_validity)),
            );

            schema_fields.push(Field::new(name, DataType::Struct(fields), true));
            arrays.push(Arc::new(struct_arr));
        } else {
            // Single-type or empty: flat column.
            if col.has_int {
                let (array, dt) = Self::build_int64_column(&col.int_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            } else if col.has_float {
                let (array, dt) = Self::build_float64_column(&col.float_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            } else if col.has_str {
                let (array, dt) = Self::build_string_column(&col.str_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            } else if col.has_bool {
                let (array, dt) = Self::build_bool_column(&col.bool_values, num_rows);
                schema_fields.push(Field::new(name, dt, true));
                arrays.push(array);
            }
            // If no values at all, field is omitted (all-null sparse field
            // with no writes). This matches StreamingBuilder behavior.
        }
    }

    // -----------------------------------------------------------------------
    // Column builders — deferred (row, value) → Arrow array
    // -----------------------------------------------------------------------

    fn build_int64_column(values: &[(u32, i64)], num_rows: usize) -> (ArrayRef, DataType) {
        let mut data = vec![0i64; num_rows];
        let mut valid = vec![false; num_rows];
        for &(row, v) in values {
            let row = row as usize;
            if row < num_rows {
                data[row] = v;
                valid[row] = true;
            }
        }
        let nulls = NullBuffer::from(valid);
        let array = Int64Array::new(data.into(), Some(nulls));
        (Arc::new(array), DataType::Int64)
    }

    fn build_float64_column(values: &[(u32, f64)], num_rows: usize) -> (ArrayRef, DataType) {
        let mut data = vec![0.0f64; num_rows];
        let mut valid = vec![false; num_rows];
        for &(row, v) in values {
            let row = row as usize;
            if row < num_rows {
                data[row] = v;
                valid[row] = true;
            }
        }
        let nulls = NullBuffer::from(valid);
        let array = Float64Array::new(data.into(), Some(nulls));
        (Arc::new(array), DataType::Float64)
    }

    fn build_bool_column(values: &[(u32, bool)], num_rows: usize) -> (ArrayRef, DataType) {
        let mut data = vec![false; num_rows];
        let mut valid = vec![false; num_rows];
        for &(row, v) in values {
            let row = row as usize;
            if row < num_rows {
                data[row] = v;
                valid[row] = true;
            }
        }
        let nulls = NullBuffer::from(valid);
        let array = BooleanArray::new(data.into(), Some(nulls));
        (Arc::new(array), DataType::Boolean)
    }

    fn build_string_column(values: &[(u32, String)], num_rows: usize) -> (ArrayRef, DataType) {
        // Use StringBuilder (detached/copy path).
        // Block store (#1844) will add StringViewBuilder with zero-copy views.
        let mut builder = StringBuilder::with_capacity(values.len(), 0);
        // Build a row-indexed lookup for sparse fill.
        let mut row_map: Vec<Option<&str>> = vec![None; num_rows];
        for (row, v) in values {
            let row = *row as usize;
            if row < num_rows {
                row_map[row] = Some(v.as_str());
            }
        }
        for slot in &row_map {
            match slot {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        (Arc::new(builder.finish()), DataType::Utf8)
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
        b.write_str(sev, "INFO");
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
        b.write_str(h, "INFO");
        b.end_row();

        b.begin_row();
        let h2 = b.resolve_dynamic("level", FieldKind::Utf8View).unwrap();
        assert_eq!(h, h2);
        b.write_str(h2, "ERROR");
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
        b.write_str(h2, "OK");
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
        b.write_str(msg, "hello");
        b.end_row();

        b.begin_row();
        b.write_i64(ts, 2000);
        let msg2 = b.resolve_dynamic("message", FieldKind::Utf8View).unwrap();
        b.write_str(msg2, "world");
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
        b.write_str(h, "not an int");
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("x").unwrap();
        let arr = col.as_primitive::<Int64Type>();
        // Planned kind is Int64, but we only wrote a string → int column is all-null.
        assert!(arr.is_null(0));
    }
}
