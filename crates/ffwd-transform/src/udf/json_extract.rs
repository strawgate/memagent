//! UDFs for extracting fields from raw JSON strings using our zero-copy scanner.
//!
//! ```sql
//! SELECT json(body, 'status') as status FROM logs
//! SELECT json_int(body, 'status') as status FROM logs WHERE json_int(body, 'status') > 400
//! SELECT json_float(body, 'duration') as dur FROM logs
//! ```
//!
//! A single parameterised struct [`JsonExtractUdf`] covers all three functions.
//! The [`JsonExtractMode`] selects name and return type.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, StringArray, StructArray, UInt32Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use ffwd_arrow::Scanner;
use ffwd_core::scan_config::{FieldSpec, ScanConfig};

// ---------------------------------------------------------------------------
// Mode enum
// ---------------------------------------------------------------------------

/// Selects which json extraction variant to expose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsonExtractMode {
    /// `json(body, 'key')` -> Utf8
    Str,
    /// `json_int(body, 'key')` -> Int64
    Int,
    /// `json_float(body, 'key')` -> Float64
    Float,
}

impl JsonExtractMode {
    fn udf_name(self) -> &'static str {
        match self {
            Self::Str => "json",
            Self::Int => "json_int",
            Self::Float => "json_float",
        }
    }

    fn return_type(self) -> DataType {
        match self {
            Self::Str => DataType::Utf8,
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: identify conflict struct columns
// ---------------------------------------------------------------------------

/// Returns `true` iff all child field names are conflict-type names.
fn is_conflict_struct_fields(fields: &arrow::datatypes::Fields) -> bool {
    !fields.is_empty()
        && fields
            .iter()
            .all(|f| matches!(f.name().as_str(), "int" | "float" | "str" | "bool"))
}

// ---------------------------------------------------------------------------
// Shared: parse raw lines with the scanner
// ---------------------------------------------------------------------------

/// Reconstruct an NDJSON buffer from `raw_array`, run the scanner for
/// `field_name`, and return the resulting [`RecordBatch`].
///
/// NULL rows in `raw_array` are emitted as `{}\n` (an empty JSON object).
/// The scanner parses `{}` as a row with no fields, so the requested field
/// is absent and naturally produces NULL in the output — preserving the
/// one-to-one row correspondence without a row-count mismatch.
fn parse_raw(raw_array: &StringArray, field_name: &str) -> Result<RecordBatch, DataFusionError> {
    let mut buf = Vec::with_capacity(raw_array.len() * 128);
    for i in 0..raw_array.len() {
        if raw_array.is_null(i) {
            // Emit an empty JSON object so the scanner produces a row with
            // no fields; the requested field is absent → NULL in the output.
            buf.extend_from_slice(b"{}");
        } else {
            buf.extend_from_slice(raw_array.value(i).as_bytes());
        }
        buf.push(b'\n');
    }

    let config = ScanConfig {
        wanted_fields: vec![FieldSpec {
            name: field_name.to_string(),
            aliases: vec![],
        }],
        extract_all: false,
        line_field_name: None,
        validate_utf8: false,
        row_predicate: None,
    };

    let mut scanner = Scanner::new(config);
    let batch = scanner
        .scan(bytes::Bytes::from(buf))
        .map_err(|e| DataFusionError::Execution(format!("scanner error: {e}")))?;

    if batch.num_rows() != raw_array.len() {
        return Err(DataFusionError::Execution(format!(
            "scanner row count mismatch: got {} rows, expected {}",
            batch.num_rows(),
            raw_array.len(),
        )));
    }

    Ok(batch)
}

fn build_non_null_raw(raw_array: &StringArray) -> (StringArray, UInt32Array) {
    let mut non_null_values = Vec::with_capacity(raw_array.len() - raw_array.null_count());
    let mut take_indices = Vec::with_capacity(raw_array.len());
    for i in 0..raw_array.len() {
        if raw_array.is_null(i) {
            take_indices.push(None);
        } else {
            let idx = non_null_values.len() as u32;
            take_indices.push(Some(idx));
            non_null_values.push(raw_array.value(i));
        }
    }
    (
        StringArray::from(non_null_values),
        UInt32Array::from(take_indices),
    )
}

/// Cast `arr` to [`StringArray`] (Utf8). Accepts Utf8, Utf8View, and LargeUtf8.
fn coerce_to_string_array(arr: &dyn Array) -> Result<StringArray, DataFusionError> {
    let coerced = if *arr.data_type() == DataType::Utf8 {
        Arc::new(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .expect("already checked Utf8")
                .clone(),
        ) as _
    } else {
        arrow::compute::cast(arr, &DataType::Utf8)
            .map_err(|e| DataFusionError::Execution(format!("Failed to cast body to Utf8: {e}")))?
    };
    Ok(coerced
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 must yield StringArray")
        .clone())
}

// ---------------------------------------------------------------------------
// The unified UDF struct
// ---------------------------------------------------------------------------

/// Scalar UDF that extracts a named field from raw JSON lines.
///
/// Instantiate once per [`JsonExtractMode`] and register with DataFusion.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonExtractUdf {
    mode: JsonExtractMode,
    signature: Signature,
}

impl JsonExtractUdf {
    pub fn new(mode: JsonExtractMode) -> Self {
        // Accept Utf8, Utf8View, and LargeUtf8 as the first argument.
        let variants = vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
            // The key argument may also come as Utf8View from the planner.
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View]),
            TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
            TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8View]),
        ];
        Self {
            mode,
            signature: Signature::new(TypeSignature::OneOf(variants), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonExtractUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        self.mode.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(self.mode.return_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let udf_name = self.mode.udf_name();

        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(format!(
                "{udf_name}() expects exactly two arguments"
            )));
        }

        // --- arg 0: the raw column (coerce to StringArray) ---
        let raw_array = match &args.args[0] {
            ColumnarValue::Array(a) => coerce_to_string_array(a.as_ref())?,
            ColumnarValue::Scalar(_) => {
                return Err(DataFusionError::Internal(format!(
                    "{udf_name}() first arg must be a string column",
                )));
            }
        };

        // --- arg 1: field name (constant string) ---
        let key = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(k)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(k))) => k.as_str(),
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{udf_name}() second arg must be a string literal",
                )));
            }
        };

        let target_dt = self.mode.return_type();
        let num_rows = raw_array.len();
        let has_nulls = raw_array.null_count() > 0;

        // Only build the filter/take indices when there are actual nulls to handle.
        let (filtered, take_indices) = if has_nulls {
            let (f, indices) = build_non_null_raw(&raw_array);
            (Some(f), Some(indices))
        } else {
            (None, None)
        };
        let raw_to_parse = filtered.as_ref().unwrap_or(&raw_array);
        if raw_to_parse.is_empty() {
            return Ok(ColumnarValue::Array(arrow::array::new_null_array(
                &target_dt, num_rows,
            )));
        }

        // --- parse ---
        let batch = parse_raw(raw_to_parse, key)?;

        // --- coerce to the declared return type ---
        let result_rows = raw_to_parse.len();

        // 1. Try flat column (single-type field — no conflict in this batch).
        // Skip StructArrays here; they are handled in step 2 below.
        if let Some(flat_col) = batch.column_by_name(key) {
            if matches!(flat_col.data_type(), DataType::Struct(_)) {
                // Fall through to step 2 (struct conflict column path).
            } else {
                let result = match self.mode {
                    JsonExtractMode::Str => {
                        // Cast any type to Utf8.
                        arrow::compute::cast(flat_col, &DataType::Utf8)?
                    }
                    JsonExtractMode::Int => {
                        if *flat_col.data_type() == DataType::Int64 {
                            Arc::clone(flat_col)
                        } else {
                            // Bare string column — the JSON value is not a number.
                            // Return all-null rather than coercing "200" → 200.
                            arrow::array::new_null_array(&DataType::Int64, result_rows)
                        }
                    }
                    JsonExtractMode::Float => {
                        if *flat_col.data_type() == DataType::Float64 {
                            Arc::clone(flat_col)
                        } else if *flat_col.data_type() == DataType::Int64 {
                            arrow::compute::cast(flat_col, &DataType::Float64)?
                        } else {
                            // Bare string column — the JSON value is not a number.
                            arrow::array::new_null_array(&DataType::Float64, result_rows)
                        }
                    }
                };
                let projected = if let Some(ref indices) = take_indices {
                    arrow::compute::take(result.as_ref(), indices, None)?
                } else {
                    result
                };
                return Ok(ColumnarValue::Array(projected));
            } // end non-struct flat column branch
        }

        // 2. Try struct conflict column.
        let struct_col_idx = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| {
                f.name() == key
                    && matches!(f.data_type(), DataType::Struct(cf) if is_conflict_struct_fields(cf))
            })
            .map(|(i, _)| i);

        if let Some(idx) = struct_col_idx {
            let struct_arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("column declared as Struct must downcast to StructArray");
            let field_dt = batch.schema().field(idx).data_type().clone();
            let child_fields = match &field_dt {
                DataType::Struct(f) => f.clone(),
                _ => unreachable!(),
            };

            let find_child = |name: &str| -> Option<&dyn Array> {
                child_fields
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == name)
                    .map(|(i, _)| struct_arr.column(i).as_ref())
            };

            let result: Arc<dyn Array> = match self.mode {
                JsonExtractMode::Str => {
                    // Coalesce all children to Utf8 (int > float > bool > str priority).
                    use ffwd_arrow::conflict_schema::merge_to_utf8;
                    merge_to_utf8(
                        find_child("int"),
                        find_child("float"),
                        find_child("bool"),
                        find_child("str"),
                        result_rows,
                    )
                }
                JsonExtractMode::Int => match find_child("int") {
                    Some(int_col) => arrow::compute::cast(int_col, &DataType::Int64)
                        .unwrap_or_else(|_| {
                            arrow::array::new_null_array(&DataType::Int64, result_rows)
                        }),
                    None => arrow::array::new_null_array(&DataType::Int64, result_rows),
                },
                JsonExtractMode::Float => {
                    if let Some(float_col) = find_child("float") {
                        arrow::compute::cast(float_col, &DataType::Float64).unwrap_or_else(|_| {
                            arrow::array::new_null_array(&DataType::Float64, result_rows)
                        })
                    } else if let Some(int_col) = find_child("int") {
                        // Promote int to float.
                        arrow::compute::cast(int_col, &DataType::Float64).unwrap_or_else(|_| {
                            arrow::array::new_null_array(&DataType::Float64, result_rows)
                        })
                    } else {
                        arrow::array::new_null_array(&DataType::Float64, result_rows)
                    }
                }
            };
            let projected = if let Some(ref indices) = take_indices {
                arrow::compute::take(result.as_ref(), indices, None)?
            } else {
                result
            };
            return Ok(ColumnarValue::Array(projected));
        }

        // 3. Neither flat nor conflict struct found → all-null.
        Ok(ColumnarValue::Array(arrow::array::new_null_array(
            &target_dt, num_rows,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::*;

    fn make_raw_batch(lines: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("body", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(lines)) as arrow::array::ArrayRef],
        )
        .unwrap()
    }

    fn make_nullable_raw_batch(lines: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("body", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(lines)) as arrow::array::ArrayRef],
        )
        .unwrap()
    }

    async fn query(sql: &str, batch: RecordBatch) -> RecordBatch {
        let schema = batch.schema();
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Str)));
        ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Int)));
        ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Float)));
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("logs", Arc::new(table)).unwrap();
        let df = ctx.sql(sql).await.unwrap();
        df.collect().await.unwrap().into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn test_json_extract_string() {
        let batch = make_raw_batch(vec![
            r#"{"status": 200, "level": "INFO"}"#,
            r#"{"status": 404, "level": "WARN"}"#,
        ]);
        let result = query("SELECT json(body, 'level') as level FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "INFO");
        assert_eq!(col.value(1), "WARN");
    }

    #[tokio::test]
    async fn test_json_extract_int() {
        let batch = make_raw_batch(vec![
            r#"{"status": 200}"#,
            r#"{"status": 500}"#,
            r#"{"status": "OK"}"#,
        ]);
        let result = query("SELECT json_int(body, 'status') as s FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 200);
        assert_eq!(col.value(1), 500);
        assert!(col.is_null(2));
    }

    #[tokio::test]
    async fn test_json_extract_float() {
        let batch = make_raw_batch(vec![r#"{"duration": 1.5}"#, r#"{"duration": "fast"}"#]);
        let result = query("SELECT json_float(body, 'duration') as d FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 1.5).abs() < 0.001);
        assert!(col.is_null(1));
    }

    #[tokio::test]
    async fn test_json_where_clause() {
        let batch = make_raw_batch(vec![
            r#"{"status": 200, "msg": "ok"}"#,
            r#"{"status": 500, "msg": "error"}"#,
            r#"{"status": 301, "msg": "redirect"}"#,
        ]);
        let result = query(
            "SELECT json(body, 'msg') as msg FROM logs WHERE json_int(body, 'status') > 400",
            batch,
        )
        .await;
        assert_eq!(result.num_rows(), 1);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "error");
    }

    #[tokio::test]
    async fn test_json_missing_key_returns_null() {
        let batch = make_raw_batch(vec![r#"{"status": 200}"#, r#"{"level": "INFO"}"#]);
        let result = query("SELECT json(body, 'status') as s FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "200");
        assert!(col.is_null(1));
    }

    #[tokio::test]
    async fn test_passthrough_raw() {
        let batch = make_raw_batch(vec![
            r#"{"status": 200, "level": "INFO"}"#,
            r#"{"status": 500, "level": "ERROR"}"#,
        ]);
        let result = query("SELECT body FROM logs", batch).await;
        assert_eq!(result.num_rows(), 2);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(col.value(0).contains("200"));
        assert!(col.value(1).contains("500"));
    }

    /// `json_int` on a field that is a quoted string (no type conflict in the
    /// batch) must return NULL, not a coerced integer. The scanner emits a bare
    /// Utf8 column `status` (no conflict), so `json_int` finds a string column
    /// and must return null rather than silently parsing "200" as 200.
    #[tokio::test]
    async fn test_json_int_on_quoted_string_is_null() {
        let batch = make_raw_batch(vec![r#"{"status": "200"}"#]);
        let result = query("SELECT json_int(body, 'status') as s FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            col.is_null(0),
            "json_int on a quoted string must return null"
        );
    }

    /// `json(body, 'status')` on a batch where some rows have `{"status": 200}`
    /// and others have `{"status": "OK"}` must coalesce all variants row-by-row
    /// and return `["200", "OK"]` — not `[null, "OK"]`.
    #[tokio::test]
    async fn test_json_str_on_conflict_batch_coalesces() {
        let batch = make_raw_batch(vec![r#"{"status": 200}"#, r#"{"status": "OK"}"#]);
        let result = query("SELECT json(body, 'status') as s FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "200", "int row must be coalesced to string");
        assert_eq!(col.value(1), "OK", "str row must be returned as-is");
    }

    /// UDF-level regression: mixed bool/string rows for the same key must
    /// coalesce with bool values preserved as "true"/"false" before string fallback.
    #[tokio::test]
    async fn test_json_str_on_bool_string_conflict_batch_coalesces() {
        let batch = make_raw_batch(vec![
            r#"{"active": true}"#,
            r#"{"active": false}"#,
            r#"{"active": "fallback"}"#,
        ]);
        let result = query("SELECT json(body, 'active') as active FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "true", "bool true must stringify to 'true'");
        assert_eq!(
            col.value(1),
            "false",
            "bool false must stringify to 'false'"
        );
        assert_eq!(col.value(2), "fallback", "string row must remain unchanged");
    }

    /// `json_float` on a field that is a quoted string must return NULL.
    #[tokio::test]
    async fn test_json_float_on_quoted_string_is_null() {
        let batch = make_raw_batch(vec![r#"{"duration": "1.5"}"#]);
        let result = query("SELECT json_float(body, 'duration') as d FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            col.is_null(0),
            "json_float on a quoted string must return null"
        );
    }

    #[tokio::test]
    async fn test_json_udfs_return_null_for_null_raw() {
        let batch = make_nullable_raw_batch(vec![
            Some(r#"{"status": 200, "duration": 1.5, "msg": "ok"}"#),
            None,
            Some(r#"{"status": 500, "duration": 2.25, "msg": "error"}"#),
        ]);
        let result = query(
            "SELECT \
                json(body, 'msg') AS msg, \
                json_int(body, 'status') AS status, \
                json_float(body, 'duration') AS duration \
             FROM logs",
            batch,
        )
        .await;
        let msg = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let status = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let duration = result
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(msg.value(0), "ok");
        assert_eq!(status.value(0), 200);
        assert!((duration.value(0) - 1.5).abs() < 0.001);

        assert!(msg.is_null(1));
        assert!(status.is_null(1));
        assert!(duration.is_null(1));

        assert_eq!(msg.value(2), "error");
        assert_eq!(status.value(2), 500);
        assert!((duration.value(2) - 2.25).abs() < 0.001);
    }

    /// `json(body, 'key')` must return NULL for rows where `body` itself is NULL,
    /// without erroring the whole batch.
    ///
    /// Regression test: previously, NULL rows in `body` were emitted as bare `\n`
    /// (empty lines).  The scanner skips empty lines, so the output had fewer rows
    /// than the input, triggering a "scanner row count mismatch" error for the
    /// entire batch.
    #[tokio::test]
    async fn test_json_null_body_returns_null_not_error() {
        // Build a nullable body column: row 0 has data, row 1 is NULL, row 2 has data.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("body", DataType::Utf8, true),
        ]));
        let raw: arrow::array::ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"status": 200}"#),
            None,
            Some(r#"{"status": 404}"#),
        ]));
        let batch = RecordBatch::try_new(schema, vec![raw]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Str)));
        ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Int)));
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("logs", Arc::new(table)).unwrap();

        // json() — must not error; null row must return null
        let result = ctx
            .sql("SELECT json(body, 'status') AS s FROM logs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result = result.into_iter().next().unwrap();
        assert_eq!(result.num_rows(), 3, "output must have 3 rows");
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "200", "row 0 must extract status");
        assert!(col.is_null(1), "row 1 (null body) must produce null");
        assert_eq!(col.value(2), "404", "row 2 must extract status");

        // json_int() — same null row must not crash
        let ctx2 = SessionContext::new();
        ctx2.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Int)));
        let raw2: arrow::array::ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"status": 200}"#),
            None,
            Some(r#"{"status": 404}"#),
        ]));
        let schema2 = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("body", DataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(schema2, vec![raw2]).unwrap();
        let table2 = MemTable::try_new(batch2.schema(), vec![vec![batch2]]).unwrap();
        ctx2.register_table("logs", Arc::new(table2)).unwrap();
        let result2 = ctx2
            .sql("SELECT json_int(body, 'status') AS s FROM logs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result2 = result2.into_iter().next().unwrap();
        let col2 = result2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col2.value(0), 200, "json_int row 0");
        assert!(
            col2.is_null(1),
            "json_int row 1 (null body) must produce null"
        );
        assert_eq!(col2.value(2), 404, "json_int row 2");
    }
}
