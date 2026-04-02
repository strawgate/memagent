//! UDFs for extracting fields from raw JSON strings using our SIMD scanner.
//!
//! ```sql
//! SELECT json(_raw, 'status') as status FROM logs
//! SELECT json_int(_raw, 'status') as status FROM logs WHERE json_int(_raw, 'status') > 400
//! SELECT json_float(_raw, 'duration') as dur FROM logs
//! ```
//!
//! A single parameterised struct [`JsonExtractUdf`] covers all three functions.
//! The [`JsonExtractMode`] selects name, return type, and suffix lookup order.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use logfwd_arrow::StreamingSimdScanner;
use logfwd_core::scan_config::{FieldSpec, ScanConfig};

// ---------------------------------------------------------------------------
// Mode enum
// ---------------------------------------------------------------------------

/// Selects which json extraction variant to expose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonExtractMode {
    /// `json(_raw, 'key')` -> Utf8
    Str,
    /// `json_int(_raw, 'key')` -> Int64
    Int,
    /// `json_float(_raw, 'key')` -> Float64
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

    /// Column-name suffixes to try, in preference order, when looking up the
    /// scanner output for this mode. Bare name (`""`) is included as a
    /// fallback for single-type fields, which use the bare field name
    /// when there is no type conflict in the batch.
    fn suffix_order(self) -> &'static [&'static str] {
        match self {
            Self::Str => &["", "__str", "__int", "__float"],
            Self::Int => &["__int", ""],
            Self::Float => &["__float", "__int", ""],
        }
    }
}

// ---------------------------------------------------------------------------
// Shared: parse raw lines with the SIMD scanner
// ---------------------------------------------------------------------------

/// Reconstruct an NDJSON buffer from `raw_array`, run the scanner for
/// `field_name`, and return the resulting [`RecordBatch`].
///
/// # NULL handling limitation
///
/// When `raw_array` contains NULL entries, this function emits an empty line
/// for each NULL (a bare `\n`) before calling the scanner. The scanner skips
/// empty lines and therefore produces fewer output rows than the length of
/// `raw_array`. The row-count check at the end of this function will catch
/// that mismatch and return a `DataFusionError::Execution` containing the
/// text `"scanner row count mismatch"`. This is a known limitation: callers
/// (and tests) that need to handle NULL `_raw` rows must either pre-filter
/// NULLs or treat the resulting error as expected.
fn parse_raw(raw_array: &StringArray, field_name: &str) -> Result<RecordBatch, DataFusionError> {
    let mut buf = Vec::with_capacity(raw_array.len() * 128);
    for i in 0..raw_array.len() {
        if !raw_array.is_null(i) {
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
        keep_raw: false,
        validate_utf8: false,
    };

    let mut scanner = StreamingSimdScanner::new(config);
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
            .map_err(|e| DataFusionError::Execution(format!("Failed to cast _raw to Utf8: {e}")))?
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
#[derive(Debug)]
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

        // --- parse ---
        let batch = parse_raw(&raw_array, key)?;

        // --- coerce to the declared return type ---
        let target_dt = self.mode.return_type();
        let num_rows = raw_array.len();

        // Special case: Str mode on a conflict batch where the bare column is
        // absent. Coalesce all variant columns row-by-row into a single Utf8.
        if self.mode == JsonExtractMode::Str && batch.column_by_name(key).is_none() {
            let int_col = batch.column_by_name(&format!("{key}__int"));
            let float_col = batch.column_by_name(&format!("{key}__float"));
            let str_col = batch.column_by_name(&format!("{key}__str"));

            if int_col.is_some() || float_col.is_some() || str_col.is_some() {
                use crate::conflict_schema::merge_to_utf8;
                let merged = merge_to_utf8(
                    int_col.map(Arc::as_ref),
                    float_col.map(Arc::as_ref),
                    str_col.map(Arc::as_ref),
                    num_rows,
                );
                return Ok(ColumnarValue::Array(merged));
            }
        }

        // --- look up the best column by suffix order ---
        let col = self.mode.suffix_order().iter().find_map(|suffix| {
            batch
                .column_by_name(&format!("{key}{suffix}"))
                .map(Arc::clone)
        });

        let result = match col {
            None => arrow::array::new_null_array(&target_dt, num_rows),
            Some(arr) => match self.mode {
                JsonExtractMode::Str => {
                    // Cast any type to Utf8.
                    arrow::compute::cast(&arr, &DataType::Utf8)?
                }
                JsonExtractMode::Int => {
                    if *arr.data_type() == DataType::Int64 {
                        arr
                    } else {
                        // The column is not Int64 (e.g. bare string column found
                        // via bare-name fallback). The JSON value is not a number,
                        // so return all-null rather than coercing a string "200" → 200.
                        arrow::array::new_null_array(&DataType::Int64, num_rows)
                    }
                }
                JsonExtractMode::Float => {
                    if *arr.data_type() == DataType::Float64 {
                        arr
                    } else if *arr.data_type() == DataType::Int64 {
                        arrow::compute::cast(&arr, &DataType::Float64)?
                    } else {
                        // The column is not a numeric type (e.g. bare string column found
                        // via bare-name fallback). The JSON value is not a number,
                        // so return all-null rather than coercing a string "1.5" → 1.5.
                        arrow::array::new_null_array(&DataType::Float64, num_rows)
                    }
                }
            },
        };

        Ok(ColumnarValue::Array(result))
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
            arrow::datatypes::Field::new("_raw", DataType::Utf8, false),
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
        let result = query("SELECT json(_raw, 'level') as level FROM logs", batch).await;
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
        let result = query("SELECT json_int(_raw, 'status') as s FROM logs", batch).await;
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
        let result = query("SELECT json_float(_raw, 'duration') as d FROM logs", batch).await;
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
            "SELECT json(_raw, 'msg') as msg FROM logs WHERE json_int(_raw, 'status') > 400",
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
        let result = query("SELECT json(_raw, 'status') as s FROM logs", batch).await;
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
        let result = query("SELECT _raw FROM logs", batch).await;
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
        let result = query("SELECT json_int(_raw, 'status') as s FROM logs", batch).await;
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

    /// `json(_raw, 'status')` on a batch where some rows have `{"status": 200}`
    /// and others have `{"status": "OK"}` must coalesce all variants row-by-row
    /// and return `["200", "OK"]` — not `[null, "OK"]`.
    #[tokio::test]
    async fn test_json_str_on_conflict_batch_coalesces() {
        let batch = make_raw_batch(vec![r#"{"status": 200}"#, r#"{"status": "OK"}"#]);
        let result = query("SELECT json(_raw, 'status') as s FROM logs", batch).await;
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "200", "int row must be coalesced to string");
        assert_eq!(col.value(1), "OK", "str row must be returned as-is");
    }

    /// `json_float` on a field that is a quoted string must return NULL.
    #[tokio::test]
    async fn test_json_float_on_quoted_string_is_null() {
        let batch = make_raw_batch(vec![r#"{"duration": "1.5"}"#]);
        let result = query("SELECT json_float(_raw, 'duration') as d FROM logs", batch).await;
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
}
