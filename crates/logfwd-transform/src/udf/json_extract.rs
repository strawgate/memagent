//! UDFs for extracting fields from raw JSON strings using our SIMD scanner.
//!
//! ```sql
//! SELECT json(_raw, 'status') as status FROM logs
//! SELECT json_int(_raw, 'status') as status FROM logs WHERE json_int(_raw, 'status') > 400
//! SELECT json_float(_raw, 'duration') as dur FROM logs
//! ```

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, Float64Builder, Int64Builder};
use arrow::datatypes::DataType;

use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use logfwd_arrow::StreamingSimdScanner;
use logfwd_core::scan_config::ScanConfig;

// ---------------------------------------------------------------------------
// Shared: parse JSON lines with our SIMD scanner, extract a named field
// ---------------------------------------------------------------------------

/// Parse a batch of JSON lines and return columns for `field_name`.
/// Returns a map of suffix → ArrayRef for all variants found.
fn parse_and_extract_all(
    raw_array: &arrow::array::StringArray,
    field_name: &str,
) -> std::collections::HashMap<String, arrow::array::ArrayRef> {
    let mut result = std::collections::HashMap::new();
    if raw_array.is_empty() {
        return result;
    }

    // Reconstruct NDJSON buffer from the StringArray
    let mut buf = Vec::with_capacity(raw_array.len() * 128);
    for i in 0..raw_array.len() {
        if !raw_array.is_null(i) {
            buf.extend_from_slice(raw_array.value(i).as_bytes());
        }
        buf.push(b'\n');
    }

    let config = ScanConfig {
        wanted_fields: vec![logfwd_core::scan_config::FieldSpec {
            name: field_name.to_string(),
            aliases: vec![],
        }],
        extract_all: false,
        keep_raw: false,
        validate_utf8: false,
    };

    let mut scanner = StreamingSimdScanner::new(config);
    let batch = match scanner.scan(bytes::Bytes::from(buf)) {
        Ok(b) => b,
        Err(_) => return result,
    };

    // Collect all column variants for this field
    for suffix in ["_int", "_float", "_str", ""] {
        let col_name = if suffix.is_empty() {
            field_name.to_string()
        } else {
            format!("{field_name}{suffix}")
        };
        if let Some(col) = batch.column_by_name(&col_name) {
            result.insert(suffix.to_string(), Arc::clone(col));
        }
    }
    result
}

// ---------------------------------------------------------------------------
// json(_raw, 'key') -> Utf8
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct JsonExtractUdf {
    signature: Signature,
}

impl Default for JsonExtractUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonExtractUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonExtractUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "json"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let raw_array = match &args.args[0] {
            ColumnarValue::Array(a) => a.as_string::<i32>(),
            ColumnarValue::Scalar(_) => {
                return Err(datafusion::common::DataFusionError::Internal(
                    "json() first arg must be a string column".into(),
                ));
            }
        };
        let key = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(k))) => k.as_str(),
            _ => {
                return Err(datafusion::common::DataFusionError::Internal(
                    "json() second arg must be a string literal".into(),
                ));
            }
        };

        let cols = parse_and_extract_all(raw_array, key);
        // For json() (string extraction), prefer _str, then cast anything else.
        let arr = cols
            .get("_str")
            .or_else(|| cols.get(""))
            .or_else(|| cols.get("_int"))
            .or_else(|| cols.get("_float"));
        match arr {
            Some(a) => {
                let str_arr = arrow::compute::cast(a, &DataType::Utf8)?;
                Ok(ColumnarValue::Array(str_arr))
            }
            None => {
                let nulls = arrow::array::new_null_array(&DataType::Utf8, raw_array.len());
                Ok(ColumnarValue::Array(nulls))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// json_int(_raw, 'key') -> Int64
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct JsonExtractIntUdf {
    signature: Signature,
}

impl Default for JsonExtractIntUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonExtractIntUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonExtractIntUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "json_int"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let raw_array = match &args.args[0] {
            ColumnarValue::Array(a) => a.as_string::<i32>(),
            ColumnarValue::Scalar(_) => {
                return Err(datafusion::common::DataFusionError::Internal(
                    "json_int() first arg must be a string column".into(),
                ));
            }
        };
        let key = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(k))) => k.as_str(),
            _ => {
                return Err(datafusion::common::DataFusionError::Internal(
                    "json_int() second arg must be a string literal".into(),
                ));
            }
        };

        let cols = parse_and_extract_all(raw_array, key);
        // For json_int(), prefer _int column directly.
        if let Some(arr) = cols.get("_int") {
            return Ok(ColumnarValue::Array(Arc::clone(arr)));
        }
        // If bare column exists and is Int64, use it.
        if let Some(arr) = cols.get("") {
            if *arr.data_type() == DataType::Int64 {
                return Ok(ColumnarValue::Array(Arc::clone(arr)));
            }
        }
        // Try parsing from string column.
        if let Some(arr) = cols.get("_str").or_else(|| cols.get("")) {
            let str_arr = arrow::compute::cast(arr, &DataType::Utf8)?;
            let str_arr = str_arr
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let mut builder = Int64Builder::with_capacity(str_arr.len());
            for i in 0..str_arr.len() {
                if str_arr.is_null(i) {
                    builder.append_null();
                } else {
                    match str_arr.value(i).parse::<i64>() {
                        Ok(v) => builder.append_value(v),
                        Err(_) => builder.append_null(),
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }
        // Field not found at all.
        let nulls = arrow::array::new_null_array(&DataType::Int64, raw_array.len());
        Ok(ColumnarValue::Array(nulls))
    }
}

// ---------------------------------------------------------------------------
// json_float(_raw, 'key') -> Float64
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct JsonExtractFloatUdf {
    signature: Signature,
}

impl Default for JsonExtractFloatUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonExtractFloatUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonExtractFloatUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "json_float"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let raw_array = match &args.args[0] {
            ColumnarValue::Array(a) => a.as_string::<i32>(),
            ColumnarValue::Scalar(_) => {
                return Err(datafusion::common::DataFusionError::Internal(
                    "json_float() first arg must be a string column".into(),
                ));
            }
        };
        let key = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(k))) => k.as_str(),
            _ => {
                return Err(datafusion::common::DataFusionError::Internal(
                    "json_float() second arg must be a string literal".into(),
                ));
            }
        };

        let cols = parse_and_extract_all(raw_array, key);
        // For json_float(), prefer _float column directly.
        if let Some(arr) = cols.get("_float") {
            return Ok(ColumnarValue::Array(Arc::clone(arr)));
        }
        if let Some(arr) = cols.get("") {
            if *arr.data_type() == DataType::Float64 {
                return Ok(ColumnarValue::Array(Arc::clone(arr)));
            }
        }
        // Try _int (ints are valid floats)
        if let Some(arr) = cols.get("_int") {
            let f_arr = arrow::compute::cast(arr, &DataType::Float64)?;
            return Ok(ColumnarValue::Array(f_arr));
        }
        // Try parsing from string column.
        if let Some(arr) = cols.get("_str").or_else(|| cols.get("")) {
            let str_arr = arrow::compute::cast(arr, &DataType::Utf8)?;
            let str_arr = str_arr
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let mut builder = Float64Builder::with_capacity(str_arr.len());
            for i in 0..str_arr.len() {
                if str_arr.is_null(i) {
                    builder.append_null();
                } else {
                    match str_arr.value(i).parse::<f64>() {
                        Ok(v) => builder.append_value(v),
                        Err(_) => builder.append_null(),
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }
        let nulls = arrow::array::new_null_array(&DataType::Float64, raw_array.len());
        Ok(ColumnarValue::Array(nulls))
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
        ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new()));
        ctx.register_udf(ScalarUDF::from(JsonExtractIntUdf::new()));
        ctx.register_udf(ScalarUDF::from(JsonExtractFloatUdf::new()));
        // json_parse removed — DataFusion can't handle data-dependent return types
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
        let col = result.column(0).as_string::<i32>();
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
        assert!(col.is_null(2)); // "OK" is not an int
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
        let col = result.column(0).as_string::<i32>();
        assert_eq!(col.value(0), "error");
    }

    #[tokio::test]
    async fn test_json_missing_key_returns_null() {
        let batch = make_raw_batch(vec![r#"{"status": 200}"#, r#"{"level": "INFO"}"#]);
        let result = query("SELECT json(_raw, 'status') as s FROM logs", batch).await;
        let col = result.column(0).as_string::<i32>();
        assert_eq!(col.value(0), "200");
        // Row 1 has no "status" — but our scanner still produces the column with null
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
        let col = result.column(0).as_string::<i32>();
        assert!(col.value(0).contains("200"));
        assert!(col.value(1).contains("500"));
    }
}
