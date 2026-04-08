//! UDF: regexp_extract(string, pattern, group_index) -> Utf8
//!
//! Spark-compatible regex extraction. Returns the capture group at the given
//! index (1-based), or the full match if index is 0. Returns NULL on no match.
//!
//! ```sql
//! SELECT regexp_extract(message_str, 'status=(\d+)', 1) AS status FROM logs
//! SELECT regexp_extract(message_str, 'duration=(\d+)ms', 1) AS duration FROM logs
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, AsArray, StringBuilder};
use arrow::datatypes::DataType;

use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use regex::Regex;

/// UDF: regexp_extract(string, pattern, group_index) -> Utf8
///
/// - `string`: the input column (Utf8)
/// - `pattern`: regex pattern with capture groups (Utf8 literal)
/// - `group_index`: 0 for full match, 1+ for capture groups (Int64)
///
/// Returns NULL when the pattern doesn't match or the group index is out of range.
#[derive(Debug)]
pub struct RegexpExtractUdf {
    signature: Signature,
    /// Per-pattern regex cache.  DataFusion shares the same `ScalarUDFImpl`
    /// instance across all `regexp_extract(...)` expressions in a query, so a
    /// single-slot `OnceLock` would permanently cache the first pattern seen and
    /// silently apply it to all subsequent calls with different patterns.
    /// Keying the cache by pattern string correctly handles multiple patterns.
    regex_cache: Mutex<HashMap<String, Arc<Regex>>>,
}

impl Default for RegexpExtractUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                Volatility::Immutable,
            ),
            regex_cache: Mutex::new(HashMap::new()),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "regexp_extract() expects exactly three arguments".to_string(),
            ));
        }

        let input = &args.args[0];
        let pattern = &args.args[1];
        let group_idx = &args.args[2];

        // Extract the pattern string (must be a constant/scalar).
        let pattern_str = match pattern {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(s) => {
                let s = s.to_string();
                s.trim_matches('"').trim_matches('\'').to_string()
            }
            ColumnarValue::Array(_) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "regexp_extract() pattern argument must be a scalar string literal".to_string(),
                ));
            }
        };

        // Get or compile the regex, keyed by pattern string.  Each distinct
        // pattern gets its own cache entry so that multiple regexp_extract(...)
        // calls with different patterns in the same SQL query each use the
        // correct compiled regex.
        let re: Arc<Regex> = {
            let mut cache = self.regex_cache.lock().map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "regexp_extract() internal cache lock poisoned".to_string(),
                )
            })?;
            if let Some(cached) = cache.get(&pattern_str) {
                Arc::clone(cached)
            } else {
                let compiled = Regex::new(&pattern_str).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "regexp_extract: invalid pattern '{}': {}",
                        pattern_str, e
                    ))
                })?;
                let arc = Arc::new(compiled);
                cache.insert(pattern_str, Arc::clone(&arc));
                arc
            }
        };

        // Extract group index.
        let idx = match group_idx {
            ColumnarValue::Scalar(s) => {
                if let datafusion::common::ScalarValue::Int64(Some(v)) = s {
                    *v as usize
                } else {
                    0
                }
            }
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_primitive::<arrow::datatypes::Int64Type>();
                if int_arr.is_empty() || int_arr.is_null(0) {
                    0
                } else {
                    int_arr.value(0) as usize
                }
            }
        };

        match input {
            ColumnarValue::Array(array) => {
                let str_array = array.as_string::<i32>();
                let mut builder =
                    StringBuilder::with_capacity(str_array.len(), str_array.len() * 32);

                for i in 0..str_array.len() {
                    if str_array.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    let val = str_array.value(i);
                    match re.captures(val) {
                        Some(caps) => match caps.get(idx) {
                            Some(m) => builder.append_value(m.as_str()),
                            None => builder.append_null(),
                        },
                        None => builder.append_null(),
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(scalar) => {
                // Propagate SQL NULL rather than matching against the literal
                // string "NULL" that ScalarValue::Utf8(None).to_string() produces.
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(
                        datafusion::common::ScalarValue::Utf8(None),
                    ));
                }
                let val = scalar.to_string();
                let val = val.trim_matches('"').trim_matches('\'');
                match re.captures(val) {
                    Some(caps) => match caps.get(idx) {
                        Some(m) => Ok(ColumnarValue::Scalar(
                            datafusion::common::ScalarValue::Utf8(Some(m.as_str().to_string())),
                        )),
                        None => Ok(ColumnarValue::Scalar(
                            datafusion::common::ScalarValue::Utf8(None),
                        )),
                    },
                    None => Ok(ColumnarValue::Scalar(
                        datafusion::common::ScalarValue::Utf8(None),
                    )),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::*;

    /// Helper to run a SQL query with regexp_extract registered.
    async fn run_sql(batch: RecordBatch, sql: &str) -> RecordBatch {
        let batches = run_sql_result(batch, sql).await.unwrap();
        batches.into_iter().next().unwrap()
    }

    async fn run_sql_result(
        batch: RecordBatch,
        sql: &str,
    ) -> datafusion::error::Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(RegexpExtractUdf::new()));
        ctx.register_udf(ScalarUDF::from(crate::IntCastUdf::new()));
        let table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("logs", Arc::new(table)).unwrap();
        let df = ctx.sql(sql).await?;
        df.collect().await
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let msgs: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some("GET /api/users 200 15ms"),
            Some("POST /api/orders 500 230ms"),
            Some("no match here"),
            None,
        ]));
        RecordBatch::try_new(schema, vec![msgs]).unwrap()
    }

    #[test]
    fn test_extract_group_1() {
        let batch = make_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT regexp_extract(msg, '(GET|POST) (\\S+) (\\d+) (\\d+)ms', 3) AS status FROM logs",
        ));

        let status = result
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(status.value(0), "200");
        assert_eq!(status.value(1), "500");
        assert!(status.is_null(2)); // no match
        assert!(status.is_null(3)); // NULL input
    }

    #[test]
    fn test_extract_group_0_full_match() {
        let batch = make_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT regexp_extract(msg, '\\d+ms', 0) AS duration FROM logs",
        ));

        let dur = result
            .column_by_name("duration")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(dur.value(0), "15ms");
        assert_eq!(dur.value(1), "230ms");
        assert!(dur.is_null(2));
    }

    #[test]
    fn test_extract_multiple_fields() {
        let batch = make_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT \
                regexp_extract(msg, '(GET|POST) (\\S+) (\\d+) (\\d+)ms', 1) AS method, \
                regexp_extract(msg, '(GET|POST) (\\S+) (\\d+) (\\d+)ms', 2) AS path, \
                int(regexp_extract(msg, '(GET|POST) (\\S+) (\\d+) (\\d+)ms', 4)) AS duration_ms \
             FROM logs",
        ));

        let method = result
            .column_by_name("method")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method.value(0), "GET");
        assert_eq!(method.value(1), "POST");

        let path = result
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path.value(0), "/api/users");
        assert_eq!(path.value(1), "/api/orders");

        // int() composed with regexp_extract
        let dur = result
            .column_by_name("duration_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(dur.value(0), 15);
        assert_eq!(dur.value(1), 230);
    }

    /// Regression: regexp_extract(NULL literal, ...) must return NULL, not
    /// match against the string "NULL" that ScalarValue::to_string() produces.
    ///
    /// Before the fix, `regexp_extract(NULL, '(.+)', 1)` returned "NULL" because
    /// the scalar branch called `scalar.to_string()` without checking `is_null()`.
    #[test]
    fn test_null_literal_input_returns_null() {
        let batch = make_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // CAST(NULL AS VARCHAR) forces a scalar Utf8(None) through the UDF.
        let result = rt.block_on(run_sql(
            batch,
            "SELECT regexp_extract(CAST(NULL AS VARCHAR), '(.+)', 1) AS extracted FROM logs",
        ));
        let col = result.column_by_name("extracted").unwrap();
        // Every row must be NULL — the pattern '(.+)' would match "NULL" if the
        // bug were present.
        for row in 0..result.num_rows() {
            assert!(
                col.is_null(row),
                "row {row}: expected NULL but got a non-null value (bug: matched against \"NULL\")"
            );
        }
    }

    /// Two regexp_extract calls with DIFFERENT patterns in the same query must
    /// each use their own compiled regex.
    ///
    /// Regression test for OnceLock caching bug: because DataFusion shares the
    /// same ScalarUDFImpl instance for all regexp_extract expressions in a query,
    /// a shared OnceLock would cache the first pattern and apply it to all
    /// subsequent calls — silently producing wrong results.
    #[test]
    fn test_two_different_patterns_in_same_query() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let msgs: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some("error=TIMEOUT user=alice"),
            Some("error=AUTH user=bob"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![msgs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT \
                regexp_extract(msg, 'error=(\\w+)', 1) AS err_code, \
                regexp_extract(msg, 'user=(\\w+)', 1)  AS user_name \
             FROM logs",
        ));

        let err = result
            .column_by_name("err_code")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            err.value(0),
            "TIMEOUT",
            "err_code row 0 must match error= pattern"
        );
        assert_eq!(
            err.value(1),
            "AUTH",
            "err_code row 1 must match error= pattern"
        );

        let user = result
            .column_by_name("user_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            user.value(0),
            "alice",
            "user_name row 0 must match user= pattern"
        );
        assert_eq!(
            user.value(1),
            "bob",
            "user_name row 1 must match user= pattern"
        );
    }

    #[test]
    fn test_regexp_extract_rejects_array_pattern_argument() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("pattern", DataType::Utf8, true),
        ]));
        let msg: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("status=200")]));
        let pattern: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("status=(\\d+)")]));
        let batch = RecordBatch::try_new(schema, vec![msg, pattern]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt
            .block_on(run_sql_result(
                batch,
                "SELECT regexp_extract(msg, pattern, 1) AS status FROM logs",
            ))
            .expect_err("array pattern argument must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("pattern argument must be a scalar string literal"),
            "unexpected error: {msg}"
        );
    }
}
