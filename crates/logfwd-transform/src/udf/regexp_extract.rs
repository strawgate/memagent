//! UDF: regexp_extract(string, pattern, group_index) -> Utf8
//!
//! Spark-compatible regex extraction. Returns the capture group at the given
//! index (1-based), or the full match if index is 0. Returns NULL on no match.
//! `string` and `pattern` accept Utf8, Utf8View, or LargeUtf8 expressions;
//! `pattern` must still be a scalar literal at runtime.
//!
//! ```sql
//! SELECT regexp_extract(message_str, 'status=(\d+)', 1) AS status FROM logs
//! SELECT regexp_extract(message_str, 'duration=(\d+)ms', 1) AS duration FROM logs
//! ```

use std::any::Any;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, AsArray, StringBuilder};
use arrow::datatypes::DataType;

use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::udf::bounded_lru::BoundedLruCache;
use regex::Regex;

const REGEXP_CACHE_CAPACITY: usize = 128;

type BoundedRegexCache = BoundedLruCache<String, Arc<Regex>>;

/// UDF: regexp_extract(string, pattern, group_index) -> Utf8
///
/// SQL shape: `regexp_extract(<text>, <regex-literal>, <group-index>)`.
/// `<text>` and `<regex-literal>` may be Utf8, Utf8View, or LargeUtf8
/// expressions; `<group-index>` is Int64 (`0` = full match, `1+` = capture).
/// Returns NULL when the pattern does not match or the group index is out of range.
pub struct RegexpExtractUdf {
    signature: Signature,
    /// Per-pattern regex cache.  DataFusion shares the same `ScalarUDFImpl`
    /// instance across all `regexp_extract(...)` expressions in a query, so a
    /// single-slot `OnceLock` would permanently cache the first pattern seen and
    /// silently apply it to all subsequent calls with different patterns.
    /// Keying the cache by pattern string correctly handles multiple patterns.
    regex_cache: Mutex<BoundedRegexCache>,
}

impl std::fmt::Debug for RegexpExtractUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegexpExtractUdf").finish_non_exhaustive()
    }
}

impl PartialEq for RegexpExtractUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for RegexpExtractUdf {}

impl std::hash::Hash for RegexpExtractUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
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
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View, DataType::Int64]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::Utf8View,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8View,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::LargeUtf8,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                        DataType::Int64,
                    ]),
                ]),
                Volatility::Immutable,
            ),
            regex_cache: Mutex::new(BoundedRegexCache::new(REGEXP_CACHE_CAPACITY)),
        }
    }

    fn get_or_compile_regex(&self, pattern: &str) -> DfResult<Arc<Regex>> {
        {
            let mut cache = self.regex_cache.lock().map_err(|_e| {
                datafusion::error::DataFusionError::Execution(
                    "regexp_extract() internal cache lock poisoned".to_string(),
                )
            })?;
            if let Some(cached) = cache.get_cloned(pattern) {
                return Ok(cached);
            }
        }

        let compiled = Arc::new(Regex::new(pattern).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "regexp_extract: invalid pattern '{pattern}': {e}"
            ))
        })?);

        let mut cache = self.regex_cache.lock().map_err(|_e| {
            datafusion::error::DataFusionError::Execution(
                "regexp_extract() internal cache lock poisoned".to_string(),
            )
        })?;
        if let Some(cached) = cache.get_cloned(pattern) {
            return Ok(cached);
        }
        cache.insert(pattern.to_owned(), Arc::clone(&compiled));
        Ok(compiled)
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
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8View(Some(s)))
            | ColumnarValue::Scalar(datafusion::common::ScalarValue::LargeUtf8(Some(s))) => {
                s.clone()
            }
            // NULL pattern -> NULL propagation: return all-null result.
            ColumnarValue::Scalar(s) if s.is_null() => {
                return Ok(ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Utf8(None),
                ));
            }
            ColumnarValue::Scalar(_) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "regexp_extract() pattern argument must be a scalar string literal".to_string(),
                ));
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
        let re = self.get_or_compile_regex(&pattern_str)?;

        // Extract group index.
        let idx = match group_idx {
            ColumnarValue::Scalar(s) => {
                if let datafusion::common::ScalarValue::Int64(Some(v)) = s {
                    *v as usize
                } else {
                    0
                }
            }
            ColumnarValue::Array(_) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "regexp_extract() group_index argument must be a scalar integer literal, not an array column".to_string(),
                ));
            }
        };

        match input {
            ColumnarValue::Array(array) => {
                let num_rows = array.len();
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);

                match array.data_type() {
                    DataType::Utf8 => {
                        let strings = array.as_string::<i32>();
                        for i in 0..num_rows {
                            if strings.is_null(i) {
                                builder.append_null();
                                continue;
                            }
                            match re.captures(strings.value(i)) {
                                Some(caps) => match caps.get(idx) {
                                    Some(m) => builder.append_value(m.as_str()),
                                    None => builder.append_null(),
                                },
                                None => builder.append_null(),
                            }
                        }
                    }
                    DataType::Utf8View => {
                        let strings = array.as_string_view();
                        for i in 0..num_rows {
                            if strings.is_null(i) {
                                builder.append_null();
                                continue;
                            }
                            match re.captures(strings.value(i)) {
                                Some(caps) => match caps.get(idx) {
                                    Some(m) => builder.append_value(m.as_str()),
                                    None => builder.append_null(),
                                },
                                None => builder.append_null(),
                            }
                        }
                    }
                    DataType::LargeUtf8 => {
                        let strings = array.as_string::<i64>();
                        for i in 0..num_rows {
                            if strings.is_null(i) {
                                builder.append_null();
                                continue;
                            }
                            match re.captures(strings.value(i)) {
                                Some(caps) => match caps.get(idx) {
                                    Some(m) => builder.append_value(m.as_str()),
                                    None => builder.append_null(),
                                },
                                None => builder.append_null(),
                            }
                        }
                    }
                    other => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "regexp_extract() input must be Utf8/Utf8View/LargeUtf8, got {other:?}"
                        )));
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
                // Extract the inner string directly from ScalarValue to avoid
                // trim_matches corruption on values with boundary quotes.
                let val = match scalar {
                    datafusion::common::ScalarValue::Utf8(Some(s))
                    | datafusion::common::ScalarValue::Utf8View(Some(s))
                    | datafusion::common::ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                    other => other.to_string(),
                };
                match re.captures(&val) {
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
        ctx.register_udf(ScalarUDF::from(crate::cast_udf::IntCastUdf::new()));
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

    /// Regression: regexp_extract() must accept Utf8View input columns.
    /// Before the fix, Utf8View was not in the signature and would cause a
    /// type-mismatch error at planning time.
    #[test]
    fn test_regexp_extract_utf8view_input() {
        use arrow::array::StringViewArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "msg",
            DataType::Utf8View,
            true,
        )]));
        let msgs: Arc<dyn Array> = Arc::new(StringViewArray::from(vec![
            Some("status=200 user=alice"),
            Some("status=500 user=bob"),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![msgs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT regexp_extract(msg, 'status=(\\d+)', 1) AS status FROM logs",
        ));

        let status = result
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(status.value(0), "200");
        assert_eq!(status.value(1), "500");
        assert!(status.is_null(2));
    }

    #[test]
    fn test_regexp_extract_largeutf8_input() {
        use arrow::array::LargeStringArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "msg",
            DataType::LargeUtf8,
            true,
        )]));
        let msgs: Arc<dyn Array> = Arc::new(LargeStringArray::from(vec![
            Some("status=201 user=carol"),
            Some("status=404 user=dave"),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![msgs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT regexp_extract(msg, 'status=(\\d+)', 1) AS status FROM logs",
        ));

        let status = result
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(status.value(0), "201");
        assert_eq!(status.value(1), "404");
        assert!(status.is_null(2));
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

    /// Regression (#1891): NULL pattern must return NULL, not compile "NULL" as regex.
    #[test]
    fn test_null_pattern_returns_null() {
        let batch = make_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT regexp_extract(msg, CAST(NULL AS VARCHAR), 1) AS extracted FROM logs",
        ));
        let col = result.column_by_name("extracted").unwrap();
        for row in 0..result.num_rows() {
            assert!(
                col.is_null(row),
                "row {row}: NULL pattern must propagate NULL"
            );
        }
    }

    /// Regression (#1901): scalar input with boundary quotes must not be corrupted.
    #[test]
    fn test_scalar_input_preserves_boundary_quotes() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let msgs: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("dummy")]));
        let batch = RecordBatch::try_new(schema, vec![msgs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            r#"SELECT regexp_extract('"hello"', '(".*")', 1) AS extracted FROM logs"#,
        ));
        let col = result
            .column_by_name("extracted")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            col.value(0),
            "\"hello\"",
            "boundary quotes must be preserved"
        );
    }

    /// Regression (#1889): array group_index must be rejected.
    #[test]
    fn test_rejects_array_group_index() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("idx", DataType::Int64, true),
        ]));
        let msg: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("status=200")]));
        let idx: Arc<dyn Array> = Arc::new(Int64Array::from(vec![Some(1)]));
        let batch = RecordBatch::try_new(schema, vec![msg, idx]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt
            .block_on(run_sql_result(
                batch,
                "SELECT regexp_extract(msg, 'status=(\\d+)', idx) AS status FROM logs",
            ))
            .expect_err("array group_index must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("group_index argument must be a scalar integer literal"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_regex_cache_reuses_compiled_pattern() {
        let udf = RegexpExtractUdf::new();
        let first = udf
            .get_or_compile_regex(r"status=(\d+)")
            .expect("pattern should compile");
        let second = udf
            .get_or_compile_regex(r"status=(\d+)")
            .expect("pattern should be cached");
        assert!(
            Arc::ptr_eq(&first, &second),
            "same pattern must reuse compiled regex"
        );
    }

    #[test]
    fn test_regex_cache_evicts_lru_entry() {
        let udf = RegexpExtractUdf::new();
        for idx in 0..REGEXP_CACHE_CAPACITY {
            let pattern = format!("field{idx}=(\\d+)");
            udf.get_or_compile_regex(&pattern)
                .expect("pattern should compile");
        }

        // Touch field0 so field1 becomes LRU.
        udf.get_or_compile_regex("field0=(\\d+)")
            .expect("pattern should remain cached");
        udf.get_or_compile_regex("overflow=(\\d+)")
            .expect("overflow pattern should compile");

        let cache = udf.regex_cache.lock().expect("cache lock");
        assert_eq!(
            cache.entries.len(),
            REGEXP_CACHE_CAPACITY,
            "cache should remain bounded"
        );
        assert!(
            cache.entries.contains_key("field0=(\\d+)"),
            "most recently used entry should stay in cache"
        );
        assert!(
            !cache.entries.contains_key("field1=(\\d+)"),
            "least recently used entry should be evicted"
        );
    }
}
