//! UDF: grok(string, pattern) -> Struct
//!
//! Logstash-style grok pattern extraction. Expands grok patterns like
//! `%{PATTERN:name}` into named regex capture groups, then returns a Struct
//! with one field per named capture.
//!
//! ```sql
//! -- Extract structured fields from access logs
//! SELECT grok(message_str, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}')
//! FROM logs
//!
//! -- Access individual fields
//! SELECT grok(message_str, '%{IP:client} %{NUMBER:duration}').client AS client_ip
//! FROM logs
//!
//! -- Compose with int()/float() for type conversion
//! SELECT int(grok(message_str, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}').status) AS code
//! FROM logs
//! ```
//!
//! Built-in patterns: IP, IPV4, IPV6, NUMBER, INT, BASE10NUM, WORD, NOTSPACE,
//! SPACE, DATA, GREEDYDATA, QUOTEDSTRING, UUID, MAC, URIPATH, URIPATHPARAM,
//! URI, TIMESTAMP_ISO8601, DATE, TIME, LOGLEVEL, HOSTNAME, EMAILADDRESS.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field, Fields};

use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ---------------------------------------------------------------------------
// Grok pattern compilation (via grok crate)
// ---------------------------------------------------------------------------

/// A compiled grok pattern: the grok Pattern for matching + ordered field names
/// for Arrow schema construction.
#[derive(Debug)]
struct CompiledGrok {
    pattern: grok::Pattern,
    field_names: Vec<String>,
}

/// Compile a grok pattern using the `grok` crate's built-in pattern library.
fn compile_grok(pattern: &str) -> Result<CompiledGrok, crate::TransformError> {
    // alias_only=true: only include user-named captures (%{PATTERN:name}) in match
    // results, not internal pattern group names. Matches VRL/Tremor usage.
    let grok = grok::Grok::with_default_patterns();
    let compiled = grok
        .compile(pattern, true)
        .map_err(|e| crate::TransformError::Sql(format!("grok pattern compilation failed: {e}")))?;

    // Derive field names from the compiled pattern's capture groups.
    // With alias_only=true, this returns only user-named captures.
    // Order is deterministic (alphabetical from BTreeMap internals).
    let field_names: Vec<String> = compiled.capture_names().map(str::to_owned).collect();

    Ok(CompiledGrok {
        pattern: compiled,
        field_names,
    })
}

// ---------------------------------------------------------------------------
// GrokUdf
// ---------------------------------------------------------------------------

/// UDF: grok(string, pattern) -> Struct<field1: Utf8, field2: Utf8, ...>
///
/// The pattern uses Logstash-style `%{PATTERN:name}` syntax. The return type
/// is a Struct with one Utf8 field per named capture group.
#[derive(Debug)]
pub struct GrokUdf {
    signature: Signature,
    /// Per-pattern grok cache.  DataFusion shares the same `ScalarUDFImpl`
    /// instance across all `grok(...)` expressions in a query, so a single-slot
    /// `OnceLock` would cache the first pattern and silently apply it to all
    /// calls with different patterns.  Keying by pattern string handles multiple
    /// patterns correctly while still avoiding recompilation across batches.
    grok_cache: Mutex<HashMap<String, Arc<CompiledGrok>>>,
}

impl Default for GrokUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl GrokUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8View]),
                ]),
                Volatility::Immutable,
            ),
            grok_cache: Mutex::new(HashMap::new()),
        }
    }
}

impl ScalarUDFImpl for GrokUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "grok"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        // We can't know the struct fields without seeing the pattern.
        // Return a placeholder — DataFusion calls return_type_from_args for actual planning.
        // For now return Utf8 as fallback; the real work is in return_type_from_args.
        Ok(DataType::Utf8)
    }

    /// Determine the return type at planning time based on the grok pattern.
    ///
    /// If the pattern argument is a string literal, compiles the grok pattern
    /// and returns a nullable Struct type with one Utf8 field per named capture
    /// group. Falls back to nullable Utf8 if the pattern is not a literal or
    /// compilation fails.
    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> DfResult<arrow::datatypes::FieldRef> {
        // If the pattern argument is a literal, extract field names and return Struct type.
        if args.scalar_arguments.len() >= 2
            && let Some(
                datafusion::common::ScalarValue::Utf8(Some(pattern_str))
                | datafusion::common::ScalarValue::Utf8View(Some(pattern_str))
                | datafusion::common::ScalarValue::LargeUtf8(Some(pattern_str)),
            ) = args.scalar_arguments[1]
            && let Ok(compiled) = compile_grok(pattern_str)
        {
            let fields: Vec<Field> = compiled
                .field_names
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect();
            if !fields.is_empty() {
                return Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::Struct(Fields::from(fields)),
                    true,
                )));
            }
        }
        // Fallback: can't determine struct fields, return Utf8
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "grok() expects exactly two arguments".to_string(),
            ));
        }

        let input = &args.args[0];
        let pattern = &args.args[1];

        // Extract pattern string.
        let pattern_str = match pattern {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8View(Some(s)))
            | ColumnarValue::Scalar(datafusion::common::ScalarValue::LargeUtf8(Some(s))) => {
                s.clone()
            }
            ColumnarValue::Scalar(s) => {
                let s = s.to_string();
                s.trim_matches('"').trim_matches('\'').to_string()
            }
            ColumnarValue::Array(_) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "grok() pattern argument must be a scalar string literal".to_string(),
                ));
            }
        };

        // Get or compile the grok pattern, keyed by pattern string.  Each
        // distinct pattern gets its own cache entry so that multiple grok(...)
        // calls with different patterns in the same SQL query each use the
        // correct compiled pattern.
        let compiled: Arc<CompiledGrok> = {
            let mut cache = self.grok_cache.lock().map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "grok() internal cache lock poisoned".to_string(),
                )
            })?;
            if let Some(cached) = cache.get(&pattern_str) {
                Arc::clone(cached)
            } else {
                let new_compiled = compile_grok(&pattern_str).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("grok: {e}"))
                })?;
                let arc = Arc::new(new_compiled);
                cache.insert(pattern_str, Arc::clone(&arc));
                arc
            }
        };

        match input {
            ColumnarValue::Array(array) => {
                let num_rows = array.len();

                // Build one StringBuilder per capture group.
                let mut builders: Vec<StringBuilder> = compiled
                    .field_names
                    .iter()
                    .map(|_| StringBuilder::with_capacity(num_rows, num_rows * 32))
                    .collect();

                match array.data_type() {
                    DataType::Utf8 => {
                        let strings = array.as_string::<i32>();
                        for row in 0..num_rows {
                            if strings.is_null(row) {
                                for b in &mut builders {
                                    b.append_null();
                                }
                                continue;
                            }
                            match compiled.pattern.match_against(strings.value(row)) {
                                Some(matches) => {
                                    for (i, name) in compiled.field_names.iter().enumerate() {
                                        match matches.get(name) {
                                            Some(v) => builders[i].append_value(v),
                                            None => builders[i].append_null(),
                                        }
                                    }
                                }
                                None => {
                                    for b in &mut builders {
                                        b.append_null();
                                    }
                                }
                            }
                        }
                    }
                    DataType::Utf8View => {
                        let strings = array.as_string_view();
                        for row in 0..num_rows {
                            if strings.is_null(row) {
                                for b in &mut builders {
                                    b.append_null();
                                }
                                continue;
                            }
                            match compiled.pattern.match_against(strings.value(row)) {
                                Some(matches) => {
                                    for (i, name) in compiled.field_names.iter().enumerate() {
                                        match matches.get(name) {
                                            Some(v) => builders[i].append_value(v),
                                            None => builders[i].append_null(),
                                        }
                                    }
                                }
                                None => {
                                    for b in &mut builders {
                                        b.append_null();
                                    }
                                }
                            }
                        }
                    }
                    DataType::LargeUtf8 => {
                        let strings = array.as_string::<i64>();
                        for row in 0..num_rows {
                            if strings.is_null(row) {
                                for b in &mut builders {
                                    b.append_null();
                                }
                                continue;
                            }
                            match compiled.pattern.match_against(strings.value(row)) {
                                Some(matches) => {
                                    for (i, name) in compiled.field_names.iter().enumerate() {
                                        match matches.get(name) {
                                            Some(v) => builders[i].append_value(v),
                                            None => builders[i].append_null(),
                                        }
                                    }
                                }
                                None => {
                                    for b in &mut builders {
                                        b.append_null();
                                    }
                                }
                            }
                        }
                    }
                    other => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "grok() input must be Utf8/Utf8View/LargeUtf8, got {other:?}"
                        )));
                    }
                }

                // Build the struct array.
                let fields: Vec<Field> = compiled
                    .field_names
                    .iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect();
                let arrays: Vec<ArrayRef> = builders
                    .into_iter()
                    .map(|mut b| Arc::new(b.finish()) as ArrayRef)
                    .collect();

                let struct_array = StructArray::new(Fields::from(fields), arrays, None);
                Ok(ColumnarValue::Array(Arc::new(struct_array)))
            }
            ColumnarValue::Scalar(scalar) => {
                // Treat SQL NULL input the same as no match: return a Struct
                // with all-null fields. This avoids matching against "NULL" —
                // the string that ScalarValue::Utf8(None).to_string() produces.
                let raw = if scalar.is_null() {
                    None
                } else {
                    Some(scalar.to_string())
                };
                let matches = raw
                    .as_deref()
                    .map(|s| s.trim_matches('"').trim_matches('\''))
                    .and_then(|s| compiled.pattern.match_against(s));

                let fields: Vec<Field> = compiled
                    .field_names
                    .iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect();

                // NULL input → no match (all fields null), same as non-matching string.
                let values: Vec<datafusion::common::ScalarValue> = compiled
                    .field_names
                    .iter()
                    .map(|name| {
                        matches
                            .as_ref()
                            .and_then(|m| m.get(name))
                            .map_or(datafusion::common::ScalarValue::Utf8(None), |v| {
                                datafusion::common::ScalarValue::Utf8(Some(v.to_string()))
                            })
                    })
                    .collect();

                let fields_and_arrays = fields
                    .into_iter()
                    .zip(values)
                    .map(|(f, v)| Ok((Arc::new(f), v.to_array()? as ArrayRef)))
                    .collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;

                Ok(ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Struct(Arc::new(StructArray::from(
                        fields_and_arrays,
                    ))),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, StringArray};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::*;

    async fn run_sql(batch: RecordBatch, sql: &str) -> RecordBatch {
        let batches = run_sql_result(batch, sql).await.unwrap();
        batches.into_iter().next().unwrap()
    }

    async fn run_sql_result(
        batch: RecordBatch,
        sql: &str,
    ) -> datafusion::error::Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GrokUdf::new()));
        // Also register int() for composition tests
        ctx.register_udf(ScalarUDF::from(crate::udf::RegexpExtractUdf::new()));
        let table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("logs", Arc::new(table)).unwrap();
        let df = ctx.sql(sql).await?;
        df.collect().await
    }

    fn make_access_log_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let msgs: ArrayRef = Arc::new(StringArray::from(vec![
            Some("GET /api/users 200 15ms"),
            Some("POST /api/orders 500 230ms"),
            Some("no match here"),
            None,
        ]));
        RecordBatch::try_new(schema, vec![msgs]).unwrap()
    }

    #[test]
    fn test_compile_grok_basic() {
        let compiled = compile_grok("%{WORD:method} %{URIPATH:path} %{NUMBER:status}").unwrap();
        // capture_names() returns alphabetical order (BTreeMap internals)
        assert_eq!(compiled.field_names, vec!["method", "path", "status"]);
        // Verify this is also alphabetical (it happens to match declaration order here)
        let mut sorted = compiled.field_names.clone();
        sorted.sort();
        assert_eq!(compiled.field_names, sorted);
        assert!(
            compiled
                .pattern
                .match_against("GET /api/users 200")
                .is_some()
        );
    }

    #[test]
    fn test_compile_grok_unnamed() {
        let compiled = compile_grok("%{WORD} %{NUMBER:code}").unwrap();
        assert_eq!(compiled.field_names, vec!["code"]);
    }

    #[test]
    fn test_compile_grok_unknown_pattern() {
        let result = compile_grok("%{DOESNOTEXIST:foo}");
        assert!(result.is_err());
    }

    #[test]
    fn test_grok_struct_access() {
        let batch = make_access_log_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status} %{NUMBER:duration}ms') AS parsed FROM logs",
        ));

        // Result should have a struct column "parsed"
        assert_eq!(result.num_rows(), 4);
        let parsed = result.column_by_name("parsed").unwrap();
        let struct_arr = parsed.as_struct();

        let method = struct_arr
            .column_by_name("method")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(method.value(0), "GET");
        assert_eq!(method.value(1), "POST");
        assert!(method.is_null(2)); // no match
        assert!(method.is_null(3)); // NULL input

        let status = struct_arr
            .column_by_name("status")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(status.value(0), "200");
        assert_eq!(status.value(1), "500");
    }

    #[test]
    fn test_grok_dot_notation() {
        let batch = make_access_log_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT get_field(grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status} %{NUMBER:duration}ms'), 'method') AS http_method FROM logs",
        ));

        let method = result
            .column_by_name("http_method")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method.value(0), "GET");
        assert_eq!(method.value(1), "POST");
    }

    #[test]
    fn test_grok_ip_pattern() {
        let schema = Arc::new(Schema::new(vec![Field::new("log", DataType::Utf8, true)]));
        let logs: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Connection from 192.168.1.100 port 22"),
            Some("Request from 10.0.0.1 port 443"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![logs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT get_field(grok(log, '%{GREEDYDATA} from %{IPV4:ip} port %{INT:port}'), 'ip') AS client_ip FROM logs",
        ));

        let ip = result
            .column_by_name("client_ip")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ip.value(0), "192.168.1.100");
        assert_eq!(ip.value(1), "10.0.0.1");
    }

    /// Regression: grok(NULL literal, pattern) must return all-null struct fields,
    /// not attempt to match against the string "NULL".
    ///
    /// Before the fix, `ScalarValue::Utf8(None).to_string()` returned "NULL" and
    /// the pattern was applied to that string, producing spurious non-null captures
    /// for patterns that happen to match the string "NULL".
    #[test]
    fn test_null_literal_input_returns_all_null_fields() {
        let batch = make_access_log_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // CAST(NULL AS VARCHAR) forces a scalar Utf8(None) through the grok UDF.
        // %{GREEDYDATA:data} would match "NULL" if the bug were present.
        let result = rt.block_on(run_sql(
            batch,
            "SELECT get_field(grok(CAST(NULL AS VARCHAR), '%{WORD:method} %{URIPATH:path} %{NUMBER:status} %{NUMBER:duration}ms'), 'method') AS method FROM logs",
        ));
        let method = result
            .column_by_name("method")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..result.num_rows() {
            assert!(
                method.is_null(row),
                "row {row}: expected NULL but got '{}'  (bug: grok matched against \"NULL\")",
                method.value(row)
            );
        }
    }

    /// Regression: grok() must accept Utf8View input columns.
    /// Before the fix, Utf8View was not in the signature's OneOf list and the
    /// UDF would fail with a type-mismatch error.
    #[test]
    fn test_grok_utf8view_input() {
        use arrow::array::StringViewArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8View,
            true,
        )]));
        let msgs: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("GET /api/users 200"),
            Some("POST /api/orders 500"),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![msgs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT get_field(grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}'), 'method') AS http_method FROM logs",
        ));

        let method = result
            .column_by_name("http_method")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method.value(0), "GET");
        assert_eq!(method.value(1), "POST");
        assert!(method.is_null(2));
    }

    #[test]
    fn test_grok_largeutf8_input() {
        use arrow::array::LargeStringArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::LargeUtf8,
            true,
        )]));
        let msgs: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("GET /api/users 200"),
            Some("POST /api/orders 500"),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![msgs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT get_field(grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}'), 'method') AS http_method FROM logs",
        ));

        let method = result
            .column_by_name("http_method")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method.value(0), "GET");
        assert_eq!(method.value(1), "POST");
        assert!(method.is_null(2));
    }

    #[test]
    fn test_grok_rejects_array_pattern_argument() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("pattern", DataType::Utf8, true),
        ]));
        let message: ArrayRef = Arc::new(StringArray::from(vec![Some("GET /ok 200 1ms")]));
        let pattern: ArrayRef = Arc::new(StringArray::from(vec![Some("%{WORD:method}")]));
        let batch = RecordBatch::try_new(schema, vec![message, pattern]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt
            .block_on(run_sql_result(
                batch,
                "SELECT grok(message, pattern) AS parsed FROM logs",
            ))
            .expect_err("array pattern argument must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("pattern argument must be a scalar string literal"),
            "unexpected error: {msg}"
        );
    }
}
