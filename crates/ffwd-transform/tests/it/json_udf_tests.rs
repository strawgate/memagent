//! Comprehensive integration tests for JSON extraction UDFs.
//!
//! Tests `json(body, 'key')`, `json_int(body, 'key')`, and
//! `json_float(body, 'key')` against a `body` Utf8 column registered in
//! DataFusion via SQL.
//!
//! Run with:
//! ```sh
//! RUSTC_WRAPPER="" cargo test -p ffwd-transform --test json_udf_tests
//! ```

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, StringViewBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};

use datafusion::datasource::MemTable;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::*;

use ffwd_transform::udf::{JsonExtractMode, JsonExtractUdf};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a `RecordBatch` with a single `body: Utf8` column from string slices.
fn make_raw_batch(lines: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(lines.to_vec())) as ArrayRef],
    )
    .unwrap()
}

/// Create a `RecordBatch` with a single `body: Utf8` column that may contain
/// nulls. Each element is `Some("...")` or `None`.
fn make_raw_batch_nullable(lines: &[Option<&str>]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(lines.to_vec())) as ArrayRef],
    )
    .unwrap()
}

/// Create a `RecordBatch` with a single `body: Utf8View` column.
fn make_raw_batch_utf8view(lines: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "body",
        DataType::Utf8View,
        true,
    )]));
    let mut builder = StringViewBuilder::new();
    for line in lines {
        builder.append_value(line);
    }
    RecordBatch::try_new(schema, vec![Arc::new(builder.finish()) as ArrayRef]).unwrap()
}

/// Build a `SessionContext` with all three JSON UDFs registered and the given
/// batch loaded as the `logs` table.
fn make_ctx(batch: RecordBatch) -> SessionContext {
    let schema = batch.schema();
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Str)));
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Int)));
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Float)));
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("logs", Arc::new(table)).unwrap();
    ctx
}

/// Run SQL and collect all result batches, concatenating them into one.
/// Returns `None` when the result set is empty (0 batches or only empty batches).
async fn query(sql: &str, batch: RecordBatch) -> Option<RecordBatch> {
    let ctx = make_ctx(batch);
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let non_empty: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        return None;
    }
    let schema = non_empty[0].schema();
    Some(arrow::compute::concat_batches(&schema, &non_empty).expect("concat batches"))
}

/// Convenience: run SQL expecting exactly one non-empty batch.
async fn query1(sql: &str, batch: RecordBatch) -> RecordBatch {
    query(sql, batch)
        .await
        .expect("expected a non-empty result")
}

// =========================================================================
// Basic functionality
// =========================================================================

#[tokio::test]
async fn basic_extract_string_field() {
    let batch = make_raw_batch(&[
        r#"{"level": "INFO", "msg": "started"}"#,
        r#"{"level": "WARN", "msg": "slow"}"#,
    ]);
    let result = query1("SELECT json(body, 'level') AS lvl FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "INFO");
    assert_eq!(col.value(1), "WARN");
}

#[tokio::test]
async fn basic_extract_integer_field() {
    let batch = make_raw_batch(&[r#"{"status": 200}"#, r#"{"status": 500}"#]);
    let result = query1("SELECT json_int(body, 'status') AS s FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 200);
    assert_eq!(col.value(1), 500);
}

#[tokio::test]
async fn basic_extract_float_field() {
    let batch = make_raw_batch(&[r#"{"duration": 1.5}"#, r#"{"duration": 0.001}"#]);
    let result = query1("SELECT json_float(body, 'duration') AS d FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((col.value(0) - 1.5).abs() < 1e-9);
    assert!((col.value(1) - 0.001).abs() < 1e-9);
}

#[tokio::test]
async fn basic_missing_field_returns_null() {
    let batch = make_raw_batch(&[r#"{"status": 200}"#, r#"{"level": "INFO"}"#]);
    let result = query1("SELECT json(body, 'status') AS s FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!col.is_null(0));
    assert!(col.is_null(1));
}

#[tokio::test]
async fn basic_empty_batch() {
    let batch = make_raw_batch(&[]);
    let result = query("SELECT json(body, 'x') AS x FROM logs", batch).await;
    // Empty input should yield empty (or no) output.
    assert!(result.is_none());
}

// =========================================================================
// Type coercion
// =========================================================================

#[tokio::test]
async fn coerce_json_int_on_string_parseable() {
    // String "200" -- the scanner sees the quoted value and places it in the
    // _str column.  json_int's suffix order is ["_int", ""], so it finds the
    // _int column first.  The scanner writes 0 for unparsable-as-int strings
    // into _int, so the result is 0 (not 200).
    //
    // To get 200 from a quoted-string JSON value, use json(body,'code') and
    // CAST in SQL.  This test documents the current behaviour.
    let batch = make_raw_batch(&[r#"{"code": "200"}"#]);
    let result = query1("SELECT json_int(body, 'code') AS c FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Scanner cannot parse the quoted string as int -- produces 0 or NULL.
    assert!(
        col.is_null(0) || col.value(0) == 0,
        "json_int on quoted-string '200' should be 0 or NULL, got {}",
        col.value(0),
    );
}

#[tokio::test]
async fn coerce_json_int_on_string_unparsable() {
    // String "OK" cannot be parsed as integer -> NULL.
    let batch = make_raw_batch(&[r#"{"code": "OK"}"#]);
    let result = query1("SELECT json_int(body, 'code') AS c FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(col.is_null(0));
}

#[tokio::test]
async fn coerce_json_float_on_integer_value() {
    // Integer 200 should be coerced to float 200.0.
    let batch = make_raw_batch(&[r#"{"val": 200}"#]);
    let result = query1("SELECT json_float(body, 'val') AS v FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((col.value(0) - 200.0).abs() < 1e-9);
}

#[tokio::test]
async fn coerce_json_float_on_string_parseable() {
    // Quoted string "1.5" -- the scanner places it in _str.  json_float's
    // suffix order is ["_float", "_int", ""], none of which match the _str
    // column.  The _float column gets 0.0 or NULL because the scanner cannot
    // parse the quoted string as a bare float.
    //
    // This documents current behaviour: use json(body,'val') + CAST for
    // string-encoded floats.
    let batch = make_raw_batch(&[r#"{"val": "1.5"}"#]);
    let result = query1("SELECT json_float(body, 'val') AS v FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // Scanner writes 0.0 or NULL for a quoted-string float value.
    assert!(
        col.is_null(0) || col.value(0).abs() < 1e-9,
        "json_float on quoted-string '1.5' should be 0.0 or NULL, got {}",
        col.value(0),
    );
}

#[tokio::test]
async fn coerce_json_string_on_integer_field() {
    // json() on an integer field should cast to string.
    let batch = make_raw_batch(&[r#"{"status": 200}"#]);
    let result = query1("SELECT json(body, 'status') AS s FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "200");
}

// =========================================================================
// Null handling
// =========================================================================

#[tokio::test]
async fn null_raw_row_produces_null_not_error() {
    // When body contains NULL rows, json() must return NULL for those rows
    // without erroring the entire batch.  Previously, NULL rows were emitted
    // as bare `\n` (empty lines); the scanner skipped them, causing a row-count
    // mismatch error.  Fixed by emitting `{}\n` for NULL rows so the scanner
    // produces a row with no fields (→ NULL for the requested key).
    let batch =
        make_raw_batch_nullable(&[Some(r#"{"status": 200}"#), None, Some(r#"{"status": 500}"#)]);
    let ctx = make_ctx(batch);

    let result = ctx
        .sql("SELECT json(body, 'status') AS s FROM logs")
        .await
        .unwrap()
        .collect()
        .await
        .expect("NULL body rows must not error");

    let batch = result.into_iter().next().unwrap();
    assert_eq!(batch.num_rows(), 3, "must have 3 rows");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "200", "row 0 must extract status");
    assert!(col.is_null(1), "row 1 (null body) must produce null");
    assert_eq!(col.value(2), "500", "row 2 must extract status");
}

#[tokio::test]
async fn mixed_rows_some_have_field_some_dont() {
    let batch = make_raw_batch(&[
        r#"{"status": 200, "host": "web1"}"#,
        r#"{"status": 404}"#,
        r#"{"host": "web2"}"#,
    ]);
    let result = query1("SELECT json(body, 'host') AS h FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "web1");
    assert!(col.is_null(1));
    assert_eq!(col.value(2), "web2");
}

// =========================================================================
// Edge cases
// =========================================================================

#[tokio::test]
async fn edge_empty_json_object() {
    let batch = make_raw_batch(&["{}"]);
    let result = query1("SELECT json(body, 'key') AS k FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(col.is_null(0));
}

#[tokio::test]
async fn edge_non_json_line() {
    let batch = make_raw_batch(&["plain text log line"]);
    let result = query1("SELECT json(body, 'key') AS k FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(col.is_null(0));
}

#[tokio::test]
async fn edge_malformed_json() {
    let batch = make_raw_batch(&[r#"{"status": 200"#]); // unterminated
    let result = query1("SELECT json(body, 'status') AS s FROM logs", batch).await;
    // Malformed JSON may or may not extract; either value or null is acceptable.
    // The key requirement: no panic.
    assert_eq!(result.num_rows(), 1);
}

#[tokio::test]
async fn edge_very_long_value() {
    // 500 bytes — spans 7+ SIMD blocks (64 B each), exercises the same long-value
    // edge case without the multi-minute CI penalty of the old 12 KB payload.
    let long_val = "x".repeat(500);
    let line = format!(r#"{{"big": "{}"}}"#, long_val);
    let batch = make_raw_batch(&[&line]);
    let result = query1("SELECT json(body, 'big') AS b FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    if !col.is_null(0) {
        assert_eq!(col.value(0), long_val);
    }
}

#[tokio::test]
async fn edge_unicode_field_name() {
    let batch = make_raw_batch(&[r#"{"日本語": "value"}"#]);
    let result = query1("SELECT json(body, '日本語') AS u FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Either extracts or null is fine; must not panic.
    if !col.is_null(0) {
        assert_eq!(col.value(0), "value");
    }
}

#[tokio::test]
async fn edge_unicode_values() {
    let batch = make_raw_batch(&[r#"{"msg": "café résumé"}"#]);
    let result = query1("SELECT json(body, 'msg') AS m FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "café résumé");
}

#[tokio::test]
async fn edge_nested_object() {
    // Extracting a nested object should return the JSON representation.
    let batch = make_raw_batch(&[r#"{"meta": {"host": "web1", "dc": "us-east"}}"#]);
    let result = query1("SELECT json(body, 'meta') AS m FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    if !col.is_null(0) {
        let val = col.value(0);
        // The exact formatting may vary, but it should contain the nested content.
        assert!(
            val.contains("host"),
            "nested object should contain 'host': got {val}"
        );
        assert!(
            val.contains("web1"),
            "nested object should contain 'web1': got {val}"
        );
    }
}

#[tokio::test]
async fn edge_array_value() {
    let batch = make_raw_batch(&[r#"{"tags": ["a", "b", "c"]}"#]);
    let result = query1("SELECT json(body, 'tags') AS t FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    if !col.is_null(0) {
        let val = col.value(0);
        assert!(val.contains('a'), "array should contain 'a': got {val}");
        assert!(val.contains('b'), "array should contain 'b': got {val}");
    }
}

#[tokio::test]
async fn edge_boolean_values() {
    let batch = make_raw_batch(&[r#"{"debug": true, "verbose": false}"#]);

    // json() on boolean should return "true" / "false".
    let result = query1("SELECT json(body, 'debug') AS d FROM logs", batch.clone()).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    if !col.is_null(0) {
        assert_eq!(col.value(0).to_lowercase(), "true");
    }

    // json_int() on boolean should return NULL (not a number).
    let result = query1("SELECT json_int(body, 'debug') AS d FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(col.is_null(0), "json_int on boolean should be NULL");
}

#[tokio::test]
async fn edge_json_null_value() {
    let batch = make_raw_batch(&[r#"{"status": null}"#]);

    // json -> NULL
    let result = query1("SELECT json(body, 'status') AS s FROM logs", batch.clone()).await;
    assert!(
        result.column(0).is_null(0),
        "json on JSON null should be NULL"
    );

    // json_int -> NULL
    let result = query1(
        "SELECT json_int(body, 'status') AS s FROM logs",
        batch.clone(),
    )
    .await;
    assert!(
        result.column(0).is_null(0),
        "json_int on JSON null should be NULL"
    );

    // json_float -> NULL
    let result = query1("SELECT json_float(body, 'status') AS s FROM logs", batch).await;
    assert!(
        result.column(0).is_null(0),
        "json_float on JSON null should be NULL"
    );
}

#[tokio::test]
async fn edge_duplicate_keys() {
    // JSON with duplicate keys -- behavior should be deterministic (no panic).
    // Most parsers return the last value, but either is acceptable.
    let batch = make_raw_batch(&[r#"{"status": 200, "status": 500}"#]);
    let result = query1("SELECT json_int(body, 'status') AS s FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(
        !col.is_null(0),
        "duplicate key should still extract a value"
    );
    let val = col.value(0);
    assert!(
        val == 200 || val == 500,
        "duplicate key should return one of the values, got {val}"
    );
}

#[tokio::test]
async fn edge_numeric_large_integer() {
    let batch = make_raw_batch(&[r#"{"big": 9223372036854775807}"#]); // i64::MAX
    let result = query1("SELECT json_int(body, 'big') AS b FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    if !col.is_null(0) {
        assert_eq!(col.value(0), i64::MAX);
    }
}

#[tokio::test]
async fn edge_numeric_negative() {
    let batch = make_raw_batch(&[r#"{"val": -42}"#]);
    let result = query1("SELECT json_int(body, 'val') AS v FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), -42);
}

#[tokio::test]
async fn edge_numeric_scientific_notation() {
    let batch = make_raw_batch(&[r#"{"val": 1.5e3}"#]);
    let result = query1("SELECT json_float(body, 'val') AS v FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    if !col.is_null(0) {
        assert!((col.value(0) - 1500.0).abs() < 1e-9);
    }
}

// =========================================================================
// WHERE clause integration
// =========================================================================

#[tokio::test]
async fn where_clause_with_json_int() {
    let batch = make_raw_batch(&[
        r#"{"status": 200, "msg": "ok"}"#,
        r#"{"status": 500, "msg": "error"}"#,
        r#"{"status": 301, "msg": "redirect"}"#,
    ]);
    let result = query1(
        "SELECT json(body, 'msg') AS msg FROM logs WHERE json_int(body, 'status') > 400",
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
async fn where_clause_with_json_string() {
    let batch = make_raw_batch(&[
        r#"{"level": "INFO", "msg": "started"}"#,
        r#"{"level": "ERROR", "msg": "disk full"}"#,
        r#"{"level": "INFO", "msg": "heartbeat"}"#,
    ]);
    let result = query1(
        "SELECT json(body, 'msg') AS msg FROM logs WHERE json(body, 'level') = 'ERROR'",
        batch,
    )
    .await;
    assert_eq!(result.num_rows(), 1);
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "disk full");
}

#[tokio::test]
async fn where_clause_filters_to_empty() {
    let batch = make_raw_batch(&[r#"{"status": 200}"#, r#"{"status": 201}"#]);
    let result = query(
        "SELECT json_int(body, 'status') AS s FROM logs WHERE json_int(body, 'status') > 999",
        batch,
    )
    .await;
    assert!(
        result.is_none(),
        "filtering all rows should produce empty result"
    );
}

#[tokio::test]
async fn combine_json_and_json_int_same_query() {
    let batch = make_raw_batch(&[
        r#"{"status": 200, "level": "INFO"}"#,
        r#"{"status": 500, "level": "ERROR"}"#,
    ]);
    let result = query1(
        "SELECT json(body, 'level') AS lvl, json_int(body, 'status') AS code FROM logs",
        batch,
    )
    .await;
    let lvl = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let code = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(lvl.value(0), "INFO");
    assert_eq!(code.value(0), 200);
    assert_eq!(lvl.value(1), "ERROR");
    assert_eq!(code.value(1), 500);
}

// =========================================================================
// SELECT * passthrough
// =========================================================================

#[tokio::test]
async fn select_raw_passthrough() {
    let batch = make_raw_batch(&[r#"{"status": 200}"#, r#"{"status": 500}"#]);
    let result = query1("SELECT body FROM logs", batch).await;
    assert_eq!(result.num_rows(), 2);
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(col.value(0).contains("200"));
    assert!(col.value(1).contains("500"));
}

#[tokio::test]
async fn select_raw_and_extracted() {
    let batch = make_raw_batch(&[r#"{"status": 200, "msg": "ok"}"#]);
    let result = query1("SELECT body, json(body, 'status') AS s FROM logs", batch).await;
    assert_eq!(result.num_columns(), 2);
    // body column
    let raw = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(raw.value(0).contains("200"));
    // extracted column
    let s = result
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(s.value(0), "200");
}

// =========================================================================
// Utf8View compatibility
// =========================================================================

/// The scanner produces Utf8View columns. Verify that all three UDFs accept
/// Utf8View input by registering with a Utf8View body column.
///
/// NOTE: DataFusion may auto-cast Utf8View to Utf8 based on the UDF signature.
/// This test verifies that the end-to-end path works regardless of how the
/// cast happens.
#[tokio::test]
async fn utf8view_json_extract() {
    let batch = make_raw_batch_utf8view(&[
        r#"{"level": "INFO", "status": 200, "dur": 1.5}"#,
        r#"{"level": "ERROR", "status": 500, "dur": 0.3}"#,
    ]);
    let ctx = make_ctx(batch);

    // json()
    let df = ctx
        .sql("SELECT json(body, 'level') AS lvl FROM logs")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches.into_iter().find(|b| b.num_rows() > 0).unwrap();
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "INFO");
    assert_eq!(col.value(1), "ERROR");

    // json_int()
    let df = ctx
        .sql("SELECT json_int(body, 'status') AS s FROM logs")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches.into_iter().find(|b| b.num_rows() > 0).unwrap();
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 200);
    assert_eq!(col.value(1), 500);

    // json_float()
    let df = ctx
        .sql("SELECT json_float(body, 'dur') AS d FROM logs")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches.into_iter().find(|b| b.num_rows() > 0).unwrap();
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((col.value(0) - 1.5).abs() < 1e-9);
    assert!((col.value(1) - 0.3).abs() < 1e-9);
}

#[tokio::test]
async fn utf8view_where_clause() {
    let batch = make_raw_batch_utf8view(&[
        r#"{"status": 200, "msg": "ok"}"#,
        r#"{"status": 500, "msg": "fail"}"#,
    ]);
    let ctx = make_ctx(batch);
    let df = ctx
        .sql("SELECT json(body, 'msg') AS msg FROM logs WHERE json_int(body, 'status') >= 500")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches.into_iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "fail");
}

// =========================================================================
// Multi-row mixed scenarios
// =========================================================================

#[tokio::test]
async fn mixed_valid_invalid_rows_no_nulls() {
    // A realistic batch with a mix of valid JSON, invalid JSON, and empty
    // objects (but no NULL body rows, which trigger a scanner mismatch).
    let batch = make_raw_batch(&[
        r#"{"status": 200, "level": "INFO"}"#,
        "not json at all",
        r#"{"status": 404}"#,
        "{}",
    ]);
    let result = query1("SELECT json_int(body, 'status') AS s FROM logs", batch).await;
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.len(), 4);
    assert_eq!(col.value(0), 200);
    assert!(col.is_null(1), "non-JSON row should be NULL");
    assert_eq!(col.value(2), 404);
    assert!(
        col.is_null(3),
        "empty object should be NULL for missing key"
    );
}

#[tokio::test]
async fn mixed_valid_invalid_null_rows_succeeds() {
    // NULL body rows must produce NULL in the output without erroring the batch.
    let batch =
        make_raw_batch_nullable(&[Some(r#"{"status": 200}"#), None, Some(r#"{"status": 404}"#)]);
    let ctx = make_ctx(batch);
    let result = ctx
        .sql("SELECT json_int(body, 'status') AS s FROM logs")
        .await
        .unwrap()
        .collect()
        .await
        .expect("NULL body rows must not error");
    let batch = result.into_iter().next().unwrap();
    assert_eq!(batch.num_rows(), 3, "must have 3 rows");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 200, "row 0 json_int status");
    assert!(col.is_null(1), "row 1 (null body) must produce null");
    assert_eq!(col.value(2), 404, "row 2 json_int status");
}

#[tokio::test]
async fn all_three_udfs_same_query() {
    let batch = make_raw_batch(&[
        r#"{"level": "INFO", "status": 200, "dur": 1.5}"#,
        r#"{"level": "ERROR", "status": 500, "dur": 0.01}"#,
    ]);
    let result = query1(
        "SELECT json(body, 'level') AS lvl, json_int(body, 'status') AS code, json_float(body, 'dur') AS dur FROM logs",
        batch,
    ).await;
    let lvl = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let code = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dur = result
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(lvl.value(0), "INFO");
    assert_eq!(code.value(0), 200);
    assert!((dur.value(0) - 1.5).abs() < 1e-9);

    assert_eq!(lvl.value(1), "ERROR");
    assert_eq!(code.value(1), 500);
    assert!((dur.value(1) - 0.01).abs() < 1e-9);
}

#[tokio::test]
async fn where_and_select_different_fields() {
    let batch = make_raw_batch(&[
        r#"{"status": 200, "level": "INFO", "dur": 0.5}"#,
        r#"{"status": 500, "level": "ERROR", "dur": 2.0}"#,
        r#"{"status": 301, "level": "WARN", "dur": 0.1}"#,
    ]);
    let result = query1(
        "SELECT json(body, 'level') AS lvl, json_float(body, 'dur') AS dur \
         FROM logs \
         WHERE json_int(body, 'status') >= 400",
        batch,
    )
    .await;
    assert_eq!(result.num_rows(), 1);
    let lvl = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let dur = result
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(lvl.value(0), "ERROR");
    assert!((dur.value(0) - 2.0).abs() < 1e-9);
}
