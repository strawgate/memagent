use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use super::*;
use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, StringViewArray, UInt64Array,
};
use arrow::datatypes::{Field, Schema};
use logfwd_types::field_names;

/// Helper: build a simple test RecordBatch with bare-name columns matching
/// what the Phase-10 scanner emits for single-type fields.
fn make_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("msg", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let level: ArrayRef = Arc::new(StringArray::from(vec![
        Some("INFO"),
        Some("ERROR"),
        Some("DEBUG"),
        Some("ERROR"),
    ]));
    let msg: ArrayRef = Arc::new(StringArray::from(vec![
        Some("started"),
        Some("disk full"),
        Some("heartbeat"),
        Some("oom killed"),
    ]));
    let status: ArrayRef = Arc::new(StringArray::from(vec![
        Some("200"),
        Some("500"),
        Some("not_a_number"),
        Some("503"),
    ]));
    RecordBatch::try_new(schema, vec![level, msg, status]).unwrap()
}

fn make_source_metadata_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("msg", DataType::Utf8, true),
        Field::new(field_names::SOURCE_ID, DataType::UInt64, true),
        Field::new("file.path", DataType::Utf8, true),
    ]));
    let level: ArrayRef = Arc::new(StringArray::from(vec![Some("INFO"), Some("ERROR")]));
    let msg: ArrayRef = Arc::new(StringArray::from(vec![Some("started"), Some("failed")]));
    let source_id: ArrayRef = Arc::new(UInt64Array::from(vec![Some(1), Some(2)]));
    let source_path: ArrayRef = Arc::new(StringArray::from(vec![
        Some("/var/log/pods/a.log"),
        Some("/var/log/pods/b.log"),
    ]));
    RecordBatch::try_new(schema, vec![level, msg, source_id, source_path]).unwrap()
}

fn assert_u64_column_value(batch: &RecordBatch, name: &str, row: usize, expected: u64) {
    let values = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.value(row), expected);
}

fn assert_utf8_column_value(batch: &RecordBatch, name: &str, row: usize, expected: &str) {
    let values = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(row), expected);
}

#[test]
fn test_simple_passthrough() {
    let batch = make_test_batch();
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
    let result = transform.execute_blocking(batch.clone()).unwrap();
    assert_eq!(result.num_rows(), 4);
    assert_eq!(result.num_columns(), 3);
    // Verify data matches.
    let level = result
        .column_by_name("level")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(level.value(0), "INFO");
    assert_eq!(level.value(3), "ERROR");
}

#[test]
fn test_filter() {
    let batch = make_test_batch();
    let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 2);
    let level = result
        .column_by_name("level")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..result.num_rows() {
        assert_eq!(level.value(i), "ERROR");
    }
    let msg = result
        .column_by_name("msg")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(msg.value(0), "disk full");
    assert_eq!(msg.value(1), "oom killed");
}

#[test]
fn wildcard_filter_preserves_sql_visible_internal_metadata() {
    let batch = make_source_metadata_batch();
    let mut transform = SqlTransform::new("SELECT * FROM logs WHERE __source_id = 2").unwrap();
    let result = transform.execute_blocking(batch).unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_u64_column_value(&result, field_names::SOURCE_ID, 0, 2);
    assert_utf8_column_value(&result, "level", 0, "ERROR");
    assert_utf8_column_value(&result, "msg", 0, "failed");
}

#[test]
fn explicit_projection_keeps_source_metadata() {
    let batch = make_source_metadata_batch();
    let mut transform =
        SqlTransform::new("SELECT __source_id FROM logs WHERE level = 'ERROR'").unwrap();
    let result = transform.execute_blocking(batch).unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_u64_column_value(&result, field_names::SOURCE_ID, 0, 2);
}

#[test]
fn aliased_projection_keeps_source_metadata_value_without_reserved_name() {
    let batch = make_source_metadata_batch();
    let mut transform =
        SqlTransform::new("SELECT *, __source_id AS source_id FROM logs WHERE level = 'ERROR'")
            .unwrap();
    let result = transform.execute_blocking(batch).unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_u64_column_value(&result, field_names::SOURCE_ID, 0, 2);
    assert_u64_column_value(&result, "source_id", 0, 2);
}

#[test]
fn test_except() {
    let batch = make_test_batch();
    let mut transform = SqlTransform::new("SELECT * EXCEPT (status) FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);
    // status should be removed.
    assert!(result.column_by_name("status").is_none());
    // Other columns should remain.
    assert!(result.column_by_name("level").is_some());
    assert!(result.column_by_name("msg").is_some());
}

#[test]
fn test_computed() {
    let batch = make_test_batch();
    let mut transform = SqlTransform::new("SELECT *, 'prod' AS env FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);
    let env = result
        .column_by_name("env")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..4 {
        assert_eq!(env.value(i), "prod");
    }
}

#[test]
fn test_int_udf() {
    let batch = make_test_batch();
    let mut transform = SqlTransform::new("SELECT int(status) AS status_int FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);
    let status = result
        .column_by_name("status_int")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(status.value(0), 200);
    assert_eq!(status.value(1), 500);
    assert!(status.is_null(2)); // "not_a_number" should be NULL
    assert_eq!(status.value(3), 503);
}

#[test]
fn test_schema_evolution() {
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

    // First batch: 2 columns.
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
    ]));
    let batch1 = RecordBatch::try_new(
        schema1,
        vec![
            Arc::new(StringArray::from(vec!["web1", "web2"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["INFO", "ERROR"])) as ArrayRef,
        ],
    )
    .unwrap();

    let result1 = transform.execute_blocking(batch1).unwrap();
    assert_eq!(result1.num_columns(), 2);
    assert_eq!(result1.num_rows(), 2);

    // Second batch: 3 columns (new field added).
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch2 = RecordBatch::try_new(
        schema2,
        vec![
            Arc::new(StringArray::from(vec!["web3"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["WARN"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["us-east-1"])) as ArrayRef,
        ],
    )
    .unwrap();

    let result2 = transform.execute_blocking(batch2).unwrap();
    assert_eq!(result2.num_columns(), 3);
    assert_eq!(result2.num_rows(), 1);
    assert!(result2.column_by_name("region").is_some());
}

#[test]
fn test_hash_udf() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        true,
    )]));
    let vals: ArrayRef = Arc::new(StringArray::from(vec![
        Some("trace123"),
        None,
        Some("trace123"),
        Some("other_trace"),
    ]));
    let batch = RecordBatch::try_new(schema, vec![vals]).unwrap();

    let mut transform = SqlTransform::new("SELECT hash(trace_id) AS h FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    let col = result
        .column_by_name("h")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert!(!col.is_null(0));
    assert!(col.is_null(1));
    assert!(!col.is_null(2));
    assert_eq!(col.value(0), col.value(2));
    assert_ne!(col.value(0), col.value(3));
}

#[test]
fn test_float_udf() {
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, true)]));
    let vals: ArrayRef = Arc::new(StringArray::from(vec![
        Some("3.25"),
        Some("not_float"),
        Some("2.125"),
    ]));
    let batch = RecordBatch::try_new(schema, vec![vals]).unwrap();

    let mut transform = SqlTransform::new("SELECT float(val) AS val_f FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    let col = result
        .column_by_name("val_f")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((col.value(0) - 3.25).abs() < 1e-10);
    assert!(col.is_null(1));
    assert!((col.value(2) - 2.125).abs() < 1e-10);
}

#[test]
fn test_int_udf_accepts_utf8view_and_preserves_invalid_and_null() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8View,
        true,
    )]));
    let status: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("200"),
        Some("not_a_number"),
        None,
        Some("503"),
    ]));
    let batch = RecordBatch::try_new(schema, vec![status]).unwrap();

    let mut transform = SqlTransform::new("SELECT int(status) AS status_int FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    let status_int = result
        .column_by_name("status_int")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(status_int.value(0), 200);
    assert!(status_int.is_null(1));
    assert!(status_int.is_null(2));
    assert_eq!(status_int.value(3), 503);
}

#[test]
fn test_int_udf_accepts_int64_without_string_coercion() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Int64,
        true,
    )]));
    let status: ArrayRef = Arc::new(Int64Array::from(vec![Some(200), Some(500), None]));
    let batch = RecordBatch::try_new(schema, vec![status]).unwrap();

    let mut transform = SqlTransform::new("SELECT int(status) AS status_int FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    let status_int = result
        .column_by_name("status_int")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(status_int.value(0), 200);
    assert_eq!(status_int.value(1), 500);
    assert!(status_int.is_null(2));
}

#[test]
fn test_float_udf_accepts_utf8view_and_preserves_invalid_and_null() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "val",
        DataType::Utf8View,
        true,
    )]));
    let val: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("3.25"),
        Some("not_float"),
        None,
        Some("2.125"),
    ]));
    let batch = RecordBatch::try_new(schema, vec![val]).unwrap();

    let mut transform = SqlTransform::new("SELECT float(val) AS val_f FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    let val_f = result
        .column_by_name("val_f")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((val_f.value(0) - 3.25).abs() < 1e-10);
    assert!(val_f.is_null(1));
    assert!(val_f.is_null(2));
    assert!((val_f.value(3) - 2.125).abs() < 1e-10);
}

#[test]
fn test_float_udf_accepts_float64_without_string_coercion() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "val",
        DataType::Float64,
        true,
    )]));
    let val: ArrayRef = Arc::new(Float64Array::from(vec![Some(3.25), Some(2.125), None]));
    let batch = RecordBatch::try_new(schema, vec![val]).unwrap();

    let mut transform = SqlTransform::new("SELECT float(val) AS val_f FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    let val_f = result
        .column_by_name("val_f")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((val_f.value(0) - 3.25).abs() < 1e-10);
    assert!((val_f.value(1) - 2.125).abs() < 1e-10);
    assert!(val_f.is_null(2));
}

#[test]
fn test_query_analyzer_column_refs() {
    let analyzer = QueryAnalyzer::new("SELECT level, msg FROM logs WHERE status = '500'").unwrap();
    assert!(!analyzer.uses_select_star);
    assert!(analyzer.referenced_columns.contains("level"));
    assert!(analyzer.referenced_columns.contains("msg"));
    assert!(analyzer.referenced_columns.contains("status"));
}

#[test]
fn test_query_analyzer_select_star() {
    let analyzer = QueryAnalyzer::new("SELECT * FROM logs").unwrap();
    assert!(analyzer.uses_select_star);
    assert!(analyzer.except_fields.is_empty());
}

#[test]
fn test_query_analyzer_except() {
    let analyzer = QueryAnalyzer::new("SELECT * EXCEPT (stack_trace) FROM logs").unwrap();
    assert!(analyzer.uses_select_star);
    assert_eq!(analyzer.except_fields, vec!["stack_trace"]);
}

#[test]
fn test_enrichment_cross_join() {
    use enrichment::StaticTable;

    let batch = make_test_batch();
    let mut transform =
        SqlTransform::new("SELECT logs.*, env.environment FROM logs CROSS JOIN env").unwrap();

    // Add a static enrichment table.
    let env_table = Arc::new(
        StaticTable::new(
            "env",
            &[("environment".to_string(), "production".to_string())],
        )
        .expect("valid labels"),
    );
    transform.add_enrichment_table(env_table).unwrap();

    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);

    // Should have original columns plus "environment".
    let env_col = result
        .column_by_name("environment")
        .expect("should have environment column")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..4 {
        assert_eq!(env_col.value(i), "production");
    }
}

#[test]
fn test_enrichment_unused_table_no_error() {
    use enrichment::StaticTable;

    let batch = make_test_batch();
    let table = Arc::new(
        StaticTable::new("unused", &[("key".to_string(), "val".to_string())])
            .expect("valid labels"),
    );

    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
    transform.add_enrichment_table(table).unwrap();

    // Enrichment table registered but not referenced in SQL — should not error.
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[test]
fn test_enrichment_empty_table_skipped() {
    use enrichment::K8sPathTable;

    let batch = make_test_batch();
    let k8s = Arc::new(K8sPathTable::new("k8s_pods"));
    // Not loaded — snapshot() returns None.

    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
    transform.add_enrichment_table(k8s).unwrap();

    // Should not error — empty table just skipped.
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);
}

// --- FilterHints predicate pushdown tests ---

/// Table-driven tests for simple severity/facility filter hints.
#[test]
fn test_filter_hints_simple_severity_and_facility() {
    // Simple severity-only cases
    let severity_cases: &[(&str, u8)] = &[
        ("SELECT * FROM logs WHERE severity <= 4", 4),
        ("SELECT * FROM logs WHERE severity < 5", 4), // < 5 becomes <= 4
        ("SELECT * FROM logs WHERE severity = 3", 3),
        ("SELECT * FROM logs WHERE 5 > severity", 4), // reversed comparison
        (
            "SELECT * FROM logs WHERE severity <= 4 AND severity <= 2",
            2,
        ), // tighter bound wins
    ];

    for (sql, expected_severity) in severity_cases {
        let a = QueryAnalyzer::new(sql).unwrap();
        let h = a.filter_hints();
        assert_eq!(
            h.max_severity,
            Some(*expected_severity),
            "severity mismatch for: {sql}"
        );
        assert!(h.facilities.is_none(), "no facilities expected for: {sql}");
    }

    // Simple facility-only case
    let a = QueryAnalyzer::new("SELECT * FROM logs WHERE facility = 16").unwrap();
    let h = a.filter_hints();
    assert!(h.max_severity.is_none());
    assert_eq!(h.facilities, Some(vec![16]));
}

#[test]
fn test_filter_hints_facility_in() {
    let a = QueryAnalyzer::new("SELECT * FROM logs WHERE facility IN (1, 4, 16)").unwrap();
    let h = a.filter_hints();
    let mut facs = h.facilities.unwrap();
    facs.sort_unstable();
    assert_eq!(facs, vec![1, 4, 16]);
}

#[test]
fn test_filter_hints_combined_and() {
    let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4 AND facility = 16").unwrap();
    let h = a.filter_hints();
    assert_eq!(h.max_severity, Some(4));
    assert_eq!(h.facilities, Some(vec![16]));
}

#[test]
fn test_filter_hints_or_not_pushed() {
    // OR with non-pushable predicate — should NOT push severity
    let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4 OR msg = 'error'").unwrap();
    let h = a.filter_hints();
    // The OR is at top level, not an AND chain — nothing should be pushed
    assert!(h.max_severity.is_none());
}

#[test]
fn test_filter_hints_no_where() {
    let a = QueryAnalyzer::new("SELECT * FROM logs").unwrap();
    let h = a.filter_hints();
    assert!(h.max_severity.is_none());
    assert!(h.facilities.is_none());
    assert!(h.wanted_fields.is_none()); // SELECT * = all fields
}

#[test]
fn test_filter_hints_disabled_for_set_operation_branches() {
    let a = QueryAnalyzer::new(
        "SELECT * FROM logs WHERE severity <= 2 UNION ALL SELECT * FROM logs WHERE severity <= 4",
    )
    .unwrap();
    let h = a.filter_hints();
    assert!(
        h.max_severity.is_none(),
        "set-operation branch predicates must not be pushed globally"
    );
}

#[test]
fn test_filter_hints_field_pushdown() {
    let a = QueryAnalyzer::new("SELECT hostname, message FROM logs WHERE severity <= 2").unwrap();
    let h = a.filter_hints();
    assert_eq!(h.max_severity, Some(2));
    let mut fields = h.wanted_fields.unwrap();
    fields.sort();
    assert!(fields.contains(&"hostname".to_string()));
    assert!(fields.contains(&"message".to_string()));
    assert!(fields.contains(&"severity".to_string())); // referenced in WHERE
}

#[test]
fn test_filter_hints_typed_column_stripped() {
    // With the struct conflict format there are no more `__int`/`__str` suffixed
    // columns. `strip_type_suffix` is a no-op. `severity__int` is an unrecognised
    // column name, so no severity pushdown fires.
    let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity__int <= 4").unwrap();
    let h = a.filter_hints();
    assert_eq!(h.max_severity, None);
}

#[test]
fn test_filter_hints_or_pushable_only_still_not_pushed() {
    // OR of only pushable predicates still blocks pushdown
    let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4 OR severity <= 2").unwrap();
    let h = a.filter_hints();
    assert!(h.max_severity.is_none());
}

#[test]
fn test_filter_hints_tighten_facility() {
    // Multiple facility constraints should intersect
    let a = QueryAnalyzer::new("SELECT * FROM logs WHERE facility IN (1, 4, 16) AND facility = 4")
        .unwrap();
    let h = a.filter_hints();
    assert_eq!(h.facilities, Some(vec![4])); // intersection
}

// -----------------------------------------------------------------------
// Multi-batch context caching tests
// -----------------------------------------------------------------------

/// Verify that cached SessionContext doesn't leak data between batches.
/// Batch 1 and batch 2 have different data — a WHERE filter on batch 2
/// should never return rows from batch 1.
#[test]
fn test_cached_context_no_data_leakage() {
    let mut transform = SqlTransform::new("SELECT host FROM logs WHERE host = 'web2'").unwrap();

    // Batch 1: only web1.
    let batch1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["web1", "web1"])) as ArrayRef],
    )
    .unwrap();

    let result1 = transform.execute_blocking(batch1).unwrap();
    assert_eq!(result1.num_rows(), 0, "batch 1 should match nothing");

    // Batch 2: has web2.
    let batch2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["web2", "web3"])) as ArrayRef],
    )
    .unwrap();

    let result2 = transform.execute_blocking(batch2).unwrap();
    assert_eq!(result2.num_rows(), 1, "batch 2 should match one row");

    // Batch 3: no web2 again — verify old data from batch 2 is gone.
    let batch3 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["web4"])) as ArrayRef],
    )
    .unwrap();

    let result3 = transform.execute_blocking(batch3).unwrap();
    assert_eq!(
        result3.num_rows(),
        0,
        "batch 3 should not contain leftover data from batch 2"
    );
}

/// Verify that many consecutive batches on the same SqlTransform work correctly.
/// This exercises the deregister/register cycle repeatedly.
#[test]
fn test_cached_context_many_batches() {
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));

    for i in 0..20 {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![i as i64])) as ArrayRef],
        )
        .unwrap();

        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 1);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), i as i64, "batch {i} has wrong value");
    }
}

// -----------------------------------------------------------------------
// CASE expression tests
// -----------------------------------------------------------------------

/// Verify that collect_column_refs correctly extracts column names from a
/// CASE expression so that scan_config() requests the right fields.
///
/// This is a regression test for the bug where CASE conditions and results
/// fell through to the catch-all arm and were silently ignored, causing the
/// scanner to never extract those fields and DataFusion to fail with
/// "column not found" errors at execution time.
#[test]
fn test_query_analyzer_case_column_refs() {
    let sql = "SELECT CASE \
                       WHEN level = 'ERROR' THEN 'high' \
                       WHEN level = 'WARN'  THEN 'medium' \
                       ELSE 'low' \
                   END AS severity FROM logs";
    let a = QueryAnalyzer::new(sql).unwrap();
    // `level` must be collected so scan_config requests it from the scanner.
    assert!(
        a.referenced_columns.contains("level"),
        "expected 'level' in referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// Verify that a CASE expression with column references in both WHEN
/// conditions and THEN results executes correctly end-to-end.
#[test]
fn test_case_expression_in_select() {
    let batch = make_test_batch();
    let mut transform = SqlTransform::new(
        "SELECT CASE \
                 WHEN level = 'ERROR' THEN 'high' \
                 WHEN level = 'WARN'  THEN 'medium' \
                 ELSE 'low' \
             END AS severity FROM logs",
    )
    .unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);

    let severity = result
        .column_by_name("severity")
        .expect("severity column must be present")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Row 0: level=INFO → 'low'
    assert_eq!(severity.value(0), "low");
    // Row 1: level=ERROR → 'high'
    assert_eq!(severity.value(1), "high");
    // Row 2: level=DEBUG → 'low'
    assert_eq!(severity.value(2), "low");
    // Row 3: level=ERROR → 'high'
    assert_eq!(severity.value(3), "high");
}

/// Verify that a searched CASE expression used in a WHERE clause executes
/// correctly end-to-end.  Column references inside the CASE conditions must
/// be resolved by DataFusion (i.e. the scanner must have been asked to
/// extract those columns via scan_config).
#[test]
fn test_case_expression_in_where() {
    let batch = make_test_batch();
    // Keep only rows where the CASE evaluates to 'high' (i.e. level=ERROR).
    let mut transform = SqlTransform::new(
        "SELECT msg FROM logs WHERE \
             CASE WHEN level = 'ERROR' THEN 'high' ELSE 'low' END = 'high'",
    )
    .unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 2);

    let msg = result
        .column_by_name("msg")
        .expect("msg column must be present")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(msg.value(0), "disk full");
    assert_eq!(msg.value(1), "oom killed");
}

/// Verify that collect_column_refs picks up column references from CASE
/// THEN results (not just WHEN conditions).
#[test]
fn test_query_analyzer_case_result_column_refs() {
    // The THEN clause references `msg` — it must appear in referenced_columns.
    let sql = "SELECT CASE WHEN level = 'ERROR' THEN msg ELSE 'ok' END AS out FROM logs";
    let a = QueryAnalyzer::new(sql).unwrap();
    assert!(
        a.referenced_columns.contains("level"),
        "expected 'level' in referenced_columns, got {:?}",
        a.referenced_columns
    );
    assert!(
        a.referenced_columns.contains("msg"),
        "expected 'msg' in referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// Verify that a CASE expression whose THEN clause yields a column value
/// executes correctly end-to-end.
#[test]
fn test_case_expression_result_is_column() {
    let batch = make_test_batch();
    // For ERROR rows return msg, otherwise return a literal.
    let mut transform = SqlTransform::new(
        "SELECT CASE WHEN level = 'ERROR' THEN msg ELSE 'ok' END AS out FROM logs",
    )
    .unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    assert_eq!(result.num_rows(), 4);

    let out = result
        .column_by_name("out")
        .expect("out column must be present")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Row 0: INFO → 'ok'
    assert_eq!(out.value(0), "ok");
    // Row 1: ERROR → msg = 'disk full'
    assert_eq!(out.value(1), "disk full");
    // Row 2: DEBUG → 'ok'
    assert_eq!(out.value(2), "ok");
    // Row 3: ERROR → msg = 'oom killed'
    assert_eq!(out.value(3), "oom killed");
}

// -----------------------------------------------------------------------
// Schema-change invalidation tests
// -----------------------------------------------------------------------

/// Regression test: schema change across batches must invalidate the cached
/// SessionContext so that the new batch's schema is planned correctly.
///
/// Before the fix, `schema_hash` was tracked but never used to clear
/// `self.ctx`, so the old plan referencing the previous schema was reused,
/// causing "column not found" errors or silent wrong results when new fields
/// appeared.
#[test]
fn test_schema_change_new_field_invalidates_cache() {
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

    // Batch 1: two columns.
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("msg", DataType::Utf8, true),
    ]));
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema1),
        vec![
            Arc::new(StringArray::from(vec!["INFO"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
        ],
    )
    .unwrap();
    let r1 = transform.execute_blocking(batch1).unwrap();
    assert_eq!(r1.num_rows(), 1);
    assert_eq!(r1.num_columns(), 2);

    // Batch 2: three columns (added "host").  Without the fix, the stale
    // SessionContext plan would fail or miss the new column.
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("msg", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, true),
    ]));
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema2),
        vec![
            Arc::new(StringArray::from(vec!["ERROR"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["disk full"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["web1"])) as ArrayRef,
        ],
    )
    .unwrap();
    let r2 = transform.execute_blocking(batch2).unwrap();
    assert_eq!(r2.num_rows(), 1);
    // All three columns must be present after schema change.
    assert_eq!(
        r2.num_columns(),
        3,
        "expected 3 columns after schema change"
    );
    let host = r2
        .column_by_name("host")
        .expect("'host' column must be present after schema change")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(host.value(0), "web1");
}

/// Schema changes across batches where column types change (e.g., a field
/// that was Int64 becomes Utf8) must also re-plan correctly.
#[test]
fn test_schema_change_type_conflict_invalidates_cache() {
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

    // Batch 1: "status" is Int64.
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
    ]));
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema1),
        vec![
            Arc::new(StringArray::from(vec!["INFO"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![200i64])) as ArrayRef,
        ],
    )
    .unwrap();
    let r1 = transform.execute_blocking(batch1).unwrap();
    assert_eq!(r1.num_rows(), 1);

    // Batch 2: "status" is now Utf8 (type conflict resolved differently).
    // Without the fix, DataFusion would reuse the Int64 plan against a Utf8
    // column and fail.
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema2),
        vec![
            Arc::new(StringArray::from(vec!["ERROR"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["not_a_number"])) as ArrayRef,
        ],
    )
    .unwrap();
    let r2 = transform.execute_blocking(batch2).unwrap();
    assert_eq!(r2.num_rows(), 1);
    let status = r2
        .column_by_name("status")
        .expect("'status' column must be present")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(status.value(0), "not_a_number");
}

// -----------------------------------------------------------------------
// Regression tests — #415: IS NULL / IS NOT NULL / LIKE / numeric IN list
// -----------------------------------------------------------------------

/// `WHERE status IS NULL` on a plain Utf8 batch.
/// collect_column_refs must collect "status" from an IsNull expression so
/// the scanner requests the field, and DataFusion must filter correctly.
#[test]
fn test_where_is_null() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        true,
    )]));
    let status: ArrayRef = Arc::new(StringArray::from(vec![Some("200"), None, Some("404")]));
    let batch = RecordBatch::try_new(schema, vec![status]).unwrap();

    let mut transform = SqlTransform::new("SELECT status FROM logs WHERE status IS NULL").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    // Only the null row should match.
    assert_eq!(result.num_rows(), 1);
}

/// `WHERE status IS NOT NULL` on a plain Utf8 batch.
#[test]
fn test_where_is_not_null() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        true,
    )]));
    let status: ArrayRef = Arc::new(StringArray::from(vec![Some("200"), None, Some("404")]));
    let batch = RecordBatch::try_new(schema, vec![status]).unwrap();

    let mut transform =
        SqlTransform::new("SELECT status FROM logs WHERE status IS NOT NULL").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    // The two non-null rows should match.
    assert_eq!(result.num_rows(), 2);
}

/// `WHERE level LIKE 'ERR%'` on a plain batch.
/// collect_column_refs must collect "level" from a Like expression.
#[test]
fn test_where_like() {
    let batch = make_test_batch();
    let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level LIKE 'ERR%'").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    // make_test_batch has two ERROR rows.
    assert_eq!(result.num_rows(), 2);
}

/// `WHERE int(status) IN (200, 503)` on a plain batch.
/// collect_column_refs must collect "status" from the InList expression's
/// function argument.
#[test]
fn test_where_int_in_list() {
    let batch = make_test_batch();
    let mut transform =
        SqlTransform::new("SELECT * FROM logs WHERE int(status) IN (200, 503)").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    // make_test_batch has status values "200", "500", "not_a_number", "503"
    // int(status) IN (200, 503) matches rows 0 and 3.
    assert_eq!(result.num_rows(), 2);
}

// -----------------------------------------------------------------------
// Regression tests — QueryAnalyzer column-ref collection for #415 forms
// -----------------------------------------------------------------------

/// Table-driven tests for QueryAnalyzer column-ref collection across expression forms.
#[test]
fn test_query_analyzer_column_refs_expression_forms() {
    // (sql, expected columns that must be in referenced_columns)
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT level FROM logs WHERE status IS NULL",
            &["status", "level"],
        ),
        ("SELECT * FROM logs WHERE level LIKE 'ERR%'", &["level"]),
        (
            "SELECT * FROM logs WHERE status IN ('200', '404')",
            &["status"],
        ),
        (
            "SELECT a.message FROM logs a JOIN logs b ON a.level = b.level LIMIT 5",
            &["message", "level"],
        ),
        (
            "SELECT level FROM logs UNION ALL SELECT severity FROM logs",
            &["level", "severity"],
        ),
        (
            "SELECT message FROM logs a JOIN logs b USING (level)",
            &["message", "level"],
        ),
        (
            "SELECT q.level FROM (SELECT level, status, host FROM logs WHERE status = '500' ORDER BY host) q",
            &["level", "status", "host"],
        ),
        (
            "SELECT l.message FROM logs l JOIN (SELECT level, service FROM logs WHERE service = 'api') d ON l.level = d.level",
            &["message", "level", "service"],
        ),
    ];

    for (sql, expected_cols) in cases {
        let a = QueryAnalyzer::new(sql).unwrap();
        for col in *expected_cols {
            assert!(
                a.referenced_columns.contains(*col),
                "SQL: {sql}\nExpected '{col}' in referenced_columns, got: {:?}",
                a.referenced_columns
            );
        }
        assert_eq!(
            a.referenced_columns.len(),
            expected_cols.len(),
            "SQL: {sql}\nExpected exactly {:?}, got: {:?}",
            expected_cols,
            a.referenced_columns
        );
    }
}

/// STRAIGHT_JOIN may not be supported — handle both parse success and expected error.
#[test]
fn test_query_analyzer_straight_join_on_column_refs() {
    let sql = "SELECT a.message FROM logs a STRAIGHT_JOIN logs b ON a.level = b.level";
    match QueryAnalyzer::new(sql) {
        Ok(a) => {
            assert!(
                a.referenced_columns.contains("message"),
                "SELECT column must be in referenced_columns"
            );
            assert!(
                a.referenced_columns.contains("level"),
                "STRAIGHT_JOIN ON column must be in referenced_columns"
            );
        }
        Err(err) => {
            assert!(
                err.to_string().contains("STRAIGHT_JOIN"),
                "unexpected parse error for STRAIGHT_JOIN: {err}"
            );
        }
    }
}

/// Table-function and TABLE() syntax: args must appear in referenced_columns.
#[test]
fn test_query_analyzer_table_function_args_column_refs() {
    let with_table_args = QueryAnalyzer::new("SELECT message FROM logs(level, host)").unwrap();
    assert!(
        with_table_args.referenced_columns.contains("level"),
        "table-args column must be in referenced_columns"
    );
    assert!(
        with_table_args.referenced_columns.contains("host"),
        "table-args column must be in referenced_columns"
    );

    let with_table_function =
        QueryAnalyzer::new("SELECT message FROM TABLE(my_tvf(level, host))").unwrap();
    assert!(
        with_table_function.referenced_columns.contains("level"),
        "table-function arg column must be in referenced_columns"
    );
    assert!(
        with_table_function.referenced_columns.contains("host"),
        "table-function arg column must be in referenced_columns"
    );
}

// -----------------------------------------------------------------------

/// Verify that a stable schema does NOT trigger repeated context recreation
/// (i.e. the hash comparison is correct and equal hashes are treated as
/// cache hits).
#[test]
fn test_stable_schema_does_not_invalidate_cache() {
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("msg", DataType::Utf8, true),
    ]));

    // Run 5 batches with the same schema; each must succeed and the context
    // must still be populated (not None) after each run.
    for i in 0u32..5 {
        let val = format!("row{i}");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![val.as_str()])) as ArrayRef,
                Arc::new(StringArray::from(vec!["msg"])) as ArrayRef,
            ],
        )
        .unwrap();
        let r = transform.execute_blocking(batch).unwrap();
        assert_eq!(r.num_rows(), 1, "batch {i} must return 1 row");
        // The context must still be alive (not wiped by a false hash mismatch).
        assert!(
            transform.ctx.is_some(),
            "ctx must remain populated for stable schema (batch {i})"
        );
    }
}

// -----------------------------------------------------------------------
// Bug #721: validate_plan catches SQL planning errors
// -----------------------------------------------------------------------

#[test]
fn validate_plan_accepts_valid_sql() {
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
    transform
        .validate_plan()
        .expect("valid SQL should pass plan validation");
}

#[test]
fn validate_plan_catches_duplicate_aliases() {
    // "SELECT level AS k, msg AS k FROM logs" produces a duplicate alias error
    // at DataFusion planning time (not at parse time).
    let mut transform = SqlTransform::new("SELECT level AS k, msg AS k FROM logs").unwrap();
    let err = transform.validate_plan();
    assert!(
        err.is_err(),
        "duplicate column alias should be caught by validate_plan"
    );
}

#[test]
fn validate_plan_catches_invalid_function() {
    // A non-existent function should fail during planning.
    let mut transform = SqlTransform::new("SELECT nonexistent_fn(level) FROM logs").unwrap();
    let err = transform.validate_plan();
    assert!(
        err.is_err(),
        "unknown function should be caught by validate_plan"
    );
}

#[test]
fn validate_plan_accepts_filter_query() {
    let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
    transform
        .validate_plan()
        .expect("filter query should pass plan validation");
}

#[test]
fn validate_plan_accepts_mixed_case_unquoted_identifier() {
    let mut transform = SqlTransform::new("SELECT Level FROM logs").unwrap();
    transform
        .validate_plan()
        .expect("unquoted identifiers should validate against scanner-normalized fields");
}

#[test]
fn validate_plan_uses_null_probe_values_for_cast_paths() {
    let mut transform =
        SqlTransform::new("SELECT int(status) FROM logs WHERE int(status) <= 4").unwrap();
    transform
        .validate_plan()
        .expect("cast-heavy query should validate with null probe values");
}

/// When a WHERE clause filters out every row, execute should return an
/// empty RecordBatch with the correct output schema — without calling
/// `ctx.sql()` a second time (the original bug).
#[test]
fn test_where_drops_all_rows_returns_correct_schema() {
    let batch = make_test_batch(); // has levels: INFO, ERROR, DEBUG, ERROR
    let mut transform =
        SqlTransform::new("SELECT level, msg FROM logs WHERE level = 'FATAL'").unwrap();
    let result = transform.execute_blocking(batch).unwrap();

    assert_eq!(result.num_rows(), 0, "no rows should match");
    assert_eq!(result.num_columns(), 2, "output should have 2 columns");
    assert_eq!(result.schema().field(0).name(), "level");
    assert_eq!(result.schema().field(1).name(), "msg");
}

#[test]
fn test_empty_input_batch_uses_transformed_output_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new("level", DataType::Utf8, true)]));
    let level: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
    let batch = RecordBatch::try_new(schema, vec![level]).unwrap();

    let mut transform = SqlTransform::new("SELECT level AS severity FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();

    assert_eq!(result.num_rows(), 0);
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.schema().field(0).name(), "severity");
}

#[test]
fn scan_config_never_forces_line_capture() {
    for sql in &[
        "SELECT * FROM logs",
        "SELECT level, msg FROM logs",
        "SELECT level FROM logs WHERE status = '500'",
    ] {
        let analyzer = QueryAnalyzer::new(sql).unwrap();
        let cfg = analyzer.scan_config();
        assert!(
            cfg.line_field_name.is_none(),
            "scan_config should not force line capture for {sql:?}"
        );
    }
}

#[test]
fn scan_config_preserves_referenced_columns_in_wanted_fields() {
    let analyzer =
        QueryAnalyzer::new("SELECT level, body FROM logs WHERE json(body, 'status') = '500'")
            .unwrap();
    let cfg = analyzer.scan_config();
    assert!(
        cfg.wanted_fields.iter().any(|f| f.name == "level"),
        "expected level in wanted_fields"
    );
    assert!(
        cfg.wanted_fields.iter().any(|f| f.name == "body"),
        "expected body in wanted_fields"
    );
}

/// `collect_column_refs` must traverse `TRIM(col)` and add `col` to the
/// referenced column set so that `scan_config()` requests it from the
/// scanner.  Without this, the scanner never extracts the column, the batch
/// has no `col` column, and the SQL transform fails or silently returns NULL.
///
/// Regression test: `SqlExpr::Trim { expr, .. }` was not handled in
/// `collect_column_refs` — it fell to the catch-all `_ => {}` arm.
#[test]
fn test_trim_col_in_select_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT TRIM(msg) AS msg FROM logs").unwrap();
    assert!(
        a.referenced_columns.contains("msg"),
        "TRIM(msg) must add 'msg' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// Same as above but for WHERE clause.
#[test]
fn test_trim_col_in_where_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT level FROM logs WHERE TRIM(msg) = 'hello'").unwrap();
    assert!(
        a.referenced_columns.contains("msg"),
        "TRIM(msg) in WHERE must add 'msg' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `SUBSTRING(col, ...)` must also register `col` in referenced_columns.
#[test]
fn test_substring_col_in_select_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT SUBSTRING(msg, 1, 4) AS prefix FROM logs").unwrap();
    assert!(
        a.referenced_columns.contains("msg"),
        "SUBSTRING(msg, ...) must add 'msg' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `EXTRACT(YEAR FROM ts_col)` must register `ts_col` in referenced_columns.
#[test]
fn test_extract_col_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT EXTRACT(YEAR FROM event_time) AS yr FROM logs").unwrap();
    assert!(
        a.referenced_columns.contains("event_time"),
        "EXTRACT(... FROM event_time) must add 'event_time' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `col AT TIME ZONE 'tz'` must register `col` in referenced_columns.
#[test]
fn test_at_time_zone_col_adds_to_referenced_columns() {
    let a =
        QueryAnalyzer::new("SELECT event_time AT TIME ZONE 'UTC' AS utc_time FROM logs").unwrap();
    assert!(
        a.referenced_columns.contains("event_time"),
        "col AT TIME ZONE must add 'event_time' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// Columns referenced only in a window function's OVER clause (PARTITION BY
/// and ORDER BY) must appear in `referenced_columns`.
///
/// Before the fix, `collect_column_refs` walked `func.args` but not
/// `func.over`, so `level` and `timestamp` were silently dropped from
/// `referenced_columns`.  The scanner would never extract those fields,
/// causing the window function to see NULLs instead of real values.
///
/// Regression test for missing `func.over` traversal in
/// `collect_column_refs`.
#[test]
fn test_window_over_partition_order_cols_added_to_referenced_columns() {
    let a = QueryAnalyzer::new(
        "SELECT ROW_NUMBER() OVER (PARTITION BY level ORDER BY timestamp) AS rn FROM logs",
    )
    .unwrap();
    assert!(
        a.referenced_columns.contains("level"),
        "PARTITION BY level must add 'level' to referenced_columns, got {:?}",
        a.referenced_columns
    );
    assert!(
        a.referenced_columns.contains("timestamp"),
        "ORDER BY timestamp must add 'timestamp' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// OVER clause with both a function arg AND window columns — all must be
/// collected.
#[test]
fn test_window_func_arg_and_over_cols_both_collected() {
    let a = QueryAnalyzer::new(
        "SELECT SUM(duration) OVER (PARTITION BY service ORDER BY ts) AS rolling FROM logs",
    )
    .unwrap();
    assert!(
        a.referenced_columns.contains("duration"),
        "SUM(duration) must add 'duration' to referenced_columns, got {:?}",
        a.referenced_columns
    );
    assert!(
        a.referenced_columns.contains("service"),
        "PARTITION BY service must add 'service' to referenced_columns, got {:?}",
        a.referenced_columns
    );
    assert!(
        a.referenced_columns.contains("ts"),
        "ORDER BY ts must add 'ts' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `col IS TRUE` in WHERE must add `col` to referenced_columns.
///
/// Before the fix, `SqlExpr::IsTrue` fell to the catch-all `_ => {}` arm,
/// so `is_error` was never added to `referenced_columns`.  The scanner
/// therefore never extracted `is_error`, the WHERE filter saw NULLs, and
/// all rows were silently dropped.
#[test]
fn test_is_true_col_in_where_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT message FROM logs WHERE is_error IS TRUE").unwrap();
    assert!(
        a.referenced_columns.contains("is_error"),
        "col IS TRUE must add 'is_error' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `col IS FALSE` in WHERE must add `col` to referenced_columns.
#[test]
fn test_is_false_col_in_where_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT message FROM logs WHERE is_debug IS FALSE").unwrap();
    assert!(
        a.referenced_columns.contains("is_debug"),
        "col IS FALSE must add 'is_debug' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `col IS DISTINCT FROM value` must add `col` to referenced_columns.
#[test]
fn test_is_distinct_from_adds_to_referenced_columns() {
    let a =
        QueryAnalyzer::new("SELECT message FROM logs WHERE status IS DISTINCT FROM 'ok'").unwrap();
    assert!(
        a.referenced_columns.contains("status"),
        "col IS DISTINCT FROM must add 'status' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `col SIMILAR TO pattern` must add `col` to referenced_columns.
#[test]
fn test_similar_to_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT message FROM logs WHERE path SIMILAR TO '/api/%'").unwrap();
    assert!(
        a.referenced_columns.contains("path"),
        "col SIMILAR TO must add 'path' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `CEIL(col)` in WHERE must add `col` to referenced_columns.
///
/// Before the fix, `SqlExpr::Ceil { expr, .. }` fell to `_ => {}`.
#[test]
fn test_ceil_col_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT message FROM logs WHERE CEIL(duration) > 100").unwrap();
    assert!(
        a.referenced_columns.contains("duration"),
        "CEIL(duration) must add 'duration' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `FLOOR(col)` in WHERE must add `col` to referenced_columns.
#[test]
fn test_floor_col_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT message FROM logs WHERE FLOOR(duration) < 50").unwrap();
    assert!(
        a.referenced_columns.contains("duration"),
        "FLOOR(duration) must add 'duration' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `POSITION(x IN col)` in WHERE must add `col` to referenced_columns.
#[test]
fn test_position_col_adds_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT message FROM logs WHERE POSITION('error' IN message) > 0")
        .unwrap();
    assert!(
        a.referenced_columns.contains("message"),
        "POSITION(... IN message) must add 'message' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `x IN UNNEST(arr_col)` must collect both `x` and `arr_col`.
#[test]
fn test_in_unnest_collects_array_expr_column() {
    let a = QueryAnalyzer::new("SELECT level FROM logs WHERE level IN UNNEST(levels)").unwrap();
    assert!(
        a.referenced_columns.contains("level"),
        "InUnnest lhs must add 'level' to referenced_columns, got {:?}",
        a.referenced_columns
    );
    assert!(
        a.referenced_columns.contains("levels"),
        "InUnnest array_expr must add 'levels' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// `OVERLAY(str PLACING repl FROM start FOR len)` must collect all argument
/// expressions used by the OVERLAY AST node.
#[test]
fn test_overlay_collects_all_argument_columns() {
    let a = QueryAnalyzer::new(
        "SELECT OVERLAY(message PLACING replacement FROM start_pos FOR count_len) AS msg FROM logs",
    )
    .unwrap();
    for col in ["message", "replacement", "start_pos", "count_len"] {
        assert!(
            a.referenced_columns.contains(col),
            "OVERLAY argument {col:?} must be in referenced_columns, got {:?}",
            a.referenced_columns
        );
    }
}

/// Columns that appear only in GROUP BY must be added to referenced_columns.
///
/// Before the fix, `SELECT COUNT(*) AS cnt FROM logs GROUP BY level` would
/// not add `level` to referenced_columns because only SELECT and WHERE were
/// walked.  The scanner would then skip `level`, GROUP BY would operate on
/// all-NULLs, and all rows would land in one group — silently wrong.
#[test]
fn test_group_by_only_col_added_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT COUNT(*) AS cnt FROM logs GROUP BY level").unwrap();
    assert!(
        a.referenced_columns.contains("level"),
        "GROUP BY level must add 'level' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// Columns that appear only in HAVING must be added to referenced_columns.
///
/// `HAVING MAX(severity) > 3` where severity is not in SELECT would see
/// all-NULLs → `MAX(NULL) = NULL`, `NULL > 3 = false` → all groups filtered out.
#[test]
fn test_having_col_added_to_referenced_columns() {
    let a = QueryAnalyzer::new(
        "SELECT level, COUNT(*) AS cnt FROM logs GROUP BY level HAVING MAX(severity) > 3",
    )
    .unwrap();
    assert!(
        a.referenced_columns.contains("severity"),
        "HAVING MAX(severity) must add 'severity' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// Columns that appear only in ORDER BY must be added to referenced_columns.
///
/// `ORDER BY timestamp` where timestamp is not in SELECT would sort on
/// all-NULLs, producing arbitrary ordering with no error.
#[test]
fn test_order_by_only_col_added_to_referenced_columns() {
    let a = QueryAnalyzer::new("SELECT message FROM logs ORDER BY timestamp").unwrap();
    assert!(
        a.referenced_columns.contains("timestamp"),
        "ORDER BY timestamp must add 'timestamp' to referenced_columns, got {:?}",
        a.referenced_columns
    );
}

/// QueryAnalyzer must collect column refs from MATCH_RECOGNIZE expressions.
#[test]
fn test_query_analyzer_match_recognize_column_refs() {
    let a = QueryAnalyzer::new(
        "SELECT * FROM logs MATCH_RECOGNIZE (PARTITION BY host ORDER BY timestamp MEASURES status AS status_measure ONE ROW PER MATCH PATTERN (A) DEFINE A AS severity > 3)",
    )
    .unwrap();

    for col in ["host", "timestamp", "status", "severity"] {
        assert!(
            a.referenced_columns.contains(col),
            "MATCH_RECOGNIZE must add {col:?} to referenced_columns, got {:?}",
            a.referenced_columns
        );
    }
}

#[test]
fn test_scan_config_selective_query_does_not_set_line_field() {
    let a = QueryAnalyzer::new("SELECT body, level FROM logs WHERE level = 'ERROR'").unwrap();
    let sc = a.scan_config();
    assert!(
        sc.line_field_name.is_none(),
        "scan_config must not set line_field_name for selective queries"
    );
    assert!(
        !sc.extract_all,
        "scan_config.extract_all must be false for a selective query"
    );
}
