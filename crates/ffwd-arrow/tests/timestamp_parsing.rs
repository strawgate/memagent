use arrow::array::{Array, ArrayRef, Float32Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ffwd_arrow::star_schema::flat_to_star;
use std::sync::Arc;

#[test]
fn test_epoch_heuristics() {
    // Indirect test via flat_to_star
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_timestamp",
        DataType::Utf8,
        true,
    )]));

    let timestamps = vec![
        Some("1718451045"),          // seconds
        Some("1718451045123"),       // milliseconds
        Some("1718451045123456"),    // microseconds
        Some("1718451045123456789"), // nanoseconds
    ];

    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(timestamps))];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let star = flat_to_star(&batch).unwrap();

    let ts_col = star
        .logs
        .column_by_name("time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    // 1718451045 seconds -> 1718451045000000000 ns
    assert_eq!(ts_col.value(0), 1718451045000000000);
    // 1718451045123 ms -> 1718451045123000000 ns
    assert_eq!(ts_col.value(1), 1718451045123000000);
    // 1718451045123456 us -> 1718451045123456000 ns
    assert_eq!(ts_col.value(2), 1718451045123456000);
    // 1718451045123456789 ns -> 1718451045123456789 ns
    assert_eq!(ts_col.value(3), 1718451045123456789);
}

#[test]
fn test_rfc3339_validation() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_timestamp",
        DataType::Utf8,
        true,
    )]));

    let timestamps = vec![
        Some("2024-01-15T99:30:00Z"), // invalid hour
        Some("2024-01-15T10:99:00Z"), // invalid minute
        Some("2024-01-15T10:30:99Z"), // invalid second
        Some("2024-02-30T10:30:00Z"), // invalid day
    ];

    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(timestamps))];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let star = flat_to_star(&batch).unwrap();

    let ts_col = star
        .logs
        .column_by_name("time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    assert!(ts_col.is_null(0));
    assert!(ts_col.is_null(1));
    assert!(ts_col.is_null(2));
    assert!(ts_col.is_null(3));
}

#[test]
fn test_timestamp_overflow() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_timestamp",
        DataType::Utf8,
        true,
    )]));

    let timestamps = vec![
        Some("3000-01-15T10:30:00Z"), // year 3000 overflows i64 nanos from 1970
    ];

    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(timestamps))];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let star = flat_to_star(&batch).unwrap();

    let ts_col = star
        .logs
        .column_by_name("time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    assert!(ts_col.is_null(0));
}

#[test]
fn test_unexpected_column_types() {
    // Test that flat_to_star returns error instead of panicking on unexpected types for well-known columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Float32, true), // level should be Utf8
    ]));

    let columns: Vec<ArrayRef> = vec![Arc::new(Float32Array::from(vec![Some(1.0)]))];

    let batch = RecordBatch::try_new(schema, columns).unwrap();

    // This should NOT panic. Reaching this line without panicking is the assertion.
    // flat_to_star handles unexpected column types gracefully (returns Ok, skipping the column).
    let _result = flat_to_star(&batch);
}
