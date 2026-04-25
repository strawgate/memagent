use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use bytes::Bytes;
use ffwd_arrow::Scanner;
use ffwd_core::scan_config::ScanConfig;
use std::collections::HashMap;

pub(crate) fn scan_json(data: Vec<u8>) -> RecordBatch {
    Scanner::new(ScanConfig::default())
        .scan_detached(Bytes::from(data))
        .expect("scan must succeed")
}

pub(crate) fn assert_batch_matches_scanned_json(expected: &RecordBatch, actual: &RecordBatch) {
    assert_eq!(expected.num_rows(), actual.num_rows(), "row count mismatch");
    assert_eq!(
        expected.num_columns(),
        actual.num_columns(),
        "column count mismatch"
    );

    let expected_schema = expected.schema();
    let actual_schema = actual.schema();
    let expected_fields = expected_schema.fields();
    let actual_fields = actual_schema.fields();
    let expected_by_name: HashMap<&str, usize> = expected_fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().as_str(), idx))
        .collect();
    let actual_by_name: HashMap<&str, usize> = actual_fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().as_str(), idx))
        .collect();

    for (name, expected_idx) in &expected_by_name {
        let actual_idx = actual_by_name
            .get(name)
            .unwrap_or_else(|| panic!("missing column {name} in direct batch"));
        let expected_field = &expected_fields[*expected_idx];
        let actual_field = &actual_fields[*actual_idx];
        assert_eq!(
            expected_field.data_type(),
            actual_field.data_type(),
            "column type mismatch for {}",
            expected_field.name()
        );
        assert_eq!(
            expected_field.is_nullable(),
            actual_field.is_nullable(),
            "nullability mismatch for {}",
            expected_field.name()
        );
    }

    for (name, expected_idx) in &expected_by_name {
        let actual_idx = actual_by_name[name];
        let expected_col = expected.column(*expected_idx);
        let actual_col = actual.column(actual_idx);
        for row_idx in 0..expected.num_rows() {
            let expected_value = array_value_to_string(expected_col.as_ref(), row_idx)
                .unwrap_or_else(|err| {
                    panic!("display expected row {row_idx} column {name}: {err}")
                });
            let actual_value = array_value_to_string(actual_col.as_ref(), row_idx)
                .unwrap_or_else(|err| panic!("display actual row {row_idx} column {name}: {err}"));
            assert_eq!(
                expected_value, actual_value,
                "value mismatch at row {row_idx}, column {name}",
            );
        }
    }
}
