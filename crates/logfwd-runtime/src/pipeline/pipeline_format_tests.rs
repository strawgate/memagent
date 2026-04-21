//! Format integration tests: JSON, raw, and CRI format → scanner → RecordBatch.

use super::*;
use logfwd_core::scan_config::ScanConfig;

/// JSON format: raw bytes pass directly through to scanner.
#[test]
fn json_format_direct_to_scanner() {
    let input =
        b"{\"level\":\"INFO\",\"msg\":\"hello\"}\n{\"level\":\"WARN\",\"msg\":\"world\"}\n";
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
    };
    let mut scanner = Scanner::new(config);
    let batch = scanner.scan_detached(Bytes::from(input.to_vec())).unwrap();
    assert_eq!(batch.num_rows(), 2);
    // Single-type string fields: bare names
    assert!(batch.schema().field_with_name("level").is_ok());
    assert!(batch.schema().field_with_name("msg").is_ok());
}

/// Raw format: lines captured as body when line_capture is true.
#[test]
fn raw_format_captures_lines() {
    let input = b"plain text line 1\nplain text line 2\n";
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: false,
    };
    let mut scanner = Scanner::new(config);
    let batch = scanner.scan_detached(Bytes::from(input.to_vec())).unwrap();
    assert_eq!(batch.num_rows(), 2);
    let body_col = batch
        .column_by_name("body")
        .expect("raw format should emit body column");
    if let Some(arr) = body_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
    {
        assert_eq!(arr.value(0), "plain text line 1");
        assert_eq!(arr.value(1), "plain text line 2");
    } else if let Some(arr) = body_col
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
    {
        assert_eq!(arr.value(0), "plain text line 1");
        assert_eq!(arr.value(1), "plain text line 2");
    } else {
        panic!("body column must be StringArray or StringViewArray");
    }
}

/// Raw format treats each line as an opaque input record captured in `body`,
/// even when the payload itself looks like JSON.
#[test]
fn raw_format_json_line_prefers_full_line_body() {
    let input = br#"{"body":"app message","level":"INFO"}"#;
    let mut with_newline = input.to_vec();
    with_newline.push(b'\n');
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: false,
    };
    let mut scanner = Scanner::new(config);
    let batch = scanner.scan_detached(Bytes::from(with_newline)).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let body_col = batch
        .column_by_name("body")
        .expect("raw format should emit body column");
    if let Some(arr) = body_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
    {
        assert_eq!(arr.value(0), std::str::from_utf8(input).unwrap());
    } else if let Some(arr) = body_col
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
    {
        assert_eq!(arr.value(0), std::str::from_utf8(input).unwrap());
    } else {
        panic!("body column must be StringArray or StringViewArray");
    }
}

/// CRI format: message extracted, timestamp/stream/flag stripped.
#[test]
fn cri_extraction_produces_json() {
    let cri_input = b"2024-01-15T10:30:00Z stdout F {\"level\":\"INFO\",\"msg\":\"hello\"}\n";
    let mut out = Vec::new();
    let stats = Arc::new(ComponentStats::new());
    let mut fmt = FormatDecoder::cri(1024, Arc::clone(&stats));
    fmt.process_lines(cri_input, &mut out);

    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
    };
    let mut scanner = Scanner::new(config);
    let batch = scanner.scan_detached(Bytes::from(out.clone())).unwrap();
    assert_eq!(batch.num_rows(), 1);
    // Single-type string field: bare name
    assert!(batch.schema().field_with_name("level").is_ok());
}

/// Mixed: CRI P+F → scanner produces correct fields.
#[test]
fn cri_pf_merge_then_scan() {
    let input = b"2024-01-15T10:30:00Z stdout P {\"level\":\"ER\n2024-01-15T10:30:00Z stdout F ROR\",\"msg\":\"boom\"}\n";
    let mut out = Vec::new();
    let stats = Arc::new(ComponentStats::new());
    let mut fmt = FormatDecoder::cri(1024, Arc::clone(&stats));
    fmt.process_lines(input, &mut out);

    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
    };
    let mut scanner = Scanner::new(config);
    let batch = scanner.scan_detached(Bytes::from(out)).unwrap();
    assert_eq!(batch.num_rows(), 1);
}
