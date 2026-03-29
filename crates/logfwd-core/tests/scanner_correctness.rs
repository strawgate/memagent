// Correctness probe tests for the scalar scanner.
// Run with: cargo test --features simd-scanner -p logfwd-core -- scanner_correctness

#[cfg(test)]
mod scanner_correctness {
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use logfwd_core::scanner::{ScanConfig, Scanner};
    use logfwd_core::simd_scanner::SimdScanner;

    fn scan_both(
        input: &[u8],
    ) -> (
        arrow::record_batch::RecordBatch,
        arrow::record_batch::RecordBatch,
    ) {
        let mut scalar = Scanner::new(ScanConfig::default(), 64);
        let mut simd = SimdScanner::new(ScanConfig::default(), 64);
        (scalar.scan(input), simd.scan(input))
    }

    fn get_str(batch: &arrow::record_batch::RecordBatch, col: &str, row: usize) -> Option<String> {
        batch
            .column_by_name(col)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .and_then(|a| {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row).to_string())
                }
            })
    }

    fn get_int(batch: &arrow::record_batch::RecordBatch, col: &str, row: usize) -> Option<i64> {
        batch
            .column_by_name(col)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .and_then(|a| {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            })
    }

    // 1. Key with escaped quote: {"k\"ey": "val"}
    #[test]
    fn escaped_quote_in_key() {
        let input = b"{\"k\\\"ey\":\"val\"}\n";
        let (scalar, simd) = scan_both(input);
        println!(
            "scalar cols: {:?}",
            scalar
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        println!(
            "simd cols: {:?}",
            simd.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        // sonic-rs should parse key as k"ey
        // scalar scanner doesn't unescape keys — it sees k\"ey
    }

    // 2. Unicode escape: {"msg": "\u0048ello"}
    #[test]
    fn unicode_escape_in_value() {
        let input = b"{\"msg\":\"\\u0048ello\"}\n";
        let (scalar, simd) = scan_both(input);
        let s_val = get_str(&scalar, "msg_str", 0);
        let d_val = get_str(&simd, "msg_str", 0);
        println!("scalar: {:?}", s_val);
        println!("simd:   {:?}", d_val);
        // scalar preserves raw \\u0048ello, simd decodes to Hello
    }

    // 3. Backslash at end of string (malformed): {"a": "test\"}
    #[test]
    fn backslash_before_closing_quote() {
        let input = b"{\"a\":\"test\\\"}\n";
        let (scalar, simd) = scan_both(input);
        println!(
            "scalar rows={} cols={}",
            scalar.num_rows(),
            scalar.num_columns()
        );
        println!("simd rows={} cols={}", simd.num_rows(), simd.num_columns());
        let s_val = get_str(&scalar, "a_str", 0);
        let d_val = get_str(&simd, "a_str", 0);
        println!("scalar a: {:?}", s_val);
        println!("simd a:   {:?}", d_val);
    }

    // 4. Deeply nested with braces in strings
    #[test]
    fn braces_inside_nested_strings() {
        let input = br#"{"data":{"msg":"has } and { inside"},"ok":true}
"#;
        let (scalar, simd) = scan_both(input);
        let s_data = get_str(&scalar, "data_str", 0);
        let s_ok = get_str(&scalar, "ok_str", 0);
        let d_data = get_str(&simd, "data_str", 0);
        let d_ok = get_str(&simd, "ok_str", 0);
        println!("scalar data: {:?}", s_data);
        println!("scalar ok:   {:?}", s_ok);
        println!("simd data:   {:?}", d_data);
        println!("simd ok:     {:?}", d_ok);
    }

    // 5. Empty string value
    #[test]
    fn empty_string_value() {
        let input = b"{\"a\":\"\"}\n";
        let (scalar, simd) = scan_both(input);
        let s = get_str(&scalar, "a_str", 0);
        let d = get_str(&simd, "a_str", 0);
        assert_eq!(s, Some("".to_string()), "scalar empty string");
        assert_eq!(d, Some("".to_string()), "simd empty string");
    }

    // 6. Number that looks like float but is just negative: -0
    #[test]
    fn negative_zero() {
        let input = b"{\"v\":-0}\n";
        let (scalar, simd) = scan_both(input);
        let s = get_int(&scalar, "v_int", 0);
        let d = get_int(&simd, "v_int", 0);
        println!("scalar: {:?}", s);
        println!("simd:   {:?}", d);
    }

    // 7. Very large integer (exceeds i64)
    #[test]
    fn large_integer_overflow() {
        let input = b"{\"big\":99999999999999999999}\n";
        let (scalar, simd) = scan_both(input);
        println!(
            "scalar cols: {:?}",
            scalar
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        println!(
            "simd cols: {:?}",
            simd.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        // scalar will try parse_int_fast which will overflow → null
        // simd will see it as u64 → falls to float path
    }

    // 8. Trailing garbage after JSON
    #[test]
    fn trailing_garbage() {
        let input = b"{\"a\":1}garbage\n";
        let (scalar, simd) = scan_both(input);
        let s = get_int(&scalar, "a_int", 0);
        let d = get_int(&simd, "a_int", 0);
        println!("scalar: {:?}, rows={}", s, scalar.num_rows());
        println!("simd:   {:?}, rows={}", d, simd.num_rows());
    }

    // 9. Value is a JSON array
    #[test]
    fn array_value() {
        let input = br#"{"tags":["a","b","c"],"n":1}
"#;
        let (scalar, simd) = scan_both(input);
        let s_tags = get_str(&scalar, "tags_str", 0);
        let d_tags = get_str(&simd, "tags_str", 0);
        let s_n = get_int(&scalar, "n_int", 0);
        let d_n = get_int(&simd, "n_int", 0);
        println!("scalar tags: {:?}", s_tags);
        println!("simd tags:   {:?}", d_tags);
        println!("scalar n: {:?}", s_n);
        println!("simd n:   {:?}", d_n);
    }

    // 10. Escaped backslash before quote: {"a": "test\\"}
    //     This is valid JSON — value is test\ (trailing backslash)
    #[test]
    fn escaped_backslash_before_quote() {
        let input = b"{\"a\":\"test\\\\\"}\n";
        let (scalar, simd) = scan_both(input);
        let s = get_str(&scalar, "a_str", 0);
        let d = get_str(&simd, "a_str", 0);
        println!("scalar: {:?}", s);
        println!("simd:   {:?}", d);
    }

    // 11. Colon inside a string value
    #[test]
    fn colon_in_string() {
        let input = br#"{"url":"http://example.com:8080/path","port":8080}
"#;
        let (scalar, simd) = scan_both(input);
        let s_url = get_str(&scalar, "url_str", 0);
        let d_url = get_str(&simd, "url_str", 0);
        let s_port = get_int(&scalar, "port_int", 0);
        let d_port = get_int(&simd, "port_int", 0);
        println!("scalar url: {:?}, port: {:?}", s_url, s_port);
        println!("simd url:   {:?}, port: {:?}", d_url, d_port);
    }

    // 12. Numeric string that starts with digit
    #[test]
    fn number_with_leading_plus() {
        let input = b"{\"v\":+42}\n";
        let (scalar, simd) = scan_both(input);
        println!(
            "scalar cols: {:?}",
            scalar
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        println!(
            "simd cols: {:?}",
            simd.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        // +42 is NOT valid JSON — how do they handle it?
    }

    // 13. Exponent-only float: 1e2
    #[test]
    fn exponent_number() {
        let input = b"{\"v\":1e2}\n";
        let (scalar, simd) = scan_both(input);
        println!(
            "scalar cols: {:?}",
            scalar
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        // scalar should see 'e' and classify as float
    }

    // 14. Multiple escaped chars in sequence
    #[test]
    fn multiple_escapes() {
        let input = br#"{"a":"line1\nline2\ttab"}
"#;
        let (scalar, simd) = scan_both(input);
        let s = get_str(&scalar, "a_str", 0);
        let d = get_str(&simd, "a_str", 0);
        println!("scalar: {:?}", s);
        println!("simd:   {:?}", d);
    }

    // 15. Empty object
    #[test]
    fn empty_object() {
        let input = b"{}\n";
        let (scalar, simd) = scan_both(input);
        assert_eq!(scalar.num_rows(), 1);
        assert_eq!(simd.num_rows(), 1);
        // _raw should still be present
    }

    // 16. Duplicate keys
    #[test]
    fn duplicate_keys() {
        let input = br#"{"a":1,"a":2}
"#;
        let (scalar, simd) = scan_both(input);
        let s = get_int(&scalar, "a_int", 0);
        let d = get_int(&simd, "a_int", 0);
        println!("scalar: {:?}", s);
        println!("simd:   {:?}", d);
        // scalar appends both (last write wins in builder?)
        // simd DOM likely keeps last
    }
}
