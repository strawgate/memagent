//! Fuzz the UDF implementations (json, json_int, json_float, hash,
//! regexp_extract, grok) against arbitrary scanner-produced RecordBatches.
//!
//! The goal is to verify that no combination of SQL expression + input data
//! can cause a panic inside a UDF. Errors returned by `TransformError` are
//! expected and acceptable — only panics are failures.
//!
//! The scanner is configured with `line_field_name = "body"` so all queries
//! referencing the `body` column are valid and UDF code paths are exercised.
//!
//! Run with:
//! ```
//! cargo +nightly fuzz run fuzz_udf -- -max_total_time=600
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;

use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;
use ffwd_transform::SqlTransform;

const UDF_QUERIES: &[&str] = &[
    // json() — extract field as string
    "SELECT json(body, 'status') AS status FROM logs",
    "SELECT json(body, 'level') AS lvl FROM logs",
    "SELECT json(body, 'msg') AS msg FROM logs",
    "SELECT json(body, 'nested') AS nested FROM logs",
    // json_int() — extract field as int
    "SELECT json_int(body, 'status') AS status FROM logs",
    "SELECT json_int(body, 'count') AS cnt FROM logs",
    "SELECT json_int(body, 'duration_ms') AS dur FROM logs",
    // json_float() — extract field as float
    "SELECT json_float(body, 'latency') AS lat FROM logs",
    "SELECT json_float(body, 'rate') AS rate FROM logs",
    // hash() — deterministic FNV-1a
    "SELECT hash(body) AS h FROM logs",
    "SELECT hash(body), hash(msg) FROM logs",
    // regexp_extract()
    "SELECT regexp_extract(body, '(GET|POST) (\\S+)', 1) AS method FROM logs",
    "SELECT regexp_extract(body, 'status=([0-9]+)', 1) AS code FROM logs",
    // grok()
    "SELECT grok(body, '%{WORD:method} %{URIPATH:path}') AS parsed FROM logs",
    // Multiple UDFs in one query
    "SELECT json(body, 'status') AS s, hash(body) AS h FROM logs",
    "SELECT json_int(body, 'status') + 1 AS s1, json_float(body, 'rate') * 2 AS r2 FROM logs",
    // Edge cases
    "SELECT json(body, 'nonexistent') AS missing FROM logs",
    "SELECT json(body, '') AS empty_key FROM logs",
    "SELECT json_int(body, 'not_a_number') AS nan FROM logs",
    "SELECT json_float(body, 'also_not_num') AS nnn FROM logs",
];

fn run_udf(data: &[u8]) {
    let mut scanner = Scanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: false,
        row_predicate: None,
    });

    let Ok(batch) = scanner.scan_detached(bytes::Bytes::copy_from_slice(data)) else {
        return;
    };

    for sql in UDF_QUERIES {
        let mut transform =
            SqlTransform::new(sql).expect("UDF fuzz query list must stay SQL-valid");
        let _ = transform.execute_blocking(batch.clone());
    }
}

fuzz_target!(|data: &[u8]| {
    run_udf(data);
});
