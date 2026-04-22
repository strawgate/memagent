#[test]
#[ignore = "microbenchmark"]
fn bench_writer_helpers_fast_vs_simple() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 200_000;
    let i64_inputs: Vec<i64> = (0..N)
        .map(|i| ((i as i64).wrapping_mul(1_048_573)).wrapping_sub(73_421))
        .collect();
    let u64_inputs: Vec<u64> = (0..N)
        .map(|i| (i as u64).wrapping_mul(2_654_435_761))
        .collect();
    let f64_inputs: Vec<f64> = (0..N)
        .map(|i| {
            let r = (i as f64) * 0.125 - 17_333.75;
            if i % 97 == 0 {
                f64::NAN
            } else if i % 89 == 0 {
                f64::INFINITY
            } else if i % 83 == 0 {
                f64::NEG_INFINITY
            } else {
                r
            }
        })
        .collect();
    let hex_inputs: Vec<Vec<u8>> = (0..N)
        .map(|i| {
            let x = (i as u64).wrapping_mul(11_400_714_819_323_198_485);
            x.to_le_bytes().to_vec()
        })
        .collect();

    let mut buf = Vec::with_capacity(256 * 1024);

    let t0 = Instant::now();
    for &n in &i64_inputs {
        buf.clear();
        write_i64_to_buf(&mut buf, n);
        black_box(buf.len());
    }
    let i64_fast = t0.elapsed();

    let t0 = Instant::now();
    for &n in &i64_inputs {
        buf.clear();
        write_i64_to_buf_simple(&mut buf, n);
        black_box(buf.len());
    }
    let i64_simple = t0.elapsed();

    let t0 = Instant::now();
    for &n in &u64_inputs {
        buf.clear();
        write_u64_to_buf(&mut buf, n);
        black_box(buf.len());
    }
    let u64_fast = t0.elapsed();

    let t0 = Instant::now();
    for &n in &u64_inputs {
        buf.clear();
        write_u64_to_buf_simple(&mut buf, n);
        black_box(buf.len());
    }
    let u64_simple = t0.elapsed();

    let t0 = Instant::now();
    for &d in &f64_inputs {
        buf.clear();
        write_f64_to_buf(&mut buf, d);
        black_box(buf.len());
    }
    let f64_fast = t0.elapsed();

    let t0 = Instant::now();
    for &d in &f64_inputs {
        buf.clear();
        write_f64_to_buf_simple(&mut buf, d);
        black_box(buf.len());
    }
    let f64_simple = t0.elapsed();

    let t0 = Instant::now();
    for v in &hex_inputs {
        buf.clear();
        write_hex_to_buf(&mut buf, v);
        black_box(buf.len());
    }
    let hex_fast = t0.elapsed();

    let t0 = Instant::now();
    for v in &hex_inputs {
        buf.clear();
        write_hex_to_buf_simple(&mut buf, v);
        black_box(buf.len());
    }
    let hex_simple = t0.elapsed();

    eprintln!("writer bench N={N}");
    eprintln!("  i64  fast={:?} simple={:?}", i64_fast, i64_simple);
    eprintln!("  u64  fast={:?} simple={:?}", u64_fast, u64_simple);
    eprintln!("  f64  fast={:?} simple={:?}", f64_fast, f64_simple);
    eprintln!("  hex  fast={:?} simple={:?}", hex_fast, hex_simple);
}

#[test]
fn json_escaping_control_chars() {
    let ctrl: String = (0u8..=0x1f).map(|b| b as char).collect();
    let mut out = Vec::new();
    write_json_string_field(&mut out, "k", &ctrl);
    let text = String::from_utf8(out).unwrap();

    for b in text.as_bytes() {
        assert!(
            *b >= 0x20,
            "raw control byte 0x{:02x} found in output: {text}",
            b
        );
    }

    assert!(text.contains(r"\u0000"), "NUL not escaped: {text}");
    assert!(text.contains(r"\u0001"), "SOH not escaped: {text}");
    assert!(text.contains(r"\u0008"), "BS not escaped: {text}");
    assert!(text.contains(r"\t"), "TAB not escaped: {text}");
    assert!(text.contains(r"\n"), "LF not escaped: {text}");
    assert!(text.contains(r"\r"), "CR not escaped: {text}");
    assert!(text.contains(r"\u000c"), "FF not escaped: {text}");
}

#[test]
fn json_escaping_unicode() {
    let input = "hello \u{00e9}\u{1f600} world \u{4e16}\u{754c}";
    let mut out = Vec::new();
    write_json_string_field(&mut out, "k", input);
    let text = String::from_utf8(out).unwrap();

    assert!(text.contains('\u{00e9}'), "e-acute missing: {text}");
    assert!(text.contains('\u{1f600}'), "emoji missing: {text}");
    assert!(text.contains('\u{4e16}'), "CJK char missing: {text}");

    let json_str = format!("{{{text}}}");
    serde_json::from_str::<serde_json::Value>(&json_str)
        .unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json_str}"));
}

#[test]
fn json_escaping_key_chars() {
    let mut out = Vec::new();
    write_json_string_field(&mut out, "my\"key\\path", "value");
    let text = String::from_utf8(out).unwrap();
    let json_str = format!("{{{text}}}");
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json_str}"));
    assert_eq!(parsed["my\"key\\path"], "value");
}

/// Regression test: when the pipeline channel is full the receiver must
/// return 429 rather than silently dropping the payload and returning 200.
#[test]
fn returns_429_when_channel_full_not_200() {
    let mut receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 2).unwrap();
    let addr = receiver.local_addr();
    let url = format!("http://{addr}/v1/logs");

    let body = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{"body": {"stringValue": "x"}}]
            }]
        }]
    })
    .to_string();

    // Fill the channel (capacity = 2 so two sends succeed).
    for i in 0..2 {
        let resp = loopback_http_client()
            .post(&url)
            .header("content-type", "application/json")
            .send(body.as_bytes())
            .unwrap_or_else(|e| panic!("request {i} failed: {e}"));
        assert_eq!(
            resp.status(),
            200,
            "expected 200 while channel has capacity (request {i})"
        );
    }

    // The channel is now full; the next request must not return 200.
    let result = loopback_http_client()
        .post(&url)
        .header("content-type", "application/json")
        .send(body.as_bytes());

    let status: u16 = match result {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_ne!(
        status, 200,
        "channel-full request must not return 200 (got {status})"
    );
    assert_eq!(status, 429, "expected 429 for backpressure, got {status}");
    assert_eq!(receiver.health(), ComponentHealth::Degraded);

    // Drain the two buffered entries so the receiver is valid.
    let _ = receiver.poll().unwrap();

    let resp = loopback_http_client()
        .post(&url)
        .header("content-type", "application/json")
        .send(body.as_bytes())
        .expect("request after drain failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(receiver.health(), ComponentHealth::Healthy);
}

// Bug #686: /v1/logsFOO and /v1/logs/extra should return 404, not 200.
#[test]
fn path_prefix_variants_return_404() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();

    for bad_path in &["/v1/logsFOO", "/v1/logs/extra", "/v1/logs2", "/v1/log"] {
        let url = format!("http://127.0.0.1:{port}{bad_path}");
        let status = match loopback_http_client().get(&url).call() {
            Ok(r) => r.status().as_u16(),
            Err(ureq::Error::StatusCode(c)) => c,
            Err(e) => panic!("unexpected error for {bad_path}: {e}"),
        };
        assert_eq!(status, 404, "{bad_path} should return 404, got {status}");
    }
}

// Bug #687: Content-Type: Application/JSON (capital A) should be treated as JSON.
#[test]
fn content_type_matching_is_case_insensitive() {
    let mut receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let body = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{"body": {"stringValue": "hello"}}]
            }]
        }]
    })
    .to_string();

    let resp = loopback_http_client()
        .post(&url)
        .header("content-type", "Application/JSON")
        .send(body.as_bytes())
        .expect("request failed");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Application/JSON should be decoded as JSON and return 200"
    );

    let data = poll_receiver_until(
        &mut receiver,
        Duration::from_secs(1),
        |events| !events.is_empty(),
        "timed out waiting for mixed-case Content-Type JSON payload",
    );
    assert!(
        !data.is_empty(),
        "expected data from JSON body with mixed-case Content-Type"
    );
}

#[test]
fn content_type_substring_match_does_not_route_json() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let body = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{"body": {"stringValue": "hello"}}]
            }]
        }]
    })
    .to_string();

    let status = match loopback_http_client()
        .post(&url)
        .header("content-type", "application/jsonl")
        .send(body.as_bytes())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_eq!(
        status, 400,
        "application/jsonl must not route to JSON decoder"
    );
}

#[test]
fn blank_content_encoding_returns_bad_request() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let status = match loopback_http_client()
        .post(&url)
        .header("content-encoding", "   ")
        .send(&make_test_request())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_eq!(status, 400, "blank Content-Encoding must be malformed");
}

// Bug #723: wrong HTTP method should return 405, not 404.
#[test]
fn wrong_http_method_returns_405() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    for (method, result) in [
        ("GET", loopback_http_client().get(&url).call()),
        ("DELETE", loopback_http_client().delete(&url).call()),
    ] {
        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error for {method}: {e}"),
        };
        assert_eq!(
            status, 405,
            "{method} /v1/logs should return 405 Method Not Allowed, got {status}"
        );
    }
}

// Bug #722: JSON body missing resourceLogs should return 400, not 200.
#[test]
fn missing_resource_logs_returns_400() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let bad_bodies = [r"{}", r#"{"foo":"bar"}"#, r#"{"resourceLogs":null}"#];
    for body in &bad_bodies {
        let result = loopback_http_client()
            .post(&url)
            .header("content-type", "application/json")
            .send(body.as_bytes());
        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error for body {body}: {e}"),
        };
        assert_eq!(status, 400, "body {body:?} should return 400, got {status}");
    }
}

#[test]
fn receiver_shuts_down_cleanly_on_drop() {
    let receiver =
        OtlpReceiverInput::new("test-drop", "127.0.0.1:0").expect("should bind successfully");
    let local_addr = receiver.local_addr();

    drop(receiver);

    wait_until(
        Duration::from_secs(1),
        || OtlpReceiverInput::new("test-drop-2", &local_addr.to_string()).is_ok(),
        "should bind successfully to the exact same port",
    );
}

#[test]
fn receiver_health_is_healthy_while_running() {
    let receiver =
        OtlpReceiverInput::new("test-health", "127.0.0.1:0").expect("should bind successfully");

    assert_eq!(receiver.health(), ComponentHealth::Healthy);
}

#[test]
fn receiver_health_reports_stopping_when_shutdown_requested() {
    let receiver = OtlpReceiverInput::new("test-health-stop", "127.0.0.1:0")
        .expect("should bind successfully");
    receiver.is_running.store(false, Ordering::Relaxed);
    receiver
        .health
        .store(ComponentHealth::Stopping.as_repr(), Ordering::Relaxed);

    assert_eq!(receiver.health(), ComponentHealth::Stopping);
}

#[test]
fn receiver_health_reports_failed_when_server_thread_exits() {
    let mut receiver = OtlpReceiverInput::new("test-health-failed", "127.0.0.1:0")
        .expect("should bind successfully");
    receiver.is_running.store(false, Ordering::Relaxed);
    let (shutdown_tx, _shutdown_rx) = oneshot::channel();
    receiver.background_task = BackgroundHttpTask::new_axum(shutdown_tx, std::thread::spawn(|| {}));
    receiver.is_running.store(true, Ordering::Relaxed);
    wait_until(
        Duration::from_secs(1),
        || receiver.health() == ComponentHealth::Failed,
        "receiver health did not transition to failed",
    );

    assert_eq!(receiver.health(), ComponentHealth::Failed);
}

#[test]
fn receiver_health_reports_failed_when_pipeline_disconnects() {
    let mut receiver = OtlpReceiverInput::new_with_capacity("test-disconnect", "127.0.0.1:0", 16)
        .expect("should bind successfully");
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");
    receiver.rx.take();

    let status = match loopback_http_client()
        .post(&url)
        .header("content-type", "application/json")
        .send(
        br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello"}}]}]}]}"#,
    ) {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };

    assert_eq!(status, 503);
    assert_eq!(receiver.health(), ComponentHealth::Failed);
}

// Valid OTLP JSON should still return 200 after the 400 fix.
#[test]
fn valid_otlp_json_returns_200() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let valid_body = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"severityText":"INFO","body":{"stringValue":"hello"}}]}]}]}"#;
    let result = loopback_http_client()
        .post(&url)
        .header("content-type", "application/json")
        .send(valid_body.as_bytes());
    let status: u16 = match result {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_eq!(
        status, 200,
        "valid OTLP JSON should return 200, got {status}"
    );
}

// Regression tests for issue #1167: non-finite floats must emit null, not "NaN"/"inf".
#[test]
fn write_f64_nan_emits_null() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, f64::NAN);
    assert_eq!(&out, b"null", "NaN must serialize as JSON null");
}

#[test]
fn write_f64_infinity_emits_null() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, f64::INFINITY);
    assert_eq!(&out, b"null", "Infinity must serialize as JSON null");
}

#[test]
fn write_f64_neg_infinity_emits_null() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, f64::NEG_INFINITY);
    assert_eq!(&out, b"null", "-Infinity must serialize as JSON null");
}

#[test]
fn write_f64_finite_unchanged() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, 1.25);
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.starts_with("1.25"),
        "finite float should be formatted normally: {text}"
    );
}

#[test]
fn write_json_string_field_escapes_key() {
    let mut out = Vec::new();
    write_json_string_field(&mut out, r#"k"ey"#, "value");
    let text = String::from_utf8(out).unwrap();
    let json_str = format!("{{{text}}}");
    serde_json::from_str::<serde_json::Value>(&json_str)
        .unwrap_or_else(|e| panic!("invalid JSON after key escaping: {e}\n{json_str}"));
    assert!(
        text.contains(r#"k\"ey"#),
        "quote in key not escaped: {text}"
    );
}

/// Regression test for issue #1691: convert_request_to_batch must also
/// fall back to observed_time_unix_nano when time_unix_nano is 0.
#[test]
fn batch_path_uses_observed_time_when_event_time_is_zero() {
    const OBSERVED_NS: u64 = 1_705_314_600_000_000_000;

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 0,
                    observed_time_unix_nano: OBSERVED_NS,
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("test".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let batch = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("batch build must succeed");
    let ts_col = batch
        .column_by_name(field_names::TIMESTAMP)
        .expect("_timestamp column must exist");
    let ts_arr = ts_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("_timestamp column must be Int64");
    assert_eq!(ts_arr.len(), 1, "expected exactly one row");
    assert_eq!(
        ts_arr.value(0),
        OBSERVED_NS as i64,
        "batch path must use observed_time_unix_nano when time_unix_nano is 0"
    );
}

#[test]
fn batch_path_errors_on_overflowed_observed_timestamp() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 0,
                    observed_time_unix_nano: u64::MAX,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("overflow".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let err = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("timestamp overflow must fail deterministically");
    let msg = err.to_string();
    assert!(
        msg.contains("observed_time_unix_nano"),
        "overflow diagnostic should name offending field: {msg}"
    );
    assert!(
        msg.contains("exceeds signed 64-bit nanosecond range"),
        "overflow diagnostic should explain range failure: {msg}"
    );
}
