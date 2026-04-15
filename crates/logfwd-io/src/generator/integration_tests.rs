//! Integration tests that verify generator profiles produce output that
//! round-trips through the scanner pipeline correctly.

use super::cloudtrail::{gen_cloudtrail_audit, gen_cloudtrail_batch};
use super::cri::{gen_cri_k8s, gen_narrow, gen_production_mixed};
use super::envoy::{EnvoyAccessProfile, gen_envoy_access, gen_envoy_access_with_profile};
use super::test_support::scan_json;
use super::wide::gen_wide;

// ---------------------------------------------------------------------------
// JSON round-trip: generate → scan → verify row count and key columns
// ---------------------------------------------------------------------------

#[test]
fn narrow_round_trips_through_scanner() {
    let data = gen_narrow(100, 42);
    let batch = scan_json(data);
    assert_eq!(batch.num_rows(), 100);
    for col in ["level", "message", "path", "status", "duration_ms"] {
        assert!(
            batch.schema().field_with_name(col).is_ok(),
            "missing column: {col}"
        );
    }
}

#[test]
fn wide_round_trips_through_scanner() {
    let data = gen_wide(50, 7);
    let batch = scan_json(data);
    assert_eq!(batch.num_rows(), 50);
    // Wide logs have 20+ fields
    assert!(
        batch.num_columns() >= 20,
        "expected 20+ columns, got {}",
        batch.num_columns()
    );
}

#[test]
fn production_mixed_round_trips_through_scanner() {
    let data = gen_production_mixed(200, 99);
    let batch = scan_json(data);
    assert_eq!(batch.num_rows(), 200);
    assert!(
        batch.schema().field_with_name("timestamp").is_ok(),
        "missing timestamp column"
    );
}

#[test]
fn envoy_round_trips_through_scanner() {
    let data = gen_envoy_access(100, 5);
    let batch = scan_json(data);
    assert_eq!(batch.num_rows(), 100);
    for col in ["service", "route_name", "response_code", "duration_ms"] {
        assert!(
            batch.schema().field_with_name(col).is_ok(),
            "missing column: {col}"
        );
    }
}

#[test]
fn cloudtrail_round_trips_through_scanner() {
    let data = gen_cloudtrail_audit(50, 3);
    let batch = scan_json(data);
    assert_eq!(batch.num_rows(), 50);
    for col in ["eventSource", "eventName", "awsRegion"] {
        assert!(
            batch.schema().field_with_name(col).is_ok(),
            "missing column: {col}"
        );
    }
}

// ---------------------------------------------------------------------------
// CRI format validation (not JSON — verify structure directly)
// ---------------------------------------------------------------------------

#[test]
fn cri_k8s_produces_valid_cri_lines() {
    let data = gen_cri_k8s(100, 42);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 100);

    for line in &lines {
        let parts: Vec<&str> = line.splitn(4, ' ').collect();
        assert_eq!(parts.len(), 4, "CRI line needs 4 parts: {line}");
        assert!(parts[0].ends_with('Z'), "timestamp must end with Z");
        assert!(
            parts[1] == "stdout" || parts[1] == "stderr",
            "stream invalid: {}",
            parts[1]
        );
        assert!(
            parts[2] == "P" || parts[2] == "F",
            "flag invalid: {}",
            parts[2]
        );
    }
}

// ---------------------------------------------------------------------------
// Batch generation row counts match
// ---------------------------------------------------------------------------

#[test]
fn cloudtrail_batch_row_count_matches_lines() {
    let lines = gen_cloudtrail_audit(128, 42);
    let line_count = std::str::from_utf8(&lines)
        .expect("valid UTF-8")
        .lines()
        .count();
    let batch = gen_cloudtrail_batch(128, 42);
    assert_eq!(batch.num_rows(), line_count);
}

// ---------------------------------------------------------------------------
// Seed determinism for profiles not covered in generator_tests
// ---------------------------------------------------------------------------

#[test]
fn cloudtrail_deterministic() {
    let a = gen_cloudtrail_audit(100, 42);
    let b = gen_cloudtrail_audit(100, 42);
    assert_eq!(a, b, "same seed must produce identical output");
}

#[test]
fn cloudtrail_different_seeds_differ() {
    let a = gen_cloudtrail_audit(100, 42);
    let b = gen_cloudtrail_audit(100, 99);
    assert_ne!(a, b, "different seeds must produce different output");
}

// ---------------------------------------------------------------------------
// Profile-specific field validation via JSON parsing
// ---------------------------------------------------------------------------

#[test]
fn envoy_fields_are_realistic() {
    let profile = EnvoyAccessProfile::benchmark();
    let data = gen_envoy_access_with_profile(50, 1, profile);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");

    for line in text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
        assert!(v["service"].is_string(), "service must be string");
        assert!(v["route_name"].is_string(), "route_name must be string");
        assert!(v["response_code"].is_u64(), "response_code must be integer");
        assert!(v["duration_ms"].is_f64(), "duration_ms must be float");
        assert!(
            v["upstream_host"].is_string(),
            "upstream_host must be string"
        );
    }
}

#[test]
fn cloudtrail_fields_are_realistic() {
    let data = gen_cloudtrail_audit(30, 1);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");

    for line in text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
        assert!(v["eventSource"].is_string(), "eventSource must be string");
        assert!(v["eventName"].is_string(), "eventName must be string");
        assert!(v["awsRegion"].is_string(), "awsRegion must be string");
        assert!(v["userIdentity"].is_object(), "userIdentity must be object");
    }
}

#[test]
fn narrow_fields_match_expected_schema() {
    let data = gen_narrow(20, 1);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");

    for line in text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
        assert!(v["level"].is_string(), "level must be string");
        assert!(v["message"].is_string(), "message must be string");
        assert!(v["path"].is_string(), "path must be string");
        assert!(v["status"].is_u64(), "status must be integer");
        assert!(v["duration_ms"].is_f64(), "duration_ms must be float");
    }
}

#[test]
fn wide_has_many_fields() {
    let data = gen_wide(10, 1);
    let text = std::str::from_utf8(&data).expect("valid UTF-8");

    for line in text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
        let obj = v.as_object().expect("line must be JSON object");
        assert!(
            obj.len() >= 20,
            "wide logs should have 20+ fields, got {}",
            obj.len()
        );
    }
}
