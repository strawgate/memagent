//! Shared deterministic data generators for the logfwd benchmark suite.
//!
//! All generators are seeded: the same `(count, seed)` pair always produces
//! identical output, making benchmark results reproducible across runs and
//! machines without pulling in an RNG crate.

use std::fmt::Write;
use std::sync::Arc;

use logfwd_output::BatchMetadata;

// ---------------------------------------------------------------------------
// Lookup tables (shared across generators)
// ---------------------------------------------------------------------------

const LEVELS: [&str; 4] = ["INFO", "ERROR", "DEBUG", "WARN"];
const PATHS: [&str; 5] = [
    "/api/users",
    "/api/orders",
    "/api/health",
    "/api/auth/login",
    "/api/metrics",
];
const STATUS_CODES: [u16; 5] = [200, 200, 200, 500, 404];
const SERVICES: [&str; 4] = [
    "api-gateway",
    "auth-service",
    "order-service",
    "user-service",
];

/// Mix a seed into an index to produce a deterministic but shuffled value.
///
/// This is a simple bijective hash (wrapping multiply + xor-shift) that avoids
/// the perfectly sequential patterns you get from bare `i % len`. It is *not*
/// cryptographic — just good enough to vary which table entry each row picks.
#[inline]
fn mix(i: usize, seed: u64) -> usize {
    let x = (i as u64)
        .wrapping_mul(0x517c_c1b7_2722_0a95)
        .wrapping_add(seed);
    let x = x ^ (x >> 17);
    x as usize
}

// ---------------------------------------------------------------------------
// 1. JSON log lines
// ---------------------------------------------------------------------------

/// Generate `count` newline-delimited JSON log lines (~250 bytes each).
///
/// Fields: `timestamp`, `level`, `message`, `status`, `duration_ms`,
/// `request_id`, `service`.
///
/// Deterministic: same `(count, seed)` always returns the same bytes.
#[must_use]
pub fn gen_json_lines(count: usize, seed: u64) -> Vec<u8> {
    let mut s = String::with_capacity(count * 260);
    for i in 0..count {
        let m = mix(i, seed);
        let _ = write!(
            s,
            r#"{{"timestamp":"2024-01-15T10:30:{:02}.{:09}Z","level":"{}","message":"GET {} HTTP/1.1","status":{},"duration_ms":{},"request_id":"req-{:08x}","service":"{}"}}"#,
            i % 60,
            i % 1_000_000_000,
            LEVELS[m % LEVELS.len()],
            PATHS[m % PATHS.len()],
            STATUS_CODES[m % STATUS_CODES.len()],
            (m % 500) + 1,
            i,
            SERVICES[m % SERVICES.len()],
        );
        s.push('\n');
    }
    s.into_bytes()
}

// ---------------------------------------------------------------------------
// 2. CRI log lines
// ---------------------------------------------------------------------------

/// Generate `count` CRI-formatted Kubernetes container log lines.
///
/// Format: `<timestamp> <stream> <flag> <json_payload>\n`
///
/// - ~10% partial (`P`) lines, 90% full (`F`).
/// - 90% stdout, 10% stderr.
/// - JSON payload uses the same field schema as [`gen_json_lines`].
///
/// Deterministic: same `(count, seed)` always returns the same bytes.
#[must_use]
pub fn gen_cri_lines(count: usize, seed: u64) -> Vec<u8> {
    let mut s = String::with_capacity(count * 320);
    for i in 0..count {
        let m = mix(i, seed);
        let stream = if m % 10 == 0 { "stderr" } else { "stdout" };
        let flag = if m % 10 == 1 { "P" } else { "F" };
        let _ = write!(
            s,
            r#"{ts} {stream} {flag} {{"level":"{level}","message":"request handled","status":{status},"duration_ms":{dur}}}"#,
            ts = format_args!("2024-01-15T10:30:{:02}.{:09}Z", i % 60, i % 1_000_000_000),
            stream = stream,
            flag = flag,
            level = LEVELS[m % LEVELS.len()],
            status = STATUS_CODES[m % STATUS_CODES.len()],
            dur = (m % 500) + 1,
        );
        s.push('\n');
    }
    s.into_bytes()
}

// ---------------------------------------------------------------------------
// 3. Wide JSON objects
// ---------------------------------------------------------------------------

/// Generate `count` wide JSON objects with `fields` key-value pairs each.
///
/// Each field is ~30 bytes on average. Value types rotate: string, int, float.
///
/// Deterministic: same `(count, fields, seed)` always returns the same bytes.
#[must_use]
pub fn gen_wide_json(count: usize, fields: usize, seed: u64) -> Vec<u8> {
    let mut s = String::with_capacity(count * fields * 32);
    for i in 0..count {
        s.push('{');
        for f in 0..fields {
            if f > 0 {
                s.push(',');
            }
            let m = mix(i.wrapping_mul(fields).wrapping_add(f), seed);
            match f % 3 {
                0 => {
                    // string value
                    let _ = write!(s, r#""field_{f}":"value_{m:08x}""#);
                }
                1 => {
                    // integer value
                    let _ = write!(s, r#""field_{f}":{}"#, m % 100_000);
                }
                _ => {
                    // float value
                    let _ = write!(s, r#""field_{f}":{:.2}"#, (m % 10_000) as f64 / 100.0);
                }
            }
        }
        s.push('}');
        s.push('\n');
    }
    s.into_bytes()
}

// ---------------------------------------------------------------------------
// 4. Shared test metadata
// ---------------------------------------------------------------------------

/// Shared `BatchMetadata` for benchmarks that need one.
///
/// Uses a fixed `service.name` attribute and zero observed timestamp.
#[must_use]
pub fn make_test_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::new(vec![("service.name".into(), "bench".into())]),
        observed_time_ns: 0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert every non-empty line is valid JSON.
    fn assert_valid_ndjson(data: &[u8]) {
        let text = std::str::from_utf8(data).expect("output should be valid UTF-8");
        for (idx, line) in text.lines().enumerate() {
            if line.is_empty() {
                continue;
            }
            serde_json::from_str::<serde_json::Value>(line)
                .unwrap_or_else(|e| panic!("line {idx} is not valid JSON: {e}\n  line: {line}"));
        }
    }

    /// Count non-empty lines.
    fn line_count(data: &[u8]) -> usize {
        std::str::from_utf8(data)
            .expect("valid UTF-8")
            .lines()
            .filter(|l| !l.is_empty())
            .count()
    }

    // -- gen_json_lines --

    #[test]
    fn json_lines_determinism() {
        let a = gen_json_lines(100, 42);
        let b = gen_json_lines(100, 42);
        assert_eq!(a, b, "same seed must produce identical output");
    }

    #[test]
    fn json_lines_different_seeds() {
        let a = gen_json_lines(100, 1);
        let b = gen_json_lines(100, 2);
        assert_ne!(a, b, "different seeds should produce different output");
    }

    #[test]
    fn json_lines_valid_ndjson() {
        let data = gen_json_lines(200, 7);
        assert_valid_ndjson(&data);
        assert_eq!(line_count(&data), 200);
    }

    // -- gen_cri_lines --

    #[test]
    fn cri_lines_determinism() {
        let a = gen_cri_lines(100, 99);
        let b = gen_cri_lines(100, 99);
        assert_eq!(a, b, "same seed must produce identical output");
    }

    #[test]
    fn cri_lines_valid_format() {
        let data = gen_cri_lines(200, 5);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        let mut partials = 0;
        let mut fulls = 0;
        let mut stdouts = 0;
        let mut stderrs = 0;
        for line in text.lines().filter(|l| !l.is_empty()) {
            // CRI format: <timestamp> <stream> <flag> <payload>
            let parts: Vec<&str> = line.splitn(4, ' ').collect();
            assert_eq!(parts.len(), 4, "CRI line must have 4 space-separated parts");
            let stream = parts[1];
            let flag = parts[2];
            let payload = parts[3];

            assert!(
                stream == "stdout" || stream == "stderr",
                "stream must be stdout or stderr, got: {stream}"
            );
            assert!(
                flag == "F" || flag == "P",
                "flag must be F or P, got: {flag}"
            );

            // Payload should be valid JSON.
            serde_json::from_str::<serde_json::Value>(payload)
                .unwrap_or_else(|e| panic!("CRI payload is not valid JSON: {e}"));

            match flag {
                "P" => partials += 1,
                "F" => fulls += 1,
                _ => unreachable!(),
            }
            match stream {
                "stdout" => stdouts += 1,
                "stderr" => stderrs += 1,
                _ => unreachable!(),
            }
        }
        assert_eq!(partials + fulls, 200);
        // With a large enough sample, we should see both types.
        assert!(partials > 0, "expected some partial lines");
        assert!(fulls > 0, "expected some full lines");
        assert!(stdouts > 0, "expected some stdout lines");
        assert!(stderrs > 0, "expected some stderr lines");
    }

    // -- gen_wide_json --

    #[test]
    fn wide_json_determinism() {
        let a = gen_wide_json(50, 20, 77);
        let b = gen_wide_json(50, 20, 77);
        assert_eq!(a, b, "same seed must produce identical output");
    }

    #[test]
    fn wide_json_valid_ndjson() {
        let data = gen_wide_json(100, 30, 11);
        assert_valid_ndjson(&data);
        assert_eq!(line_count(&data), 100);
    }

    #[test]
    fn wide_json_field_count() {
        let data = gen_wide_json(10, 25, 0);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines().filter(|l| !l.is_empty()) {
            let obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(line).expect("valid JSON object");
            assert_eq!(obj.len(), 25, "each line should have 25 fields");
        }
    }

    // -- make_test_metadata --

    #[test]
    fn test_metadata_has_service_name() {
        let meta = make_test_metadata();
        assert_eq!(meta.resource_attrs.len(), 1);
        assert_eq!(meta.resource_attrs[0].0, "service.name");
        assert_eq!(meta.resource_attrs[0].1, "bench");
    }
}
