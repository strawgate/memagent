//! Deterministic data generators for benchmarks.
//!
//! Every generator uses a seeded [`fastrand::Rng`] so that identical
//! `(count, seed)` pairs always produce byte-identical output.  This ensures
//! reproducible Criterion results across runs and CI.

use std::fmt::Write;
use std::sync::Arc;

use logfwd_output::BatchMetadata;

// ---------------------------------------------------------------------------
// Constants — realistic data pools
// ---------------------------------------------------------------------------

const LEVELS: &[&str] = &["INFO", "WARN", "ERROR", "DEBUG", "TRACE"];
const STREAMS: &[&str] = &["stdout", "stderr"];
const NAMESPACES: &[&str] = &[
    "default",
    "kube-system",
    "monitoring",
    "app-prod",
    "data-pipeline",
];
const PODS: &[&str] = &[
    "api-gateway-7b8c9d-abc12",
    "worker-5f6a7b-def34",
    "frontend-3c4d5e-ghi56",
    "scheduler-1a2b3c-jkl78",
    "ingester-9e0f1a-mno90",
];
const CONTAINERS: &[&str] = &["main", "sidecar", "init", "envoy"];
const PATHS: &[&str] = &[
    "/api/users",
    "/api/orders",
    "/api/health",
    "/api/auth/login",
    "/api/metrics",
    "/api/v2/search",
    "/api/v2/ingest",
    "/graphql",
];
const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];
const SERVICES: &[&str] = &[
    "api-gateway",
    "user-service",
    "order-service",
    "auth-service",
    "metrics-collector",
];
const STATUS_CODES: &[u16] = &[200, 200, 200, 201, 204, 400, 401, 403, 404, 500, 502, 503];

const STACK_TRACE: &str = "java.lang.NullPointerException: Cannot invoke method on null object\n\
    \tat com.example.service.UserHandler.getUser(UserHandler.java:142)\n\
    \tat com.example.service.UserHandler.handleRequest(UserHandler.java:87)\n\
    \tat com.example.framework.Router.dispatch(Router.java:234)\n\
    \tat com.example.framework.HttpServer.processRequest(HttpServer.java:456)\n\
    \tat com.example.framework.HttpServer$Worker.run(HttpServer.java:678)\n\
    \tat java.base/java.lang.Thread.run(Thread.java:829)";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn pick<'a>(rng: &mut fastrand::Rng, items: &'a [&str]) -> &'a str {
    items[rng.usize(..items.len())]
}

fn pick_u16(rng: &mut fastrand::Rng, items: &[u16]) -> u16 {
    items[rng.usize(..items.len())]
}

/// Escape a string for JSON embedding (handles `\n`, `\t`, `\`, `"`).
fn json_escape(s: &str, out: &mut String) {
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
}

// ---------------------------------------------------------------------------
// gen_cri_k8s — CRI-formatted K8s container logs
// ---------------------------------------------------------------------------

/// Generate `count` CRI-formatted Kubernetes container log lines.
///
/// ~10% of lines are partial (P flag), the rest are full (F flag).
/// JSON payloads have variable schemas and line lengths (100–2000 bytes).
/// Output is deterministic for a given `(count, seed)` pair.
///
/// Format: `<timestamp> <stream> <P|F> <json-payload>\n`
pub fn gen_cri_k8s(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut buf = String::with_capacity(count * 350);

    for i in 0..count {
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let stream = pick(&mut rng, STREAMS);
        let is_partial = rng.u8(..10) == 0; // ~10% partial
        let flag = if is_partial { "P" } else { "F" };

        let _ = write!(buf, "2024-01-15T10:30:{sec:02}.{nano:09}Z {stream} {flag} ");

        // Vary the JSON payload complexity
        let variant = rng.u8(..10);
        let level = pick(&mut rng, LEVELS);
        let status = pick_u16(&mut rng, STATUS_CODES);
        let duration = rng.f64() * 500.0;

        if variant < 6 {
            // Short message (~150 bytes): 5-6 fields
            let path = pick(&mut rng, PATHS);
            let method = pick(&mut rng, METHODS);
            let _ = write!(
                buf,
                r#"{{"level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"request_id":"req-{:08x}"}}"#,
                rng.u32(..),
            );
        } else if variant < 9 {
            // Medium message (~400 bytes): nested fields, more attributes
            let ns = pick(&mut rng, NAMESPACES);
            let pod = pick(&mut rng, PODS);
            let container = pick(&mut rng, CONTAINERS);
            let path = pick(&mut rng, PATHS);
            let method = pick(&mut rng, METHODS);
            let trace_id = rng.u128(..);
            let span_id = rng.u64(..);
            let _ = write!(
                buf,
                r#"{{"level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"namespace":"{ns}","pod":"{pod}","container":"{container}","trace_id":"{trace_id:032x}","span_id":"{span_id:016x}","service":"{}","request_id":"req-{:08x}","bytes_sent":{}}}"#,
                pick(&mut rng, SERVICES),
                rng.u32(..),
                rng.u32(..65536),
            );
        } else {
            // Long message (~1500 bytes): stack trace
            let _ = write!(buf, r#"{{"level":"ERROR","message":""#);
            json_escape(STACK_TRACE, &mut buf);
            let _ = write!(
                buf,
                r#"","exception_class":"java.lang.NullPointerException","service":"{}","request_id":"req-{:08x}","status":500,"duration_ms":{duration:.1}}}"#,
                pick(&mut rng, SERVICES),
                rng.u32(..),
            );
        }

        buf.push('\n');
    }
    buf.into_bytes()
}

// ---------------------------------------------------------------------------
// gen_production_mixed — Non-uniform JSON lines
// ---------------------------------------------------------------------------

/// Generate `count` non-uniform JSON log lines simulating production traffic.
///
/// Distribution: 70% short (~150 bytes, 5-8 fields), 20% medium (~400 bytes,
/// nested), 10% long (~1500 bytes, stack traces with escaped newlines).
/// Field presence is variable (sparse).  Output is deterministic for a given
/// `(count, seed)` pair.
pub fn gen_production_mixed(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut buf = String::with_capacity(count * 250);

    for i in 0..count {
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let ts = format!("2024-01-15T10:30:{sec:02}.{nano:09}Z");
        let variant = rng.u8(..10);

        if variant < 7 {
            // Short: 5-8 fields, ~150 bytes
            let level = pick(&mut rng, LEVELS);
            let path = pick(&mut rng, PATHS);
            let method = pick(&mut rng, METHODS);
            let status = pick_u16(&mut rng, STATUS_CODES);
            let duration = rng.f64() * 500.0;
            let _ = write!(
                buf,
                r#"{{"timestamp":"{ts}","level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1}"#,
            );
            // Sparse fields: ~50% chance each
            if rng.bool() {
                let _ = write!(buf, r#","service":"{}""#, pick(&mut rng, SERVICES));
            }
            if rng.bool() {
                let _ = write!(buf, r#","request_id":"req-{:08x}""#, rng.u32(..));
            }
            if rng.bool() {
                let _ = write!(buf, r#","bytes_sent":{}"#, rng.u32(..65536));
            }
            buf.push_str("}\n");
        } else if variant < 9 {
            // Medium: ~400 bytes, nested-like (K8s metadata + trace context)
            let level = pick(&mut rng, LEVELS);
            let ns = pick(&mut rng, NAMESPACES);
            let pod = pick(&mut rng, PODS);
            let container = pick(&mut rng, CONTAINERS);
            let path = pick(&mut rng, PATHS);
            let method = pick(&mut rng, METHODS);
            let status = pick_u16(&mut rng, STATUS_CODES);
            let duration = rng.f64() * 500.0;
            let trace_id = rng.u128(..);
            let span_id = rng.u64(..);
            let _ = write!(
                buf,
                r#"{{"timestamp":"{ts}","level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"namespace":"{ns}","pod":"{pod}","container":"{container}","trace_id":"{trace_id:032x}","span_id":"{span_id:016x}","service":"{}","request_id":"req-{:08x}","bytes_sent":{},"user_agent":"Mozilla/5.0","content_type":"application/json"}}"#,
                pick(&mut rng, SERVICES),
                rng.u32(..),
                rng.u32(..65536),
            );
            buf.push('\n');
        } else {
            // Long: ~1500 bytes, stack traces
            let _ = write!(buf, r#"{{"timestamp":"{ts}","level":"ERROR","message":""#,);
            json_escape(STACK_TRACE, &mut buf);
            let _ = write!(
                buf,
                r#"","exception_class":"java.lang.NullPointerException","service":"{}","namespace":"{}","pod":"{}","request_id":"req-{:08x}","status":500,"duration_ms":{:.1}}}"#,
                pick(&mut rng, SERVICES),
                pick(&mut rng, NAMESPACES),
                pick(&mut rng, PODS),
                rng.u32(..),
                rng.f64() * 5000.0,
            );
            buf.push('\n');
        }
    }
    buf.into_bytes()
}

// ---------------------------------------------------------------------------
// gen_narrow — Simple narrow JSON lines (5 fields, ~120 bytes/line)
// ---------------------------------------------------------------------------

/// Generate `count` narrow JSON log lines (~120 bytes each, 5 fields).
///
/// This is the simplest benchmark payload: uniform schema, short values.
/// Deterministic for a given `(count, seed)` pair.
pub fn gen_narrow(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut buf = String::with_capacity(count * 130);

    for i in 0..count {
        let level = pick(&mut rng, LEVELS);
        let path = pick(&mut rng, PATHS);
        let status = pick_u16(&mut rng, STATUS_CODES);
        let duration = rng.f64() * 500.0;
        let _ = write!(
            buf,
            r#"{{"level":"{level}","message":"{} {path}","path":"{path}","status":{status},"duration_ms":{duration:.1}}}"#,
            METHODS[i % METHODS.len()],
        );
        buf.push('\n');
    }
    buf.into_bytes()
}

// ---------------------------------------------------------------------------
// gen_wide — Wide JSON lines (20+ fields, ~600 bytes/line)
// ---------------------------------------------------------------------------

/// Generate `count` wide JSON log lines (~600 bytes each, 20+ fields).
///
/// Simulates verbose structured logging with many attributes.
/// Deterministic for a given `(count, seed)` pair.
pub fn gen_wide(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut buf = String::with_capacity(count * 650);

    for i in 0..count {
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let level = pick(&mut rng, LEVELS);
        let path = pick(&mut rng, PATHS);
        let method = pick(&mut rng, METHODS);
        let status = pick_u16(&mut rng, STATUS_CODES);
        let duration = rng.f64() * 500.0;
        let ns = pick(&mut rng, NAMESPACES);
        let pod = pick(&mut rng, PODS);
        let container = pick(&mut rng, CONTAINERS);
        let service = pick(&mut rng, SERVICES);
        let trace_id = rng.u128(..);
        let span_id = rng.u64(..);

        let _ = write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:{sec:02}.{nano:09}Z","level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"namespace":"{ns}","pod":"{pod}","container":"{container}","service":"{service}","trace_id":"{trace_id:032x}","span_id":"{span_id:016x}","request_id":"req-{:08x}","bytes_sent":{},"bytes_received":{},"user_agent":"Mozilla/5.0 (X11; Linux x86_64)","content_type":"application/json","remote_addr":"10.{}.{}.{}","host":"api.example.com","protocol":"HTTP/1.1","tls_version":"TLSv1.3","upstream_latency_ms":{:.1},"retry_count":{}}}"#,
            rng.u32(..),
            rng.u32(..65536),
            rng.u32(..65536),
            rng.u8(..),
            rng.u8(..),
            rng.u8(..),
            rng.f64() * 200.0,
            rng.u8(..3),
        );
        buf.push('\n');
    }
    buf.into_bytes()
}

// ---------------------------------------------------------------------------
// Metadata helper
// ---------------------------------------------------------------------------

/// Create benchmark-standard `BatchMetadata` with typical K8s resource attributes.
pub fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::new(vec![
            ("service.name".into(), "bench-service".into()),
            ("service.version".into(), "1.0.0".into()),
            ("host.name".into(), "bench-node-01".into()),
        ]),
        observed_time_ns: 1_705_312_200_000_000_000, // 2024-01-15T10:30:00Z
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cri_k8s_deterministic() {
        let a = gen_cri_k8s(100, 42);
        let b = gen_cri_k8s(100, 42);
        assert_eq!(a, b, "same seed must produce identical output");
    }

    #[test]
    fn cri_k8s_different_seeds_differ() {
        let a = gen_cri_k8s(100, 42);
        let b = gen_cri_k8s(100, 99);
        assert_ne!(a, b, "different seeds must produce different output");
    }

    #[test]
    fn cri_k8s_format_correctness() {
        let data = gen_cri_k8s(200, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            // CRI format: <timestamp> <stream> <P|F> <payload>
            let parts: Vec<&str> = line.splitn(4, ' ').collect();
            assert_eq!(parts.len(), 4, "CRI line must have 4 parts: {line}");
            assert!(
                parts[0].ends_with('Z'),
                "timestamp must end with Z: {}",
                parts[0]
            );
            assert!(
                parts[1] == "stdout" || parts[1] == "stderr",
                "stream must be stdout/stderr: {}",
                parts[1]
            );
            assert!(
                parts[2] == "P" || parts[2] == "F",
                "flag must be P or F: {}",
                parts[2]
            );
            assert!(
                parts[3].starts_with('{'),
                "payload must be JSON: {}",
                parts[3]
            );
        }
    }

    #[test]
    fn cri_k8s_has_partial_lines() {
        let data = gen_cri_k8s(1000, 42);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        let partial_count = text.lines().filter(|l| l.contains(" P ")).count();
        assert!(
            partial_count > 0,
            "expected some partial (P) lines in 1000 lines"
        );
        assert!(
            partial_count < 200,
            "expected ~10% partial lines, got {partial_count}"
        );
    }

    #[test]
    fn production_mixed_deterministic() {
        let a = gen_production_mixed(100, 42);
        let b = gen_production_mixed(100, 42);
        assert_eq!(a, b, "same seed must produce identical output");
    }

    #[test]
    fn production_mixed_format_correctness() {
        let data = gen_production_mixed(200, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            assert!(line.starts_with('{'), "each line must be JSON: {line}");
            assert!(line.ends_with('}'), "each line must end with }}: {line}");
            // Must parse as valid JSON
            let _: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
        }
    }

    #[test]
    fn production_mixed_has_length_variety() {
        let data = gen_production_mixed(1000, 42);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        let lengths: Vec<usize> = text.lines().map(str::len).collect();
        let min = *lengths.iter().min().unwrap();
        let max = *lengths.iter().max().unwrap();
        assert!(
            max > min * 3,
            "expected significant length variety: min={min}, max={max}"
        );
    }

    #[test]
    fn narrow_deterministic() {
        let a = gen_narrow(100, 42);
        let b = gen_narrow(100, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn wide_deterministic() {
        let a = gen_wide(100, 42);
        let b = gen_wide(100, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn narrow_valid_json() {
        let data = gen_narrow(50, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            let _: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
        }
    }

    #[test]
    fn wide_valid_json() {
        let data = gen_wide(50, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            let _: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
        }
    }
}
