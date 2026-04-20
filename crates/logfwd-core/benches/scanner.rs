use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::{FieldSpec, ScanConfig};
use std::hint::black_box;

// ===========================================================================
// Data generators
// ===========================================================================

/// Narrow: 5 fields, ~120 bytes/line.
fn gen_narrow(count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(count * 150);
    for i in 0..count {
        let line = format!(
            r#"{{"level":"INFO","msg":"handling request","path":"/api/v1/users/{}","status":{},"duration_ms":{:.2}}}"#,
            i,
            if i % 10 == 0 { 500 } else { 200 },
            0.5 + (i as f64) * 0.1
        );
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    buf
}

/// Wide: 55 fields, ~3KB/line.
fn gen_wide(count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(count * 3000);
    for i in 0..count {
        buf.extend_from_slice(b"{");
        for j in 0..40 {
            buf.extend_from_slice(
                format!(
                    r#""junk_field_{}":"some_long_useless_string_value_here_that_takes_up_space","#,
                    j
                )
                .as_bytes(),
            );
        }
        buf.extend_from_slice(
            format!(
                r#""level":"INFO","msg":"handling request","path":"/api/v1/users/{}","status":{},"duration_ms":{:.2}"#,
                i,
                if i % 10 == 0 { 500 } else { 200 },
                0.5 + (i as f64) * 0.1
            )
            .as_bytes(),
        );
        for j in 40..50 {
            buf.extend_from_slice(format!(r#","post_junk_{}":{}"#, j, j * 100).as_bytes());
        }
        buf.extend_from_slice(b"}\n");
    }
    buf
}

/// K8s-realistic: 12 fields with namespace, pod, container metadata.
fn gen_k8s(count: usize) -> Vec<u8> {
    let ns = ["default", "kube-system", "monitoring", "prod", "staging"];
    let pods = [
        "api-7f8d9c6b5-x2k4m",
        "nginx-5b9c7d8e6-q3r7p",
        "prom-0",
        "coredns-6d4b-wk9tn",
    ];
    let levels = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"];
    let mut buf = Vec::with_capacity(count * 300);
    for i in 0..count {
        buf.extend_from_slice(
            format!(
                r#"{{"timestamp":"2024-01-15T10:30:{:02}.{:03}Z","namespace":"{}","pod":"{}","container":"main","stream":"stdout","level":"{}","msg":"request processed path=/api/v1/resource/{} user_id=usr_{:06}","status":{},"latency_ms":{:.3},"request_id":"req-{:08x}"}}"#,
                i % 60, i % 1000, ns[i % ns.len()], pods[i % pods.len()],
                levels[i % levels.len()], i, i % 100_000,
                if i % 20 == 0 { 500 } else if i % 7 == 0 { 404 } else { 200 },
                0.1 + (i as f64 % 100.0) * 0.5, i as u32,
            )
            .as_bytes(),
        );
        buf.push(b'\n');
    }
    buf
}

/// Long messages: ~500 byte stack traces.
fn gen_long_messages(count: usize) -> Vec<u8> {
    let stack = "java.lang.NullPointerException: Cannot invoke method on null\\n\\tat com.example.service.UserService.getUser(UserService.java:42)\\n\\tat com.example.controller.UserController.handleRequest(UserController.java:108)\\n\\tat org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:897)";
    let mut buf = Vec::with_capacity(count * 600);
    for i in 0..count {
        buf.extend_from_slice(
            format!(
                r#"{{"level":"ERROR","msg":"{}","exception":"NullPointerException","service":"user-svc","trace_id":"{:032x}"}}"#,
                stack, i as u128 * 0xDEADBEEF,
            )
            .as_bytes(),
        );
        buf.push(b'\n');
    }
    buf
}

/// Sparse: 50 possible fields, each row has 3-8.
fn gen_sparse(count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(count * 200);
    for i in 0..count {
        let mut fields = vec![
            format!(r#""ts":"2024-01-15T10:30:00Z""#),
            format!(r#""level":"INFO""#),
        ];
        if i % 2 == 0 {
            fields.push(format!(r#""host":"web-{}""#, i % 10));
        }
        if i % 3 == 0 {
            fields.push(format!(
                r#""status":{}"#,
                if i % 10 == 0 { 500 } else { 200 }
            ));
        }
        if i % 4 == 0 {
            fields.push(format!(r#""duration_ms":{:.2}"#, 0.5 + (i as f64) * 0.01));
        }
        if i % 5 == 0 {
            fields.push(format!(r#""user_id":"usr_{:06}""#, i % 100000));
        }
        if i % 7 == 0 {
            fields.push(r#""region":"us-east-1""#.to_string());
        }
        if i % 11 == 0 {
            fields.push(r#""tags":["prod","v2"]"#.to_string());
        }
        buf.extend_from_slice(format!("{{{}}}", fields.join(",")).as_bytes());
        buf.push(b'\n');
    }
    buf
}

/// Nested JSON values.
fn gen_nested(count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(count * 400);
    for i in 0..count {
        buf.extend_from_slice(
            format!(
                r#"{{"event":"request","metadata":{{"request_id":"req-{:08x}","trace_id":"trace-{:08x}"}},"headers":{{"content-type":"application/json","user-agent":"Mozilla/5.0"}},"tags":["prod","v2","us-east"],"status":{}}}"#,
                i, i * 7, if i % 10 == 0 { 500 } else { 200 },
            )
            .as_bytes(),
        );
        buf.push(b'\n');
    }
    buf
}

/// Escape-heavy strings.
fn gen_escapes(count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(count * 300);
    for i in 0..count {
        buf.extend_from_slice(
            format!(
                r#"{{"level":"INFO","msg":"parsing config \"config.json\": {{\"key\": \"value-{}\"}}","path":"C:\\Users\\admin\\logs\\app-{}.log","query":"SELECT * FROM users WHERE name = \"user_{}\""}}"#,
                i, i, i,
            )
            .as_bytes(),
        );
        buf.push(b'\n');
    }
    buf
}

// ===========================================================================
// Config helpers
// ===========================================================================

fn pushdown_config() -> ScanConfig {
    ScanConfig {
        wanted_fields: vec![
            FieldSpec {
                name: "level".to_string(),
                aliases: vec![],
            },
            FieldSpec {
                name: "status".to_string(),
                aliases: vec![],
            },
            FieldSpec {
                name: "duration_ms".to_string(),
                aliases: vec![],
            },
        ],
        extract_all: false,
        line_field_name: None,
        validate_utf8: false,
    }
}

fn select_star() -> ScanConfig {
    ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
    }
}

// ===========================================================================
// Arrow-json baseline: spec-compliant, battle-tested
// ===========================================================================

fn arrow_json_parse(data: &[u8]) -> arrow::record_batch::RecordBatch {
    use arrow_json::reader::{ReaderBuilder, infer_json_schema_from_seekable};
    use std::io::Cursor;
    use std::sync::Arc;

    let mut cursor = Cursor::new(data);
    let (schema, _) = infer_json_schema_from_seekable(&mut cursor, None).unwrap();
    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_batch_size(data.len()) // one big batch
        .build(cursor)
        .unwrap();
    // Collect all batches (should be just one since batch_size is large)
    let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
    if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    }
}

// ===========================================================================
// sonic-rs baseline: spec-compliant SIMD JSON parser → Scanner
// ===========================================================================

fn sonic_rs_parse(data: &[u8], ndjson: &mut Vec<u8>) -> arrow::record_batch::RecordBatch {
    // Use sonic-rs to produce NDJSON, then feed through Scanner::scan_detached.
    // This gives a fair comparison: sonic-rs parses, Scanner builds Arrow arrays.
    use sonic_rs::JsonValueTrait;

    ndjson.clear();
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if let Ok(val) = sonic_rs::from_slice::<sonic_rs::Value>(line)
            && val.is_object()
            && let Ok(s) = sonic_rs::to_string(&val)
        {
            ndjson.extend_from_slice(s.as_bytes());
            ndjson.push(b'\n');
        }
    }

    let mut scanner = Scanner::new(ScanConfig::default());
    scanner
        .scan_detached(bytes::Bytes::copy_from_slice(ndjson))
        .expect("bench: scan_detached failed")
}

// ===========================================================================
// Benchmark macro
// ===========================================================================

macro_rules! bench_scenario {
    ($fn_name:ident, $group_name:expr, $data_gen:expr, $count:expr, $config:expr) => {
        fn $fn_name(c: &mut Criterion) {
            let data = $data_gen($count);
            let mut group = c.benchmark_group($group_name);
            group.throughput(Throughput::Bytes(data.len() as u64));

            group.bench_function("Scanner", |b| {
                let mut scanner = Scanner::new($config());
                let data_bytes = bytes::Bytes::from(data.clone());
                b.iter(|| {
                    black_box(
                        scanner
                            .scan_detached(data_bytes.clone())
                            .expect("bench: scan should not fail"),
                    )
                })
            });

            group.bench_function("sonic-rs DOM", |b| {
                let mut ndjson = Vec::with_capacity(data.len());
                b.iter(|| black_box(sonic_rs_parse(black_box(&data), &mut ndjson)))
            });

            group.bench_function("arrow-json", |b| {
                b.iter(|| black_box(arrow_json_parse(black_box(&data))))
            });

            group.finish();
        }
    };
}

// ===========================================================================
// Scenarios
// ===========================================================================

bench_scenario!(
    bench_narrow_pushdown,
    "Narrow 5f – pushdown",
    gen_narrow,
    10_000,
    pushdown_config
);
bench_scenario!(
    bench_narrow_star,
    "Narrow 5f – SELECT *",
    gen_narrow,
    10_000,
    select_star
);
bench_scenario!(
    bench_wide_pushdown,
    "Wide 55f – pushdown",
    gen_wide,
    2_000,
    pushdown_config
);
bench_scenario!(
    bench_wide_star,
    "Wide 55f – SELECT *",
    gen_wide,
    2_000,
    select_star
);
bench_scenario!(
    bench_k8s_pushdown,
    "K8s 12f – pushdown",
    gen_k8s,
    10_000,
    pushdown_config
);
bench_scenario!(
    bench_k8s_star,
    "K8s 12f – SELECT *",
    gen_k8s,
    10_000,
    select_star
);
bench_scenario!(
    bench_long_msg,
    "Long msg – SELECT *",
    gen_long_messages,
    5_000,
    select_star
);
bench_scenario!(
    bench_sparse,
    "Sparse – SELECT *",
    gen_sparse,
    10_000,
    select_star
);
bench_scenario!(
    bench_nested,
    "Nested – SELECT *",
    gen_nested,
    5_000,
    select_star
);
bench_scenario!(
    bench_escapes,
    "Escapes – SELECT *",
    gen_escapes,
    10_000,
    select_star
);

// Batch size scaling
bench_scenario!(
    bench_batch_100,
    "Batch 100 rows",
    gen_narrow,
    100,
    pushdown_config
);
bench_scenario!(
    bench_batch_1k,
    "Batch 1K rows",
    gen_narrow,
    1_000,
    pushdown_config
);
bench_scenario!(
    bench_batch_10k,
    "Batch 10K rows",
    gen_narrow,
    10_000,
    pushdown_config
);
bench_scenario!(
    bench_batch_100k,
    "Batch 100K rows",
    gen_narrow,
    100_000,
    pushdown_config
);

// ===========================================================================
// Groups
// ===========================================================================

criterion_group!(
    core_benches,
    bench_narrow_pushdown,
    bench_narrow_star,
    bench_wide_pushdown,
    bench_wide_star,
);

criterion_group!(
    realistic_benches,
    bench_k8s_pushdown,
    bench_k8s_star,
    bench_long_msg,
    bench_sparse,
    bench_nested,
    bench_escapes,
);

criterion_group!(
    scaling_benches,
    bench_batch_100,
    bench_batch_1k,
    bench_batch_10k,
    bench_batch_100k,
);

criterion_main!(core_benches, realistic_benches, scaling_benches);
