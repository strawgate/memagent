#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use logfwd_bench::{generators, make_otlp_sink};
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::otlp_receiver::{OtlpReceiverInput, decode_protobuf_to_batch};
use logfwd_output::Compression;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

struct HttpPoster {
    rt: tokio::runtime::Runtime,
    client: reqwest::Client,
}

impl HttpPoster {
    fn new(max_idle_per_host: usize) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(max_idle_per_host.max(1))
            // Keep benchmark traffic local and deterministic; ignore ambient proxy env.
            .no_proxy()
            .build()
            .expect("reqwest client");
        Self { rt, client }
    }

    fn post(&self, url: &str, body: &[u8]) {
        let response = self
            .rt
            .block_on(async {
                self.client
                    .post(url)
                    .header("Content-Type", "application/x-protobuf")
                    .body(body.to_vec())
                    .send()
                    .await
            })
            .expect("OTLP POST must succeed");
        assert_eq!(response.status(), 200, "unexpected OTLP status");
    }

    fn post_many(&self, url: &str, body: &[u8], concurrency: usize) {
        if concurrency <= 1 {
            self.post(url, body);
            return;
        }
        self.rt.block_on(async {
            let payload = Bytes::copy_from_slice(body);
            let url: Arc<str> = Arc::from(url);
            let client = self.client.clone();
            let mut handles = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                let client = client.clone();
                let url = Arc::clone(&url);
                let payload = payload.clone();
                handles.push(tokio::spawn(async move {
                    client
                        .post(&*url)
                        .header("Content-Type", "application/x-protobuf")
                        .body(payload)
                        .send()
                        .await
                }));
            }
            for handle in handles {
                let response = handle
                    .await
                    .expect("spawned OTLP POST task must complete")
                    .expect("OTLP POST must succeed");
                assert_eq!(response.status(), 200, "unexpected OTLP status");
            }
        });
    }
}

struct Case {
    name: &'static str,
    rows: usize,
    extra_string_attrs: usize,
}

#[derive(Default)]
struct Totals {
    receiver: Duration,
    receiver_direct: Duration,
    encode_manual: Duration,
    encode_generated: Duration,
}

fn main() {
    let iterations = std::env::var("OTLP_E2E_ITERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(20);
    let timeout_ms = std::env::var("OTLP_E2E_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10_000);
    let concurrencies = read_concurrency_list();
    let case_filter = std::env::var("OTLP_E2E_CASE").ok();
    let timeout = Duration::from_millis(timeout_ms);
    let cases = [
        Case {
            name: "narrow-1k",
            rows: 1_000,
            extra_string_attrs: 0,
        },
        Case {
            name: "wide-10k",
            rows: 10_000,
            extra_string_attrs: 12,
        },
    ];

    println!(
        "=== OTLP Receiver -> OtlpSink EPS Profile (iters={iterations}, concurrency={concurrencies:?}) ===\n"
    );

    for case in cases {
        if case_filter.as_deref().is_some_and(|name| name != case.name) {
            continue;
        }
        for &concurrency in &concurrencies {
            run_case(&case, iterations, timeout, concurrency);
        }
    }
}

fn run_case(case: &Case, iterations: usize, timeout: Duration, concurrency: usize) {
    let request = build_request(case.rows, case.extra_string_attrs);
    let request_body = request.encode_to_vec();
    let request_mb = request_body.len() as f64 / (1024.0 * 1024.0);

    let mut input = OtlpReceiverInput::new("bench-otlp", "127.0.0.1:0").expect("receiver binds");
    let url = format!("http://{}/v1/logs", input.local_addr());
    let poster = HttpPoster::new(concurrency);
    let metadata = generators::make_metadata();
    let mut totals = Totals::default();
    let mut total_rows = 0usize;

    let warmup_batch = warm_up(
        concurrency,
        &poster,
        &url,
        &request_body,
        case.rows,
        timeout,
        &mut input,
    );

    for _ in 0..iterations {
        let t0 = Instant::now();
        poster.post_many(&url, &request_body, concurrency);
        let batch = poll_until_batch(&mut input, case.rows * concurrency, timeout);
        totals.receiver += t0.elapsed();

        total_rows = total_rows.saturating_add(batch.num_rows());

        let t2 = Instant::now();
        let mut manual = make_otlp_sink(Compression::None);
        manual.encode_batch(&batch, &metadata);
        totals.encode_manual += t2.elapsed();

        let t3 = Instant::now();
        let mut generated = make_otlp_sink(Compression::None);
        generated.encode_batch_generated_fast(&batch, &metadata);
        totals.encode_generated += t3.elapsed();
    }

    totals.receiver_direct = run_receiver_direct(&request_body, case.rows, iterations, concurrency);

    let sink_only = run_sink_only(&warmup_batch, &metadata, iterations);
    let sink_total_rows = warmup_batch.num_rows().saturating_mul(iterations);

    println!(
        "{} [c={}]  rows={}  body={:.2} MiB",
        case.name, concurrency, case.rows, request_mb
    );
    print_stage_block(
        "receiver-direct",
        case.rows * concurrency * iterations,
        totals.receiver_direct,
        None,
    );
    print_stage_block(
        "e2e manual",
        total_rows,
        totals.receiver + totals.encode_manual,
        Some(&[
            ("receive+decode", totals.receiver),
            ("sink encode", totals.encode_manual),
        ]),
    );
    print_stage_block(
        "e2e generated-fast",
        total_rows,
        totals.receiver + totals.encode_generated,
        Some(&[
            ("receive+decode", totals.receiver),
            ("sink encode", totals.encode_generated),
        ]),
    );
    print_stage_block("sink-only manual", sink_total_rows, sink_only.0, None);
    print_stage_block(
        "sink-only generated-fast",
        sink_total_rows,
        sink_only.1,
        None,
    );
    println!();
}

fn run_receiver_direct(
    request_body: &[u8],
    expected_rows: usize,
    iterations: usize,
    concurrency: usize,
) -> Duration {
    let t0 = Instant::now();
    for _ in 0..iterations.saturating_mul(concurrency) {
        let direct_batch =
            decode_protobuf_to_batch(request_body).expect("receiver direct decode must succeed");
        assert_eq!(direct_batch.num_rows(), expected_rows);
    }
    t0.elapsed()
}

fn warm_up(
    concurrency: usize,
    poster: &HttpPoster,
    url: &str,
    request_body: &[u8],
    expected_rows: usize,
    timeout: Duration,
    input: &mut OtlpReceiverInput,
) -> arrow::record_batch::RecordBatch {
    poster.post_many(url, request_body, concurrency);
    poll_until_batch(input, expected_rows * concurrency, timeout)
}

fn run_sink_only(
    batch: &arrow::record_batch::RecordBatch,
    metadata: &logfwd_output::BatchMetadata,
    iterations: usize,
) -> (Duration, Duration) {
    let mut manual = make_otlp_sink(Compression::None);
    let t0 = Instant::now();
    for _ in 0..iterations {
        manual.encode_batch(batch, metadata);
    }
    let manual_dur = t0.elapsed();

    let mut generated = make_otlp_sink(Compression::None);
    let t1 = Instant::now();
    for _ in 0..iterations {
        generated.encode_batch_generated_fast(batch, metadata);
    }
    let generated_dur = t1.elapsed();

    (manual_dur, generated_dur)
}

fn print_stage_block(
    label: &str,
    total_rows: usize,
    total: Duration,
    stages: Option<&[(&str, Duration)]>,
) {
    if total_rows == 0 {
        println!(
            "  {:<22} {:>10} EPS  {:>10} ns/row  {:>9.3} ms total",
            label,
            "n/a",
            "n/a",
            total.as_secs_f64() * 1_000.0
        );
        return;
    }
    let secs = total.as_secs_f64();
    let eps = total_rows as f64 / secs;
    let ns_per_row = secs * 1e9 / total_rows as f64;
    println!(
        "  {:<22} {:>10.1} EPS  {:>10.1} ns/row  {:>9.3} ms total",
        label,
        eps,
        ns_per_row,
        secs * 1_000.0
    );
    if let Some(stages) = stages {
        for (stage, dur) in stages {
            println!("    {:<18} {:>9.3} ms", stage, dur.as_secs_f64() * 1_000.0);
        }
    }
}

fn poll_until_batch(
    input: &mut dyn InputSource,
    expected_rows: usize,
    timeout: Duration,
) -> arrow::record_batch::RecordBatch {
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(5);
    let max_backoff = Duration::from_millis(200);
    let mut all_batches = Vec::new();
    let mut total_batch_rows = 0usize;

    while Instant::now() < deadline {
        for event in input.poll().expect("input poll must succeed") {
            if let InputEvent::Batch { batch, .. } = event {
                total_batch_rows += batch.num_rows();
                all_batches.push(batch);
                if total_batch_rows >= expected_rows {
                    let schema = all_batches[0].schema();
                    let batch = arrow::compute::concat_batches(&schema, &all_batches)
                        .expect("benchmark batches should concatenate");
                    return batch;
                }
            }
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(max_backoff);
    }

    panic!("timed out waiting for {expected_rows} rows; only got {total_batch_rows} batch rows");
}

fn read_concurrency_list() -> Vec<usize> {
    let raw = std::env::var("OTLP_E2E_CONCURRENCY").unwrap_or_else(|_| "1".to_string());
    let mut values = Vec::new();
    let mut invalid = Vec::new();
    for part in raw.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        match token.parse::<usize>() {
            Ok(value) if value > 0 => values.push(value),
            _ => invalid.push(token.to_string()),
        }
    }
    assert!(
        invalid.is_empty(),
        "invalid OTLP_E2E_CONCURRENCY value(s): {} (expected comma-separated positive integers)",
        invalid.join(", ")
    );
    assert!(
        !values.is_empty(),
        "OTLP_E2E_CONCURRENCY must include at least one positive integer"
    );
    values.sort_unstable();
    values.dedup();
    values
}

fn build_request(rows: usize, extra_string_attrs: usize) -> ExportLogsServiceRequest {
    let mut log_records = Vec::with_capacity(rows);
    for i in 0..rows {
        let mut attributes = vec![
            kv_str("host", &format!("host-{}", i % 8)),
            kv_i64("status", 200 + (i % 5) as i64),
            kv_f64("duration_ms", (i % 100) as f64 * 1.25),
            kv_bool("success", i % 11 != 0),
        ];
        for idx in 0..extra_string_attrs {
            attributes.push(kv_str(
                &format!("attr_{idx}"),
                &format!("v{idx}-{}", i % 17),
            ));
        }

        log_records.push(LogRecord {
            time_unix_nano: 1_712_509_200_123_456_789 + i as u64,
            severity_text: match i % 4 {
                0 => "INFO".into(),
                1 => "WARN".into(),
                2 => "ERROR".into(),
                _ => "DEBUG".into(),
            },
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!(
                    "request {i} completed with synthetic payload"
                ))),
            }),
            trace_id: if i % 5 == 0 {
                Vec::new()
            } else {
                vec![
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                    0xdd, 0xee, 0xff,
                ]
            },
            span_id: if i % 7 == 0 {
                Vec::new()
            } else {
                vec![0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77]
            },
            flags: 1,
            attributes,
            ..Default::default()
        });
    }

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    kv_str("service.name", "bench-service"),
                    kv_str("service.version", "1.0.0"),
                    kv_str("host.name", "bench-node-01"),
                ],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "bench-generator".into(),
                    version: "1.0.0".into(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

fn kv_i64(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
    }
}

fn kv_f64(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
    }
}

fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::BoolValue(value)),
        }),
    }
}
