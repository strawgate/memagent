use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use logfwd_arrow::scanner::Scanner;
use logfwd_bench::{generators, make_otlp_sink};
use logfwd_core::scan_config::ScanConfig;
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::otlp_receiver::OtlpReceiverInput;
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
    fn new() -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(1)
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
}

struct Case {
    name: &'static str,
    rows: usize,
    extra_string_attrs: usize,
}

#[derive(Clone, Copy)]
enum ReceiverMode {
    LegacyJsonLines,
    StructuredBatch,
}

impl ReceiverMode {
    fn label(self) -> &'static str {
        match self {
            ReceiverMode::LegacyJsonLines => "legacy-jsonlines",
            ReceiverMode::StructuredBatch => "structured-batch",
        }
    }

    fn matches_filter(self, filter: Option<&str>) -> bool {
        match filter {
            None => true,
            Some("legacy") | Some("legacy-jsonlines") => {
                matches!(self, ReceiverMode::LegacyJsonLines)
            }
            Some("structured") | Some("structured-batch") => {
                matches!(self, ReceiverMode::StructuredBatch)
            }
            Some(_) => false,
        }
    }
}

#[derive(Default)]
struct Totals {
    receiver: Duration,
    scan: Duration,
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
    let case_filter = std::env::var("OTLP_E2E_CASE").ok();
    let mode_filter = std::env::var("OTLP_E2E_MODE").ok();
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

    println!("=== OTLP Receiver -> Scanner -> OtlpSink EPS Profile (iters={iterations}) ===\n");

    for case in cases {
        for mode in [ReceiverMode::LegacyJsonLines, ReceiverMode::StructuredBatch] {
            if case_filter.as_deref().is_some_and(|name| name != case.name) {
                continue;
            }
            if !mode.matches_filter(mode_filter.as_deref()) {
                continue;
            }
            run_case(mode, &case, iterations, timeout);
        }
    }
}

fn run_case(mode: ReceiverMode, case: &Case, iterations: usize, timeout: Duration) {
    let request = build_request(case.rows, case.extra_string_attrs);
    let request_body = request.encode_to_vec();
    let request_mb = request_body.len() as f64 / (1024.0 * 1024.0);

    let mut input = match mode {
        ReceiverMode::LegacyJsonLines => {
            OtlpReceiverInput::new("bench-otlp", "127.0.0.1:0").expect("receiver binds")
        }
        ReceiverMode::StructuredBatch => {
            OtlpReceiverInput::new_structured("bench-otlp", "127.0.0.1:0").expect("receiver binds")
        }
    };
    let url = format!("http://{}/v1/logs", input.local_addr());
    let poster = HttpPoster::new();
    let metadata = generators::make_metadata();
    let mut totals = Totals::default();

    let scanned_batch = warm_up(
        mode,
        &poster,
        &url,
        &request_body,
        case.rows,
        timeout,
        &mut input,
    );

    for _ in 0..iterations {
        let t0 = Instant::now();
        poster.post(&url, &request_body);
        let payload = poll_until_payload(&mut input, case.rows, timeout);
        totals.receiver += t0.elapsed();

        let batch = match payload {
            ReceiverOutput::JsonLines(lines) => {
                let t1 = Instant::now();
                let mut scanner = Scanner::new(ScanConfig::default());
                let batch = scanner
                    .scan(Bytes::from(lines))
                    .expect("receiver output must scan");
                totals.scan += t1.elapsed();
                batch
            }
            ReceiverOutput::Batch(batch) => batch,
        };

        let t2 = Instant::now();
        let mut manual = make_otlp_sink(Compression::None);
        manual.encode_batch(&batch, &metadata);
        totals.encode_manual += t2.elapsed();

        let t3 = Instant::now();
        let mut generated = make_otlp_sink(Compression::None);
        generated.encode_batch_generated_fast(&batch, &metadata);
        totals.encode_generated += t3.elapsed();
    }

    let sink_only = run_sink_only(&scanned_batch, &metadata, iterations);

    println!(
        "{} [{}]  rows={}  body={:.2} MiB",
        case.name,
        mode.label(),
        case.rows,
        request_mb
    );
    print_stage_block(
        "e2e manual",
        case.rows,
        iterations,
        totals.receiver + totals.scan + totals.encode_manual,
        Some(&[
            ("receive+decode", totals.receiver),
            ("scan", totals.scan),
            ("sink encode", totals.encode_manual),
        ]),
    );
    print_stage_block(
        "e2e generated-fast",
        case.rows,
        iterations,
        totals.receiver + totals.scan + totals.encode_generated,
        Some(&[
            ("receive+decode", totals.receiver),
            ("scan", totals.scan),
            ("sink encode", totals.encode_generated),
        ]),
    );
    print_stage_block("sink-only manual", case.rows, iterations, sink_only.0, None);
    print_stage_block(
        "sink-only generated-fast",
        case.rows,
        iterations,
        sink_only.1,
        None,
    );
    println!();
}

fn warm_up(
    mode: ReceiverMode,
    poster: &HttpPoster,
    url: &str,
    request_body: &[u8],
    expected_rows: usize,
    timeout: Duration,
    input: &mut OtlpReceiverInput,
) -> arrow::record_batch::RecordBatch {
    poster.post(url, request_body);
    match poll_until_payload(input, expected_rows, timeout) {
        ReceiverOutput::JsonLines(lines) => {
            debug_assert!(matches!(mode, ReceiverMode::LegacyJsonLines));
            let mut scanner = Scanner::new(ScanConfig::default());
            scanner
                .scan(Bytes::from(lines))
                .expect("warmup receiver output must scan")
        }
        ReceiverOutput::Batch(batch) => {
            debug_assert!(matches!(mode, ReceiverMode::StructuredBatch));
            batch
        }
    }
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
    rows: usize,
    iterations: usize,
    total: Duration,
    stages: Option<&[(&str, Duration)]>,
) {
    let total_rows = rows * iterations;
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

enum ReceiverOutput {
    JsonLines(Vec<u8>),
    Batch(arrow::record_batch::RecordBatch),
}

fn poll_until_payload(
    input: &mut dyn InputSource,
    expected_rows: usize,
    timeout: Duration,
) -> ReceiverOutput {
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(5);
    let max_backoff = Duration::from_millis(200);
    let mut all_lines = Vec::new();
    let mut all_batches = Vec::new();
    let mut total_batch_rows = 0usize;

    while Instant::now() < deadline {
        for event in input.poll().expect("input poll must succeed") {
            match event {
                InputEvent::Data { bytes, .. } => {
                    all_lines.extend_from_slice(&bytes);
                }
                InputEvent::Batch { batch, .. } => {
                    total_batch_rows += batch.num_rows();
                    all_batches.push(batch);
                    if total_batch_rows >= expected_rows {
                        let schema = all_batches[0].schema();
                        let batch = arrow::compute::concat_batches(&schema, &all_batches)
                            .expect("benchmark batches should concatenate");
                        return ReceiverOutput::Batch(batch);
                    }
                }
                InputEvent::Rotated { .. }
                | InputEvent::Truncated { .. }
                | InputEvent::EndOfFile { .. } => {}
            }
        }
        if newline_count(&all_lines) >= expected_rows {
            return ReceiverOutput::JsonLines(all_lines);
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(max_backoff);
    }

    panic!(
        "timed out waiting for {expected_rows} rows; only got {} lines / {} batch rows",
        newline_count(&all_lines),
        total_batch_rows
    );
}

fn newline_count(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&b| b == b'\n').count()
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
