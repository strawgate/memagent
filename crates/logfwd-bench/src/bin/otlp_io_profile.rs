use std::fs::File;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use logfwd_bench::{make_metadata, make_otlp_sink};
use logfwd_io::otlp_receiver::{
    decode_protobuf_bytes_to_batch_projected_only_experimental, decode_protobuf_to_batch,
    decode_protobuf_to_batch_projected_detached_experimental,
    decode_protobuf_to_batch_prost_reference,
};
use logfwd_output::Compression;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;

#[cfg(feature = "otlp-profile-alloc")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[cfg(feature = "otlp-profile-alloc")]
#[global_allocator]
static GLOBAL: &StatsAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

const NANOS_BASE: u64 = 1_712_509_200_123_456_789;

#[derive(Clone, Copy)]
struct FixtureProfile {
    name: &'static str,
    rows: usize,
    attrs_per_record: usize,
    resource_attrs: usize,
    body_len: usize,
}

impl FixtureProfile {
    fn by_name(name: &str) -> Self {
        match name {
            "attrs-heavy" => Self {
                name: "attrs-heavy",
                rows: 2_000,
                attrs_per_record: 40,
                resource_attrs: 8,
                body_len: 96,
            },
            "wide-10k" => Self {
                name: "wide-10k",
                rows: 10_000,
                attrs_per_record: 20,
                resource_attrs: 6,
                body_len: 240,
            },
            _ => panic!("unknown fixture {name}; expected attrs-heavy or wide-10k"),
        }
    }
}

#[derive(Clone, Copy)]
enum Mode {
    ProstReferenceToBatch,
    ProductionCurrentToBatch,
    ProjectedDetachedToBatch,
    ProjectedViewToBatch,
    E2eProstReference,
    E2eProductionCurrent,
    E2eProjectedDetached,
    E2eProjectedView,
}

impl Mode {
    const ALL: [Mode; 8] = [
        Mode::ProstReferenceToBatch,
        Mode::ProductionCurrentToBatch,
        Mode::ProjectedDetachedToBatch,
        Mode::ProjectedViewToBatch,
        Mode::E2eProstReference,
        Mode::E2eProductionCurrent,
        Mode::E2eProjectedDetached,
        Mode::E2eProjectedView,
    ];

    fn name(self) -> &'static str {
        match self {
            Mode::ProstReferenceToBatch => "prost_reference_to_batch",
            Mode::ProductionCurrentToBatch => "production_current_to_batch",
            Mode::ProjectedDetachedToBatch => "projected_detached_to_batch",
            Mode::ProjectedViewToBatch => "projected_view_to_batch",
            Mode::E2eProstReference => "e2e_prost_reference",
            Mode::E2eProductionCurrent => "e2e_production_current",
            Mode::E2eProjectedDetached => "e2e_projected_detached",
            Mode::E2eProjectedView => "e2e_projected_view",
        }
    }

    fn by_name(name: &str) -> Self {
        match name {
            // canonical names
            "prost_reference_to_batch" => Mode::ProstReferenceToBatch,
            "production_current_to_batch" => Mode::ProductionCurrentToBatch,
            "projected_detached_to_batch" => Mode::ProjectedDetachedToBatch,
            "projected_view_to_batch" => Mode::ProjectedViewToBatch,
            "e2e_prost_reference" => Mode::E2eProstReference,
            "e2e_production_current" => Mode::E2eProductionCurrent,
            "e2e_projected_detached" => Mode::E2eProjectedDetached,
            "e2e_projected_view" => Mode::E2eProjectedView,
            // deprecated aliases from earlier naming
            "prost_decode" => Mode::ProstReferenceToBatch,
            "production_to_batch" => Mode::ProductionCurrentToBatch,
            "projected_detached_decode" => Mode::ProjectedDetachedToBatch,
            "projected_view_decode" => Mode::ProjectedViewToBatch,
            "e2e_prost" => Mode::E2eProstReference,
            "e2e_production" => Mode::E2eProductionCurrent,
            _ => panic!("unknown mode {name}; run with --help to list supported modes"),
        }
    }
}

struct Cli {
    case: FixtureProfile,
    mode: Option<Mode>,
    iterations: usize,
    flamegraph: Option<PathBuf>,
}

struct FixtureData {
    payload: Vec<u8>,
    payload_bytes: Bytes,
    rows: usize,
}

struct ProfileResult {
    mode: &'static str,
    iterations: usize,
    rows: usize,
    elapsed: Duration,
    bytes_allocated: u64,
    allocations: u64,
    bytes_deallocated: u64,
}

#[derive(Default)]
struct AllocStats {
    bytes_allocated: u64,
    allocations: u64,
    bytes_deallocated: u64,
}

fn main() {
    let cli = Cli::parse();
    let fixture = build_fixture(cli.case);

    println!(
        "# OTLP IO profile\ncase={} rows={} iterations={}\n",
        cli.case.name, fixture.rows, cli.iterations
    );

    if cli.flamegraph.is_some() && cli.mode.is_none() {
        eprintln!("--flamegraph requires --mode so each profile writes to a distinct path");
        std::process::exit(2);
    }

    let modes: Vec<Mode> = match cli.mode {
        Some(mode) => vec![mode],
        None => Mode::ALL.to_vec(),
    };

    for mode in modes {
        let result = if let Some(path) = cli.flamegraph.as_ref() {
            run_with_flamegraph(&fixture, mode, cli.iterations, path)
        } else {
            run_profile(&fixture, mode, cli.iterations)
        };
        print_result(&result);
    }
}

impl Cli {
    fn parse() -> Self {
        let mut case = FixtureProfile::by_name("attrs-heavy");
        let mut mode = None;
        let mut iterations = 1_000usize;
        let mut flamegraph = None;
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--case" => {
                    let value = args.next().expect("--case requires a value");
                    case = FixtureProfile::by_name(&value);
                }
                "--mode" => {
                    let value = args.next().expect("--mode requires a value");
                    mode = Some(Mode::by_name(&value));
                }
                "--iterations" => {
                    let value = args.next().expect("--iterations requires a value");
                    iterations = value.parse().expect("--iterations must be a number");
                }
                "--flamegraph" => {
                    let value = args.next().expect("--flamegraph requires a path");
                    flamegraph = Some(PathBuf::from(value));
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => panic!("unknown argument {arg}"),
            }
        }

        Self {
            case,
            mode,
            iterations,
            flamegraph,
        }
    }
}

fn print_usage() {
    eprintln!(
        "Usage: otlp_io_profile [--case attrs-heavy|wide-10k] [--mode MODE] [--iterations N] [--flamegraph PATH]"
    );
    eprintln!("Modes:");
    for mode in Mode::ALL {
        eprintln!("  {}", mode.name());
    }
    eprintln!("Deprecated aliases:");
    eprintln!("  prost_decode => prost_reference_to_batch");
    eprintln!("  production_to_batch => production_current_to_batch");
    eprintln!("  projected_detached_decode => projected_detached_to_batch");
    eprintln!("  projected_view_decode => projected_view_to_batch");
    eprintln!("  e2e_prost => e2e_prost_reference");
    eprintln!("  e2e_production => e2e_production_current");
}

fn run_with_flamegraph(
    fixture: &FixtureData,
    mode: Mode,
    iterations: usize,
    path: &PathBuf,
) -> ProfileResult {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1_000)
        .blocklist(&["libc", "libgcc", "pthread"])
        .build()
        .expect("pprof guard");
    let result = run_profile(fixture, mode, iterations);
    if let Ok(report) = guard.report().build() {
        let file = File::create(path).expect("create flamegraph");
        report.flamegraph(file).expect("write flamegraph");
        println!("flamegraph={}", path.display());
    }
    result
}

fn run_profile(fixture: &FixtureData, mode: Mode, iterations: usize) -> ProfileResult {
    // Warm reusable encoder buffers before allocation measurement so this
    // captures steady-state decode/encode work, not sink capacity growth.
    let metadata = make_metadata();
    let mut sink = make_otlp_sink(Compression::None);
    if matches!(
        mode,
        Mode::E2eProstReference
            | Mode::E2eProductionCurrent
            | Mode::E2eProjectedDetached
            | Mode::E2eProjectedView
    ) {
        let batch = match mode {
            Mode::E2eProstReference => decode_protobuf_to_batch_prost_reference(&fixture.payload)
                .expect("warmup prost reference decode"),
            Mode::E2eProductionCurrent => {
                decode_protobuf_to_batch(&fixture.payload).expect("warmup production decode")
            }
            Mode::E2eProjectedDetached => {
                decode_protobuf_to_batch_projected_detached_experimental(&fixture.payload)
                    .expect("warmup projected decode")
            }
            Mode::E2eProjectedView => decode_protobuf_bytes_to_batch_projected_only_experimental(
                fixture.payload_bytes.clone(),
            )
            .expect("warmup projected view decode"),
            Mode::ProstReferenceToBatch
            | Mode::ProductionCurrentToBatch
            | Mode::ProjectedDetachedToBatch
            | Mode::ProjectedViewToBatch => unreachable!("decode-only modes are not warmed here"),
        };
        sink.encode_batch(&batch, &metadata);
    }

    #[cfg(feature = "otlp-profile-alloc")]
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let started = Instant::now();
    for _ in 0..iterations {
        match mode {
            Mode::ProstReferenceToBatch => {
                let batch =
                    decode_protobuf_to_batch_prost_reference(&fixture.payload).expect("prost");
                black_box(batch.num_rows());
            }
            Mode::ProductionCurrentToBatch => {
                let batch = decode_protobuf_to_batch(&fixture.payload).expect("production");
                black_box(batch.num_rows());
            }
            Mode::ProjectedDetachedToBatch => {
                let batch =
                    decode_protobuf_to_batch_projected_detached_experimental(&fixture.payload)
                        .expect("projected detached");
                black_box(batch.num_rows());
            }
            Mode::ProjectedViewToBatch => {
                let batch = decode_protobuf_bytes_to_batch_projected_only_experimental(
                    fixture.payload_bytes.clone(),
                )
                .expect("projected view");
                black_box(batch.num_rows());
            }
            Mode::E2eProstReference => {
                let batch =
                    decode_protobuf_to_batch_prost_reference(&fixture.payload).expect("prost");
                sink.encode_batch(&batch, &metadata);
                black_box(sink.encoded_payload().len());
            }
            Mode::E2eProductionCurrent => {
                let batch = decode_protobuf_to_batch(&fixture.payload).expect("production");
                sink.encode_batch(&batch, &metadata);
                black_box(sink.encoded_payload().len());
            }
            Mode::E2eProjectedDetached => {
                let batch =
                    decode_protobuf_to_batch_projected_detached_experimental(&fixture.payload)
                        .expect("projected detached");
                sink.encode_batch(&batch, &metadata);
                black_box(sink.encoded_payload().len());
            }
            Mode::E2eProjectedView => {
                let batch = decode_protobuf_bytes_to_batch_projected_only_experimental(
                    fixture.payload_bytes.clone(),
                )
                .expect("projected view");
                sink.encode_batch(&batch, &metadata);
                black_box(sink.encoded_payload().len());
            }
        }
    }
    let elapsed = started.elapsed();

    #[cfg(feature = "otlp-profile-alloc")]
    let stats = region.change();
    #[cfg(feature = "otlp-profile-alloc")]
    let stats = AllocStats {
        bytes_allocated: stats.bytes_allocated as u64,
        allocations: stats.allocations as u64,
        bytes_deallocated: stats.bytes_deallocated as u64,
    };
    #[cfg(not(feature = "otlp-profile-alloc"))]
    let stats = AllocStats::default();

    ProfileResult {
        mode: mode.name(),
        iterations,
        rows: fixture.rows.saturating_mul(iterations),
        elapsed,
        bytes_allocated: stats.bytes_allocated,
        allocations: stats.allocations,
        bytes_deallocated: stats.bytes_deallocated,
    }
}

fn print_result(result: &ProfileResult) {
    let rows = result.rows.max(1) as f64;
    let secs = result.elapsed.as_secs_f64();
    println!(
        "| {} | {:.3} ms | {:.1} ns/row | {:.1} MiB alloc | {:.1} B/row | {:.3} allocs/row | net {:.1} MiB |",
        result.mode,
        secs * 1_000.0,
        secs * 1e9 / rows,
        result.bytes_allocated as f64 / 1_048_576.0,
        result.bytes_allocated as f64 / rows,
        result.allocations as f64 / rows,
        (result.bytes_allocated as i128 - result.bytes_deallocated as i128) as f64 / 1_048_576.0,
    );
    println!(
        "  iterations={} rows={} allocations={}",
        result.iterations, result.rows, result.allocations
    );
}

fn build_fixture(profile: FixtureProfile) -> FixtureData {
    let payload = build_request(profile).encode_to_vec();
    let payload_bytes = Bytes::from(payload.clone());
    let batch = decode_protobuf_to_batch_prost_reference(&payload).expect("fixture decodes");
    assert_eq!(batch.num_rows(), profile.rows);
    let production = decode_protobuf_to_batch(&payload).expect("production fixture decodes");
    assert_batch_matches(&batch, &production, profile.name);
    assert_encode_paths_match(&production, profile.name);
    let projected = decode_protobuf_to_batch_projected_detached_experimental(&payload)
        .expect("projected fixture decodes");
    assert_batch_matches(&batch, &projected, profile.name);
    assert_encode_paths_match(&projected, profile.name);
    let view = decode_protobuf_bytes_to_batch_projected_only_experimental(payload_bytes.clone())
        .expect("view fixture decodes");
    let detached_view = logfwd_arrow::materialize::detach(&view);
    assert_batch_matches(&batch, &detached_view, profile.name);
    assert_encode_paths_match(&view, profile.name);

    FixtureData {
        payload,
        payload_bytes,
        rows: profile.rows,
    }
}

fn assert_encode_paths_match(batch: &arrow::record_batch::RecordBatch, fixture_name: &str) {
    let metadata = make_metadata();
    let mut handwritten_sink = make_otlp_sink(Compression::None);
    handwritten_sink.encode_batch(batch, &metadata);
    let handwritten_request = ExportLogsServiceRequest::decode(handwritten_sink.encoded_payload())
        .expect("handwritten OTLP encode path should produce protobuf");

    let mut generated_sink = make_otlp_sink(Compression::None);
    generated_sink.encode_batch_generated_fast(batch, &metadata);
    let generated_request = ExportLogsServiceRequest::decode(generated_sink.encoded_payload())
        .expect("generated-fast OTLP encode path should produce protobuf");

    assert_eq!(
        handwritten_request, generated_request,
        "generated-fast OTLP encode must match handwritten encode for profile fixture {fixture_name}"
    );
}

fn assert_batch_matches(
    expected: &arrow::record_batch::RecordBatch,
    actual: &arrow::record_batch::RecordBatch,
    fixture_name: &str,
) {
    assert_eq!(
        expected.schema(),
        actual.schema(),
        "profile fixture schema must match prost batch for {fixture_name}"
    );
    assert_eq!(
        expected.num_rows(),
        actual.num_rows(),
        "profile fixture row count must match prost batch for {fixture_name}"
    );
    assert_eq!(
        expected.num_columns(),
        actual.num_columns(),
        "profile fixture column count must match prost batch for {fixture_name}"
    );
    for (idx, (expected_column, actual_column)) in
        expected.columns().iter().zip(actual.columns()).enumerate()
    {
        assert_eq!(
            expected_column.to_data(),
            actual_column.to_data(),
            "profile fixture column {idx} must match prost batch for {fixture_name}"
        );
    }
}

fn build_request(profile: FixtureProfile) -> ExportLogsServiceRequest {
    let mut records = Vec::with_capacity(profile.rows);
    for row in 0..profile.rows {
        records.push(LogRecord {
            time_unix_nano: NANOS_BASE + row as u64,
            observed_time_unix_nano: NANOS_BASE + row as u64 + 123,
            severity_text: severity_text(row).to_string(),
            severity_number: severity_number(row),
            body: Some(AnyValue {
                value: Some(Value::StringValue(make_message(row, profile.body_len))),
            }),
            trace_id: trace_id_for(row),
            span_id: span_id_for(row),
            flags: 1,
            attributes: make_record_attrs(profile, row),
            ..Default::default()
        });
    }

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: make_resource_attrs(profile),
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "bench-scope-0-0".to_string(),
                    version: "2026.04".to_string(),
                    attributes: vec![kv_string("scope.env", "bench")],
                    dropped_attributes_count: 0,
                }),
                log_records: records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn make_resource_attrs(profile: FixtureProfile) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(profile.resource_attrs.max(4));
    attrs.push(kv_string("service.name", "checkout"));
    attrs.push(kv_string("service.namespace", "payments"));
    attrs.push(kv_string("deployment.environment", "benchmark"));
    attrs.push(kv_string("host.name", "bench-host-0"));
    for idx in 0..profile.resource_attrs.saturating_sub(4) {
        attrs.push(kv_string(
            format!("resource.extra.{idx}"),
            format!("resource-value-{idx}"),
        ));
    }
    attrs
}

fn make_record_attrs(profile: FixtureProfile, row: usize) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(profile.attrs_per_record);
    attrs.push(kv_string("http.method", method(row)));
    attrs.push(kv_string(
        "http.route",
        format!("/api/v1/item/{}", row % 128),
    ));
    attrs.push(kv_int("http.status_code", status_code(row)));
    attrs.push(kv_bool("cache.hit", row.is_multiple_of(3)));
    for idx in 0..profile.attrs_per_record.saturating_sub(4) {
        match idx % 5 {
            0 => attrs.push(kv_string(
                format!("attr.string.{idx}"),
                format!("value-{row}-{idx}"),
            )),
            1 => attrs.push(kv_int(format!("attr.int.{idx}"), (row * (idx + 1)) as i64)),
            2 => attrs.push(kv_bool(
                format!("attr.bool.{idx}"),
                (row + idx).is_multiple_of(2),
            )),
            3 => attrs.push(kv_double(
                format!("attr.double.{idx}"),
                row as f64 * 0.25 + idx as f64,
            )),
            _ => attrs.push(kv_bytes(
                format!("attr.bytes.{idx}"),
                vec![row as u8, idx as u8, (row >> 8) as u8, 0xab],
            )),
        }
    }
    attrs
}

fn kv_string(key: impl Into<String>, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
    }
}

fn kv_int(key: impl Into<String>, value: i64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
    }
}

fn kv_bool(key: impl Into<String>, value: bool) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::BoolValue(value)),
        }),
    }
}

fn kv_double(key: impl Into<String>, value: f64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
    }
}

fn kv_bytes(key: impl Into<String>, value: Vec<u8>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::BytesValue(value)),
        }),
    }
}

fn severity_text(row: usize) -> &'static str {
    match row % 5 {
        0 => "TRACE",
        1 => "DEBUG",
        2 => "INFO",
        3 => "WARN",
        _ => "ERROR",
    }
}

fn severity_number(row: usize) -> i32 {
    match row % 5 {
        0 => 1,
        1 => 5,
        2 => 9,
        3 => 13,
        _ => 17,
    }
}

fn method(row: usize) -> &'static str {
    match row % 4 {
        0 => "GET",
        1 => "POST",
        2 => "PUT",
        _ => "DELETE",
    }
}

fn status_code(row: usize) -> i64 {
    match row % 20 {
        0 => 500,
        1 | 2 => 404,
        _ => 200,
    }
}

fn make_message(row: usize, len: usize) -> String {
    let mut message = format!("benchmark row={row} subsystem=otlp_io ");
    while message.len() < len {
        message.push_str("payload ");
    }
    message.truncate(len);
    message
}

fn trace_id_for(row: usize) -> Vec<u8> {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&(row as u64).to_be_bytes());
    bytes[8..].copy_from_slice(&((row as u64).wrapping_mul(0x9e37)).to_be_bytes());
    bytes.to_vec()
}

fn span_id_for(row: usize) -> Vec<u8> {
    (row as u64)
        .wrapping_mul(0x517c_c1b7_2722_0a95)
        .to_be_bytes()
        .to_vec()
}
