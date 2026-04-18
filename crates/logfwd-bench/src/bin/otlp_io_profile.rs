use std::fs::File;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, StructArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};

use bytes::Bytes;
use logfwd_bench::generators::otlp::{self, OtlpFixtureProfile};
use logfwd_bench::{generators, make_otlp_sink};
use logfwd_io::otlp_receiver::{
    ProjectedOtlpDecoder, decode_protobuf_bytes_to_batch_projected_only_experimental,
    decode_protobuf_to_batch, decode_protobuf_to_batch_projected_detached_experimental,
    decode_protobuf_to_batch_prost_reference,
};
use logfwd_output::Compression;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message as _;

#[cfg(feature = "otlp-profile-alloc")]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[cfg(feature = "otlp-profile-alloc")]
#[global_allocator]
static GLOBAL: &StatsAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

#[derive(Clone, Copy)]
enum Mode {
    ProstReferenceToBatch,
    ProductionCurrentToBatch,
    ProjectedDetachedToBatch,
    ProjectedViewToBatch,
    ProjectedViewReuseToBatch,
    E2eProstReference,
    E2eProductionCurrent,
    E2eProjectedDetached,
    E2eProjectedView,
}

impl Mode {
    const ALL: [Mode; 9] = [
        Mode::ProstReferenceToBatch,
        Mode::ProductionCurrentToBatch,
        Mode::ProjectedDetachedToBatch,
        Mode::ProjectedViewToBatch,
        Mode::ProjectedViewReuseToBatch,
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
            Mode::ProjectedViewReuseToBatch => "projected_view_reuse_to_batch",
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
            "projected_view_reuse_to_batch" => Mode::ProjectedViewReuseToBatch,
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
    case: OtlpFixtureProfile,
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
        let mut case = OtlpFixtureProfile::by_name("attrs-heavy").expect("attrs-heavy profile");
        let mut mode = None;
        let mut iterations = 1_000usize;
        let mut flamegraph = None;
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--case" => {
                    let value = args.next().expect("--case requires a value");
                    case = OtlpFixtureProfile::by_name(&value).unwrap_or_else(|| {
                        panic!(
                            "unknown fixture {value}; available: {:?}",
                            otlp::ALL_PROFILES
                                .iter()
                                .map(|p| p.name)
                                .collect::<Vec<_>>()
                        )
                    });
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
        "Usage: otlp_io_profile [--case PROFILE] [--mode MODE] [--iterations N] [--flamegraph PATH]"
    );
    eprintln!("Profiles:");
    for profile in &otlp::ALL_PROFILES {
        eprintln!("  {}", profile.name);
    }
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
    let metadata = generators::make_metadata();
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
            | Mode::ProjectedViewToBatch
            | Mode::ProjectedViewReuseToBatch => {
                unreachable!("decode-only modes are not warmed here")
            }
        };
        sink.encode_batch(&batch, &metadata);
    }

    // For reuse mode, warm the decoder with one batch before timing starts.
    let mut reuse_decoder = if matches!(mode, Mode::ProjectedViewReuseToBatch) {
        let mut dec = ProjectedOtlpDecoder::new("resource.");
        let _ = dec
            .decode_view_bytes(fixture.payload_bytes.clone())
            .expect("warmup reuse decoder");
        Some(dec)
    } else {
        None
    };

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
            Mode::ProjectedViewReuseToBatch => {
                let dec = reuse_decoder.as_mut().expect("reuse decoder initialized");
                let batch = dec
                    .decode_view_bytes(fixture.payload_bytes.clone())
                    .expect("projected view reuse");
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

fn build_fixture(profile: OtlpFixtureProfile) -> FixtureData {
    let otlp_data = otlp::build_fixture(profile);
    let batch =
        decode_protobuf_to_batch_prost_reference(&otlp_data.payload).expect("fixture decodes");
    assert_eq!(batch.num_rows(), profile.total_rows());
    let production =
        decode_protobuf_to_batch(&otlp_data.payload).expect("production fixture decodes");
    assert_batch_matches(&batch, &production, profile.name);
    assert_encode_paths_match(&production, profile.name);
    if !profile.has_complex_any {
        let projected =
            decode_protobuf_to_batch_projected_detached_experimental(&otlp_data.payload)
                .expect("projected fixture decodes");
        assert_batch_matches(&batch, &projected, profile.name);
        assert_encode_paths_match(&projected, profile.name);
        let view = decode_protobuf_bytes_to_batch_projected_only_experimental(
            otlp_data.payload_bytes.clone(),
        )
        .expect("view fixture decodes");
        let detached_view = logfwd_arrow::materialize::detach(&view);
        assert_batch_matches(&batch, &detached_view, profile.name);
        assert_encode_paths_match(&view, profile.name);
    }

    FixtureData {
        payload: otlp_data.payload,
        payload_bytes: otlp_data.payload_bytes,
        rows: profile.total_rows(),
    }
}

fn assert_encode_paths_match(batch: &arrow::record_batch::RecordBatch, fixture_name: &str) {
    let metadata = generators::make_metadata();
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

/// Recursively cast Utf8View → Utf8 in an array, including Struct children.
fn normalize_utf8view_array(arr: &dyn Array) -> Arc<dyn Array> {
    match arr.data_type() {
        DataType::Utf8View => cast(arr, &DataType::Utf8).expect("cast Utf8View→Utf8"),
        DataType::Struct(fields) => {
            let struct_arr = arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct downcast");
            let new_fields: Vec<_> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let child = normalize_utf8view_array(struct_arr.column(i).as_ref());
                    let new_dt = child.data_type().clone();
                    let new_field = if &new_dt == f.data_type() {
                        Arc::clone(f)
                    } else {
                        Arc::new(f.as_ref().clone().with_data_type(new_dt))
                    };
                    (new_field, child)
                })
                .collect();
            let (new_field_refs, new_arrays): (Vec<_>, Vec<_>) = new_fields.into_iter().unzip();
            Arc::new(
                StructArray::try_new(
                    new_field_refs.into(),
                    new_arrays,
                    struct_arr.nulls().cloned(),
                )
                .expect("struct rebuild"),
            )
        }
        _ => make_array(arr.to_data()),
    }
}

/// Normalize a batch by recursively casting Utf8View columns to Utf8.
/// ColumnarBatchBuilder produces Utf8View; prost/StreamingBuilder produces Utf8.
fn normalize_utf8view(
    batch: &arrow::record_batch::RecordBatch,
) -> arrow::record_batch::RecordBatch {
    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let normalized = normalize_utf8view_array(col.as_ref());
        let new_dt = normalized.data_type().clone();
        // Preserve field metadata — only change data_type when it differs.
        let new_field = if &new_dt == field.data_type() {
            Arc::clone(field)
        } else {
            Arc::new(field.as_ref().clone().with_data_type(new_dt))
        };
        new_fields.push(new_field);
        new_columns.push(normalized);
    }
    let new_schema = Arc::new(Schema::new(new_fields));
    arrow::record_batch::RecordBatch::try_new(new_schema, new_columns).expect("normalized batch")
}

fn assert_batch_matches(
    expected: &arrow::record_batch::RecordBatch,
    actual: &arrow::record_batch::RecordBatch,
    fixture_name: &str,
) {
    let actual = normalize_utf8view(actual);
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
