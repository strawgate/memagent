//! Criterion benchmark for generator Arrow-direct output vs JSON->Scanner.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use ffwd_arrow::Scanner;
use ffwd_core::scan_config::ScanConfig;
use ffwd_io::generator::{
    GeneratorComplexity, GeneratorConfig, GeneratorInput, GeneratorProfile, GeneratorTimestamp,
    generate_arrow_batch,
};
use ffwd_io::input::{InputSource, SourceEvent};

fn bench_generator_arrow_vs_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("generator_arrow_vs_json");
    let timestamp = GeneratorTimestamp::default();

    for &rows in &[1_000usize, 10_000usize] {
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::new("arrow_direct", rows), &rows, |b, &rows| {
            b.iter(|| {
                std::hint::black_box(generate_arrow_batch(rows, 0, &timestamp, None));
            });
        });

        group.bench_with_input(BenchmarkId::new("json_scanner", rows), &rows, |b, &rows| {
            b.iter(|| {
                let mut generator = GeneratorInput::new(
                    "bench-generator",
                    GeneratorConfig {
                        profile: GeneratorProfile::Logs,
                        complexity: GeneratorComplexity::Simple,
                        batch_size: rows,
                        total_events: rows as u64,
                        timestamp: timestamp.clone(),
                        ..Default::default()
                    },
                );
                let events = generator.poll().expect("generator poll should succeed");
                let SourceEvent::Data { bytes, .. } = events
                    .into_iter()
                    .next()
                    .expect("finite generator should emit one batch")
                else {
                    panic!("generator benchmark expected a Data event");
                };
                let mut scanner = Scanner::new(ScanConfig {
                    extract_all: true,
                    ..Default::default()
                });
                std::hint::black_box(
                    scanner
                        .scan_detached(Bytes::from(bytes.to_vec()))
                        .expect("scanner benchmark input should be valid"),
                );
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_generator_arrow_vs_json);
criterion_main!(benches);
