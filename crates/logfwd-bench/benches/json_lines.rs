//! JSON lines output encoding benchmarks.
//!
//! Focuses on the `write_row_json` hot path and the `StdoutSink::write_batch_to`
//! batch-level path for the JSON format:
//!
//! - **write_row_json/narrow/10k** — tight loop over 10K narrow rows (5 fields)
//! - **write_row_json/wide/10k**   — tight loop over 10K wide rows (20 fields)
//! - **write_row_json_resolved/narrow/10k** — pre-resolved columns, 10K narrow
//! - **write_batch_to/narrow/10k** — `StdoutSink::write_batch_to` JSON, 10K narrow rows
//! - **write_batch_to/wide/10k**   — `StdoutSink::write_batch_to` JSON, 10K wide rows

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use logfwd_bench::generators;
use logfwd_output::{
    BatchMetadata, StdoutFormat, StdoutSink, build_col_infos, resolve_col_infos, write_row_json,
    write_row_json_resolved,
};
use logfwd_types::diagnostics::ComponentStats;

fn bench_json_lines(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_lines");
    group.sample_size(20);

    let meta = generators::make_metadata();

    let variants: Vec<(&str, Vec<(usize, arrow::record_batch::RecordBatch)>)> = vec![
        (
            "narrow",
            vec![
                (1_000, generators::gen_narrow_batch(1_000, 42)),
                (10_000, generators::gen_narrow_batch(10_000, 42)),
            ],
        ),
        (
            "wide",
            vec![
                (1_000, generators::gen_wide_batch(1_000, 42)),
                (10_000, generators::gen_wide_batch(10_000, 42)),
            ],
        ),
    ];

    for (schema_name, sizes) in &variants {
        for (n, batch) in sizes {
            let label = format!("{schema_name}/{n}");

            group.throughput(Throughput::Elements(*n as u64));

            // ── write_row_json tight loop (baseline) ─────────────────────
            group.bench_with_input(
                BenchmarkId::new("write_row_json", &label),
                batch,
                |b, batch| {
                    let cols = build_col_infos(batch);
                    let mut buf = Vec::with_capacity(*n * 300);
                    b.iter(|| {
                        buf.clear();
                        for row in 0..batch.num_rows() {
                            write_row_json(batch, row, &cols, &mut buf, false)
                                .expect("JSON serialization should not fail");
                            buf.push(b'\n');
                        }
                        std::hint::black_box(&buf);
                    });
                },
            );

            // ── write_row_json_resolved tight loop (pre-resolved arrays) ─
            group.bench_with_input(
                BenchmarkId::new("write_row_json_resolved", &label),
                batch,
                |b, batch| {
                    let cols = build_col_infos(batch);
                    let resolved = resolve_col_infos(batch, &cols);
                    let mut buf = Vec::with_capacity(*n * 300);
                    b.iter(|| {
                        buf.clear();
                        for row in 0..batch.num_rows() {
                            write_row_json_resolved(row, &resolved, &mut buf, false)
                                .expect("JSON serialization should not fail");
                            buf.push(b'\n');
                        }
                        std::hint::black_box(&buf);
                    });
                },
            );

            // ── StdoutSink::write_batch_to (JSON — uses resolved path) ───
            group.bench_with_input(
                BenchmarkId::new("write_batch_to", &label),
                batch,
                |b, batch| {
                    let stats = Arc::new(ComponentStats::new());
                    let mut sink = StdoutSink::new("bench".to_string(), StdoutFormat::Json, stats);
                    let mut out = Vec::with_capacity(*n * 300);
                    b.iter(|| {
                        out.clear();
                        sink.write_batch_to(batch, &meta, &mut out)
                            .expect("write_batch_to should not fail");
                        std::hint::black_box(&out);
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_json_lines);
criterion_main!(benches);
