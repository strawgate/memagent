//! Source metadata attachment benchmarks.
//!
//! These benches isolate the hot path introduced by post-scan source metadata:
//! row-origin spans become Arrow columns after scanning and before SQL.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, StringBuilder, StringDictionaryBuilder, StringViewBuilder,
    UInt64Array, UInt64Builder,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use logfwd_arrow::scanner::Scanner;
use logfwd_bench::generators;
use logfwd_core::scan_config::ScanConfig;
use logfwd_transform::SqlTransform;

#[derive(Clone, Copy)]
enum SpanPattern {
    Contiguous,
    Runs32,
    RoundRobin,
}

impl SpanPattern {
    fn as_str(self) -> &'static str {
        match self {
            SpanPattern::Contiguous => "contiguous",
            SpanPattern::Runs32 => "runs32",
            SpanPattern::RoundRobin => "round_robin",
        }
    }
}

#[derive(Clone)]
struct BenchSpan {
    source_idx: Option<usize>,
    rows: usize,
}

struct MetadataFixture {
    rows: usize,
    spans: Vec<BenchSpan>,
    source_paths: Vec<String>,
    batch: RecordBatch,
}

fn make_base_batch(rows: usize) -> RecordBatch {
    let values: Vec<u64> = (0..rows as u64).collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "row",
        DataType::UInt64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))])
        .expect("base benchmark batch")
}

fn make_fixture(rows: usize, sources: usize, pattern: SpanPattern) -> MetadataFixture {
    assert!(rows > 0);
    assert!(sources > 0);

    let source_paths = (0..sources)
        .map(|idx| {
            format!(
                "/var/log/pods/namespace-{idx}_pod-{idx}_uid-{idx}/container-{}/{}.log",
                idx % 8,
                idx % 4
            )
        })
        .collect::<Vec<_>>();

    let spans = match pattern {
        SpanPattern::Contiguous => contiguous_spans(rows, sources),
        SpanPattern::Runs32 => fixed_run_spans(rows, sources, 32),
        SpanPattern::RoundRobin => fixed_run_spans(rows, sources, 1),
    };

    MetadataFixture {
        rows,
        spans,
        source_paths,
        batch: make_base_batch(rows),
    }
}

fn contiguous_spans(rows: usize, sources: usize) -> Vec<BenchSpan> {
    let active_sources = sources.min(rows);
    let base = rows / active_sources;
    let remainder = rows % active_sources;
    (0..active_sources)
        .map(|source_idx| BenchSpan {
            source_idx: Some(source_idx),
            rows: base + usize::from(source_idx < remainder),
        })
        .collect()
}

fn fixed_run_spans(rows: usize, sources: usize, run_len: usize) -> Vec<BenchSpan> {
    let mut spans = Vec::with_capacity(rows.div_ceil(run_len));
    let mut remaining = rows;
    let mut source_idx = 0usize;
    while remaining > 0 {
        let span_rows = remaining.min(run_len);
        spans.push(BenchSpan {
            source_idx: Some(source_idx % sources),
            rows: span_rows,
        });
        remaining -= span_rows;
        source_idx += 1;
    }
    spans
}

fn attach_source_id_current(batch: RecordBatch, fixture: &MetadataFixture) -> RecordBatch {
    let mut builder = UInt64Builder::with_capacity(fixture.rows);
    for span in &fixture.spans {
        for _ in 0..span.rows {
            match span.source_idx {
                Some(source_idx) => builder.append_value(source_idx as u64 + 1),
                None => builder.append_null(),
            }
        }
    }
    replace_or_append_column(
        batch,
        "__source_id",
        DataType::UInt64,
        Arc::new(builder.finish()) as ArrayRef,
    )
    .expect("attach source id")
}

fn attach_path_utf8view_string_keyed(batch: RecordBatch, fixture: &MetadataFixture) -> RecordBatch {
    let mut builder = StringViewBuilder::new();
    let mut blocks: HashMap<String, (u32, u32)> = HashMap::new();
    for span in &fixture.spans {
        let value = span
            .source_idx
            .map(|idx| fixture.source_paths[idx].as_str());
        append_metadata_views_current(&mut builder, &mut blocks, value, span.rows)
            .expect("append path views");
    }
    let array = Arc::new(builder.finish()) as ArrayRef;
    replace_or_append_column(batch, "file.path", DataType::Utf8View, array)
        .expect("attach source path")
}

fn attach_path_utf8view_by_source(batch: RecordBatch, fixture: &MetadataFixture) -> RecordBatch {
    let mut builder = StringViewBuilder::new();
    let mut blocks: Vec<Option<(u32, u32)>> = vec![None; fixture.source_paths.len()];
    for span in &fixture.spans {
        match span.source_idx {
            Some(source_idx) => {
                let (block, len) = match blocks[source_idx] {
                    Some((block, len)) => (block, len),
                    None => {
                        let value = fixture.source_paths[source_idx].as_str();
                        let len = u32::try_from(value.len()).expect("benchmark path fits u32");
                        let block = builder.append_block(Buffer::from(value.as_bytes().to_vec()));
                        blocks[source_idx] = Some((block, len));
                        (block, len)
                    }
                };
                for _ in 0..span.rows {
                    builder
                        .try_append_view(block, 0, len)
                        .expect("append source-keyed path view");
                }
            }
            None => {
                for _ in 0..span.rows {
                    builder.append_null();
                }
            }
        }
    }
    let array = Arc::new(builder.finish()) as ArrayRef;
    replace_or_append_column(batch, "file.path", DataType::Utf8View, array)
        .expect("attach source-keyed source path")
}

fn attach_path_utf8(batch: RecordBatch, fixture: &MetadataFixture) -> RecordBatch {
    let value_bytes = fixture
        .spans
        .iter()
        .map(|span| {
            span.source_idx.map_or(0, |source_idx| {
                fixture.source_paths[source_idx].len() * span.rows
            })
        })
        .sum::<usize>();
    let mut builder = StringBuilder::with_capacity(fixture.rows, value_bytes);
    for span in &fixture.spans {
        match span.source_idx {
            Some(source_idx) => {
                let value = fixture.source_paths[source_idx].as_str();
                for _ in 0..span.rows {
                    builder.append_value(value);
                }
            }
            None => {
                for _ in 0..span.rows {
                    builder.append_null();
                }
            }
        }
    }
    replace_or_append_column(
        batch,
        "file.path",
        DataType::Utf8,
        Arc::new(builder.finish()) as ArrayRef,
    )
    .expect("attach Utf8 source path")
}

fn attach_path_dictionary(batch: RecordBatch, fixture: &MetadataFixture) -> RecordBatch {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for span in &fixture.spans {
        match span.source_idx {
            Some(source_idx) => {
                let value = fixture.source_paths[source_idx].as_str();
                for _ in 0..span.rows {
                    builder.append(value).expect("append dictionary value");
                }
            }
            None => {
                for _ in 0..span.rows {
                    builder.append_null();
                }
            }
        }
    }
    let array: DictionaryArray<Int32Type> = builder.finish();
    replace_or_append_column(
        batch,
        "file.path",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        Arc::new(array) as ArrayRef,
    )
    .expect("attach dictionary source path")
}

fn append_metadata_views_current(
    builder: &mut StringViewBuilder,
    blocks: &mut HashMap<String, (u32, u32)>,
    value: Option<&str>,
    rows: usize,
) -> Result<(), ArrowError> {
    match value {
        Some(value) => {
            let (block, len) = if let Some(&(block, len)) = blocks.get(value) {
                (block, len)
            } else {
                let len = u32::try_from(value.len()).map_err(|_e| {
                    ArrowError::InvalidArgumentError(
                        "benchmark metadata string exceeds Utf8View limit".to_string(),
                    )
                })?;
                let block = builder.append_block(Buffer::from(value.as_bytes().to_vec()));
                blocks.insert(value.to_owned(), (block, len));
                (block, len)
            };
            for _ in 0..rows {
                builder.try_append_view(block, 0, len)?;
            }
        }
        None => {
            for _ in 0..rows {
                builder.append_null();
            }
        }
    }
    Ok(())
}

fn replace_or_append_column(
    batch: RecordBatch,
    name: &'static str,
    data_type: DataType,
    array: ArrayRef,
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len() + 1);
    let mut arrays = Vec::with_capacity(batch.num_columns() + 1);
    let mut replaced = false;

    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name().as_str() == name {
            fields.push(Field::new(name, data_type.clone(), true));
            arrays.push(Arc::clone(&array));
            replaced = true;
        } else {
            fields.push((**field).clone());
            arrays.push(Arc::clone(batch.column(idx)));
        }
    }

    if !replaced {
        fields.push(Field::new(name, data_type, true));
        arrays.push(array);
    }

    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

fn raw_rewrite_source_path(input: &[u8], source_path: &str, out: &mut Vec<u8>) {
    out.clear();
    let injection = format!("\"_source_path\":\"{}\",", escape_json_string(source_path));
    for line in input.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        if line.first().copied() == Some(b'{') {
            out.push(b'{');
            out.extend_from_slice(injection.as_bytes());
            out.extend_from_slice(&line[1..]);
        } else {
            out.extend_from_slice(line);
        }
        out.push(b'\n');
    }
}

fn escape_json_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn bench_attach_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_metadata_attach");
    group.sample_size(30);

    let rows = 50_000;
    for pattern in [
        SpanPattern::Contiguous,
        SpanPattern::Runs32,
        SpanPattern::RoundRobin,
    ] {
        for sources in [1usize, 30, 300] {
            let fixture = make_fixture(rows, sources, pattern);
            group.throughput(Throughput::Elements(rows as u64));

            let id = format!("{}/sources_{sources}/rows_{rows}", pattern.as_str());
            group.bench_with_input(
                BenchmarkId::new("source_id_uint64", &id),
                &fixture,
                |b, f| {
                    b.iter(|| {
                        std::hint::black_box(attach_source_id_current(f.batch.clone(), f));
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new("path_utf8view_string_keyed", &id),
                &fixture,
                |b, f| {
                    b.iter(|| {
                        std::hint::black_box(attach_path_utf8view_string_keyed(f.batch.clone(), f));
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new("path_utf8view_by_source", &id),
                &fixture,
                |b, f| {
                    b.iter(|| {
                        std::hint::black_box(attach_path_utf8view_by_source(f.batch.clone(), f));
                    });
                },
            );
            group.bench_with_input(BenchmarkId::new("path_utf8", &id), &fixture, |b, f| {
                b.iter(|| {
                    std::hint::black_box(attach_path_utf8(f.batch.clone(), f));
                });
            });
            group.bench_with_input(
                BenchmarkId::new("path_dictionary_i32", &id),
                &fixture,
                |b, f| {
                    b.iter(|| {
                        std::hint::black_box(attach_path_dictionary(f.batch.clone(), f));
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_scan_attach_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_metadata_scan_sql");
    group.sample_size(20);

    let rows = 10_000;
    let data = generators::gen_production_mixed(rows, 42);
    let data_bytes = bytes::Bytes::from(data.clone());
    let fixture = make_fixture(rows, 30, SpanPattern::Runs32);

    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("baseline_scan_select_message", |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform =
            SqlTransform::new("SELECT message FROM logs").expect("baseline SQL should parse");
        b.iter(|| {
            let batch = scanner.scan_detached(data_bytes.clone()).expect("scan");
            let result = transform.execute_blocking(batch).expect("transform");
            std::hint::black_box(result);
        });
    });
    group.bench_function("attach_source_id_project", |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform =
            SqlTransform::new("SELECT message, __source_id FROM logs").expect("metadata SQL");
        b.iter(|| {
            let batch = scanner.scan_detached(data_bytes.clone()).expect("scan");
            let batch = attach_source_id_current(batch, &fixture);
            let result = transform.execute_blocking(batch).expect("transform");
            std::hint::black_box(result);
        });
    });
    group.bench_function("attach_path_filter", |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let needle = fixture.source_paths[0].as_str();
        let sql = format!("SELECT message FROM logs WHERE \"file.path\" = '{needle}'");
        let mut transform = SqlTransform::new(&sql).expect("metadata filter SQL");
        b.iter(|| {
            let batch = scanner.scan_detached(data_bytes.clone()).expect("scan");
            let batch = attach_path_utf8view_string_keyed(batch, &fixture);
            let result = transform.execute_blocking(batch).expect("transform");
            std::hint::black_box(result);
        });
    });

    group.finish();
}

fn bench_raw_rewrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_metadata_raw_rewrite_baseline");
    group.sample_size(20);

    for rows in [10_000usize, 100_000] {
        let data = generators::gen_production_mixed(rows, 42);
        let mut out = Vec::with_capacity(data.len() + rows * 96);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("json_source_path_insertion", rows),
            &data,
            |b, data| {
                b.iter(|| {
                    raw_rewrite_source_path(
                        data,
                        "/var/log/pods/namespace_pod_uid/container/0.log",
                        &mut out,
                    );
                    std::hint::black_box(&out);
                });
            },
        );
    }

    group.finish();
}

fn bench_source_path_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_metadata_snapshot_filter");
    group.sample_size(30);

    for sources in [30usize, 300, 10_000] {
        let paths = (0..sources)
            .map(|idx| {
                (
                    idx as u64 + 1,
                    format!("/var/log/pods/ns-{idx}_pod-{idx}_uid/container/0.log"),
                )
            })
            .collect::<Vec<_>>();
        let wanted = (0..sources)
            .step_by((sources / 30).max(1))
            .take(30)
            .map(|idx| idx as u64 + 1)
            .collect::<HashSet<_>>();

        group.bench_with_input(
            BenchmarkId::new("filter_paths_for_batch", sources),
            &paths,
            |b, paths| {
                b.iter(|| {
                    let filtered = paths
                        .iter()
                        .filter(|(sid, _)| wanted.contains(sid))
                        .map(|(sid, path)| (*sid, path.clone()))
                        .collect::<HashMap<_, _>>();
                    std::hint::black_box(filtered);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_attach_columns,
    bench_scan_attach_transform,
    bench_raw_rewrite,
    bench_source_path_snapshot
);
criterion_main!(benches);
