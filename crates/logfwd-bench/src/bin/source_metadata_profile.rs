#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Quick source metadata attachment timing/allocation profile.
//!
//! Run:
//! `cargo run -p logfwd-bench --release --bin source_metadata_profile -- --rows 50000 --sources 300 --iterations 200`

use std::alloc::System;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, StringBuilder, StringDictionaryBuilder, StringViewBuilder, UInt64Array, UInt64Builder,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use logfwd_bench::generators;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[derive(Clone)]
struct Span {
    source_idx: usize,
    rows: usize,
}

struct Fixture {
    rows: usize,
    spans: Vec<Span>,
    paths: Vec<String>,
    batch: RecordBatch,
}

struct ResultRow {
    mode: &'static str,
    iterations: usize,
    elapsed_ms: f64,
    ns_per_row: f64,
    allocations: u64,
    bytes_allocated: u64,
    bytes_per_row: f64,
}

fn main() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let rows = parse_flag(&args, "--rows", 50_000usize);
    let sources = parse_flag(&args, "--sources", 300usize).max(1);
    let iterations = parse_flag(&args, "--iterations", 200usize).max(1);
    let run_len = parse_flag(&args, "--run-len", 32usize).max(1);

    let fixture = make_fixture(rows, sources, run_len);
    let raw_json = generators::gen_production_mixed(rows, 42);
    let raw_path = "/var/log/pods/namespace_pod_uid/container/0.log";
    let mut stdout = io::stdout().lock();

    writeln!(
        stdout,
        "source metadata profile rows={rows} sources={sources} run_len={run_len} iterations={iterations}"
    )?;
    writeln!(
        stdout,
        "| mode | total ms | ns/row | allocs/iter | bytes/row | allocated MiB |"
    )?;
    writeln!(stdout, "|---|---:|---:|---:|---:|---:|")?;

    for result in [
        measure("__source_id_uint64", iterations, rows, || {
            attach_source_id(fixture.batch.clone(), &fixture)
        }),
        measure("file_path_utf8view_string_keyed", iterations, rows, || {
            attach_path_utf8view_string_keyed(fixture.batch.clone(), &fixture)
        }),
        measure("file_path_utf8view_by_source", iterations, rows, || {
            attach_path_utf8view_by_source(fixture.batch.clone(), &fixture)
        }),
        measure("file_path_utf8", iterations, rows, || {
            attach_path_utf8(fixture.batch.clone(), &fixture)
        }),
        measure("file_path_dictionary_i32", iterations, rows, || {
            attach_path_dictionary(fixture.batch.clone(), &fixture)
        }),
        measure_bytes("raw_json_source_path_rewrite", iterations, rows, || {
            raw_rewrite_source_path(&raw_json, raw_path)
        }),
    ] {
        writeln!(
            stdout,
            "| {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
            result.mode,
            result.elapsed_ms,
            result.ns_per_row,
            result.allocations as f64 / result.iterations as f64,
            result.bytes_per_row,
            result.bytes_allocated as f64 / 1_048_576.0,
        )?;
    }

    Ok(())
}

fn parse_flag<T>(args: &[String], name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    args.windows(2)
        .find(|pair| pair[0] == name)
        .and_then(|pair| pair[1].parse().ok())
        .unwrap_or(default)
}

fn measure<F>(mode: &'static str, iterations: usize, rows: usize, mut f: F) -> ResultRow
where
    F: FnMut() -> RecordBatch,
{
    for _ in 0..5 {
        std::hint::black_box(f());
    }

    let region = Region::new(GLOBAL);
    let start = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(f());
    }
    let elapsed = start.elapsed();
    let stats = region.change();
    let total_rows = (iterations * rows) as f64;
    ResultRow {
        mode,
        iterations,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        ns_per_row: elapsed.as_secs_f64() * 1_000_000_000.0 / total_rows,
        allocations: stats.allocations as u64,
        bytes_allocated: stats.bytes_allocated as u64,
        bytes_per_row: stats.bytes_allocated as f64 / total_rows,
    }
}

fn measure_bytes<F>(mode: &'static str, iterations: usize, rows: usize, mut f: F) -> ResultRow
where
    F: FnMut() -> Vec<u8>,
{
    for _ in 0..5 {
        std::hint::black_box(f());
    }

    let region = Region::new(GLOBAL);
    let start = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(f());
    }
    let elapsed = start.elapsed();
    let stats = region.change();
    let total_rows = (iterations * rows) as f64;
    ResultRow {
        mode,
        iterations,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        ns_per_row: elapsed.as_secs_f64() * 1_000_000_000.0 / total_rows,
        allocations: stats.allocations as u64,
        bytes_allocated: stats.bytes_allocated as u64,
        bytes_per_row: stats.bytes_allocated as f64 / total_rows,
    }
}

fn make_fixture(rows: usize, sources: usize, run_len: usize) -> Fixture {
    let paths = (0..sources)
        .map(|idx| {
            format!(
                "/var/log/pods/namespace-{idx}_pod-{idx}_uid-{idx}/container-{}/{}.log",
                idx % 8,
                idx % 4
            )
        })
        .collect::<Vec<_>>();

    let mut spans = Vec::with_capacity(rows.div_ceil(run_len));
    let mut remaining = rows;
    let mut source_idx = 0usize;
    while remaining > 0 {
        let span_rows = remaining.min(run_len);
        spans.push(Span {
            source_idx: source_idx % sources,
            rows: span_rows,
        });
        source_idx += 1;
        remaining -= span_rows;
    }

    let values = (0..rows as u64).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "row",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))])
        .expect("base batch");

    Fixture {
        rows,
        spans,
        paths,
        batch,
    }
}

fn attach_source_id(batch: RecordBatch, fixture: &Fixture) -> RecordBatch {
    let mut builder = UInt64Builder::with_capacity(fixture.rows);
    for span in &fixture.spans {
        for _ in 0..span.rows {
            builder.append_value(span.source_idx as u64 + 1);
        }
    }
    replace_or_append(
        batch,
        "__source_id",
        DataType::UInt64,
        Arc::new(builder.finish()) as ArrayRef,
    )
}

fn attach_path_utf8view_string_keyed(batch: RecordBatch, fixture: &Fixture) -> RecordBatch {
    let mut builder = StringViewBuilder::new();
    let mut blocks: HashMap<String, (u32, u32)> = HashMap::new();
    for span in &fixture.spans {
        let value = fixture.paths[span.source_idx].as_str();
        let (block, len) = if let Some(&(block, len)) = blocks.get(value) {
            (block, len)
        } else {
            let len = u32::try_from(value.len()).expect("path fits u32");
            let block = builder.append_block(Buffer::from_slice_ref(value.as_bytes()));
            blocks.insert(value.to_owned(), (block, len));
            (block, len)
        };
        for _ in 0..span.rows {
            builder.try_append_view(block, 0, len).expect("view");
        }
    }
    replace_or_append(
        batch,
        "file.path",
        DataType::Utf8View,
        Arc::new(builder.finish()) as ArrayRef,
    )
}

fn attach_path_utf8view_by_source(batch: RecordBatch, fixture: &Fixture) -> RecordBatch {
    let mut builder = StringViewBuilder::new();
    let mut blocks = vec![None; fixture.paths.len()];
    for span in &fixture.spans {
        let (block, len) = match blocks[span.source_idx] {
            Some(block) => block,
            None => {
                let value = fixture.paths[span.source_idx].as_str();
                let len = u32::try_from(value.len()).expect("path fits u32");
                let block = builder.append_block(Buffer::from_slice_ref(value.as_bytes()));
                blocks[span.source_idx] = Some((block, len));
                (block, len)
            }
        };
        for _ in 0..span.rows {
            builder.try_append_view(block, 0, len).expect("view");
        }
    }
    replace_or_append(
        batch,
        "file.path",
        DataType::Utf8View,
        Arc::new(builder.finish()) as ArrayRef,
    )
}

fn attach_path_utf8(batch: RecordBatch, fixture: &Fixture) -> RecordBatch {
    let mut builder = StringBuilder::with_capacity(fixture.rows, fixture.rows * 96);
    for span in &fixture.spans {
        let value = fixture.paths[span.source_idx].as_str();
        for _ in 0..span.rows {
            builder.append_value(value);
        }
    }
    replace_or_append(
        batch,
        "file.path",
        DataType::Utf8,
        Arc::new(builder.finish()) as ArrayRef,
    )
}

fn attach_path_dictionary(batch: RecordBatch, fixture: &Fixture) -> RecordBatch {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for span in &fixture.spans {
        let value = fixture.paths[span.source_idx].as_str();
        for _ in 0..span.rows {
            builder.append(value).expect("dictionary append");
        }
    }
    replace_or_append(
        batch,
        "file.path",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        Arc::new(builder.finish()) as ArrayRef,
    )
}

fn replace_or_append(
    batch: RecordBatch,
    name: &'static str,
    data_type: DataType,
    array: ArrayRef,
) -> RecordBatch {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len() + 1);
    let mut arrays = Vec::with_capacity(batch.num_columns() + 1);
    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name().as_str() == name {
            fields.push(Field::new(name, data_type.clone(), true));
            arrays.push(Arc::clone(&array));
        } else {
            fields.push((**field).clone());
            arrays.push(Arc::clone(batch.column(idx)));
        }
    }
    if !schema
        .fields()
        .iter()
        .any(|field| field.name().as_str() == name)
    {
        fields.push(Field::new(name, data_type, true));
        arrays.push(array);
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .expect("replace or append column")
}

fn raw_rewrite_source_path(input: &[u8], source_path: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() + input.len() / 4);
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
    out
}

fn escape_json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}
