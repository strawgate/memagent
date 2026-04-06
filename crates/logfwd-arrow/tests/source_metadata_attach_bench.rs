//! Benchmark-style POCs for source metadata column attachment after scan.
//!
//! Run with:
//! `cargo test --release -p logfwd-arrow source_metadata_attach_benchmark -- --ignored --nocapture`

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use logfwd_arrow::Scanner;
use logfwd_core::scan_config::ScanConfig;

#[derive(Clone, Copy)]
struct RowOriginSpan<'a> {
    rows: usize,
    source_id: u64,
    input_name: &'a str,
}

fn make_ndjson(rows: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows * 48);
    for i in 0..rows {
        out.extend_from_slice(br#"{"msg":"hello","seq":"#);
        out.extend_from_slice(i.to_string().as_bytes());
        out.extend_from_slice(b"}\n");
    }
    out
}

fn make_spans<'a>(rows: usize, sources: usize, input_name: &'a str) -> Vec<RowOriginSpan<'a>> {
    let base = rows / sources;
    let extra = rows % sources;
    (0..sources)
        .map(|i| RowOriginSpan {
            rows: base + usize::from(i < extra),
            source_id: 1_000_000 + i as u64,
            input_name,
        })
        .collect()
}

fn scan_batch(rows: usize) -> RecordBatch {
    let mut scanner = Scanner::new(ScanConfig::default());
    scanner
        .scan_detached(Bytes::from(make_ndjson(rows)))
        .expect("scan benchmark batch")
}

fn append_source_id_u64(batch: &RecordBatch, spans: &[RowOriginSpan<'_>]) -> RecordBatch {
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();

    let mut builder = UInt64Builder::with_capacity(batch.num_rows());
    for span in spans {
        for _ in 0..span.rows {
            builder.append_value(span.source_id);
        }
    }

    fields.push(Arc::new(Field::new("_source_id", DataType::UInt64, false)));
    columns.push(Arc::new(builder.finish()) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid augmented batch")
}

fn append_source_id_u64_and_input(batch: &RecordBatch, spans: &[RowOriginSpan<'_>]) -> RecordBatch {
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();

    let mut id_builder = UInt64Builder::with_capacity(batch.num_rows());
    let mut input_builder = StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 8);
    for span in spans {
        for _ in 0..span.rows {
            id_builder.append_value(span.source_id);
            input_builder.append_value(span.input_name);
        }
    }

    fields.push(Arc::new(Field::new("_source_id", DataType::UInt64, false)));
    fields.push(Arc::new(Field::new("_input", DataType::Utf8, false)));
    columns.push(Arc::new(id_builder.finish()) as ArrayRef);
    columns.push(Arc::new(input_builder.finish()) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid augmented batch")
}

fn append_source_id_str_and_input(batch: &RecordBatch, spans: &[RowOriginSpan<'_>]) -> RecordBatch {
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();

    let mut id_builder = StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 12);
    let mut input_builder = StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 8);
    for span in spans {
        for _ in 0..span.rows {
            id_builder.append_value(span.source_id.to_string());
            input_builder.append_value(span.input_name);
        }
    }

    fields.push(Arc::new(Field::new("_source_id", DataType::Utf8, false)));
    fields.push(Arc::new(Field::new("_input", DataType::Utf8, false)));
    columns.push(Arc::new(id_builder.finish()) as ArrayRef);
    columns.push(Arc::new(input_builder.finish()) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid augmented batch")
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn bench_case<F>(label: &str, batch: &RecordBatch, spans: &[RowOriginSpan<'_>], mut f: F)
where
    F: FnMut(&RecordBatch, &[RowOriginSpan<'_>]) -> RecordBatch,
{
    for _ in 0..2 {
        black_box(f(batch, spans));
    }

    let mut samples = Vec::with_capacity(11);
    let mut rows = 0usize;
    let mut cols = 0usize;
    for _ in 0..11 {
        let start = Instant::now();
        let out = f(batch, spans);
        let elapsed = start.elapsed();
        rows = out.num_rows();
        cols = out.num_columns();
        black_box(out);
        samples.push(elapsed);
    }

    let med = median(samples.clone());
    let avg = samples.iter().map(Duration::as_secs_f64).sum::<f64>() / samples.len() as f64;
    println!(
        "{label:42} median={:7.3} ms avg={:7.3} ms rows={rows:6} cols={cols}",
        med.as_secs_f64() * 1000.0,
        avg * 1000.0
    );
}

#[test]
#[ignore = "benchmark: run manually in release mode for investigation"]
fn source_metadata_attach_benchmark() {
    println!("\n== Source Metadata Column Attach Bench ==");
    for &rows in &[1_000usize, 10_000, 50_000] {
        let batch = scan_batch(rows);
        let spans = make_spans(rows, 30, "pods");
        println!(
            "\nrows={rows} scanned_cols={} source_spans={}",
            batch.num_columns(),
            spans.len()
        );
        bench_case(
            "append _source_id as UInt64",
            &batch,
            &spans,
            append_source_id_u64,
        );
        bench_case(
            "append _source_id UInt64 + _input",
            &batch,
            &spans,
            append_source_id_u64_and_input,
        );
        bench_case(
            "append _source_id Utf8 + _input",
            &batch,
            &spans,
            append_source_id_str_and_input,
        );
    }
}
