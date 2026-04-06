//! Benchmark-style POCs for source metadata enrichment snapshot construction.
//!
//! Run with:
//! `cargo test --release -p logfwd-transform source_metadata_snapshot_benchmark -- --ignored --nocapture`

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use logfwd_transform::enrichment::parse_cri_log_path;

#[derive(Clone)]
struct SourceEntry {
    source_id: String,
    source_path: String,
    input_name: String,
}

fn make_entries(count: usize) -> Vec<SourceEntry> {
    (0..count)
        .map(|i| SourceEntry {
            source_id: format!("{}", 1_000_000u64 + i as u64),
            source_path: format!(
                "/var/log/pods/ns-{}_pod-{}_12345678-1234-1234-1234-{:012}/container-{}/0.log",
                i % 17,
                i,
                i,
                i % 5
            ),
            input_name: "pods".to_string(),
        })
        .collect()
}

fn build_sources_batch(entries: &[SourceEntry]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("source_id", DataType::Utf8, false),
        Field::new("source_path", DataType::Utf8, false),
        Field::new("input_name", DataType::Utf8, false),
    ]));
    let ids: Vec<&str> = entries.iter().map(|e| e.source_id.as_str()).collect();
    let paths: Vec<&str> = entries.iter().map(|e| e.source_path.as_str()).collect();
    let inputs: Vec<&str> = entries.iter().map(|e| e.input_name.as_str()).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(StringArray::from(inputs)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn build_sources_plus_k8s_batch(entries: &[SourceEntry]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("source_id", DataType::Utf8, false),
        Field::new("source_path", DataType::Utf8, false),
        Field::new("input_name", DataType::Utf8, false),
        Field::new("namespace", DataType::Utf8, true),
        Field::new("pod_name", DataType::Utf8, true),
        Field::new("pod_uid", DataType::Utf8, true),
        Field::new("container_name", DataType::Utf8, true),
    ]));

    let ids: Vec<&str> = entries.iter().map(|e| e.source_id.as_str()).collect();
    let paths: Vec<&str> = entries.iter().map(|e| e.source_path.as_str()).collect();
    let inputs: Vec<&str> = entries.iter().map(|e| e.input_name.as_str()).collect();

    let parsed: Vec<_> = entries
        .iter()
        .map(|e| parse_cri_log_path(&e.source_path))
        .collect();
    let namespaces: Vec<Option<&str>> = parsed
        .iter()
        .map(|p| p.as_ref().map(|v| v.namespace.as_str()))
        .collect();
    let pod_names: Vec<Option<&str>> = parsed
        .iter()
        .map(|p| p.as_ref().map(|v| v.pod_name.as_str()))
        .collect();
    let pod_uids: Vec<Option<&str>> = parsed
        .iter()
        .map(|p| p.as_ref().map(|v| v.pod_uid.as_str()))
        .collect();
    let container_names: Vec<Option<&str>> = parsed
        .iter()
        .map(|p| p.as_ref().map(|v| v.container_name.as_str()))
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(StringArray::from(inputs)) as ArrayRef,
            Arc::new(StringArray::from(namespaces)) as ArrayRef,
            Arc::new(StringArray::from(pod_names)) as ArrayRef,
            Arc::new(StringArray::from(pod_uids)) as ArrayRef,
            Arc::new(StringArray::from(container_names)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn bench_table_case<F>(label: &str, entries: &[SourceEntry], iterations: usize, mut f: F)
where
    F: FnMut(&[SourceEntry]) -> RecordBatch,
{
    for _ in 0..2 {
        black_box(f(entries));
    }

    let mut samples = Vec::with_capacity(iterations);
    let mut rows = 0usize;
    let mut cols = 0usize;
    for _ in 0..iterations {
        let start = Instant::now();
        let batch = f(entries);
        let elapsed = start.elapsed();
        rows = batch.num_rows();
        cols = batch.num_columns();
        black_box(batch);
        samples.push(elapsed);
    }

    let med = median(samples.clone());
    let avg = samples.iter().map(Duration::as_secs_f64).sum::<f64>() / iterations as f64;
    println!(
        "{label:42} median={:7.3} ms avg={:7.3} ms rows={rows:5} cols={cols}",
        med.as_secs_f64() * 1000.0,
        avg * 1000.0,
    );
}

#[test]
#[ignore = "benchmark: run manually in release mode for investigation"]
fn source_metadata_snapshot_benchmark() {
    println!("\n== Source Metadata Snapshot Bench ==");
    for &count in &[30usize, 300, 3_000, 10_000] {
        let entries = make_entries(count);
        println!("\nsource entries: {count}");
        bench_table_case("sources table only", &entries, 11, build_sources_batch);
        bench_table_case("sources table with k8s fields", &entries, 11, |e| {
            build_sources_plus_k8s_batch(e)
        });
    }
}
