#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Measure data sizes across the pipeline: raw JSON → Arrow RecordBatch → IPC → Parquet
//! Run with: cargo run -p logfwd-bench --release --bin sizes

use std::io::Write;
use std::time::Instant;

use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_transform::SqlTransform;

fn main() {
    println!("=== Data Size Analysis: Raw JSON → Arrow → IPC → Parquet ===\n");

    // Generate datasets of increasing size
    let configs = vec![
        ("simple-6f", 6, 100_000, generate_simple(100_000)),
        ("simple-6f", 6, 500_000, generate_simple(500_000)),
        ("simple-6f", 6, 1_000_000, generate_simple(1_000_000)),
        ("wide-20f", 20, 100_000, generate_wide(100_000)),
        ("wide-20f", 20, 500_000, generate_wide(500_000)),
        ("narrow-3f", 3, 100_000, generate_narrow(100_000)),
        ("narrow-3f", 3, 1_000_000, generate_narrow(1_000_000)),
    ];

    // Also load real nginx data if available
    let nginx_data = std::fs::read("/tmp/bench-data/nginx_1m.ndjson").ok();

    println!(
        "{:<15} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8} {:>8} {:>8}",
        "dataset", "lines", "raw_json", "arrow_mem", "ipc_raw", "ipc_zstd", "parquet",
        "json:arr", "json:ipc", "json:pqt"
    );
    println!("{}", "-".repeat(130));

    for (name, fields, lines, data) in &configs {
        measure(name, *fields, *lines, data);
    }

    if let Some(ref data) = nginx_data {
        let lines = memchr::memchr_iter(b'\n', data).count();
        measure("nginx-real", 8, lines, data);
    }

    // === Memory analysis: what's in RAM during processing ===
    println!("\n=== Memory During Processing (1M simple lines) ===\n");
    let data = generate_simple(1_000_000);
    memory_analysis(&data, 1_000_000);

    // === Batch size impact ===
    println!("\n=== Batch Size Impact on Compression (simple 6f) ===\n");
    println!(
        "{:<12} {:>8} {:>10} {:>10} {:>10} {:>8}",
        "batch_size", "lines", "arrow_mem", "ipc_zstd", "per_line", "ratio"
    );
    println!("{}", "-".repeat(70));
    for batch_lines in [1_000, 5_000, 10_000, 50_000, 100_000, 500_000] {
        let data = generate_simple(batch_lines);
        let batch = scan(&data, "SELECT * FROM logs");
        let ipc_zstd = write_ipc_zstd(&batch);
        let arrow_mem = batch_memory_size(&batch);
        println!(
            "{:<12} {:>8} {:>10} {:>10} {:>10} {:>8.2}x",
            format!("{}K", batch_lines / 1000),
            batch_lines,
            fmt_bytes(arrow_mem),
            fmt_bytes(ipc_zstd.len()),
            fmt_bytes(ipc_zstd.len() / batch_lines),
            data.len() as f64 / ipc_zstd.len() as f64,
        );
    }

    deep_memory_analysis();
    deep_memory_analysis_v2();
}

fn measure(name: &str, _fields: usize, lines: usize, data: &[u8]) {
    let raw_size = data.len();

    // Scan to Arrow
    let batch = scan(data, "SELECT * FROM logs");
    let arrow_mem = batch_memory_size(&batch);

    // Write IPC uncompressed
    let ipc_raw = write_ipc_raw(&batch);
    let ipc_raw_size = ipc_raw.len();

    // Write IPC with zstd
    let ipc_zstd = write_ipc_zstd(&batch);
    let ipc_zstd_size = ipc_zstd.len();

    // Write Parquet
    let parquet_size = write_parquet(&batch);

    println!(
        "{:<15} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8.2}x {:>8.2}x {:>8.2}x",
        format!("{}-{}",name, fmt_count(lines)),
        lines,
        fmt_bytes(raw_size),
        fmt_bytes(arrow_mem),
        fmt_bytes(ipc_raw_size),
        fmt_bytes(ipc_zstd_size),
        fmt_bytes(parquet_size),
        raw_size as f64 / arrow_mem as f64,
        raw_size as f64 / ipc_zstd_size as f64,
        raw_size as f64 / parquet_size as f64,
    );
}

fn memory_analysis(data: &[u8], lines: usize) {
    let raw_size = data.len();
    println!("  Raw JSON input:        {}", fmt_bytes(raw_size));

    // Scanner input buffer (bytes::Bytes clone)
    println!("  Scanner input buffer:  {} (Bytes ref-counted, same allocation)", fmt_bytes(raw_size));

    // Arrow RecordBatch
    let batch = scan(data, "SELECT * FROM logs");
    let arrow_mem = batch_memory_size(&batch);
    println!("  Arrow RecordBatch:     {} ({} columns × {} rows)", fmt_bytes(arrow_mem), batch.num_columns(), batch.num_rows());

    // After transform (passthrough = same size)
    println!("  After SQL transform:   {} (passthrough, same batch)", fmt_bytes(arrow_mem));

    // Per-column breakdown
    println!("\n  Per-column breakdown:");
    println!("  {:<35} {:<15} {:>10} {:>10}", "Column", "DataType", "Memory", "Per row");
    println!("  {}", "-".repeat(75));
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let mem = col.get_array_memory_size();
        println!("  {:<35} {:<15} {:>10} {:>8} B",
            field.name(),
            format!("{:?}", field.data_type()),
            fmt_bytes(mem),
            mem / lines,
        );
    }

    // IPC encoding buffer
    let ipc = write_ipc_zstd(&batch);
    println!("\n  IPC zstd encode buf:   {}", fmt_bytes(ipc.len()));

    // Total peak memory (all held simultaneously during encode)
    let peak = raw_size + arrow_mem + ipc.len();
    println!("\n  Peak memory (all held): {}", fmt_bytes(peak));
    println!("  Per line:               {} bytes", peak / lines);
    println!("  Amplification vs raw:   {:.2}x", peak as f64 / raw_size as f64);

    // With streaming (drop input after scan)
    let streaming_peak = arrow_mem + ipc.len();
    println!("\n  Streaming peak (drop input after scan): {}", fmt_bytes(streaming_peak));
    println!("  Streaming per line:     {} bytes", streaming_peak / lines);
}

fn scan(data: &[u8], sql: &str) -> RecordBatch {
    let mut transform = SqlTransform::new(sql).expect("bad SQL");
    let config = transform.scan_config();
    let mut scanner = Scanner::new(config);
    let batch = scanner.scan(bytes::Bytes::from(data.to_vec())).expect("scan failed");
    // Run through transform for accurate schema
    transform.execute_blocking(batch).unwrap_or_else(|_| {
        // If transform fails, return the raw scan
        let mut scanner2 = Scanner::new(ScanConfig::default());
        scanner2.scan(bytes::Bytes::from(data.to_vec())).unwrap()
    })
}

fn batch_memory_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

fn write_ipc_raw(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = FileWriter::try_new(&mut buf, &batch.schema()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    buf
}

fn write_ipc_zstd(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let opts = IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
        .unwrap();
    let mut writer = FileWriter::try_new_with_options(&mut buf, &batch.schema(), opts).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    buf
}

fn write_parquet(batch: &RecordBatch) -> usize {
    use arrow::datatypes::DataType;

    // Parquet doesn't support Utf8View — convert to Utf8 first
    let schema = batch.schema();
    let mut needs_conversion = false;
    for field in schema.fields() {
        if *field.data_type() == DataType::Utf8View {
            needs_conversion = true;
            break;
        }
    }

    let batch = if needs_conversion {
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            if *field.data_type() == DataType::Utf8View {
                let col = batch.column(i);
                let utf8_col = arrow::compute::cast(col, &DataType::Utf8).unwrap();
                fields.push(arrow::datatypes::Field::new(field.name(), DataType::Utf8, field.is_nullable()));
                columns.push(utf8_col);
            } else {
                fields.push((**field).clone());
                columns.push(batch.column(i).clone());
            }
        }
        let new_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(new_schema, columns).unwrap()
    } else {
        batch.clone()
    };

    let mut buf = Vec::new();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(1).unwrap()))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf.len()
}

// --- Helpers ---

fn generate_simple(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 180);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = ["/api/v1/users", "/api/v1/orders", "/api/v2/products", "/health", "/api/v1/auth"];
    for i in 0..n {
        write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled GET {}/{}","duration_ms":{},"request_id":"{:016x}","service":"myapp"}}"#,
            i % 1000, levels[i % 4], paths[i % 5], 10000 + (i * 7) % 90000,
            1 + (i * 13) % 500, (i as u64).wrapping_mul(0x517cc1b727220a95),
        ).unwrap();
        buf.push(b'\n');
    }
    buf
}

fn generate_wide(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 600);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let methods = ["GET", "POST", "PUT", "DELETE", "PATCH"];
    let regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];
    let namespaces = ["default", "kube-system", "monitoring", "logging"];
    for i in 0..n {
        write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request {}","duration_ms":{},"service":"myapp","host":"node-{}","pod":"app-{:04}","namespace":"{}","method":"{}","status_code":{},"region":"{}","user_id":"user-{}","trace_id":"{:032x}","response_bytes":{},"latency_p99_ms":{},"error_count":{},"cache_hit":{},"db_query_ms":{},"upstream":"svc-{}","version":"v{}.{}"}}"#,
            i % 1000, levels[i % 4], i, 1 + (i * 13) % 500,
            i % 10, i % 100, namespaces[i % 4], methods[i % 5],
            [200, 201, 400, 404, 500][i % 5], regions[i % 4],
            i % 1000, (i as u64).wrapping_mul(0x517cc1b727220a95),
            100 + (i * 37) % 10000, 10 + (i * 11) % 1000,
            if i % 20 == 0 { 1 } else { 0 },
            if i % 3 == 0 { "true" } else { "false" },
            (i * 7) % 200, i % 4, 1 + i % 5, i % 10,
        ).unwrap();
        buf.push(b'\n');
    }
    buf
}

fn generate_narrow(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 60);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    for i in 0..n {
        write!(buf, r#"{{"ts":"{}","lvl":"{}","msg":"event {}"}}"#, i, levels[i % 4], i).unwrap();
        buf.push(b'\n');
    }
    buf
}

fn fmt_bytes(n: usize) -> String {
    if n >= 1_073_741_824 { format!("{:.1}GB", n as f64 / 1_073_741_824.0) }
    else if n >= 1_048_576 { format!("{:.1}MB", n as f64 / 1_048_576.0) }
    else if n >= 1024 { format!("{:.1}KB", n as f64 / 1024.0) }
    else { format!("{}B", n) }
}

fn fmt_count(n: usize) -> String {
    if n >= 1_000_000 { format!("{}M", n / 1_000_000) }
    else if n >= 1_000 { format!("{}K", n / 1_000) }
    else { format!("{}", n) }
}

// Additional analysis: actual memory vs reported
fn deep_memory_analysis() {
    use arrow::array::Array;
    
    let data = generate_simple(100_000);
    let raw_size = data.len();
    println!("\n=== Deep Memory Analysis (100K simple lines) ===\n");
    println!("  Raw JSON: {} ({} bytes/line)", fmt_bytes(raw_size), raw_size / 100_000);

    let mut scanner = Scanner::new(ScanConfig::default());
    let batch = scanner.scan(bytes::Bytes::from(data.to_vec())).unwrap();

    println!("  Reported total: {}", fmt_bytes(batch.get_array_memory_size()));
    println!("  Rows: {}, Columns: {}\n", batch.num_rows(), batch.num_columns());

    let mut total_buffers = 0usize;
    let mut total_views = 0usize;
    let mut buffer_addrs: Vec<(usize, usize)> = Vec::new(); // (ptr, len)

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let arr_data = col.to_data();
        
        let mut col_buffer_total = 0;
        let mut col_buffer_count = 0;
        for buf in arr_data.buffers() {
            let ptr = buf.as_ptr() as usize;
            let len = buf.len();
            col_buffer_total += len;
            col_buffer_count += 1;
            buffer_addrs.push((ptr, len));
        }
        
        // Check child data too
        for child in arr_data.child_data() {
            for buf in child.buffers() {
                let ptr = buf.as_ptr() as usize;
                let len = buf.len();
                col_buffer_total += len;
                col_buffer_count += 1;
                buffer_addrs.push((ptr, len));
            }
        }
        
        let null_size = arr_data.nulls().map(|n| n.buffer().len()).unwrap_or(0);
        let reported = col.get_array_memory_size();
        
        println!("  {:<30} {:>3} bufs  {:>10} buf bytes  {:>10} null bytes  {:>10} reported",
            field.name(), col_buffer_count, fmt_bytes(col_buffer_total), fmt_bytes(null_size), fmt_bytes(reported));
        
        total_buffers += col_buffer_total;
        total_views += col_buffer_count;
    }

    // Deduplicate buffers by pointer address
    buffer_addrs.sort();
    buffer_addrs.dedup();
    let unique_buffer_bytes: usize = buffer_addrs.iter().map(|(_, len)| len).sum();
    let total_buffer_bytes: usize = buffer_addrs.iter().map(|(_, len)| len).sum();

    // Check for shared buffers
    let mut seen_ptrs: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut shared_count = 0;
    let mut unique_bytes = 0usize;
    for &(ptr, len) in &buffer_addrs {
        if !seen_ptrs.insert(ptr) {
            shared_count += 1;
        } else {
            unique_bytes += len;
        }
    }

    println!("\n  Total buffers referenced: {}", buffer_addrs.len());
    println!("  Unique buffer pointers:   {}", seen_ptrs.len());
    println!("  Shared (same ptr):        {}", shared_count);
    println!("  Sum of all buffer bytes:  {}", fmt_bytes(total_buffers));
    println!("  Unique buffer bytes:      {}", fmt_bytes(unique_bytes));
    println!("  Reported by Arrow:        {}", fmt_bytes(batch.get_array_memory_size()));
    println!("\n  ACTUAL memory:            {} ({:.2}x vs raw JSON)", 
        fmt_bytes(unique_bytes), unique_bytes as f64 / raw_size as f64);
}

fn deep_memory_analysis_v2() {
    use arrow::array::Array;
    
    let data = generate_simple(10_000);
    let raw_size = data.len();
    println!("\n=== Deep Memory V2 (10K lines, {} raw) ===\n", fmt_bytes(raw_size));

    let mut scanner = Scanner::new(ScanConfig::default());
    let batch = scanner.scan(bytes::Bytes::from(data.to_vec())).unwrap();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let arr_data = col.to_data();
        
        println!("  {}: {:?}", field.name(), field.data_type());
        for (j, buf) in arr_data.buffers().iter().enumerate() {
            println!("    buffer[{}]: ptr=0x{:x} len={} ({})", 
                j, buf.as_ptr() as usize, buf.len(), fmt_bytes(buf.len()));
        }
        if let Some(nulls) = arr_data.nulls() {
            println!("    nulls: {} bytes", nulls.buffer().len());
        }
        println!("    reported: {}", fmt_bytes(col.get_array_memory_size()));
        println!();
    }
    
    // Check if any buffers share the same allocation
    let mut all_ptrs: Vec<(usize, usize, String)> = Vec::new();
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let arr_data = col.to_data();
        for (j, buf) in arr_data.buffers().iter().enumerate() {
            all_ptrs.push((buf.as_ptr() as usize, buf.len(), format!("{}[{}]", field.name(), j)));
        }
    }
    all_ptrs.sort_by_key(|x| x.0);
    
    println!("  All buffer pointers (sorted):");
    for (ptr, len, name) in &all_ptrs {
        println!("    0x{:012x}  {:>10}  {}", ptr, fmt_bytes(*len), name);
    }
    
    // Check for overlapping ranges
    println!("\n  Overlap check:");
    for i in 0..all_ptrs.len() {
        for j in (i+1)..all_ptrs.len() {
            let (p1, l1, n1) = &all_ptrs[i];
            let (p2, _l2, n2) = &all_ptrs[j];
            if p1 + l1 > *p2 {
                println!("    OVERLAP: {} and {} share memory", n1, n2);
            }
        }
    }
}
