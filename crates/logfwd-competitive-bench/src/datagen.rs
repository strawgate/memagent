//! Generate synthetic JSON log data for benchmarks.

use std::io::{self, BufWriter, Write};
use std::path::Path;

/// Generate `num_lines` JSON log lines to `output`. Returns file size in bytes.
pub fn generate_json_log_file(num_lines: usize, output: &Path) -> io::Result<u64> {
    let file = std::fs::File::create(output)?;
    let mut w = BufWriter::with_capacity(1024 * 1024, file);

    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v2/products",
        "/health",
        "/api/v1/auth",
    ];

    for i in 0..num_lines {
        let level = levels[i % 4];
        let path = paths[i % 5];
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", (i as u64).wrapping_mul(0x517cc1b727220a95));

        write!(
            w,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{level}","message":"request handled GET {path}/{id}","duration_ms":{dur},"request_id":"{rid}","service":"myapp"}}"#,
            i % 1000,
        )?;
        w.write_all(b"\n")?;
    }

    w.flush()?;
    let size = std::fs::metadata(output)?.len();
    eprintln!(
        "  {:.1} MB, avg {:.0} bytes/line",
        size as f64 / 1_048_576.0,
        size as f64 / num_lines as f64,
    );
    Ok(size)
}
