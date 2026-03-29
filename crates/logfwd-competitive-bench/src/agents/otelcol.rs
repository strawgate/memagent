use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, SetupState};
use crate::download::arch_alt;
use crate::runner::BenchContext;

const VERSION: &str = "0.148.0";

pub struct Otelcol;

impl Agent for Otelcol {
    fn name(&self) -> &str {
        "otelcol"
    }

    fn binary_name(&self) -> &str {
        "otelcol-contrib"
    }

    fn download_url(&self, os: &str, arch: &str) -> Option<String> {
        let alt = arch_alt(arch);
        Some(format!(
            "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v{VERSION}/otelcol-contrib_{VERSION}_{os}_{alt}.tar.gz"
        ))
    }

    fn write_config(&self, ctx: &BenchContext) -> Result<PathBuf, String> {
        let cfg_path = ctx.bench_dir.join("otelcol.yaml");
        let config = format!(
            r#"receivers:
  filelog:
    include:
      - "{data_file}"
    start_at: beginning

processors:
  batch:
    send_batch_size: 1000
    timeout: 200ms

exporters:
  otlphttp:
    endpoint: "http://{blackhole}"
    tls:
      insecure: true

service:
  telemetry:
    logs:
      level: error
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [otlphttp]
"#,
            data_file = ctx.data_file.display(),
            blackhole = ctx.blackhole_addr,
        );
        std::fs::write(&cfg_path, config).map_err(|e| e.to_string())?;
        Ok(cfg_path)
    }

    fn command(&self, binary: &Path, config: &Path, _ctx: &BenchContext) -> Command {
        let mut cmd = Command::new(binary);
        cmd.arg("--config").arg(config);
        cmd
    }

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}
