use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, AgentSample, Scenario, SetupState};
use crate::runner::BenchContext;

pub struct Logfwd;

impl Agent for Logfwd {
    fn name(&self) -> &'static str {
        "ff"
    }

    fn binary_name(&self) -> &'static str {
        "ff"
    }

    fn download_url(&self, _os: &str, _arch: &str) -> Option<String> {
        // ff is built locally; never downloaded.
        None
    }

    fn write_config(&self, ctx: &BenchContext, scenario: Scenario) -> Result<PathBuf, String> {
        let cfg_path = ctx.bench_dir.join("ff.yaml");
        // Since the type-suffix redesign (#684), columns are only suffixed on
        // type conflict. With uniform test data, columns are unsuffixed.
        let transform = match scenario {
            Scenario::Passthrough => "SELECT * FROM logs".to_string(),
            Scenario::JsonParse => {
                "SELECT timestamp, level, message, duration_ms AS latency_ms, request_id, service FROM logs".to_string()
            }
            Scenario::Filter => {
                "SELECT * FROM logs WHERE level IN ('WARN', 'ERROR')".to_string()
            }
        };
        let config = format!(
            r#"server:
  diagnostics: "127.0.0.1:19876"
pipelines:
  bench:
    inputs:
      - type: file
        path: "{data_file}"
        format: json
        read_from: beginning
    transform: "{transform}"
    outputs:
      - type: http
        endpoint: "http://{blackhole}"
        format: json
"#,
            data_file = ctx.data_file.display(),
            blackhole = ctx.blackhole_addr,
        );
        std::fs::write(&cfg_path, config).map_err(|e| e.to_string())?;
        Ok(cfg_path)
    }

    fn command(&self, binary: &Path, config: &Path, _ctx: &BenchContext) -> Command {
        let mut cmd = Command::new(binary);
        cmd.arg("run").arg("--config").arg(config);
        cmd
    }

    fn stats_url(&self) -> Option<String> {
        Some("http://127.0.0.1:19876/admin/v1/stats".to_string())
    }

    fn parse_stats(&self, body: &str) -> Option<AgentSample> {
        let v: serde_json::Value = serde_json::from_str(body).ok()?;
        Some(AgentSample {
            rss_bytes: v.get("rss_bytes")?.as_u64().unwrap_or(0),
            cpu_user_ms: v.get("cpu_user_ms")?.as_u64().unwrap_or(0),
            cpu_sys_ms: v.get("cpu_sys_ms")?.as_u64().unwrap_or(0),
            events_total: v.get("output_lines")?.as_u64().unwrap_or(0),
            bytes_total: v.get("input_bytes")?.as_u64().unwrap_or(0),
            errors_total: v.get("output_errors")?.as_u64().unwrap_or(0),
            ..Default::default()
        })
    }

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}
