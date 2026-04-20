use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, AgentSample, Scenario, SetupState};
use crate::download::arch_alt;
use crate::runner::BenchContext;

const VERSION: &str = "0.148.0";
const DOCKER_IMAGE: &str = "otel/opentelemetry-collector-contrib";

pub struct Otelcol;

impl Agent for Otelcol {
    fn name(&self) -> &'static str {
        "otelcol"
    }

    fn binary_name(&self) -> &'static str {
        "otelcol-contrib"
    }

    fn download_url(&self, os: &str, arch: &str) -> Option<String> {
        let alt = arch_alt(arch);
        Some(format!(
            "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v{VERSION}/otelcol-contrib_{VERSION}_{os}_{alt}.tar.gz"
        ))
    }

    fn docker_image(&self) -> Option<String> {
        Some(format!("{DOCKER_IMAGE}:{VERSION}"))
    }

    fn write_config(&self, ctx: &BenchContext, scenario: Scenario) -> Result<PathBuf, String> {
        let (processors_def, processors_list) = match scenario {
            Scenario::Passthrough => (String::new(), "batch".to_string()),
            Scenario::JsonParse => {
                let def = r#"
  transform/json_parse:
    log_statements:
      - context: log
        statements:
          - merge_maps(cache, ParseJSON(body), "insert")
          - set(attributes["latency_ms"], cache["duration_ms"])
          - delete_key(cache, "duration_ms")
          - set(body, cache)
"#
                .to_string();
                (def, "transform/json_parse, batch".to_string())
            }
            Scenario::Filter => {
                let def = r#"
  filter/level:
    logs:
      log_record:
        - 'not IsMatch(body, "(WARN|ERROR)")'
"#
                .to_string();
                (def, "filter/level, batch".to_string())
            }
        };

        let cfg_path = ctx.bench_dir.join("otelcol.yaml");
        let config = format!(
            r#"receivers:
  filelog:
    include:
      - "{data_file}"
    start_at: beginning

processors:
  batch:
    send_batch_size: 5000
    timeout: 1s
{processors_def}
exporters:
  otlphttp:
    endpoint: "http://{blackhole}"
    encoding: json
    tls:
      insecure: true

service:
  telemetry:
    logs:
      level: error
    metrics:
      level: detailed
      address: "127.0.0.1:8888"
  pipelines:
    logs:
      receivers: [filelog]
      processors: [{processors_list}]
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

    fn stats_url(&self) -> Option<String> {
        Some("http://127.0.0.1:8888/metrics".to_string())
    }

    fn parse_stats(&self, body: &str) -> Option<AgentSample> {
        let mut rss_bytes = 0u64;
        let mut accepted = 0u64;
        let mut sent = 0u64;
        let mut failed = 0u64;
        for line in body.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            if let Some(val) = extract_prom_value(line, "process_resident_memory_bytes") {
                rss_bytes = val as u64;
            } else if let Some(val) =
                extract_prom_value(line, "otelcol_receiver_accepted_log_records")
            {
                accepted += val as u64;
            } else if let Some(val) = extract_prom_value(line, "otelcol_exporter_sent_log_records")
            {
                sent += val as u64;
            } else if let Some(val) =
                extract_prom_value(line, "otelcol_exporter_send_failed_log_records")
            {
                failed += val as u64;
            }
        }
        Some(AgentSample {
            rss_bytes,
            events_total: accepted.max(sent),
            errors_total: failed,
            ..Default::default()
        })
    }

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}

fn extract_prom_value(line: &str, prefix: &str) -> Option<f64> {
    if !line.starts_with(prefix) {
        return None;
    }
    let val_str = line.rsplit_once(' ')?.1;
    val_str.parse::<f64>().ok()
}
