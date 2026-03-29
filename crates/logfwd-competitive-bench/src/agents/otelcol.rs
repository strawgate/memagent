use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, Scenario, SetupState};
use crate::download::arch_alt;
use crate::runner::BenchContext;

const VERSION: &str = "0.148.0";
const DOCKER_IMAGE: &str = "otel/opentelemetry-collector-contrib";

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

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}
