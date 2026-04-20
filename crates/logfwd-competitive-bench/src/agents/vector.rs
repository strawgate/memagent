use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, Scenario, SetupState};
use crate::runner::BenchContext;

const VERSION: &str = "0.54.0";
const DOCKER_IMAGE: &str = "timberio/vector";

pub struct Vector;

impl Agent for Vector {
    fn name(&self) -> &'static str {
        "vector"
    }

    fn binary_name(&self) -> &'static str {
        "vector"
    }

    fn download_url(&self, os: &str, arch: &str) -> Option<String> {
        // Vector uses arm64 on macOS, aarch64 on Linux.
        let target = if os == "darwin" {
            let varch = if arch == "aarch64" { "arm64" } else { arch };
            format!("{varch}-apple-darwin")
        } else {
            format!("{arch}-unknown-linux-gnu")
        };
        Some(format!(
            "https://github.com/vectordotdev/vector/releases/download/v{VERSION}/vector-{VERSION}-{target}.tar.gz"
        ))
    }

    fn docker_image(&self) -> Option<String> {
        Some(format!("{DOCKER_IMAGE}:{VERSION}-debian"))
    }

    fn write_config(&self, ctx: &BenchContext, scenario: Scenario) -> Result<PathBuf, String> {
        let data_dir = ctx.bench_dir.join("vector_data");
        std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

        let transforms_section = match scenario {
            Scenario::Passthrough => String::new(),
            Scenario::JsonParse => r#"
transforms:
  parse_json:
    type: remap
    inputs: ["bench_in"]
    source: |
      . = parse_json!(string!(.message))
      .latency_ms = del(.duration_ms)
"#
            .to_string(),
            Scenario::Filter => r#"
transforms:
  filter_level:
    type: filter
    inputs: ["bench_in"]
    condition:
      type: vrl
      source: |
        match(string!(.message), r'"level":"(WARN|ERROR)"')
"#
            .to_string(),
        };

        let sink_input = match scenario {
            Scenario::Passthrough => "bench_in",
            Scenario::JsonParse => "parse_json",
            Scenario::Filter => "filter_level",
        };

        let cfg_path = ctx.bench_dir.join("vector.yaml");
        let config = format!(
            r#"data_dir: "{data_dir}"
sources:
  bench_in:
    type: file
    include:
      - "{data_file}"
    read_from: beginning
    ignore_checkpoints: true
{transforms}sinks:
  bench_out:
    type: http
    inputs: ["{sink_input}"]
    uri: "http://{blackhole}"
    encoding:
      codec: text
    framing:
      method: newline_delimited
    method: post
    batch:
      max_bytes: 1048576
      timeout_secs: 1
"#,
            data_dir = data_dir.display(),
            data_file = ctx.data_file.display(),
            transforms = transforms_section,
            blackhole = ctx.blackhole_addr,
        );
        std::fs::write(&cfg_path, config).map_err(|e| e.to_string())?;
        Ok(cfg_path)
    }

    fn command(&self, binary: &Path, config: &Path, _ctx: &BenchContext) -> Command {
        let mut cmd = Command::new(binary);
        cmd.arg("--config").arg(config).arg("--quiet");
        cmd
    }

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}
