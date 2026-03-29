use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, Scenario, SetupState};
use crate::runner::BenchContext;

const VERSION: &str = "4.2.3";
const DOCKER_IMAGE: &str = "fluent/fluent-bit";

pub struct FluentBit;

impl Agent for FluentBit {
    fn name(&self) -> &str {
        "fluent-bit"
    }

    fn binary_name(&self) -> &str {
        "fluent-bit"
    }

    fn download_url(&self, os: &str, _arch: &str) -> Option<String> {
        if os != "linux" {
            return None;
        }
        None
    }

    fn docker_image(&self) -> Option<String> {
        Some(format!("{DOCKER_IMAGE}:{VERSION}"))
    }

    fn write_config(&self, ctx: &BenchContext, scenario: Scenario) -> Result<PathBuf, String> {
        let db_path = ctx.bench_dir.join("fb.db");
        let blackhole_parts: Vec<&str> = ctx.blackhole_addr.split(':').collect();
        let (host, port) = (blackhole_parts[0], blackhole_parts[1]);

        let filter_section = match scenario {
            Scenario::Passthrough => String::new(),
            Scenario::JsonParse => r#"
[FILTER]
    name         parser
    match        *
    key_name     log
    parser       json_parser
    reserve_data on

[FILTER]
    name         modify
    match        *
    rename       duration_ms latency_ms
"#
            .to_string(),
            Scenario::Filter => {
                // Fluent Bit uses Lua or grep to filter. grep supports regex.
                r#"
[FILTER]
    name         grep
    match        *
    regex        log (WARN|ERROR)
"#
                .to_string()
            }
        };

        let parsers_section = match scenario {
            Scenario::JsonParse => {
                let parsers_path = ctx.bench_dir.join("fb_parsers.conf");
                let parsers = r#"[PARSER]
    name         json_parser
    format       json
    time_key     timestamp
    time_format  %Y-%m-%dT%H:%M:%S.%LZ
"#;
                std::fs::write(&parsers_path, parsers).map_err(|e| e.to_string())?;
                format!("    parsers_file {}\n", parsers_path.display())
            }
            _ => String::new(),
        };

        let cfg_path = ctx.bench_dir.join("fluent-bit.conf");
        let config = format!(
            r#"[SERVICE]
    flush        1
    log_level    error
{parsers}
[INPUT]
    name         tail
    path         {data_file}
    read_from_head true
    db           {db}
{filters}
[OUTPUT]
    name         http
    match        *
    host         {host}
    port         {port}
    uri          /
    format       json_lines
"#,
            parsers = parsers_section,
            data_file = ctx.data_file.display(),
            db = db_path.display(),
            filters = filter_section,
        );
        std::fs::write(&cfg_path, config).map_err(|e| e.to_string())?;
        Ok(cfg_path)
    }

    fn command(&self, binary: &Path, config: &Path, _ctx: &BenchContext) -> Command {
        let mut cmd = Command::new(binary);
        cmd.arg("-c").arg(config);
        cmd
    }

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}
