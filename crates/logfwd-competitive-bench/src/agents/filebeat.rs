use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Agent, AgentSample, Scenario, SetupState};
use crate::download::arch_alt;
use crate::runner::BenchContext;

const VERSION: &str = "8.17.0";
const DOCKER_IMAGE: &str = "docker.elastic.co/beats/filebeat";

pub struct Filebeat;

impl Agent for Filebeat {
    fn name(&self) -> &'static str {
        "filebeat"
    }

    fn binary_name(&self) -> &'static str {
        "filebeat"
    }

    fn download_url(&self, os: &str, arch: &str) -> Option<String> {
        let fb_arch = match arch_alt(arch) {
            "arm64" => "aarch64",
            "amd64" => "x86_64",
            other => other,
        };
        Some(format!(
            "https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-{VERSION}-{os}-{fb_arch}.tar.gz"
        ))
    }

    fn docker_image(&self) -> Option<String> {
        Some(format!("{DOCKER_IMAGE}:{VERSION}"))
    }

    fn write_config(&self, ctx: &BenchContext, scenario: Scenario) -> Result<PathBuf, String> {
        let data_dir = ctx.bench_dir.join("fb_data");
        let logs_dir = ctx.bench_dir.join("fb_logs");
        std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&logs_dir).map_err(|e| e.to_string())?;

        let processors_section = match scenario {
            Scenario::Passthrough => String::new(),
            Scenario::JsonParse => r#"
processors:
  - decode_json_fields:
      fields: ["message"]
      target: ""
      overwrite_keys: true
  - rename:
      fields:
        - from: "duration_ms"
          to: "latency_ms"
      ignore_missing: true
"#
            .to_string(),
            Scenario::Filter => {
                // Filebeat uses drop_event with a condition to filter.
                // Keep only lines containing WARN or ERROR.
                r#"
processors:
  - drop_event:
      when:
        not:
          regexp:
            message: "(WARN|ERROR)"
"#
                .to_string()
            }
        };

        let cfg_path = ctx.bench_dir.join("filebeat.yaml");
        let config = format!(
            r#"filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - "{data_file}"
{processors}
output.elasticsearch:
  hosts: ["http://{blackhole}"]

setup.ilm.enabled: false
setup.template.enabled: false

path.data: "{data_dir}"
path.logs: "{logs_dir}"
logging.level: error
http.enabled: true
http.host: "127.0.0.1"
http.port: 5066
"#,
            data_file = ctx.data_file.display(),
            processors = processors_section,
            blackhole = ctx.blackhole_addr,
            data_dir = data_dir.display(),
            logs_dir = logs_dir.display(),
        );
        std::fs::write(&cfg_path, config).map_err(|e| e.to_string())?;
        Ok(cfg_path)
    }

    fn command(&self, binary: &Path, config: &Path, _ctx: &BenchContext) -> Command {
        let mut cmd = Command::new(binary);
        cmd.arg("-e")
            .arg("-c")
            .arg(config)
            .arg("--strict.perms=false");
        cmd
    }

    fn stats_url(&self) -> Option<String> {
        Some("http://127.0.0.1:5066/stats".to_string())
    }

    fn parse_stats(&self, body: &str) -> Option<AgentSample> {
        let v: serde_json::Value = serde_json::from_str(body).ok()?;
        let get = |keys: &[&str]| -> u64 {
            let mut cur = &v;
            for k in keys {
                match cur.get(*k) {
                    Some(next) => cur = next,
                    None => return 0,
                }
            }
            cur.as_u64().unwrap_or(0)
        };
        let get_any = |candidates: &[&[&str]]| -> u64 {
            candidates
                .iter()
                .map(|path| get(path))
                .find(|value| *value > 0)
                .unwrap_or(0)
        };
        let cpu_user_ms = get_any(&[&["beat", "cpu", "user", "time", "ms"]]);
        let cpu_sys_ms = get_any(&[&["beat", "cpu", "system", "time", "ms"]]);
        let cpu_total_ms = get_any(&[&["beat", "cpu", "total", "time", "ms"]]);
        // Some Filebeat versions report ticks instead of ms; convert with HZ=100.
        // Filebeat reports Go runtime CPU ticks; dividing by 100 is a
        // reasonable approximation (Go uses CLOCK_THREAD_CPUTIME_ID).
        let cpu_user_ms = if cpu_user_ms > 0 {
            cpu_user_ms
        } else {
            get(&["beat", "cpu", "user", "ticks"]) * 1000 / 100
        };
        let cpu_sys_ms = if cpu_sys_ms > 0 {
            cpu_sys_ms
        } else {
            get(&["beat", "cpu", "system", "ticks"]) * 1000 / 100
        };
        let cpu_total_ms = if cpu_total_ms > 0 {
            cpu_total_ms
        } else {
            get(&["beat", "cpu", "total", "ticks"]) * 1000 / 100
        };
        let cpu_user_ms = if cpu_user_ms > 0 {
            cpu_user_ms
        } else if cpu_total_ms > 0 && cpu_sys_ms > 0 {
            cpu_total_ms.saturating_sub(cpu_sys_ms)
        } else {
            cpu_total_ms
        };
        Some(AgentSample {
            rss_bytes: get(&["beat", "memstats", "rss"]),
            cpu_user_ms,
            cpu_sys_ms,
            events_total: get(&["libbeat", "pipeline", "events", "total"]),
            errors_total: get(&["libbeat", "output", "events", "failed"]),
            ..Default::default()
        })
    }

    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stats_prefers_explicit_user_cpu() {
        let body = r#"{
            "beat":{"memstats":{"rss":4096},"cpu":{"user":{"time":{"ms":20}},"system":{"time":{"ms":5}},"total":{"time":{"ms":40}}}},
            "libbeat":{"pipeline":{"events":{"total":10}},"output":{"events":{"failed":1}}}
        }"#;
        let sample = Filebeat
            .parse_stats(body)
            .expect("valid filebeat stats payload");
        assert_eq!(sample.cpu_user_ms, 20);
        assert_eq!(sample.cpu_sys_ms, 5);
    }

    #[test]
    fn parse_stats_derives_user_from_total_minus_sys() {
        let body = r#"{
            "beat":{"cpu":{"system":{"time":{"ms":7}},"total":{"time":{"ms":25}}}},
            "libbeat":{"pipeline":{"events":{"total":3}}}
        }"#;
        let sample = Filebeat
            .parse_stats(body)
            .expect("valid filebeat stats payload");
        assert_eq!(sample.cpu_user_ms, 18);
        assert_eq!(sample.cpu_sys_ms, 7);
    }
}
