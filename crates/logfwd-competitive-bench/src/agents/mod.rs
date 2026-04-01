//! Agent trait and registry.

mod filebeat;
mod fluent_bit;
mod logfwd;
mod otelcol;
mod vector;
mod vlagent;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::runner::BenchContext;

/// A benchmark scenario defining what work each agent does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// Tail file → sink, no transforms. Measures raw I/O throughput.
    Passthrough,
    /// Parse JSON log lines and extract/rename fields.
    JsonParse,
    /// Filter logs: only forward ERROR and WARN levels (~50% of input).
    Filter,
}

impl Scenario {
    pub fn name(&self) -> &str {
        match self {
            Scenario::Passthrough => "passthrough",
            Scenario::JsonParse => "json_parse",
            Scenario::Filter => "filter",
        }
    }

    pub fn description(&self) -> &str {
        match self {
            Scenario::Passthrough => "file tail -> sink (no transforms)",
            Scenario::JsonParse => "file tail -> JSON parse + field extract -> sink",
            Scenario::Filter => "file tail -> filter ERROR/WARN only -> sink",
        }
    }

    pub fn all() -> &'static [Scenario] {
        &[Scenario::Passthrough, Scenario::JsonParse, Scenario::Filter]
    }

    pub fn from_name(s: &str) -> Option<Scenario> {
        match s {
            "passthrough" => Some(Scenario::Passthrough),
            "json_parse" => Some(Scenario::JsonParse),
            "filter" => Some(Scenario::Filter),
            _ => None,
        }
    }

    /// For the filter scenario, how many lines should the blackhole expect?
    /// Our datagen cycles levels as INFO, DEBUG, WARN, ERROR (i%4),
    /// so exactly 50% are ERROR or WARN.
    pub fn expected_line_ratio(&self) -> f64 {
        match self {
            Scenario::Filter => 0.5,
            _ => 1.0,
        }
    }
}

/// State returned by `Agent::setup()`, passed back to `teardown()`.
#[derive(Default)]
pub struct SetupState {
    /// Environment variables to set on the agent process.
    pub env: Option<HashMap<String, String>>,
    /// Opaque handles to keep alive until teardown (e.g., fake K8s API server).
    /// The handles are dropped when SetupState is dropped, stopping any
    /// background servers they represent.
    #[expect(dead_code, reason = "handles kept alive via Drop, not read")]
    pub handles: Vec<Box<dyn std::any::Any + Send>>,
    /// Child processes to kill on teardown.
    pub children: Vec<std::process::Child>,
}

/// A snapshot of runtime metrics collected during a benchmark run.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AgentSample {
    /// Seconds elapsed since the benchmark run started.
    #[serde(default)]
    pub elapsed_sec: f64,
    /// Resident set size in bytes.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub rss_bytes: u64,
    /// User CPU time in milliseconds.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub cpu_user_ms: u64,
    /// System CPU time in milliseconds.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub cpu_sys_ms: u64,
    /// Total events observed by the agent.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub events_total: u64,
    /// Total bytes observed by the agent.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub bytes_total: u64,
    /// Total failed events observed by the agent.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub errors_total: u64,
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

/// A benchmark agent (logfwd, vector, filebeat, etc.).
pub trait Agent {
    /// Short name used in output (e.g., "vector").
    fn name(&self) -> &str;

    /// Name of the binary to search for on PATH.
    fn binary_name(&self) -> &str;

    /// Download URL for this agent, or None if download isn't supported.
    fn download_url(&self, os: &str, arch: &str) -> Option<String>;

    /// Docker image tag (e.g., "timberio/vector:0.54.0-debian").
    /// Returns None if this agent doesn't support Docker mode.
    fn docker_image(&self) -> Option<String> {
        None
    }

    /// Command-line args for Docker mode. The runner handles `docker run`,
    /// resource limits, network, and volume mounts — this just returns the
    /// container entrypoint args.
    fn docker_args(&self, config: &Path, ctx: &BenchContext) -> Vec<String> {
        // Default: same as binary command args.
        let cmd = self.command(Path::new("/unused"), config, ctx);
        cmd.get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    /// Extra volume mounts for Docker mode, as (host_path, container_path) pairs.
    /// The runner always mounts bench_dir → /bench.
    fn docker_volumes(&self, _ctx: &BenchContext) -> Vec<(PathBuf, PathBuf)> {
        Vec::new()
    }

    /// Write a config file for this agent and scenario. Returns path to the config.
    fn write_config(&self, ctx: &BenchContext, scenario: Scenario) -> Result<PathBuf, String>;

    /// Build the Command to launch this agent in binary mode.
    fn command(&self, binary: &Path, config: &Path, ctx: &BenchContext) -> Command;

    /// Optional pre-flight setup (e.g., fake K8s API for vlagent).
    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }

    /// URL to poll for runtime stats during the benchmark (polled every 1s).
    fn stats_url(&self) -> Option<String> {
        None
    }

    /// Parse agent-specific stats response into a common AgentSample.
    fn parse_stats(&self, _body: &str) -> Option<AgentSample> {
        None
    }

    /// Clean up after benchmark.
    fn teardown(&self, mut state: SetupState) {
        for mut child in state.children.drain(..) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// All known agents in benchmark order.
pub fn all_agents() -> Vec<Box<dyn Agent>> {
    vec![
        Box::new(logfwd::Logfwd),
        Box::new(vector::Vector),
        Box::new(fluent_bit::FluentBit),
        Box::new(filebeat::Filebeat),
        Box::new(otelcol::Otelcol),
        Box::new(vlagent::Vlagent),
    ]
}
