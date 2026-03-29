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


/// A benchmark agent (logfwd, vector, filebeat, etc.).
pub trait Agent {
    /// Short name used in output (e.g., "vector").
    fn name(&self) -> &str;

    /// Name of the binary to search for on PATH.
    fn binary_name(&self) -> &str;

    /// Download URL for this agent, or None if download isn't supported
    /// (e.g., fluent-bit on macOS).
    fn download_url(&self, os: &str, arch: &str) -> Option<String>;

    /// Write a config file for this agent. Returns path to the config.
    fn write_config(&self, ctx: &BenchContext) -> Result<PathBuf, String>;

    /// Build the Command to launch this agent.
    fn command(&self, binary: &Path, config: &Path, ctx: &BenchContext) -> Command;

    /// Optional pre-flight setup (e.g., fake K8s API for vlagent).
    /// Returns state that will be passed to `teardown()`.
    fn setup(&self, _ctx: &BenchContext) -> Result<SetupState, String> {
        Ok(SetupState::default())
    }

    /// Clean up after benchmark.
    fn teardown(&self, mut state: SetupState) {
        for mut child in state.children.drain(..) {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Handles drop automatically.
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
