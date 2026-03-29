//! Generic agent runner: start process, wait for blackhole completion, measure.

use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use crate::agents::Agent;
use crate::blackhole::Blackhole;

/// Everything an agent needs to set up and run.
pub struct BenchContext {
    pub bench_dir: PathBuf,
    pub data_file: PathBuf,
    pub blackhole_addr: String,
    pub lines: usize,
    /// Path to the logfwd binary (reserved for future use).
    #[expect(dead_code, reason = "reserved for logfwd-specific setup")]
    pub logfwd_binary: Option<PathBuf>,
}

pub struct BenchResult {
    pub name: String,
    pub lines_done: u64,
    pub elapsed_ms: u64,
}

/// Run a single agent benchmark against a shared blackhole instance.
/// Resets blackhole counters before each run.
pub fn run_agent(
    agent: &dyn Agent,
    binary: &Path,
    ctx: &BenchContext,
    blackhole: &Blackhole,
) -> Result<BenchResult, String> {
    // Reset blackhole counters for this agent.
    blackhole.reset();

    // Agent-specific setup (e.g., fake K8s API for vlagent).
    let setup_state = agent.setup(ctx)?;

    // Write config and build command.
    let config_path = agent.write_config(ctx)?;
    let mut cmd = agent.command(binary, &config_path, ctx);
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    // Apply any env vars from setup.
    if let Some(env) = &setup_state.env {
        for (k, v) in env {
            cmd.env(k, v);
        }
    }

    let start = Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn {}: {e}", agent.name()))?;

    // Wait for blackhole to receive all data (bytes-based stabilization).
    let lines_done = wait_blackhole_done(blackhole, ctx.lines, Duration::from_secs(120));
    let elapsed = start.elapsed();

    // Clean up agent process.
    kill_and_wait(&mut child);

    // Agent-specific teardown.
    agent.teardown(setup_state);

    Ok(BenchResult {
        name: agent.name().to_string(),
        lines_done,
        elapsed_ms: elapsed.as_millis() as u64,
    })
}

/// Poll blackhole stats until lines reach expected count or bytes stabilize.
fn wait_blackhole_done(blackhole: &Blackhole, expected: usize, timeout: Duration) -> u64 {
    let start = Instant::now();
    let mut prev_bytes = 0u64;
    let mut stable_count = 0u32;

    loop {
        if start.elapsed() > timeout {
            let (lines, _) = blackhole.stats();
            return lines;
        }

        std::thread::sleep(Duration::from_millis(100));
        let (lines, bytes) = blackhole.stats();

        // Fast path: line count reached expected.
        if lines >= expected as u64 {
            return lines;
        }

        // Slow path: bytes stopped flowing for 3s.
        if bytes == prev_bytes && bytes > 0 {
            stable_count += 1;
            if stable_count >= 30 {
                return lines;
            }
        } else {
            stable_count = 0;
        }
        prev_bytes = bytes;
    }
}

/// Wait for an HTTP endpoint to respond with 200.
fn wait_for_http(url: &str, timeout: Duration) -> Result<(), String> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(format!("timeout waiting for {url}"));
        }
        match ureq::get(url).call() {
            Ok(resp) if resp.status() == 200 => return Ok(()),
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// Send SIGKILL and wait for process to exit.
fn kill_and_wait(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Wait for an arbitrary HTTP endpoint to be ready (used by agents for setup).
pub fn wait_for_ready(url: &str, timeout: Duration) -> Result<(), String> {
    wait_for_http(url, timeout)
}
