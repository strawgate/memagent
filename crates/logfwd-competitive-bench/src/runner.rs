//! Generic agent runner: start process, wait for blackhole completion, measure.
//!
//! Supports two execution modes:
//! - **Binary mode** (default): runs agents as local processes.
//! - **Docker mode** (`--docker`): runs agents in containers with resource limits.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use crate::agents::{Agent, AgentSample, Scenario};
use crate::blackhole::Blackhole;

/// Everything an agent needs to set up and run.
pub struct BenchContext {
    pub bench_dir: PathBuf,
    pub data_file: PathBuf,
    /// Blackhole address as seen from the host (used for polling stats).
    pub blackhole_addr: String,
    /// Blackhole address as seen from inside Docker containers.
    /// On Linux with --network=host this is the same as blackhole_addr.
    /// On macOS Docker, this uses host.docker.internal.
    pub docker_blackhole_addr: String,
    pub lines: usize,
}

/// Resource limits for Docker mode.
pub struct DockerLimits {
    /// CPU limit (e.g., "1" for 1 core, "2" for 2 cores).
    pub cpus: String,
    /// Memory limit (e.g., "1g", "512m").
    pub memory: String,
}

impl Default for DockerLimits {
    fn default() -> Self {
        DockerLimits {
            cpus: "1".to_string(),
            memory: "1g".to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct BenchResult {
    pub name: String,
    pub scenario: Scenario,
    pub mode: String,
    pub lines_done: u64,
    pub elapsed_ms: u64,
    /// True if the run hit the timeout before completing.
    #[serde(default)]
    pub timed_out: bool,
    /// Iteration index for repeated runs; defaults to `1` when omitted in input JSON.
    #[serde(default = "default_iteration")]
    pub iteration: usize,
    /// Per-second resource and runtime samples collected during the run.
    ///
    /// Defaults to an empty vector when absent and is omitted from serialized
    /// output when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub samples: Vec<AgentSample>,
}

fn default_iteration() -> usize {
    1
}

/// Run a single agent benchmark in binary mode.
pub fn run_agent(
    agent: &dyn Agent,
    binary: &Path,
    ctx: &BenchContext,
    blackhole: &Blackhole,
    scenario: Scenario,
    iteration: usize,
) -> Result<BenchResult, String> {
    blackhole.reset();

    let setup_state = agent.setup(ctx)?;

    let config_path = agent.write_config(ctx, scenario)?;
    let mut cmd = agent.command(binary, &config_path, ctx);
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    if let Some(env) = &setup_state.env {
        for (k, v) in env {
            cmd.env(k, v);
        }
    }

    let start = Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn {}: {e}", agent.name()))?;

    // Start sampling thread.
    let pid = child.id();
    let stats_url = agent.stats_url();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop.clone();
    let sample_start = start;
    let sample_handle = std::thread::spawn(move || {
        collect_samples(pid, stats_url.as_deref(), sample_start, &stop_clone)
    });

    let expected = (ctx.lines as f64 * scenario.expected_line_ratio()) as usize;
    let (lines_done, timed_out) =
        wait_blackhole_done(blackhole, expected, Duration::from_secs(300));
    let elapsed = start.elapsed();

    // Stop sampling and merge agent-specific stats.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let raw_samples = sample_handle.join().unwrap_or_default();
    let samples: Vec<AgentSample> = raw_samples
        .into_iter()
        .map(|mut rs| {
            if let Some(ref body) = rs.http_body
                && let Some(parsed) = agent.parse_stats(body)
            {
                rs.sample.rss_bytes = rs.sample.rss_bytes.max(parsed.rss_bytes);
                rs.sample.cpu_user_ms = rs.sample.cpu_user_ms.max(parsed.cpu_user_ms);
                rs.sample.cpu_sys_ms = rs.sample.cpu_sys_ms.max(parsed.cpu_sys_ms);
                rs.sample.events_total = parsed.events_total;
                rs.sample.bytes_total = parsed.bytes_total;
                rs.sample.errors_total = parsed.errors_total;
            }
            rs.sample
        })
        .collect();

    kill_and_wait(&mut child);
    agent.teardown(setup_state);

    Ok(BenchResult {
        name: agent.name().to_string(),
        scenario,
        mode: "binary".to_string(),
        lines_done,
        elapsed_ms: elapsed.as_millis() as u64,
        timed_out,
        iteration,
        samples,
    })
}

/// Run a single agent benchmark in Docker mode with resource limits.
pub fn run_agent_docker(
    agent: &dyn Agent,
    image: &str,
    ctx: &BenchContext,
    blackhole: &Blackhole,
    limits: &DockerLimits,
    scenario: Scenario,
    iteration: usize,
) -> Result<BenchResult, String> {
    blackhole.reset();

    let setup_state = agent.setup(ctx)?;

    // Write config. Agent writes it using host addresses; we rewrite for Docker.
    let config_path = agent.write_config(ctx, scenario)?;

    // Rewrite config file: host paths → /bench, host addresses → Docker-reachable.
    // Replace the full host:port first, then bare IP for other ports (e.g., K8s API).
    if let Ok(content) = std::fs::read_to_string(&config_path) {
        let host_ip = ctx.blackhole_addr.split(':').next().unwrap_or("127.0.0.1");
        let docker_ip = ctx
            .docker_blackhole_addr
            .split(':')
            .next()
            .unwrap_or(host_ip);
        let rewritten = content
            .replace(&ctx.bench_dir.to_string_lossy().into_owned(), "/bench")
            .replace(&ctx.blackhole_addr, &ctx.docker_blackhole_addr)
            .replace(host_ip, docker_ip);
        let _ = std::fs::write(&config_path, rewritten);
    }

    // Container name for cleanup.
    let container_name = format!("logfwd-bench-{}", agent.name());

    // Kill any leftover container from a previous run.
    let _ = Command::new("docker")
        .args(["rm", "-f", &container_name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    // Build docker run command.
    let mut cmd = Command::new("docker");
    cmd.arg("run")
        .arg("--rm")
        .arg("--name")
        .arg(&container_name)
        .arg("--cpus")
        .arg(&limits.cpus)
        .arg("--memory")
        .arg(&limits.memory);

    // On Linux, --network=host gives containers direct localhost access.
    // On macOS, Docker runs in a VM so we use host.docker.internal instead.
    if cfg!(target_os = "linux") {
        cmd.arg("--network=host");
    } else {
        cmd.arg("--add-host=host.docker.internal:host-gateway");
    }

    // Mount bench dir so configs and data are accessible.
    cmd.arg("-v")
        .arg(format!("{}:/bench", ctx.bench_dir.display()));

    // Agent-specific extra volumes (e.g., vlagent's /var/log/containers).
    for (host, container) in agent.docker_volumes(ctx) {
        cmd.arg("-v")
            .arg(format!("{}:{}", host.display(), container.display()));
    }

    // Apply env vars from setup.
    if let Some(env) = &setup_state.env {
        for (k, v) in env {
            cmd.arg("-e").arg(format!("{k}={v}"));
        }
    }

    cmd.arg(image);

    // Agent-specific args — rewrite paths and addresses for container context.
    let bench_dir_str = ctx.bench_dir.to_string_lossy().into_owned();
    let args = agent.docker_args(&config_path, ctx);
    for arg in &args {
        let rewritten = arg
            .replace(&bench_dir_str, "/bench")
            .replace(&ctx.blackhole_addr, &ctx.docker_blackhole_addr);
        cmd.arg(rewritten);
    }

    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    let start = Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to start docker container for {}: {e}", agent.name()))?;

    let expected = (ctx.lines as f64 * scenario.expected_line_ratio()) as usize;
    let (lines_done, timed_out) =
        wait_blackhole_done(blackhole, expected, Duration::from_secs(180));
    let elapsed = start.elapsed();

    // Stop the container.
    let _ = Command::new("docker")
        .args(["kill", &container_name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    let _ = child.wait();

    agent.teardown(setup_state);

    Ok(BenchResult {
        name: agent.name().to_string(),
        scenario,
        mode: "docker".to_string(),
        lines_done,
        elapsed_ms: elapsed.as_millis() as u64,
        timed_out,
        iteration,
        samples: Vec::new(),
    })
}

/// Pull a Docker image. Returns Ok if successful.
pub fn docker_pull(image: &str) -> Result<(), String> {
    eprintln!("  Pulling {image}...");
    let status = Command::new("docker")
        .args(["pull", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| format!("failed to run docker pull: {e}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("docker pull {image} failed"))
    }
}

/// Check if Docker is available and running.
pub fn docker_available() -> bool {
    Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Run logfwd under `perf record` for CPU profiling.
/// Returns the path to perf.data. Linux only.
pub fn run_agent_perf(
    agent: &dyn Agent,
    binary: &Path,
    ctx: &BenchContext,
    blackhole: &Blackhole,
    output_dir: &Path,
) -> Result<PathBuf, String> {
    blackhole.reset();

    let setup_state = agent.setup(ctx)?;
    let config_path = agent.write_config(ctx, Scenario::Passthrough)?;

    let perf_data = output_dir.join("perf.data");

    let mut cmd = Command::new("perf");
    cmd.arg("record")
        .arg("-g")
        .arg("--call-graph")
        .arg("dwarf,16384")
        .arg("-F")
        .arg("99")
        .arg("-o")
        .arg(&perf_data)
        .arg("--")
        .arg(binary)
        .args(agent.command(binary, &config_path, ctx).get_args());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    if let Some(env) = &setup_state.env {
        for (k, v) in env {
            cmd.env(k, v);
        }
    }

    let start = Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn perf record: {e}"))?;

    let (_lines_done, _timed_out) =
        wait_blackhole_done(blackhole, ctx.lines, Duration::from_secs(300));
    let elapsed = start.elapsed();

    kill_and_wait(&mut child);
    agent.teardown(setup_state);

    eprintln!(
        "  perf record: {:.1}s, wrote {}",
        elapsed.as_secs_f64(),
        perf_data.display()
    );

    Ok(perf_data)
}

/// Generate a flamegraph SVG from perf.data using inferno.
pub fn generate_flamegraph(perf_data: &Path, output_dir: &Path) -> Result<PathBuf, String> {
    let svg_path = output_dir.join("flamegraph.svg");

    // perf script | inferno-collapse-perf | inferno-flamegraph > flamegraph.svg
    let perf_script = Command::new("perf")
        .arg("script")
        .arg("-i")
        .arg(perf_data)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| format!("perf script: {e}"))?;

    let collapse = Command::new("inferno-collapse-perf")
        .stdin(perf_script.stdout.unwrap())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| format!("inferno-collapse-perf: {e}"))?;

    let flamegraph_out =
        std::fs::File::create(&svg_path).map_err(|e| format!("create flamegraph.svg: {e}"))?;

    let status = Command::new("inferno-flamegraph")
        .arg("--title")
        .arg("logfwd CPU profile")
        .stdin(collapse.stdout.unwrap())
        .stdout(flamegraph_out)
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| format!("inferno-flamegraph: {e}"))?;

    if !status.success() {
        return Err("inferno-flamegraph failed".to_string());
    }

    eprintln!("  flamegraph: {}", svg_path.display());
    Ok(svg_path)
}

/// Run logfwd built with dhat-heap for memory profiling.
/// The dhat binary writes dhat-heap.json to its working directory on exit.
pub fn run_agent_dhat(
    agent: &dyn Agent,
    dhat_binary: &Path,
    ctx: &BenchContext,
    blackhole: &Blackhole,
    output_dir: &Path,
) -> Result<PathBuf, String> {
    blackhole.reset();

    let setup_state = agent.setup(ctx)?;
    let config_path = agent.write_config(ctx, Scenario::Passthrough)?;

    let mut cmd = agent.command(dhat_binary, &config_path, ctx);
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    // dhat writes dhat-heap.json to the current directory.
    cmd.current_dir(output_dir);

    if let Some(env) = &setup_state.env {
        for (k, v) in env {
            cmd.env(k, v);
        }
    }

    let start = Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn dhat binary: {e}"))?;

    let (_lines_done, _timed_out) =
        wait_blackhole_done(blackhole, ctx.lines, Duration::from_secs(300));
    let elapsed = start.elapsed();

    // dhat writes its output on clean exit, so send SIGTERM first.
    #[cfg(unix)]
    {
        // SAFETY: `child.id()` returns the PID of a live child process that
        // we spawned. Sending `SIGTERM` to a valid PID is safe.
        unsafe {
            libc::kill(child.id() as i32, libc::SIGTERM);
        }
        // Give it a moment to write the profile, then force kill.
        std::thread::sleep(Duration::from_secs(2));
    }
    kill_and_wait(&mut child);
    agent.teardown(setup_state);

    let dhat_file = output_dir.join("dhat-heap.json");
    if dhat_file.exists() {
        eprintln!(
            "  dhat-heap: {:.1}s, wrote {}",
            elapsed.as_secs_f64(),
            dhat_file.display()
        );
        Ok(dhat_file)
    } else {
        Err("dhat-heap.json not found — was logfwd built with --features dhat-heap?".to_string())
    }
}

/// Check if `perf` is available (Linux only).
pub fn perf_available() -> bool {
    Command::new("perf")
        .arg("version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Check if inferno tools are available.
pub fn inferno_available() -> bool {
    Command::new("inferno-collapse-perf")
        .arg("--help")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Per-second sampling during benchmarks
// ---------------------------------------------------------------------------

struct RawSample {
    sample: AgentSample,
    http_body: Option<String>,
}

/// Collect time-series samples every second until stopped.
fn collect_samples(
    pid: u32,
    stats_url: Option<&str>,
    start: Instant,
    stop: &std::sync::atomic::AtomicBool,
) -> Vec<RawSample> {
    let mut samples = Vec::new();
    let mut next_tick = Instant::now() + Duration::from_secs(1);

    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        let now = Instant::now();
        if now < next_tick {
            std::thread::sleep(next_tick - now);
        }
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        let elapsed = start.elapsed().as_secs_f64();
        let (rss, cpu_user, cpu_sys) = procfs_stats(pid);

        let http_body = stats_url.and_then(|url| {
            ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(2)))
                .build()
                .new_agent()
                .get(url)
                .call()
                .ok()
                .and_then(|resp| resp.into_body().read_to_string().ok())
        });

        samples.push(RawSample {
            sample: AgentSample {
                elapsed_sec: elapsed,
                rss_bytes: rss,
                cpu_user_ms: cpu_user,
                cpu_sys_ms: cpu_sys,
                ..Default::default()
            },
            http_body,
        });
        next_tick += Duration::from_secs(1);
    }

    samples
}

/// Read RSS and CPU from /proc/{pid} (Linux only).
fn procfs_stats(pid: u32) -> (u64, u64, u64) {
    #[cfg(target_os = "linux")]
    {
        let rss = std::fs::read_to_string(format!("/proc/{pid}/status"))
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
            .unwrap_or(0);

        let (user_ms, sys_ms) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|s| {
                let after_comm = s.rfind(')')?.checked_add(2)?;
                let fields: Vec<&str> = s[after_comm..].split_whitespace().collect();
                let tps = clock_ticks_per_second();
                let u = fields.get(11)?.parse::<u64>().ok()? * 1000 / tps;
                let sy = fields.get(12)?.parse::<u64>().ok()? * 1000 / tps;
                Some((u, sy))
            })
            .unwrap_or((0, 0));

        (rss, user_ms, sys_ms)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        (0, 0, 0)
    }
}

#[cfg(target_os = "linux")]
fn clock_ticks_per_second() -> u64 {
    static TICKS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *TICKS.get_or_init(|| {
        Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(100)
    })
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
fn clock_ticks_per_second() -> u64 {
    100
}

/// Poll blackhole stats until lines reach expected count or bytes stabilize.
/// Returns (lines_done, timed_out).
fn wait_blackhole_done(blackhole: &Blackhole, expected: usize, timeout: Duration) -> (u64, bool) {
    let start = Instant::now();
    let mut prev_bytes = 0u64;
    let mut stable_count = 0u32;

    loop {
        if start.elapsed() > timeout {
            let (lines, _) = blackhole.stats();
            return (lines, true);
        }

        std::thread::sleep(Duration::from_millis(100));
        let (lines, bytes) = blackhole.stats();

        if lines >= expected as u64 {
            return (lines, false);
        }

        if bytes == prev_bytes && bytes > 0 {
            stable_count += 1;
            if stable_count >= 30 {
                return (lines, false);
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

fn kill_and_wait(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Wait for an arbitrary HTTP endpoint to be ready (used by agents for setup).
pub fn wait_for_ready(url: &str, timeout: Duration) -> Result<(), String> {
    wait_for_http(url, timeout)
}
