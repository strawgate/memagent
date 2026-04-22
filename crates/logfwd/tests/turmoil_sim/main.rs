#![cfg(feature = "turmoil")]

mod barrier_sim;
mod bug_hunt;
mod channel_input;
mod crash_sim;
mod fault_harness;
mod fault_scenario_sim;
mod fs_crash_sim;
mod instrumented_sink;
mod linearizability;
mod network_sim;
mod observable_checkpoint;
mod pipeline_sim;
mod tcp_server;
mod trace_bridge;
mod trace_validation;
mod turmoil_tcp_sink;
mod validators;

use std::time::Duration;
use std::{env, sync::OnceLock};

/// Return the deterministic seed used by all turmoil simulations.
///
/// Controllable via `TURMOIL_SEED` env var for reproducing failures.
fn turmoil_seed() -> u64 {
    env::var("TURMOIL_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(42)
}

#[derive(Clone, Copy, Debug)]
struct SimProfile {
    duration_secs: u64,
    tick_ms: u64,
    tcp_capacity: Option<usize>,
    fail_rate: Option<f64>,
    repair_rate: Option<f64>,
}

impl SimProfile {
    const DEFAULT: Self = Self {
        duration_secs: 60,
        tick_ms: 1,
        tcp_capacity: None,
        fail_rate: None,
        repair_rate: None,
    };

    const LONG_IO: Self = Self {
        duration_secs: 120,
        tick_ms: 1,
        tcp_capacity: Some(1024 * 1024),
        fail_rate: None,
        repair_rate: None,
    };

    const TCP_INTERMITTENT: Self = Self {
        duration_secs: 120,
        tick_ms: 1,
        tcp_capacity: None,
        fail_rate: Some(0.001),
        repair_rate: Some(0.90),
    };

    const fn with_duration(mut self, duration_secs: u64) -> Self {
        self.duration_secs = duration_secs;
        self
    }

    const fn with_tick(mut self, tick_ms: u64) -> Self {
        self.tick_ms = tick_ms;
        self
    }
}

#[derive(Clone, Copy)]
struct TraceProgressConfig {
    enabled: bool,
    interval_steps: usize,
}

fn trace_progress_config() -> TraceProgressConfig {
    static CONFIG: OnceLock<TraceProgressConfig> = OnceLock::new();
    *CONFIG.get_or_init(|| {
        let enabled = env::var("TURMOIL_TRACE_PROGRESS")
            .ok()
            .map(|raw| {
                let value = raw.trim().to_ascii_lowercase();
                matches!(value.as_str(), "1" | "true" | "yes" | "on")
            })
            .unwrap_or(false);
        let interval_steps = env::var("TURMOIL_TRACE_PROGRESS_INTERVAL")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(250);
        TraceProgressConfig {
            enabled,
            interval_steps,
        }
    })
}

fn maybe_trace_sim_step(scope: &str, step: usize) {
    let cfg = trace_progress_config();
    if cfg.enabled && step % cfg.interval_steps == 0 {
        eprintln!(
            "[turmoil-progress] scope={scope} seed={} step={step}",
            turmoil_seed()
        );
    }
}

/// Create a deterministic builder and apply a shared simulation profile.
fn sim_builder_with_profile(profile: SimProfile) -> turmoil::Builder {
    let mut b = turmoil::Builder::new();
    b.rng_seed(turmoil_seed())
        .enable_random_order()
        .simulation_duration(Duration::from_secs(profile.duration_secs))
        .tick_duration(Duration::from_millis(profile.tick_ms));
    if let Some(tcp_capacity) = profile.tcp_capacity {
        b.tcp_capacity(tcp_capacity);
    }
    if let Some(fail_rate) = profile.fail_rate {
        b.fail_rate(fail_rate);
    }
    if let Some(repair_rate) = profile.repair_rate {
        b.repair_rate(repair_rate);
    }
    b
}

/// Shorthand: build a sim with the given duration and tick.
///
/// Use `sim_builder_with_profile()` directly when you need `sim.host()` before
/// `.build()` or when you need extra builder methods like `fail_rate()`.
fn build_sim(duration_secs: u64, tick_ms: u64) -> turmoil::Sim<'static> {
    build_sim_with_profile(
        SimProfile::DEFAULT
            .with_duration(duration_secs)
            .with_tick(tick_ms),
    )
}

fn build_sim_with_profile(profile: SimProfile) -> turmoil::Sim<'static> {
    sim_builder_with_profile(profile).build()
}
