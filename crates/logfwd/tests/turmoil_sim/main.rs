#![cfg(feature = "turmoil")]

mod bug_hunt;
mod channel_input;
mod crash_sim;
mod instrumented_sink;
mod network_sim;
mod observable_checkpoint;
mod pipeline_sim;
mod tcp_server;
mod turmoil_tcp_sink;

use std::time::Duration;

/// Return the deterministic seed used by all turmoil simulations.
///
/// Controllable via `TURMOIL_SEED` env var for reproducing failures.
fn turmoil_seed() -> u64 {
    std::env::var("TURMOIL_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(42)
}

/// Create a turmoil simulation builder with deterministic seed and random
/// host scheduling order. Every test should use this instead of
/// `turmoil::Builder::new()` directly.
///
/// With `enable_random_order()`, different seeds exercise different
/// `select!` branch orderings, making every test a property test over
/// scheduling interleaving.
///
/// Use this when you need to call `sim.host()` before `.build()`, or when
/// you need builder methods not covered by `build_sim()` (e.g. `fail_rate`).
fn sim_builder() -> turmoil::Builder {
    let mut b = turmoil::Builder::new();
    b.rng_seed(turmoil_seed()).enable_random_order();
    b
}

/// Shorthand: build a sim with the given duration and tick.
///
/// Use `sim_builder()` directly when you need `sim.host()` before `.build()`
/// or when you need extra builder methods like `fail_rate()`.
fn build_sim(duration_secs: u64, tick_ms: u64) -> turmoil::Sim<'static> {
    let mut b = sim_builder();
    b.simulation_duration(Duration::from_secs(duration_secs))
        .tick_duration(Duration::from_millis(tick_ms));
    b.build()
}
