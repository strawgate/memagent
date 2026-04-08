# Turmoil Deterministic Simulation Testing — Reference

Version: turmoil 0.7.1 (January 2026)

Turmoil is a deterministic simulation testing framework from the Tokio project.
It runs multiple simulated "hosts" as concurrent futures on a single OS thread
with simulated time, network, and (optionally) filesystem. Tests are
reproducible via seeded RNG — a failing seed can be replayed locally.

---

## 1. Core API

### Builder

```rust
let mut sim = turmoil::Builder::new()
    .simulation_duration(Duration::from_secs(60))
    .tick_duration(Duration::from_millis(10))   // virtual time granularity
    .min_message_latency(Duration::from_millis(1))
    .max_message_latency(Duration::from_millis(50))
    .fail_rate(0.05)        // random message loss (5%)
    .tcp_capacity(65536)    // per-host TCP buffer
    .enable_random_order()  // shuffle host execution each tick
    .rng_seed(42)           // deterministic seed
    .build();
```

### Hosts and clients

```rust
// Long-lived server — restartable via crash/bounce
sim.host("server", || async {
    let listener = turmoil::net::TcpListener::bind("0.0.0.0:9999").await?;
    loop { let (stream, _) = listener.accept().await?; /* ... */ }
});

// One-shot client — runs to completion or error
sim.client("test", async {
    let stream = turmoil::net::TcpStream::connect(("server", 9999)).await?;
    /* assertions */
    Ok(())
});

sim.run().unwrap(); // blocks until all clients complete
```

### Fault injection

```rust
// Bidirectional partition
turmoil::partition("a", "b");
turmoil::repair("a", "b");

// One-way (a can't reach b, but b can reach a)
turmoil::partition_oneway("a", "b");

// Hold messages (buffer, don't deliver)
turmoil::hold("a", "b");
turmoil::release("a", "b");

// Host crash (kills runtime, loses unsynced fs state) and restart
sim.crash("server");
sim.bounce("server");   // restarts with same closure

// Per-link tuning
sim.set_link_latency("a", "b", Duration::from_millis(200));
sim.set_link_fail_rate("a", "b", 0.1);
```

### Step-by-step execution

```rust
let result = sim.step()?; // advance one tick manually
// Returns Ok(true) when all clients completed
```

### Message inspection

```rust
sim.links(|iter| {
    for link in iter {
        for msg in link.iter() {
            // inspect in-flight messages
        }
    }
});
```

---

## 2. What works inside Turmoil

| Primitive | Status | Notes |
|-----------|--------|-------|
| `tokio::sync::mpsc` | Works | Pure waker-based |
| `tokio::time::sleep/timeout/interval` | Works | Uses simulated clock |
| `tokio::select!` | Works | Pure polling macro |
| `CancellationToken` | Works | Pure waker-based |
| `tokio::task::spawn` | Works | Runs on simulated runtime |
| `tokio::task::spawn_local` | Works | Single-threaded, natural fit |
| `tokio::time::Instant::now()` | Works | Returns simulated time |
| `turmoil::net::TcpStream` | Works | Implements AsyncRead/AsyncWrite |

## 3. What breaks inside Turmoil

| Primitive | Status | Workaround |
|-----------|--------|------------|
| `tokio::task::block_in_place` | **Panics** | Call function directly (OK on single-thread) |
| `std::thread::spawn` | **Escapes sim** | Use `tokio::task::spawn` |
| `std::thread::sleep` | **Real time** | Use `tokio::time::sleep` |
| `std::time::Instant::now()` | **Real time** | Use `tokio::time::Instant::now()` for timing-sensitive logic |
| `tokio::net::TcpStream` | **Bypasses sim** | Use `turmoil::net::TcpStream` |
| `reqwest::Client` | **Bypasses sim** | Use raw `turmoil::net::TcpStream` with HTTP framing |
| `spawn_blocking` | **Escapes sim** | Restructure as async or call inline |
| Hash randomization | **Non-deterministic** | Use `BTreeMap` for order-dependent logic |

---

## 4. Unstable features

### Filesystem simulation (`unstable-fs`)

```rust
use turmoil::fs::shim::std::fs::{OpenOptions, create_dir_all, sync_dir};

create_dir_all("/data")?;
let file = OpenOptions::new().write(true).create(true).open("/data/db")?;
file.write_all_at(b"data", 0)?;
file.sync_all()?;  // durable — survives crash
// Without sync_all(), data is LOST on sim.crash()
```

`FsConfig` options: `sync_probability(f64)`, `capacity(usize)`, `io_error_probability(f64)`.

Useful for testing checkpoint write-rename-fsync atomicity.

### Barriers (`unstable-barriers`)

```rust
// In source code:
turmoil::barriers::trigger(MyEvent::WriteComplete).await;

// In test code:
let mut barrier = Barrier::build(Reaction::Suspend, |e| matches!(e, MyEvent::WriteComplete));
let triggered = barrier.wait().await.unwrap();
// source is now suspended — inspect state
drop(triggered); // resume
```

Reactions: `Noop` (observe), `Suspend` (pause), `Panic` (inject crash).

---

## 5. Best practices

### Seed-based reproducibility

- Log the seed on every test run: `eprintln!("turmoil seed: {seed}")`
- Accept `TURMOIL_SEED` env var override for replay
- Store seeds of discovered bugs as regression tests
- Run nightly CI with 100+ random seeds (property-based campaign)

### Determinism hygiene

- **Audit all time calls**: `std::time::Instant` leaks real time into simulated
  code. Use `tokio::time::Instant` for timing-sensitive decisions.
- **Avoid HashMap for order-dependent logic**: Hash randomization breaks
  determinism across runs. Use `BTreeMap` where iteration order matters.
- **Never use `spawn_blocking`**: OS threads escape the simulation entirely.
  If you need blocking work, call it inline (safe on single-thread runtime).
- **Meta-test for determinism**: Run same seed twice, compare TRACE logs
  byte-for-byte. Catches hidden non-determinism from dependencies.

### Test design

- **Document the failure scenario**: Add comments explaining what each
  `partition()` / `hold()` / `crash()` is testing. Readers won't understand
  the intent from the API calls alone.
- **Use step-by-step execution for precise timing**: `sim.step()` gives
  tick-level control when you need to inject faults at exact moments.
- **Combine with proptest**: Generate random fault injection sequences
  (partition timing, message drop rates, crash points) systematically.
- **Test both the happy path AND the recovery**: A partition test should
  verify the system survives the fault AND recovers after repair.

### Architecture for simulability

- **State machines over threads**: Core logic as single-threaded message
  handlers makes simulation trivial. Our `BatchAccumulator` is an example.
- **Trait-based IO**: Abstract network IO behind traits so `turmoil::net`
  can be swapped in. Our `Sink` trait already enables this.
- **Feature-gate simulation code**: Use `#[cfg(feature = "turmoil")]` to
  keep simulation-specific paths out of production builds.

---

## 6. Pitfalls

1. **Assuming determinism without verification** — Seeding the RNG is
   necessary but not sufficient. Dependencies may use `std::time`,
   `getrandom`, or hash randomization internally.

2. **Testing CPU-intensive code** — Single-threaded execution means no
   parallelism. Use Turmoil for IO-dominated tests; use traditional
   benchmarks for CPU-bound scanner/parser code.

3. **Over-mocking external services** — If you mock too aggressively, your
   simulation doesn't test real failure modes. Use in-memory implementations
   (like `InMemoryCheckpointStore`) that preserve real semantics.

4. **Ignoring unstable feature warnings** — `unstable-fs` and
   `unstable-barriers` may change API. Pin turmoil version.

---

## 7. logfwd-specific patterns

### Our feature flag

```toml
[features]
turmoil = ["dep:turmoil"]
```

### scan_maybe_blocking

```rust
fn scan_maybe_blocking(scanner: &mut Scanner, buf: Bytes) -> Result<RecordBatch, ArrowError> {
    #[cfg(feature = "turmoil")]
    { scanner.scan(buf) }
    #[cfg(not(feature = "turmoil"))]
    { tokio::task::block_in_place(|| scanner.scan(buf)) }
}
```

### async_input_poll_loop

Replaces `std::thread::spawn` + `std::thread::sleep` with
`tokio::task::spawn` + `tokio::time::sleep` under the turmoil feature.
Deterministic time advances instead of real wall-clock delays.

### Pipeline::for_simulation

Minimal constructor that bypasses config parsing, filesystem, and OTel
meter setup. Uses default scanner (JSON passthrough) and identity SQL
transform. Accepts injected `Sink` and `InputSource`.

### ChannelInputSource

Mock `InputSource` backed by `VecDeque<Vec<u8>>`. Returns pre-loaded
chunks one at a time via `poll()`. No filesystem dependency.

---

## 8. Competitive landscape

| Framework | Scope | Trade-off |
|-----------|-------|-----------|
| **Turmoil** | Tokio-native, single-thread sim | Rust-only, no CPU/memory pressure sim |
| **MadSim** | libc-level control (time, entropy) | Higher complexity, less Tokio integration |
| **mad-turmoil** | Bridge: MadSim determinism + Turmoil API | Early (2025), limited adoption |
| **Antithesis** | Hypervisor-level (Docker/VM) | Language-agnostic, full system, but SaaS-only |
| **Jepsen** | Consensus protocol verification | Clojure DSL, specialized for distributed DBs |
| **Loom** | Thread interleaving at executor level | Complementary — different fault class |

---

## 9. References

- [Turmoil docs](https://docs.rs/turmoil/latest/turmoil/)
- [Turmoil GitHub](https://github.com/tokio-rs/turmoil)
- [Announcing turmoil — source post](https://github.com/tokio-rs/website/blob/master/content/blog/2023-01-03-announcing-turmoil.md)
- [DST for async Rust — S2.dev](https://s2.dev/blog/dst)
- [RisingWave DST part 1](https://www.risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/)
- [RisingWave DST part 2](https://www.risingwave.com/blog/applying-deterministic-simulation-the-risingwave-story-part-2-of-2/)
- [WarpStream DST](https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas)
- [Polar Signals DST in Rust](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust)
- [FoundationDB DST whitepaper](https://www.foundationdb.org/files/fdb-paper.pdf)
