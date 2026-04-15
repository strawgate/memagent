# Production Patterns Reference

Concrete patterns for error handling, retry, backpressure, logging, and
health checks. Based on research of production Rust projects and the
Rust ecosystem (2025-2026).

---

## Error handling

Use `thiserror` for library error enums. Carry retriability as a method
on the error type, not as an enum variant:

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    #[error("http transport: {0}")]
    Transport(#[source] ureq::Error),

    #[error("http {status}")]
    HttpStatus { status: u16, body: String },

    #[error("serialization: {0}")]
    Serialize(#[source] serde_json::Error),
}

impl SinkError {
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Transport(e) => matches!(e,
                ureq::Error::Io(_) |
                ureq::Error::Timeout(_) |
                ureq::Error::ConnectionFailed
            ),
            Self::HttpStatus { status, .. } => {
                *status == 429 || *status == 408 || (500..600).contains(status)
            }
            Self::Serialize(_) => false,
        }
    }
}
```

Key principles:
- Separate transport errors (always retriable) from status errors
  (depends on code)
- Use `#[non_exhaustive]` on public error enums
- Don't erase error types with `.to_string()` — preserve the original
  for classification

---

## Retry

Use the `backon` crate. It provides composable retry without coupling to
tower's Service abstraction. Clean API for error classification,
backoff, and observability:

```rust
use backon::{ExponentialBuilder, Retryable};

let result = (|| async { send_batch(&payload).await })
    .retry(ExponentialBuilder::default()
        .with_jitter()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_max_times(8))
    .when(|e: &SinkError| e.is_retriable())
    .notify(|err, dur| {
        tracing::warn!(?err, ?dur, "retrying request");
    })
    .await?;
```

For synchronous code (current pipeline), `backon` also supports blocking
retry via `BlockingRetryable`.

### On retry exhaustion

Drop the batch, increment `batches_dropped` metric, continue. No
dead-letter queue. The source checkpoint doesn't advance (data stays in
file/queue for re-read). This is the standard approach — production
pipelines don't implement DLQ at the forwarder level.

### Backoff strategy

Exponential with full jitter. Defaults: 100ms min, 30s max, 8 attempts.
Fibonacci backoff is also common but exponential is simpler and equally
effective.

### ureq error classification

```rust
fn is_retriable_ureq(err: &ureq::Error) -> bool {
    match err {
        ureq::Error::StatusCode(code) => {
            *code == 429 || *code == 408 || (500..600).contains(code)
        }
        ureq::Error::Io(_)
        | ureq::Error::Timeout(_)
        | ureq::Error::ConnectionFailed => true,
        _ => false,
    }
}
```

---

## Backpressure

### Synchronous pipeline (current)

Check buffer size before polling. Don't call poll() when buffer is full.
The file is the durable buffer — data stays on disk until read:

```rust
const MAX_INPUT_BUFFER: usize = 16 * 1024 * 1024;  // 16MB per input
const MAX_PIPELINE_BUFFER: usize = 64 * 1024 * 1024; // 64MB total

let total = self.inputs.iter().map(|i| i.json_buf.len()).sum::<usize>();
if total >= MAX_PIPELINE_BUFFER {
    self.metrics.inc_backpressure();
    // skip polling — flush what we have
} else {
    for input in &mut self.inputs {
        if input.json_buf.len() >= MAX_INPUT_BUFFER { continue; }
        // poll...
    }
}
```

Not polling is safe: kernel read-ahead uses page cache (no per-process
memory). inotify queue overflow is handled by the poll-based safety net
(tailer re-scans every 250ms regardless of inotify). The notify crate's
crossbeam channel is unbounded but each event is ~100 bytes.

### Async pipeline

Bounded `tokio::sync::mpsc` channels between stages. Channel capacity
controls backpressure naturally — `send().await` parks the task when
full. For byte-based limits, layer a `tokio::sync::Semaphore` where
each event acquires `allocated_bytes()` permits.

### Default limits

| Setting | Default | Rationale |
|---------|---------|-----------|
| Per-input buffer | 16 MB | ~1.6s at 10 MB/s throughput |
| Pipeline total buffer | 64 MB | 4 inputs × 16 MB |
| Batch flush threshold | 4 MB | Existing default |
| Output retry buffer | 8 batches | ~32 MB in flight |

These fit within a 256-512 MB K8s pod memory limit with headroom for
Arrow batches, transform state, and HTTP client buffers.

---

## Structured logging

### Subscriber setup

```rust
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_env("LOGFWD_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info"));

    if json {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer()
                .json()
                .flatten_event(true)
                .with_timer(fmt::time::UtcTime::rfc_3339())
                .with_target(true)
                .with_writer(std::io::stderr))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer()
                .with_ansi(atty::is(atty::Stream::Stderr))
                .with_timer(fmt::time::uptime())
                .with_target(true)
                .with_writer(std::io::stderr))
            .init();
    }
}
```

- JSON mode for K8s (machine-readable), text mode for local dev
- Switch via `--log-format json|text` CLI flag or auto-detect (non-TTY
  → JSON)
- Filter via `LOGFWD_LOG` env var (same syntax as `RUST_LOG`)

### What to convert

Library crate `eprintln!` calls → `tracing::warn!`/`tracing::error!`
with structured fields. CLI output (banners, help, progress) stays as
direct stderr writes — those are user-facing UI, not diagnostics.

### OTel bridge

Don't use `tracing-opentelemetry` or `opentelemetry-appender-tracing`
for now. Keep tracing-to-stderr and OTel-metrics-push as separate
systems. The bridge adds allocation overhead on the hot path and creates
feedback loop risk (error logs about OTel export failure trying to
export via OTel). Add it later behind a feature flag if needed.

### Feedback loop prevention

Internal logs go to stderr. The file tailer reads external files. These
are separate paths — no self-ingestion. If we later add an
`internal_logs` source (for routing internal diagnostics through the
pipeline), it must be explicitly opt-in.

---

## Health checks

### Liveness (`/healthz`)

Returns 200 if the HTTP server can respond. No business logic. If the
tokio runtime is deadlocked, the TCP accept times out and K8s restarts
the pod.

### Readiness (`/readyz`)

Latch-based startup: each initialization phase holds a latch (Arc).
When all latches drop, readiness triggers. Pattern:

```rust
pub struct Readiness(Weak<()>);
pub struct Latch(Arc<()>);

impl Readiness {
    pub fn is_ready(&self) -> bool {
        self.0.upgrade().is_none()  // ready when all latches dropped
    }
}
```

After startup, optionally check pipeline heartbeat — if no batch
processed within 60s, return 503. Be careful with idle pipelines that
legitimately have no data (only check staleness if we've ever processed
a batch).

### K8s probe config

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 9090 }
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3
readinessProbe:
  httpGet: { path: /readyz, port: 9090 }
  periodSeconds: 5
  timeoutSeconds: 2
  failureThreshold: 3
startupProbe:
  httpGet: { path: /readyz, port: 9090 }
  initialDelaySeconds: 1
  periodSeconds: 2
  failureThreshold: 30   # 60s max startup
```

---

## Chaos testing

### Approach

Docker Compose + Toxiproxy for network fault injection. No Rust-native
chaos framework exists for operational testing — use external tools.

### Test scenarios

1. **Output crash loop:** restart collector every 5s, verify retry +
   recovery
2. **Output slow drain:** add 2s latency via Toxiproxy, verify
   backpressure
3. **Output reject:** return 503 for 30s, verify retry
4. **File rotation storm:** rotate every 1s for 60s, verify no data loss
5. **Burst input:** 100MB file write, verify no OOM

### Verification

Use the existing blackhole server — it already counts lines/bytes per
request and exposes `/stats`. The test script polls `/stats` and asserts
`received >= expected` within a timeout.

### CI cadence

| Type | Cadence | Duration |
|------|---------|----------|
| Unit + integration | Every PR | 2-5 min |
| E2E (compose, no faults) | Every PR | 5-10 min |
| Chaos (output failure) | Nightly | 10-15 min |
| Long-running reliability | Weekly | 1-4 hours |

Report results via `$GITHUB_STEP_SUMMARY` for nightly runs. Create
GitHub issues automatically on failure.
