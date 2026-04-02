# Tokio Async Patterns

Reference for migrating a synchronous pipeline (std::thread + crossbeam + polling loops)
to async (tokio). Covers non-obvious behavior, not basics.

---

## 1. Runtime vs Handle

### Creating a Runtime

```rust
// Top-level only. Typically in main().
let rt = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(4)          // default: num CPUs
    .enable_all()               // enable io + time drivers
    .build()
    .unwrap();

// Block the calling (non-tokio) thread until the future completes.
rt.block_on(async { /* ... */ });
```

### Handle

A `Handle` is a cheap, cloneable reference to a running runtime. Use it to
spawn work or enter the runtime context without owning the `Runtime`.

```rust
let handle = rt.handle().clone(); // Clone freely, send across threads

// From another thread (that is NOT a tokio worker):
handle.block_on(some_future);

// Or just spawn onto it:
handle.spawn(async { /* runs on the runtime's thread pool */ });
```

### CRITICAL GOTCHA: block_on inside a worker thread panics

```rust
// This PANICS at runtime:
rt.block_on(async {
    let result = rt.block_on(another_future); // PANIC: cannot block_on from async context
});

// Also panics:
#[tokio::main]
async fn main() {
    tokio::runtime::Handle::current().block_on(fut); // PANIC
}
```

**Why:** `block_on` parks the calling thread. If that thread is a tokio worker,
you deadlock the runtime. Tokio detects this and panics.

**Fix:** If you need to call async from sync code that *might* be on a worker thread,
use `Handle::current()` + `spawn` + a oneshot channel, or restructure to stay async.

### Avoiding the OTel runtime leak pattern

A common anti-pattern creates a separate runtime for OTel and `mem::forget`s it:
```rust
let rt = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(1)
    .enable_time()
    .enable_io()
    .build()?;
let _guard = rt.enter();
// ... build OTel exporter ...
std::mem::forget(rt); // leaked so background export threads keep running
```

Better approach: share the main runtime. Pass a `Handle` to the metrics setup
instead of creating a second runtime.

---

## 2. sync::mpsc Bounded Channels

### Creation

```rust
use tokio::sync::mpsc;

// Capacity = buffer size. Sender blocks (awaits) when buffer is full.
let (tx, mut rx) = mpsc::channel::<RecordBatch>(capacity);
```

**Capacity is NOT the high-water mark.** The channel can hold `capacity` items
before `send().await` suspends. A `channel(0)` is invalid (panics) -- use
`channel(1)` for rendezvous-like behavior.

### Send patterns

```rust
// Async send -- suspends if buffer full (backpressure).
tx.send(item).await.map_err(|_| "receiver dropped")?;

// Try send -- returns immediately. Good for "drop if overloaded" semantics.
match tx.try_send(item) {
    Ok(()) => {},
    Err(TrySendError::Full(item)) => { /* channel full, item returned */ },
    Err(TrySendError::Closed(item)) => { /* receiver gone */ },
}

// Blocking send from sync code (see section 6):
tx.blocking_send(item).unwrap();
```

### Receive patterns

```rust
// Returns None when all senders dropped (clean shutdown signal).
while let Some(item) = rx.recv().await {
    process(item);
}

// Batch drain: pull everything available without waiting.
let mut batch = Vec::new();
while let Ok(item) = rx.try_recv() {
    batch.push(item);
}
```

### Capacity sizing for data pipelines

Pipeline stages: input -> scanner -> transform -> output.

- **Between input and scanner/transform:** 8-16 items. Each item might be a `Vec<u8>`
  buffer (typically 4MB). So 8 * 4MB = 32MB max buffered. This provides
  backpressure -- if transform is slow, input pauses reading.

- **Between transform and output:** 4-8 items. Output is often the bottleneck (HTTP).
  Smaller buffer here means faster drain on shutdown.

- **General rule:** `2 * num_producers` is a reasonable starting point. Larger
  buffers absorb bursts but increase memory and shutdown drain time.

### Backpressure behavior

When the channel is full, `send().await` suspends the sender's task. This is
how backpressure propagates: a slow output sink eventually pauses file reading.
This replaces the current `thread::sleep(poll_interval)` polling pattern.

---

## 3. CancellationToken (tokio-util)

```toml
# Cargo.toml
tokio-util = { version = "0.7", features = ["rt"] }  # "rt" not needed for CancellationToken alone
```

### Basic usage

```rust
use tokio_util::sync::CancellationToken;

let token = CancellationToken::new();

// Signal cancellation (non-async, can call from signal handler):
token.cancel();

// Check without waiting:
if token.is_cancelled() { /* ... */ }

// Await cancellation (use in select!):
token.cancelled().await;
```

### Child tokens for hierarchical shutdown

```rust
let root = CancellationToken::new();
let child = root.child_token();

// Cancelling root also cancels child.
// Cancelling child does NOT cancel root.
root.cancel();
assert!(child.is_cancelled()); // true
```

### Ordered shutdown pattern for pipelines

Cancel stages in reverse order so upstream drains into downstream before
downstream shuts down.

```rust
let input_token = CancellationToken::new();
let transform_token = CancellationToken::new();
let output_token = CancellationToken::new();

// On SIGTERM:
async fn graceful_shutdown(
    input_token: CancellationToken,
    transform_token: CancellationToken,
    output_token: CancellationToken,
) {
    // 1. Stop reading new data.
    input_token.cancel();

    // 2. Wait for transform channel to drain (rx returns None when all tx dropped).
    //    Or use a short timeout.
    tokio::time::sleep(Duration::from_secs(2)).await;
    transform_token.cancel();

    // 3. Wait for output channel to drain.
    tokio::time::sleep(Duration::from_secs(5)).await;
    output_token.cancel();
}
```

Better: each stage cancels the next token when it finishes draining:

```rust
// Input task:
async fn input_task(token: CancellationToken, tx: mpsc::Sender<Batch>) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            data = read_input() => { tx.send(data).await.ok(); }
        }
    }
    drop(tx); // dropping sender signals receiver that no more data is coming
}

// Transform task: exits when rx returns None (all senders dropped).
async fn transform_task(mut rx: mpsc::Receiver<Batch>, tx: mpsc::Sender<Batch>) {
    while let Some(batch) = rx.recv().await {
        let result = transform(batch);
        tx.send(result).await.ok();
    }
    drop(tx); // propagate shutdown downstream
}
```

This pattern replaces the common `AtomicBool` shutdown flag approach.

---

## 4. select! Macro

Waits on multiple futures, executes the branch of whichever completes first.

```rust
use tokio::select;

select! {
    msg = rx.recv() => {
        match msg {
            Some(data) => process(data),
            None => break, // channel closed
        }
    }
    _ = token.cancelled() => {
        // graceful shutdown
        break;
    }
    _ = tokio::time::sleep(Duration::from_millis(100)) => {
        // batch timeout: flush what we have
        flush_batch();
    }
}
```

### Cancellation safety

When one branch completes, the other futures are **dropped**. This is safe
only if dropping a future mid-`.await` does not lose data.

| Future | Cancel-safe? | Notes |
|---|---|---|
| `mpsc::Receiver::recv()` | YES | No message lost on drop |
| `mpsc::Sender::send()` | YES | Item returned in error if dropped |
| `oneshot::Receiver::recv()` | YES | |
| `tokio::time::sleep()` | YES | But timer resets next iteration -- see below |
| `TcpStream::read()` | YES | |
| `tokio::io::AsyncReadExt::read_exact()` | NO | Partial read lost |
| `tokio::io::AsyncBufReadExt::read_line()` | NO | Partial line lost |

**The sleep-in-select trap:**

```rust
// BUG: timeout resets every time a message arrives.
loop {
    select! {
        msg = rx.recv() => { buffer.push(msg); }
        _ = tokio::time::sleep(Duration::from_millis(100)) => { flush(); }
    }
}

// FIX: create the sleep outside the loop, pin it.
let mut timeout = Box::pin(tokio::time::sleep(Duration::from_millis(100)));
loop {
    select! {
        msg = rx.recv() => {
            buffer.push(msg);
        }
        _ = &mut timeout => {
            flush();
            timeout = Box::pin(tokio::time::sleep(Duration::from_millis(100)));
        }
    }
}

// Or use tokio::time::interval for periodic flushes:
let mut interval = tokio::time::interval(Duration::from_millis(100));
loop {
    select! {
        msg = rx.recv() => { buffer.push(msg); }
        _ = interval.tick() => { flush(); }
    }
}
```

### biased mode

By default, `select!` randomly picks among ready branches (fairness).
Use `biased` to check in order -- useful when you want cancellation to
always win:

```rust
select! {
    biased;
    _ = token.cancelled() => break,          // checked first
    _ = interval.tick() => flush(),           // checked second
    msg = rx.recv() => process(msg),          // checked last
}
```

Use `biased` when:
- Cancellation must preempt data processing
- You want deterministic behavior in tests

---

## 5. spawn_blocking

Runs a closure on a dedicated thread pool (separate from async workers).
Returns a `JoinHandle<T>` that you `.await`.

```rust
let result = tokio::task::spawn_blocking(move || {
    // OK: CPU-heavy work, sync file I/O, calling C libraries
    zstd::encode_all(&data[..], 3).unwrap()
}).await.unwrap(); // .await the JoinHandle; unwrap the JoinError
```

### When to use

- **YES:** Sync HTTP calls (ureq), heavy compression (zstd at high levels),
  filesystem operations that might block (open, stat, read large files),
  CPU-bound work > ~10us
- **NO:** Fast operations like `stdout.write()` on a small buffer,
  in-memory data transforms, channel sends. The overhead of scheduling onto
  the blocking pool (~1-5us) exceeds the operation cost.

### Overhead

Each `spawn_blocking` call:
1. Allocates a task
2. May spawn a new OS thread (pool grows to 512 by default)
3. Context switches to the blocking thread and back

For a pipeline's hot path (scanner + transform), keep these on the async runtime
using `tokio::task::spawn` or run inline. Only use `spawn_blocking` for
output HTTP calls during a transition period.

### Pool sizing

```rust
tokio::runtime::Builder::new_multi_thread()
    .max_blocking_threads(16)  // default 512, lower it for resource control
    .build()
```

---

## 6. Mixing Sync and Async (Incremental Migration)

### Calling async from sync code

**Pattern: create a new runtime.** Use this at the boundary (e.g., `main()`,
test functions, or a sync output method wrapping an async HTTP client).

```rust
// In a sync context that is NOT on a tokio worker thread:
let rt = tokio::runtime::Runtime::new().unwrap();
let result = rt.block_on(async {
    client.post(url).send().await
});
```

**Or use the existing runtime's handle:**

```rust
// If you have access to a Handle (stored at startup):
let result = handle.block_on(async { client.post(url).send().await });
```

### Calling sync from async code

```rust
// Wrap blocking calls with spawn_blocking:
async fn send_http(body: Vec<u8>) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || {
        ureq::post("http://...").send_bytes(&body)?;
        Ok(())
    }).await.unwrap()
}
```

### The "nested runtime" panic

```rust
#[tokio::main]
async fn main() {
    // PANIC: "Cannot start a runtime from within a runtime"
    let rt = tokio::runtime::Runtime::new().unwrap();
}
```

`Runtime::new()` panics if a tokio runtime is already active on the current thread.
`block_on()` panics if called from an async context.

**Typical migration strategy:**

1. Add `#[tokio::main]` to `main()`. All code below runs in async context.
2. Convert the main pipeline loop from blocking to `async fn run()`.
3. Replace `crossbeam-channel` with `tokio::sync::mpsc` one stage at a time.
4. For output sinks still using sync HTTP (e.g., `ureq`), wrap in `spawn_blocking`.
5. Later, replace sync HTTP with an async client (e.g., `reqwest`) and drop `spawn_blocking`.
6. Replace `AtomicBool` shutdown with `CancellationToken`.

**Transition shim** -- wrapping a sync output sink for use in async code:

```rust
// Dedicated writer thread pattern
fn spawn_output_thread<T: Send + 'static>(
    mut sink: Box<dyn Sink<T>>,
    mut rx: tokio::sync::mpsc::Receiver<T>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        while let Some(item) = rx.blocking_recv() {
            if let Err(e) = sink.send(item) {
                eprintln!("output error: {e}");
            }
        }
        sink.flush().ok();
    })
}
```

### blocking_send / blocking_recv

Tokio mpsc channels have sync counterparts for bridging:

```rust
// From a sync thread, send into an async channel:
tx.blocking_send(item).unwrap();

// From a sync thread, receive from an async channel:
let item = rx.blocking_recv(); // Returns Option<T>, None when all senders dropped
```

These must NOT be called from an async context (they block the thread).

---

## 7. Common Mistakes

### Holding MutexGuard across .await

```rust
// BUG: MutexGuard (std or tokio) held across await point.
// std::sync::Mutex: blocks a worker thread, starving the runtime.
// tokio::sync::Mutex: safe but holds the lock for the entire await duration.
let mut guard = mutex.lock().await;
do_network_call().await; // other tasks waiting on this mutex are blocked
*guard = new_value;
drop(guard);

// FIX: clone data out, drop guard, then await.
let data = {
    let guard = mutex.lock().await;
    guard.clone()
};
let result = do_network_call(data).await;
{
    let mut guard = mutex.lock().await;
    *guard = result;
}
```

**Best practice:** prefer channels over shared mutexes. Pipeline stages
should communicate via mpsc, not shared state.

### Forgetting to .await a future

```rust
// BUG: future is created but never polled. This is a no-op.
async fn send_metrics() { /* ... */ }
send_metrics(); // does nothing! No compiler error, just a warning.

// FIX:
send_metrics().await;
```

Rust emits `#[must_use]` warnings for unawaited futures. Treat warnings as errors:
```toml
# Cargo.toml or .cargo/config.toml
[lints.rust]
unused_must_use = "deny"
```

### CPU-heavy work on the async runtime

```rust
// BUG: CPU-bound work runs directly on async worker. Blocks other tasks.
async fn process_pipeline(batch: Vec<u8>) {
    let record_batch = scanner.scan(&batch);       // CPU-bound, blocks worker
    let result = transform.execute(record_batch);   // CPU-bound, blocks worker
    output.send(result).await;
}

// FIX for migration period: wrap in spawn_blocking.
async fn process_pipeline(batch: Vec<u8>) {
    let result = tokio::task::spawn_blocking(move || {
        let record_batch = scanner.scan(&batch);
        transform.execute(record_batch)
    }).await.unwrap();
    output.send(result).await;
}
```

CPU-bound pipeline stages (scanning, SQL transforms) typically take microseconds
to low milliseconds per batch. During migration, `spawn_blocking` is fine.
Long-term, consider a dedicated compute thread pool.

### Dropping a Runtime inside async

```rust
// BUG: dropping Runtime inside an async context blocks the current thread
// while it waits for all spawned tasks to complete.
async fn cleanup() {
    let rt = some_runtime; // moved in
    drop(rt); // blocks! can deadlock if tasks need the current thread
}

// FIX: shut down explicitly, or use shutdown_background() for non-blocking drop.
rt.shutdown_timeout(Duration::from_secs(5));
// Or:
rt.shutdown_background(); // tasks are aborted, does not block
```

### Unbounded channels hide backpressure bugs

```rust
// BAD: if output is slow, memory grows without limit.
let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

// GOOD: bounded channel applies backpressure.
let (tx, rx) = tokio::sync::mpsc::channel(8);
```

Always use bounded channels in data pipelines. Unbounded channels are only
appropriate for low-volume control signals (e.g., config reload notifications).
