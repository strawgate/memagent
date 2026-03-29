# notify / memchr / zstd -- Agent Reference

Versions: `notify 7`, `memchr 2`, `zstd 0.13`

---

## notify (file watching)

### Basic setup

```rust
// Create watcher with crossbeam channel (not std::sync::mpsc)
let (tx, rx) = crossbeam_channel::unbounded();
let mut watcher = notify::recommended_watcher(move |res| {
    let _ = tx.send(res);
}).map_err(io::Error::other)?;

// Watch DIRECTORIES, not files -- catches create/rename/delete
use notify::Watcher;
watcher.watch(parent_dir, notify::RecursiveMode::NonRecursive)
    .map_err(io::Error::other)?;
```

The watcher must be stored to keep it alive (dropping it stops all watches).
Events are drained via `try_recv()` -- a typical file tailer does NOT block on them.

### Best practice: notify as hint + polling as backup

Treat filesystem events as a **latency hint**, not a source of truth:

```rust
// Drain events -- just sets a flag, doesn't read data yet
let mut something_changed = false;
while let Ok(res) = self.fs_events.try_recv() {
    if let Ok(_event) = res { something_changed = true; }
}

// Poll if event fired OR timer expired (safety net)
let should_poll = something_changed || self.last_poll.elapsed() >= poll_interval;
```

A typical poll interval is 250ms. On each poll:
1. Check for new/rotated files (compare identity = dev + inode + content fingerprint)
2. `read()` all tailed files until EOF

This hybrid approach handles:
- inotify queue overflow (events silently dropped when kernel buffer full)
- NFS/overlayfs (no inotify events at all)
- macOS FSEvents coalescing (batched, delayed events)

### inotify (Linux) vs kqueue (macOS) differences

| | inotify | kqueue/FSEvents |
|---|---|---|
| Granularity | Per-file events with specific masks | Per-vnode (kqueue) or directory-level (FSEvents) |
| Rename tracking | IN_MOVED_FROM/IN_MOVED_TO with cookie | No cookie -- just "something changed" |
| Watch limit | `fs.inotify.max_user_watches` (default 8192) | Per-process open file limit |
| Network FS | No events on NFS | No events on network mounts |
| Event ordering | Events ordered within single fd | FSEvents may coalesce/reorder |

`notify::RecommendedWatcher` picks:
- Linux: `INotifyWatcher` (inotify + mio)
- macOS: `FsEventWatcher` (FSEvents) by default, `KqueueWatcher` with `macos_kqueue` feature

### Gotchas

**Watch handle must stay alive.** Dropping the `RecommendedWatcher` stops all watches.
A common pattern is to store it as `_watcher` (prefixed underscore = intentionally unused except for lifetime).

**Event ordering not guaranteed.** FSEvents batches events. inotify can overflow.
Never rely on seeing Create before Modify, or Rename before Create.

**Rename detection is platform-dependent.** inotify gives paired MOVED_FROM/MOVED_TO
with a cookie. kqueue/FSEvents just says "something changed in the directory."
A robust approach is to track `(device, inode, fingerprint)` and re-identify
files on each poll -- ignoring rename events entirely.

**Editor atomic saves.** Many editors save via write-to-temp + rename. Watching the
file directly misses this. Watch the parent directory instead.

**`/proc`, `/sys`, NFS** -- no events. PollWatcher is the only option. A
built-in periodic poll achieves the same thing.

---

## memchr (SIMD string search)

### Core API

```rust
use memchr::{memchr, memchr2, memchr3};
use memchr::{memchr_iter, memchr2_iter, memchr3_iter};

// Single byte -- find first '\n'
let pos: Option<usize> = memchr(b'\n', haystack);

// Two bytes -- find first ' ' or '\t'
let pos: Option<usize> = memchr2(b' ', b'\t', haystack);

// Three bytes -- find first of three delimiters
let pos: Option<usize> = memchr3(b'"', b'\\', b'\n', haystack);

// Iterator -- all newline positions
for pos in memchr_iter(b'\n', haystack) { /* ... */ }
```

### memmem -- substring search

```rust
use memchr::memmem;

// One-shot substring search
let pos: Option<usize> = memmem::find(haystack, b"\"body\":");

// Iterator over all matches
let count = memmem::find_iter(haystack, b"\"body\":").count();

// Pre-built finder (amortize setup cost across many haystacks)
let finder = memmem::Finder::new(b"\"body\":");
let pos = finder.find(haystack);
```

### Why it's faster than manual search

`memchr` uses SIMD instructions to scan 16-64 bytes per cycle:
- x86_64: SSE2 (16B), AVX2 (32B) -- auto-detected at runtime
- aarch64: NEON (16B)
- Fallback: still uses libc `memchr` which is optimized per-platform

For `memmem`, it picks the best algorithm based on needle length:
- 1 byte: delegates to `memchr`
- 2-3 bytes: SIMD-accelerated prefilter + verification
- Longer: Two-Way or Rabin-Karp with SIMD prefilter

A manual `for b in haystack { if *b == needle ... }` loop processes 1 byte/cycle.
`memchr` processes 32 bytes/cycle on AVX2. That's the difference between 1M lines/sec
and 32M lines/sec for newline counting.

### Common usage patterns

**Newline counting:**
```rust
let newlines = memchr::memchr_iter(b'\n', body).count() as u64;
```

**Substring counting (e.g., OTLP body keys):**
```rust
// Count `"body":` occurrences = number of log records in a JSON payload
fn count_body_keys(body: &[u8]) -> u64 {
    memchr::memmem::find_iter(body, b"\"body\":").count() as u64
}
```

**Space-delimited log parsing (e.g., CRI format):**
```rust
// CRI format: <timestamp> <stream> <flags> <log>
let sp1 = memchr::memchr(b' ', line)?;           // first space
let sp2 = memchr::memchr(b' ', &line[sp1+1..])? + sp1 + 1;  // second space
```

**JSON field extraction:**
```rust
// Walk JSON finding quoted keys via memchr for '"' then ':'
let Some(q1) = memchr(b'"', &line[pos..]) else { break };
let Some(q2) = memchr(b'"', &line[key_start..]) else { break };
let Some(colon) = memchr(b':', &line[pos..]) else { break };
```

**Line splitting:**
```rust
// Split raw byte chunk into lines
let eol = match memchr(b'\n', &buf[pos..]) { ... };
```

---

## zstd (compression)

### API surface

**One-shot (bulk)** -- compress/decompress a complete buffer:
```rust
// Compress -- level 0 = default (3), level 1 = fastest
let compressed: Vec<u8> = zstd::bulk::compress(data, 1)?;

// Decompress -- must provide max output size
let raw: Vec<u8> = zstd::bulk::decompress(&compressed, max_size)?;

// Compress into pre-allocated buffer (avoids alloc)
let n = zstd::bulk::compress_to_buffer(data, &mut dest_buf, 1)?;
```

**Streaming** -- wrap a `Read` or `Write`:
```rust
// Compress: Encoder wraps a Write
let mut enc = zstd::Encoder::new(output_file, 1)?;
enc.write_all(data)?;
enc.finish()?;  // MUST call finish() to flush frame

// Decompress: Decoder wraps a Read
let mut dec = zstd::Decoder::new(input_file)?;
let mut buf = Vec::new();
dec.read_to_end(&mut buf)?;
```

**Reusable compressor** (avoids context allocation per call):
```rust
// bulk::Compressor reuses the ZSTD_CCtx (~128KB) across calls
let mut comp = zstd::bulk::Compressor::new(1)?;
let c1 = comp.compress(data1)?;
let c2 = comp.compress(data2)?;  // reuses context
```

### Compression levels

| Level | Speed (MB/s) | Ratio | Use case |
|-------|-------------|-------|----------|
| 1 | ~500 | ~2.5x | Real-time pipelines -- speed over ratio |
| 3 | ~350 | ~3.0x | zstd default |
| 9 | ~100 | ~3.5x | archival |
| 19 | ~10 | ~4.0x | maximum compression |

Level 1 is the right choice for log pipelines: logs are highly repetitive so even
level 1 compresses well, and the CPU cost is negligible vs. network savings.

### Dictionary training

Dictionaries improve compression for small payloads (<1KB) by pre-seeding the
compressor with common patterns. Requires `zdict_builder` feature.

```rust
// Train from samples
let dict_data = zstd::dict::from_samples(&[sample1, sample2, ...], 110_000)?;

// Train from contiguous buffer + size array
let dict_data = zstd::dict::from_continuous(&all_data, &sizes, 110_000)?;

// Train from files
let dict_data = zstd::dict::from_files(&["a.log", "b.log"], 110_000)?;

// Use with streaming encoder
let mut enc = zstd::Encoder::with_dictionary(writer, 1, &dict_data)?;

// Pre-compiled dictionary (avoids re-parsing dict on each use)
let cdict = zstd::dict::EncoderDictionary::copy(&dict_data, 1);
// Use via zstd_safe APIs
```

Dictionaries are rarely needed for log forwarding -- OTLP protobuf batches are
typically large enough (10-100KB) that level 1 without a dictionary works well.

### Common usage patterns

**Chunk compressor with reusable buffer:**

```rust
pub struct ChunkCompressor {
    level: i32,          // level 1 is typical for log forwarding
    out_buf: Vec<u8>,    // reused across calls
}

impl ChunkCompressor {
    pub fn compress(&mut self, raw: &[u8]) -> io::Result<Vec<u8>> {
        let compressed = zstd::bulk::compress(raw, self.level)?;
        Ok(compressed)
    }
}
```

**Compression in an HTTP output sink:**

```rust
let compressor = match compression {
    Compression::Zstd => Some(ChunkCompressor::new(1)),
    _ => None,
};

// On send: compress protobuf, set Content-Encoding: zstd
let compressed = compressor.compress(&payload)?;
req = req.header("Content-Encoding", "zstd");
req.send(&compressed)?;
```

**Performance note:** `zstd::bulk::compress` allocates a new `Vec<u8>` each call.
Switching to `bulk::Compressor` (reusable context) + `compress_to_buffer`
(pre-allocated dest) eliminates both allocations per batch.
