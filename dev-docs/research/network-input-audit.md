# Network Input Audit (TCP + UDP)

Date: 2026-04-03
Context: End-to-end audit of logfwd's TCP and UDP input paths,
following the same format as the file tailing audit.

## Data Flow Diagram

```text
                   TCP Path                              UDP Path
                   ────────                              ────────

  ┌──────────┐   accept()    ┌──────────────┐     ┌──────────┐  recv()   ┌──────────────┐
  │ Clients  │──────────────►│  TcpInput    │     │ Senders  │─────────►│  UdpInput    │
  │ (N conns)│               │              │     │          │          │              │
  └──────────┘               │ ① Per-conn   │     └──────────┘          │ ③ Per-dgram  │
                             │   read loop  │                           │   recv loop  │
                             │              │                           │              │
                             │ ② ALL bytes  │                           │ ④ Appends \n │
                             │   merged into│                           │   if missing  │
                             │   one Vec    │                           │              │
                             └──────┬───────┘                           └──────┬───────┘
                                    │                                          │
                             InputEvent::Data                          InputEvent::Data
                             (all clients mixed)                       (all dgrams, \n-terminated)
                                    │                                          │
                                    └──────────────┬───────────────────────────┘
                                                   │
                                    ┌──────────────▼───────────────┐
                                    │         FramedInput          │
                                    │                              │
                                    │  ⑤ Single remainder buffer   │
                                    │     (shared across ALL       │
                                    │      connections/dgrams)     │
                                    │                              │
                                    │  ⑥ Split on \n               │
                                    │     + format processing      │
                                    └──────────────┬───────────────┘
                                                   │
                                            InputEvent::Data
                                            (complete lines)
                                                   │
                                    ┌──────────────▼───────────────┐
                                    │    input_poll_loop           │
                                    │    (same as file path)       │
                                    │                              │
                                    │  Accumulate → batch → channel│
                                    └──────────────────────────────┘
```

---

## ① TCP Connection Management

**Current**: Non-blocking `TcpListener`, accept loop drains kernel queue.
Max 1024 concurrent clients. Keepalive (60s time, 10s interval).
Idle timeout 60s. Max line length 1 MiB (disconnects client on exceed).

**No issues with the connection management itself.** The accept/read/evict
cycle is correct. The max-clients overflow handling (accept then immediately
close) prevents kernel queue backup.

---

## ② TCP Data Merging (CRITICAL)

**Current**: All client data from all connections is merged into a single
`Vec<u8>` per `poll()` call (`tcp_input.rs:131`). No per-connection
identity is preserved.

**Problem**: Combined with the shared FramedInput remainder (⑤), this
causes cross-connection data corruption. If client A sends `"hello wor"`
and client B sends `"ld\n"`, the framing layer produces `"hello world\n"` —
a corrupted line combining two clients.

For TCP this is WORSE than the file input version of the same bug because:
- Multiple concurrent TCP senders are the normal case (not edge case)
- TCP segments are often partial (Nagle, network fragmentation)
- Every sender produces partial data every poll cycle

### Options

**A. Per-connection InputEvent (emit separate Data per client)**
- Each client's bytes become a separate `InputEvent::Data`
- FramedInput processes them in order, but remainder is still shared
- Pro: Partial improvement — partial lines from one client don't cross
  to data within the same poll from another client
- Con: Doesn't fully fix it — remainder from poll N still merges with
  data from poll N+1's different client

**B. Per-connection framing (best)**
- `TcpInput` maintains per-connection line buffers
- Only emits complete lines (newline-terminated)
- Partial data stays per-connection until complete or disconnect
- Pro: Fully correct, matches every production collector
- Con: Larger refactor — TcpInput becomes a framing layer itself

**C. Per-connection FramedInput**
- Each connection gets its own FramedInput wrapper
- Pro: Reuses existing framing logic
- Con: Architecture doesn't support this — FramedInput wraps InputSource,
  not individual connections

**Recommendation**: B — per-connection line buffering inside `TcpInput`.
The `Client` struct already exists per-connection. Add a `remainder: Vec<u8>`
field. Only emit data when a complete line is found. On disconnect, flush
remainder as final line (or discard, documenting the choice).

---

## ③ UDP Datagram Handling

**Current**: Non-blocking `UdpSocket`, 8 MiB kernel receive buffer.
Recv loop drains all available datagrams per poll. Drop detection via
`ENOBUFS` counter.

**No major issues.** UDP datagrams are self-contained messages. The design
is correct for the protocol.

### Minor gaps

**No source attribution**: Uses `recv()` not `recv_from()` for performance.
Source IP/port is lost. For syslog use cases, knowing the sender is valuable.

**No rate limiting**: A flood of datagrams could overwhelm the pipeline.
The 8 MiB kernel buffer is the only cushion.

---

## ④ UDP Newline Appending

**Current**: If a datagram doesn't end with `\n`, one is appended
(`udp_input.rs:114-116`).

**This is correct.** Each datagram becomes a self-contained line.
The FramedInput remainder should always be empty for UDP inputs.

---

## ⑤ Shared Remainder Buffer (CRITICAL for TCP)

**Current**: Single `FramedInput` remainder buffer shared across all
TCP connections (same issue as file input #797).

**This is the root cause of the cross-connection corruption.** Even if
TcpInput emitted per-connection events (option A above), the shared
remainder would still mix data across connections between poll cycles.

### Options

Same as file tailing audit ⑤, but with the additional option:

**D. Move framing into TcpInput (per-connection)**
- TCP connections are inherently ordered byte streams — framing belongs
  at the connection level, not in a shared downstream layer
- `Client` struct gets a `line_buf: Vec<u8>`
- `TcpInput::poll()` emits only complete lines
- `FramedInput` becomes a pass-through for TCP (no remainder needed)
- Pro: Architecturally correct, each connection is independent
- Con: TcpInput does its own `memchr` scanning (duplicates FramedInput logic)

**Recommendation**: D for TCP specifically. TCP connections are stateful
byte streams — framing MUST be per-connection. This is fundamentally
different from file input where one file = one stream.

---

## ⑥ Format Processing

**Current**: TCP and UDP reject CRI/Auto formats (only JSON allowed).

**Correct.** CRI format is container-specific and doesn't apply to
network inputs.

---

## Missing: TCP EndOfFile on Disconnect

**Current**: When a TCP client disconnects (`read() → Ok(0)`), the client
is removed but no `EndOfFile` event is emitted. Any partial data in the
FramedInput remainder is stranded.

**With per-connection framing (option D above)**, this is solved — the
disconnect handler flushes the per-connection line buffer.

---

## Missing: TLS Support

**Current**: Neither TCP nor UDP supports TLS/DTLS.

### Options

**A. Add TLS via rustls**
- Pro: No OpenSSL dependency, pure Rust
- Con: Adds dependency, increases complexity
- Industry: All production collectors support TLS

**B. Recommend stunnel/haproxy in front**
- Pro: Zero code change
- Con: Extra operational component, latency

**Recommendation**: A eventually, B as documented workaround for now.

---

## Missing: Source Attribution

**Current**: No source IP/port tracking for TCP or UDP.

### Options

**A. Add source_addr to InputEvent::Data**
- `InputEvent::Data { bytes, source: Option<SocketAddr> }`
- Pro: Enables filtering/routing by source, audit logging
- Con: Changes InputEvent type, propagates through pipeline

**B. Add _source_addr as injected column**
- Scanner injects `_source_addr_str` column like `_file_str`
- Pro: Available in SQL transforms
- Con: Per-row overhead for a rarely-used field

**Recommendation**: A — carry it in the event. Let the pipeline decide
whether to inject it as a column or use it for routing.

---

## Summary: Priority Order

| Priority | Issue | Protocol | Fix | Effort |
|----------|-------|----------|-----|--------|
| 1 | Cross-connection corruption | TCP | Per-connection line buffering in TcpInput | Medium |
| 2 | No EndOfFile on disconnect | TCP | Flush per-connection buffer on disconnect | Small (part of #1) |
| 3 | No TLS | TCP/UDP | rustls integration | Large |
| 4 | No source attribution | TCP/UDP | Add source to InputEvent | Small |
| 5 | No rate limiting | UDP | Configurable recv budget per poll | Small |

**Note**: Issue #1 (cross-connection corruption) is the same class of bug
as the file input shared remainder (#797). The fix is different though —
for files, per-file remainder in FramedInput works. For TCP, framing must
move into TcpInput itself because connections are independent byte streams.
