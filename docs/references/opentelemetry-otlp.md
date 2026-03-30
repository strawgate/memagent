# OpenTelemetry Rust SDK -- Agent Reference

Crate versions: `opentelemetry 0.31`, `opentelemetry_sdk 0.31` (feature `rt-tokio`),
`opentelemetry-otlp 0.31` (features `http-proto`, `reqwest-client`).

Covers two common OTel usage patterns:
1. **Internal metrics export** -- push pipeline counters to an OTLP endpoint via the SDK.
2. **OTLP log encoding** -- hand-encode `ExportLogsServiceRequest` protobuf for log output (bypassing the SDK log exporter for performance).

---

## 1. MeterProvider + PeriodicReader

### With endpoint configured

```rust
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{SdkMeterProvider, PeriodicReader};

// PeriodicReader needs a tokio runtime for async HTTP export.
let rt = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(1)
    .enable_time()
    .enable_io()
    .build()?;
let _guard = rt.enter();  // must be in scope when building exporter + reader

let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
    .with_http()
    .with_endpoint(endpoint)  // e.g. "http://localhost:4318"
    .build()?;

let reader = PeriodicReader::builder(otlp_exporter)
    .with_interval(Duration::from_secs(60))
    .build();

let provider = SdkMeterProvider::builder()
    .with_reader(reader)
    .build();

// IMPORTANT: the tokio runtime must outlive the provider.
// One approach is to leak it (not recommended -- see tokio-async-patterns.md):
std::mem::forget(rt);

let meter = provider.meter("my_service");
```

### No-op provider (no endpoint / validation mode)

```rust
let provider = SdkMeterProvider::builder().build();
let meter = provider.meter("my_service");
// Counters created from this meter are functional but never exported.
// No tokio runtime needed.
```

### Key runtime requirement

`PeriodicReader` spawns a tokio task. You must have an active tokio runtime context
(`rt.enter()`) when calling `.build()` on the exporter and the reader. The runtime
must stay alive for the lifetime of the provider. A common pattern for long-running
services is to share the main runtime rather than creating a dedicated one.

---

## 2. OTLP protobuf format (logs)

Hand-encoding protobuf rather than using the SDK's log exporter avoids allocation
and intermediate Rust structs on the hot path.

### Message nesting

```
ExportLogsServiceRequest          (top-level, POST body)
  field 1: ResourceLogs           (repeated, length-delimited)
    field 1: Resource             (length-delimited)
      field 1: KeyValue           (repeated -- resource attributes)
    field 2: ScopeLogs            (repeated, length-delimited)
      field 2: LogRecord          (repeated, length-delimited)
```

### LogRecord field numbers

From `opentelemetry/proto/logs/v1/logs.proto`:

| Field # | Wire type | Name | Notes |
|---------|-----------|------|-------|
| 1 | 1 (fixed64) | `time_unix_nano` | Event timestamp |
| 2 | 0 (varint) | `severity_number` | Enum: TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17, FATAL=21 |
| 3 | 2 (bytes) | `severity_text` | e.g. "INFO" |
| 5 | 2 (bytes) | `body` | AnyValue message |
| 6 | 2 (bytes) | `attributes` | Repeated KeyValue |
| 11 | 1 (fixed64) | `observed_time_unix_nano` | Collector receive time |

### AnyValue encoding

`body` (field 5) wraps an `AnyValue` message. For string bodies:
```
AnyValue.string_value = field 1, wire type 2 (length-delimited)
AnyValue.int_value    = field 3, wire type 0 (varint)
AnyValue.double_value = field 4, wire type 1 (fixed64)
```

### KeyValue encoding (attributes)

Each attribute is a `KeyValue` message written as LogRecord field 6:
```
KeyValue {
  field 1: key    (string, length-delimited)
  field 2: value  (AnyValue message, length-delimited)
}
```

### Wire format primitives

```rust
// tag = (field_number << 3) | wire_type
// wire_type: 0=varint, 1=64-bit fixed, 2=length-delimited
encode_tag(buf, field_number, wire_type);
encode_varint(buf, value);              // LEB128
encode_fixed64(buf, field_number, val); // tag + 8 bytes LE
encode_bytes_field(buf, field_number, data); // tag + varint len + bytes
encode_varint_field(buf, field_number, val); // tag + varint

// Size calculation (no write):
varint_len(value) -> usize
bytes_field_size(field_number, data_len) -> usize
```

### Encoding strategy (two-phase)

1. **Phase 1**: Encode all `LogRecord` payloads into a flat `records_buf`, tracking `(start, end)` ranges.
2. **Phase 2**: Compute sizes bottom-up (ScopeLogs inner size, ResourceLogs inner size, request size).
3. **Phase 3**: Write final protobuf: `ExportLogsServiceRequest > ResourceLogs > ScopeLogs > [LogRecord...]`.

This avoids back-patching length prefixes. A typical implementation reuses buffers across calls via a batch encoder struct.

---

## 3. Metric types and the dual-write pattern

### OTel metric types

The most commonly used type in pipelines is `Counter<u64>`:

```rust
use opentelemetry::metrics::{Counter, Meter};

let lines: Counter<u64> = meter.u64_counter("input_lines").build();
lines.add(n, &[KeyValue::new("pipeline", "main")]);
```

Other available types:
- `Histogram<f64>` -- `meter.f64_histogram("name").build()` / `.record(value, &attrs)`
- `Gauge<f64>` -- `meter.f64_gauge("name").build()` / `.record(value, &attrs)`
- `UpDownCounter<i64>` -- for values that go up and down

### Dual-write pattern

A common pattern writes each metric to **both** an `AtomicU64` and an OTel `Counter` on the hot path:

```rust
pub struct ComponentStats {
    pub lines_total: AtomicU64,     // for local /metrics endpoint + diagnostics
    pub bytes_total: AtomicU64,
    pub errors_total: AtomicU64,
    otel_lines: Counter<u64>,       // for OTLP push export
    otel_bytes: Counter<u64>,
    otel_errors: Counter<u64>,
    otel_attrs: Vec<KeyValue>,      // pre-allocated, reused every call
}

pub fn inc_lines(&self, n: u64) {
    self.lines_total.fetch_add(n, Ordering::Relaxed);
    self.otel_lines.add(n, &self.otel_attrs);  // lock-free internally
}
```

**Why dual-write**: The atomics serve the local diagnostics HTTP server (Prometheus-style
scrape at `/metrics`). The OTel counters feed the PeriodicReader for remote OTLP push.
Both paths are lock-free.

**Hot-path performance**: `Counter::add()` in the OTel SDK is lock-free (uses internal
atomics). Pre-allocating `otel_attrs` as a `Vec<KeyValue>` avoids per-call allocation.
When no metrics endpoint is configured, the no-op provider makes `Counter::add()` a no-op
(just a trait method that returns immediately).

---

## 4. OTLP HTTP vs gRPC

### Endpoint URL conventions

| Signal | HTTP path | gRPC service |
|--------|-----------|-------------|
| Logs | `POST /v1/logs` | `opentelemetry.proto.collector.logs.v1.LogsService/Export` |
| Metrics | `POST /v1/metrics` | `opentelemetry.proto.collector.metrics.v1.MetricsService/Export` |
| Traces | `POST /v1/traces` | `opentelemetry.proto.collector.trace.v1.TraceService/Export` |

Default base: `http://localhost:4318` (HTTP), `http://localhost:4317` (gRPC).

The SDK's `MetricExporter::builder().with_http().with_endpoint(url)` appends `/v1/metrics`
automatically. For log output, you typically construct the full URL yourself.

### Content-Type headers

```
HTTP protobuf:  "application/x-protobuf"
HTTP JSON:      "application/json"
gRPC:           "application/grpc"
```

### Compression

OTLP supports compression on log output:
```rust
req = req.header("Content-Encoding", "zstd");
```

Standard OTLP compression options: `gzip`, `zstd`. Set via `Content-Encoding` header.

The SDK metric exporter handles its own compression config separately (via builder methods).

---

## 5. Shutdown

```rust
// After all pipelines finish:
if let Err(e) = meter_provider.shutdown() {
    eprintln!("warning: meter provider shutdown: {e}");
}
```

`SdkMeterProvider::shutdown()`:
- Triggers a final metric collection + export (flushes pending data).
- Must be called before process exit or the last interval's metrics are lost.
- After shutdown, all meters from this provider become no-ops.
- Returns `Err` if export fails or times out.
- Blocks the calling thread until export completes (or times out).

There is no equivalent shutdown needed for hand-encoded log output if it sends
synchronously on each batch call.
