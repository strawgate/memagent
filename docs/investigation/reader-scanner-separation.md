# Reader/Scanner Separation Design

## The Insight

Three distinct responsibilities, not two:
1. **Reader** — I/O + format parsing. Produces lines or pre-structured batches.
2. **Scanner** — JSON lines → Arrow RecordBatch. Pipeline-owned, swappable.
3. **Pipeline** — owns the scanner, accumulates lines, decides when to flush.

## Traits

```rust
pub enum ReadOutput {
    /// NDJSON lines ready for scanning. No partial lines.
    Lines { data: Vec<u8>, line_count: usize },
    /// Already-structured data. Bypasses scanner.
    Batch { batch: RecordBatch, metadata: BatchMetadata },
    /// Lifecycle events.
    Event(ReaderEvent),
}

pub trait Reader: Send {
    fn poll(&mut self) -> io::Result<Option<ReadOutput>>;
    fn ack(&mut self, checkpoint: &ReaderCheckpoint);
    fn id(&self) -> &ReaderId;
    fn checkpoint(&self) -> ReaderCheckpoint;
    fn shutdown(&mut self);
}

pub trait Scan: Send {
    fn scan(&mut self, buf: &[u8]) -> RecordBatch;
    fn update_config(&mut self, config: ScanConfig);
}
```

## How All 5 Input Paths Work

| Path | Reader impl | ReadOutput | Scanner used? |
|------|------------|-----------|---------------|
| File + JSON | TextReader(FileInput, JsonParser) | Lines | Yes |
| File + CRI | TextReader(FileInput, CriParser) | Lines | Yes |
| File + Raw | TextReader(FileInput, RawParser) | Lines | Yes |
| OTLP receiver | OtlpReader | Batch | No |
| Arrow Flight | ArrowFlightReader | Batch | No |

## Pipeline Loop

```rust
pub struct Pipeline<S: Scan> {
    readers: Vec<ReaderState>,
    scanner: S,           // pipeline-owned, swappable at construction
    transform: SqlTransform,
    output: Box<dyn OutputSink>,
}

// Loop:
// 1. Poll all readers
// 2. Lines → accumulate in per-reader line_buf
// 3. Batch → queue directly for transform
// 4. When line_buf reaches 4MB or 100ms: scanner.scan(&combined) → queue
// 5. Process queued batches: transform → output
```

## Key Design Decisions

- **Reader is sync** (`poll()` not async). File reading is sync. Network readers use internal channels (gRPC server on its own runtime, poll does try_recv). Async migration happens later at the pipeline level.
- **Line accumulation lives in the pipeline**, not the reader. Reader produces small chunks; pipeline decides when to flush. Keeps reader simple and testable.
- **Scanner is swappable at pipeline level.** `Pipeline<SimdScanner>` vs `Pipeline<StreamingSimdScanner>`. All readers unaffected.
- **Format parsing stays with the Reader.** CRI reassembly, raw-to-JSON wrapping are internal to TextReader via FormatParser.
- **Checkpointing belongs to the Reader.** Each reader tracks its own position.
- **Mixed inputs work naturally.** A pipeline with File + OTLP readers accumulates file lines for scanning AND receives OTLP batches directly. Both go through the same transform + output.

## Migration (incremental)

1. Define `Reader` trait + `ReadOutput` enum (new code, nothing changes)
2. Implement `TextReader` composing existing `InputSource` + `FormatParser` (extract from pipeline.rs)
3. Add `Scan` trait, make `SimdScanner` implement it (trivial)
4. Refactor `Pipeline` to use `Vec<Box<dyn Reader>>` + generic `S: Scan`
5. Later: add `OtlpReader`, `ArrowFlightReader` — no changes to Scanner/Pipeline

## Replaces

This design replaces the `Source` trait from the architecture spec (which merged too many concerns). The phase issues (#86, #95, #96, #97) should be updated to use Reader + Scan instead of Source.
