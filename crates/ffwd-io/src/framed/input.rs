/// Wraps a raw [`InputSource`] with newline framing and format processing.
///
/// The inner source provides raw bytes (from file, TCP, UDP, etc.). This
/// wrapper splits on newlines, manages partial-line remainders across polls,
/// and runs format-specific processing (CRI extraction, passthrough, etc.).
/// The output is scanner-ready bytes.
///
/// All per-source state (remainder, format, checkpoint tracker) is keyed by
/// `Option<SourceId>` so that interleaved data from multiple sources never
/// mixes partial lines or CRI aggregation state. Sources without identity
/// (`None`) share a single state entry.
pub struct FramedInput {
    inner: Box<dyn InputSource>,
    /// Template format processor — cloned per-source on first data arrival.
    format_template: FormatDecoder,
    /// Per-source state: remainder, format processor, checkpoint tracker.
    sources: HashMap<Option<SourceId>, SourceState>,
    out_buf: Vec<u8>,
    cri_metadata_buf: CriMetadata,
    /// Spare buffer swapped in when out_buf is emitted, preserving capacity
    /// across polls without allocating.
    spare_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
    last_raw_had_payload: bool,
}
