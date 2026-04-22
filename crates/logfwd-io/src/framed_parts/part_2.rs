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
