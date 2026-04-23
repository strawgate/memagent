impl FramedInput {
    /// Wrap `inner` with newline framing and per-source format state.
    ///
    /// The wrapper takes ownership of `inner`, preserves partial-line
    /// remainders across polls, isolates decoder state per `SourceId`, and
    /// records emitted bytes and lines into `stats`.
    pub fn new(
        inner: Box<dyn InputSource>,
        format: FormatDecoder,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let cri_metadata_buf = if format.emits_cri_metadata() {
            CriMetadata::with_capacity(INITIAL_CRI_METADATA_SPANS, INITIAL_CRI_TIMESTAMP_BYTES)
        } else {
            CriMetadata::default()
        };
        Self {
            inner,
            format_template: format,
            sources: HashMap::new(),
            out_buf: Vec::with_capacity(64 * 1024),
            cri_metadata_buf,
            spare_buf: Vec::with_capacity(64 * 1024),
            stats,
            last_raw_had_payload: false,
        }
    }

    fn source_state_mut(&mut self, key: Option<SourceId>) -> &mut SourceState {
        self.sources.get_mut(&key).unwrap_or_else(|| {
            panic!("source state missing for key {key:?}; framed input invariant violated")
        })
    }

    fn cri_metadata_for_emitted_data(&mut self) -> Option<CriMetadata> {
        if self.cri_metadata_buf.is_empty() {
            return None;
        }
        let replacement = self.cri_metadata_buf.empty_with_preserved_capacity();
        let metadata = std::mem::replace(&mut self.cri_metadata_buf, replacement);
        Some(metadata)
    }

    fn process_raw_events(&mut self, raw_events: Vec<InputEvent>) -> Vec<InputEvent> {
        self.last_raw_had_payload = raw_events
            .iter()
            .any(|event| matches!(event, InputEvent::Data { .. } | InputEvent::Batch { .. }));
        if raw_events.is_empty() {
            return vec![];
        }

        let mut result_events: Vec<InputEvent> = Vec::new();

        for event in raw_events {
            match event {
                InputEvent::Data {
                    bytes,
                    source_id,
                    accounted_bytes,
                    ..
                } => {
                    self.stats.inc_bytes(accounted_bytes);

                    let key = source_id;
                    let n_bytes = bytes.len() as u64;

                    // Get or create per-source state.
                    let state = {
                        let template = &self.format_template;
                        self.sources.entry(key).or_insert_with(|| SourceState {
                            remainder: Vec::new(),
                            format: template.new_instance(),
                            tracker: CheckpointTracker::new(0),
                            overflow_tainted: false,
                        })
                    };
                    let tainted_on_entry = state.overflow_tainted;

                    // --- ZERO-COPY FAST PATH ---
                    // For passthrough formats with no remainder and no tainted
                    // state, forward the Bytes directly without copying.
                    if state.format.is_passthrough()
                        && state.remainder.is_empty()
                        && !tainted_on_entry
                    {
                        let last_nl = memchr::memrchr(b'\n', &bytes);
                        match last_nl {
                            Some(pos) if pos + 1 == bytes.len() => {
                                // Entire chunk is complete lines — zero-copy pass.
                                state.tracker.apply_read(n_bytes, Some(pos as u64));
                                let line_count = memchr::memchr_iter(b'\n', &bytes).count();
                                self.stats.inc_lines(line_count as u64);
                                if let FormatDecoder::PassthroughJson { stats, .. } = &state.format
                                {
                                    crate::format::count_json_parse_errors(&bytes, stats);
                                }
                                result_events.push(InputEvent::Data {
                                    bytes,
                                    source_id,
                                    accounted_bytes: 0,
                                    cri_metadata: None,
                                });
                                if key.is_some()
                                    && self.inner.should_reclaim_completed_source_state()
                                    && self
                                        .sources
                                        .get(&key)
                                        .is_some_and(SourceState::is_reclaimable)
                                {
                                    self.sources.remove(&key);
                                }
                                continue;
                            }
                            Some(pos) => {
                                // Complete lines + remainder tail.
                                let complete = bytes.slice(0..=pos);
                                state.tracker.apply_read(n_bytes, Some(pos as u64));
                                let tail = &bytes[pos + 1..];
                                if tail.len() > MAX_REMAINDER_BYTES {
                                    tracing::warn!(
                                        source_key = ?key,
                                        tail_bytes = tail.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — tail after newline \
                                         exceeds MAX_REMAINDER_BYTES; keeping last \
                                         MAX_REMAINDER_BYTES bytes"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let start = tail.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = tail[start..].to_vec();
                                    state.format.reset();
                                    state.overflow_tainted = true;
                                } else {
                                    state.remainder = tail.to_vec();
                                }
                                let line_count = memchr::memchr_iter(b'\n', &complete).count();
                                self.stats.inc_lines(line_count as u64);
                                if let FormatDecoder::PassthroughJson { stats, .. } = &state.format
                                {
                                    crate::format::count_json_parse_errors(&complete, stats);
                                }
                                result_events.push(InputEvent::Data {
                                    bytes: complete,
                                    source_id,
                                    accounted_bytes: 0,
                                    cri_metadata: None,
                                });
                                continue;
                            }
                            None => {
                                // No newline — entire chunk is remainder.
                                state.tracker.apply_read(n_bytes, None);
                                if bytes.len() > MAX_REMAINDER_BYTES {
                                    tracing::warn!(
                                        source_key = ?key,
                                        chunk_bytes = bytes.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — partial line exceeds \
                                         MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                         bytes and resetting format state"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let start = bytes.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = bytes[start..].to_vec();
                                    state.format.reset();
                                    state.overflow_tainted = true;
                                } else {
                                    state.remainder = bytes.to_vec();
                                }
                                continue;
                            }
                        }
                    }
                    // --- END ZERO-COPY FAST PATH ---

                    // General path: prepend remainder from last poll,
                    // reusing the Vec's capacity.
                    let mut chunk = std::mem::take(&mut state.remainder);
                    chunk.extend_from_slice(&bytes);

                    // Find last newline — everything before is complete lines,
                    // everything after is the new remainder.
                    let last_newline_pos = memchr::memrchr(b'\n', &chunk);

                    // Compute the last newline position relative to the NEW
                    // bytes only (for the checkpoint tracker). The tracker
                    // only sees bytes read from the file, not the remainder
                    // prefix.
                    let remainder_prefix_len = chunk.len() - bytes.len();
                    // If the last newline is inside the old remainder (not
                    // the new bytes), the tracker sees no newline in the read.
                    let last_newline_in_new_bytes = last_newline_pos.and_then(|pos| {
                        (pos >= remainder_prefix_len).then(|| (pos - remainder_prefix_len) as u64)
                    });

                    // Update checkpoint tracker with the new read.
                    state.tracker.apply_read(n_bytes, last_newline_in_new_bytes);

                    match last_newline_pos {
                        Some(pos) => {
                            if pos + 1 < chunk.len() {
                                // Move tail to remainder without allocating.
                                let mut tail = chunk.split_off(pos + 1);
                                if tail.len() > MAX_REMAINDER_BYTES {
                                    // Tail exceeds the per-source cap. Discard the
                                    // oldest bytes and keep the most recent
                                    // MAX_REMAINDER_BYTES so the source can
                                    // eventually emit a complete line. Emit a warning
                                    // so the data loss is not silent.
                                    tracing::warn!(
                                        source_key = ?key,
                                        tail_bytes = tail.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — partial line exceeds \
                                         MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                         bytes and resetting format state"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let state = self.source_state_mut(key);
                                    // Reset format so the next line starts from a clean
                                    // state (the discarded prefix may have broken CRI P/F
                                    // sequence alignment).
                                    state.format.reset();
                                    // Keep the tail of the overflow data in the remainder
                                    // buffer so the next newline can complete it. Do NOT
                                    // call apply_remainder_consumed() — the data is still
                                    // pending and the checkpoint must not advance past it.
                                    let start = tail.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = tail.split_off(start);
                                    state.overflow_tainted = true;
                                } else {
                                    let state = self.source_state_mut(key);
                                    state.remainder = tail;
                                }
                                chunk.truncate(pos + 1);
                            }
                        }
                        None => {
                            // No newline at all — entire chunk is remainder.
                            if chunk.len() > MAX_REMAINDER_BYTES {
                                // Same overflow policy as the tail case: warn, reset
                                // format state, and keep the most recent bytes.
                                tracing::warn!(
                                    source_key = ?key,
                                    chunk_bytes = chunk.len(),
                                    max_remainder_bytes = MAX_REMAINDER_BYTES,
                                    "framed.remainder_overflow — partial line exceeds \
                                     MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                     bytes and resetting format state"
                                );
                                self.stats.inc_parse_errors(1);
                                let state = self.source_state_mut(key);
                                state.format.reset();
                                let start = chunk.len() - MAX_REMAINDER_BYTES;
                                state.remainder = chunk.split_off(start);
                                state.overflow_tainted = true;
                                // Do NOT call apply_remainder_consumed() — data is preserved.
                            } else {
                                let state = self.source_state_mut(key);
                                state.remainder = chunk;
                            }
                            continue;
                        }
                    }

                    // Process complete lines through per-source format handler.
                    self.out_buf.clear();
                    self.cri_metadata_buf.clear();
                    let (sources, out_buf, cri_metadata_buf) = (
                        &mut self.sources,
                        &mut self.out_buf,
                        &mut self.cri_metadata_buf,
                    );
                    let state = sources.get_mut(&key).unwrap_or_else(|| {
                        panic!("source state missing for key {key:?}; framed input invariant violated")
                    });
                    let mut process_start = 0usize;
                    if tainted_on_entry {
                        // Overflow truncation preserves a suffix of a long line.
                        // Once a newline arrives, that first "line" is a
                        // synthetic mid-line fragment and must be dropped.
                        if let Some(first_newline) = memchr::memchr(b'\n', &chunk) {
                            process_start = first_newline + 1;
                            state.overflow_tainted = false;
                        }
                    }
                    if process_start < chunk.len() {
                        state.format.process_lines_with_metadata(
                            &chunk[process_start..],
                            out_buf,
                            Some(cri_metadata_buf),
                        );
                    }
                    let line_count = memchr::memchr_iter(b'\n', &chunk[process_start..]).count();
                    self.stats.inc_lines(line_count as u64);

                    if !self.out_buf.is_empty() {
                        // Take out_buf's content, swap in spare_buf's capacity
                        // for next iteration. No allocation — the 64KB bounces
                        // between the two buffers.
                        let data = std::mem::take(&mut self.out_buf);
                        let cri_metadata = self.cri_metadata_for_emitted_data();
                        std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                        result_events.push(InputEvent::Data {
                            bytes: Bytes::from(data),
                            source_id,
                            accounted_bytes: 0,
                            cri_metadata,
                        });
                    }

                    if key.is_some()
                        && self.inner.should_reclaim_completed_source_state()
                        && self
                            .sources
                            .get(&key)
                            .is_some_and(SourceState::is_reclaimable)
                    {
                        self.sources.remove(&key);
                    }
                }
                InputEvent::Batch {
                    batch,
                    source_id,
                    accounted_bytes,
                } => {
                    self.stats.inc_lines(batch.num_rows() as u64);
                    self.stats.inc_bytes(accounted_bytes);
                    result_events.push(InputEvent::Batch {
                        batch,
                        source_id,
                        accounted_bytes: 0,
                    });
                }
                // Rotation/truncation: clear framing state + forward event.
                //
                // When source_id is known, clear only the affected source's
                // state. When unknown (None), clear all sources as a
                // conservative fallback.
                event @ (InputEvent::Rotated { source_id }
                | InputEvent::Truncated { source_id }) => {
                    match source_id {
                        Some(_) => {
                            self.sources.remove(&source_id);
                        }
                        None => {
                            self.sources.clear();
                        }
                    }
                    result_events.push(event);
                }
                // End of file: flush any partial-line remainder.
                //
                // When a file ends without a trailing newline the last record
                // sits in the remainder indefinitely.  Appending a synthetic
                // `\n` lets the format processor treat it as a complete line so
                // it reaches the scanner instead of being silently dropped.
                //
                // When source_id is known, flush only the affected source's
                // remainder. When unknown (None), flush all remainders as a
                // conservative fallback.
                InputEvent::EndOfFile { source_id } => {
                    let keys_to_flush: Vec<Option<SourceId>> = match source_id {
                        Some(_) => vec![source_id],
                        None => self.sources.keys().copied().collect(),
                    };
                    for key in keys_to_flush {
                        if let Some(state) = self.sources.get_mut(&key)
                            && !state.remainder.is_empty()
                        {
                            let mut remainder = std::mem::take(&mut state.remainder);
                            remainder.push(b'\n');
                            let mut process_start = 0usize;
                            if state.overflow_tainted {
                                process_start =
                                    memchr::memchr(b'\n', &remainder).map_or(0, |i| i + 1);
                                state.overflow_tainted = false;
                            }

                            self.out_buf.clear();
                            self.cri_metadata_buf.clear();
                            let (sources, out_buf, cri_metadata_buf) = (
                                &mut self.sources,
                                &mut self.out_buf,
                                &mut self.cri_metadata_buf,
                            );
                            let state = sources.get_mut(&key).unwrap_or_else(|| {
                                panic!(
                                    "source state missing for key {key:?}; framed input invariant violated"
                                )
                            });
                            if process_start < remainder.len() {
                                state.format.process_lines_with_metadata(
                                    &remainder[process_start..],
                                    out_buf,
                                    Some(cri_metadata_buf),
                                );
                            }
                            let emitted_line_count =
                                memchr::memchr_iter(b'\n', &remainder[process_start..]).count();
                            self.stats.inc_lines(emitted_line_count as u64);

                            // Remainder was flushed — update tracker so
                            // checkpointable_offset advances past the
                            // flushed bytes.
                            let state = self.source_state_mut(key);
                            state.tracker.apply_remainder_consumed();

                            if !self.out_buf.is_empty() {
                                let data = std::mem::take(&mut self.out_buf);
                                let cri_metadata = self.cri_metadata_for_emitted_data();
                                std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                                result_events.push(InputEvent::Data {
                                    bytes: Bytes::from(data),
                                    source_id: key,
                                    accounted_bytes: 0,
                                    cri_metadata,
                                });
                            }
                        }
                        // Reclaim only completed EOF state. CRI P/F assembly
                        // can hold pending data even when the line remainder
                        // is empty, and file-tail EOF can be an idle signal
                        // rather than a terminal source event.
                        if self
                            .sources
                            .get(&key)
                            .is_some_and(SourceState::is_reclaimable)
                        {
                            self.sources.remove(&key);
                        }
                    }
                    result_events.push(InputEvent::EndOfFile { source_id });
                }
            }
        }

        result_events
    }
}
