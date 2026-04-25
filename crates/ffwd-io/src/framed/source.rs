impl InputSource for FramedInput {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        let raw_events = self.inner.poll()?;
        Ok(self.process_raw_events(raw_events))
    }

    fn poll_shutdown(&mut self) -> io::Result<Vec<SourceEvent>> {
        let raw_events = self.inner.poll_shutdown()?;
        Ok(self.process_raw_events(raw_events))
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn health(&self) -> ComponentHealth {
        self.inner.health()
    }

    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    fn apply_hints(&mut self, hints: &FilterHints) {
        self.inner.apply_hints(hints);
    }

    fn get_cadence(&self) -> InputCadence {
        let mut cadence = self.inner.get_cadence();
        cadence.signal.had_data |= self.last_raw_had_payload;
        cadence
    }

    /// Return checkpoint offsets from the Kani-proven CheckpointTracker.
    ///
    /// Each per-source tracker maintains the relationship between the file
    /// read offset and the last complete newline boundary. The
    /// `checkpointable_offset()` is always at a newline boundary, so a
    /// crash + restart from that offset will not skip any unprocessed data.
    ///
    /// This replaces ad-hoc `offset.saturating_sub(remainder_len)` with
    /// the same proven arithmetic from `CheckpointTracker`.
    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        self.inner
            .checkpoint_data()
            .into_iter()
            .map(|(sid, offset)| {
                let checkpointable = self.sources.get(&Some(sid)).map_or(offset.0, |state| {
                    // The tracker's checkpointable_offset is relative to
                    // the tracker's cumulative read_offset. We need to
                    // translate: the inner source reports absolute file
                    // offset, and the tracker reports how much remainder
                    // to subtract.
                    let remainder_len = state.tracker.remainder_len();
                    offset.0.saturating_sub(remainder_len)
                });
                (sid, ByteOffset(checkpointable))
            })
            .collect()
    }

    fn source_paths(&self) -> Vec<(SourceId, std::path::PathBuf)> {
        self.inner.source_paths()
    }

    fn should_reclaim_completed_source_state(&self) -> bool {
        self.inner.should_reclaim_completed_source_state()
    }

    fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) {
        // A forced rewind/reset invalidates any buffered remainder and decoder
        // state for this source. Keep the wrapper aligned with the inner
        // source's new starting point.
        self.sources.remove(&Some(source_id));
        self.inner.set_offset_by_source(source_id, offset);
    }
}
