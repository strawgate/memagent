    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::VecDeque;

    /// Mock input source for testing.
    struct MockSource {
        name: String,
        events: VecDeque<Vec<SourceEvent>>,
        shutdown_events: VecDeque<Vec<SourceEvent>>,
        offsets: Vec<(SourceId, ByteOffset)>,
        source_paths: Vec<(SourceId, std::path::PathBuf)>,
        health: ComponentHealth,
        cadence_signal: PollCadenceSignal,
        cadence_max: u8,
    }

    impl MockSource {
        fn new(batches: Vec<Vec<SourceEvent>>) -> Self {
            Self {
                name: "mock".to_string(),
                events: batches.into(),
                shutdown_events: VecDeque::new(),
                offsets: vec![],
                source_paths: vec![],
                health: ComponentHealth::Healthy,
                cadence_signal: PollCadenceSignal::default(),
                cadence_max: 0,
            }
        }

        fn from_chunks(chunks: Vec<&[u8]>) -> Self {
            Self::new(
                chunks
                    .into_iter()
                    .map(|c| {
                        vec![SourceEvent::Data {
                            bytes: Bytes::from(c.to_vec()),
                            source_id: None,
                            accounted_bytes: c.len() as u64,
                            cri_metadata: None,
                        }]
                    })
                    .collect(),
            )
        }

        /// Like `from_chunks`, but tags every event with `Some(sid)` so that
        /// `FramedInput::checkpoint_data()` can actually find the per-source
        /// state under `Some(SourceId)` instead of falling back to the raw
        /// offset.  Use this whenever a test needs to assert `checkpoint_data`.
        fn from_chunks_with_source(chunks: Vec<&[u8]>, sid: SourceId) -> Self {
            Self::new(
                chunks
                    .into_iter()
                    .map(|c| {
                        vec![SourceEvent::Data {
                            bytes: Bytes::from(c.to_vec()),
                            source_id: Some(sid),
                            accounted_bytes: c.len() as u64,
                            cri_metadata: None,
                        }]
                    })
                    .collect(),
            )
        }

        fn with_offsets(mut self, offsets: Vec<(SourceId, ByteOffset)>) -> Self {
            self.offsets = offsets;
            self
        }

        fn with_shutdown_events(mut self, events: Vec<Vec<SourceEvent>>) -> Self {
            self.shutdown_events = events.into();
            self
        }

        fn with_source_paths(mut self, source_paths: Vec<(SourceId, std::path::PathBuf)>) -> Self {
            self.source_paths = source_paths;
            self
        }

        fn with_health(mut self, health: ComponentHealth) -> Self {
            self.health = health;
            self
        }

        fn with_cadence(mut self, signal: PollCadenceSignal, max: u8) -> Self {
            self.cadence_signal = signal;
            self.cadence_max = max;
            self
        }
    }

    impl InputSource for MockSource {
        fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
            Ok(self.events.pop_front().unwrap_or_default())
        }

        fn poll_shutdown(&mut self) -> io::Result<Vec<SourceEvent>> {
            Ok(self.shutdown_events.pop_front().unwrap_or_default())
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn health(&self) -> ComponentHealth {
            self.health
        }

        fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
            self.offsets.clone()
        }

        fn source_paths(&self) -> Vec<(SourceId, std::path::PathBuf)> {
            self.source_paths.clone()
        }

        fn get_cadence(&self) -> InputCadence {
            InputCadence {
                signal: self.cadence_signal,
                adaptive_fast_polls_max: self.cadence_max,
            }
        }
    }

    fn make_stats() -> Arc<ComponentStats> {
        Arc::new(ComponentStats::new())
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("seq", DataType::Int64, true),
        ]));
        let msg = StringArray::from(vec![Some("alpha"), Some("beta")]);
        let seq = Int64Array::from(vec![Some(1), Some(2)]);
        RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(seq)]).expect("batch")
    }

    fn collect_data(events: Vec<SourceEvent>) -> Vec<u8> {
        let mut out = Vec::new();
        for e in events {
            if let SourceEvent::Data { bytes, .. } = e {
                out.extend_from_slice(&bytes);
            }
        }
        out
    }

    #[test]
    fn passthrough_complete_lines() {
        let stats = make_stats();
        let source = MockSource::from_chunks(vec![b"line1\nline2\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"line1\nline2\n");
    }

    #[test]
    fn framed_input_forwards_inner_health() {
        let stats = make_stats();
        let source = MockSource::new(vec![]).with_health(ComponentHealth::Degraded);
        let framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        assert_eq!(framed.health(), ComponentHealth::Degraded);
    }

    #[test]
    fn framed_input_forwards_cadence_snapshot() {
        let stats = make_stats();
        let source = MockSource::new(vec![]).with_cadence(
            PollCadenceSignal {
                had_data: true,
                hit_read_budget: true,
            },
            7,
        );
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let _ = framed.poll().expect("framed poll should succeed");
        assert_eq!(
            framed.get_cadence(),
            InputCadence {
                signal: PollCadenceSignal {
                    had_data: true,
                    hit_read_budget: true,
                },
                adaptive_fast_polls_max: 7,
            }
        );
    }

    #[test]
    fn framed_input_reports_raw_shutdown_payload_when_output_is_empty() {
        let stats = make_stats();
        let source = MockSource::new(vec![]).with_shutdown_events(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"partial"),
            source_id: Some(SourceId(1)),
            accounted_bytes: 7,
            cri_metadata: None,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let events = framed
            .poll_shutdown()
            .expect("shutdown poll should succeed");
        assert!(events.is_empty());
        assert!(framed.get_cadence().signal.had_data);
    }

    #[test]
    fn remainder_across_polls() {
        let stats = make_stats();
        let source = MockSource::from_chunks(vec![b"hello\nwor", b"ld\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // First poll: "hello\n" is complete, "wor" is remainder
        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"hello\n");

        // Second poll: remainder "wor" + "ld\n" → "world\n"
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"world\n");
    }

    #[test]
    fn no_newline_becomes_remainder() {
        let stats = make_stats();
        let source = MockSource::from_chunks(vec![b"partial", b"more\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: no newline, everything goes to remainder
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Second poll: remainder + new data → complete line
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"partialmore\n");
    }

    #[test]
    fn batch_events_increment_stats() {
        let stats = make_stats();
        let batch = make_batch();
        let expected_rows = batch.num_rows() as u64;
        let expected_bytes = 1234;
        let source = MockSource::new(vec![vec![SourceEvent::Batch {
            batch,
            source_id: None,
            accounted_bytes: expected_bytes,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events = framed.poll().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], SourceEvent::Batch { .. }));
        assert_eq!(stats.lines(), expected_rows);
        assert_eq!(stats.bytes(), expected_bytes);
    }

    #[test]
    fn data_events_use_accounted_bytes_for_stats() {
        let stats = make_stats();
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"line\n"),
            source_id: None,
            accounted_bytes: 99,
            cri_metadata: None,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"line\n");
        assert_eq!(stats.lines(), 1);
        assert_eq!(stats.bytes(), 99);
    }

    #[test]
    fn remainder_capped_at_max_and_tainted_line_is_dropped() {
        let stats = make_stats();
        // Send > 2 MiB without a newline.
        let big = vec![b'x'; MAX_REMAINDER_BYTES + 1];
        let source = MockSource::from_chunks(vec![&big, b"\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: no newline, overflow triggers parse_error.
        let events = framed.poll().unwrap();
        assert!(collect_data(events).is_empty());
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        // The overflow remainder is capped but NOT discarded.
        let state = framed.sources.get(&None).unwrap();
        assert_eq!(
            state.remainder.len(),
            MAX_REMAINDER_BYTES,
            "overflow remainder must be capped to MAX_REMAINDER_BYTES, not dropped"
        );

        // Second poll: newline completes only the tainted fragment; it must be dropped.
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert!(
            data2.is_empty(),
            "tainted overflow remainder must be discarded when the first newline arrives"
        );
    }

    #[test]
    fn tail_after_newline_is_capped_at_max_and_tainted_line_is_dropped() {
        let stats = make_stats();
        let mut chunk = b"ok\n".to_vec();
        chunk.extend(vec![b'x'; MAX_REMAINDER_BYTES + 1]);
        let source = MockSource::from_chunks(vec![&chunk, b"\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: "ok\n" emitted; overflow tail triggers parse_error and
        // is preserved as remainder (last MAX_REMAINDER_BYTES bytes).
        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"ok\n");
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        // The overflow remainder is preserved (not silently dropped).
        let state = framed.sources.get(&None).unwrap();
        assert_eq!(
            state.remainder.len(),
            MAX_REMAINDER_BYTES,
            "overflow tail must be truncated to MAX_REMAINDER_BYTES, not dropped"
        );

        // Second poll: newline terminates only tainted overflow bytes; that
        // line must be discarded.
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert!(
            data2.is_empty(),
            "tainted overflow remainder must be discarded when the first newline arrives"
        );
    }

    /// Regression for #1030: after remainder overflow, the first completed
    /// line is a truncated mid-line fragment and must be discarded.
    #[test]
    fn overflow_fragment_is_discarded_when_newline_arrives() {
        let stats = make_stats();
        let big = vec![b'x'; MAX_REMAINDER_BYTES + 1];
        let source = MockSource::from_chunks(vec![&big, b"\nreal-line\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: overflow, nothing emitted.
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Second poll: truncated overflow fragment must be dropped, while the
        // next complete real line is preserved.
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"real-line\n");
    }
