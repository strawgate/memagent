    #[test]
    fn checkpoint_advances_after_tainted_fragment_is_discarded() {
        let stats = make_stats();
        let sid = SourceId(9);
        let big = vec![b'x'; MAX_REMAINDER_BYTES + 1];
        let first = big.len() as u64;
        let second_bytes = b"\nreal-line\n";
        let total = first + second_bytes.len() as u64;
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: Bytes::from(big),
                source_id: Some(sid),
                accounted_bytes: 0,
                cri_metadata: None,
            }],
            vec![InputEvent::Data {
                bytes: Bytes::from(second_bytes.to_vec()),
                source_id: Some(sid),
                accounted_bytes: 0,
                cri_metadata: None,
            }],
        ])
        .with_offsets(vec![(sid, ByteOffset(total))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let _ = framed.poll().unwrap();
        let cp1 = framed.checkpoint_data();
        assert!(
            cp1[0].1.0 < total,
            "checkpoint must stay behind raw offset while overflow remainder is buffered"
        );

        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"real-line\n");
        let cp2 = framed.checkpoint_data();
        assert_eq!(cp2[0].1, ByteOffset(total));
    }

    /// `checkpoint_data()` must account for overflow remainder when the source
    /// has a real `SourceId`.  With `source_id: None` the lookup in
    /// `self.sources.get(&Some(sid))` silently falls back to the raw offset,
    /// making the assertion trivially true regardless of correctness.  This
    /// test uses `from_chunks_with_source` so the per-source state is actually
    /// keyed under `Some(SourceId(1))` and the checkpoint path is exercised.
    #[test]
    fn checkpoint_data_accounts_for_overflow_remainder() {
        let stats = make_stats();
        let sid = SourceId(1);
        // The inner source claims it has read `big.len()` bytes.  We set the
        // reported offset to exactly that many bytes so the checkpoint must
        // subtract the remainder to be correct.
        let big_len = MAX_REMAINDER_BYTES + 1;
        let big = vec![b'x'; big_len];
        // Reported offset equals the number of bytes the inner source has
        // "read" so far: big_len bytes with no newline.
        let reported_offset = big_len as u64;
        let source = MockSource::from_chunks_with_source(vec![&big, b"\n"], sid)
            .with_offsets(vec![(sid, ByteOffset(reported_offset))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: no newline — overflow triggers parse_error and remainder
        // is capped to MAX_REMAINDER_BYTES.
        let _ = framed.poll().unwrap();

        // The per-source state must be reachable under Some(sid).
        let state = framed.sources.get(&Some(sid)).unwrap();
        assert_eq!(state.remainder.len(), MAX_REMAINDER_BYTES);

        // checkpoint_data() must subtract the remainder length from the raw
        // offset, not return the raw offset unchanged.
        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1, "expected exactly one checkpoint entry");
        let (cp_sid, cp_offset) = cp[0];
        assert_eq!(cp_sid, sid);
        // The checkpointable offset must be strictly less than the reported
        // offset because there is an undelivered remainder buffered.
        assert!(
            cp_offset.0 < reported_offset,
            "checkpoint offset ({}) must be less than raw offset ({}) when remainder is buffered",
            cp_offset.0,
            reported_offset
        );
    }

    /// `checkpoint_data()` must account for the overflow tail remainder when
    /// a newline precedes the overflow.  Uses `from_chunks_with_source` so the
    /// assertion exercises the real `self.sources.get(&Some(sid))` path.
    #[test]
    fn checkpoint_data_accounts_for_overflow_tail_remainder() {
        let stats = make_stats();
        let sid = SourceId(1);
        let mut chunk = b"ok\n".to_vec();
        chunk.extend(vec![b'x'; MAX_REMAINDER_BYTES + 1]);
        let chunk_len = chunk.len() as u64;
        let source = MockSource::from_chunks_with_source(vec![&chunk, b"\n"], sid)
            .with_offsets(vec![(sid, ByteOffset(chunk_len))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: "ok\n" is emitted and the overflow tail becomes the
        // remainder (capped to MAX_REMAINDER_BYTES).
        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"ok\n");

        // Per-source state is keyed under Some(sid).
        let state = framed.sources.get(&Some(sid)).unwrap();
        assert_eq!(state.remainder.len(), MAX_REMAINDER_BYTES);

        // The checkpoint must be behind the raw offset because the remainder
        // has not yet been delivered as a complete line.
        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1);
        let (cp_sid, cp_offset) = cp[0];
        assert_eq!(cp_sid, sid);
        assert!(
            cp_offset.0 < chunk_len,
            "checkpoint offset ({}) must be less than raw offset ({}) when overflow tail is buffered",
            cp_offset.0,
            chunk_len
        );
    }

    #[test]
    fn rotated_clears_remainder_and_format() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: Bytes::from_static(b"partial"),
                source_id: None,
                accounted_bytes: 7,
                cri_metadata: None,
            }],
            vec![InputEvent::Rotated { source_id: None }],
            vec![InputEvent::Data {
                bytes: Bytes::from_static(b"fresh\n"),
                source_id: None,
                accounted_bytes: 6,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // Partial goes to remainder
        let _ = framed.poll().unwrap();

        // Rotation clears remainder
        let events2 = framed.poll().unwrap();
        assert!(
            events2
                .iter()
                .any(|e| matches!(e, InputEvent::Rotated { .. }))
        );

        // Fresh data starts clean (no stale "partial" prefix)
        let events3 = framed.poll().unwrap();
        assert_eq!(collect_data(events3), b"fresh\n");
    }

    #[test]
    fn cri_format_extracts_messages() {
        let stats = make_stats();
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let source = MockSource::from_chunks(vec![input.as_slice()]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn cri_format_emits_metadata_sidecar() {
        let stats = make_stats();
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let source = MockSource::from_chunks(vec![input.as_slice()]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        let InputEvent::Data {
            bytes,
            cri_metadata: Some(metadata),
            ..
        } = &events[0]
        else {
            panic!("expected data event with CRI metadata");
        };
        assert_eq!(bytes.as_ref(), b"{\"msg\":\"hello\"}\n");
        assert_eq!(metadata.rows, 1);
        let values = metadata.spans[0].values.as_ref().expect("metadata values");
        assert_eq!(metadata.timestamp(values), b"2024-01-15T10:30:00Z");
        assert_eq!(values.stream.as_str(), "stdout");
    }

    #[test]
    fn split_anywhere_produces_same_output() {
        let stats = make_stats();
        let full_input = b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n";

        // Reference: process entire input at once
        let source_full = MockSource::from_chunks(vec![full_input.as_slice()]);
        let mut framed_full = FramedInput::new(
            Box::new(source_full),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );
        let reference = collect_data(framed_full.poll().unwrap());

        // Split at every possible byte position
        for split_at in 1..full_input.len() {
            let stats2 = make_stats();
            let chunk1 = &full_input[..split_at];
            let chunk2 = &full_input[split_at..];
            let source = MockSource::from_chunks(vec![chunk1, chunk2]);
            let mut framed = FramedInput::new(
                Box::new(source),
                FormatDecoder::passthrough(Arc::clone(&stats2)),
                stats2,
            );

            let mut collected = collect_data(framed.poll().unwrap());
            collected.extend_from_slice(&collect_data(framed.poll().unwrap()));

            assert_eq!(
                collected, reference,
                "split at byte {split_at} produced different output"
            );
        }
    }

    /// A file (or any source) that ends without a trailing newline must not
    /// silently drop its last record.  The `EndOfFile` event causes
    /// `FramedInput` to flush the remainder buffer with a synthetic newline.
    #[test]
    fn eof_flushes_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: Bytes::from_static(b"no-newline"),
                source_id: None,
                accounted_bytes: 10,
                cri_metadata: None,
            }],
            vec![InputEvent::EndOfFile { source_id: None }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        // First poll: data with no newline — goes to remainder, nothing emitted.
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Second poll: EndOfFile flushes the remainder as a complete line.
        let events2 = framed.poll().unwrap();
        let saw_eof = events2
            .iter()
            .any(|event| matches!(event, InputEvent::EndOfFile { source_id: None }));
        assert_eq!(collect_data(events2), b"no-newline\n");
        assert!(
            saw_eof,
            "framed input must forward EOF markers after flushing remainders"
        );
    }

    /// Runtime shutdown is a terminal lifecycle event: when the wrapped source
    /// emits EOF from `poll_shutdown`, `FramedInput` must flush bytes that were
    /// already held in its per-source remainder buffer.
    #[test]
    fn poll_shutdown_flushes_existing_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![vec![InputEvent::Data {
            bytes: Bytes::from_static(b"no-newline"),
            source_id: None,
            accounted_bytes: 10,
            cri_metadata: None,
        }]])
        .with_shutdown_events(vec![vec![InputEvent::EndOfFile { source_id: None }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        let events2 = framed.poll_shutdown().unwrap();
        let saw_eof = events2
            .iter()
            .any(|event| matches!(event, InputEvent::EndOfFile { source_id: None }));
        assert_eq!(collect_data(events2), b"no-newline\n");
        assert!(
            saw_eof,
            "shutdown EOF must propagate after flushing buffered bytes"
        );
    }

    /// Multiple records in a file where only the last one lacks a newline:
    /// all records must be emitted.
    #[test]
    fn eof_flushes_only_partial_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: Bytes::from_static(b"complete\npartial"),
                source_id: None,
                accounted_bytes: 16,
                cri_metadata: None,
            }],
            vec![InputEvent::EndOfFile { source_id: None }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        // First poll: "complete\n" is emitted; "partial" stays in remainder.
        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"complete\n");

        // Second poll: EndOfFile flushes "partial" with a synthetic newline.
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"partial\n");
    }

    /// A redundant EndOfFile (no bytes in remainder) must produce no output.
    #[test]
    fn eof_with_empty_remainder_is_noop() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: Bytes::from_static(b"line\n"),
                source_id: None,
                accounted_bytes: 5,
                cri_metadata: None,
            }],
            vec![InputEvent::EndOfFile { source_id: None }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"line\n");

        let events2 = framed.poll().unwrap();
        assert!(collect_data(events2).is_empty());
    }

    #[test]
    fn eof_flushes_remainder_and_advances_checkpoint() {
        let stats = make_stats();
        let sid = SourceId(42);
        let chunk = b"partial-line";
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: Bytes::from(chunk.to_vec()),
                source_id: Some(sid),
                accounted_bytes: chunk.len() as u64,
                cri_metadata: None,
            }],
            vec![InputEvent::EndOfFile {
                source_id: Some(sid),
            }],
        ])
        .with_offsets(vec![(sid, ByteOffset(chunk.len() as u64))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events1 = framed.poll().expect("first poll");
        assert!(collect_data(events1).is_empty());
        let before_flush = framed.checkpoint_data();
        assert_eq!(before_flush[0].0, sid);
        assert!(
            before_flush[0].1.0 < chunk.len() as u64,
            "checkpoint should remain behind raw offset while remainder is buffered"
        );

        let events2 = framed.poll().expect("eof poll");
        assert_eq!(collect_data(events2), b"partial-line\n");
        let after_flush = framed.checkpoint_data();
        assert_eq!(after_flush[0], (sid, ByteOffset(chunk.len() as u64)));
    }

    #[test]
    fn eof_for_source_only_flushes_matching_remainder() {
        let stats = make_stats();
        let sid_a = SourceId(10);
        let sid_b = SourceId(11);
        let source = MockSource::new(vec![
            vec![
                InputEvent::Data {
                    bytes: Bytes::from_static(b"alpha"),
                    source_id: Some(sid_a),
                    accounted_bytes: 5,
                    cri_metadata: None,
                },
                InputEvent::Data {
                    bytes: Bytes::from_static(b"beta"),
                    source_id: Some(sid_b),
                    accounted_bytes: 4,
                    cri_metadata: None,
                },
            ],
            vec![InputEvent::EndOfFile {
                source_id: Some(sid_a),
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let first = framed.poll().expect("first poll");
        assert!(collect_data(first).is_empty());

        let flushed = framed.poll().expect("second poll");
        assert_eq!(collect_data(flushed), b"alpha\n");

        let state_b = framed
            .sources
            .get(&Some(sid_b))
            .expect("sid_b state should still exist");
        assert_eq!(state_b.remainder, b"beta");
    }

    // -----------------------------------------------------------------------
    // Per-source remainder tests
    // -----------------------------------------------------------------------
