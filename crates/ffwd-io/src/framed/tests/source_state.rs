    #[test]
    fn two_sources_interleaved_no_cross_contamination() {
        let stats = make_stats();
        let sid_a = SourceId(100);
        let sid_b = SourceId(200);

        let source = MockSource::new(vec![
            // Poll 1: partial lines from both sources in one batch
            vec![
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"hello-from-A"),
                    source_id: Some(sid_a),
                    accounted_bytes: 12,
                    cri_metadata: None,
                },
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"hello-from-B"),
                    source_id: Some(sid_b),
                    accounted_bytes: 12,
                    cri_metadata: None,
                },
            ],
            // Poll 2: complete the lines from each source
            vec![
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"-done\n"),
                    source_id: Some(sid_a),
                    accounted_bytes: 6,
                    cri_metadata: None,
                },
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"-done\n"),
                    source_id: Some(sid_b),
                    accounted_bytes: 6,
                    cri_metadata: None,
                },
            ],
        ]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // Poll 1: no complete lines -- everything in remainders.
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Poll 2: each source gets its own remainder prepended.
        let events2 = framed.poll().unwrap();
        let mut output_a = Vec::new();
        let mut output_b = Vec::new();
        for e in events2 {
            if let SourceEvent::Data {
                bytes, source_id, ..
            } = e
            {
                match source_id {
                    Some(sid) if sid == sid_a => output_a.extend_from_slice(&bytes),
                    Some(sid) if sid == sid_b => output_b.extend_from_slice(&bytes),
                    _ => panic!("unexpected source_id"),
                }
            }
        }
        assert_eq!(output_a, b"hello-from-A-done\n");
        assert_eq!(output_b, b"hello-from-B-done\n");
    }

    /// Truncation clears all remainders (current behavior).
    #[test]
    fn truncation_clears_all_remainders() {
        let stats = make_stats();
        let sid_a = SourceId(100);
        let sid_b = SourceId(200);

        let source = MockSource::new(vec![
            // Partial lines from two sources
            vec![
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"partial-A"),
                    source_id: Some(sid_a),
                    accounted_bytes: 9,
                    cri_metadata: None,
                },
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"partial-B"),
                    source_id: Some(sid_b),
                    accounted_bytes: 9,
                    cri_metadata: None,
                },
            ],
            // Truncation
            vec![SourceEvent::Truncated { source_id: None }],
            // Fresh data from source A
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"fresh-A\n"),
                source_id: Some(sid_a),
                accounted_bytes: 8,
                cri_metadata: None,
            }],
        ]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // Partials go into remainders
        let _ = framed.poll().unwrap();

        // Truncation clears all
        let _ = framed.poll().unwrap();

        // Fresh data from A -- must NOT include "partial-A" prefix
        let events3 = framed.poll().unwrap();
        assert_eq!(collect_data(events3), b"fresh-A\n");
    }

    #[test]
    fn set_offset_clears_remainder_for_rewound_source() {
        let stats = make_stats();
        let sid = SourceId(7);
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"partial"),
                source_id: Some(sid),
                accounted_bytes: 7,
                cri_metadata: None,
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"fresh\n"),
                source_id: Some(sid),
                accounted_bytes: 6,
                cri_metadata: None,
            }],
        ]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let _ = framed.poll().unwrap();
        framed.set_offset_by_source(sid, 0);

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"fresh\n");
        assert!(
            framed
                .sources
                .get(&Some(sid))
                .is_none_or(|state| state.remainder.is_empty()),
            "rewind should not preserve stale buffered bytes for the source"
        );
    }

    /// checkpoint_data() subtracts remainder length from inner offsets.
    #[test]
    fn checkpoint_data_subtracts_remainder() {
        let stats = make_stats();
        let sid = SourceId(42);

        // The inner source reports offset 1000 for our source.
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"hello\nwor"),
            source_id: Some(sid),
            accounted_bytes: 9,
            cri_metadata: None,
        }]])
        .with_offsets(vec![(sid, ByteOffset(1000))]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // After poll, "wor" (3 bytes) is in the remainder for sid.
        let _ = framed.poll().unwrap();

        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1);
        assert_eq!(cp[0].0, sid);
        // 1000 - 3 = 997
        assert_eq!(cp[0].1, ByteOffset(997));
    }

    /// checkpoint_data() returns the raw offset when no remainder is buffered.
    #[test]
    fn checkpoint_data_no_remainder() {
        let stats = make_stats();
        let sid = SourceId(42);

        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"complete\n"),
            source_id: Some(sid),
            accounted_bytes: 9,
            cri_metadata: None,
        }]])
        .with_offsets(vec![(sid, ByteOffset(500))]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let _ = framed.poll().unwrap();

        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1);
        assert_eq!(cp[0].1, ByteOffset(500));
    }

    // -----------------------------------------------------------------------
    // Per-source CRI isolation tests
    // -----------------------------------------------------------------------

    /// CRI P/F aggregation state is isolated between sources.
    ///
    /// Source A sends a P (partial) line, source B sends an F (full) line.
    /// Without per-source format state, B's F would complete A's P, merging
    /// data from two different files into one record.
    #[test]
    fn cri_pf_state_isolated_between_sources() {
        let stats = make_stats();
        let sid_a = SourceId(100);
        let sid_b = SourceId(200);

        let source = MockSource::new(vec![
            // Source A: CRI partial line
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:00Z stdout P hello \n"),
                source_id: Some(sid_a),
                accounted_bytes: 38,
                cri_metadata: None,
            }],
            // Source B: CRI full line (must NOT merge with A's partial)
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:01Z stderr F {\"msg\":\"world\"}\n"),
                source_id: Some(sid_b),
                accounted_bytes: 50,
                cri_metadata: None,
            }],
            // Source A: CRI full line (completes A's partial)
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:02Z stdout F from-A\n"),
                source_id: Some(sid_a),
                accounted_bytes: 39,
                cri_metadata: None,
            }],
        ]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        // Poll 1: A's partial — nothing emitted
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Poll 2: B's full line — emitted as standalone
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert_eq!(data2, b"{\"msg\":\"world\"}\n");
        assert!(
            !data2.windows(5).any(|w| w == b"hello"),
            "B's output must NOT contain A's partial"
        );

        // Poll 3: A's full line — completes A's P+F sequence
        let events3 = framed.poll().unwrap();
        let data3 = collect_data(events3);
        assert!(
            data3.windows(5).any(|w| w == b"hello"),
            "A's output should contain the merged P+F data"
        );
    }

    /// CheckpointTracker is updated correctly through framing operations.
    #[test]
    fn checkpoint_tracker_tracks_remainder() {
        let stats = make_stats();
        let sid = SourceId(42);

        let source = MockSource::new(vec![
            // First read: 9 bytes, newline at position 5
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"hello\nwor"),
                source_id: Some(sid),
                accounted_bytes: 9,
                cri_metadata: None,
            }],
            // Second read: 3 bytes, newline at position 1 (the 'd\n')
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"ld\n"),
                source_id: Some(sid),
                accounted_bytes: 3,
                cri_metadata: None,
            }],
        ])
        .with_offsets(vec![(sid, ByteOffset(12))]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // After first poll: remainder is "wor" (3 bytes)
        let _ = framed.poll().unwrap();
        let state = framed.sources.get(&Some(sid)).unwrap();
        assert_eq!(state.tracker.remainder_len(), 3);

        // After second poll: remainder is empty (all consumed)
        let _ = framed.poll().unwrap();
        assert!(
            !framed.sources.contains_key(&Some(sid)),
            "complete source state should be reclaimed once no remainder is buffered"
        );

        // Checkpoint should fall back to the raw source offset when no
        // remainder state remains: 12 - 0 = 12.
        let cp = framed.checkpoint_data();
        assert_eq!(cp[0].1, ByteOffset(12));
    }

    #[test]
    fn file_json_does_not_inject_source_path_column() {
        let stats = make_stats();
        let sid = SourceId(7);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/main.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn file_json_preserves_source_id_without_rewriting_payload() {
        let stats = make_stats();
        let sid = SourceId(17);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/main.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            SourceEvent::Data {
                bytes,
                source_id: Some(actual_sid),
                ..
            } => {
                assert_eq!(&bytes[..], b"{\"msg\":\"hello\"}\n");
                assert_eq!(*actual_sid, sid);
            }
            _ => panic!("expected data event with preserved source_id"),
        }
    }

    #[test]
    fn file_cri_does_not_inject_source_path_alongside_cri_metadata() {
        let stats = make_stats();
        let sid = SourceId(8);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/0.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn file_json_preserves_leading_whitespace_without_source_path_rewrite() {
        let stats = make_stats();
        let sid = SourceId(9);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"  \t{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/1.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"  \t{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn file_json_empty_object_is_not_rewritten_for_source_path() {
        let stats = make_stats();
        let sid = SourceId(11);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/2.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{}\n");
    }

    #[test]
    fn file_raw_passthrough_does_not_inject_source_path() {
        let stats = make_stats();
        let sid = SourceId(10);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/raw.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    /// EndOfFile must reclaim per-source state so long-running inputs (S3)
    /// don't accumulate dead entries in the `sources` HashMap.
    #[test]
    fn eof_removes_source_state() {
        let stats = make_stats();
        let sid = SourceId(42);
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"hello\npartial"),
                source_id: Some(sid),
                accounted_bytes: 13,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile {
                source_id: Some(sid),
            }],
            // New data for the same SourceId after EOF — must work.
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"world\n"),
                source_id: Some(sid),
                accounted_bytes: 6,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        // Poll 1: "hello\n" emitted, "partial" in remainder.
        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"hello\n");
        assert!(framed.sources.contains_key(&Some(sid)));

        // Poll 2: EOF flushes "partial\n" and removes source state.
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"partial\n");
        assert!(
            !framed.sources.contains_key(&Some(sid)),
            "source state should be removed after EOF"
        );

        // Poll 3: new data for the same SourceId creates fresh state.
        let events3 = framed.poll().unwrap();
        assert_eq!(collect_data(events3), b"world\n");
    }

    #[test]
    fn complete_source_state_is_reclaimed_without_eof() {
        let stats = make_stats();
        let sid = SourceId(42);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"done\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 15,
            cri_metadata: None,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(stats.clone()),
            stats,
        );

        let events = framed.poll().unwrap();

        assert_eq!(collect_data(events), b"{\"msg\":\"done\"}\n");
        assert!(
            framed.sources.is_empty(),
            "complete source state should not accumulate for ephemeral sources"
        );
    }

    #[test]
    fn cri_partial_source_state_survives_until_full_record() {
        let stats = make_stats();
        let sid = SourceId(42);
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(
                    b"2024-01-01T00:00:00.000000000Z stdout P {\"msg\":\"part",
                ),
                source_id: Some(sid),
                accounted_bytes: 57,
                cri_metadata: None,
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"ial\"}\n2024-01-01T00:00:00.000000000Z stdout F \n"),
                source_id: Some(sid),
                accounted_bytes: 54,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(1024, stats.clone()),
            stats,
        );

        let first = framed.poll().unwrap();
        assert!(first.is_empty());
        assert!(
            framed.sources.contains_key(&Some(sid)),
            "CRI partial state must survive across polls"
        );

        let second = framed.poll().unwrap();
        let output = collect_data(second);
        assert!(
            output.ends_with(b"\"msg\":\"partial\"}\n"),
            "unexpected CRI output: {}",
            String::from_utf8_lossy(&output)
        );
        assert!(
            framed.sources.is_empty(),
            "CRI source state should be reclaimed after the full record completes"
        );
    }

    #[test]
    fn eof_does_not_reclaim_pending_cri_fragment() {
        let stats = make_stats();
        let sid = SourceId(43);
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(
                    b"2024-01-01T00:00:00.000000000Z stdout P {\"msg\":\"part\n",
                ),
                source_id: Some(sid),
                accounted_bytes: 58,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile {
                source_id: Some(sid),
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-01T00:00:00.000000000Z stdout F ial\"}\n"),
                source_id: Some(sid),
                accounted_bytes: 53,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(1024, stats.clone()),
            stats,
        );

        assert!(framed.poll().unwrap().is_empty());
        assert!(
            framed.sources.contains_key(&Some(sid)),
            "CRI P fragment should leave pending format state"
        );

        let eof_events = framed.poll().unwrap();
        assert_eq!(eof_events.len(), 1);
        match &eof_events[0] {
            SourceEvent::EndOfFile { source_id } => assert_eq!(*source_id, Some(sid)),
            _ => panic!("expected EOF event"),
        }
        assert!(
            framed.sources.contains_key(&Some(sid)),
            "EOF must not remove pending CRI state"
        );

        let output = collect_data(framed.poll().unwrap());
        assert!(
            output.ends_with(b"\"msg\":\"partial\"}\n"),
            "unexpected CRI output after EOF: {}",
            String::from_utf8_lossy(&output)
        );
    }
