    #[test]
    fn test_tcp_max_clients_drops_and_warns() {
        let mut options = TcpInputOptions::default();
        options.max_clients = 2;

        let mut input = TcpInput::with_options(
            "test_max_conns",
            "127.0.0.1:0",
            options,
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();

        let addr = input.local_addr().unwrap();

        let _client1 = StdTcpStream::connect(addr).unwrap();
        let _client2 = StdTcpStream::connect(addr).unwrap();
        let _client3 = StdTcpStream::connect(addr).unwrap();
        let _client4 = StdTcpStream::connect(addr).unwrap();

        // Wait a bit to ensure connections hit the kernel accept queue
        std::thread::sleep(Duration::from_millis(50));

        // The first poll should accept 2 connections, log a warning, and drop the other 2.
        let _events = input.poll().unwrap();

        assert_eq!(
            input.clients.len(),
            2,
            "Should only accept max_clients clients"
        );
        assert!(
            input.last_max_clients_warning.is_some(),
            "Should have logged a warning"
        );
        assert_eq!(
            input.connections_accepted, 4,
            "Should have accepted and immediately dropped the excess connections"
        );

        // Let's also check a second poll does not blow up
        let _events2 = input.poll().unwrap();
        assert_eq!(input.clients.len(), 2, "Still 2 clients");
    }

    #[test]
    fn tcp_max_line_length() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        // Spawn the writer in a background thread because write_all of >1MB
        // will block until the reader drains the kernel buffer.
        // We send the oversized record AND the follow-up "ok\n" on the SAME
        // connection so the test actually verifies that the connection remains
        // usable after an oversized record is discarded, not just that a brand
        // new connection works (fix for Bug 2 / test correctness).
        let writer = std::thread::spawn(move || -> StdTcpStream {
            let mut client = StdTcpStream::connect(addr).unwrap();
            let big = vec![b'A'; MAX_LINE_LENGTH + 1];
            // Ignore errors — the server may apply backpressure before we
            // finish sending >1MB; we just need the server to see enough to
            // trigger the oversized-discard path.
            let _ = client.write_all(&big);
            // Follow-up newline terminates the oversized record so the parser
            // can discard it and resume normal framing on this connection.
            let _ = client.write_all(b"\n");
            // Now send a well-sized record on the same connection.
            client.write_all(b"ok\n").unwrap();
            client.flush().unwrap();
            client
        });

        // Poll until the "ok" record is observed or deadline expires.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut has_ok = false;
        while Instant::now() < deadline {
            let events = input.poll().unwrap();
            if events.iter().any(|e| match e {
                InputEvent::Data { bytes, .. } => bytes.windows(3).any(|w| w == b"ok\n"),
                _ => false,
            }) {
                has_ok = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Keep the client alive until after polling so the connection is not
        // torn down before we observe the "ok" record.
        let _client = writer.join().unwrap();

        assert!(
            input.connections_accepted() > 0,
            "server must have accepted the connection"
        );
        assert!(
            has_ok,
            "same connection should remain usable after oversized discard"
        );
    }

    #[test]
    fn tcp_max_line_length_exact_boundary() {
        // A line of exactly MAX_LINE_LENGTH bytes (content only, excluding \n)
        // is accepted; only records strictly larger are dropped.
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let writer = std::thread::spawn(move || {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Exactly MAX_LINE_LENGTH content bytes followed by a newline.
            let mut line = vec![b'A'; MAX_LINE_LENGTH];
            line.push(b'\n');
            let _ = client.write_all(&line);
            client.flush().unwrap();
        });

        // Use a deadline/polling loop rather than a fixed sleep so the test is
        // not flaky on slow CI runners.  Poll until the expected Data event is
        // observed or a generous timeout elapses.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut got_boundary = false;
        while Instant::now() < deadline {
            let events = input.poll().unwrap();
            if events.into_iter().any(|e| match e {
                InputEvent::Data { bytes, .. } => {
                    bytes.len() == (MAX_LINE_LENGTH + 1) && bytes.ends_with(b"\n")
                }
                _ => false,
            }) {
                got_boundary = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = writer.join();
        assert!(got_boundary, "boundary-sized line should be forwarded");
    }

    #[test]
    fn tcp_octet_counted_frame_with_embedded_newline_is_single_record() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let mut client = StdTcpStream::connect(addr).unwrap();
        client.write_all(b"11 hello\nworld").unwrap();
        client.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();
        let joined = events
            .into_iter()
            .filter_map(|e| match e {
                InputEvent::Data { bytes, .. } => Some(bytes),
                _ => None,
            })
            .flatten()
            .collect::<Vec<u8>>();
        assert_eq!(joined, b"hello\nworld\n");
    }

    #[test]
    fn tcp_legacy_line_starting_with_digits_space_is_not_stalled_as_octet() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let mut client = StdTcpStream::connect(addr).unwrap();
        client.write_all(b"200 OK\n").unwrap();
        client.flush().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = Vec::new();
        while Instant::now() < deadline {
            for event in input.poll().unwrap() {
                if let InputEvent::Data { bytes, .. } = event {
                    got.extend_from_slice(&bytes);
                }
            }
            if !got.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(got, b"200 OK\n");
    }

    #[test]
    fn tcp_connection_storm() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        // Rapidly connect and disconnect 100 times.
        for _ in 0..100 {
            let _ = StdTcpStream::connect(addr).unwrap();
            // Immediately dropped — connection closed.
        }

        std::thread::sleep(Duration::from_millis(100));

        // Poll several times to accept and then clean up all connections.
        for _ in 0..20 {
            let _ = input.poll().unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }

        assert_eq!(
            input.client_count(),
            0,
            "all storm connections should be cleaned up (no fd leak)"
        );
    }

    /// Pending bytes held in `client.pending` (an unterminated line with no
    /// trailing newline) must be emitted as a `Data` event — with a synthetic
    /// trailing newline — before the `EndOfFile` event when the connection
    /// closes.  Previously those bytes were silently dropped (data loss bug).
    #[test]
    fn pending_bytes_flushed_as_data_event_on_close() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Write a partial line with NO trailing newline so it ends up
            // in client.pending without being emitted by extract_complete_records.
            client.write_all(b"no-newline-tail").unwrap();
            client.flush().unwrap();
        } // client drops -> clean EOF

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        // There must be a Data event whose bytes contain "no-newline-tail".
        let data_bytes: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                InputEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        assert!(
            !data_bytes.is_empty(),
            "pending tail bytes must be emitted as a Data event on close (data loss fix)"
        );
        assert!(
            data_bytes
                .windows(b"no-newline-tail".len())
                .any(|w| w == b"no-newline-tail"),
            "emitted Data must contain the pending tail bytes; got: {:?}",
            String::from_utf8_lossy(&data_bytes),
        );

        // The flushed tail must be followed by EndOfFile for the same source.
        let has_eof = events
            .iter()
            .any(|e| matches!(e, InputEvent::EndOfFile { source_id } if source_id.is_some()));
        assert!(
            has_eof,
            "EndOfFile must still be emitted after the pending Data"
        );
    }

    #[test]
    fn incomplete_octet_tail_is_not_flushed_on_close_after_octet_mode() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Complete frame (`hello`) followed by an incomplete octet-counted tail.
            client.write_all(b"5 hello4 tes").unwrap();
            client.flush().unwrap();
        } // close connection

        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().unwrap();

        let data_bytes: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                InputEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let rendered = String::from_utf8_lossy(&data_bytes);
        assert!(
            rendered.contains("hello\n"),
            "complete octet frame should still be emitted; got: {rendered}"
        );
        assert!(
            !rendered.contains("tes"),
            "incomplete octet-counted tail must be dropped on close; got: {rendered}"
        );
    }

    #[test]
    fn overflowing_octet_prefix_is_treated_as_incomplete_tail() {
        let buf = format!("{} ", usize::MAX).into_bytes();
        assert!(
            has_incomplete_octet_frame_tail(&buf),
            "overflowing octet prefix must be treated as incomplete to avoid flushing malformed tails"
        );
    }

    #[test]
    fn truncated_digits_only_octet_prefix_is_treated_as_incomplete_tail() {
        assert!(
            has_incomplete_octet_frame_tail(b"12"),
            "truncated octet-count prefix must be treated as incomplete to avoid flushing malformed tails"
        );
    }

    /// Two concurrent TCP connections must receive distinct `SourceId` values
    /// so that `FramedInput` can track per-connection remainders independently.
    #[test]
    fn distinct_source_ids_per_connection() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let mut client_a = StdTcpStream::connect(addr).unwrap();
        let mut client_b = StdTcpStream::connect(addr).unwrap();

        client_a.write_all(b"from_a\n").unwrap();
        client_b.write_all(b"from_b\n").unwrap();
        client_a.flush().unwrap();
        client_b.flush().unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        // Collect all source_ids from data events.
        let source_ids: Vec<SourceId> = events
            .iter()
            .filter_map(|e| match e {
                InputEvent::Data { source_id, .. } => *source_id,
                _ => None,
            })
            .collect();

        assert!(
            source_ids.len() >= 2,
            "expected at least 2 data events (one per connection), got {}",
            source_ids.len()
        );

        // All source_ids must be distinct.
        let unique: std::collections::HashSet<u64> = source_ids.iter().map(|s| s.0).collect();
        assert_eq!(
            unique.len(),
            source_ids.len(),
            "each TCP connection must have a distinct SourceId"
        );
    }

    #[test]
    fn stop_predicate_matches_per_client_bounded_policy() {
        assert!(!should_stop_client_read(0, 0));
        assert!(should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, 0));
        assert!(should_stop_client_read(0, MAX_READS_PER_CLIENT_PER_POLL));
    }

    proptest! {
        #[test]
        fn stop_predicate_equivalent_to_limit_expression(
            bytes_read in 0usize..(2 * MAX_BYTES_PER_CLIENT_PER_POLL),
            reads_done in 0usize..(2 * MAX_READS_PER_CLIENT_PER_POLL),
        ) {
            let expected = bytes_read >= MAX_BYTES_PER_CLIENT_PER_POLL
                || reads_done >= MAX_READS_PER_CLIENT_PER_POLL;
            prop_assert_eq!(should_stop_client_read(bytes_read, reads_done), expected);
        }
    }

    #[test]
    fn noisy_client_is_bounded_and_quiet_client_still_progresses() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let mut noisy = StdTcpStream::connect(addr).unwrap();
        let mut quiet = StdTcpStream::connect(addr).unwrap();

        for _ in 0..10 {
            let _ = input.poll().unwrap();
            if input.client_count() >= 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            input.client_count(),
            2,
            "both clients must be accepted before write workload"
        );

        let noisy_payload = b"noisy-line\n".repeat((MAX_BYTES_PER_CLIENT_PER_POLL / 11) + 128);
        noisy.write_all(&noisy_payload).unwrap();
        noisy.flush().unwrap();

        let quiet_payload = b"quiet-line\n";
        quiet.write_all(quiet_payload).unwrap();
        quiet.flush().unwrap();

        std::thread::sleep(Duration::from_millis(75));

        let events = input.poll().unwrap();
        let mut bytes_by_source: std::collections::HashMap<SourceId, usize> =
            std::collections::HashMap::new();
        let mut saw_quiet = false;

        for event in events {
            if let InputEvent::Data {
                bytes, source_id, ..
            } = event
                && let Some(source_id) = source_id
            {
                if bytes
                    .windows(quiet_payload.len())
                    .any(|w| w == quiet_payload)
                {
                    saw_quiet = true;
                }
                *bytes_by_source.entry(source_id).or_insert(0) += bytes.len();
            }
        }

        assert!(saw_quiet, "quiet client should make progress in same poll");
        assert!(
            bytes_by_source.len() >= 2,
            "expected data from both quiet and noisy connections"
        );
        let max_forwarded = bytes_by_source.values().copied().max().unwrap_or(0);
        assert!(
            max_forwarded > quiet_payload.len(),
            "a larger noisy payload should also be forwarded"
        );
        assert!(
            max_forwarded <= MAX_BYTES_PER_CLIENT_PER_POLL,
            "a client's forwarded payload should respect per-poll bounded reads"
        );
    }
