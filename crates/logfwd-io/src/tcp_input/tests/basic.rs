    #[test]
    fn tls_listener_fails_on_malformed_key_pem() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let (_tmp, cert_file, key_file) = write_tls_files(&certified.cert.pem(), "not-a-key");

        let err = match TcpInput::with_options(
            "tls-test",
            "127.0.0.1:0",
            make_tls_options(cert_file, key_file),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("malformed key must fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("tcp.tls.key_file"),
            "error should identify tcp.tls.key_file: {err}"
        );
    }

    #[test]
    fn tls_listener_fails_on_cert_key_mismatch() {
        let cert_a = generate_simple_self_signed(vec!["localhost".into()])
            .expect("cert generation should succeed");
        let cert_b = generate_simple_self_signed(vec!["localhost".into()])
            .expect("cert generation should succeed");
        let (_tmp, cert_file, key_file) =
            write_tls_files(&cert_a.cert.pem(), &cert_b.signing_key.serialize_pem());

        let err = match TcpInput::with_options(
            "tls-test",
            "127.0.0.1:0",
            make_tls_options(cert_file, key_file),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("mismatched cert/key must fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("cert/key mismatch"),
            "error should mention cert/key mismatch: {err}"
        );
    }

    #[test]
    fn mtls_listener_fails_on_malformed_client_ca_pem() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let (tmp, cert_file, key_file) = write_tls_files(
            &certified.cert.pem(),
            &certified.signing_key.serialize_pem(),
        );
        let ca_path = tmp.path().join("client-ca.crt");
        fs::write(&ca_path, "not-a-ca").expect("client CA file should be written");

        let err = match TcpInput::with_options(
            "mtls-test",
            "127.0.0.1:0",
            make_mtls_options(cert_file, key_file, ca_path.to_string_lossy().to_string()),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("malformed client CA must fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("tcp.tls.client_ca_file"),
            "error should identify tcp.tls.client_ca_file: {err}"
        );
    }

    #[test]
    fn mtls_listener_requires_non_empty_client_ca_path() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let (_tmp, cert_file, key_file) = write_tls_files(
            &certified.cert.pem(),
            &certified.signing_key.serialize_pem(),
        );

        let err = match TcpInput::with_options(
            "mtls-test",
            "127.0.0.1:0",
            make_mtls_options(cert_file, key_file, "   ".to_string()),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("blank client CA path must fail startup"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("client_ca_file must not be empty"),
            "error should identify blank tcp.tls.client_ca_file: {err}"
        );
    }

    #[test]
    fn mtls_listener_rejects_blank_client_ca_when_auth_disabled() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let (_tmp, cert_file, key_file) = write_tls_files(
            &certified.cert.pem(),
            &certified.signing_key.serialize_pem(),
        );

        let err = match TcpInput::with_options(
            "mtls-test",
            "127.0.0.1:0",
            TcpInputOptions {
                tls: Some(TcpInputTlsOptions {
                    cert_file,
                    key_file,
                    client_ca_file: Some("   ".to_string()),
                    require_client_auth: false,
                }),
                ..Default::default()
            },
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("blank client CA path must fail startup"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("client_ca_file must not be empty"),
            "error should identify blank tcp.tls.client_ca_file: {err}"
        );
    }

    #[test]
    fn mtls_listener_rejects_client_ca_when_auth_disabled() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let (tmp, cert_file, key_file) = write_tls_files(
            &certified.cert.pem(),
            &certified.signing_key.serialize_pem(),
        );
        let ca_path = tmp.path().join("client-ca.crt");
        fs::write(&ca_path, certified.cert.pem()).expect("client CA file should be written");

        let err = match TcpInput::with_options(
            "mtls-test",
            "127.0.0.1:0",
            TcpInputOptions {
                tls: Some(TcpInputTlsOptions {
                    cert_file,
                    key_file,
                    client_ca_file: Some(ca_path.to_string_lossy().to_string()),
                    require_client_auth: false,
                }),
                ..Default::default()
            },
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("client CA without client auth must fail startup"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("client_ca_file requires tcp.tls.require_client_auth: true"),
            "error should identify disabled client auth: {err}"
        );
    }

    #[test]
    fn octet_frame_consumes_delimiter_split_across_reads() {
        let (mut input, mut client) = tcp_input_with_client();
        client.write_all(b"5 hello").unwrap();
        client.flush().unwrap();

        let first = poll_tcp_bytes_until(&mut input, |bytes| bytes == b"hello\n");
        assert_eq!(first, b"hello\n");
        assert!(
            input
                .clients
                .first()
                .is_some_and(|client| client.should_consume_octet_delimiter_newline),
            "frame ending at a read boundary should wait for an optional delimiter"
        );

        client.write_all(b"\n").unwrap();
        client.flush().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut next = Vec::new();
        let mut consumed_delimiter = false;
        while Instant::now() < deadline {
            for event in input.poll().expect("tcp poll should succeed") {
                if let InputEvent::Data { bytes, .. } = event {
                    next.extend_from_slice(&bytes);
                }
            }
            if input.clients.first().is_some_and(|client| {
                !client.should_consume_octet_delimiter_newline && client.pending.is_empty()
            }) {
                consumed_delimiter = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(consumed_delimiter, "split delimiter should be consumed");
        assert!(next.is_empty(), "delimiter must not emit an empty record");
    }

    #[test]
    fn receives_tcp_data() {
        let input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.listener.local_addr().unwrap();

        let mut client = StdTcpStream::connect(addr).unwrap();
        let msg1 = b"{\"msg\":\"hello\"}\n";
        let msg2 = b"{\"msg\":\"world\"}\n";
        client.write_all(msg1).unwrap();
        client.write_all(msg2).unwrap();
        client.flush().unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let mut input = input; // make mutable
        let events = input.poll().unwrap();

        // Should have accepted the connection and read data.
        assert_eq!(events.len(), 1);
        if let InputEvent::Data {
            bytes,
            source_id,
            accounted_bytes,
            ..
        } = &events[0]
        {
            let text = String::from_utf8_lossy(bytes);
            assert!(text.contains("hello"), "got: {text}");
            assert!(text.contains("world"), "got: {text}");
            assert!(source_id.is_some(), "TCP data must have a source_id");
            assert_eq!(
                *accounted_bytes,
                (msg1.len() + msg2.len()) as u64,
                "accounted_bytes must equal the raw bytes sent over the socket"
            );
        }
    }

    #[test]
    fn handles_disconnect() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.listener.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            client.write_all(b"line1\n").unwrap();
        } // client drops here -> connection closed

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        // After the clean disconnect we expect both a Data event and an
        // EndOfFile event.  The EndOfFile signals FramedInput to flush any
        // partial-line remainder held for this SourceId.
        let data_count = events
            .iter()
            .filter(|e| matches!(e, InputEvent::Data { .. }))
            .count();
        let eof_count = events
            .iter()
            .filter(|e| matches!(e, InputEvent::EndOfFile { .. }))
            .count();
        assert_eq!(data_count, 1, "expected 1 data event");
        assert_eq!(eof_count, 1, "expected 1 EndOfFile event on disconnect");

        // Second poll should clean up the closed connection.
        let events = input.poll().unwrap();
        assert!(events.is_empty());
        assert!(input.clients.is_empty());
    }

    #[test]
    fn tcp_health_recovers_after_clean_poll() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        input.health = ComponentHealth::Degraded;

        let events = input.poll().unwrap();
        assert!(events.is_empty());
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    /// A TCP client that sends a partial line (no trailing newline) and then
    /// disconnects must cause an EndOfFile event so that FramedInput can flush
    /// the partial remainder — fixes #804 / #580.
    #[test]
    fn tcp_partial_line_on_disconnect_emits_eof() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Intentionally no trailing newline — this is the partial line.
            client.write_all(b"partial line without newline").unwrap();
            client.flush().unwrap();
        } // client drops here -> EOF

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        let has_eof = events
            .iter()
            .any(|e| matches!(e, InputEvent::EndOfFile { source_id } if source_id.is_some()));

        assert!(
            has_eof,
            "should emit EndOfFile on disconnect so FramedInput can flush the partial line"
        );

        // EndOfFile source_id must match any source id observed in this poll.
        let data_sid = events.iter().find_map(|e| match e {
            InputEvent::Data { source_id, .. } => *source_id,
            _ => None,
        });
        let eof_sid = events.iter().find_map(|e| {
            if let InputEvent::EndOfFile { source_id } = e {
                *source_id
            } else {
                None
            }
        });
        if let Some(data_sid) = data_sid {
            assert_eq!(
                Some(data_sid),
                eof_sid,
                "Data and EndOfFile must carry the same SourceId"
            );
        } else {
            assert!(eof_sid.is_some(), "EndOfFile should carry SourceId");
        }
    }

    #[test]
    fn tcp_idle_timeout() {
        // Use a very short idle timeout so the test runs fast.
        let mut input = TcpInput::with_idle_timeout(
            "test",
            "127.0.0.1:0",
            Duration::from_millis(200),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        // Connect but send nothing.
        let _client = StdTcpStream::connect(addr).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        // First poll: accept the connection.
        let _ = input.poll().unwrap();
        assert_eq!(input.client_count(), 1, "should have 1 client after accept");

        // Wait longer than the idle timeout.
        std::thread::sleep(Duration::from_millis(300));

        // Next poll should evict the idle connection.
        let _ = input.poll().unwrap();
        assert_eq!(
            input.client_count(),
            0,
            "idle client should have been disconnected"
        );
    }

    #[test]
    fn test_tcp_custom_options() {
        let mut options = TcpInputOptions::default();
        options.max_clients = 2;
        options.connection_timeout_ms = 1000;
        options.read_timeout_ms = Some(500);

        let input = TcpInput::with_options(
            "test_custom",
            "127.0.0.1:0",
            options,
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();

        assert_eq!(input.max_clients, 2);
        assert_eq!(input.idle_timeout.as_millis(), 1000);
        assert_eq!(input.read_timeout.unwrap().as_millis(), 500);
    }
