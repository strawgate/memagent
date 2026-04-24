    use super::*;
    use proptest::prelude::*;
    use rcgen::{
        BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose, generate_simple_self_signed,
    };
    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
    use std::fs;
    use std::io::Write;
    use std::net::TcpStream as StdTcpStream;
    use tempfile::tempdir;

    fn tcp_input_with_client() -> (TcpInput, StdTcpStream) {
        let input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .expect("test input should bind");
        let addr = input.local_addr().expect("input should expose local addr");
        let client = StdTcpStream::connect(addr).expect("client should connect");
        (input, client)
    }

    fn poll_tcp_bytes_until(
        input: &mut TcpInput,
        mut is_complete: impl FnMut(&[u8]) -> bool,
    ) -> Vec<u8> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut joined = Vec::new();
        while Instant::now() < deadline {
            for event in input.poll().expect("tcp poll should succeed") {
                if let InputEvent::Data { bytes, .. } = event {
                    joined.extend_from_slice(&bytes);
                }
            }
            if is_complete(&joined) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        joined
    }

    fn write_tls_files(cert_pem: &str, key_pem: &str) -> (tempfile::TempDir, String, String) {
        let dir = tempdir().expect("temp dir should be created");
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        fs::write(&cert_path, cert_pem).expect("cert file should be written");
        fs::write(&key_path, key_pem).expect("key file should be written");
        (
            dir,
            cert_path.to_string_lossy().to_string(),
            key_path.to_string_lossy().to_string(),
        )
    }

    fn write_mtls_files(
        server_cert_pem: &str,
        server_key_pem: &str,
        ca_cert_pem: &str,
    ) -> (tempfile::TempDir, String, String, String) {
        let (dir, cert_file, key_file) = write_tls_files(server_cert_pem, server_key_pem);
        let ca_path = dir.path().join("client-ca.crt");
        fs::write(&ca_path, ca_cert_pem).expect("CA file should be written");
        (
            dir,
            cert_file,
            key_file,
            ca_path.to_string_lossy().to_string(),
        )
    }

    fn make_tls_options(cert_file: String, key_file: String) -> TcpInputOptions {
        TcpInputOptions {
            tls: Some(TcpInputTlsOptions {
                cert_file,
                key_file,
                client_ca_file: None,
                require_client_auth: false,
            }),
            ..Default::default()
        }
    }

    fn make_mtls_options(cert_file: String, key_file: String, ca_file: String) -> TcpInputOptions {
        TcpInputOptions {
            tls: Some(TcpInputTlsOptions {
                cert_file,
                key_file,
                client_ca_file: Some(ca_file),
                require_client_auth: true,
            }),
            ..Default::default()
        }
    }

    fn make_ca() -> (Certificate, Issuer<'static, KeyPair>) {
        let mut params =
            CertificateParams::new(Vec::<String>::new()).expect("empty SAN should be valid");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages.push(KeyUsagePurpose::KeyCertSign);
        params.key_usages.push(KeyUsagePurpose::DigitalSignature);

        let key_pair = KeyPair::generate().expect("CA key should generate");
        let cert = params
            .self_signed(&key_pair)
            .expect("CA cert should self-sign");
        (cert, Issuer::new(params, key_pair))
    }

    fn make_signed_cert(
        name: &str,
        eku: ExtendedKeyUsagePurpose,
        issuer: &Issuer<'static, KeyPair>,
    ) -> (Certificate, String) {
        let mut params = CertificateParams::new(vec![name.to_string()])
            .expect("certificate SAN should be valid");
        params.key_usages.push(KeyUsagePurpose::DigitalSignature);
        params.extended_key_usages.push(eku);
        let key_pair = KeyPair::generate().expect("leaf key should generate");
        let cert = params
            .signed_by(&key_pair, issuer)
            .expect("leaf cert should be signed by CA");
        (cert, key_pair.serialize_pem())
    }

    fn client_config(
        server_ca: &Certificate,
        client_cert: Option<(&Certificate, &str)>,
    ) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        roots
            .add(server_ca.der().clone())
            .expect("generated CA should load as trust anchor");
        let builder = ClientConfig::builder().with_root_certificates(roots);
        match client_cert {
            Some((cert, key_pem)) => builder
                .with_client_auth_cert(
                    vec![cert.der().clone()],
                    PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
                        .expect("client key PEM should parse"),
                )
                .expect("client cert/key should match"),
            None => builder.with_no_client_auth(),
        }
    }

    fn poll_for_data_events(input: &mut TcpInput, rounds: usize) -> Vec<Vec<u8>> {
        let mut records = Vec::new();
        for _ in 0..rounds {
            let events = input.poll().expect("tcp poll should succeed");
            for event in events {
                if let InputEvent::Data { bytes, .. } = event {
                    records.push(bytes.to_vec());
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        records
    }

    #[test]
    fn tls_listener_accepts_tls_client_data() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let cert_pem = certified.cert.pem();
        let key_pem = certified.signing_key.serialize_pem();
        let (_tmp, cert_file, key_file) = write_tls_files(&cert_pem, &key_pem);

        let stats = std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new());
        let mut input = TcpInput::with_options(
            "tls-test",
            "127.0.0.1:0",
            make_tls_options(cert_file, key_file),
            stats,
        )
        .expect("tls listener should start");
        let addr = input.local_addr().expect("tls listener local addr");

        let mut roots = RootCertStore::empty();
        let cert_der = certified.cert.der().clone();
        roots
            .add(cert_der)
            .expect("generated cert should load as trust anchor");
        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let conn = ClientConnection::new(
            std::sync::Arc::new(client_config),
            ServerName::try_from("localhost").expect("valid SNI"),
        )
        .expect("client connection should initialize");
        let tcp = StdTcpStream::connect(addr).expect("client should connect");
        let writer = std::thread::spawn(move || {
            let mut tls = StreamOwned::new(conn, tcp);
            tls.write_all(b"{\"msg\":\"tls\"}\n")
                .expect("tls write should succeed");
            tls.flush().expect("tls flush should succeed");
        });

        let joined = poll_tcp_bytes_until(&mut input, |bytes| bytes == b"{\"msg\":\"tls\"}\n");
        writer
            .join()
            .expect("tls client writer thread should complete");
        assert_eq!(joined, b"{\"msg\":\"tls\"}\n");
    }

    #[test]
    fn mtls_listener_accepts_trusted_client_certificate() {
        let (ca_cert, ca_issuer) = make_ca();
        let (server_cert, server_key_pem) =
            make_signed_cert("localhost", ExtendedKeyUsagePurpose::ServerAuth, &ca_issuer);
        let (client_cert, client_key_pem) =
            make_signed_cert("client", ExtendedKeyUsagePurpose::ClientAuth, &ca_issuer);
        let (_tmp, cert_file, key_file, ca_file) =
            write_mtls_files(&server_cert.pem(), &server_key_pem, &ca_cert.pem());

        let stats = std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new());
        let mut input = TcpInput::with_options(
            "mtls-test",
            "127.0.0.1:0",
            make_mtls_options(cert_file, key_file, ca_file),
            stats,
        )
        .expect("mTLS listener should start");
        let addr = input.local_addr().expect("mTLS listener local addr");

        let conn = ClientConnection::new(
            std::sync::Arc::new(client_config(
                &ca_cert,
                Some((&client_cert, &client_key_pem)),
            )),
            ServerName::try_from("localhost").expect("valid SNI"),
        )
        .expect("client connection should initialize");
        let tcp = StdTcpStream::connect(addr).expect("client should connect");
        let writer = std::thread::spawn(move || {
            let mut tls = StreamOwned::new(conn, tcp);
            tls.write_all(b"{\"msg\":\"mtls\"}\n")?;
            tls.flush()
        });

        let joined = poll_tcp_bytes_until(&mut input, |bytes| bytes == b"{\"msg\":\"mtls\"}\n");
        writer
            .join()
            .expect("mTLS client writer thread should complete")
            .expect("mTLS write should succeed");
        assert_eq!(joined, b"{\"msg\":\"mtls\"}\n");
    }

    #[test]
    fn mtls_listener_rejects_missing_client_certificate() {
        let (ca_cert, ca_issuer) = make_ca();
        let (server_cert, server_key_pem) =
            make_signed_cert("localhost", ExtendedKeyUsagePurpose::ServerAuth, &ca_issuer);
        let (_tmp, cert_file, key_file, ca_file) =
            write_mtls_files(&server_cert.pem(), &server_key_pem, &ca_cert.pem());

        let stats = std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new());
        let mut input = TcpInput::with_options(
            "mtls-test",
            "127.0.0.1:0",
            make_mtls_options(cert_file, key_file, ca_file),
            stats,
        )
        .expect("mTLS listener should start");
        let addr = input.local_addr().expect("mTLS listener local addr");

        let conn = ClientConnection::new(
            std::sync::Arc::new(client_config(&ca_cert, None)),
            ServerName::try_from("localhost").expect("valid SNI"),
        )
        .expect("client connection should initialize");
        let tcp = StdTcpStream::connect(addr).expect("client should connect");
        let writer = std::thread::spawn(move || {
            let mut tls = StreamOwned::new(conn, tcp);
            let _ = tls.write_all(b"{\"msg\":\"missing-cert\"}\n");
            let _ = tls.flush();
        });

        let data_events = poll_for_data_events(&mut input, 50);
        writer
            .join()
            .expect("mTLS client writer thread should complete");
        assert!(
            data_events.is_empty(),
            "mTLS listener must not emit records when the client omits a certificate"
        );
    }

    #[test]
    fn mtls_listener_rejects_untrusted_client_certificate() {
        let (ca_cert, ca_issuer) = make_ca();
        let (_untrusted_ca_cert, untrusted_ca_issuer) = make_ca();
        let (server_cert, server_key_pem) =
            make_signed_cert("localhost", ExtendedKeyUsagePurpose::ServerAuth, &ca_issuer);
        let (client_cert, client_key_pem) = make_signed_cert(
            "client",
            ExtendedKeyUsagePurpose::ClientAuth,
            &untrusted_ca_issuer,
        );
        let (_tmp, cert_file, key_file, ca_file) =
            write_mtls_files(&server_cert.pem(), &server_key_pem, &ca_cert.pem());

        let stats = std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new());
        let mut input = TcpInput::with_options(
            "mtls-test",
            "127.0.0.1:0",
            make_mtls_options(cert_file, key_file, ca_file),
            stats,
        )
        .expect("mTLS listener should start");
        let addr = input.local_addr().expect("mTLS listener local addr");

        let conn = ClientConnection::new(
            std::sync::Arc::new(client_config(
                &ca_cert,
                Some((&client_cert, &client_key_pem)),
            )),
            ServerName::try_from("localhost").expect("valid SNI"),
        )
        .expect("client connection should initialize");
        let tcp = StdTcpStream::connect(addr).expect("client should connect");
        let writer = std::thread::spawn(move || {
            let mut tls = StreamOwned::new(conn, tcp);
            let _ = tls.write_all(b"{\"msg\":\"untrusted\"}\n");
            let _ = tls.flush();
        });

        let data_events = poll_for_data_events(&mut input, 50);
        writer
            .join()
            .expect("mTLS client writer thread should complete");
        assert!(
            data_events.is_empty(),
            "mTLS listener must not emit records for untrusted client certificates"
        );
    }

    #[test]
    fn tls_listener_rejects_plaintext_without_emitting_records() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let cert_pem = certified.cert.pem();
        let key_pem = certified.signing_key.serialize_pem();
        let (_tmp, cert_file, key_file) = write_tls_files(&cert_pem, &key_pem);

        let stats = std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new());
        let mut input = TcpInput::with_options(
            "tls-test",
            "127.0.0.1:0",
            make_tls_options(cert_file, key_file),
            stats,
        )
        .expect("tls listener should start");
        let addr = input.local_addr().expect("tls listener local addr");

        let mut plain = StdTcpStream::connect(addr).expect("plaintext client should connect");
        plain
            .write_all(b"{\"msg\":\"plaintext\"}\n")
            .expect("plaintext write should succeed");
        plain.flush().expect("plaintext flush should succeed");

        let data_events = poll_for_data_events(&mut input, 25);
        assert!(
            data_events.is_empty(),
            "TLS listener must not emit records for plaintext clients"
        );
    }

    #[test]
    fn tls_listener_fails_when_cert_file_missing() {
        let dir = tempdir().expect("temp dir should be created");
        let key_path = dir.path().join("server.key");
        fs::write(&key_path, "not-a-key").expect("key file should be written");
        let cert_path = dir.path().join("missing.crt");

        let err = match TcpInput::with_options(
            "tls-test",
            "127.0.0.1:0",
            make_tls_options(
                cert_path.to_string_lossy().to_string(),
                key_path.to_string_lossy().to_string(),
            ),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("missing cert file must fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("tcp.tls.cert_file"),
            "error should identify tcp.tls.cert_file: {err}"
        );
    }

    #[test]
    fn tls_listener_fails_when_key_file_missing() {
        let certified = generate_simple_self_signed(vec!["localhost".into()])
            .expect("test cert generation should succeed");
        let dir = tempdir().expect("temp dir should be created");
        let cert_path = dir.path().join("server.crt");
        fs::write(&cert_path, certified.cert.pem()).expect("cert file should be written");
        let key_path = dir.path().join("missing.key");

        let err = match TcpInput::with_options(
            "tls-test",
            "127.0.0.1:0",
            make_tls_options(
                cert_path.to_string_lossy().to_string(),
                key_path.to_string_lossy().to_string(),
            ),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("missing key file must fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("tcp.tls.key_file"),
            "error should identify tcp.tls.key_file: {err}"
        );
    }

    #[test]
    fn tls_listener_fails_on_malformed_cert_pem() {
        let (_tmp, cert_file, key_file) = write_tls_files(
            "not-a-certificate",
            "-----BEGIN PRIVATE KEY-----\ninvalid\n-----END PRIVATE KEY-----\n",
        );

        let err = match TcpInput::with_options(
            "tls-test",
            "127.0.0.1:0",
            make_tls_options(cert_file, key_file),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        ) {
            Ok(_) => panic!("malformed cert must fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("tcp.tls.cert_file"),
            "error should identify tcp.tls.cert_file: {err}"
        );
    }
