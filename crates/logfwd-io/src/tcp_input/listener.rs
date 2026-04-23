/// TCP input that accepts connections and reads newline-delimited data.
///
/// Each connection is assigned a unique `SourceId` so downstream components
/// can track per-connection state (e.g., partial-line remainders).
pub struct TcpInput {
    name: String,
    listener: TcpListener,
    listener_mode: ListenerMode,
    clients: Vec<Client>,
    buf: Vec<u8>,
    idle_timeout: Duration,
    read_timeout: Option<Duration>,
    max_clients: usize,
    /// Total connections accepted since creation (never decreases).
    connections_accepted: u64,
    /// Rate-limit state for max_clients warnings.
    last_max_clients_warning: Option<Instant>,
    /// Monotonic counter for generating per-connection `SourceId` values.
    next_connection_seq: u64,
    /// Coarse control-plane health derived from the most recent poll cycle.
    health: ComponentHealth,
    stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
}

impl TcpInput {
    fn ensure_tls_provider_installed() {
        TLS_PROVIDER_INIT.call_once(|| {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        });
    }

    fn load_tls_server_config(
        opts: &TcpInputTlsOptions,
    ) -> io::Result<std::sync::Arc<ServerConfig>> {
        Self::ensure_tls_provider_installed();

        let certs = CertificateDer::pem_file_iter(&opts.cert_file)
            .map_err(|e| {
                io::Error::new(
                    pem_error_kind(&e),
                    format!(
                        "tcp.tls.cert_file '{}' could not be opened or parsed as PEM: {e}",
                        opts.cert_file
                    ),
                )
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "tcp.tls.cert_file '{}' could not be parsed as PEM: {e}",
                        opts.cert_file
                    ),
                )
            })?;
        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tcp.tls.cert_file '{}' contained no certificates",
                    opts.cert_file
                ),
            ));
        }

        let key = PrivateKeyDer::from_pem_file(&opts.key_file).map_err(|e| {
            io::Error::new(
                pem_error_kind(&e),
                format!(
                    "tcp.tls.key_file '{}' could not be opened or parsed as PEM: {e}",
                    opts.key_file
                ),
            )
        })?;
        let client_ca_file = match opts.client_ca_file.as_deref() {
            Some(path) => {
                let path = path.trim();
                if path.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "tcp.tls.client_ca_file must not be empty",
                    ));
                }
                Some(path)
            }
            None => None,
        };
        if client_ca_file.is_some() && !opts.require_client_auth {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tcp.tls.client_ca_file requires tcp.tls.require_client_auth: true",
            ));
        }
        let builder = ServerConfig::builder();
        let builder = if opts.require_client_auth {
            let client_ca_file = client_ca_file.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "tcp.tls.require_client_auth requires tcp.tls.client_ca_file",
                )
            })?;
            let client_ca_certs = CertificateDer::pem_file_iter(client_ca_file)
                .map_err(|e| {
                    io::Error::new(
                        pem_error_kind(&e),
                        format!(
                            "tcp.tls.client_ca_file '{client_ca_file}' could not be opened or parsed as PEM: {e}"
                        ),
                    )
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "tcp.tls.client_ca_file '{client_ca_file}' could not be parsed as PEM: {e}"
                        ),
                    )
                })?;
            if client_ca_certs.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("tcp.tls.client_ca_file '{client_ca_file}' contained no certificates"),
                ));
            }
            let mut roots = RootCertStore::empty();
            for cert in client_ca_certs {
                roots.add(cert).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "tcp.tls.client_ca_file '{client_ca_file}' contained an invalid CA certificate: {e}"
                        ),
                    )
                })?;
            }
            let verifier = WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
                .build()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "tcp.tls.client_ca_file '{client_ca_file}' could not build a client certificate verifier: {e}"
                        ),
                    )
                })?;
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        let cfg = builder.with_single_cert(certs, key).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tcp.tls cert/key mismatch between '{}' and '{}': {e}",
                    opts.cert_file, opts.key_file
                ),
            )
        })?;
        Ok(std::sync::Arc::new(cfg))
    }

    /// Bind to `addr` (e.g. "0.0.0.0:5140") with the given options.
    pub fn with_options(
        name: impl Into<String>,
        addr: &str,
        options: TcpInputOptions,
        stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        let listener_mode = match &options.tls {
            Some(tls) => ListenerMode::Tls(Self::load_tls_server_config(tls)?),
            None => ListenerMode::Plain,
        };
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            name: name.into(),
            listener,
            listener_mode,
            clients: Vec::new(),
            buf: vec![0u8; READ_BUF_SIZE],
            idle_timeout: Duration::from_millis(options.connection_timeout_ms),
            read_timeout: options.read_timeout_ms.map(Duration::from_millis),
            max_clients: options.max_clients,
            connections_accepted: 0,
            last_max_clients_warning: None,
            next_connection_seq: 0,
            health: ComponentHealth::Healthy,
            stats,
        })
    }

    /// Bind to `addr` (e.g. "0.0.0.0:5140") with the default options.
    pub fn new(
        name: impl Into<String>,
        addr: &str,
        stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        Self::with_options(name, addr, TcpInputOptions::default(), stats)
    }

    /// Bind to `addr` with a custom idle timeout. (Legacy, use with_options())
    pub fn with_idle_timeout(
        name: impl Into<String>,
        addr: &str,
        idle_timeout: Duration,
        stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        let options = TcpInputOptions {
            connection_timeout_ms: idle_timeout.as_millis() as u64,
            ..Default::default()
        };
        Self::with_options(name, addr, options, stats)
    }

    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Returns the number of currently tracked client connections.
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Returns the total number of connections accepted since creation.
    /// Monotonically increasing — useful for tests that need to verify a
    /// connection was accepted even if it was immediately disconnected.
    #[cfg(test)]
    pub fn connections_accepted(&self) -> u64 {
        self.connections_accepted
    }
}
