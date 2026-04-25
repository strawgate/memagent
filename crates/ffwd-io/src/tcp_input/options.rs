#[derive(Debug, Clone)]
pub struct TcpInputOptions {
    /// Maximum number of concurrent connections. Defaults to unlimited.
    pub max_clients: Option<usize>,
    /// Connection idle timeout in milliseconds. Defaults to 60000 (60s).
    pub connection_timeout_ms: u64,
    /// Connection read timeout in milliseconds (disconnects if a read yields incomplete data and the next read takes too long). Defaults to no timeout.
    pub read_timeout_ms: Option<u64>,
    /// Optional TLS server configuration for inbound TCP connections.
    pub tls: Option<TcpInputTlsOptions>,
}

impl Default for TcpInputOptions {
    fn default() -> Self {
        Self {
            max_clients: None,
            connection_timeout_ms: DEFAULT_IDLE_TIMEOUT.as_millis() as u64,
            read_timeout_ms: None,
            tls: None,
        }
    }
}

/// TLS configuration for the TCP input listener.
///
/// When present, the listener requires clients to connect via TLS.
/// Both `cert_file` and `key_file` must point to valid PEM-encoded files.
#[derive(Debug, Clone)]
pub struct TcpInputTlsOptions {
    /// Path to a PEM-encoded certificate chain file for the server identity.
    pub cert_file: String,
    /// Path to a PEM-encoded private key file matching `cert_file`.
    pub key_file: String,
    /// Path to a PEM-encoded CA bundle used to verify client certificates.
    pub client_ca_file: Option<String>,
    /// Require clients to present a certificate signed by `client_ca_file`.
    pub require_client_auth: bool,
}
