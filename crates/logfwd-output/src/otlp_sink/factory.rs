//! Factory for creating [`OtlpSink`] instances in the output worker pool.

use std::io;
use std::sync::Arc;

use logfwd_config::OtlpProtocol;
use logfwd_types::diagnostics::ComponentStats;

use super::types::OtlpSink;
use crate::Compression;

// ---------------------------------------------------------------------------
// OtlpSinkFactory
// ---------------------------------------------------------------------------

/// Creates [`OtlpSink`] instances for the output worker pool.
///
/// All workers share a single `reqwest::Client` (which is internally
/// `Arc`-wrapped) so they reuse the same connection pool, TLS sessions,
/// and DNS cache.
pub struct OtlpSinkFactory {
    name: String,
    endpoint: String,
    protocol: OtlpProtocol,
    compression: Compression,
    headers: Vec<(String, String)>,
    message_field: String,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

impl OtlpSinkFactory {
    /// Create a new factory.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        endpoint: String,
        protocol: OtlpProtocol,
        compression: Compression,
        headers: Vec<(String, String)>,
        message_field: String,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Ok(OtlpSinkFactory {
            name,
            endpoint,
            protocol,
            compression,
            headers,
            message_field,
            client,
            stats,
        })
    }
}

impl super::super::sink::SinkFactory for OtlpSinkFactory {
    fn create(&self) -> io::Result<Box<dyn super::super::sink::Sink>> {
        let sink = OtlpSink::new(
            self.name.clone(),
            self.endpoint.clone(),
            self.protocol,
            self.compression,
            self.headers.clone(),
            self.client.clone(),
            Arc::clone(&self.stats),
        )?
        .with_message_field(self.message_field.clone());
        Ok(Box::new(sink))
    }

    fn name(&self) -> &str {
        &self.name
    }
}
