//! Factory for creating [`ElasticsearchSink`] instances in the output worker pool.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use ffwd_config::ElasticsearchRequestMode;
use ffwd_types::diagnostics::ComponentStats;

use super::types::{ElasticsearchConfig, ElasticsearchSink};

// ---------------------------------------------------------------------------
// ElasticsearchSinkFactory
// ---------------------------------------------------------------------------

/// Creates `ElasticsearchSink` instances for the output worker pool.
///
/// All workers share a single `reqwest::Client` (which is internally
/// `Arc`-wrapped) so they reuse the same connection pool, TLS sessions,
/// and DNS cache.
pub struct ElasticsearchSinkFactory {
    name: String,
    pub(crate) config: Arc<ElasticsearchConfig>,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

impl ElasticsearchSinkFactory {
    /// Create a new factory.
    ///
    /// - `endpoint`: Base URL (e.g. `http://localhost:9200`)
    /// - `index`: Target index name (e.g. `logs`)
    /// - `headers`: Authentication headers (e.g. `Authorization: ApiKey …`)
    /// - `compress`: Enable gzip compression of the request body
    /// - `request_mode`: Buffered or experimental streaming bulk body mode
    pub fn new(
        name: String,
        endpoint: String,
        index: String,
        headers: Vec<(String, String)>,
        compress: bool,
        request_mode: ElasticsearchRequestMode,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_client(
            name,
            endpoint,
            index,
            headers,
            compress,
            request_mode,
            reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(64),
            stats,
        )
    }

    #[allow(clippy::too_many_arguments)]
    /// Creates an Elasticsearch sink factory with a caller-provided HTTP client builder.
    ///
    /// The caller owns all cross-cutting HTTP client policy on `client_builder`, including
    /// request timeout, connection-pool sizing, TLS roots, mTLS identity, and certificate
    /// verification behavior. This constructor validates Elasticsearch-specific request
    /// mode and compression compatibility, then builds and reuses one `reqwest::Client`
    /// for every sink created by the factory.
    pub fn new_with_client(
        name: String,
        endpoint: String,
        index: String,
        headers: Vec<(String, String)>,
        compress: bool,
        request_mode: ElasticsearchRequestMode,
        client_builder: reqwest::ClientBuilder,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        if compress && request_mode == ElasticsearchRequestMode::Streaming {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "streaming Elasticsearch request mode does not support gzip compression yet",
            ));
        }

        let parsed_headers = headers
            .into_iter()
            .map(|(k, v)| {
                let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                let value = reqwest::header::HeaderValue::from_str(&v)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                Ok((name, value))
            })
            .collect::<io::Result<Vec<_>>>()?;

        let client = client_builder.build().map_err(io::Error::other)?;

        let endpoint = endpoint.trim_end_matches('/').to_string();
        let bulk_url =
            format!("{endpoint}/_bulk?filter_path=errors,took,items.*.error,items.*.status");

        // Pre-compute the action line bytes once so serialize_batch doesn't have
        // to allocate a String on every call.
        let escaped_index = serde_json::to_string(&index).map_err(io::Error::other)?;
        let action_line = format!("{{\"index\":{{\"_index\":{escaped_index}}}}}\n");

        Ok(ElasticsearchSinkFactory {
            name,
            config: Arc::new(ElasticsearchConfig {
                endpoint,
                headers: parsed_headers,
                compress,
                request_mode,
                max_bulk_bytes: 5 * 1024 * 1024, // 5 MiB default
                stream_chunk_bytes: 64 * 1024,
                bulk_url,
                action_bytes: action_line.into_bytes().into_boxed_slice(),
            }),
            client,
            stats,
        })
    }
}

impl crate::sink::SinkFactory for ElasticsearchSinkFactory {
    fn create(&self) -> io::Result<Box<dyn crate::sink::Sink>> {
        Ok(Box::new(ElasticsearchSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            self.client.clone(), // reqwest::Client is Arc-wrapped, clone is cheap
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl ElasticsearchSinkFactory {
    /// Create a concrete `ElasticsearchSink` without boxing it.
    ///
    /// Intended for benchmarks and tests that need access to
    /// `serialize_batch` or `serialized_len` directly.
    pub fn create_sink(&self) -> ElasticsearchSink {
        ElasticsearchSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            self.client.clone(),
            Arc::clone(&self.stats),
        )
    }
}
