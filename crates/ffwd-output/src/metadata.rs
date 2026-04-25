use std::sync::Arc;

use ffwd_config::AuthConfig;

/// Metadata about the batch for output serialization.
#[derive(Clone)]
pub struct BatchMetadata {
    /// Resource attributes (k8s pod name, namespace, etc.) — Arc so cloning is cheap.
    pub resource_attrs: Arc<[(String, String)]>,
    /// Observed timestamp in nanoseconds.
    pub observed_time_ns: u64,
}

/// Compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Zstd,
    Gzip,
    None,
}

/// Build a flat list of HTTP headers from an [`AuthConfig`].
///
/// - `bearer_token` produces `Authorization: Bearer <token>`.
/// - `headers` entries are appended as-is.
pub(crate) fn build_auth_headers(auth: Option<&AuthConfig>) -> Vec<(String, String)> {
    let Some(auth) = auth else {
        return Vec::new();
    };
    let mut headers: Vec<(String, String)> = Vec::new();
    if let Some(token) = &auth.bearer_token {
        headers.push(("Authorization".to_string(), format!("Bearer {token}")));
    }
    for (k, v) in &auth.headers {
        headers.push((k.clone(), v.clone()));
    }
    headers
}
