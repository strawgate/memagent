//! AWS S3 HTTP client with SigV4 request signing.
//!
//! Supports both virtual-hosted style (AWS) and path-style (MinIO / other
//! S3-compatible endpoints).

use std::collections::BTreeMap;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// SHA-256 of an empty body — used for GET / HEAD requests.
const EMPTY_BODY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Max bytes of a response body to include in error messages.
pub(super) const ERROR_BODY_PREVIEW_LEN: usize = 1024;

/// Read up to [`ERROR_BODY_PREVIEW_LEN`] bytes from a response body.
///
/// Uses chunked reading to avoid buffering arbitrarily large error
/// responses into memory.
pub(super) async fn error_body_preview(resp: reqwest::Response) -> String {
    let mut buf = Vec::with_capacity(ERROR_BODY_PREVIEW_LEN);
    let mut stream = resp;
    let mut truncated = false;

    while buf.len() < ERROR_BODY_PREVIEW_LEN {
        match stream.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = ERROR_BODY_PREVIEW_LEN - buf.len();
                if chunk.len() >= remaining {
                    buf.extend_from_slice(&chunk[..remaining]);
                    truncated = chunk.len() > remaining;
                    break;
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // If we filled exactly to the limit, check if more data exists.
    if !truncated
        && buf.len() == ERROR_BODY_PREVIEW_LEN
        && let Ok(Some(_)) = stream.chunk().await
    {
        truncated = true;
    }

    let text = String::from_utf8_lossy(&buf);
    if truncated {
        format!("{text}... (truncated)")
    } else {
        text.into_owned()
    }
}

/// A single object returned by `ListObjectsV2`.
#[derive(Debug)]
pub struct S3Object {
    /// Object key (relative to bucket root).
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
}

/// Metadata returned by `HEAD` object.
#[derive(Debug)]
pub struct ObjectMeta {
    /// Object size in bytes.
    pub content_length: u64,
    /// `Content-Encoding` header value (e.g. `"gzip"`, `"zstd"`).
    pub content_encoding: Option<String>,
    /// `Content-Type` header value.
    pub content_type: Option<String>,
}

/// Lightweight S3 client with AWS SigV4 signing.
pub struct S3Client {
    client: reqwest::Client,
    region: String,
    bucket: String,
    /// Base URL without trailing slash (e.g. `"https://s3.us-east-1.amazonaws.com"` or
    /// `"http://localhost:9000"`).
    endpoint_base: String,
    /// `true` when using path-style addressing (MinIO / explicit endpoint).
    path_style: bool,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl S3Client {
    /// Create a new `S3Client`.
    ///
    /// When `endpoint` is `Some`, path-style addressing is used. Otherwise the
    /// standard AWS virtual-hosted style is used.
    pub fn new(
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        max_idle_connections: usize,
    ) -> io::Result<Self> {
        let (endpoint_base, path_style) = if let Some(ep) = endpoint {
            (ep.trim_end_matches('/').to_string(), true)
        } else if bucket.contains('.') {
            // Dotted bucket names (e.g. "logs.prod") break virtual-hosted
            // style because the wildcard TLS cert *.s3.region.amazonaws.com
            // does not cover multi-label subdomains. Use path-style instead.
            (format!("https://s3.{region}.amazonaws.com"), true)
        } else {
            (format!("https://s3.{region}.amazonaws.com"), false)
        };

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(max_idle_connections)
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(300))
            .use_rustls_tls()
            .build()
            .map_err(|e| io::Error::other(format!("S3 client build: {e}")))?;

        Ok(Self {
            client,
            region: region.to_string(),
            bucket: bucket.to_string(),
            endpoint_base,
            path_style,
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    // ── URL construction ───────────────────────────────────────────────

    fn host(&self) -> String {
        if self.path_style {
            self.endpoint_base
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .to_string()
        } else {
            format!("{}.s3.{}.amazonaws.com", self.bucket, self.region)
        }
    }

    /// Build the HTTP URL for an object operation.
    fn object_url(&self, key: &str) -> String {
        let encoded = url_encode_path(key);
        if self.path_style {
            format!("{}/{}/{}", self.endpoint_base, self.bucket, encoded)
        } else {
            format!(
                "https://{}.s3.{}.amazonaws.com/{}",
                self.bucket, self.region, encoded
            )
        }
    }

    /// Canonical URI for SigV4 signing of an object request.
    fn object_signing_path(&self, key: &str) -> String {
        let encoded = url_encode_path(key);
        if self.path_style {
            format!("/{}/{encoded}", self.bucket)
        } else {
            format!("/{encoded}")
        }
    }

    /// Build the HTTP URL for `ListObjectsV2`.
    fn list_url(&self, query: &str) -> String {
        if self.path_style {
            format!("{}/{}?{}", self.endpoint_base, self.bucket, query)
        } else {
            format!(
                "https://{}.s3.{}.amazonaws.com/?{}",
                self.bucket, self.region, query
            )
        }
    }

    /// Canonical URI for SigV4 signing of `ListObjectsV2`.
    fn list_signing_path() -> &'static str {
        "/"
    }

    // ── SigV4 ─────────────────────────────────────────────────────────

    fn sha256_hex(data: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(data);
        hex::encode(h.finalize())
    }

    fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        // HMAC accepts any key length.
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn signing_key(&self, date: &str, service: &str) -> Vec<u8> {
        let k_secret = format!("AWS4{}", self.secret_access_key);
        let k_date = Self::hmac_sha256(k_secret.as_bytes(), date.as_bytes());
        let k_region = Self::hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = Self::hmac_sha256(&k_region, service.as_bytes());
        Self::hmac_sha256(&k_service, b"aws4_request")
    }

    /// Build the `Authorization` header and return `(auth, x-amz-date)`.
    ///
    /// `path` must already be URI-encoded. `query` must already be in
    /// canonical form (sorted, URI-encoded). `extra_headers` are additional
    /// headers beyond `host`, `x-amz-date`, and `x-amz-content-sha256`.
    #[allow(clippy::too_many_arguments)]
    fn sigv4_auth(
        &self,
        method: &str,
        path: &str,
        query: &str,
        extra_headers: &BTreeMap<String, String>,
        body_sha256: &str,
        service: &str,
        host: &str,
    ) -> io::Result<(String, String)> {
        let (date, datetime) = utc_datetime_now()?;

        // Build the full set of canonical headers (always sign host,
        // x-amz-content-sha256, x-amz-date, and any extras).
        let mut headers: BTreeMap<String, String> = BTreeMap::new();
        headers.insert("host".to_string(), host.to_string());
        headers.insert("x-amz-content-sha256".to_string(), body_sha256.to_string());
        headers.insert("x-amz-date".to_string(), datetime.clone());
        if let Some(token) = &self.session_token {
            headers.insert("x-amz-security-token".to_string(), token.clone());
        }
        for (k, v) in extra_headers {
            headers.insert(k.to_lowercase(), v.trim().to_string());
        }

        let canonical_headers: String = headers.iter().fold(String::new(), |mut s, (k, v)| {
            use std::fmt::Write;
            let _ = writeln!(s, "{k}:{v}");
            s
        });
        let signed_headers: String = headers.keys().cloned().collect::<Vec<_>>().join(";");

        let canonical_request = format!(
            "{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{body_sha256}"
        );

        let credential_scope = format!("{date}/{}/{service}/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            datetime,
            credential_scope,
            Self::sha256_hex(canonical_request.as_bytes())
        );

        let signing_key = self.signing_key(&date, service);
        let signature = hex::encode(Self::hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key_id, credential_scope, signed_headers, signature
        );
        Ok((auth, datetime))
    }

    /// Assemble a signed `HeaderMap` for an HTTP request.
    #[allow(clippy::too_many_arguments)]
    fn signed_headers(
        &self,
        method: &str,
        signing_path: &str,
        query: &str,
        extra: &BTreeMap<String, String>,
        body: &[u8],
        service: &str,
        host: &str,
    ) -> io::Result<HeaderMap> {
        let body_sha256 = if body.is_empty() {
            EMPTY_BODY_SHA256.to_string()
        } else {
            Self::sha256_hex(body)
        };

        let (auth, datetime) = self.sigv4_auth(
            method,
            signing_path,
            query,
            extra,
            &body_sha256,
            service,
            host,
        )?;

        let mut map = HeaderMap::new();
        map.insert(
            reqwest::header::AUTHORIZATION,
            HeaderValue::from_str(&auth)
                .map_err(|e| io::Error::other(format!("auth header: {e}")))?,
        );
        map.insert(
            HeaderName::from_static("x-amz-date"),
            HeaderValue::from_str(&datetime)
                .map_err(|e| io::Error::other(format!("x-amz-date header: {e}")))?,
        );
        map.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            HeaderValue::from_str(&body_sha256)
                .map_err(|e| io::Error::other(format!("x-amz-content-sha256 header: {e}")))?,
        );
        if let Some(token) = &self.session_token {
            map.insert(
                HeaderName::from_static("x-amz-security-token"),
                HeaderValue::from_str(token)
                    .map_err(|e| io::Error::other(format!("x-amz-security-token: {e}")))?,
            );
        }
        for (k, v) in extra {
            if k == "host" {
                continue;
            }
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| io::Error::other(format!("header name '{k}': {e}")))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| io::Error::other(format!("header value for '{k}': {e}")))?;
            map.insert(name, value);
        }
        Ok(map)
    }

    // ── Public API ─────────────────────────────────────────────────────

    /// Fetch a byte range of an object (`Range: bytes=start-end`).
    pub async fn get_object_range(&self, key: &str, start: u64, end: u64) -> io::Result<Bytes> {
        let host = self.host();
        let signing_path = self.object_signing_path(key);
        let range_str = format!("bytes={start}-{end}");
        let mut extra: BTreeMap<String, String> = BTreeMap::new();
        extra.insert("range".to_string(), range_str);

        let headers = self.signed_headers("GET", &signing_path, "", &extra, b"", "s3", &host)?;
        let url = self.object_url(key);

        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("S3 GET range {key} {start}-{end}: {e}")))?;

        let status = resp.status();
        // AWS returns 206 Partial Content for range requests.
        // 200 OK means the server ignored the Range header and returned the
        // full object — treat as an error to avoid silent data corruption in
        // the parallel range assembly.
        if status.as_u16() == 200 {
            return Err(io::Error::other(format!(
                "S3 GET range {key} {start}-{end}: server returned 200 OK instead of 206 Partial Content (range ignored)"
            )));
        }
        if !status.is_success() {
            let body = error_body_preview(resp).await;
            return Err(io::Error::other(format!(
                "S3 GET range {key}: HTTP {status}: {body}"
            )));
        }
        resp.bytes()
            .await
            .map_err(|e| io::Error::other(format!("S3 GET range read {key}: {e}")))
    }

    /// Fetch the full object body (no Range header).
    pub async fn get_object(&self, key: &str) -> io::Result<Bytes> {
        let host = self.host();
        let signing_path = self.object_signing_path(key);

        let headers =
            self.signed_headers("GET", &signing_path, "", &BTreeMap::new(), b"", "s3", &host)?;
        let url = self.object_url(key);

        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("S3 GET {key}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = error_body_preview(resp).await;
            return Err(io::Error::other(format!(
                "S3 GET {key}: HTTP {status}: {body}"
            )));
        }
        resp.bytes()
            .await
            .map_err(|e| io::Error::other(format!("S3 GET read {key}: {e}")))
    }

    /// Fetch the full object as a streaming response body.
    ///
    /// Returns the `reqwest::Response` after status validation. The caller
    /// should use `resp.bytes_stream()` to consume the body incrementally.
    pub async fn get_object_stream(&self, key: &str) -> io::Result<reqwest::Response> {
        let host = self.host();
        let signing_path = self.object_signing_path(key);
        let headers =
            self.signed_headers("GET", &signing_path, "", &BTreeMap::new(), b"", "s3", &host)?;
        let url = self.object_url(key);

        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("S3 GET stream {key}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = error_body_preview(resp).await;
            return Err(io::Error::other(format!(
                "S3 GET stream {key}: HTTP {status}: {body}"
            )));
        }
        Ok(resp)
    }

    /// Fetch a byte range as a streaming response body.
    ///
    /// Returns the `reqwest::Response` after status validation (expects 206).
    pub async fn get_object_range_stream(
        &self,
        key: &str,
        start: u64,
        end: u64,
    ) -> io::Result<reqwest::Response> {
        let host = self.host();
        let signing_path = self.object_signing_path(key);
        let range_str = format!("bytes={start}-{end}");
        let mut extra: BTreeMap<String, String> = BTreeMap::new();
        extra.insert("range".to_string(), range_str);

        let headers = self.signed_headers("GET", &signing_path, "", &extra, b"", "s3", &host)?;
        let url = self.object_url(key);

        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| {
                io::Error::other(format!("S3 GET range stream {key} {start}-{end}: {e}"))
            })?;

        let status = resp.status();
        if status.as_u16() == 200 {
            return Err(io::Error::other(format!(
                "S3 GET range stream {key} {start}-{end}: server returned 200 OK instead of 206 Partial Content"
            )));
        }
        if !status.is_success() {
            let body = error_body_preview(resp).await;
            return Err(io::Error::other(format!(
                "S3 GET range stream {key}: HTTP {status}: {body}"
            )));
        }
        Ok(resp)
    }

    /// Issue a `HEAD` request and return the `Content-Length`.
    pub async fn head_object(&self, key: &str) -> io::Result<u64> {
        let meta = self.head_object_metadata(key).await?;
        Ok(meta.content_length)
    }

    /// Issue a `HEAD` request and return object metadata.
    pub async fn head_object_metadata(&self, key: &str) -> io::Result<ObjectMeta> {
        let host = self.host();
        let signing_path = self.object_signing_path(key);

        let headers = self.signed_headers(
            "HEAD",
            &signing_path,
            "",
            &BTreeMap::new(),
            b"",
            "s3",
            &host,
        )?;
        let url = self.object_url(key);

        let resp = self
            .client
            .head(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("S3 HEAD {key}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            return Err(io::Error::other(format!("S3 HEAD {key}: HTTP {status}")));
        }

        let content_length = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                io::Error::other(format!("S3 HEAD {key}: missing or invalid Content-Length"))
            })?;

        let content_encoding = resp
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        Ok(ObjectMeta {
            content_length,
            content_encoding,
            content_type,
        })
    }

    /// Upload an object using a `PUT` request with SigV4 signing.
    ///
    /// Used by benchmarks to seed test data into MinIO.
    pub async fn put_object(&self, key: &str, data: &[u8]) -> io::Result<()> {
        let host = self.host();
        let signing_path = self.object_signing_path(key);

        let headers = self.signed_headers(
            "PUT",
            &signing_path,
            "",
            &BTreeMap::new(),
            data,
            "s3",
            &host,
        )?;
        let url = self.object_url(key);

        let resp = self
            .client
            .put(&url)
            .headers(headers)
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| io::Error::other(format!("S3 PUT {key}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = error_body_preview(resp).await;
            return Err(io::Error::other(format!(
                "S3 PUT {key}: HTTP {status}: {body}"
            )));
        }
        Ok(())
    }

    /// Create the bucket (path-style PUT). Ignores `BucketAlreadyOwnedByYou`.
    ///
    /// Used by benchmarks to set up MinIO test buckets.
    pub async fn create_bucket(&self) -> io::Result<()> {
        let host = self.host();
        let signing_path = if self.path_style {
            format!("/{}", self.bucket)
        } else {
            "/".to_string()
        };

        let headers =
            self.signed_headers("PUT", &signing_path, "", &BTreeMap::new(), b"", "s3", &host)?;

        let url = if self.path_style {
            format!("{}/{}", self.endpoint_base, self.bucket)
        } else {
            format!("https://{}.s3.{}.amazonaws.com", self.bucket, self.region)
        };

        let resp = self
            .client
            .put(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("S3 create bucket: {e}")))?;

        let status = resp.status();
        // 200 OK or 409 Conflict (BucketAlreadyOwnedByYou) are both fine.
        if !status.is_success() && status.as_u16() != 409 {
            let body = error_body_preview(resp).await;
            return Err(io::Error::other(format!(
                "S3 create bucket: HTTP {status}: {body}"
            )));
        }
        Ok(())
    }

    /// List objects using `ListObjectsV2`.
    ///
    /// Returns the objects found and an optional continuation token (present
    /// when the result is truncated).
    pub async fn list_objects_v2(
        &self,
        prefix: Option<&str>,
        start_after: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> io::Result<(Vec<S3Object>, Option<String>)> {
        let mut params: Vec<(String, String)> = vec![
            ("list-type".to_string(), "2".to_string()),
            ("max-keys".to_string(), max_keys.to_string()),
        ];
        if let Some(p) = prefix {
            params.push(("prefix".to_string(), p.to_string()));
        }
        if let Some(sa) = start_after {
            params.push(("start-after".to_string(), sa.to_string()));
        }
        if let Some(ct) = continuation_token {
            params.push(("continuation-token".to_string(), ct.to_string()));
        }

        // Canonical query string: sort by key, then URI-encode.
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let query: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let host = self.host();
        let list_path = Self::list_signing_path();

        let headers = if self.path_style {
            // For path-style MinIO: the signing path includes the bucket (no trailing slash).
            let signing_path = format!("/{}", self.bucket);
            self.signed_headers(
                "GET",
                &signing_path,
                &query,
                &BTreeMap::new(),
                b"",
                "s3",
                &host,
            )?
        } else {
            self.signed_headers("GET", list_path, &query, &BTreeMap::new(), b"", "s3", &host)?
        };

        let url = self.list_url(&query);

        let resp = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("S3 ListObjectsV2: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = error_body_preview(resp).await;
            return Err(io::Error::other(format!(
                "S3 ListObjectsV2: HTTP {status}: {body}"
            )));
        }

        let body = resp
            .bytes()
            .await
            .map_err(|e| io::Error::other(format!("S3 ListObjectsV2 read: {e}")))?;

        parse_list_objects_response(&body)
    }
}

// ── XML parsing ────────────────────────────────────────────────────────────

fn parse_list_objects_response(data: &[u8]) -> io::Result<(Vec<S3Object>, Option<String>)> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);

    let mut objects: Vec<S3Object> = Vec::new();
    let mut next_token: Option<String> = None;

    let mut in_contents = false;
    let mut current_key = String::new();
    let mut current_size: u64 = 0;
    let mut capture: Option<&'static str> = None; // which field we're reading

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"Contents" => {
                    in_contents = true;
                    current_key.clear();
                    current_size = 0;
                }
                b"Key" if in_contents => capture = Some("key"),
                b"Size" if in_contents => capture = Some("size"),
                b"NextContinuationToken" => capture = Some("next_token"),
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if let Some(field) = capture {
                    let text = e
                        .unescape()
                        .map_err(|e| io::Error::other(format!("XML unescape: {e}")))?;
                    match field {
                        "key" => current_key = text.into_owned(),
                        "size" => {
                            current_size = text.parse::<u64>().unwrap_or(0);
                        }
                        "next_token" => next_token = Some(text.into_owned()),
                        _ => {}
                    }
                    capture = None;
                }
            }
            Ok(Event::End(e)) if e.local_name().as_ref() == b"Contents" && in_contents => {
                objects.push(S3Object {
                    key: current_key.clone(),
                    size: current_size,
                });
                in_contents = false;
                capture = None;
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(io::Error::other(format!("S3 list XML parse: {e}")));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok((objects, next_token))
}

// ── SigV4 helpers ─────────────────────────────────────────────────────────

/// Percent-encode a string for use in a URL query component or header value.
///
/// Unreserved characters (`A-Z a-z 0-9 - _ . ~`) are passed through;
/// everything else is `%XX`-encoded with uppercase hex digits.
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b => {
                out.push('%');
                out.push(nibble_to_hex(b >> 4));
                out.push(nibble_to_hex(b & 0x0f));
            }
        }
    }
    out
}

/// Percent-encode each path segment while preserving `/` separators.
pub fn url_encode_path(key: &str) -> String {
    key.split('/').map(url_encode).collect::<Vec<_>>().join("/")
}

fn nibble_to_hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'A' + n - 10) as char,
        // SAFETY: nibble is always 4 bits wide (caller masks with 0x0f / >> 4).
        _ => '0',
    }
}

/// Return `(date_str, datetime_str)` for the current UTC instant.
///
/// `date_str` is `"YYYYMMDD"`, `datetime_str` is `"YYYYMMDDTHHMMSSZcanonical"`.
fn utc_datetime_now() -> io::Result<(String, String)> {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| io::Error::other(format!("system clock before Unix epoch: {e}")))?
        .as_secs();
    Ok(format_sigv4_datetime(secs))
}

/// Format a Unix timestamp (seconds since epoch) into SigV4 date/datetime strings.
fn format_sigv4_datetime(secs: u64) -> (String, String) {
    let tod = secs % 86400;
    let days = secs / 86400;
    let h = tod / 3600;
    let m = (tod % 3600) / 60;
    let s = tod % 60;

    let (y, mo, d) = civil_from_days(days);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{date}T{h:02}{m:02}{s:02}Z");
    (date, datetime)
}

/// Convert days since Unix epoch (1970-01-01) to `(year, month, day)`.
///
/// Uses Howard Hinnant's civil-calendar algorithm
/// (<https://howardhinnant.github.io/date_algorithms.html>).
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days as i64 + 719_468;
    let era: i64 = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let mo = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = yoe + era as u64 * 400 + u64::from(mo <= 2); // year
    (y, mo, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_from_days_known_dates() {
        // 1970-01-01 = day 0
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2024-01-01 = 19723 days (leap year detection test)
        assert_eq!(civil_from_days(19723), (2024, 1, 1));
        // 2024-03-01 = 19783 days (31 Jan + 29 Feb in leap year = 60 days after 19723)
        assert_eq!(civil_from_days(19783), (2024, 3, 1));
    }

    #[test]
    fn format_sigv4_datetime_known() {
        // 2024-01-01 00:00:00 UTC = 1704067200
        let (date, dt) = format_sigv4_datetime(1_704_067_200);
        assert_eq!(date, "20240101");
        assert_eq!(dt, "20240101T000000Z");
    }

    #[test]
    fn url_encode_special_chars() {
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("path/key"), "path%2Fkey");
    }

    #[test]
    fn url_encode_path_preserves_slashes() {
        assert_eq!(url_encode_path("path/to/file.log"), "path/to/file.log");
        assert_eq!(url_encode_path("a b/c d"), "a%20b/c%20d");
    }

    #[test]
    fn parse_list_objects_response_basic() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>my-bucket</Name>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>logs/app.log</Key>
    <Size>1234</Size>
  </Contents>
  <Contents>
    <Key>logs/app2.log</Key>
    <Size>5678</Size>
  </Contents>
</ListBucketResult>"#;
        let (objects, token) = parse_list_objects_response(xml).expect("parse ok");
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].key, "logs/app.log");
        assert_eq!(objects[0].size, 1234);
        assert_eq!(objects[1].key, "logs/app2.log");
        assert_eq!(objects[1].size, 5678);
        assert!(token.is_none());
    }

    #[test]
    fn parse_list_objects_response_truncated() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>abc123</NextContinuationToken>
  <Contents>
    <Key>a.log</Key>
    <Size>10</Size>
  </Contents>
</ListBucketResult>"#;
        let (objects, token) = parse_list_objects_response(xml).expect("parse ok");
        assert_eq!(objects.len(), 1);
        assert_eq!(token.as_deref(), Some("abc123"));
    }
}
