//! Lightweight AWS SQS client using the Query API with SigV4 signing.
//!
//! Used by `S3Input` in SQS-driven discovery mode. Receives S3 `ObjectCreated`
//! event notifications, extracts object key + size, and manages message
//! visibility for at-least-once delivery.

use std::collections::BTreeMap;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use super::client::{error_body_preview, url_encode};

type HmacSha256 = Hmac<Sha256>;

/// A single SQS message including the parsed S3 event body.
#[derive(Debug)]
pub struct SqsMessage {
    /// SQS message receipt handle (used to delete or extend visibility).
    pub receipt_handle: String,
    /// S3 object records parsed from the message body.
    pub records: Vec<S3EventRecord>,
}

/// An S3 object created event extracted from an SQS message body.
#[derive(Debug)]
pub struct S3EventRecord {
    /// S3 object key (URL-decoded).
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
}

/// Lightweight SQS client with AWS SigV4 signing.
pub struct SqsClient {
    http: reqwest::Client,
    queue_url: String,
    region: String,
    host: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl SqsClient {
    /// Create a new `SqsClient` for the given queue URL.
    ///
    /// The region is extracted from the URL for `https://sqs.{region}.amazonaws.com/…`
    /// URLs. For custom endpoints, `region` must be supplied explicitly.
    pub fn new(
        queue_url: impl Into<String>,
        region_override: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    ) -> io::Result<Self> {
        let queue_url = queue_url.into();
        let host = extract_host(&queue_url)?;
        let region = region_override
            .or_else(|| extract_region_from_sqs_url(&queue_url))
            .ok_or_else(|| {
                io::Error::other(format!(
                    "SQS: cannot determine region from queue URL '{queue_url}'; \
                     set region in config"
                ))
            })?;

        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(60))
            .use_rustls_tls()
            .build()
            .map_err(|e| io::Error::other(format!("SQS client build: {e}")))?;

        Ok(Self {
            http,
            queue_url,
            region,
            host,
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    // ── Public API ─────────────────────────────────────────────────────

    /// Long-poll for up to `max_messages` messages (max 10 per SQS limit),
    /// waiting up to `wait_secs` seconds (max 20).
    /// Sets VisibilityTimeout on receive so messages are hidden for the configured
    /// duration, not just the queue default.
    pub async fn receive_messages(
        &self,
        max_messages: u8,
        wait_secs: u8,
        visibility_timeout_secs: u32,
    ) -> io::Result<Vec<SqsMessage>> {
        let n = max_messages.clamp(1, 10);
        let w = wait_secs.min(20);

        let mut params: BTreeMap<String, String> = BTreeMap::new();
        params.insert("Action".to_string(), "ReceiveMessage".to_string());
        params.insert("MaxNumberOfMessages".to_string(), n.to_string());
        params.insert("WaitTimeSeconds".to_string(), w.to_string());
        params.insert(
            "VisibilityTimeout".to_string(),
            visibility_timeout_secs.to_string(),
        );
        params.insert("AttributeName.1".to_string(), "All".to_string());
        params.insert("Version".to_string(), "2012-11-05".to_string());

        let body = sqs_post(
            &self.http,
            &self.queue_url,
            &self.host,
            &self.region,
            &params,
            &self.access_key_id,
            &self.secret_access_key,
            self.session_token.as_deref(),
        )
        .await?;
        parse_receive_messages_response(&body)
    }

    /// Delete a message after successful processing.
    pub async fn delete_message(&self, receipt_handle: &str) -> io::Result<()> {
        let mut params: BTreeMap<String, String> = BTreeMap::new();
        params.insert("Action".to_string(), "DeleteMessage".to_string());
        params.insert("ReceiptHandle".to_string(), receipt_handle.to_string());
        params.insert("Version".to_string(), "2012-11-05".to_string());

        sqs_post(
            &self.http,
            &self.queue_url,
            &self.host,
            &self.region,
            &params,
            &self.access_key_id,
            &self.secret_access_key,
            self.session_token.as_deref(),
        )
        .await?;
        Ok(())
    }

    /// Extend the visibility timeout of a message still being processed.
    pub async fn change_message_visibility(
        &self,
        receipt_handle: &str,
        timeout_secs: u32,
    ) -> io::Result<()> {
        let mut params: BTreeMap<String, String> = BTreeMap::new();
        params.insert("Action".to_string(), "ChangeMessageVisibility".to_string());
        params.insert("ReceiptHandle".to_string(), receipt_handle.to_string());
        params.insert("VisibilityTimeout".to_string(), timeout_secs.to_string());
        params.insert("Version".to_string(), "2012-11-05".to_string());

        sqs_post(
            &self.http,
            &self.queue_url,
            &self.host,
            &self.region,
            &params,
            &self.access_key_id,
            &self.secret_access_key,
            self.session_token.as_deref(),
        )
        .await?;
        Ok(())
    }
}

// ── HTTP + SigV4 internals ─────────────────────────────────────────────────

/// Issue an SQS Query API POST request and return the response body.
#[allow(clippy::too_many_arguments)]
async fn sqs_post(
    client: &reqwest::Client,
    queue_url: &str,
    host: &str,
    region: &str,
    params: &BTreeMap<String, String>,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
) -> io::Result<Bytes> {
    // Build URL-encoded form body (sorted by key for canonical form).
    let body: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let body_bytes = body.as_bytes();
    let body_sha256 = sha256_hex(body_bytes);

    let (date, datetime) = utc_datetime_now()?;
    let credential_scope = format!("{date}/{region}/sqs/aws4_request");

    // Canonical headers for POST form submission.
    let mut cheaders: BTreeMap<String, String> = BTreeMap::new();
    cheaders.insert(
        "content-type".to_string(),
        "application/x-www-form-urlencoded".to_string(),
    );
    cheaders.insert("host".to_string(), host.to_string());
    cheaders.insert("x-amz-content-sha256".to_string(), body_sha256.clone());
    cheaders.insert("x-amz-date".to_string(), datetime.clone());
    if let Some(token) = session_token {
        cheaders.insert("x-amz-security-token".to_string(), token.to_string());
    }

    // Extract the path from the queue URL for the canonical URI.
    let path = sqs_path(queue_url);

    let canonical_headers: String = cheaders.iter().fold(String::new(), |mut s, (k, v)| {
        use std::fmt::Write;
        let _ = writeln!(s, "{k}:{v}");
        s
    });
    let signed_headers: String = cheaders.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_request =
        format!("POST\n{path}\n\n{canonical_headers}\n{signed_headers}\n{body_sha256}");

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        datetime,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = build_signing_key(secret_access_key, &date, region, "sqs");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{credential_scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut req = client
        .post(queue_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("x-amz-date", &datetime)
        .header("x-amz-content-sha256", &body_sha256)
        .header("Authorization", &auth);
    if let Some(token) = session_token {
        req = req.header("x-amz-security-token", token);
    }
    let resp = req
        .body(body)
        .send()
        .await
        .map_err(|e| io::Error::other(format!("SQS POST: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let preview = error_body_preview(resp).await;
        return Err(io::Error::other(format!(
            "SQS POST HTTP {status}: {preview}"
        )));
    }

    resp.bytes()
        .await
        .map_err(|e| io::Error::other(format!("SQS read response: {e}")))
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn build_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn utc_datetime_now() -> io::Result<(String, String)> {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| io::Error::other(format!("system clock before Unix epoch: {e}")))?
        .as_secs();
    // Reuse the same algorithm as client.rs — replicated to keep this module
    // self-contained without creating a shared internal dependency.
    let tod = secs % 86400;
    let days = secs / 86400;
    let h = tod / 3600;
    let m = (tod % 3600) / 60;
    let s = tod % 60;
    let (y, mo, d) = civil_from_days(days);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{date}T{h:02}{m:02}{s:02}Z");
    Ok((date, datetime))
}

fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days as i64 + 719_468;
    let era: i64 = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era as u64 * 400 + u64::from(mo <= 2);
    (y, mo, d)
}

/// Extract the path component from an SQS queue URL for canonical URI.
fn sqs_path(url: &str) -> String {
    // Queue URL format: https://sqs.{region}.amazonaws.com/{account}/{queue}
    // Strip scheme + host, keep the path.
    if let Some(pos) = url.find("://") {
        let after_scheme = &url[pos + 3..];
        if let Some(slash) = after_scheme.find('/') {
            return after_scheme[slash..].to_string();
        }
    }
    "/".to_string()
}

/// Extract the hostname from a URL.
fn extract_host(url: &str) -> io::Result<String> {
    let pos = url
        .find("://")
        .ok_or_else(|| io::Error::other(format!("SQS: invalid URL (no scheme): {url}")))?;
    let after = &url[pos + 3..];
    let end = after.find('/').unwrap_or(after.len());
    Ok(after[..end].to_string())
}

/// Extract AWS region from an SQS queue URL.
///
/// For `https://sqs.{region}.amazonaws.com/…` URLs only.
pub(super) fn extract_region_from_sqs_url(url: &str) -> Option<String> {
    // Strip scheme + host prefix "sqs."
    let host = url
        .find("://")
        .map(|p| &url[p + 3..])
        .and_then(|s| s.find('/').map(|e| &s[..e]))?;

    // host = "sqs.{region}.amazonaws.com"
    let rest = host.strip_prefix("sqs.")?;
    let region = rest.strip_suffix(".amazonaws.com")?;
    Some(region.to_string())
}

// ── XML + JSON parsing ─────────────────────────────────────────────────────

fn parse_receive_messages_response(data: &Bytes) -> io::Result<Vec<SqsMessage>> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CaptureField {
        Receipt,
        Body,
    }

    let mut reader = Reader::from_reader(data.as_ref());
    reader.config_mut().trim_text(true);

    let mut messages: Vec<SqsMessage> = Vec::new();
    let mut in_message = false;
    let mut receipt_handle = String::new();
    let mut body = String::new();
    let mut capture: Option<CaptureField> = None;
    let mut current_text = String::new();

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"Message" => {
                    in_message = true;
                    receipt_handle.clear();
                    body.clear();
                }
                b"ReceiptHandle" if in_message => {
                    capture = Some(CaptureField::Receipt);
                    current_text.clear();
                }
                b"Body" if in_message => {
                    capture = Some(CaptureField::Body);
                    current_text.clear();
                }
                _ => {}
            },
            Ok(Event::Text(e)) if capture.is_some() => {
                let decoded = e
                    .decode()
                    .map_err(|e| io::Error::other(format!("SQS XML decode: {e}")))?;
                current_text.push_str(decoded.as_ref());
            }
            Ok(Event::GeneralRef(e)) if capture.is_some() => {
                super::push_xml_reference(&mut current_text, e, "SQS")?;
            }
            Ok(Event::CData(e)) if capture.is_some() => {
                current_text.push_str(&String::from_utf8_lossy(e.into_inner().as_ref()));
            }
            Ok(Event::End(e)) => match e.local_name().as_ref() {
                b"ReceiptHandle" if capture == Some(CaptureField::Receipt) => {
                    receipt_handle = std::mem::take(&mut current_text);
                    capture = None;
                }
                b"Body" if capture == Some(CaptureField::Body) => {
                    body = std::mem::take(&mut current_text);
                    capture = None;
                }
                b"Message" if in_message => {
                    in_message = false;
                    let records = parse_s3_event_body(&body);
                    // Always push the message even when records is empty.
                    // The discovery loop deletes non-actionable messages
                    // (test notifications, non-ObjectCreated events) by
                    // checking `records.is_empty()`.
                    messages.push(SqsMessage {
                        receipt_handle: std::mem::take(&mut receipt_handle),
                        records,
                    });
                    body.clear();
                    capture = None;
                    current_text.clear();
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(io::Error::other(format!("SQS XML parse: {e}")));
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(messages)
}

/// Parse an S3 event notification JSON body (may be SNS-wrapped or direct).
fn parse_s3_event_body(body: &str) -> Vec<S3EventRecord> {
    if body.is_empty() {
        return vec![];
    }

    let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(body) else {
        return vec![];
    };

    // SNS-wrapped: actual payload is in wrapper["Message"].
    if let Some(inner) = wrapper.get("Message").and_then(|v| v.as_str()) {
        if let Ok(inner_val) = serde_json::from_str::<serde_json::Value>(inner) {
            return parse_s3_records_from_value(&inner_val);
        }
        return vec![];
    }

    parse_s3_records_from_value(&wrapper)
}

fn parse_s3_records_from_value(v: &serde_json::Value) -> Vec<S3EventRecord> {
    let mut records = Vec::new();
    let Some(arr) = v.get("Records").and_then(|r| r.as_array()) else {
        return records;
    };
    for record in arr {
        let event_name = record
            .get("eventName")
            .and_then(|n| n.as_str())
            .unwrap_or("");
        // Only process ObjectCreated events.
        if !event_name.starts_with("ObjectCreated") {
            continue;
        }
        let key_encoded = record
            .pointer("/s3/object/key")
            .and_then(|k| k.as_str())
            .unwrap_or("");
        let size = record
            .pointer("/s3/object/size")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        // Keys in S3 event notifications are URL-encoded.
        let key = url_decode(key_encoded);
        if !key.is_empty() {
            records.push(S3EventRecord { key, size });
        }
    }
    records
}

/// Percent-decode a URL-encoded string.
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_nibble(bytes[i + 1]), hex_nibble(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        // S3 event notifications use RFC 3986 percent-encoding where `+` is
        // a literal character (spaces are encoded as `%20`). Do NOT convert
        // `+` to space — that is `application/x-www-form-urlencoded` behavior.
        out.push(bytes[i]);
        i += 1;
    }
    // Use lossy UTF-8 — object keys should be valid but be defensive.
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_region_from_standard_sqs_url() {
        let url = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue";
        assert_eq!(
            extract_region_from_sqs_url(url).as_deref(),
            Some("us-east-1")
        );
    }

    #[test]
    fn extract_host_standard() {
        let url = "https://sqs.us-east-1.amazonaws.com/123/q";
        assert_eq!(extract_host(url).unwrap(), "sqs.us-east-1.amazonaws.com");
    }

    #[test]
    fn sqs_path_standard() {
        let url = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue";
        assert_eq!(sqs_path(url), "/123456789/my-queue");
    }

    #[test]
    fn url_decode_encoded_key() {
        assert_eq!(url_decode("path%2Fto%2Ffile.log"), "path/to/file.log");
        // S3 notifications use RFC 3986 encoding — `+` is a literal, not a space.
        assert_eq!(url_decode("hello+world"), "hello+world");
        // Spaces are encoded as %20 in S3 notifications.
        assert_eq!(url_decode("hello%20world"), "hello world");
    }

    #[test]
    fn parse_s3_event_body_object_created() {
        let body = r#"{
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "object": {
                            "key": "logs%2Fapp.log.gz",
                            "size": 12345
                        }
                    }
                }
            ]
        }"#;
        let records = parse_s3_event_body(body);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "logs/app.log.gz");
        assert_eq!(records[0].size, 12345);
    }

    #[test]
    fn parse_s3_event_body_ignores_delete() {
        let body = r#"{"Records":[{"eventName":"ObjectRemoved:Delete","s3":{"object":{"key":"k","size":0}}}]}"#;
        let records = parse_s3_event_body(body);
        assert!(records.is_empty());
    }

    #[test]
    fn parse_receive_messages_response_preserves_entity_fragments() {
        let xml = Bytes::from_static(
            br#"<ReceiveMessageResponse>
  <ReceiveMessageResult>
    <Message>
      <ReceiptHandle>rh&amp;&#49;</ReceiptHandle>
      <Body>{&quot;Records&quot;:[{&quot;eventName&quot;:&quot;ObjectCreated:Put&quot;,&quot;s3&quot;:{&quot;object&quot;:{&quot;key&quot;:&quot;logs%2Fapp%26x.log&quot;,&quot;size&quot;:42}}}]}</Body>
    </Message>
  </ReceiveMessageResult>
</ReceiveMessageResponse>"#,
        );

        let messages = parse_receive_messages_response(&xml).expect("parse ok");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].receipt_handle, "rh&1");
        assert_eq!(messages[0].records.len(), 1);
        assert_eq!(messages[0].records[0].key, "logs/app&x.log");
        assert_eq!(messages[0].records[0].size, 42);
    }

    #[test]
    fn parse_receive_messages_response_preserves_cdata_fragments() {
        let xml = Bytes::from_static(
            br#"<ReceiveMessageResponse>
  <ReceiveMessageResult>
    <Message>
      <ReceiptHandle><![CDATA[rh-cdata]]></ReceiptHandle>
      <Body><![CDATA[{"Records":[{"eventName":"ObjectCreated:Put","s3":{"object":{"key":"logs%2Fcdata.log","size":7}}}]}]]></Body>
    </Message>
  </ReceiveMessageResult>
</ReceiveMessageResponse>"#,
        );

        let messages = parse_receive_messages_response(&xml).expect("parse ok");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].receipt_handle, "rh-cdata");
        assert_eq!(messages[0].records.len(), 1);
        assert_eq!(messages[0].records[0].key, "logs/cdata.log");
        assert_eq!(messages[0].records[0].size, 7);
    }
}
