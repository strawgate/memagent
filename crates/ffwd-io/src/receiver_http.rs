use axum::body::Body;
use axum::http::{
    HeaderMap, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
};
use http_body_util::BodyExt as _;

/// Maximum request body size shared by all HTTP receivers: 10 MB.
pub(crate) const MAX_REQUEST_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Parses the Content-Length header value.
///
/// Returns `None` when the header is missing, non-UTF-8, or not a valid `u64`.
pub(crate) fn parse_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

pub(crate) fn parse_content_type(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_e| StatusCode::UNSUPPORTED_MEDIA_TYPE)?;
    let media_type = raw.split(';').next().unwrap_or_default().trim();
    if media_type.is_empty() {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    Ok(Some(media_type.to_ascii_lowercase()))
}

pub(crate) async fn read_limited_body(
    body: Body,
    max_body_size: usize,
    content_length_hint: Option<u64>,
) -> Result<Vec<u8>, StatusCode> {
    if content_length_hint.is_some_and(|hint| hint > max_body_size as u64) {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let mut body = body;
    let mut out = Vec::with_capacity(
        content_length_hint
            .map(|hint| hint.min(max_body_size as u64) as usize)
            .unwrap_or_default(),
    );

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_e| StatusCode::BAD_REQUEST)?;
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if out.len().saturating_add(chunk.len()) > max_body_size {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}
