use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::InputError;
use crate::diagnostics::ComponentStats;
use crate::receiver_http::{declared_content_length, read_limited_body};
use logfwd_types::diagnostics::ComponentHealth;

use super::OtlpServerState;
use super::decode::{
    MAX_BODY_SIZE, decode_otlp_logs_with_mode, decode_otlp_logs_with_mode_json, decompress_gzip,
    decompress_zstd,
};

pub(super) fn record_error(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_errors();
    }
}

fn record_parse_error(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_parse_errors(1);
    }
}

pub(super) async fn handle_otlp_request(
    State(state): State<Arc<OtlpServerState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let content_length = declared_content_length(&headers);
    if content_length.is_some_and(|body_len| body_len > MAX_BODY_SIZE as u64) {
        record_error(state.stats.as_ref());
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
    }

    let content_encoding = match parse_content_encoding(&headers) {
        Ok(content_encoding) => content_encoding,
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-encoding header").into_response();
        }
    };

    let mut body = match read_limited_body(body, MAX_BODY_SIZE, content_length).await {
        Ok(body) => body,
        Err(status) => {
            record_error(state.stats.as_ref());
            let message = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload too large"
            } else {
                "read error"
            };
            return (status, message).into_response();
        }
    };

    let accounted_bytes = body.len() as u64;
    body = match content_encoding.as_deref() {
        Some("zstd") => match decompress_zstd(&body) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(InputError::Io(e)) if e.kind() == io::ErrorKind::InvalidData => {
                record_error(state.stats.as_ref());
                return (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response();
            }
            Err(_) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, "zstd decompression failed").into_response();
            }
        },
        Some("gzip") => match decompress_gzip(&body) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(InputError::Io(e)) if e.kind() == io::ErrorKind::InvalidData => {
                record_error(state.stats.as_ref());
                return (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response();
            }
            Err(_) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, "gzip decompression failed").into_response();
            }
        },
        None | Some("identity") => body,
        Some(other) => {
            record_error(state.stats.as_ref());
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                format!("unsupported content-encoding: {other}"),
            )
                .into_response();
        }
    };

    let content_type = match parse_content_type(&headers) {
        Ok(content_type) => content_type,
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-type header").into_response();
        }
    };
    let is_json = matches!(content_type.as_deref(), Some("application/json"));

    let payload = if is_json {
        decode_otlp_logs_with_mode_json(&body, state.mode, accounted_bytes)
    } else {
        decode_otlp_logs_with_mode(&body, state.mode, accounted_bytes)
    };
    let payload = match payload {
        Ok(payload) => payload,
        Err(msg) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
        }
    };

    let send_result = if payload.is_empty() {
        Ok(())
    } else {
        state.tx.try_send(payload)
    };

    match send_result {
        Ok(()) => {
            state
                .health
                .store(ComponentHealth::Healthy.as_repr(), Ordering::Relaxed);
            (StatusCode::OK, [(CONTENT_TYPE, "application/json")], "{}").into_response()
        }
        Err(mpsc::TrySendError::Full(_)) => {
            state
                .health
                .store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);
            (
                StatusCode::TOO_MANY_REQUESTS,
                "too many requests: pipeline backpressure",
            )
                .into_response()
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            record_error(state.stats.as_ref());
            if state.is_running.load(Ordering::Relaxed) {
                state
                    .health
                    .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
            }
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable: pipeline disconnected",
            )
                .into_response()
        }
    }
}

fn parse_content_encoding(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Some(parsed.to_ascii_lowercase()))
}

fn parse_content_type(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return Ok(None);
    };
    let raw = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    let media_type = raw.split(';').next().unwrap_or_default().trim();
    if media_type.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(media_type.to_ascii_lowercase()))
}
