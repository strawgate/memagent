use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

use crate::InputError;
use crate::receiver_http::{
    MAX_REQUEST_BODY_SIZE, parse_content_length, parse_content_type, read_limited_body,
};

use super::decode::{
    decode_otlp_json, decode_otlp_protobuf, decode_otlp_protobuf_bytes_with_mode, decompress_gzip,
    decompress_zstd,
};
use super::{OtlpProtobufDecodeMode, OtlpServerState, ReceiverPayload};

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
    let max_request_body_size = state
        .max_recv_message_size_bytes
        .unwrap_or(MAX_REQUEST_BODY_SIZE);

    let content_length = parse_content_length(&headers);
    if content_length.is_some_and(|body_len| body_len > max_request_body_size as u64) {
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

    let mut body = match read_limited_body(body, max_request_body_size, content_length).await {
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
        Some("zstd") => match decompress_zstd(&body, max_request_body_size) {
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
        Some("gzip") => match decompress_gzip(&body, max_request_body_size) {
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

    let batch = if is_json {
        decode_otlp_json(&body, &state.resource_prefix)
    } else if state.protobuf_decode_mode == OtlpProtobufDecodeMode::Prost {
        decode_otlp_protobuf(&body, &state.resource_prefix)
    } else {
        decode_otlp_protobuf_bytes_with_mode(
            Bytes::from(body),
            &state.resource_prefix,
            state.protobuf_decode_mode,
        )
    };
    let batch = match batch {
        Ok(batch) => batch,
        Err(msg) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
        }
    };

    if batch.num_rows() == 0 {
        state
            .health
            .store(ComponentHealth::Healthy.as_repr(), Ordering::Relaxed);
        return (StatusCode::OK, [(CONTENT_TYPE, "application/json")], "{}").into_response();
    }

    let payload = ReceiverPayload {
        batch,
        accounted_bytes,
    };

    let send_result = state.tx.try_send(payload);

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
    let parsed = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?.trim();
    if parsed.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(parsed.to_ascii_lowercase()))
}
