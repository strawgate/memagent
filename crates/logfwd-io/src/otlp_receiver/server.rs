use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
#[cfg(any(feature = "otlp-research", test))]
use bytes::Bytes;
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};
use tokio::sync::OwnedSemaphorePermit;

use crate::InputError;
use crate::receiver_http::{parse_content_length, parse_content_type, read_limited_body};

#[cfg(any(feature = "otlp-research", test))]
use super::OtlpProtobufDecodeMode;
use super::decode::{decode_otlp_json, decode_otlp_protobuf, decompress_gzip, decompress_zstd};
#[cfg(any(feature = "otlp-research", test))]
use super::projection::ProjectionError;
use super::{OtlpServerState, ReceiverPayload};

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
    let max_body = state.max_message_size_bytes;
    let content_length = parse_content_length(&headers);
    if content_length.is_some_and(|body_len| body_len > max_body as u64) {
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

    let content_type = match parse_content_type(&headers) {
        Ok(content_type) => content_type,
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-type header").into_response();
        }
    };
    let is_json = matches!(content_type.as_deref(), Some("application/json"));

    let mut body = match read_limited_body(body, max_body, content_length).await {
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
        Some("zstd") => match decompress_zstd(&body, max_body) {
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
        Some("gzip") => match decompress_gzip(&body, max_body) {
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

    let batch = if is_json {
        decode_otlp_json(&body, &state.resource_prefix).map_err(OtlpRequestDecodeError::Payload)
    } else {
        let decode_permit = match Arc::clone(&state.protobuf_decode_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                state
                    .health
                    .store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    "too many requests: protobuf decode backpressure",
                )
                    .into_response();
            }
        };
        decode_otlp_protobuf_request(body, Arc::clone(&state), decode_permit).await
    };
    let batch = match batch {
        Ok(batch) => batch,
        Err(OtlpRequestDecodeError::Payload(msg)) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
        }
        Err(OtlpRequestDecodeError::Internal(msg)) => {
            record_error(state.stats.as_ref());
            tracing::error!(error = %msg, "OTLP protobuf decode internal failure");
            if state.is_running.load(Ordering::Relaxed) {
                state
                    .health
                    .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
            }
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal OTLP decode error",
            )
                .into_response();
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

enum OtlpRequestDecodeError {
    Payload(InputError),
    Internal(String),
}

async fn decode_otlp_protobuf_request(
    body: Vec<u8>,
    state: Arc<OtlpServerState>,
    decode_permit: OwnedSemaphorePermit,
) -> Result<RecordBatch, OtlpRequestDecodeError> {
    // Protobuf decode and Arrow materialization are CPU work. Keep them off the
    // current-thread Axum runtime so the receiver can continue polling I/O.
    tokio::task::spawn_blocking(move || {
        let result = decode_otlp_protobuf_request_blocking(body, &state);
        drop(decode_permit);
        result
    })
    .await
    .map_err(|err| {
        OtlpRequestDecodeError::Internal(format!("OTLP protobuf decode task failed: {err}"))
    })?
}

#[cfg(any(feature = "otlp-research", test))]
fn decode_otlp_protobuf_request_blocking(
    body: Vec<u8>,
    state: &OtlpServerState,
) -> Result<RecordBatch, OtlpRequestDecodeError> {
    let body = Bytes::from(body);
    match state.protobuf_decode_mode {
        OtlpProtobufDecodeMode::Prost => {
            decode_otlp_protobuf(body.as_ref(), &state.resource_prefix)
                .map_err(OtlpRequestDecodeError::Payload)
        }
        #[cfg(any(feature = "otlp-research", test))]
        OtlpProtobufDecodeMode::ProjectedFallback => {
            decode_otlp_protobuf_projected_fallback(body, state)
        }
        #[cfg(any(feature = "otlp-research", test))]
        OtlpProtobufDecodeMode::ProjectedOnly => decode_otlp_protobuf_projected_only(body, state),
    }
}

#[cfg(not(any(feature = "otlp-research", test)))]
fn decode_otlp_protobuf_request_blocking(
    body: Vec<u8>,
    state: &OtlpServerState,
) -> Result<RecordBatch, OtlpRequestDecodeError> {
    debug_assert_eq!(
        state.protobuf_decode_mode,
        super::OtlpProtobufDecodeMode::Prost
    );
    decode_otlp_protobuf(&body, &state.resource_prefix).map_err(OtlpRequestDecodeError::Payload)
}

#[cfg(any(feature = "otlp-research", test))]
fn decode_otlp_protobuf_projected_fallback(
    body: Bytes,
    state: &OtlpServerState,
) -> Result<RecordBatch, OtlpRequestDecodeError> {
    match decode_with_reusable_projected_decoder(body.clone(), state) {
        Ok(batch) => {
            record_projected_success(state.stats.as_ref());
            Ok(batch)
        }
        Err(ProjectedDecodeError::Projection(ProjectionError::Unsupported(_))) => {
            record_projected_fallback(state.stats.as_ref());
            decode_otlp_protobuf(&body, &state.resource_prefix)
                .map_err(OtlpRequestDecodeError::Payload)
        }
        Err(ProjectedDecodeError::Projection(err)) => {
            record_projection_invalid(state.stats.as_ref());
            Err(OtlpRequestDecodeError::Payload(err.into_input_error()))
        }
        Err(ProjectedDecodeError::Internal(msg)) => Err(OtlpRequestDecodeError::Internal(msg)),
    }
}

#[cfg(any(feature = "otlp-research", test))]
fn decode_otlp_protobuf_projected_only(
    body: Bytes,
    state: &OtlpServerState,
) -> Result<RecordBatch, OtlpRequestDecodeError> {
    match decode_with_reusable_projected_decoder(body, state) {
        Ok(batch) => {
            record_projected_success(state.stats.as_ref());
            Ok(batch)
        }
        Err(ProjectedDecodeError::Projection(ProjectionError::Unsupported(err))) => Err(
            OtlpRequestDecodeError::Payload(ProjectionError::Unsupported(err).into_input_error()),
        ),
        Err(ProjectedDecodeError::Projection(err)) => {
            record_projection_invalid(state.stats.as_ref());
            Err(OtlpRequestDecodeError::Payload(err.into_input_error()))
        }
        Err(ProjectedDecodeError::Internal(msg)) => Err(OtlpRequestDecodeError::Internal(msg)),
    }
}

#[cfg(any(feature = "otlp-research", test))]
enum ProjectedDecodeError {
    Projection(ProjectionError),
    Internal(String),
}

#[cfg(any(feature = "otlp-research", test))]
fn decode_with_reusable_projected_decoder(
    body: Bytes,
    state: &OtlpServerState,
) -> Result<RecordBatch, ProjectedDecodeError> {
    let decoder_pool = state.projected_decoders.as_ref().ok_or_else(|| {
        ProjectedDecodeError::Internal("projected decoder pool not initialized".to_string())
    })?;
    let mut saw_poisoned_shard = false;
    for _ in 0..decoder_pool.len() {
        let Some(shard) = decoder_pool.next() else {
            break;
        };
        let mut decoder = match shard.decoder.lock() {
            Ok(decoder) => decoder,
            Err(_) => {
                saw_poisoned_shard = true;
                shard.mark_unavailable();
                continue;
            }
        };
        return decoder
            .try_decode_view_bytes(body)
            .map_err(ProjectedDecodeError::Projection);
    }

    if saw_poisoned_shard {
        Err(ProjectedDecodeError::Internal(
            "all projected decoder shards are poisoned or unavailable".to_string(),
        ))
    } else {
        Err(ProjectedDecodeError::Internal(
            "projected decoder pool has no available shards".to_string(),
        ))
    }
}

#[cfg(any(feature = "otlp-research", test))]
fn record_projected_success(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_otlp_projected_success();
    }
}

#[cfg(any(feature = "otlp-research", test))]
fn record_projected_fallback(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_otlp_projected_fallback();
    }
}

#[cfg(any(feature = "otlp-research", test))]
fn record_projection_invalid(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_otlp_projection_invalid();
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

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, AtomicU8};

    use axum::http::Request;
    use axum::routing::post;
    use logfwd_types::field_names;
    use tokio::sync::Semaphore;
    use tower::ServiceExt;

    use super::*;
    use crate::otlp_receiver::ProjectedDecoderPool;

    fn projected_only_state(max_message_size_bytes: usize) -> Arc<OtlpServerState> {
        let (tx, _rx) = mpsc::sync_channel(1);
        Arc::new(OtlpServerState {
            tx,
            is_running: Arc::new(AtomicBool::new(true)),
            health: Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr())),
            resource_prefix: field_names::DEFAULT_RESOURCE_PREFIX.to_string(),
            protobuf_decode_mode: OtlpProtobufDecodeMode::ProjectedOnly,
            protobuf_decode_permits: Arc::new(Semaphore::new(1)),
            projected_decoders: Some(ProjectedDecoderPool::new(
                field_names::DEFAULT_RESOURCE_PREFIX,
                1,
            )),
            stats: None,
            max_message_size_bytes,
        })
    }

    fn poison_projected_decoder_lock(state: &OtlpServerState) {
        let decoder_pool = state
            .projected_decoders
            .as_ref()
            .expect("projected decoder pool should be initialized");
        let poison_result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = decoder_pool
                .first()
                .decoder
                .lock()
                .expect("initial lock should succeed");
            panic!("poison projected decoder");
        }));
        assert!(poison_result.is_err(), "test must poison the decoder lock");
    }

    #[test]
    fn projected_decoder_pool_uses_requested_shards() {
        let pool = ProjectedDecoderPool::new(field_names::DEFAULT_RESOURCE_PREFIX, 4);
        assert_eq!(pool.len(), 4);
    }

    #[test]
    fn projected_decoder_pool_caps_requested_shards() {
        let pool = ProjectedDecoderPool::new(field_names::DEFAULT_RESOURCE_PREFIX, usize::MAX);
        assert_eq!(pool.len(), super::super::MAX_PROJECTED_DECODER_SHARDS);
    }

    #[test]
    fn projected_decoder_pool_round_robins_shards() {
        let pool = ProjectedDecoderPool::new(field_names::DEFAULT_RESOURCE_PREFIX, 2);
        let first = pool.next().expect("first shard should be available");
        let second = pool.next().expect("second shard should be available");
        let third = pool.next().expect("first shard should be selected again");

        assert!(!std::ptr::eq(first, second));
        assert!(std::ptr::eq(first, third));
    }

    #[test]
    fn projected_decoder_pool_skips_unavailable_shards() {
        let pool = ProjectedDecoderPool::new(field_names::DEFAULT_RESOURCE_PREFIX, 2);
        let first = pool.next().expect("first shard should be available");
        first.mark_unavailable();
        let second = pool.next().expect("second shard should be available");
        let third = pool.next().expect("second shard should remain available");

        assert!(!std::ptr::eq(first, second));
        assert!(std::ptr::eq(second, third));
    }

    #[test]
    fn single_projected_decoder_poison_is_internal_error() {
        let state = projected_only_state(1024);
        poison_projected_decoder_lock(&state);

        match decode_otlp_protobuf_request_blocking(Vec::new(), &state) {
            Err(OtlpRequestDecodeError::Internal(msg)) => {
                assert!(msg.contains("all projected decoder shards are poisoned"));
            }
            Err(OtlpRequestDecodeError::Payload(_)) => {
                panic!("poisoned decoder lock must not be reported as a payload error");
            }
            Ok(_) => panic!("poisoned decoder lock must fail"),
        }
    }

    #[test]
    fn projected_decoder_poisoned_shard_is_removed_from_rotation_without_failing_request() {
        let (tx, _rx) = mpsc::sync_channel(1);
        let state = OtlpServerState {
            tx,
            is_running: Arc::new(AtomicBool::new(true)),
            health: Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr())),
            resource_prefix: field_names::DEFAULT_RESOURCE_PREFIX.to_string(),
            protobuf_decode_mode: OtlpProtobufDecodeMode::ProjectedOnly,
            protobuf_decode_permits: Arc::new(Semaphore::new(1)),
            projected_decoders: Some(ProjectedDecoderPool::new(
                field_names::DEFAULT_RESOURCE_PREFIX,
                2,
            )),
            stats: None,
            max_message_size_bytes: 1024,
        };
        poison_projected_decoder_lock(&state);

        let first = decode_otlp_protobuf_request_blocking(Vec::new(), &state);
        assert!(
            first.is_ok(),
            "request should retry against a healthy shard after disabling the poisoned shard"
        );

        let second = decode_otlp_protobuf_request_blocking(Vec::new(), &state);
        assert!(
            second.is_ok(),
            "subsequent request should skip the poisoned shard"
        );
    }

    #[test]
    fn poisoned_projected_decoder_returns_http_500_and_failed_health() {
        let state = projected_only_state(1024);
        poison_projected_decoder_lock(&state);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");

        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::empty())
            .expect("test request should build");

        let response = runtime
            .block_on(app.oneshot(request))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            state.health.load(Ordering::Relaxed),
            ComponentHealth::Failed.as_repr()
        );
    }

    #[test]
    fn protobuf_decode_permit_exhaustion_does_not_mask_pre_decode_errors() {
        let state = projected_only_state(1024);
        let _held_permit = Arc::clone(&state.protobuf_decode_permits)
            .try_acquire_owned()
            .expect("test should exhaust the only decode permit");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");

        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "unsupported")
            .body(Body::empty())
            .expect("test request should build");

        let response = runtime
            .block_on(app.oneshot(request))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            state.health.load(Ordering::Relaxed),
            ComponentHealth::Healthy.as_repr()
        );
    }

    #[test]
    fn protobuf_decode_permit_exhaustion_returns_429_and_degraded_health() {
        let state = projected_only_state(1024);
        let _held_permit = Arc::clone(&state.protobuf_decode_permits)
            .try_acquire_owned()
            .expect("test should exhaust the only decode permit");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");

        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(Vec::new()))
            .expect("test request should build");

        let response = runtime
            .block_on(app.oneshot(request))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            state.health.load(Ordering::Relaxed),
            ComponentHealth::Degraded.as_repr()
        );
    }
}
