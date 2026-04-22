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
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

use crate::InputError;
use crate::blocking_stage::{BlockingStageReceiveError, BlockingStageSubmitError};
use crate::receiver_http::{parse_content_length, parse_content_type, read_limited_body};

use super::decode_stage::{
    OtlpContentEncoding, OtlpRequestContent, OtlpRequestCpuError, OtlpRequestCpuJob,
    OtlpRequestCpuOutcome, OtlpRequestCpuOutput,
};
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
    let content_encoding = match content_encoding.as_deref() {
        Some("zstd") => OtlpContentEncoding::Zstd,
        Some("gzip") => OtlpContentEncoding::Gzip,
        None | Some("identity") => OtlpContentEncoding::Identity,
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
    let content = if matches!(content_type.as_deref(), Some("application/json")) {
        OtlpRequestContent::Json
    } else {
        OtlpRequestContent::Protobuf
    };

    let body = match read_limited_body(body, max_body, content_length).await {
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

    let batch = match decode_otlp_request_cpu(
        body,
        content,
        content_encoding,
        max_body,
        Arc::clone(&state),
    )
    .await
    {
        Ok(batch) => Ok(batch),
        Err(OtlpRequestDecodeError::Backpressure) => {
            state
                .health
                .store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);
            return (
                StatusCode::TOO_MANY_REQUESTS,
                "too many requests: OTLP request CPU backpressure",
            )
                .into_response();
        }
        Err(err) => Err(err),
    };
    let batch = match batch {
        Ok(batch) => batch,
        Err(OtlpRequestDecodeError::Payload(msg)) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
        }
        Err(OtlpRequestDecodeError::PayloadTooLarge(msg)) => {
            record_error(state.stats.as_ref());
            return (StatusCode::PAYLOAD_TOO_LARGE, msg).into_response();
        }
        Err(OtlpRequestDecodeError::Transport(msg)) => {
            record_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg).into_response();
        }
        Err(OtlpRequestDecodeError::Internal(msg)) => {
            record_error(state.stats.as_ref());
            tracing::error!(error = %msg, "OTLP request CPU stage internal failure");
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
        // Handled by the first match — early-returns before reaching here.
        Err(OtlpRequestDecodeError::Backpressure) => unreachable!(),
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
    PayloadTooLarge(String),
    Transport(String),
    Backpressure,
    Internal(String),
}

async fn decode_otlp_request_cpu(
    body: Vec<u8>,
    content: OtlpRequestContent,
    encoding: OtlpContentEncoding,
    max_body_size: usize,
    state: Arc<OtlpServerState>,
) -> Result<RecordBatch, OtlpRequestDecodeError> {
    let result = state
        .request_cpu_stage
        .try_submit(OtlpRequestCpuJob {
            body,
            content,
            encoding,
            max_body_size,
        })
        .map_err(|err| match err {
            BlockingStageSubmitError::Full => OtlpRequestDecodeError::Backpressure,
            BlockingStageSubmitError::Closed => {
                OtlpRequestDecodeError::Internal("OTLP request CPU stage is closed".to_string())
            }
            BlockingStageSubmitError::WorkerFailed => {
                OtlpRequestDecodeError::Internal("OTLP request CPU stage worker failed".to_string())
            }
        })?;

    match result.recv().await {
        Ok(output) => {
            record_decode_outcome(&output, state.stats.as_ref());
            Ok(output.batch)
        }
        Err(BlockingStageReceiveError::Worker(err)) => map_worker_error(err, state.stats.as_ref()),
        Err(BlockingStageReceiveError::WorkerPanicked) => Err(OtlpRequestDecodeError::Internal(
            "OTLP request CPU worker panicked".to_string(),
        )),
        Err(BlockingStageReceiveError::Closed) => Err(OtlpRequestDecodeError::Internal(
            "OTLP request CPU stage closed before returning a result".to_string(),
        )),
    }
}

fn map_worker_error(
    err: OtlpRequestCpuError,
    stats: Option<&Arc<ComponentStats>>,
) -> Result<RecordBatch, OtlpRequestDecodeError> {
    #[cfg(not(any(feature = "otlp-research", test)))]
    let _ = stats;

    match err {
        OtlpRequestCpuError::Decompression(err) => map_decompression_error(err),
        OtlpRequestCpuError::Payload(err) => map_payload_error(err),
        #[cfg(any(feature = "otlp-research", test))]
        OtlpRequestCpuError::ProjectionInvalid(err) => {
            record_projection_invalid(stats);
            map_payload_error(err)
        }
    }
}

fn map_decompression_error(err: InputError) -> Result<RecordBatch, OtlpRequestDecodeError> {
    match err {
        InputError::Io(e) if e.kind() == io::ErrorKind::InvalidData => {
            Err(OtlpRequestDecodeError::PayloadTooLarge(e.to_string()))
        }
        err => Err(OtlpRequestDecodeError::Transport(err.to_string())),
    }
}

fn map_payload_error(err: InputError) -> Result<RecordBatch, OtlpRequestDecodeError> {
    match err {
        InputError::Io(e) if e.kind() == io::ErrorKind::InvalidData => {
            Err(OtlpRequestDecodeError::PayloadTooLarge(e.to_string()))
        }
        err => Err(OtlpRequestDecodeError::Payload(err)),
    }
}

fn record_decode_outcome(output: &OtlpRequestCpuOutput, stats: Option<&Arc<ComponentStats>>) {
    #[cfg(not(any(feature = "otlp-research", test)))]
    let _ = stats;

    match output.outcome {
        OtlpRequestCpuOutcome::Json | OtlpRequestCpuOutcome::Prost => {}
        #[cfg(any(feature = "otlp-research", test))]
        OtlpRequestCpuOutcome::ProjectedSuccess => record_projected_success(stats),
        #[cfg(any(feature = "otlp-research", test))]
        OtlpRequestCpuOutcome::ProjectedFallback => record_projected_fallback(stats),
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
    let parsed = value.to_str().map_err(|_e| StatusCode::BAD_REQUEST)?.trim();
    if parsed.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(parsed.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU8};

    use axum::http::Request;
    use axum::routing::post;
    use logfwd_types::field_names;
    use tower::ServiceExt;

    use super::super::OtlpProtobufDecodeMode;
    use super::super::decode_stage::{
        OtlpContentEncoding, OtlpRequestContent, OtlpRequestCpuJob, build_otlp_request_cpu_stage,
    };
    use super::*;

    fn projected_only_state(
        max_message_size_bytes: usize,
        max_cpu_outstanding: usize,
    ) -> Arc<OtlpServerState> {
        projected_only_state_with_stats(max_message_size_bytes, max_cpu_outstanding, None)
    }

    fn projected_only_state_with_stats(
        max_message_size_bytes: usize,
        max_cpu_outstanding: usize,
        stats: Option<Arc<ComponentStats>>,
    ) -> Arc<OtlpServerState> {
        let (tx, _rx) = mpsc::sync_channel(1);
        let request_cpu_stage = build_otlp_request_cpu_stage(
            1,
            max_cpu_outstanding,
            Arc::<str>::from(field_names::DEFAULT_RESOURCE_PREFIX),
            OtlpProtobufDecodeMode::ProjectedOnly,
        )
        .expect("request CPU stage should build");
        Arc::new(OtlpServerState {
            tx,
            is_running: Arc::new(AtomicBool::new(true)),
            health: Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr())),
            request_cpu_stage: Arc::new(request_cpu_stage),
            stats,
            max_message_size_bytes,
        })
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build")
    }

    fn protobuf_request(body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(body.into())
            .expect("test request should build")
    }

    fn json_request(body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/json")
            .body(body.into())
            .expect("test request should build")
    }

    fn gzip_protobuf_request(body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "gzip")
            .body(body.into())
            .expect("test request should build")
    }

    fn occupy_cpu_stage(state: &OtlpServerState) {
        state
            .request_cpu_stage
            .try_submit(OtlpRequestCpuJob {
                body: b"__sleep_worker__".to_vec(),
                content: OtlpRequestContent::Protobuf,
                encoding: OtlpContentEncoding::Identity,
                max_body_size: 1024,
            })
            .expect("test should occupy CPU stage capacity");
    }

    #[test]
    fn request_cpu_worker_panic_returns_http_500_and_failed_health() {
        let state = projected_only_state(1024, 1);
        let runtime = test_runtime();
        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));

        let response = runtime
            .block_on(app.oneshot(protobuf_request(Vec::from("__panic_worker__"))))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            state.health.load(Ordering::Relaxed),
            ComponentHealth::Failed.as_repr()
        );
    }

    #[test]
    fn request_cpu_stage_backpressure_does_not_mask_pre_decode_errors() {
        let state = projected_only_state(1024, 1);
        occupy_cpu_stage(&state);
        let runtime = test_runtime();
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
    fn request_cpu_stage_backpressure_returns_429_for_protobuf_request() {
        let state = projected_only_state(1024, 1);
        occupy_cpu_stage(&state);
        let runtime = test_runtime();
        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));

        let response = runtime
            .block_on(app.oneshot(protobuf_request(Vec::new())))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            state.health.load(Ordering::Relaxed),
            ComponentHealth::Degraded.as_repr()
        );
    }

    #[test]
    fn request_cpu_stage_backpressure_returns_429_for_json_request() {
        let state = projected_only_state(1024, 1);
        occupy_cpu_stage(&state);
        let runtime = test_runtime();
        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));

        let response = runtime
            .block_on(app.oneshot(json_request(b"{}".as_slice())))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            state.health.load(Ordering::Relaxed),
            ComponentHealth::Degraded.as_repr()
        );
    }

    #[test]
    fn request_cpu_stage_backpressure_returns_429_before_decompressing_gzip() {
        let state = projected_only_state(1024, 1);
        occupy_cpu_stage(&state);
        let runtime = test_runtime();
        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));

        let response = runtime
            .block_on(app.oneshot(gzip_protobuf_request(b"not gzip".as_slice())))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            state.health.load(Ordering::Relaxed),
            ComponentHealth::Degraded.as_repr()
        );
    }

    #[test]
    fn invalid_gzip_decompression_counts_transport_error() {
        let stats = Arc::new(ComponentStats::new());
        let state = projected_only_state_with_stats(1024, 1, Some(Arc::clone(&stats)));
        let runtime = test_runtime();
        let app = axum::Router::new()
            .route("/v1/logs", post(handle_otlp_request))
            .with_state(Arc::clone(&state));

        let response = runtime
            .block_on(app.oneshot(gzip_protobuf_request(b"not gzip".as_slice())))
            .expect("handler should return a response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(stats.errors(), 1);
        assert_eq!(stats.parse_errors(), 0);
    }
}
