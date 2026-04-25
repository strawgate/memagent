#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Apples-to-apples HTTP receiver benchmark for OTLP-like ingestion.
//!
//! Compares three transport stacks under the same workload and decode path:
//! - `tiny_http` (current receiver model)
//! - `axum` (candidate high-level async framework)
//! - `hyper` (candidate low-level async framework)
//!
//! Workload models expected user traffic: OTLP protobuf requests to `POST /v1/logs`,
//! with optional `Content-Encoding: zstd`.
//!
//! Run:
//!   cargo run --release --bin http_receiver_apples -p ffwd-bench
//!
//! Optional env vars:
//!   LOGFWD_HTTP_BENCH_DURATION_SECS=6
//!   LOGFWD_HTTP_BENCH_CONCURRENCY=8,32
use std::convert::Infallible;
use std::io::Read as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use flate2::read::GzDecoder;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};

const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;
const CHANNEL_CAPACITY: usize = 16_384;
const DEFAULT_DURATION_SECS: u64 = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Framework {
    TinyHttp,
    Axum,
    Hyper,
}

impl Framework {
    const ALL: [Self; 3] = [Self::TinyHttp, Self::Axum, Self::Hyper];

    const fn name(self) -> &'static str {
        match self {
            Self::TinyHttp => "tiny_http",
            Self::Axum => "axum",
            Self::Hyper => "hyper",
        }
    }
}

#[derive(Clone)]
struct Scenario {
    name: String,
    body: Bytes,
    content_encoding: Option<&'static str>,
    logs_per_request: u64,
}

#[derive(Default)]
struct ClientStats {
    requests: u64,
    ok: u64,
    too_many: u64,
    errors: u64,
    events: u64,
    accepted_bytes: u64,
    latencies_ns: Vec<u64>,
}

impl ClientStats {
    fn merge(&mut self, other: Self) {
        self.requests += other.requests;
        self.ok += other.ok;
        self.too_many += other.too_many;
        self.errors += other.errors;
        self.events += other.events;
        self.accepted_bytes += other.accepted_bytes;
        self.latencies_ns.extend(other.latencies_ns);
    }
}

struct RunResult {
    framework: Framework,
    scenario_name: String,
    concurrency: usize,
    req_per_sec: f64,
    eps: f64,
    mib_per_sec: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    too_many: u64,
    errors: u64,
}

#[derive(Clone)]
struct AsyncState {
    tx: mpsc::Sender<u64>,
}

struct TinyServer {
    addr: SocketAddr,
    running: Arc<AtomicBool>,
    server: Arc<tiny_http::Server>,
    request_handle: Option<std::thread::JoinHandle<()>>,
    drain_handle: Option<std::thread::JoinHandle<()>>,
    tx: Option<std_mpsc::SyncSender<u64>>,
}

struct AxumServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: tokio::task::JoinHandle<()>,
    drain_handle: tokio::task::JoinHandle<()>,
    tx: Option<mpsc::Sender<u64>>,
}

struct HyperServer {
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    server_handle: tokio::task::JoinHandle<()>,
    drain_handle: tokio::task::JoinHandle<()>,
    tx: Option<mpsc::Sender<u64>>,
}

enum RunningServer {
    Tiny(TinyServer),
    Axum(AxumServer),
    Hyper(HyperServer),
}

impl RunningServer {
    fn addr(&self) -> SocketAddr {
        match self {
            Self::Tiny(s) => s.addr,
            Self::Axum(s) => s.addr,
            Self::Hyper(s) => s.addr,
        }
    }

    async fn shutdown(self) {
        match self {
            Self::Tiny(mut s) => {
                s.running.store(false, Ordering::Relaxed);
                s.server.unblock();
                drop(s.tx.take());
                if let Some(h) = s.request_handle.take() {
                    let _ = h.join();
                }
                if let Some(h) = s.drain_handle.take() {
                    let _ = h.join();
                }
            }
            Self::Axum(mut s) => {
                if let Some(tx) = s.shutdown_tx.take() {
                    let _ = tx.send(());
                }
                drop(s.tx.take());
                let _ = s.server_handle.await;
                let _ = s.drain_handle.await;
            }
            Self::Hyper(mut s) => {
                let _ = s.shutdown_tx.send(true);
                drop(s.tx.take());
                let _ = s.server_handle.await;
                let _ = s.drain_handle.await;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let duration = Duration::from_secs(read_duration_secs());
    let concurrencies = read_concurrency_list();
    let scenarios = make_scenarios();

    println!("=== HTTP Receiver Apples-to-Apples Benchmark ===");
    println!(
        "duration={}s | concurrencies={:?} | max_body={} MiB",
        duration.as_secs(),
        concurrencies,
        MAX_BODY_SIZE / (1024 * 1024)
    );
    println!();

    let mut rows = Vec::new();

    for scenario in &scenarios {
        println!(
            "Scenario: {} | payload={} bytes | encoding={} | logs/req={}",
            scenario.name,
            scenario.body.len(),
            scenario.content_encoding.unwrap_or("identity"),
            scenario.logs_per_request
        );

        for framework in Framework::ALL {
            for &concurrency in &concurrencies {
                let server = match start_server(framework).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("failed to start {} server: {e}", framework.name());
                        continue;
                    }
                };

                tokio::time::sleep(Duration::from_millis(120)).await;

                let addr = server.addr();
                let stats = run_load(addr, scenario, concurrency, duration).await;
                server.shutdown().await;

                let mut latencies = stats.latencies_ns;
                latencies.sort_unstable();
                let p50 = percentile_ns(&latencies, 0.50);
                let p95 = percentile_ns(&latencies, 0.95);
                let p99 = percentile_ns(&latencies, 0.99);

                let seconds = duration.as_secs_f64();
                rows.push(RunResult {
                    framework,
                    scenario_name: scenario.name.clone(),
                    concurrency,
                    req_per_sec: stats.ok as f64 / seconds,
                    eps: stats.events as f64 / seconds,
                    mib_per_sec: stats.accepted_bytes as f64 / (1024.0 * 1024.0) / seconds,
                    p50_ns: p50,
                    p95_ns: p95,
                    p99_ns: p99,
                    too_many: stats.too_many,
                    errors: stats.errors,
                });
            }
        }

        println!();
    }

    println!(
        "| scenario | framework | conc | req/s | EPS | MiB/s | p50 | p95 | p99 | 429 | errors |"
    );
    println!("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for row in &rows {
        println!(
            "| {} | {} | {} | {:.0} | {:.0} | {:.1} | {} | {} | {} | {} | {} |",
            row.scenario_name,
            row.framework.name(),
            row.concurrency,
            row.req_per_sec,
            row.eps,
            row.mib_per_sec,
            format_latency(row.p50_ns),
            format_latency(row.p95_ns),
            format_latency(row.p99_ns),
            row.too_many,
            row.errors
        );
    }

    println!();
    println!("Relative req/s vs tiny_http (same scenario+concurrency):");
    for scenario in &scenarios {
        for &concurrency in &concurrencies {
            let tiny = rows
                .iter()
                .find(|r| {
                    r.scenario_name == scenario.name
                        && r.concurrency == concurrency
                        && r.framework == Framework::TinyHttp
                })
                .map_or(0.0, |r| r.req_per_sec);
            if tiny <= 0.0 {
                continue;
            }

            let axum = rows
                .iter()
                .find(|r| {
                    r.scenario_name == scenario.name
                        && r.concurrency == concurrency
                        && r.framework == Framework::Axum
                })
                .map_or(0.0, |r| r.req_per_sec / tiny);
            let hyper = rows
                .iter()
                .find(|r| {
                    r.scenario_name == scenario.name
                        && r.concurrency == concurrency
                        && r.framework == Framework::Hyper
                })
                .map_or(0.0, |r| r.req_per_sec / tiny);

            println!(
                "  {} @ c{}: axum={:.2}x, hyper={:.2}x",
                scenario.name, concurrency, axum, hyper
            );
        }
    }
}

async fn start_server(framework: Framework) -> std::io::Result<RunningServer> {
    match framework {
        Framework::TinyHttp => start_tiny_http_server().map(RunningServer::Tiny),
        Framework::Axum => start_axum_server().await.map(RunningServer::Axum),
        Framework::Hyper => start_hyper_server().await.map(RunningServer::Hyper),
    }
}

fn start_tiny_http_server() -> std::io::Result<TinyServer> {
    let server = Arc::new(
        tiny_http::Server::http("127.0.0.1:0")
            .map_err(|e| std::io::Error::other(format!("tiny_http bind failed: {e}")))?,
    );
    let addr = match server.server_addr() {
        tiny_http::ListenAddr::IP(a) => a,
        tiny_http::ListenAddr::Unix(_) => {
            return Err(std::io::Error::other("unexpected tiny_http listen addr"));
        }
    };

    let running = Arc::new(AtomicBool::new(true));
    let (tx, rx) = std_mpsc::sync_channel::<u64>(CHANNEL_CAPACITY);

    let drain_handle = std::thread::Builder::new()
        .name("bench-tiny-drain".into())
        .spawn(move || while rx.recv().is_ok() {})
        .map_err(std::io::Error::other)?;

    let running_req = Arc::clone(&running);
    let server_req = Arc::clone(&server);
    let tx_req = tx.clone();
    let request_handle = std::thread::Builder::new()
        .name("bench-tiny-http".into())
        .spawn(move || {
            while running_req.load(Ordering::Relaxed) {
                let mut request = match server_req.recv_timeout(Duration::from_millis(100)) {
                    Ok(Some(req)) => req,
                    Ok(None) => continue,
                    Err(_) => break,
                };

                let url = request.url().to_string();
                let path = url.split('?').next().unwrap_or(&url);
                if path != "/v1/logs" {
                    let _ = request.respond(
                        tiny_http::Response::from_string("not found").with_status_code(404),
                    );
                    continue;
                }
                if request.method() != &tiny_http::Method::Post {
                    let _ = request.respond(
                        tiny_http::Response::from_string("method not allowed")
                            .with_status_code(405),
                    );
                    continue;
                }

                if request.body_length().unwrap_or(0) > MAX_BODY_SIZE {
                    let _ = request.respond(
                        tiny_http::Response::from_string("payload too large").with_status_code(413),
                    );
                    continue;
                }

                let mut body =
                    Vec::with_capacity(request.body_length().unwrap_or(0).min(MAX_BODY_SIZE));
                let read_result = request
                    .as_reader()
                    .take(MAX_BODY_SIZE as u64 + 1)
                    .read_to_end(&mut body);

                match read_result {
                    Ok(n) if n > MAX_BODY_SIZE => {
                        let _ = request.respond(
                            tiny_http::Response::from_string("payload too large")
                                .with_status_code(413),
                        );
                        continue;
                    }
                    Ok(_) => {}
                    Err(_) => {
                        let _ = request.respond(
                            tiny_http::Response::from_string("read error").with_status_code(400),
                        );
                        continue;
                    }
                }

                let content_encoding = request
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("Content-Encoding"))
                    .and_then(|h| {
                        let v = h.value.as_str().to_ascii_lowercase();
                        if v.is_empty() { None } else { Some(v) }
                    });

                let Ok(logs) = decode_and_count(body, content_encoding.as_deref()) else {
                    let _ = request.respond(
                        tiny_http::Response::from_string("decode error").with_status_code(400),
                    );
                    continue;
                };

                match tx_req.try_send(logs) {
                    Ok(()) => {
                        let _ = request.respond(tiny_http::Response::empty(200));
                    }
                    Err(std_mpsc::TrySendError::Full(_)) => {
                        let _ = request.respond(
                            tiny_http::Response::from_string("backpressure").with_status_code(429),
                        );
                    }
                    Err(std_mpsc::TrySendError::Disconnected(_)) => {
                        let _ = request.respond(
                            tiny_http::Response::from_string("disconnected").with_status_code(503),
                        );
                    }
                }
            }
        })
        .map_err(std::io::Error::other)?;

    Ok(TinyServer {
        addr,
        running,
        server,
        request_handle: Some(request_handle),
        drain_handle: Some(drain_handle),
        tx: Some(tx),
    })
}

async fn start_axum_server() -> std::io::Result<AxumServer> {
    let (tx, mut rx) = mpsc::channel::<u64>(CHANNEL_CAPACITY);
    let state = AsyncState { tx: tx.clone() };
    let app = axum::Router::new()
        .route("/v1/logs", axum::routing::post(axum_handler))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(std::io::Error::other)?;
    let addr = listener.local_addr().map_err(std::io::Error::other)?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let drain_handle = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    Ok(AxumServer {
        addr,
        shutdown_tx: Some(shutdown_tx),
        server_handle,
        drain_handle,
        tx: Some(tx),
    })
}

async fn axum_handler(
    State(state): State<AsyncState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if body.len() > MAX_BODY_SIZE {
        return StatusCode::PAYLOAD_TOO_LARGE;
    }

    let content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(str::to_ascii_lowercase);

    let Ok(logs) = decode_and_count(body.to_vec(), content_encoding.as_deref()) else {
        return StatusCode::BAD_REQUEST;
    };

    match state.tx.try_send(logs) {
        Ok(()) => StatusCode::OK,
        Err(mpsc::error::TrySendError::Full(_)) => StatusCode::TOO_MANY_REQUESTS,
        Err(mpsc::error::TrySendError::Closed(_)) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn start_hyper_server() -> std::io::Result<HyperServer> {
    let (tx, mut rx) = mpsc::channel::<u64>(CHANNEL_CAPACITY);
    let state = AsyncState { tx: tx.clone() };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(std::io::Error::other)?;
    let addr = listener.local_addr().map_err(std::io::Error::other)?;

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let server_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else {
                        continue;
                    };
                    let io = TokioIo::new(stream);
                    let state = state.clone();
                    tokio::spawn(async move {
                        let svc = service_fn(move |req| {
                            let state = state.clone();
                            async move { hyper_handler(req, state).await }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await;
                    });
                }
            }
        }
    });

    let drain_handle = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    Ok(HyperServer {
        addr,
        shutdown_tx,
        server_handle,
        drain_handle,
        tx: Some(tx),
    })
}

type RespBody = Full<Bytes>;

fn hyper_response(status: StatusCode) -> Response<RespBody> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("response builder should succeed")
}

async fn hyper_handler(
    req: Request<Incoming>,
    state: AsyncState,
) -> Result<Response<RespBody>, Infallible> {
    if req.uri().path() != "/v1/logs" {
        return Ok(hyper_response(StatusCode::NOT_FOUND));
    }
    if req.method() != Method::POST {
        return Ok(hyper_response(StatusCode::METHOD_NOT_ALLOWED));
    }

    let content_encoding = req
        .headers()
        .get(hyper::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(str::to_ascii_lowercase);

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Ok(hyper_response(StatusCode::BAD_REQUEST)),
    };
    if body_bytes.len() > MAX_BODY_SIZE {
        return Ok(hyper_response(StatusCode::PAYLOAD_TOO_LARGE));
    }

    let Ok(logs) = decode_and_count(body_bytes.to_vec(), content_encoding.as_deref()) else {
        return Ok(hyper_response(StatusCode::BAD_REQUEST));
    };

    let status = match state.tx.try_send(logs) {
        Ok(()) => StatusCode::OK,
        Err(mpsc::error::TrySendError::Full(_)) => StatusCode::TOO_MANY_REQUESTS,
        Err(mpsc::error::TrySendError::Closed(_)) => StatusCode::SERVICE_UNAVAILABLE,
    };
    Ok(hyper_response(status))
}

async fn run_load(
    addr: SocketAddr,
    scenario: &Scenario,
    concurrency: usize,
    duration: Duration,
) -> ClientStats {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency.max(4))
        .http1_only()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_millis(1500))
        .build()
        .expect("reqwest client should build");

    let url = format!("http://{addr}/v1/logs");
    let deadline = tokio::time::Instant::now() + duration;

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let url = url.clone();
        let body = scenario.body.clone();
        let encoding = scenario.content_encoding.map(str::to_string);
        let logs_per_request = scenario.logs_per_request;
        handles.push(tokio::spawn(async move {
            let mut partial = ClientStats::default();
            while tokio::time::Instant::now() < deadline {
                let start = Instant::now();
                let mut req = client
                    .post(&url)
                    .header("content-type", "application/x-protobuf");
                if let Some(enc) = encoding.as_deref() {
                    req = req.header("content-encoding", enc);
                }
                let resp = req.body(body.clone()).send().await;
                let latency_ns = start.elapsed().as_nanos() as u64;
                partial.latencies_ns.push(latency_ns);
                partial.requests += 1;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        partial.ok += 1;
                        partial.events += logs_per_request;
                        partial.accepted_bytes += body.len() as u64;
                    }
                    Ok(r) if r.status() == StatusCode::TOO_MANY_REQUESTS => {
                        partial.too_many += 1;
                    }
                    Ok(_) => {
                        partial.errors += 1;
                    }
                    Err(_) => {
                        partial.errors += 1;
                    }
                }
            }
            partial
        }));
    }

    let mut stats = ClientStats::default();
    for h in handles {
        if let Ok(partial) = h.await {
            stats.merge(partial);
        } else {
            stats.errors += 1;
        }
    }
    stats
}

fn decode_and_count(body: Vec<u8>, content_encoding: Option<&str>) -> Result<u64, String> {
    let decoded = decode_content(body, content_encoding)?;
    let req = ExportLogsServiceRequest::decode(decoded.as_slice())
        .map_err(|e| format!("otlp decode error: {e}"))?;
    let mut count = 0u64;
    for rl in req.resource_logs {
        for sl in rl.scope_logs {
            count += sl.log_records.len() as u64;
        }
    }
    Ok(count)
}

fn decode_content(body: Vec<u8>, content_encoding: Option<&str>) -> Result<Vec<u8>, String> {
    match content_encoding {
        None | Some("identity") => Ok(body),
        Some("zstd") => {
            let decoder = zstd::Decoder::new(body.as_slice())
                .map_err(|e| format!("zstd decoder init failed: {e}"))?;
            read_decompressed_body(decoder, body.len(), "zstd decompression failed")
        }
        Some("gzip") => {
            let decoder = GzDecoder::new(body.as_slice());
            read_decompressed_body(decoder, body.len(), "gzip decompression failed")
        }
        Some(other) => Err(format!("unsupported content-encoding: {other}")),
    }
}

fn read_decompressed_body(
    reader: impl std::io::Read,
    compressed_len: usize,
    label: &str,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(compressed_len.min(MAX_BODY_SIZE));
    let n = reader
        .take(MAX_BODY_SIZE as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| format!("{label}: {e}"))?;
    if n > MAX_BODY_SIZE {
        return Err(format!("payload too large: {n} > {MAX_BODY_SIZE}"));
    }
    Ok(out)
}

fn make_scenarios() -> Vec<Scenario> {
    let small_logs = 25usize;
    let medium_logs = 250usize;
    let small_payload = make_otlp_payload(small_logs, 160);
    let medium_payload = make_otlp_payload(medium_logs, 220);
    let small_zstd = zstd::encode_all(small_payload.as_slice(), 1).expect("zstd encode small");
    let medium_zstd = zstd::encode_all(medium_payload.as_slice(), 1).expect("zstd encode medium");

    vec![
        Scenario {
            name: format!("otlp.protobuf.identity.small({small_logs} logs)"),
            body: Bytes::from(small_payload),
            content_encoding: None,
            logs_per_request: small_logs as u64,
        },
        Scenario {
            name: format!("otlp.protobuf.zstd.small({small_logs} logs)"),
            body: Bytes::from(small_zstd),
            content_encoding: Some("zstd"),
            logs_per_request: small_logs as u64,
        },
        Scenario {
            name: format!("otlp.protobuf.zstd.medium({medium_logs} logs)"),
            body: Bytes::from(medium_zstd),
            content_encoding: Some("zstd"),
            logs_per_request: medium_logs as u64,
        },
    ]
}

fn make_otlp_payload(log_records: usize, message_len: usize) -> Vec<u8> {
    let mut records = Vec::with_capacity(log_records);
    for i in 0..log_records {
        let message = pseudo_message(i as u64, message_len.max(8));
        let trace_hex = pseudo_hex(i as u64 ^ 0x9E37_79B9_7F4A_7C15, 32);
        let span_hex = pseudo_hex(i as u64 ^ 0xD6E8_FDCD_A123_45B1, 16);
        records.push(LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(message)),
            }),
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("receiver-bench".to_string())),
                    }),
                },
                KeyValue {
                    key: "http.status_code".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::IntValue(200)),
                    }),
                },
                KeyValue {
                    key: "trace_id".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(trace_hex)),
                    }),
                },
                KeyValue {
                    key: "span_id".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(span_hex)),
                    }),
                },
            ],
            ..Default::default()
        });
    }

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let mut buf = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut buf)
        .expect("OTLP payload encode should succeed");
    buf
}

fn pseudo_message(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./";
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xA5A5_5A5A_F0F0_0F0F;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let idx = (x as usize) % ALPHABET.len();
        out.push(ALPHABET[idx] as char);
    }
    out
}

fn pseudo_hex(seed: u64, digits: usize) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut x = seed.wrapping_mul(0xD6E8_FDCD_A123_45B1) ^ 0xC3A5_C85C_97CB_3127;
    let mut out = String::with_capacity(digits);
    for _ in 0..digits {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push(HEX[(x as usize) & 0x0f] as char);
    }
    out
}

fn read_duration_secs() -> u64 {
    std::env::var("LOGFWD_HTTP_BENCH_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_DURATION_SECS)
}

fn read_concurrency_list() -> Vec<usize> {
    let raw = std::env::var("LOGFWD_HTTP_BENCH_CONCURRENCY").unwrap_or_else(|_| "8,32".to_string());
    let mut out = Vec::new();
    for part in raw.split(',') {
        if let Ok(v) = part.trim().parse::<usize>()
            && v > 0
        {
            out.push(v);
        }
    }
    if out.is_empty() {
        vec![8, 32]
    } else {
        out.sort_unstable();
        out.dedup();
        out
    }
}

fn percentile_ns(values: &[u64], quantile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let q = quantile.clamp(0.0, 1.0);
    let idx = ((values.len() - 1) as f64 * q).round() as usize;
    values[idx]
}

fn format_latency(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}
