// ---------------------------------------------------------------------------
// gen_wide — Wide JSON lines (20+ fields, ~600 bytes/line)
// ---------------------------------------------------------------------------

/// Generate `count` wide JSON log lines (~600 bytes each, 20+ fields).
///
/// Simulates verbose structured logging with many attributes.
/// Deterministic for a given `(count, seed)` pair.
pub fn gen_wide(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut cardinality = CardinalityState::new(CardinalityProfile::infra_like());
    let mut buf = String::with_capacity(count * 650);

    for i in 0..count {
        let sample = cardinality.sample(&mut rng);
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let level = level_for_phase(sample.phase);
        let path = pick_by_idx(PATHS, sample.path_idx);
        let method = pick(&mut rng, METHODS);
        let status = sample.status_code;
        let duration = rng.f64()
            * match sample.phase {
                SamplePhase::Hot => 300.0,
                SamplePhase::Warm => 900.0,
                SamplePhase::Cold => 3500.0,
            };
        let ns = pick_by_idx(NAMESPACES, sample.namespace_idx);
        let service = pick_by_idx(SERVICES, sample.service_idx);
        let user = user_label(sample.user_idx);

        let _ = write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:{sec:02}.{nano:09}Z","level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"cluster":"{}","node":"{}","namespace":"{ns}","pod":"{}","container":"{}","service":"{service}","trace_id":"{:032x}","span_id":"{:016x}","request_id":"req-{:08x}","session_id":{},"user":"{user}","bytes_sent":{},"bytes_received":{},"user_agent":"Mozilla/5.0 (X11; Linux x86_64)","content_type":"application/json","remote_addr":"10.{}.{}.{}","host":"api.example.com","protocol":"HTTP/1.1","tls_version":"TLSv1.3","upstream_latency_ms":{:.1},"retry_count":{},"error_count":{}}}"#,
            sample.cluster,
            sample.node,
            sample.pod,
            sample.container,
            sample.trace_id,
            sample.span_id,
            sample.request_id as u32,
            sample.session_id,
            rng.u32(..65536),
            rng.u32(..65536),
            rng.u8(..),
            rng.u8(..),
            rng.u8(..),
            rng.f64() * 200.0,
            sample.retry_count,
            sample.error_count,
        );
        buf.push('\n');
    }
    buf.into_bytes()
}

/// Generate wide flat logs directly as a detached [`RecordBatch`].
pub fn gen_wide_batch(count: usize, seed: u64) -> RecordBatch {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut cardinality = CardinalityState::new(CardinalityProfile::infra_like());

    let mut timestamp = StringBuilder::with_capacity(count, count.saturating_mul(32));
    let mut level = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut message = StringBuilder::with_capacity(count, count.saturating_mul(96));
    let mut status = Vec::with_capacity(count);
    let mut duration_ms = Vec::with_capacity(count);
    let mut cluster = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut node = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut namespace = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut pod = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut container = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut service = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut trace_id = StringBuilder::with_capacity(count, count.saturating_mul(32));
    let mut span_id = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut request_id = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut session_id = Vec::with_capacity(count);
    let mut user = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut bytes_sent = Vec::with_capacity(count);
    let mut bytes_received = Vec::with_capacity(count);
    let mut user_agent = StringBuilder::with_capacity(count, count.saturating_mul(40));
    let mut content_type = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut remote_addr = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut host = StringBuilder::with_capacity(count, count.saturating_mul(20));
    let mut protocol = StringBuilder::with_capacity(count, count.saturating_mul(12));
    let mut tls_version = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut upstream_latency_ms = Vec::with_capacity(count);
    let mut retry_count = Vec::with_capacity(count);
    let mut error_count = Vec::with_capacity(count);
    let mut timestamp_buf = String::with_capacity(32);
    let mut message_buf = String::with_capacity(96);
    let mut trace_id_buf = String::with_capacity(32);
    let mut span_id_buf = String::with_capacity(16);
    let mut request_id_buf = String::with_capacity(16);
    let mut user_buf = String::with_capacity(16);
    let mut remote_addr_buf = String::with_capacity(24);

    for i in 0..count {
        let sample = cardinality.sample(&mut rng);
        let phase = sample.phase;
        let status_code = sample.status_code;
        let session_id_value = sample.session_id;
        let retry_count_value = sample.retry_count;
        let error_count_value = sample.error_count;
        let service_idx = sample.service_idx;
        let namespace_idx = sample.namespace_idx;
        let path_idx = sample.path_idx;
        let user_idx = sample.user_idx;
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let level_value = level_for_phase(phase);
        let path = pick_by_idx(PATHS, path_idx);
        let method = pick(&mut rng, METHODS);
        let status_value = status_code;
        let duration_value = round_tenths(
            rng.f64()
                * match phase {
                    SamplePhase::Hot => 300.0,
                    SamplePhase::Warm => 900.0,
                    SamplePhase::Cold => 3500.0,
                },
        );
        let namespace_value = pick_by_idx(NAMESPACES, namespace_idx);
        let service_value = pick_by_idx(SERVICES, service_idx);
        append_user_label(&mut user_buf, user_idx);
        append_timestamp_utc(&mut timestamp_buf, sec, nano);
        timestamp.append_value(&timestamp_buf);
        level.append_value(level_value);
        message_buf.clear();
        let _ = write!(message_buf, "{method} {path} HTTP/1.1");
        message.append_value(&message_buf);
        status.push(status_value);
        duration_ms.push(duration_value);
        cluster.append_value(sample.cluster);
        node.append_value(sample.node);
        namespace.append_value(namespace_value);
        pod.append_value(sample.pod);
        container.append_value(sample.container);
        service.append_value(service_value);
        trace_id_buf.clear();
        let _ = write!(trace_id_buf, "{:032x}", sample.trace_id);
        trace_id.append_value(&trace_id_buf);
        span_id_buf.clear();
        let _ = write!(span_id_buf, "{:016x}", sample.span_id);
        span_id.append_value(&span_id_buf);
        request_id_buf.clear();
        let _ = write!(request_id_buf, "req-{:08x}", sample.request_id as u32);
        request_id.append_value(&request_id_buf);
        session_id.push(session_id_value);
        user.append_value(&user_buf);
        bytes_sent.push(rng.u32(..65536));
        bytes_received.push(rng.u32(..65536));
        user_agent.append_value("Mozilla/5.0 (X11; Linux x86_64)");
        content_type.append_value("application/json");
        remote_addr_buf.clear();
        let _ = write!(
            remote_addr_buf,
            "10.{}.{}.{}",
            rng.u8(..),
            rng.u8(..),
            rng.u8(..)
        );
        remote_addr.append_value(&remote_addr_buf);
        host.append_value("api.example.com");
        protocol.append_value("HTTP/1.1");
        tls_version.append_value("TLSv1.3");
        upstream_latency_ms.push(round_tenths(rng.f64() * 200.0));
        retry_count.push(retry_count_value);
        error_count.push(error_count_value);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration_ms", DataType::Float64, true),
        Field::new("cluster", DataType::Utf8, true),
        Field::new("node", DataType::Utf8, true),
        Field::new("namespace", DataType::Utf8, true),
        Field::new("pod", DataType::Utf8, true),
        Field::new("container", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("request_id", DataType::Utf8, true),
        Field::new("session_id", DataType::Int64, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("bytes_sent", DataType::Int64, true),
        Field::new("bytes_received", DataType::Int64, true),
        Field::new("user_agent", DataType::Utf8, true),
        Field::new("content_type", DataType::Utf8, true),
        Field::new("remote_addr", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, true),
        Field::new("protocol", DataType::Utf8, true),
        Field::new("tls_version", DataType::Utf8, true),
        Field::new("upstream_latency_ms", DataType::Float64, true),
        Field::new("retry_count", DataType::Int64, true),
        Field::new("error_count", DataType::Int64, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(timestamp.finish()) as ArrayRef,
        Arc::new(level.finish()) as ArrayRef,
        Arc::new(message.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            status.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(duration_ms)) as ArrayRef,
        Arc::new(cluster.finish()) as ArrayRef,
        Arc::new(node.finish()) as ArrayRef,
        Arc::new(namespace.finish()) as ArrayRef,
        Arc::new(pod.finish()) as ArrayRef,
        Arc::new(container.finish()) as ArrayRef,
        Arc::new(service.finish()) as ArrayRef,
        Arc::new(trace_id.finish()) as ArrayRef,
        Arc::new(span_id.finish()) as ArrayRef,
        Arc::new(request_id.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            session_id.into_iter().map(|n| n as i64).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(user.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_sent.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_received
                .into_iter()
                .map(i64::from)
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(user_agent.finish()) as ArrayRef,
        Arc::new(content_type.finish()) as ArrayRef,
        Arc::new(remote_addr.finish()) as ArrayRef,
        Arc::new(host.finish()) as ArrayRef,
        Arc::new(protocol.finish()) as ArrayRef,
        Arc::new(tls_version.finish()) as ArrayRef,
        Arc::new(Float64Array::from(upstream_latency_ms)) as ArrayRef,
        Arc::new(Int64Array::from(
            retry_count.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int64Array::from(
            error_count.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
    ];
    RecordBatch::try_new(schema, arrays)
        .unwrap_or_else(|err| panic!("wide batch generation failed for {count} rows: {err}"))
}

// ---------------------------------------------------------------------------
// Metadata helper
// ---------------------------------------------------------------------------

/// Create benchmark-standard `BatchMetadata` with typical K8s resource attributes.
pub fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::from([
            ("service.name".into(), "bench-service".into()),
            ("service.version".into(), "1.0.0".into()),
            ("host.name".into(), "bench-node-01".into()),
        ]),
        observed_time_ns: 1_705_312_200_000_000_000, // 2024-01-15T10:30:00Z
    }
}
