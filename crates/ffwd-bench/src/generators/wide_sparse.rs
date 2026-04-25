// ---------------------------------------------------------------------------
// gen_wide_sparse — Wide batch with nullable columns (~25% null rate)
// ---------------------------------------------------------------------------
//
// Mirrors `gen_wide_batch` but several "optional" columns have roughly 25% of
// rows set to null.  This exercises the `has_nulls = true` path in
// `encode_col_attr` and provides a stable regression baseline for encoders
// that must handle nullable Arrow arrays.
//
// Nullable columns (25% null rate):
//   upstream_latency_ms, retry_count, error_count, user, remote_addr,
//   tls_version, content_type
//
// All other columns remain fully dense (null_count == 0).

/// Generate a wide nullable [`RecordBatch`] with ~25% null rate on optional columns.
///
/// Deterministic for a given `(count, seed)` pair.
pub fn gen_wide_sparse_batch(count: usize, seed: u64) -> RecordBatch {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut cardinality = CardinalityState::new(CardinalityProfile::infra_like());

    // Dense columns (always have a value)
    let mut timestamp = StringBuilder::with_capacity(count, count.saturating_mul(32));
    let mut level = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut message = StringBuilder::with_capacity(count, count.saturating_mul(96));
    let mut status: Vec<i64> = Vec::with_capacity(count);
    let mut duration_ms: Vec<f64> = Vec::with_capacity(count);
    let mut cluster = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut node = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut namespace = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut pod = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut container = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut service = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut trace_id = StringBuilder::with_capacity(count, count.saturating_mul(32));
    let mut span_id = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut request_id = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut session_id: Vec<i64> = Vec::with_capacity(count);
    let mut bytes_sent: Vec<i64> = Vec::with_capacity(count);
    let mut bytes_received: Vec<i64> = Vec::with_capacity(count);
    let mut user_agent = StringBuilder::with_capacity(count, count.saturating_mul(40));
    let mut host = StringBuilder::with_capacity(count, count.saturating_mul(20));
    let mut protocol = StringBuilder::with_capacity(count, count.saturating_mul(12));

    // Sparse columns (~25% null rate)
    let mut upstream_latency_ms: Vec<Option<f64>> = Vec::with_capacity(count);
    let mut retry_count: Vec<Option<i64>> = Vec::with_capacity(count);
    let mut error_count: Vec<Option<i64>> = Vec::with_capacity(count);
    let mut user = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut remote_addr = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut tls_version = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut content_type = StringBuilder::with_capacity(count, count.saturating_mul(24));

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
        let service_idx = sample.service_idx;
        let namespace_idx = sample.namespace_idx;
        let path_idx = sample.path_idx;
        let user_idx = sample.user_idx;
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let level_value = level_for_phase(phase);
        let path = pick_by_idx(PATHS, path_idx);
        let method = pick(&mut rng, METHODS);
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

        // ~25% null: use the low two bits of a fresh random byte as independent
        // null signals for each sparse column group.
        // Each sparse column independently has ~25% probability of being null.
        let null_a = rng.u8(..) < 64; // upstream_latency_ms, retry_count
        let null_b = rng.u8(..) < 64; // error_count, user
        let null_c = rng.u8(..) < 64; // remote_addr, tls_version
        let null_d = rng.u8(..) < 64; // content_type

        append_timestamp_utc(&mut timestamp_buf, sec, nano);
        timestamp.append_value(&timestamp_buf);
        level.append_value(level_value);
        message_buf.clear();
        let _ = write!(message_buf, "{method} {path} HTTP/1.1");
        message.append_value(&message_buf);
        status.push(i64::from(status_code));
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
        session_id.push(session_id_value as i64);
        bytes_sent.push(i64::from(rng.u32(..65536)));
        bytes_received.push(i64::from(rng.u32(..65536)));
        user_agent.append_value("Mozilla/5.0 (X11; Linux x86_64)");
        host.append_value("api.example.com");
        protocol.append_value("HTTP/1.1");

        // Sparse columns
        if null_a {
            upstream_latency_ms.push(None);
            retry_count.push(None);
        } else {
            upstream_latency_ms.push(Some(round_tenths(rng.f64() * 200.0)));
            retry_count.push(Some(i64::from(sample.retry_count)));
        }
        if null_b {
            error_count.push(None);
            user.append_null();
        } else {
            error_count.push(Some(i64::from(sample.error_count)));
            append_user_label(&mut user_buf, user_idx);
            user.append_value(&user_buf);
        }
        if null_c {
            remote_addr.append_null();
            tls_version.append_null();
        } else {
            remote_addr_buf.clear();
            let _ = write!(
                remote_addr_buf,
                "10.{}.{}.{}",
                rng.u8(..),
                rng.u8(..),
                rng.u8(..)
            );
            remote_addr.append_value(&remote_addr_buf);
            tls_version.append_value("TLSv1.3");
        }
        if null_d {
            content_type.append_null();
        } else {
            content_type.append_value("application/json");
        }
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
        Field::new("bytes_sent", DataType::Int64, true),
        Field::new("bytes_received", DataType::Int64, true),
        Field::new("user_agent", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, true),
        Field::new("protocol", DataType::Utf8, true),
        Field::new("upstream_latency_ms", DataType::Float64, true),
        Field::new("retry_count", DataType::Int64, true),
        Field::new("error_count", DataType::Int64, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("remote_addr", DataType::Utf8, true),
        Field::new("tls_version", DataType::Utf8, true),
        Field::new("content_type", DataType::Utf8, true),
    ]));

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(timestamp.finish()) as ArrayRef,
        Arc::new(level.finish()) as ArrayRef,
        Arc::new(message.finish()) as ArrayRef,
        Arc::new(Int64Array::from(status)) as ArrayRef,
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
        Arc::new(Int64Array::from(session_id)) as ArrayRef,
        Arc::new(Int64Array::from(bytes_sent)) as ArrayRef,
        Arc::new(Int64Array::from(bytes_received)) as ArrayRef,
        Arc::new(user_agent.finish()) as ArrayRef,
        Arc::new(host.finish()) as ArrayRef,
        Arc::new(protocol.finish()) as ArrayRef,
        Arc::new(Float64Array::from(upstream_latency_ms)) as ArrayRef,
        Arc::new(Int64Array::from(retry_count)) as ArrayRef,
        Arc::new(Int64Array::from(error_count)) as ArrayRef,
        Arc::new(user.finish()) as ArrayRef,
        Arc::new(remote_addr.finish()) as ArrayRef,
        Arc::new(tls_version.finish()) as ArrayRef,
        Arc::new(content_type.finish()) as ArrayRef,
    ];

    RecordBatch::try_new(schema, arrays)
        .unwrap_or_else(|err| panic!("wide sparse batch generation failed for {count} rows: {err}"))
}
