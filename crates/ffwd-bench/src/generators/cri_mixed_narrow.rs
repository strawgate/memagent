// gen_cri_k8s — CRI-formatted K8s container logs
// ---------------------------------------------------------------------------

/// Generate `count` CRI-formatted Kubernetes container log lines.
///
/// ~10% of lines are partial (P flag), the rest are full (F flag).
/// JSON payloads have variable schemas and line lengths (100–2000 bytes).
/// Output is deterministic for a given `(count, seed)` pair.
///
/// Format: `<timestamp> <stream> <P|F> <json-payload>\n`
pub fn gen_cri_k8s(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut buf = String::with_capacity(count * 350);

    for i in 0..count {
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let stream = pick(&mut rng, STREAMS);
        let is_partial = rng.u8(..10) == 0; // ~10% partial
        let flag = if is_partial { "P" } else { "F" };

        let _ = write!(buf, "2024-01-15T10:30:{sec:02}.{nano:09}Z {stream} {flag} ");

        // Vary the JSON payload complexity
        let variant = rng.u8(..10);
        let level = pick(&mut rng, LEVELS);
        let status = pick_u16(&mut rng, STATUS_CODES);
        let duration = rng.f64() * 500.0;

        if variant < 6 {
            // Short message (~150 bytes): 5-6 fields
            let path = pick(&mut rng, PATHS);
            let method = pick(&mut rng, METHODS);
            let _ = write!(
                buf,
                r#"{{"level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"request_id":"req-{:08x}"}}"#,
                rng.u32(..),
            );
        } else if variant < 9 {
            // Medium message (~400 bytes): nested fields, more attributes
            let ns = pick(&mut rng, NAMESPACES);
            let pod = pick(&mut rng, PODS);
            let container = pick(&mut rng, CONTAINERS);
            let path = pick(&mut rng, PATHS);
            let method = pick(&mut rng, METHODS);
            let trace_id = rng.u128(..);
            let span_id = rng.u64(..);
            let _ = write!(
                buf,
                r#"{{"level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"namespace":"{ns}","pod":"{pod}","container":"{container}","trace_id":"{trace_id:032x}","span_id":"{span_id:016x}","service":"{}","request_id":"req-{:08x}","bytes_sent":{}}}"#,
                pick(&mut rng, SERVICES),
                rng.u32(..),
                rng.u32(..65536),
            );
        } else {
            // Long message (~1500 bytes): stack trace
            let _ = write!(buf, r#"{{"level":"ERROR","message":""#);
            json_escape(STACK_TRACE, &mut buf);
            let _ = write!(
                buf,
                r#"","exception_class":"java.lang.NullPointerException","service":"{}","request_id":"req-{:08x}","status":500,"duration_ms":{duration:.1}}}"#,
                pick(&mut rng, SERVICES),
                rng.u32(..),
            );
        }

        buf.push('\n');
    }
    buf.into_bytes()
}

// ---------------------------------------------------------------------------
// gen_production_mixed — Non-uniform JSON lines
// ---------------------------------------------------------------------------

/// Generate `count` non-uniform JSON log lines simulating production traffic.
///
/// Distribution: 70% short (~150 bytes, 5-8 fields), 20% medium (~400 bytes,
/// nested), 10% long (~1500 bytes, stack traces with escaped newlines).
/// Field presence is variable (sparse).  Output is deterministic for a given
/// `(count, seed)` pair.
pub fn gen_production_mixed(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut cardinality = CardinalityState::new(CardinalityProfile::infra_like());
    let mut buf = String::with_capacity(count * 250);

    for i in 0..count {
        let sample = cardinality.sample(&mut rng);
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let ts = format!("2024-01-15T10:30:{sec:02}.{nano:09}Z");
        let variant = rng.u8(..10);
        let level = level_for_phase(sample.phase);
        let service = pick_by_idx(SERVICES, sample.service_idx);
        let namespace = pick_by_idx(NAMESPACES, sample.namespace_idx);
        let path = pick_by_idx(PATHS, sample.path_idx);
        let user = user_label(sample.user_idx);

        if variant < 7 {
            // Short: 5-8 fields, ~150 bytes
            let method = pick(&mut rng, METHODS);
            let status = sample.status_code;
            let duration = rng.f64()
                * match sample.phase {
                    SamplePhase::Hot => 250.0,
                    SamplePhase::Warm => 750.0,
                    SamplePhase::Cold => 2500.0,
                };
            let _ = write!(
                buf,
                r#"{{"timestamp":"{ts}","level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"service":"{service}""#,
            );
            // Sparse fields: ~50% chance each
            if sample.phase != SamplePhase::Hot && rng.bool() {
                let _ = write!(buf, r#","namespace":"{namespace}","user":"{user}""#);
            }
            if rng.bool() || sample.phase == SamplePhase::Cold {
                let _ = write!(
                    buf,
                    r#","request_id":"req-{:08x}""#,
                    sample.request_id as u32
                );
            }
            if rng.bool() {
                let _ = write!(buf, r#","bytes_sent":{}"#, rng.u32(..65536));
            }
            buf.push_str("}\n");
        } else if variant < 9 {
            // Medium: ~400 bytes, nested-like (K8s metadata + trace context)
            let cluster = &sample.cluster;
            let node = &sample.node;
            let pod = &sample.pod;
            let container = &sample.container;
            let method = pick(&mut rng, METHODS);
            let status = sample.status_code;
            let duration = rng.f64()
                * match sample.phase {
                    SamplePhase::Hot => 400.0,
                    SamplePhase::Warm => 900.0,
                    SamplePhase::Cold => 3000.0,
                };
            let _ = write!(
                buf,
                r#"{{"timestamp":"{ts}","level":"{level}","message":"{method} {path} HTTP/1.1","status":{status},"duration_ms":{duration:.1},"cluster":"{cluster}","node":"{node}","namespace":"{namespace}","pod":"{pod}","container":"{container}","trace_id":"{:032x}","span_id":"{:016x}","service":"{service}","request_id":"req-{:08x}","bytes_sent":{},"user_agent":"Mozilla/5.0","content_type":"application/json","session_id":{},"user":"{user}"}}"#,
                sample.trace_id,
                sample.span_id,
                sample.request_id as u32,
                rng.u32(..65536),
                sample.session_id,
            );
            buf.push('\n');
        } else {
            // Long: ~1500 bytes, stack traces
            let _ = write!(
                buf,
                r#"{{"timestamp":"{ts}","level":"{level}","message":""#,
                level = level_for_phase(sample.phase),
            );
            json_escape(STACK_TRACE, &mut buf);
            let _ = write!(
                buf,
                r#"","exception_class":"java.lang.NullPointerException","cluster":"{}","node":"{}","namespace":"{}","pod":"{}","container":"{}","service":"{}","request_id":"req-{:08x}","status":{},"duration_ms":{:.1},"trace_id":"{:032x}","span_id":"{:016x}","session_id":{},"retry_count":{},"error_count":{},"user":"{}"}}"#,
                sample.cluster,
                sample.node,
                namespace,
                sample.pod,
                sample.container,
                service,
                sample.request_id as u32,
                sample.status_code,
                rng.f64() * 5000.0,
                sample.trace_id,
                sample.span_id,
                sample.session_id,
                sample.retry_count,
                sample.error_count,
                user,
            );
            buf.push('\n');
        }
    }
    buf.into_bytes()
}

/// Generate non-uniform production-like logs directly as a detached
/// [`RecordBatch`].
pub fn gen_production_mixed_batch(count: usize, seed: u64) -> RecordBatch {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut cardinality = CardinalityState::new(CardinalityProfile::infra_like());

    let mut timestamp = StringBuilder::with_capacity(count, count.saturating_mul(32));
    let mut level = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut message = StringBuilder::with_capacity(count, count.saturating_mul(96));
    let mut status = Vec::with_capacity(count);
    let mut duration_ms = Vec::with_capacity(count);
    let mut service = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut namespace = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut user = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut request_id = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut bytes_sent = Vec::with_capacity(count);
    let mut cluster = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut node = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut pod = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut container = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut trace_id = StringBuilder::with_capacity(count, count.saturating_mul(32));
    let mut span_id = StringBuilder::with_capacity(count, count.saturating_mul(16));
    let mut session_id = Vec::with_capacity(count);
    let mut user_agent = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut content_type = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut retry_count = Vec::with_capacity(count);
    let mut error_count = Vec::with_capacity(count);
    let mut exception_class = StringBuilder::with_capacity(count, count.saturating_mul(48));
    let mut timestamp_buf = String::with_capacity(32);
    let mut message_buf = String::with_capacity(96);
    let mut request_id_buf = String::with_capacity(16);
    let mut user_buf = String::with_capacity(16);
    let mut trace_id_buf = String::with_capacity(32);
    let mut span_id_buf = String::with_capacity(16);

    for i in 0..count {
        let sample = cardinality.sample(&mut rng);
        let phase = sample.phase;
        let status_code = sample.status_code;
        let session_id_value = sample.session_id;
        let retry_count_value = sample.retry_count;
        let error_count_value = sample.error_count;
        let request_id_value = sample.request_id as u32;
        let service_idx = sample.service_idx;
        let namespace_idx = sample.namespace_idx;
        let path_idx = sample.path_idx;
        let user_idx = sample.user_idx;
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let variant = rng.u8(..10);
        let level_value = level_for_phase(phase);
        let service_value = pick_by_idx(SERVICES, service_idx);
        let namespace_value = pick_by_idx(NAMESPACES, namespace_idx);
        let path = pick_by_idx(PATHS, path_idx);
        append_user_label(&mut user_buf, user_idx);
        append_timestamp_utc(&mut timestamp_buf, sec, nano);
        timestamp.append_value(&timestamp_buf);
        level.append_value(level_value);
        service.append_value(service_value);

        if variant < 7 {
            let method = pick(&mut rng, METHODS);
            let status_value = status_code;
            let duration_value = round_tenths(
                rng.f64()
                    * match phase {
                        SamplePhase::Hot => 250.0,
                        SamplePhase::Warm => 750.0,
                        SamplePhase::Cold => 2500.0,
                    },
            );
            let include_namespace_user = phase != SamplePhase::Hot && rng.bool();
            message_buf.clear();
            let _ = write!(message_buf, "{method} {path} HTTP/1.1");
            message.append_value(&message_buf);
            status.push(status_value);
            duration_ms.push(duration_value);
            if include_namespace_user {
                namespace.append_value(namespace_value);
                user.append_value(&user_buf);
            } else {
                namespace.append_null();
                user.append_null();
            }
            if rng.bool() || phase == SamplePhase::Cold {
                request_id_buf.clear();
                let _ = write!(request_id_buf, "req-{request_id_value:08x}");
                request_id.append_value(&request_id_buf);
            } else {
                request_id.append_null();
            }
            bytes_sent.push(rng.bool().then(|| rng.u32(..65536)));
            cluster.append_null();
            node.append_null();
            pod.append_null();
            container.append_null();
            trace_id.append_null();
            span_id.append_null();
            session_id.push(None);
            user_agent.append_null();
            content_type.append_null();
            retry_count.push(None);
            error_count.push(None);
            exception_class.append_null();
        } else if variant < 9 {
            let method = pick(&mut rng, METHODS);
            let status_value = status_code;
            let duration_value = round_tenths(
                rng.f64()
                    * match phase {
                        SamplePhase::Hot => 400.0,
                        SamplePhase::Warm => 900.0,
                        SamplePhase::Cold => 3000.0,
                    },
            );
            message_buf.clear();
            let _ = write!(message_buf, "{method} {path} HTTP/1.1");
            message.append_value(&message_buf);
            status.push(status_value);
            duration_ms.push(duration_value);
            namespace.append_value(namespace_value);
            user.append_value(&user_buf);
            request_id_buf.clear();
            let _ = write!(request_id_buf, "req-{request_id_value:08x}");
            request_id.append_value(&request_id_buf);
            bytes_sent.push(Some(rng.u32(..65536)));
            cluster.append_value(sample.cluster);
            node.append_value(sample.node);
            pod.append_value(sample.pod);
            container.append_value(sample.container);
            trace_id_buf.clear();
            let _ = write!(trace_id_buf, "{:032x}", sample.trace_id);
            trace_id.append_value(&trace_id_buf);
            span_id_buf.clear();
            let _ = write!(span_id_buf, "{:016x}", sample.span_id);
            span_id.append_value(&span_id_buf);
            session_id.push(Some(session_id_value));
            user_agent.append_value("Mozilla/5.0");
            content_type.append_value("application/json");
            retry_count.push(None);
            error_count.push(None);
            exception_class.append_null();
        } else {
            message.append_value(STACK_TRACE);
            status.push(status_code);
            duration_ms.push(round_tenths(rng.f64() * 5000.0));
            namespace.append_value(namespace_value);
            user.append_value(&user_buf);
            request_id_buf.clear();
            let _ = write!(request_id_buf, "req-{request_id_value:08x}");
            request_id.append_value(&request_id_buf);
            bytes_sent.push(None);
            cluster.append_value(sample.cluster);
            node.append_value(sample.node);
            pod.append_value(sample.pod);
            container.append_value(sample.container);
            trace_id_buf.clear();
            let _ = write!(trace_id_buf, "{:032x}", sample.trace_id);
            trace_id.append_value(&trace_id_buf);
            span_id_buf.clear();
            let _ = write!(span_id_buf, "{:016x}", sample.span_id);
            span_id.append_value(&span_id_buf);
            session_id.push(Some(session_id_value));
            user_agent.append_null();
            content_type.append_null();
            retry_count.push(Some(retry_count_value));
            error_count.push(Some(error_count_value));
            exception_class.append_value("java.lang.NullPointerException");
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration_ms", DataType::Float64, true),
        Field::new("service", DataType::Utf8, true),
        Field::new("namespace", DataType::Utf8, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("request_id", DataType::Utf8, true),
        Field::new("bytes_sent", DataType::Int64, true),
        Field::new("cluster", DataType::Utf8, true),
        Field::new("node", DataType::Utf8, true),
        Field::new("pod", DataType::Utf8, true),
        Field::new("container", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("session_id", DataType::Int64, true),
        Field::new("user_agent", DataType::Utf8, true),
        Field::new("content_type", DataType::Utf8, true),
        Field::new("retry_count", DataType::Int64, true),
        Field::new("error_count", DataType::Int64, true),
        Field::new("exception_class", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(timestamp.finish()) as ArrayRef,
        Arc::new(level.finish()) as ArrayRef,
        Arc::new(message.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            status.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(duration_ms)) as ArrayRef,
        Arc::new(service.finish()) as ArrayRef,
        Arc::new(namespace.finish()) as ArrayRef,
        Arc::new(user.finish()) as ArrayRef,
        Arc::new(request_id.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_sent
                .into_iter()
                .map(|v| v.map(i64::from))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(cluster.finish()) as ArrayRef,
        Arc::new(node.finish()) as ArrayRef,
        Arc::new(pod.finish()) as ArrayRef,
        Arc::new(container.finish()) as ArrayRef,
        Arc::new(trace_id.finish()) as ArrayRef,
        Arc::new(span_id.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            session_id
                .into_iter()
                .map(|v| v.map(|n| n as i64))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(user_agent.finish()) as ArrayRef,
        Arc::new(content_type.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            retry_count
                .into_iter()
                .map(|v| v.map(i64::from))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int64Array::from(
            error_count
                .into_iter()
                .map(|v| v.map(i64::from))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(exception_class.finish()) as ArrayRef,
    ];
    RecordBatch::try_new(schema, arrays).unwrap_or_else(|err| {
        panic!("production mixed batch generation failed for {count} rows: {err}")
    })
}

// ---------------------------------------------------------------------------
// gen_narrow — Simple narrow JSON lines (5 fields, ~120 bytes/line)
// ---------------------------------------------------------------------------

/// Generate `count` narrow JSON log lines (~120 bytes each, 5 fields).
///
/// This is the simplest benchmark payload: uniform schema, short values.
/// Deterministic for a given `(count, seed)` pair.
pub fn gen_narrow(count: usize, seed: u64) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut buf = String::with_capacity(count * 130);

    for i in 0..count {
        let level = pick(&mut rng, LEVELS);
        let path = pick(&mut rng, PATHS);
        let status = pick_u16(&mut rng, STATUS_CODES);
        let duration = rng.f64() * 500.0;
        let _ = write!(
            buf,
            r#"{{"level":"{level}","message":"{} {path}","path":"{path}","status":{status},"duration_ms":{duration:.1}}}"#,
            METHODS[i % METHODS.len()],
        );
        buf.push('\n');
    }
    buf.into_bytes()
}

/// Generate narrow flat logs directly as a detached [`RecordBatch`].
pub fn gen_narrow_batch(count: usize, seed: u64) -> RecordBatch {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut level = Vec::with_capacity(count);
    let mut message = Vec::with_capacity(count);
    let mut path_col = Vec::with_capacity(count);
    let mut status = Vec::with_capacity(count);
    let mut duration_ms = Vec::with_capacity(count);

    for i in 0..count {
        let level_value = pick(&mut rng, LEVELS).to_string();
        let path = pick(&mut rng, PATHS).to_string();
        let status_value = pick_u16(&mut rng, STATUS_CODES);
        let duration_value = round_tenths(rng.f64() * 500.0);
        let method = METHODS[i % METHODS.len()];

        level.push(level_value);
        message.push(format!("{method} {path}"));
        path_col.push(path);
        status.push(status_value);
        duration_ms.push(duration_value);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration_ms", DataType::Float64, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(level)) as ArrayRef,
        Arc::new(StringArray::from(message)) as ArrayRef,
        Arc::new(StringArray::from(path_col)) as ArrayRef,
        Arc::new(Int64Array::from(
            status.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(duration_ms)) as ArrayRef,
    ];
    RecordBatch::try_new(schema, arrays)
        .unwrap_or_else(|err| panic!("narrow batch generation failed for {count} rows: {err}"))
}

