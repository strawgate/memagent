#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EnvoyAccessKind {
    PublicRead,
    PublicWrite,
    Auth,
    Search,
    StaticAssets,
    Metrics,
    Graphql,
}

#[derive(Clone, Copy, Debug)]
struct EnvoyAccessScenario {
    kind: EnvoyAccessKind,
    weight: usize,
    service: &'static str,
    authority: &'static str,
    route_prefix: &'static str,
    cluster_prefix: &'static str,
    status_weights: &'static [(u16, usize)],
    flag_weights: &'static [(&'static str, usize)],
    response_code_details: &'static [&'static str],
}

/// Tunable parameters for Envoy-like access log generation.
#[derive(Clone, Copy, Debug)]
pub struct EnvoyAccessProfile {
    /// Number of distinct route/cluster variants per scenario.
    pub cardinality_scale: usize,
    /// Minimum number of consecutive rows that share the same scenario.
    pub burst_min: usize,
    /// Maximum number of consecutive rows that share the same scenario.
    pub burst_max: usize,
    /// Number of distinct source IP addresses in the pool.
    pub source_ip_pool_size: usize,
    /// Number of distinct user-agent strings in the pool.
    pub user_agent_pool_size: usize,
    /// Maximum number of `x-forwarded-for` hops.
    pub xff_hops_max: usize,
}

impl EnvoyAccessProfile {
    pub fn benchmark() -> Self {
        Self {
            cardinality_scale: 1,
            burst_min: 4,
            burst_max: 24,
            source_ip_pool_size: 32,
            user_agent_pool_size: 12,
            xff_hops_max: 3,
        }
    }

    pub fn for_scale(scale: usize) -> Self {
        let scale = scale.max(1);
        Self {
            cardinality_scale: scale,
            burst_min: 3 + scale.min(4),
            burst_max: 12 + scale * 4,
            source_ip_pool_size: 32 * scale,
            user_agent_pool_size: 12 + scale * 6,
            xff_hops_max: 2 + scale.min(2),
        }
    }
}

impl Default for EnvoyAccessProfile {
    fn default() -> Self {
        Self::benchmark()
    }
}

const ENVOY_STATUS_PUBLIC_READ: &[(u16, usize)] =
    &[(200, 82), (404, 7), (401, 4), (429, 4), (500, 3)];
const ENVOY_STATUS_PUBLIC_WRITE: &[(u16, usize)] = &[
    (201, 48),
    (200, 16),
    (409, 16),
    (422, 10),
    (500, 6),
    (503, 4),
];
const ENVOY_STATUS_AUTH: &[(u16, usize)] = &[(200, 65), (401, 18), (403, 10), (429, 4), (500, 3)];
const ENVOY_STATUS_SEARCH: &[(u16, usize)] = &[(200, 76), (404, 4), (429, 10), (503, 7), (500, 3)];
const ENVOY_STATUS_STATIC: &[(u16, usize)] = &[(200, 88), (304, 6), (404, 4), (503, 2)];
const ENVOY_STATUS_METRICS: &[(u16, usize)] = &[(200, 92), (503, 8)];
const ENVOY_STATUS_GRAPHQL: &[(u16, usize)] = &[(200, 78), (400, 8), (429, 6), (500, 6), (502, 2)];

const ENVOY_FLAG_PUBLIC_READ: &[(&str, usize)] = &[("-", 84), ("NR", 10), ("UF", 4), ("UH", 2)];
const ENVOY_FLAG_PUBLIC_WRITE: &[(&str, usize)] = &[
    ("-", 70),
    ("UF", 10),
    ("UH", 6),
    ("UT", 7),
    ("LR", 5),
    ("DC", 2),
];
const ENVOY_FLAG_AUTH: &[(&str, usize)] = &[("-", 82), ("NR", 6), ("UF", 6), ("LR", 4), ("DC", 2)];
const ENVOY_FLAG_SEARCH: &[(&str, usize)] = &[
    ("-", 74),
    ("UT", 8),
    ("UF", 8),
    ("UH", 5),
    ("LR", 3),
    ("NR", 2),
];
const ENVOY_FLAG_STATIC: &[(&str, usize)] = &[("-", 90), ("NR", 6), ("DC", 4)];
const ENVOY_FLAG_METRICS: &[(&str, usize)] = &[("-", 95), ("UF", 3), ("UT", 2)];
const ENVOY_FLAG_GRAPHQL: &[(&str, usize)] =
    &[("-", 78), ("UF", 10), ("UT", 6), ("UH", 4), ("LR", 2)];

const ENVOY_SCENARIOS: &[EnvoyAccessScenario] = &[
    EnvoyAccessScenario {
        kind: EnvoyAccessKind::PublicRead,
        weight: 28,
        service: "user-service",
        authority: "users.api.example.com",
        route_prefix: "users-read",
        cluster_prefix: "user-service",
        status_weights: ENVOY_STATUS_PUBLIC_READ,
        flag_weights: ENVOY_FLAG_PUBLIC_READ,
        response_code_details: &[
            "via_upstream",
            "route_not_found",
            "upstream_reset_before_response_started{remote_reset}",
        ],
    },
    EnvoyAccessScenario {
        kind: EnvoyAccessKind::PublicWrite,
        weight: 20,
        service: "order-service",
        authority: "orders.api.example.com",
        route_prefix: "orders-write",
        cluster_prefix: "order-service",
        status_weights: ENVOY_STATUS_PUBLIC_WRITE,
        flag_weights: ENVOY_FLAG_PUBLIC_WRITE,
        response_code_details: &[
            "via_upstream",
            "upstream_request_timeout",
            "upstream_reset_before_response_started{connection_termination}",
        ],
    },
    EnvoyAccessScenario {
        kind: EnvoyAccessKind::Auth,
        weight: 14,
        service: "auth-service",
        authority: "auth.example.com",
        route_prefix: "auth",
        cluster_prefix: "auth-service",
        status_weights: ENVOY_STATUS_AUTH,
        flag_weights: ENVOY_FLAG_AUTH,
        response_code_details: &[
            "via_upstream",
            "rbac_access_denied",
            "upstream_reset_before_response_started{remote_reset}",
        ],
    },
    EnvoyAccessScenario {
        kind: EnvoyAccessKind::Search,
        weight: 12,
        service: "search-service",
        authority: "search.example.com",
        route_prefix: "search",
        cluster_prefix: "search-service",
        status_weights: ENVOY_STATUS_SEARCH,
        flag_weights: ENVOY_FLAG_SEARCH,
        response_code_details: &["via_upstream", "rate_limited", "upstream_overflow"],
    },
    EnvoyAccessScenario {
        kind: EnvoyAccessKind::StaticAssets,
        weight: 18,
        service: "frontend",
        authority: "www.example.com",
        route_prefix: "static-assets",
        cluster_prefix: "frontend",
        status_weights: ENVOY_STATUS_STATIC,
        flag_weights: ENVOY_FLAG_STATIC,
        response_code_details: &["via_upstream", "route_not_found", "cached"],
    },
    EnvoyAccessScenario {
        kind: EnvoyAccessKind::Metrics,
        weight: 5,
        service: "metrics-collector",
        authority: "metrics.example.com",
        route_prefix: "metrics",
        cluster_prefix: "metrics-collector",
        status_weights: ENVOY_STATUS_METRICS,
        flag_weights: ENVOY_FLAG_METRICS,
        response_code_details: &[
            "via_upstream",
            "upstream_reset_before_response_started{connect_failure}",
        ],
    },
    EnvoyAccessScenario {
        kind: EnvoyAccessKind::Graphql,
        weight: 7,
        service: "api-gateway",
        authority: "api.example.com",
        route_prefix: "graphql",
        cluster_prefix: "api-gateway",
        status_weights: ENVOY_STATUS_GRAPHQL,
        flag_weights: ENVOY_FLAG_GRAPHQL,
        response_code_details: &[
            "via_upstream",
            "upstream_request_timeout",
            "upstream_reset_before_response_started{remote_reset}",
        ],
    },
];

/// Generate synthetic Envoy edge access logs as flat JSON lines.
///
/// The generator intentionally correlates route, service, cluster, status,
/// user-agent, and source-IP locality so the payload resembles a real edge
/// proxy rather than independent random columns.
pub fn gen_envoy_access(count: usize, seed: u64) -> Vec<u8> {
    gen_envoy_access_with_profile(count, seed, EnvoyAccessProfile::benchmark())
}

/// Generate synthetic Envoy access logs directly as a detached [`RecordBatch`].
pub fn gen_envoy_access_batch(count: usize, seed: u64) -> RecordBatch {
    gen_envoy_access_batch_with_profile(count, seed, EnvoyAccessProfile::benchmark())
}

/// Generate synthetic Envoy edge access logs using a tunable realism profile.
pub fn gen_envoy_access_with_profile(
    count: usize,
    seed: u64,
    profile: EnvoyAccessProfile,
) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut buf = String::with_capacity(count * 420);

    let mut burst_remaining = 0usize;
    let mut current_scenario = ENVOY_SCENARIOS[0];
    let mut current_source_bucket = 0usize;
    let mut current_client_bucket = 0usize;

    for i in 0..count {
        if burst_remaining == 0 {
            current_scenario = weighted_choice_pick(&mut rng, ENVOY_SCENARIOS);
            let burst_span = profile.burst_max.saturating_sub(profile.burst_min);
            let burst_len = profile.burst_min + rng.usize(..=burst_span.max(1));
            burst_remaining = burst_len.max(1);
            current_source_bucket = rng.usize(..profile.source_ip_pool_size.max(1));
            current_client_bucket = rng.usize(..profile.user_agent_pool_size.max(1));
        }
        burst_remaining -= 1;

        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let method = match current_scenario.kind {
            EnvoyAccessKind::PublicRead => pick(&mut rng, &["GET", "GET", "GET", "HEAD"]),
            EnvoyAccessKind::PublicWrite => pick(&mut rng, &["POST", "POST", "PUT", "PATCH"]),
            EnvoyAccessKind::Auth => pick(&mut rng, &["POST", "POST", "POST", "DELETE"]),
            EnvoyAccessKind::Search => pick(&mut rng, &["GET", "GET", "GET", "POST"]),
            EnvoyAccessKind::StaticAssets => pick(&mut rng, &["GET", "GET", "HEAD"]),
            EnvoyAccessKind::Metrics => pick(&mut rng, &["GET", "GET", "GET"]),
            EnvoyAccessKind::Graphql => pick(&mut rng, &["POST", "POST", "GET"]),
        };
        let route_variant = rng.usize(..profile.cardinality_scale.max(1) * 4);
        let route_name = format!("{}-v{}", current_scenario.route_prefix, route_variant);
        let upstream_cluster = format!("{}-v{}", current_scenario.cluster_prefix, route_variant);
        let host_bucket =
            (current_source_bucket + route_variant) % (16 * profile.cardinality_scale.max(1) + 16);
        let source_ip = make_source_ip(current_source_bucket, i);
        let direct_ip = make_direct_ip(host_bucket, i);
        let response_code = weighted_pick(&mut rng, current_scenario.status_weights);
        let response_flags =
            pick_response_flag(&mut rng, current_scenario.flag_weights, response_code);
        let response_code_details = pick_response_code_detail(
            &mut rng,
            current_scenario.response_code_details,
            response_code,
        );
        let user_agent = make_user_agent(
            &mut rng,
            current_scenario.kind,
            current_client_bucket,
            profile.user_agent_pool_size.max(1),
        );
        let path = make_path(&mut rng, current_scenario.kind, route_variant);
        let authority = current_scenario.authority;
        let bytes_received = match current_scenario.kind {
            EnvoyAccessKind::PublicRead
            | EnvoyAccessKind::StaticAssets
            | EnvoyAccessKind::Metrics => rng.u32(..512),
            EnvoyAccessKind::Search => rng.u32(..1024),
            EnvoyAccessKind::Auth => rng.u32(..256),
            EnvoyAccessKind::PublicWrite | EnvoyAccessKind::Graphql => rng.u32(..2048),
        };
        let bytes_sent = match response_code {
            200 | 201 | 204 | 304 => 200 + rng.u32(..16_000),
            400 | 401 | 403 | 404 => 64 + rng.u32(..1_024),
            409 | 422 | 429 => 32 + rng.u32(..512),
            _ => 128 + rng.u32(..8_192),
        };
        let duration_ms = match current_scenario.kind {
            EnvoyAccessKind::Metrics => 3.0 + rng.f64() * 18.0,
            EnvoyAccessKind::StaticAssets => 4.0 + rng.f64() * 24.0,
            EnvoyAccessKind::Auth => 12.0 + rng.f64() * 75.0,
            EnvoyAccessKind::PublicRead => 8.0 + rng.f64() * 90.0,
            EnvoyAccessKind::Search => 15.0 + rng.f64() * 140.0,
            EnvoyAccessKind::PublicWrite => 20.0 + rng.f64() * 180.0,
            EnvoyAccessKind::Graphql => 25.0 + rng.f64() * 220.0,
        };
        let upstream_service_time_ms = (response_code >= 500 || rng.usize(..100) < 90)
            .then(|| (duration_ms * (0.35 + rng.f64() * 0.45)).max(1.0));
        let xff =
            make_x_forwarded_for(&mut rng, current_source_bucket, profile.xff_hops_max.max(1));
        let is_tls = current_scenario.kind != EnvoyAccessKind::Metrics && rng.usize(..100) < 96;
        let tls_version = if is_tls {
            pick(&mut rng, &["TLSv1.3", "TLSv1.2"])
        } else {
            "-"
        };
        let protocol = if matches!(
            current_scenario.kind,
            EnvoyAccessKind::Metrics | EnvoyAccessKind::StaticAssets
        ) && rng.usize(..100) < 25
        {
            "HTTP/1.1"
        } else {
            "HTTP/2"
        };

        let _ = write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:{sec:02}.{nano:09}Z","method":"{method}","path":"{path}","protocol":"{protocol}","response_code":{response_code},"response_flags":"{response_flags}","response_code_details":"{response_code_details}","bytes_received":{bytes_received},"bytes_sent":{bytes_sent},"duration_ms":{duration_ms:.1},"upstream_service_time_ms":{},"user_agent":"{user_agent}","x_request_id":"{}","authority":"{authority}","route_name":"{route_name}","service":"{}","upstream_cluster":"{upstream_cluster}","upstream_host":"{}","downstream_remote_address":"{source_ip}","downstream_direct_remote_address":"{direct_ip}","x_forwarded_for":"{xff}","tls_version":"{tls_version}"}}"#,
            upstream_service_time_ms.map_or_else(|| "null".to_string(), |ms| format!("{ms:.1}")),
            {
                let mut request_id = String::with_capacity(36);
                append_uuid_like(&mut rng, &mut request_id);
                request_id
            },
            current_scenario.service,
            {
                let mut upstream_host = String::new();
                let o1 = 10 + (current_source_bucket % 20) as u8;
                let o2 = (route_variant % 250) as u8;
                let o3 = ((i + current_client_bucket) % 250) as u8;
                let o4 = 10 + ((route_variant + current_client_bucket) % 200) as u8;
                let port = 8080 + ((route_variant + current_client_bucket) % 5) as u16;
                append_ipv4(&mut upstream_host, o1, o2, o3, o4, port);
                upstream_host
            },
        );
        buf.push('\n');
    }

    buf.into_bytes()
}

/// Generate synthetic Envoy access logs as a detached [`RecordBatch`] using a
/// tunable realism profile.
pub fn gen_envoy_access_batch_with_profile(
    count: usize,
    seed: u64,
    profile: EnvoyAccessProfile,
) -> RecordBatch {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut burst_remaining = 0usize;
    let mut current_scenario = ENVOY_SCENARIOS[0];
    let mut current_source_bucket = 0usize;
    let mut current_client_bucket = 0usize;

    let mut timestamp = StringBuilder::with_capacity(count, count.saturating_mul(32));
    let mut method = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut path = StringBuilder::with_capacity(count, count.saturating_mul(40));
    let mut protocol = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut response_code = Vec::with_capacity(count);
    let mut response_flags = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut response_code_details = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut bytes_received = Vec::with_capacity(count);
    let mut bytes_sent = Vec::with_capacity(count);
    let mut duration_ms = Vec::with_capacity(count);
    let mut upstream_service_time_ms = Vec::with_capacity(count);
    let mut user_agent = StringBuilder::with_capacity(count, count.saturating_mul(72));
    let mut x_request_id = StringBuilder::with_capacity(count, count.saturating_mul(36));
    let mut authority = StringBuilder::with_capacity(count, count.saturating_mul(28));
    let mut route_name = StringBuilder::with_capacity(count, count.saturating_mul(28));
    let mut service = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut upstream_cluster = StringBuilder::with_capacity(count, count.saturating_mul(28));
    let mut upstream_host = StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut downstream_remote_address =
        StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut downstream_direct_remote_address =
        StringBuilder::with_capacity(count, count.saturating_mul(24));
    let mut x_forwarded_for = StringBuilder::with_capacity(count, count.saturating_mul(48));
    let mut tls_version = StringBuilder::with_capacity(count, count.saturating_mul(8));
    let mut timestamp_buf = String::with_capacity(32);
    let mut request_id_buf = String::with_capacity(36);
    let mut route_name_buf = String::with_capacity(32);
    let mut upstream_cluster_buf = String::with_capacity(32);
    let mut upstream_host_buf = String::with_capacity(32);
    let mut source_ip_buf = String::with_capacity(24);
    let mut direct_ip_buf = String::with_capacity(24);
    let mut xff_buf = String::with_capacity(64);

    for i in 0..count {
        if burst_remaining == 0 {
            current_scenario = weighted_choice_pick(&mut rng, ENVOY_SCENARIOS);
            let burst_span = profile.burst_max.saturating_sub(profile.burst_min);
            let burst_len = profile.burst_min + rng.usize(..=burst_span.max(1));
            burst_remaining = burst_len.max(1);
            current_source_bucket = rng.usize(..profile.source_ip_pool_size.max(1));
            current_client_bucket = rng.usize(..profile.user_agent_pool_size.max(1));
        }
        burst_remaining -= 1;

        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let method_value = match current_scenario.kind {
            EnvoyAccessKind::PublicRead => pick(&mut rng, &["GET", "GET", "GET", "HEAD"]),
            EnvoyAccessKind::PublicWrite => pick(&mut rng, &["POST", "POST", "PUT", "PATCH"]),
            EnvoyAccessKind::Auth => pick(&mut rng, &["POST", "POST", "POST", "DELETE"]),
            EnvoyAccessKind::Search => pick(&mut rng, &["GET", "GET", "GET", "POST"]),
            EnvoyAccessKind::StaticAssets => pick(&mut rng, &["GET", "GET", "HEAD"]),
            EnvoyAccessKind::Metrics => pick(&mut rng, &["GET", "GET", "GET"]),
            EnvoyAccessKind::Graphql => pick(&mut rng, &["POST", "POST", "GET"]),
        };
        let route_variant = rng.usize(..profile.cardinality_scale.max(1) * 4);
        route_name_buf.clear();
        let _ = write!(
            route_name_buf,
            "{}-v{}",
            current_scenario.route_prefix, route_variant
        );
        upstream_cluster_buf.clear();
        let _ = write!(
            upstream_cluster_buf,
            "{}-v{}",
            current_scenario.cluster_prefix, route_variant
        );
        let host_bucket =
            (current_source_bucket + route_variant) % (16 * profile.cardinality_scale.max(1) + 16);
        make_source_ip_into(current_source_bucket, i, &mut source_ip_buf);
        make_direct_ip_into(host_bucket, i, &mut direct_ip_buf);
        let response_code_value = weighted_pick(&mut rng, current_scenario.status_weights);
        let response_flags_value =
            pick_response_flag(&mut rng, current_scenario.flag_weights, response_code_value);
        let response_code_details_value = pick_response_code_detail(
            &mut rng,
            current_scenario.response_code_details,
            response_code_value,
        );
        let user_agent_value = make_user_agent(
            &mut rng,
            current_scenario.kind,
            current_client_bucket,
            profile.user_agent_pool_size.max(1),
        );
        let path_value = make_path(&mut rng, current_scenario.kind, route_variant);
        let bytes_received_value = match current_scenario.kind {
            EnvoyAccessKind::PublicRead
            | EnvoyAccessKind::StaticAssets
            | EnvoyAccessKind::Metrics => rng.u32(..512),
            EnvoyAccessKind::Search => rng.u32(..1024),
            EnvoyAccessKind::Auth => rng.u32(..256),
            EnvoyAccessKind::PublicWrite | EnvoyAccessKind::Graphql => rng.u32(..2048),
        };
        let bytes_sent_value = match response_code_value {
            200 | 201 | 204 | 304 => 200 + rng.u32(..16_000),
            400 | 401 | 403 | 404 => 64 + rng.u32(..1_024),
            409 | 422 | 429 => 32 + rng.u32(..512),
            _ => 128 + rng.u32(..8_192),
        };
        let raw_duration_ms_value = match current_scenario.kind {
            EnvoyAccessKind::Metrics => 3.0 + rng.f64() * 18.0,
            EnvoyAccessKind::StaticAssets => 4.0 + rng.f64() * 24.0,
            EnvoyAccessKind::Auth => 12.0 + rng.f64() * 75.0,
            EnvoyAccessKind::PublicRead => 8.0 + rng.f64() * 90.0,
            EnvoyAccessKind::Search => 15.0 + rng.f64() * 140.0,
            EnvoyAccessKind::PublicWrite => 20.0 + rng.f64() * 180.0,
            EnvoyAccessKind::Graphql => 25.0 + rng.f64() * 220.0,
        };
        let upstream_service_time_ms_value = (response_code_value >= 500
            || rng.usize(..100) < 90)
            .then(|| round_tenths((raw_duration_ms_value * (0.35 + rng.f64() * 0.45)).max(1.0)));
        let duration_ms_value = round_tenths(raw_duration_ms_value);
        make_x_forwarded_for_into(
            &mut rng,
            current_source_bucket,
            profile.xff_hops_max.max(1),
            &mut xff_buf,
        );
        let is_tls = current_scenario.kind != EnvoyAccessKind::Metrics && rng.usize(..100) < 96;
        let tls_version_value = if is_tls {
            pick(&mut rng, &["TLSv1.3", "TLSv1.2"])
        } else {
            "-"
        };
        let protocol_value = if matches!(
            current_scenario.kind,
            EnvoyAccessKind::Metrics | EnvoyAccessKind::StaticAssets
        ) && rng.usize(..100) < 25
        {
            "HTTP/1.1"
        } else {
            "HTTP/2"
        };

        append_uuid_like(&mut rng, &mut request_id_buf);
        upstream_host_buf.clear();
        let o1 = 10 + (current_source_bucket % 20) as u8;
        let o2 = (route_variant % 250) as u8;
        let o3 = ((i + current_client_bucket) % 250) as u8;
        let o4 = 10 + ((route_variant + current_client_bucket) % 200) as u8;
        let port = 8080 + ((route_variant + current_client_bucket) % 5) as u16;
        append_ipv4(&mut upstream_host_buf, o1, o2, o3, o4, port);

        timestamp_buf.clear();
        let _ = write!(timestamp_buf, "2024-01-15T10:30:{sec:02}.{nano:09}Z");

        timestamp.append_value(&timestamp_buf);
        method.append_value(method_value);
        path.append_value(&path_value);
        protocol.append_value(protocol_value);
        response_code.push(response_code_value);
        response_flags.append_value(response_flags_value);
        response_code_details.append_value(response_code_details_value);
        bytes_received.push(bytes_received_value);
        bytes_sent.push(bytes_sent_value);
        duration_ms.push(duration_ms_value);
        upstream_service_time_ms.push(upstream_service_time_ms_value);
        user_agent.append_value(&user_agent_value);
        x_request_id.append_value(&request_id_buf);
        authority.append_value(current_scenario.authority);
        route_name.append_value(&route_name_buf);
        service.append_value(current_scenario.service);
        upstream_cluster.append_value(&upstream_cluster_buf);
        upstream_host.append_value(&upstream_host_buf);
        downstream_remote_address.append_value(&source_ip_buf);
        downstream_direct_remote_address.append_value(&direct_ip_buf);
        x_forwarded_for.append_value(&xff_buf);
        tls_version.append_value(tls_version_value);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("method", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("protocol", DataType::Utf8, true),
        Field::new("response_code", DataType::Int64, true),
        Field::new("response_flags", DataType::Utf8, true),
        Field::new("response_code_details", DataType::Utf8, true),
        Field::new("bytes_received", DataType::Int64, true),
        Field::new("bytes_sent", DataType::Int64, true),
        Field::new("duration_ms", DataType::Float64, true),
        Field::new("upstream_service_time_ms", DataType::Float64, true),
        Field::new("user_agent", DataType::Utf8, true),
        Field::new("x_request_id", DataType::Utf8, true),
        Field::new("authority", DataType::Utf8, true),
        Field::new("route_name", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, true),
        Field::new("upstream_cluster", DataType::Utf8, true),
        Field::new("upstream_host", DataType::Utf8, true),
        Field::new("downstream_remote_address", DataType::Utf8, true),
        Field::new("downstream_direct_remote_address", DataType::Utf8, true),
        Field::new("x_forwarded_for", DataType::Utf8, true),
        Field::new("tls_version", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(timestamp.finish()) as ArrayRef,
        Arc::new(method.finish()) as ArrayRef,
        Arc::new(path.finish()) as ArrayRef,
        Arc::new(protocol.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            response_code.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(response_flags.finish()) as ArrayRef,
        Arc::new(response_code_details.finish()) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_received
                .into_iter()
                .map(i64::from)
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_sent.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(duration_ms)) as ArrayRef,
        Arc::new(Float64Array::from(upstream_service_time_ms)) as ArrayRef,
        Arc::new(user_agent.finish()) as ArrayRef,
        Arc::new(x_request_id.finish()) as ArrayRef,
        Arc::new(authority.finish()) as ArrayRef,
        Arc::new(route_name.finish()) as ArrayRef,
        Arc::new(service.finish()) as ArrayRef,
        Arc::new(upstream_cluster.finish()) as ArrayRef,
        Arc::new(upstream_host.finish()) as ArrayRef,
        Arc::new(downstream_remote_address.finish()) as ArrayRef,
        Arc::new(downstream_direct_remote_address.finish()) as ArrayRef,
        Arc::new(x_forwarded_for.finish()) as ArrayRef,
        Arc::new(tls_version.finish()) as ArrayRef,
    ];
    RecordBatch::try_new(schema, arrays)
        .unwrap_or_else(|err| panic!("envoy batch generation failed for {count} rows: {err}"))
}

fn make_source_ip(bucket: usize, salt: usize) -> String {
    let mut out = String::with_capacity(24);
    make_source_ip_into(bucket, salt, &mut out);
    out
}

fn make_source_ip_into(bucket: usize, salt: usize, out: &mut String) {
    out.clear();
    let a = 203u8;
    let b = 0u8;
    let c = (bucket % 250) as u8;
    let d = (17 + (salt % 200)) as u8;
    let port = 40_000 + (bucket as u16 % 20_000);
    append_ipv4(out, a, b, c, d, port);
}

fn make_direct_ip(bucket: usize, salt: usize) -> String {
    let mut out = String::with_capacity(24);
    make_direct_ip_into(bucket, salt, &mut out);
    out
}

fn make_direct_ip_into(bucket: usize, salt: usize, out: &mut String) {
    out.clear();
    let a = 10u8;
    let b = 1u8 + ((bucket / 32) % 10) as u8;
    let c = (bucket % 32) as u8;
    let d = (salt % 250) as u8;
    let port = 44_300 + (bucket as u16 % 700);
    append_ipv4(out, a, b, c, d, port);
}

fn make_x_forwarded_for(rng: &mut fastrand::Rng, bucket: usize, max_hops: usize) -> String {
    let mut out = String::with_capacity(64);
    make_x_forwarded_for_into(rng, bucket, max_hops, &mut out);
    out
}

fn make_x_forwarded_for_into(
    rng: &mut fastrand::Rng,
    bucket: usize,
    max_hops: usize,
    out: &mut String,
) {
    let hops = 1 + rng.usize(..max_hops.max(1));
    out.clear();
    for hop in 0..hops {
        if hop > 0 {
            out.push_str(", ");
        }
        let a = if hop == 0 { 203 } else { 172 };
        let b = if hop == 0 { 0 } else { 16 };
        let c = ((bucket + hop) % 250) as u8;
        let d = (1 + ((bucket + hop * 7) % 200)) as u8;
        append_ipv4_without_port(out, a, b, c, d);
    }
}

fn pick_response_flag(
    rng: &mut fastrand::Rng,
    weights: &[(&'static str, usize)],
    response_code: u16,
) -> &'static str {
    if response_code < 400 {
        return "-";
    }
    weighted_pick(rng, weights)
}

fn pick_response_code_detail(
    rng: &mut fastrand::Rng,
    options: &[&'static str],
    response_code: u16,
) -> &'static str {
    if response_code < 400 {
        return "via_upstream";
    }
    options[rng.usize(..options.len().max(1))]
}

fn make_path(rng: &mut fastrand::Rng, kind: EnvoyAccessKind, route_variant: usize) -> String {
    match kind {
        EnvoyAccessKind::PublicRead => {
            if rng.usize(..100) < 35 {
                format!(
                    "/api/v1/users/{}/profile",
                    1000 + route_variant * 13 + rng.usize(..97)
                )
            } else {
                format!(
                    "/api/v1/users/{}",
                    1000 + route_variant * 17 + rng.usize(..10_000)
                )
            }
        }
        EnvoyAccessKind::PublicWrite => {
            let order_id = 10_000 + route_variant * 31 + rng.usize(..9_000);
            if rng.usize(..100) < 25 {
                format!("/api/v1/orders/{order_id}/items")
            } else {
                format!("/api/v1/orders/{order_id}")
            }
        }
        EnvoyAccessKind::Auth => match rng.usize(..3) {
            0 => "/api/v1/auth/login".to_string(),
            1 => "/api/v1/auth/refresh".to_string(),
            _ => "/api/v1/auth/logout".to_string(),
        },
        EnvoyAccessKind::Search => {
            let terms = [
                "checkout", "orders", "users", "billing", "catalog", "support",
            ];
            format!(
                "/api/v2/search?q={}&limit={}",
                terms[rng.usize(..terms.len())],
                [10, 25, 50][rng.usize(..3)]
            )
        }
        EnvoyAccessKind::StaticAssets => {
            let files = ["app.js", "main.css", "vendor.js", "logo.svg", "chunk.js"];
            let v = 10_000 + route_variant * 7 + rng.usize(..1_000);
            format!("/assets/{}?v={v}", files[rng.usize(..files.len())])
        }
        EnvoyAccessKind::Metrics => {
            if rng.usize(..100) < 30 {
                "/healthz".to_string()
            } else {
                "/metrics".to_string()
            }
        }
        EnvoyAccessKind::Graphql => {
            if rng.usize(..100) < 20 {
                "/graphql?operationName=CheckoutQuery".to_string()
            } else {
                "/graphql".to_string()
            }
        }
    }
}

fn make_user_agent(
    rng: &mut fastrand::Rng,
    kind: EnvoyAccessKind,
    client_bucket: usize,
    pool_size: usize,
) -> String {
    let slot = (client_bucket + rng.usize(..pool_size.max(1))) % pool_size.max(1);
    match kind {
        EnvoyAccessKind::PublicRead | EnvoyAccessKind::PublicWrite => match slot % 6 {
            0 => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.{:04}.0 Safari/537.36", 1000 + slot),
            1 => format!("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_{}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.{} Safari/605.1.15", slot % 6, 1 + slot % 4),
            2 => format!("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.{:04}.0 Safari/537.36", 2000 + slot),
            3 => format!("Mozilla/5.0 (iPhone; CPU iPhone OS 17_{} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.{} Mobile/15E148 Safari/604.1", slot % 5, 1 + slot % 3),
            4 => "Mozilla/5.0 (Android 14; Mobile; rv:125.0) Gecko/125.0 Firefox/125.0".to_string(),
            _ => format!("curl/8.{}.{}", 2 + slot % 3, slot % 10),
        },
        EnvoyAccessKind::Auth | EnvoyAccessKind::Search => match slot % 5 {
            0 => format!("Go-http-client/2.0 (envoy-edge/{})", 1 + slot % 8),
            1 => format!("okhttp/4.12.0 (build:{:04})", 1000 + slot),
            2 => format!("Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.{:04}.0 Mobile Safari/537.36", 1000 + slot),
            3 => format!("k6/0.49.0 (envoy bench {})", 1 + slot % 6),
            _ => format!("curl/8.{}.{}", 1 + slot % 4, slot % 10),
        },
        EnvoyAccessKind::StaticAssets => match slot % 4 {
            0 => "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15".to_string(),
            1 => "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36".to_string(),
            2 => format!("Mozilla/5.0 (iPhone; CPU iPhone OS 17_{} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.{} Mobile/15E148 Safari/604.1", slot % 4, 1 + slot % 3),
            _ => "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)".to_string(),
        },
        EnvoyAccessKind::Metrics => match slot % 4 {
            0 => "kube-probe/1.29".to_string(),
            1 => "Prometheus/2.49.1".to_string(),
            2 => "Grafana/10.4.1".to_string(),
            _ => "curl/8.7.1".to_string(),
        },
        EnvoyAccessKind::Graphql => match slot % 4 {
            0 => "Apollo/3.10.0".to_string(),
            1 => "insomnia/9.3.1".to_string(),
            2 => "PostmanRuntime/7.39.0".to_string(),
            _ => "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36".to_string(),
        },
    }
}

// ---------------------------------------------------------------------------
