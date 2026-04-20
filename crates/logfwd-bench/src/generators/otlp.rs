//! Shared OTLP fixture generation for benchmarks and profiling tools.
//!
//! Provides deterministic OTLP `ExportLogsServiceRequest` payloads with
//! configurable shape: resources, scopes, rows, attribute count, body size,
//! and edge-case flags (complex AnyValues, duplicate keys, trace-heavy).

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

/// Base nanosecond timestamp used by all fixtures.
pub const NANOS_BASE: u64 = 1_712_509_200_123_456_789;

/// Describes the shape of an OTLP log fixture.
#[derive(Clone, Copy, Debug)]
pub struct OtlpFixtureProfile {
    pub name: &'static str,
    pub resources: usize,
    pub scopes_per_resource: usize,
    pub rows_per_scope: usize,
    pub attrs_per_record: usize,
    pub resource_attrs: usize,
    pub body_len: usize,
    pub has_complex_any: bool,
    pub has_duplicate_keys: bool,
    pub has_trace_heavy: bool,
}

impl OtlpFixtureProfile {
    /// Total log records across all resources and scopes.
    pub const fn total_rows(self) -> usize {
        self.resources * self.scopes_per_resource * self.rows_per_scope
    }

    /// Look up a profile by name from [`ALL_PROFILES`].
    pub fn by_name(name: &str) -> Option<Self> {
        ALL_PROFILES.iter().find(|p| p.name == name).copied()
    }
}

// ── Built-in profiles (from Criterion bench) ───────────────────────────

pub const TINY: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "tiny",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 32,
    attrs_per_record: 4,
    resource_attrs: 3,
    body_len: 48,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

pub const NARROW_1K: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "narrow-1k",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 1_000,
    attrs_per_record: 4,
    resource_attrs: 4,
    body_len: 72,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

pub const WIDE_10K: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "wide-10k",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 10_000,
    attrs_per_record: 20,
    resource_attrs: 6,
    body_len: 240,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

pub const ATTRS_HEAVY: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "attrs-heavy",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 2_000,
    attrs_per_record: 40,
    resource_attrs: 8,
    body_len: 96,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

pub const MULTI_RESOURCE_SCOPE: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "multi-resource-scope",
    resources: 12,
    scopes_per_resource: 4,
    rows_per_scope: 120,
    attrs_per_record: 6,
    resource_attrs: 8,
    body_len: 96,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

pub const COMPLEX_ANYVALUE: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "complex-anyvalue",
    resources: 1,
    scopes_per_resource: 2,
    rows_per_scope: 1_000,
    attrs_per_record: 12,
    resource_attrs: 6,
    body_len: 128,
    has_complex_any: true,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

pub const DUPLICATE_COLLISION: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "duplicate-collision",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 1_500,
    attrs_per_record: 14,
    resource_attrs: 6,
    body_len: 80,
    has_complex_any: false,
    has_duplicate_keys: true,
    has_trace_heavy: false,
};

pub const TRACE_HEAVY: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "trace-heavy",
    resources: 1,
    scopes_per_resource: 2,
    rows_per_scope: 4_000,
    attrs_per_record: 8,
    resource_attrs: 6,
    body_len: 120,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: true,
};

pub const RESOURCE_HEAVY: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "resource-heavy",
    resources: 80,
    scopes_per_resource: 1,
    rows_per_scope: 64,
    attrs_per_record: 5,
    resource_attrs: 24,
    body_len: 64,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

pub const COMPRESSION_RELEVANT: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "compression-relevant",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 5_000,
    attrs_per_record: 20,
    resource_attrs: 16,
    body_len: 512,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: true,
};

// ── New expanded-coverage profiles ─────────────────────────────────────

/// Minimal payload — 10 rows, 1 attr, tiny body.
pub const MINIMAL: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "minimal",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 10,
    attrs_per_record: 1,
    resource_attrs: 1,
    body_len: 16,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

/// Single row — tests per-batch overhead vs per-row.
pub const SINGLE_ROW: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "single-row",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 1,
    attrs_per_record: 8,
    resource_attrs: 4,
    body_len: 64,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

/// Wide-attrs — extreme number of attributes per record (200 attrs).
pub const WIDE_ATTRS: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "wide-attrs",
    resources: 1,
    scopes_per_resource: 1,
    rows_per_scope: 500,
    attrs_per_record: 200,
    resource_attrs: 8,
    body_len: 64,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

/// Many small resources — tests resource-level overhead.
pub const MANY_RESOURCES: OtlpFixtureProfile = OtlpFixtureProfile {
    name: "many-resources",
    resources: 200,
    scopes_per_resource: 1,
    rows_per_scope: 10,
    attrs_per_record: 4,
    resource_attrs: 6,
    body_len: 48,
    has_complex_any: false,
    has_duplicate_keys: false,
    has_trace_heavy: false,
};

/// All built-in fixture profiles.
pub const ALL_PROFILES: [OtlpFixtureProfile; 14] = [
    TINY,
    NARROW_1K,
    WIDE_10K,
    ATTRS_HEAVY,
    MULTI_RESOURCE_SCOPE,
    COMPLEX_ANYVALUE,
    DUPLICATE_COLLISION,
    TRACE_HEAVY,
    RESOURCE_HEAVY,
    COMPRESSION_RELEVANT,
    MINIMAL,
    SINGLE_ROW,
    WIDE_ATTRS,
    MANY_RESOURCES,
];

// ── Fixture data ───────────────────────────────────────────────────────

/// Prebuilt OTLP fixture: the request, its protobuf-encoded payload, and a
/// zero-copy `Bytes` handle.
pub struct OtlpFixtureData {
    pub profile: OtlpFixtureProfile,
    pub request: ExportLogsServiceRequest,
    pub payload: Vec<u8>,
    pub payload_bytes: Bytes,
}

/// Build a single fixture from a profile.
pub fn build_fixture(profile: OtlpFixtureProfile) -> OtlpFixtureData {
    let request = build_request(profile);
    let payload = request.encode_to_vec();
    let payload_bytes = Bytes::from(payload.clone());
    OtlpFixtureData {
        profile,
        request,
        payload,
        payload_bytes,
    }
}

/// Build fixtures for every profile in [`ALL_PROFILES`].
pub fn all_fixtures() -> Vec<OtlpFixtureData> {
    ALL_PROFILES.iter().copied().map(build_fixture).collect()
}

// ── Request builder ────────────────────────────────────────────────────

/// Synthesize a complete `ExportLogsServiceRequest` matching `profile`.
pub fn build_request(profile: OtlpFixtureProfile) -> ExportLogsServiceRequest {
    let mut resource_logs = Vec::with_capacity(profile.resources);

    for resource_idx in 0..profile.resources {
        let mut scopes = Vec::with_capacity(profile.scopes_per_resource);
        for scope_idx in 0..profile.scopes_per_resource {
            let mut records = Vec::with_capacity(profile.rows_per_scope);
            for row in 0..profile.rows_per_scope {
                let global_row = ((resource_idx * profile.scopes_per_resource + scope_idx)
                    * profile.rows_per_scope)
                    + row;
                let mut attributes = make_record_attrs(profile, global_row);
                if profile.has_duplicate_keys {
                    attributes.push(kv_string("body", "attr-body-shadow"));
                    attributes.push(kv_string("trace_id", "attr-trace-shadow"));
                    attributes.push(kv_string("flags", "attr-flags-shadow"));
                    attributes.push(kv_string(
                        "resource_shadow.service.name",
                        "attr-resource-shadow",
                    ));
                }

                let body = if profile.has_complex_any && row % 2 == 0 {
                    complex_body_any_value(global_row)
                } else {
                    AnyValue {
                        value: Some(Value::StringValue(make_message(
                            global_row,
                            profile.body_len,
                        ))),
                    }
                };

                let trace_id = if profile.has_trace_heavy || row % 3 != 0 {
                    trace_id_for(global_row)
                } else {
                    Vec::new()
                };
                let span_id = if profile.has_trace_heavy || row % 5 != 0 {
                    span_id_for(global_row)
                } else {
                    Vec::new()
                };

                records.push(LogRecord {
                    time_unix_nano: NANOS_BASE + global_row as u64,
                    observed_time_unix_nano: NANOS_BASE + global_row as u64 + 123,
                    severity_text: severity_text(global_row).to_string(),
                    severity_number: severity_number(global_row),
                    body: Some(body),
                    trace_id,
                    span_id,
                    flags: 1,
                    attributes,
                    ..Default::default()
                });
            }

            scopes.push(ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: format!("bench-scope-{resource_idx}-{scope_idx}"),
                    version: "2026.04".to_string(),
                    attributes: vec![kv_string("scope.env", "bench")],
                    dropped_attributes_count: 0,
                }),
                log_records: records,
                schema_url: String::new(),
            });
        }

        resource_logs.push(ResourceLogs {
            resource: Some(Resource {
                attributes: make_resource_attrs(profile, resource_idx),
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: scopes,
            schema_url: String::new(),
        });
    }

    ExportLogsServiceRequest { resource_logs }
}

// ── Attribute helpers ──────────────────────────────────────────────────

pub fn make_resource_attrs(profile: OtlpFixtureProfile, resource_idx: usize) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(profile.resource_attrs.max(4));
    attrs.push(kv_string(
        "service.name",
        &format!("bench-service-{resource_idx}"),
    ));
    attrs.push(kv_string("service.namespace", "bench"));
    attrs.push(kv_string(
        "host.name",
        &format!("bench-node-{}", resource_idx % 32),
    ));
    attrs.push(kv_string("k8s.cluster.name", "bench-cluster"));

    for idx in 0..profile.resource_attrs.saturating_sub(4) {
        attrs.push(kv_string(
            &format!("resource.attr.{idx}"),
            &format!("rv{}_{}", resource_idx % 19, idx % 11),
        ));
    }

    attrs
}

pub fn make_record_attrs(profile: OtlpFixtureProfile, row: usize) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(profile.attrs_per_record + 4);
    attrs.push(kv_string("host", &format!("host-{}", row % 64)));
    attrs.push(kv_i64("status", 200 + (row % 9) as i64));
    attrs.push(kv_f64("duration_ms", (row % 1000) as f64 / 10.0));
    attrs.push(kv_bool("success", !row.is_multiple_of(17)));

    for idx in 0..profile.attrs_per_record.saturating_sub(4) {
        let value = if profile.has_complex_any && idx % 5 == 0 {
            complex_body_any_value(row + idx)
        } else if idx % 3 == 0 {
            AnyValue {
                value: Some(Value::IntValue((row as i64 + idx as i64) % 4096)),
            }
        } else {
            AnyValue {
                value: Some(Value::StringValue(format!("v{}_{}", idx % 37, row % 23))),
            }
        };
        attrs.push(KeyValue {
            key: format!("attr_{idx}"),
            value: Some(value),
        });
    }

    attrs
}

pub fn complex_body_any_value(row: usize) -> AnyValue {
    AnyValue {
        value: Some(Value::KvlistValue(KeyValueList {
            values: vec![
                kv_string("event.kind", "benchmark"),
                KeyValue {
                    key: "event.row".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::IntValue(row as i64)),
                    }),
                },
                KeyValue {
                    key: "nested".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(Value::StringValue("alpha".to_string())),
                                },
                                AnyValue {
                                    value: Some(Value::BoolValue(row.is_multiple_of(2))),
                                },
                            ],
                        })),
                    }),
                },
            ],
        })),
    }
}

// ── Scalar helpers ─────────────────────────────────────────────────────

pub fn severity_text(row: usize) -> &'static str {
    match row % 4 {
        0 => "INFO",
        1 => "WARN",
        2 => "ERROR",
        _ => "DEBUG",
    }
}

pub fn severity_number(row: usize) -> i32 {
    match row % 4 {
        0 => 9,
        1 => 13,
        2 => 17,
        _ => 5,
    }
}

pub fn trace_id_for(row: usize) -> Vec<u8> {
    let mut out = vec![0u8; 16];
    for (idx, byte) in out.iter_mut().enumerate() {
        *byte = ((row + idx) % 251) as u8;
    }
    out
}

pub fn span_id_for(row: usize) -> Vec<u8> {
    let mut out = vec![0u8; 8];
    for (idx, byte) in out.iter_mut().enumerate() {
        *byte = ((row + idx + 31) % 251) as u8;
    }
    out
}

pub fn make_message(row: usize, target_len: usize) -> String {
    let mut message = format!("benchmark row={row} subsystem=otlp_io ");
    while message.len() < target_len {
        message.push_str("payload_token ");
    }
    message.truncate(target_len);
    message
}

// ── KeyValue constructors ──────────────────────────────────────────────

pub fn kv_string(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

pub fn kv_i64(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
    }
}

pub fn kv_f64(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
    }
}

pub fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::BoolValue(value)),
        }),
    }
}
