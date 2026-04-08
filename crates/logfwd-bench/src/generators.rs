//! Deterministic data generators for benchmarks.
//!
//! Every generator uses a seeded [`fastrand::Rng`] so that identical
//! `(count, seed)` pairs always produce byte-identical output.  This ensures
//! reproducible Criterion results across runs and CI.

use std::fmt::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_output::BatchMetadata;

use crate::cardinality::cardinality_helpers::{CardinalityProfile, CardinalityState, SamplePhase};

pub mod cloudtrail;

// ---------------------------------------------------------------------------
// Constants — realistic data pools
// ---------------------------------------------------------------------------

const LEVELS: &[&str] = &["INFO", "WARN", "ERROR", "DEBUG", "TRACE"];
const STREAMS: &[&str] = &["stdout", "stderr"];
const NAMESPACES: &[&str] = &[
    "default",
    "kube-system",
    "monitoring",
    "app-prod",
    "data-pipeline",
];
const PODS: &[&str] = &[
    "api-gateway-7b8c9d-abc12",
    "worker-5f6a7b-def34",
    "frontend-3c4d5e-ghi56",
    "scheduler-1a2b3c-jkl78",
    "ingester-9e0f1a-mno90",
];
const CONTAINERS: &[&str] = &["main", "sidecar", "init", "envoy"];
const PATHS: &[&str] = &[
    "/api/users",
    "/api/orders",
    "/api/health",
    "/api/auth/login",
    "/api/metrics",
    "/api/v2/search",
    "/api/v2/ingest",
    "/graphql",
];
const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];
const SERVICES: &[&str] = &[
    "api-gateway",
    "user-service",
    "order-service",
    "auth-service",
    "metrics-collector",
];
const STATUS_CODES: &[u16] = &[200, 200, 200, 201, 204, 400, 401, 403, 404, 500, 502, 503];

const STACK_TRACE: &str = "java.lang.NullPointerException: Cannot invoke method on null object\n\
    \tat com.example.service.UserHandler.getUser(UserHandler.java:142)\n\
    \tat com.example.service.UserHandler.handleRequest(UserHandler.java:87)\n\
    \tat com.example.framework.Router.dispatch(Router.java:234)\n\
    \tat com.example.framework.HttpServer.processRequest(HttpServer.java:456)\n\
    \tat com.example.framework.HttpServer$Worker.run(HttpServer.java:678)\n\
    \tat java.base/java.lang.Thread.run(Thread.java:829)";

const CLOUDTRAIL_EVENT_VERSION: &str = "1.11";
const CLOUDTRAIL_REGIONS_GLOBAL: &[&str] = &["us-east-1"];
const CLOUDTRAIL_REGIONS_REGIONAL: &[&str] = &["us-east-1", "us-west-2", "eu-west-1"];
const CLOUDTRAIL_REGIONS_MULTI: &[&str] = &[
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "ap-southeast-1",
    "ca-central-1",
    "ap-northeast-1",
];
const CLOUDTRAIL_PUBLIC_IPS: &[&str] = &[
    "198.51.100.10",
    "198.51.100.12",
    "198.51.100.18",
    "203.0.113.24",
    "203.0.113.37",
    "203.0.113.42",
];
const CLOUDTRAIL_USER_AGENTS: &[&str] = &[
    "aws-cli/2.15.0 Python/3.11.8 Linux/6.6",
    "Boto3/1.34.0 Python/3.11.8 Linux/6.6",
    "console.amazonaws.com",
    "terraform/1.7.4 terraform-provider-aws/5.42.0",
    "aws-sdk-go/1.51.0 (go1.22; linux; amd64)",
    "aws-internal/3 aws-sdk-java/1.12.650",
];

/// Controls the relative weight of AWS service families in generated CloudTrail events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudTrailServiceMix {
    /// Even spread across IAM, S3, EC2, STS, KMS, Lambda, and RDS.
    Balanced,
    /// Heavier IAM, STS, and KMS traffic typical of security-focused accounts.
    SecurityHeavy,
    /// Heavier S3 and EC2 traffic typical of data-lake accounts.
    StorageHeavy,
    /// Heavier EC2 and Lambda traffic typical of compute-intensive accounts.
    ComputeHeavy,
}

/// Controls the AWS region distribution in generated CloudTrail events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudTrailRegionMix {
    /// All events originate from `us-east-1`.
    GlobalOnly,
    /// Events spread across three regions.
    Regional,
    /// Events spread across six regions with realistic skew.
    MultiRegion,
}

/// Tunable parameters for CloudTrail audit log generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloudTrailProfile {
    /// Number of distinct AWS account IDs in the rotating pool.
    pub account_count: usize,
    /// Rows to keep an account hot before rotating to the next.
    pub account_tenure: usize,
    /// Number of distinct IAM principals in the rotating pool.
    pub principal_count: usize,
    /// Rows to keep a principal hot before rotating.
    pub principal_tenure: usize,
    /// Which AWS service families to emphasize.
    pub service_mix: CloudTrailServiceMix,
    /// How many regions to spread events across.
    pub region_mix: CloudTrailRegionMix,
    /// Probability (0-100) that optional nested sub-structures are emitted.
    pub optional_field_density: u8,
}

impl Default for CloudTrailProfile {
    fn default() -> Self {
        Self::benchmark_default()
    }
}

impl CloudTrailProfile {
    pub fn benchmark_default() -> Self {
        Self {
            account_count: 12,
            account_tenure: 48,
            principal_count: 64,
            principal_tenure: 12,
            service_mix: CloudTrailServiceMix::Balanced,
            region_mix: CloudTrailRegionMix::MultiRegion,
            optional_field_density: 65,
        }
    }

    pub fn with_account_count(mut self, account_count: usize) -> Self {
        self.account_count = account_count.max(1);
        self
    }

    pub fn with_account_tenure(mut self, account_tenure: usize) -> Self {
        self.account_tenure = account_tenure.max(1);
        self
    }

    pub fn with_principal_count(mut self, principal_count: usize) -> Self {
        self.principal_count = principal_count.max(1);
        self
    }

    pub fn with_principal_tenure(mut self, principal_tenure: usize) -> Self {
        self.principal_tenure = principal_tenure.max(1);
        self
    }

    pub fn with_service_mix(mut self, service_mix: CloudTrailServiceMix) -> Self {
        self.service_mix = service_mix;
        self
    }

    pub fn with_region_mix(mut self, region_mix: CloudTrailRegionMix) -> Self {
        self.region_mix = region_mix;
        self
    }

    pub fn with_optional_field_density(mut self, optional_field_density: u8) -> Self {
        self.optional_field_density = optional_field_density.min(100);
        self
    }
}

#[derive(Debug, Clone, Copy)]
enum CloudTrailIdentityKind {
    Root,
    IamUser,
    AssumedRole,
    AwsService,
    FederatedUser,
}

#[derive(Debug, Clone, Copy)]
enum CloudTrailServiceKind {
    Ec2,
    S3,
    Iam,
    Sts,
    Kms,
    Lambda,
    Rds,
    CloudTrail,
}

#[derive(Debug, Clone, Copy)]
struct CloudTrailActionSpec {
    event_name: &'static str,
    read_only: bool,
    data_event: bool,
    resource_type: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct CloudTrailServiceSpec {
    event_source: &'static str,
    weight: usize,
    kind: CloudTrailServiceKind,
    actions: &'static [CloudTrailActionSpec],
}

const EC2_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "DescribeInstances",
        read_only: true,
        data_event: false,
        resource_type: "AWS::EC2::Instance",
    },
    CloudTrailActionSpec {
        event_name: "RunInstances",
        read_only: false,
        data_event: false,
        resource_type: "AWS::EC2::Instance",
    },
    CloudTrailActionSpec {
        event_name: "StopInstances",
        read_only: false,
        data_event: false,
        resource_type: "AWS::EC2::Instance",
    },
    CloudTrailActionSpec {
        event_name: "TerminateInstances",
        read_only: false,
        data_event: false,
        resource_type: "AWS::EC2::Instance",
    },
    CloudTrailActionSpec {
        event_name: "DescribeInstances",
        read_only: true,
        data_event: false,
        resource_type: "AWS::EC2::Instance",
    },
];

const S3_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "GetObject",
        read_only: true,
        data_event: true,
        resource_type: "AWS::S3::Object",
    },
    CloudTrailActionSpec {
        event_name: "PutObject",
        read_only: false,
        data_event: true,
        resource_type: "AWS::S3::Object",
    },
    CloudTrailActionSpec {
        event_name: "DeleteObject",
        read_only: false,
        data_event: true,
        resource_type: "AWS::S3::Object",
    },
    CloudTrailActionSpec {
        event_name: "ListBucket",
        read_only: true,
        data_event: false,
        resource_type: "AWS::S3::Bucket",
    },
    CloudTrailActionSpec {
        event_name: "GetBucketLocation",
        read_only: true,
        data_event: false,
        resource_type: "AWS::S3::Bucket",
    },
];

const IAM_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "GetUser",
        read_only: true,
        data_event: false,
        resource_type: "AWS::IAM::User",
    },
    CloudTrailActionSpec {
        event_name: "CreateUser",
        read_only: false,
        data_event: false,
        resource_type: "AWS::IAM::User",
    },
    CloudTrailActionSpec {
        event_name: "CreateRole",
        read_only: false,
        data_event: false,
        resource_type: "AWS::IAM::Role",
    },
    CloudTrailActionSpec {
        event_name: "AttachRolePolicy",
        read_only: false,
        data_event: false,
        resource_type: "AWS::IAM::Role",
    },
    CloudTrailActionSpec {
        event_name: "UpdateAssumeRolePolicy",
        read_only: false,
        data_event: false,
        resource_type: "AWS::IAM::Role",
    },
];

const STS_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "AssumeRole",
        read_only: false,
        data_event: false,
        resource_type: "AWS::STS::AssumedRole",
    },
    CloudTrailActionSpec {
        event_name: "GetCallerIdentity",
        read_only: true,
        data_event: false,
        resource_type: "AWS::STS::Session",
    },
    CloudTrailActionSpec {
        event_name: "AssumeRole",
        read_only: false,
        data_event: false,
        resource_type: "AWS::STS::AssumedRole",
    },
];

const KMS_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "Decrypt",
        read_only: true,
        data_event: false,
        resource_type: "AWS::KMS::Key",
    },
    CloudTrailActionSpec {
        event_name: "Encrypt",
        read_only: false,
        data_event: false,
        resource_type: "AWS::KMS::Key",
    },
    CloudTrailActionSpec {
        event_name: "CreateKey",
        read_only: false,
        data_event: false,
        resource_type: "AWS::KMS::Key",
    },
    CloudTrailActionSpec {
        event_name: "DescribeKey",
        read_only: true,
        data_event: false,
        resource_type: "AWS::KMS::Key",
    },
];

const LAMBDA_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "Invoke",
        read_only: true,
        data_event: true,
        resource_type: "AWS::Lambda::Function",
    },
    CloudTrailActionSpec {
        event_name: "CreateFunction",
        read_only: false,
        data_event: false,
        resource_type: "AWS::Lambda::Function",
    },
    CloudTrailActionSpec {
        event_name: "UpdateFunctionCode",
        read_only: false,
        data_event: false,
        resource_type: "AWS::Lambda::Function",
    },
    CloudTrailActionSpec {
        event_name: "AddPermission",
        read_only: false,
        data_event: false,
        resource_type: "AWS::Lambda::Function",
    },
];

const RDS_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "DescribeDBInstances",
        read_only: true,
        data_event: false,
        resource_type: "AWS::RDS::DBInstance",
    },
    CloudTrailActionSpec {
        event_name: "CreateDBInstance",
        read_only: false,
        data_event: false,
        resource_type: "AWS::RDS::DBInstance",
    },
    CloudTrailActionSpec {
        event_name: "ModifyDBInstance",
        read_only: false,
        data_event: false,
        resource_type: "AWS::RDS::DBInstance",
    },
    CloudTrailActionSpec {
        event_name: "DeleteDBInstance",
        read_only: false,
        data_event: false,
        resource_type: "AWS::RDS::DBInstance",
    },
];

const CLOUDTRAIL_ACTIONS: &[CloudTrailActionSpec] = &[
    CloudTrailActionSpec {
        event_name: "LookupEvents",
        read_only: true,
        data_event: false,
        resource_type: "AWS::CloudTrail::Trail",
    },
    CloudTrailActionSpec {
        event_name: "PutEventSelectors",
        read_only: false,
        data_event: false,
        resource_type: "AWS::CloudTrail::Trail",
    },
    CloudTrailActionSpec {
        event_name: "StartLogging",
        read_only: false,
        data_event: false,
        resource_type: "AWS::CloudTrail::Trail",
    },
];

const SERVICE_BALANCED: &[CloudTrailServiceSpec] = &[
    CloudTrailServiceSpec {
        event_source: "ec2.amazonaws.com",
        weight: 4,
        kind: CloudTrailServiceKind::Ec2,
        actions: EC2_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "s3.amazonaws.com",
        weight: 4,
        kind: CloudTrailServiceKind::S3,
        actions: S3_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "iam.amazonaws.com",
        weight: 3,
        kind: CloudTrailServiceKind::Iam,
        actions: IAM_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "lambda.amazonaws.com",
        weight: 2,
        kind: CloudTrailServiceKind::Lambda,
        actions: LAMBDA_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "sts.amazonaws.com",
        weight: 2,
        kind: CloudTrailServiceKind::Sts,
        actions: STS_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "kms.amazonaws.com",
        weight: 2,
        kind: CloudTrailServiceKind::Kms,
        actions: KMS_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "rds.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::Rds,
        actions: RDS_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "cloudtrail.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::CloudTrail,
        actions: CLOUDTRAIL_ACTIONS,
    },
];

const SERVICE_SECURITY_HEAVY: &[CloudTrailServiceSpec] = &[
    CloudTrailServiceSpec {
        event_source: "iam.amazonaws.com",
        weight: 6,
        kind: CloudTrailServiceKind::Iam,
        actions: IAM_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "sts.amazonaws.com",
        weight: 5,
        kind: CloudTrailServiceKind::Sts,
        actions: STS_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "kms.amazonaws.com",
        weight: 5,
        kind: CloudTrailServiceKind::Kms,
        actions: KMS_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "cloudtrail.amazonaws.com",
        weight: 4,
        kind: CloudTrailServiceKind::CloudTrail,
        actions: CLOUDTRAIL_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "ec2.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::Ec2,
        actions: EC2_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "s3.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::S3,
        actions: S3_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "lambda.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::Lambda,
        actions: LAMBDA_ACTIONS,
    },
];

const SERVICE_STORAGE_HEAVY: &[CloudTrailServiceSpec] = &[
    CloudTrailServiceSpec {
        event_source: "s3.amazonaws.com",
        weight: 7,
        kind: CloudTrailServiceKind::S3,
        actions: S3_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "lambda.amazonaws.com",
        weight: 3,
        kind: CloudTrailServiceKind::Lambda,
        actions: LAMBDA_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "ec2.amazonaws.com",
        weight: 2,
        kind: CloudTrailServiceKind::Ec2,
        actions: EC2_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "rds.amazonaws.com",
        weight: 2,
        kind: CloudTrailServiceKind::Rds,
        actions: RDS_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "iam.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::Iam,
        actions: IAM_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "kms.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::Kms,
        actions: KMS_ACTIONS,
    },
];

const SERVICE_COMPUTE_HEAVY: &[CloudTrailServiceSpec] = &[
    CloudTrailServiceSpec {
        event_source: "ec2.amazonaws.com",
        weight: 6,
        kind: CloudTrailServiceKind::Ec2,
        actions: EC2_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "lambda.amazonaws.com",
        weight: 4,
        kind: CloudTrailServiceKind::Lambda,
        actions: LAMBDA_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "rds.amazonaws.com",
        weight: 3,
        kind: CloudTrailServiceKind::Rds,
        actions: RDS_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "s3.amazonaws.com",
        weight: 2,
        kind: CloudTrailServiceKind::S3,
        actions: S3_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "iam.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::Iam,
        actions: IAM_ACTIONS,
    },
    CloudTrailServiceSpec {
        event_source: "kms.amazonaws.com",
        weight: 1,
        kind: CloudTrailServiceKind::Kms,
        actions: KMS_ACTIONS,
    },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn pick<'a>(rng: &mut fastrand::Rng, items: &'a [&str]) -> &'a str {
    items[rng.usize(..items.len())]
}

fn pick_by_idx<'a>(items: &'a [&str], idx: usize) -> &'a str {
    items[idx % items.len()]
}

fn pick_u16(rng: &mut fastrand::Rng, items: &[u16]) -> u16 {
    items[rng.usize(..items.len())]
}

fn round_tenths(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn level_for_phase(phase: SamplePhase) -> &'static str {
    match phase {
        SamplePhase::Hot => "INFO",
        SamplePhase::Warm => "WARN",
        SamplePhase::Cold => "ERROR",
    }
}

fn user_label(idx: usize) -> String {
    format!("user-{idx:03}")
}

/// Escape a string for JSON embedding (handles `\n`, `\t`, `\`, `"`).
fn json_escape(s: &str, out: &mut String) {
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
}

trait WeightedChoice {
    fn weight(&self) -> usize;
}

impl WeightedChoice for EnvoyAccessScenario {
    fn weight(&self) -> usize {
        self.weight
    }
}

impl WeightedChoice for CloudTrailServiceSpec {
    fn weight(&self) -> usize {
        self.weight
    }
}

fn weighted_pick<T: Copy>(rng: &mut fastrand::Rng, items: &[(T, usize)]) -> T {
    debug_assert!(!items.is_empty());
    let total: usize = items.iter().map(|(_, weight)| *weight).sum();
    let mut roll = rng.usize(..total.max(1));
    for item in items {
        let (value, weight) = item;
        if roll < *weight {
            return *value;
        }
        roll -= *weight;
    }
    items[items.len() - 1].0
}

fn weighted_choice_pick<T: WeightedChoice + Copy>(rng: &mut fastrand::Rng, items: &[T]) -> T {
    debug_assert!(!items.is_empty());
    let total: usize = items.iter().map(WeightedChoice::weight).sum();
    let mut roll = rng.usize(..total.max(1));
    for item in items {
        let weight = item.weight();
        if roll < weight {
            return *item;
        }
        roll -= weight;
    }
    items[items.len() - 1]
}

fn append_uuid_like(rng: &mut fastrand::Rng, out: &mut String) {
    append_uuid_like_value(rng.u128(..), out);
}

fn append_uuid_like_value(value: u128, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let bytes = value.to_be_bytes();
    for (idx, byte) in bytes.into_iter().enumerate() {
        if matches!(idx, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

fn uuid_like_from_value(value: u128) -> String {
    let mut out = String::with_capacity(36);
    append_uuid_like_value(value, &mut out);
    out
}

fn append_ipv4(out: &mut String, a: u8, b: u8, c: u8, d: u8, port: u16) {
    let _ = write!(out, "{a}.{b}.{c}.{d}:{port}");
}

fn append_ipv4_without_port(out: &mut String, a: u8, b: u8, c: u8, d: u8) {
    let _ = write!(out, "{a}.{b}.{c}.{d}");
}

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
        let upstream_service_time_ms = if response_code >= 500 || rng.usize(..100) < 90 {
            Some((duration_ms * (0.35 + rng.f64() * 0.45)).max(1.0))
        } else {
            None
        };
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

    let mut timestamp = Vec::with_capacity(count);
    let mut method = Vec::with_capacity(count);
    let mut path = Vec::with_capacity(count);
    let mut protocol = Vec::with_capacity(count);
    let mut response_code = Vec::with_capacity(count);
    let mut response_flags = Vec::with_capacity(count);
    let mut response_code_details = Vec::with_capacity(count);
    let mut bytes_received = Vec::with_capacity(count);
    let mut bytes_sent = Vec::with_capacity(count);
    let mut duration_ms = Vec::with_capacity(count);
    let mut upstream_service_time_ms = Vec::with_capacity(count);
    let mut user_agent = Vec::with_capacity(count);
    let mut x_request_id = Vec::with_capacity(count);
    let mut authority = Vec::with_capacity(count);
    let mut route_name = Vec::with_capacity(count);
    let mut service = Vec::with_capacity(count);
    let mut upstream_cluster = Vec::with_capacity(count);
    let mut upstream_host = Vec::with_capacity(count);
    let mut downstream_remote_address = Vec::with_capacity(count);
    let mut downstream_direct_remote_address = Vec::with_capacity(count);
    let mut x_forwarded_for = Vec::with_capacity(count);
    let mut tls_version = Vec::with_capacity(count);

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
        let route_name_value = format!("{}-v{}", current_scenario.route_prefix, route_variant);
        let upstream_cluster_value =
            format!("{}-v{}", current_scenario.cluster_prefix, route_variant);
        let host_bucket =
            (current_source_bucket + route_variant) % (16 * profile.cardinality_scale.max(1) + 16);
        let source_ip = make_source_ip(current_source_bucket, i);
        let direct_ip = make_direct_ip(host_bucket, i);
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
        let upstream_service_time_ms_value = if response_code_value >= 500 || rng.usize(..100) < 90
        {
            Some(round_tenths(
                (raw_duration_ms_value * (0.35 + rng.f64() * 0.45)).max(1.0),
            ))
        } else {
            None
        };
        let duration_ms_value = round_tenths(raw_duration_ms_value);
        let xff_value =
            make_x_forwarded_for(&mut rng, current_source_bucket, profile.xff_hops_max.max(1));
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

        let mut request_id = String::with_capacity(36);
        append_uuid_like(&mut rng, &mut request_id);
        let mut upstream_host_value = String::new();
        let o1 = 10 + (current_source_bucket % 20) as u8;
        let o2 = (route_variant % 250) as u8;
        let o3 = ((i + current_client_bucket) % 250) as u8;
        let o4 = 10 + ((route_variant + current_client_bucket) % 200) as u8;
        let port = 8080 + ((route_variant + current_client_bucket) % 5) as u16;
        append_ipv4(&mut upstream_host_value, o1, o2, o3, o4, port);

        timestamp.push(format!("2024-01-15T10:30:{sec:02}.{nano:09}Z"));
        method.push(method_value.to_string());
        path.push(path_value);
        protocol.push(protocol_value.to_string());
        response_code.push(response_code_value);
        response_flags.push(response_flags_value.to_string());
        response_code_details.push(response_code_details_value.to_string());
        bytes_received.push(bytes_received_value);
        bytes_sent.push(bytes_sent_value);
        duration_ms.push(duration_ms_value);
        upstream_service_time_ms.push(upstream_service_time_ms_value);
        user_agent.push(user_agent_value);
        x_request_id.push(request_id);
        authority.push(current_scenario.authority.to_string());
        route_name.push(route_name_value);
        service.push(current_scenario.service.to_string());
        upstream_cluster.push(upstream_cluster_value);
        upstream_host.push(upstream_host_value);
        downstream_remote_address.push(source_ip);
        downstream_direct_remote_address.push(direct_ip);
        x_forwarded_for.push(xff_value);
        tls_version.push(tls_version_value.to_string());
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
        Arc::new(StringArray::from(timestamp)) as ArrayRef,
        Arc::new(StringArray::from(method)) as ArrayRef,
        Arc::new(StringArray::from(path)) as ArrayRef,
        Arc::new(StringArray::from(protocol)) as ArrayRef,
        Arc::new(Int64Array::from(
            response_code.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(response_flags)) as ArrayRef,
        Arc::new(StringArray::from(response_code_details)) as ArrayRef,
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
        Arc::new(StringArray::from(user_agent)) as ArrayRef,
        Arc::new(StringArray::from(x_request_id)) as ArrayRef,
        Arc::new(StringArray::from(authority)) as ArrayRef,
        Arc::new(StringArray::from(route_name)) as ArrayRef,
        Arc::new(StringArray::from(service)) as ArrayRef,
        Arc::new(StringArray::from(upstream_cluster)) as ArrayRef,
        Arc::new(StringArray::from(upstream_host)) as ArrayRef,
        Arc::new(StringArray::from(downstream_remote_address)) as ArrayRef,
        Arc::new(StringArray::from(downstream_direct_remote_address)) as ArrayRef,
        Arc::new(StringArray::from(x_forwarded_for)) as ArrayRef,
        Arc::new(StringArray::from(tls_version)) as ArrayRef,
    ];
    RecordBatch::try_new(schema, arrays)
        .unwrap_or_else(|err| panic!("envoy batch generation failed for {count} rows: {err}"))
}

fn make_source_ip(bucket: usize, salt: usize) -> String {
    let mut out = String::new();
    let a = 203u8;
    let b = 0u8;
    let c = (bucket % 250) as u8;
    let d = (17 + (salt % 200)) as u8;
    let port = 40_000 + (bucket as u16 % 20_000);
    append_ipv4(&mut out, a, b, c, d, port);
    out
}

fn make_direct_ip(bucket: usize, salt: usize) -> String {
    let mut out = String::new();
    let a = 10u8;
    let b = 1u8 + ((bucket / 32) % 10) as u8;
    let c = (bucket % 32) as u8;
    let d = (salt % 250) as u8;
    let port = 44_300 + (bucket as u16 % 700);
    append_ipv4(&mut out, a, b, c, d, port);
    out
}

fn make_x_forwarded_for(rng: &mut fastrand::Rng, bucket: usize, max_hops: usize) -> String {
    let hops = 1 + rng.usize(..max_hops.max(1));
    let mut out = String::new();
    for hop in 0..hops {
        if hop > 0 {
            out.push_str(", ");
        }
        let a = if hop == 0 { 203 } else { 172 };
        let b = if hop == 0 { 0 } else { 16 };
        let c = ((bucket + hop) % 250) as u8;
        let d = (1 + ((bucket + hop * 7) % 200)) as u8;
        append_ipv4_without_port(&mut out, a, b, c, d);
    }
    out
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

    let mut timestamp = Vec::with_capacity(count);
    let mut level = Vec::with_capacity(count);
    let mut message = Vec::with_capacity(count);
    let mut status = Vec::with_capacity(count);
    let mut duration_ms = Vec::with_capacity(count);
    let mut service = Vec::with_capacity(count);
    let mut namespace = Vec::with_capacity(count);
    let mut user = Vec::with_capacity(count);
    let mut request_id = Vec::with_capacity(count);
    let mut bytes_sent = Vec::with_capacity(count);
    let mut cluster = Vec::with_capacity(count);
    let mut node = Vec::with_capacity(count);
    let mut pod = Vec::with_capacity(count);
    let mut container = Vec::with_capacity(count);
    let mut trace_id = Vec::with_capacity(count);
    let mut span_id = Vec::with_capacity(count);
    let mut session_id = Vec::with_capacity(count);
    let mut user_agent = Vec::with_capacity(count);
    let mut content_type = Vec::with_capacity(count);
    let mut retry_count = Vec::with_capacity(count);
    let mut error_count = Vec::with_capacity(count);
    let mut exception_class = Vec::with_capacity(count);

    for i in 0..count {
        let sample = cardinality.sample(&mut rng);
        let phase = sample.phase;
        let status_code = sample.status_code;
        let cluster_value = sample.cluster.to_string();
        let node_value = sample.node.to_string();
        let pod_value = sample.pod.to_string();
        let container_value = sample.container.to_string();
        let trace_id_value = format!("{:032x}", sample.trace_id);
        let span_id_value = format!("{:016x}", sample.span_id);
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
        let ts = format!("2024-01-15T10:30:{sec:02}.{nano:09}Z");
        let variant = rng.u8(..10);
        let level_value = level_for_phase(phase).to_string();
        let service_value = pick_by_idx(SERVICES, service_idx).to_string();
        let namespace_value = pick_by_idx(NAMESPACES, namespace_idx).to_string();
        let path = pick_by_idx(PATHS, path_idx);
        let user_value = user_label(user_idx);

        timestamp.push(ts);
        level.push(level_value.clone());
        service.push(service_value.clone());

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
            message.push(format!("{method} {path} HTTP/1.1"));
            status.push(status_value);
            duration_ms.push(duration_value);
            namespace.push(include_namespace_user.then_some(namespace_value));
            user.push(include_namespace_user.then_some(user_value));
            request_id.push(
                (rng.bool() || phase == SamplePhase::Cold)
                    .then(|| format!("req-{request_id_value:08x}")),
            );
            bytes_sent.push(rng.bool().then(|| rng.u32(..65536)));
            cluster.push(None);
            node.push(None);
            pod.push(None);
            container.push(None);
            trace_id.push(None);
            span_id.push(None);
            session_id.push(None);
            user_agent.push(None);
            content_type.push(None);
            retry_count.push(None);
            error_count.push(None);
            exception_class.push(None);
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
            message.push(format!("{method} {path} HTTP/1.1"));
            status.push(status_value);
            duration_ms.push(duration_value);
            namespace.push(Some(namespace_value));
            user.push(Some(user_value));
            request_id.push(Some(format!("req-{request_id_value:08x}")));
            bytes_sent.push(Some(rng.u32(..65536)));
            cluster.push(Some(cluster_value));
            node.push(Some(node_value));
            pod.push(Some(pod_value));
            container.push(Some(container_value));
            trace_id.push(Some(trace_id_value));
            span_id.push(Some(span_id_value));
            session_id.push(Some(session_id_value));
            user_agent.push(Some("Mozilla/5.0".to_string()));
            content_type.push(Some("application/json".to_string()));
            retry_count.push(None);
            error_count.push(None);
            exception_class.push(None);
        } else {
            message.push(STACK_TRACE.to_string());
            status.push(status_code);
            duration_ms.push(round_tenths(rng.f64() * 5000.0));
            namespace.push(Some(namespace_value));
            user.push(Some(user_value));
            request_id.push(Some(format!("req-{request_id_value:08x}")));
            bytes_sent.push(None);
            cluster.push(Some(cluster_value));
            node.push(Some(node_value));
            pod.push(Some(pod_value));
            container.push(Some(container_value));
            trace_id.push(Some(trace_id_value));
            span_id.push(Some(span_id_value));
            session_id.push(Some(session_id_value));
            user_agent.push(None);
            content_type.push(None);
            retry_count.push(Some(retry_count_value));
            error_count.push(Some(error_count_value));
            exception_class.push(Some("java.lang.NullPointerException".to_string()));
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
        Arc::new(StringArray::from(timestamp)) as ArrayRef,
        Arc::new(StringArray::from(level)) as ArrayRef,
        Arc::new(StringArray::from(message)) as ArrayRef,
        Arc::new(Int64Array::from(
            status.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(duration_ms)) as ArrayRef,
        Arc::new(StringArray::from(service)) as ArrayRef,
        Arc::new(StringArray::from(namespace)) as ArrayRef,
        Arc::new(StringArray::from(user)) as ArrayRef,
        Arc::new(StringArray::from(request_id)) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_sent
                .into_iter()
                .map(|v| v.map(i64::from))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(cluster)) as ArrayRef,
        Arc::new(StringArray::from(node)) as ArrayRef,
        Arc::new(StringArray::from(pod)) as ArrayRef,
        Arc::new(StringArray::from(container)) as ArrayRef,
        Arc::new(StringArray::from(trace_id)) as ArrayRef,
        Arc::new(StringArray::from(span_id)) as ArrayRef,
        Arc::new(Int64Array::from(
            session_id
                .into_iter()
                .map(|v| v.map(|n| n as i64))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(user_agent)) as ArrayRef,
        Arc::new(StringArray::from(content_type)) as ArrayRef,
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
        Arc::new(StringArray::from(exception_class)) as ArrayRef,
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

    let mut timestamp = Vec::with_capacity(count);
    let mut level = Vec::with_capacity(count);
    let mut message = Vec::with_capacity(count);
    let mut status = Vec::with_capacity(count);
    let mut duration_ms = Vec::with_capacity(count);
    let mut cluster = Vec::with_capacity(count);
    let mut node = Vec::with_capacity(count);
    let mut namespace = Vec::with_capacity(count);
    let mut pod = Vec::with_capacity(count);
    let mut container = Vec::with_capacity(count);
    let mut service = Vec::with_capacity(count);
    let mut trace_id = Vec::with_capacity(count);
    let mut span_id = Vec::with_capacity(count);
    let mut request_id = Vec::with_capacity(count);
    let mut session_id = Vec::with_capacity(count);
    let mut user = Vec::with_capacity(count);
    let mut bytes_sent = Vec::with_capacity(count);
    let mut bytes_received = Vec::with_capacity(count);
    let mut user_agent = Vec::with_capacity(count);
    let mut content_type = Vec::with_capacity(count);
    let mut remote_addr = Vec::with_capacity(count);
    let mut host = Vec::with_capacity(count);
    let mut protocol = Vec::with_capacity(count);
    let mut tls_version = Vec::with_capacity(count);
    let mut upstream_latency_ms = Vec::with_capacity(count);
    let mut retry_count = Vec::with_capacity(count);
    let mut error_count = Vec::with_capacity(count);

    for i in 0..count {
        let sample = cardinality.sample(&mut rng);
        let phase = sample.phase;
        let status_code = sample.status_code;
        let cluster_value = sample.cluster.to_string();
        let node_value = sample.node.to_string();
        let pod_value = sample.pod.to_string();
        let container_value = sample.container.to_string();
        let trace_id_value = format!("{:032x}", sample.trace_id);
        let span_id_value = format!("{:016x}", sample.span_id);
        let request_id_value = format!("req-{:08x}", sample.request_id as u32);
        let session_id_value = sample.session_id;
        let retry_count_value = sample.retry_count;
        let error_count_value = sample.error_count;
        let service_idx = sample.service_idx;
        let namespace_idx = sample.namespace_idx;
        let path_idx = sample.path_idx;
        let user_idx = sample.user_idx;
        let sec = i % 60;
        let nano = rng.u32(..1_000_000_000);
        let level_value = level_for_phase(phase).to_string();
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
        let namespace_value = pick_by_idx(NAMESPACES, namespace_idx).to_string();
        let service_value = pick_by_idx(SERVICES, service_idx).to_string();
        let user_value = user_label(user_idx);

        timestamp.push(format!("2024-01-15T10:30:{sec:02}.{nano:09}Z"));
        level.push(level_value);
        message.push(format!("{method} {path} HTTP/1.1"));
        status.push(status_value);
        duration_ms.push(duration_value);
        cluster.push(cluster_value);
        node.push(node_value);
        namespace.push(namespace_value);
        pod.push(pod_value);
        container.push(container_value);
        service.push(service_value);
        trace_id.push(trace_id_value);
        span_id.push(span_id_value);
        request_id.push(request_id_value);
        session_id.push(session_id_value);
        user.push(user_value);
        bytes_sent.push(rng.u32(..65536));
        bytes_received.push(rng.u32(..65536));
        user_agent.push("Mozilla/5.0 (X11; Linux x86_64)".to_string());
        content_type.push("application/json".to_string());
        remote_addr.push(format!("10.{}.{}.{}", rng.u8(..), rng.u8(..), rng.u8(..)));
        host.push("api.example.com".to_string());
        protocol.push("HTTP/1.1".to_string());
        tls_version.push("TLSv1.3".to_string());
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
        Arc::new(StringArray::from(timestamp)) as ArrayRef,
        Arc::new(StringArray::from(level)) as ArrayRef,
        Arc::new(StringArray::from(message)) as ArrayRef,
        Arc::new(Int64Array::from(
            status.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(duration_ms)) as ArrayRef,
        Arc::new(StringArray::from(cluster)) as ArrayRef,
        Arc::new(StringArray::from(node)) as ArrayRef,
        Arc::new(StringArray::from(namespace)) as ArrayRef,
        Arc::new(StringArray::from(pod)) as ArrayRef,
        Arc::new(StringArray::from(container)) as ArrayRef,
        Arc::new(StringArray::from(service)) as ArrayRef,
        Arc::new(StringArray::from(trace_id)) as ArrayRef,
        Arc::new(StringArray::from(span_id)) as ArrayRef,
        Arc::new(StringArray::from(request_id)) as ArrayRef,
        Arc::new(Int64Array::from(
            session_id.into_iter().map(|n| n as i64).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(user)) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_sent.into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int64Array::from(
            bytes_received
                .into_iter()
                .map(i64::from)
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(user_agent)) as ArrayRef,
        Arc::new(StringArray::from(content_type)) as ArrayRef,
        Arc::new(StringArray::from(remote_addr)) as ArrayRef,
        Arc::new(StringArray::from(host)) as ArrayRef,
        Arc::new(StringArray::from(protocol)) as ArrayRef,
        Arc::new(StringArray::from(tls_version)) as ArrayRef,
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
        resource_attrs: Arc::new(vec![
            ("service.name".into(), "bench-service".into()),
            ("service.version".into(), "1.0.0".into()),
            ("host.name".into(), "bench-node-01".into()),
        ]),
        observed_time_ns: 1_705_312_200_000_000_000, // 2024-01-15T10:30:00Z
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use arrow::util::display::array_value_to_string;
    use bytes::Bytes;
    use logfwd_arrow::Scanner;
    use logfwd_core::scan_config::ScanConfig;
    use std::collections::{HashMap, HashSet};

    fn scan_json(data: Vec<u8>) -> RecordBatch {
        Scanner::new(ScanConfig::default())
            .scan_detached(Bytes::from(data))
            .expect("scan must succeed")
    }

    fn assert_batch_matches_scanned_json(expected: &RecordBatch, actual: &RecordBatch) {
        assert_eq!(expected.num_rows(), actual.num_rows(), "row count mismatch");
        assert_eq!(
            expected.num_columns(),
            actual.num_columns(),
            "column count mismatch"
        );

        let expected_schema = expected.schema();
        let actual_schema = actual.schema();
        let expected_fields = expected_schema.fields();
        let actual_fields = actual_schema.fields();
        let expected_by_name: HashMap<&str, usize> = expected_fields
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.name().as_str(), idx))
            .collect();
        let actual_by_name: HashMap<&str, usize> = actual_fields
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.name().as_str(), idx))
            .collect();

        for (name, expected_idx) in &expected_by_name {
            let actual_idx = actual_by_name
                .get(name)
                .unwrap_or_else(|| panic!("missing column {name} in direct batch"));
            let expected_field = &expected_fields[*expected_idx];
            let actual_field = &actual_fields[*actual_idx];
            assert_eq!(
                expected_field.data_type(),
                actual_field.data_type(),
                "column type mismatch for {}",
                expected_field.name()
            );
            assert_eq!(
                expected_field.is_nullable(),
                actual_field.is_nullable(),
                "nullability mismatch for {}",
                expected_field.name()
            );
        }

        for (name, expected_idx) in &expected_by_name {
            let actual_idx = actual_by_name[name];
            let expected_col = expected.column(*expected_idx);
            let actual_col = actual.column(actual_idx);
            for row_idx in 0..expected.num_rows() {
                let expected_value = array_value_to_string(expected_col.as_ref(), row_idx)
                    .unwrap_or_else(|err| {
                        panic!("display expected row {row_idx} column {name}: {err}")
                    });
                let actual_value = array_value_to_string(actual_col.as_ref(), row_idx)
                    .unwrap_or_else(|err| {
                        panic!("display actual row {row_idx} column {name}: {err}")
                    });
                assert_eq!(
                    expected_value, actual_value,
                    "value mismatch at row {row_idx}, column {name}",
                );
            }
        }
    }

    #[test]
    fn cri_k8s_deterministic() {
        let a = gen_cri_k8s(100, 42);
        let b = gen_cri_k8s(100, 42);
        assert_eq!(a, b, "same seed must produce identical output");
    }

    #[test]
    fn cri_k8s_different_seeds_differ() {
        let a = gen_cri_k8s(100, 42);
        let b = gen_cri_k8s(100, 99);
        assert_ne!(a, b, "different seeds must produce different output");
    }

    #[test]
    fn cri_k8s_format_correctness() {
        let data = gen_cri_k8s(200, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            // CRI format: <timestamp> <stream> <P|F> <payload>
            let parts: Vec<&str> = line.splitn(4, ' ').collect();
            assert_eq!(parts.len(), 4, "CRI line must have 4 parts: {line}");
            assert!(
                parts[0].ends_with('Z'),
                "timestamp must end with Z: {}",
                parts[0]
            );
            assert!(
                parts[1] == "stdout" || parts[1] == "stderr",
                "stream must be stdout/stderr: {}",
                parts[1]
            );
            assert!(
                parts[2] == "P" || parts[2] == "F",
                "flag must be P or F: {}",
                parts[2]
            );
            assert!(
                parts[3].starts_with('{'),
                "payload must be JSON: {}",
                parts[3]
            );
        }
    }

    #[test]
    fn cri_k8s_has_partial_lines() {
        let data = gen_cri_k8s(1000, 42);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        let partial_count = text.lines().filter(|l| l.contains(" P ")).count();
        assert!(
            partial_count > 0,
            "expected some partial (P) lines in 1000 lines"
        );
        assert!(
            partial_count < 200,
            "expected ~10% partial lines, got {partial_count}"
        );
    }

    #[test]
    fn production_mixed_deterministic() {
        let a = gen_production_mixed(100, 42);
        let b = gen_production_mixed(100, 42);
        assert_eq!(a, b, "same seed must produce identical output");
    }

    #[test]
    fn production_mixed_format_correctness() {
        let data = gen_production_mixed(200, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            assert!(line.starts_with('{'), "each line must be JSON: {line}");
            assert!(line.ends_with('}'), "each line must end with }}: {line}");
            // Must parse as valid JSON
            let _: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
        }
    }

    #[test]
    fn production_mixed_has_length_variety() {
        let data = gen_production_mixed(1000, 42);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        let lengths: Vec<usize> = text.lines().map(str::len).collect();
        let min = *lengths.iter().min().unwrap();
        let max = *lengths.iter().max().unwrap();
        assert!(
            max > min * 3,
            "expected significant length variety: min={min}, max={max}"
        );
    }

    #[test]
    fn narrow_deterministic() {
        let a = gen_narrow(100, 42);
        let b = gen_narrow(100, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn wide_deterministic() {
        let a = gen_wide(100, 42);
        let b = gen_wide(100, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn narrow_valid_json() {
        let data = gen_narrow(50, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            let _: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
        }
    }

    #[test]
    fn wide_valid_json() {
        let data = gen_wide(50, 1);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");
        for line in text.lines() {
            let _: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
        }
    }

    #[test]
    fn envoy_access_deterministic() {
        let profile = EnvoyAccessProfile::benchmark();
        let a = gen_envoy_access_with_profile(200, 42, profile);
        let b = gen_envoy_access_with_profile(200, 42, profile);
        assert_eq!(a, b, "same seed/profile must produce identical output");
    }

    #[test]
    fn envoy_access_valid_json_and_realistic_skew() {
        let profile = EnvoyAccessProfile::benchmark();
        let data = gen_envoy_access_with_profile(300, 7, profile);
        let text = std::str::from_utf8(&data).expect("valid UTF-8");

        let mut prev_service: Option<String> = None;
        let mut same_service_runs = 0usize;
        let mut success_2xx = 0usize;
        let mut errors_5xx = 0usize;
        let mut routes = HashSet::new();
        let mut services = HashSet::new();

        for line in text.lines() {
            let v: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));

            let service = v["service"].as_str().expect("service string");
            let route_name = v["route_name"].as_str().expect("route_name string");
            let response_code = v["response_code"].as_u64().expect("response_code integer");

            if let Some(prev) = &prev_service {
                if prev == service {
                    same_service_runs += 1;
                }
            }
            prev_service = Some(service.to_string());
            services.insert(service.to_string());
            routes.insert(route_name.to_string());

            if (200..300).contains(&response_code) {
                success_2xx += 1;
            }
            if response_code >= 500 {
                errors_5xx += 1;
            }
        }

        assert!(
            same_service_runs > 120,
            "expected bursty locality, got only {same_service_runs} same-service adjacencies"
        );
        assert!(
            success_2xx > 180,
            "expected strong 2xx skew, got {success_2xx} successes"
        );
        assert!(errors_5xx > 0, "expected some 5xx traffic");
        assert!(
            routes.len() > 8,
            "expected route cardinality beyond a trivial set, got {}",
            routes.len()
        );
        assert!(
            services.len() > 3,
            "expected service diversity, got {}",
            services.len()
        );
    }

    #[test]
    fn narrow_batch_matches_scanned_json() {
        let scanned = scan_json(gen_narrow(256, 42));
        let direct = gen_narrow_batch(256, 42);
        assert_batch_matches_scanned_json(&scanned, &direct);
    }

    #[test]
    fn wide_batch_matches_scanned_json() {
        let scanned = scan_json(gen_wide(256, 42));
        let direct = gen_wide_batch(256, 42);
        assert_batch_matches_scanned_json(&scanned, &direct);
    }

    #[test]
    fn production_mixed_batch_matches_scanned_json() {
        let scanned = scan_json(gen_production_mixed(256, 42));
        let direct = gen_production_mixed_batch(256, 42);
        assert_batch_matches_scanned_json(&scanned, &direct);
    }

    #[test]
    fn envoy_access_batch_matches_scanned_json() {
        let profile = EnvoyAccessProfile::benchmark();
        let scanned = scan_json(gen_envoy_access_with_profile(256, 42, profile));
        let direct = gen_envoy_access_batch_with_profile(256, 42, profile);
        assert_batch_matches_scanned_json(&scanned, &direct);
    }

    #[test]
    fn envoy_access_scale_controls_cardinality() {
        let narrow = gen_envoy_access_with_profile(300, 5, EnvoyAccessProfile::for_scale(1));
        let wide = gen_envoy_access_with_profile(300, 5, EnvoyAccessProfile::for_scale(4));

        let narrow_routes: HashSet<String> = std::str::from_utf8(&narrow)
            .expect("valid UTF-8")
            .lines()
            .map(|line| {
                let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
                v["route_name"]
                    .as_str()
                    .expect("route_name string")
                    .to_string()
            })
            .collect();
        let wide_routes: HashSet<String> = std::str::from_utf8(&wide)
            .expect("valid UTF-8")
            .lines()
            .map(|line| {
                let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
                v["route_name"]
                    .as_str()
                    .expect("route_name string")
                    .to_string()
            })
            .collect();

        assert!(
            wide_routes.len() > narrow_routes.len(),
            "scale should increase route cardinality: narrow={} wide={}",
            narrow_routes.len(),
            wide_routes.len()
        );
    }
}
