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

    #[must_use]
    pub fn with_account_count(mut self, account_count: usize) -> Self {
        self.account_count = account_count.max(1);
        self
    }

    #[must_use]
    pub fn with_account_tenure(mut self, account_tenure: usize) -> Self {
        self.account_tenure = account_tenure.max(1);
        self
    }

    #[must_use]
    pub fn with_principal_count(mut self, principal_count: usize) -> Self {
        self.principal_count = principal_count.max(1);
        self
    }

    #[must_use]
    pub fn with_principal_tenure(mut self, principal_tenure: usize) -> Self {
        self.principal_tenure = principal_tenure.max(1);
        self
    }

    #[must_use]
    pub fn with_service_mix(mut self, service_mix: CloudTrailServiceMix) -> Self {
        self.service_mix = service_mix;
        self
    }

    #[must_use]
    pub fn with_region_mix(mut self, region_mix: CloudTrailRegionMix) -> Self {
        self.region_mix = region_mix;
        self
    }

    #[must_use]
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

fn append_user_label(out: &mut String, idx: usize) {
    out.clear();
    let _ = write!(out, "user-{idx:03}");
}

fn append_timestamp_utc(out: &mut String, sec: usize, nano: u32) {
    out.clear();
    let _ = write!(out, "2024-01-15T10:30:{sec:02}.{nano:09}Z");
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
    out.clear();
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
