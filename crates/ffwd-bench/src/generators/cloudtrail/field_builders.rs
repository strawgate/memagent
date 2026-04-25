struct JsonObjectWriter {
    out: String,
    first: bool,
}

impl JsonObjectWriter {
    fn new(capacity: usize) -> Self {
        let mut out = String::with_capacity(capacity);
        out.push('{');
        Self { out, first: true }
    }

    fn field_prefix(&mut self) {
        if self.first {
            self.first = false;
        } else {
            self.out.push(',');
        }
    }

    fn field_str(&mut self, key: &str, value: &str) {
        self.field_prefix();
        self.out.push('"');
        self.out.push_str(key);
        self.out.push_str("\":\"");
        json_escape(value, &mut self.out);
        self.out.push('"');
    }

    fn field_bool(&mut self, key: &str, value: bool) {
        self.field_prefix();
        self.out.push('"');
        self.out.push_str(key);
        self.out.push_str("\":");
        self.out.push_str(if value { "true" } else { "false" });
    }

    fn field_raw(&mut self, key: &str, value: &str) {
        self.field_prefix();
        self.out.push('"');
        self.out.push_str(key);
        self.out.push_str("\":");
        self.out.push_str(value);
    }

    fn finish(mut self) -> String {
        self.out.push('}');
        self.out
    }
}

fn cloudtrail_services(mix: CloudTrailServiceMix) -> &'static [CloudTrailServiceSpec] {
    match mix {
        CloudTrailServiceMix::Balanced => SERVICE_BALANCED,
        CloudTrailServiceMix::SecurityHeavy => SERVICE_SECURITY_HEAVY,
        CloudTrailServiceMix::StorageHeavy => SERVICE_STORAGE_HEAVY,
        CloudTrailServiceMix::ComputeHeavy => SERVICE_COMPUTE_HEAVY,
    }
}

fn pick_cloudtrail_action<'a>(
    rng: &mut fastrand::Rng,
    actions: &'a [CloudTrailActionSpec],
) -> &'a CloudTrailActionSpec {
    debug_assert!(!actions.is_empty());
    &actions[rng.usize(..actions.len())]
}

fn cloudtrail_region_pool(region_mix: CloudTrailRegionMix) -> &'static [&'static str] {
    match region_mix {
        CloudTrailRegionMix::GlobalOnly => CLOUDTRAIL_REGIONS_GLOBAL,
        CloudTrailRegionMix::Regional => CLOUDTRAIL_REGIONS_REGIONAL,
        CloudTrailRegionMix::MultiRegion => CLOUDTRAIL_REGIONS_MULTI,
    }
}

fn pick_cloudtrail_region(
    rng: &mut fastrand::Rng,
    region_mix: CloudTrailRegionMix,
    service_kind: CloudTrailServiceKind,
) -> &'static str {
    if matches!(
        service_kind,
        CloudTrailServiceKind::Iam | CloudTrailServiceKind::Sts | CloudTrailServiceKind::CloudTrail
    ) {
        return CLOUDTRAIL_REGIONS_GLOBAL[0];
    }
    pick(rng, cloudtrail_region_pool(region_mix))
}

fn pick_tenure_slot(
    rng: &mut fastrand::Rng,
    index: usize,
    tenure: usize,
    pool_len: usize,
) -> usize {
    if pool_len <= 1 {
        return 0;
    }
    let tenure = tenure.max(1);
    let base = index / tenure;
    let jitter = rng.usize(..2);
    (base + jitter) % pool_len
}

fn chance(rng: &mut fastrand::Rng, pct: u8) -> bool {
    pct > 0 && rng.u8(..100) < pct
}

fn cloudtrail_uuid_like(rng: &mut fastrand::Rng) -> String {
    uuid_like_from_value(rng.u128(..))
}

fn cloudtrail_event_time(index: usize, rng: &mut fastrand::Rng) -> String {
    let sec = index % 60;
    let nano = rng.u32(..1_000_000_000);
    format!("2024-01-15T10:30:{sec:02}.{nano:09}Z")
}

fn cloudtrail_source_ip<'a>(
    rng: &mut fastrand::Rng,
    state: &'a CloudTrailState,
    account_slot: usize,
    principal_slot: usize,
    service_kind: CloudTrailServiceKind,
    optional_density: u8,
) -> &'a str {
    if matches!(service_kind, CloudTrailServiceKind::CloudTrail)
        && chance(rng, optional_density / 2)
    {
        return "AWS Internal";
    }
    state.source_ip(
        principal_slot
            .wrapping_mul(7)
            .wrapping_add(account_slot.wrapping_mul(11)),
    )
}

fn cloudtrail_user_agent(
    rng: &mut fastrand::Rng,
    service_kind: CloudTrailServiceKind,
    principal_slot: usize,
) -> String {
    match service_kind {
        CloudTrailServiceKind::CloudTrail => "cloudtrail.amazonaws.com".to_string(),
        CloudTrailServiceKind::Iam | CloudTrailServiceKind::Sts => {
            if principal_slot.is_multiple_of(4) {
                "console.amazonaws.com".to_string()
            } else {
                pick(rng, CLOUDTRAIL_USER_AGENTS).to_string()
            }
        }
        CloudTrailServiceKind::S3 | CloudTrailServiceKind::Kms => {
            pick(rng, CLOUDTRAIL_USER_AGENTS).to_string()
        }
        CloudTrailServiceKind::Ec2 | CloudTrailServiceKind::Lambda | CloudTrailServiceKind::Rds => {
            if principal_slot.is_multiple_of(3) {
                "aws-cli/2.15.0 Python/3.11.8 Linux/6.6".to_string()
            } else {
                pick(rng, CLOUDTRAIL_USER_AGENTS).to_string()
            }
        }
    }
}

fn pick_cloudtrail_identity_kind(
    rng: &mut fastrand::Rng,
    service_kind: CloudTrailServiceKind,
    read_only: bool,
) -> CloudTrailIdentityKind {
    match service_kind {
        CloudTrailServiceKind::CloudTrail => CloudTrailIdentityKind::AwsService,
        CloudTrailServiceKind::Sts => weighted_pick(
            rng,
            &[
                (CloudTrailIdentityKind::AssumedRole, 44),
                (CloudTrailIdentityKind::FederatedUser, 26),
                (CloudTrailIdentityKind::IamUser, 20),
                (CloudTrailIdentityKind::Root, 10),
            ],
        ),
        CloudTrailServiceKind::Iam => weighted_pick(
            rng,
            &[
                (CloudTrailIdentityKind::IamUser, 40),
                (CloudTrailIdentityKind::AssumedRole, 32),
                (CloudTrailIdentityKind::Root, 18),
                (CloudTrailIdentityKind::FederatedUser, 10),
            ],
        ),
        CloudTrailServiceKind::Ec2 | CloudTrailServiceKind::Rds => weighted_pick(
            rng,
            &[
                (
                    CloudTrailIdentityKind::AssumedRole,
                    if read_only { 48 } else { 58 },
                ),
                (CloudTrailIdentityKind::IamUser, 24),
                (CloudTrailIdentityKind::FederatedUser, 10),
                (CloudTrailIdentityKind::AwsService, 8),
                (CloudTrailIdentityKind::Root, 4),
            ],
        ),
        CloudTrailServiceKind::S3 => weighted_pick(
            rng,
            &[
                (
                    CloudTrailIdentityKind::AssumedRole,
                    if read_only { 52 } else { 42 },
                ),
                (CloudTrailIdentityKind::IamUser, 24),
                (CloudTrailIdentityKind::AwsService, 16),
                (CloudTrailIdentityKind::FederatedUser, 8),
            ],
        ),
        CloudTrailServiceKind::Kms => weighted_pick(
            rng,
            &[
                (CloudTrailIdentityKind::AssumedRole, 50),
                (CloudTrailIdentityKind::IamUser, 24),
                (CloudTrailIdentityKind::AwsService, 16),
                (CloudTrailIdentityKind::Root, 10),
            ],
        ),
        CloudTrailServiceKind::Lambda => weighted_pick(
            rng,
            &[
                (CloudTrailIdentityKind::AwsService, 34),
                (CloudTrailIdentityKind::AssumedRole, 40),
                (CloudTrailIdentityKind::IamUser, 18),
                (CloudTrailIdentityKind::FederatedUser, 8),
            ],
        ),
    }
}

fn cloudtrail_identity_type(identity_kind: CloudTrailIdentityKind) -> &'static str {
    match identity_kind {
        CloudTrailIdentityKind::Root => "Root",
        CloudTrailIdentityKind::IamUser => "IAMUser",
        CloudTrailIdentityKind::AssumedRole => "AssumedRole",
        CloudTrailIdentityKind::AwsService => "AWSService",
        CloudTrailIdentityKind::FederatedUser => "FederatedUser",
    }
}

fn cloudtrail_event_type(
    service_kind: CloudTrailServiceKind,
    identity_kind: CloudTrailIdentityKind,
    action: &CloudTrailActionSpec,
) -> &'static str {
    match (
        service_kind,
        identity_kind,
        action.read_only,
        action.data_event,
    ) {
        (CloudTrailServiceKind::CloudTrail, _, _, _) => "AwsServiceEvent",
        (_, CloudTrailIdentityKind::AwsService, _, _) => "AwsServiceEvent",
        (_, _, _, true) => "AwsApiCall",
        (_, CloudTrailIdentityKind::Root, _, _) => "AwsConsoleAction",
        (_, CloudTrailIdentityKind::IamUser, true, _) => "AwsConsoleAction",
        _ => "AwsApiCall",
    }
}

struct IdentityInputs<'a> {
    account_id: &'a str,
    principal_name: &'a str,
    role_name: &'a str,
    session_name: &'a str,
    principal_id: &'a str,
    arn: Option<&'a str>,
    optional_density: u8,
    service_kind: CloudTrailServiceKind,
}

struct EventShapeInputs<'a> {
    service_kind: CloudTrailServiceKind,
    action: &'a CloudTrailActionSpec,
    account_id: &'a str,
    principal_name: &'a str,
    role_name: &'a str,
    session_name: &'a str,
    region: &'a str,
    event_index: usize,
    optional_density: u8,
}

fn build_user_identity_json(
    rng: &mut fastrand::Rng,
    identity_kind: CloudTrailIdentityKind,
    inputs: &IdentityInputs<'_>,
) -> String {
    let mut out = JsonObjectWriter::new(384);
    out.field_str("type", cloudtrail_identity_type(identity_kind));
    out.field_str("principalId", inputs.principal_id);
    out.field_str("accountId", inputs.account_id);
    if let Some(arn) = inputs.arn {
        out.field_str("arn", arn);
    } else if chance(rng, inputs.optional_density / 2) {
        out.field_str("invokedBy", "cloudtrail.amazonaws.com");
    }

    match identity_kind {
        CloudTrailIdentityKind::Root => {}
        CloudTrailIdentityKind::IamUser => {
            out.field_str("userName", inputs.principal_name);
            if chance(rng, inputs.optional_density) {
                out.field_str("accessKeyId", &format!("AKIA{:08X}", rng.u32(..)));
            }
        }
        CloudTrailIdentityKind::AssumedRole | CloudTrailIdentityKind::FederatedUser => {
            out.field_str("userName", inputs.principal_name);
            if chance(rng, inputs.optional_density) {
                out.field_raw(
                    "sessionContext",
                    &build_session_context_json(rng, identity_kind, inputs),
                );
            }
        }
        CloudTrailIdentityKind::AwsService => {}
    }

    if matches!(
        inputs.service_kind,
        CloudTrailServiceKind::Lambda | CloudTrailServiceKind::Kms
    ) && chance(rng, inputs.optional_density / 2)
    {
        out.field_str("sourceIdentity", inputs.principal_name);
    }

    if matches!(
        inputs.service_kind,
        CloudTrailServiceKind::Sts | CloudTrailServiceKind::CloudTrail
    ) || matches!(identity_kind, CloudTrailIdentityKind::AwsService)
    {
        out.field_str("invokedBy", "cloudtrail.amazonaws.com");
    }

    out.finish()
}

fn build_session_context_json(
    rng: &mut fastrand::Rng,
    identity_kind: CloudTrailIdentityKind,
    inputs: &IdentityInputs<'_>,
) -> String {
    let mut out = JsonObjectWriter::new(320);
    out.field_raw(
        "sessionIssuer",
        &build_session_issuer_json(
            identity_kind,
            inputs.account_id,
            inputs.role_name,
            inputs.principal_id,
            inputs.arn,
        ),
    );
    let mut attr = JsonObjectWriter::new(160);
    attr.field_str("creationDate", &cloudtrail_event_time(rng.usize(..60), rng));
    attr.field_str(
        "mfaAuthenticated",
        if chance(rng, inputs.optional_density) {
            "true"
        } else {
            "false"
        },
    );
    if chance(rng, inputs.optional_density / 2) {
        attr.field_str("sessionCredentialFromConsole", "true");
    }
    if chance(rng, inputs.optional_density / 4) {
        attr.field_str("sessionName", inputs.session_name);
    }
    out.field_raw("attributes", &attr.finish());
    out.finish()
}

fn build_session_issuer_json(
    identity_kind: CloudTrailIdentityKind,
    account_id: &str,
    role_name: &str,
    principal_id: &str,
    arn: Option<&str>,
) -> String {
    let mut out = JsonObjectWriter::new(256);
    let issuer_type = match identity_kind {
        CloudTrailIdentityKind::FederatedUser => "Role",
        CloudTrailIdentityKind::AssumedRole => "Role",
        CloudTrailIdentityKind::IamUser => "User",
        CloudTrailIdentityKind::Root => "Root",
        CloudTrailIdentityKind::AwsService => "AWSService",
    };
    out.field_str("type", issuer_type);
    out.field_str("principalId", principal_id);
    if let Some(arn) = arn {
        out.field_str("arn", arn);
    }
    out.field_str("accountId", account_id);
    out.field_str("userName", role_name);
    out.finish()
}

fn shared_event_tenure(profile: CloudTrailProfile) -> usize {
    (profile.principal_tenure / 2).max(2)
}

fn shared_event_slot(
    event_index: usize,
    account_slot: usize,
    principal_slot: usize,
    pool_len: usize,
    profile: CloudTrailProfile,
) -> usize {
    let epoch = event_index / shared_event_tenure(profile);
    account_slot
        .wrapping_mul(131)
        .wrapping_add(principal_slot.wrapping_mul(17))
        .wrapping_add(epoch)
        % pool_len.max(1)
}

fn cloudtrail_source_ip_seeded(idx: usize) -> String {
    match idx % 3 {
        0 => format!("198.51.100.{}", 10 + ((idx as u32 * 17 + 3) % 200)),
        1 => format!("203.0.113.{}", 10 + ((idx as u32 * 29 + 7) % 200)),
        _ => format!("192.0.2.{}", 10 + ((idx as u32 * 41 + 11) % 200)),
    }
}

fn build_request_parameters_json(
    rng: &mut fastrand::Rng,
    inputs: &EventShapeInputs<'_>,
) -> Option<String> {
    if inputs.action.read_only && !chance(rng, inputs.optional_density / 2) {
        return None;
    }

    let mut out = JsonObjectWriter::new(384);
    match inputs.service_kind {
        CloudTrailServiceKind::Ec2 => {
            if inputs.action.event_name == "RunInstances" {
                out.field_raw(
                    "instancesSet",
                    &format!(
                        r#"{{"items":[{{"imageId":"ami-{:08x}","instanceType":"{}","subnetId":"subnet-{:08x}","securityGroupId":"sg-{:08x}"}}]}}"#,
                        rng.u32(..),
                        pick(rng, &["t3.micro", "t3.small", "m6i.large", "c7g.large"]),
                        rng.u32(..),
                        rng.u32(..)
                    ),
                );
                out.field_raw("minCount", "1");
                out.field_raw("maxCount", "1");
                out.field_str("keyName", &format!("cloudtrail-key-{:04}", inputs.event_index));
            } else {
                out.field_raw("instanceIds", &format!(r#"["i-{:08x}"]"#, rng.u32(..)));
                out.field_bool("dryRun", chance(rng, inputs.optional_density / 2));
            }
        }
        CloudTrailServiceKind::S3 => {
            out.field_str(
                "bucketName",
                &format!("audit-{}-{}", inputs.account_id, inputs.principal_name),
            );
            out.field_str(
                "key",
                &format!(
                    "cloudtrail/{region}/{event_index:04}/{}.json",
                    inputs.action.event_name.to_lowercase(),
                    region = inputs.region,
                    event_index = inputs.event_index
                ),
            );
            if inputs.action.data_event {
                out.field_str(
                    "x-amz-server-side-encryption",
                    pick(rng, &["AES256", "aws:kms"]),
                );
            }
        }
        CloudTrailServiceKind::Iam => {
            out.field_str("userName", inputs.principal_name);
            if inputs.action.event_name.contains("Role") {
                out.field_str("roleName", inputs.role_name);
            }
            out.field_str("path", "/service/");
        }
        CloudTrailServiceKind::Sts => {
            out.field_str(
                "roleArn",
                &format!("arn:aws:iam::{}:role/{}", inputs.account_id, inputs.role_name),
            );
            out.field_str("roleSessionName", inputs.session_name);
            out.field_raw("durationSeconds", "3600");
        }
        CloudTrailServiceKind::Kms => {
            out.field_str(
                "keyId",
                &format!(
                    "arn:aws:kms:{}:{}:key/{:08x}",
                    inputs.region,
                    inputs.account_id,
                    rng.u32(..)
                ),
            );
            out.field_raw(
                "encryptionContext",
                &format!(
                    r#"{{"env":"{}","service":"{}"}}"#,
                    pick(rng, &["prod", "stage", "dev"]),
                    inputs.action.resource_type
                ),
            );
        }
        CloudTrailServiceKind::Lambda => {
            out.field_str(
                "functionName",
                &format!(
                    "cloudtrail-{}-{}",
                    inputs.principal_name,
                    inputs.event_index % 16
                ),
            );
            if inputs.action.event_name == "Invoke" {
                out.field_str("qualifier", "$LATEST");
            } else {
                out.field_bool("publish", chance(rng, inputs.optional_density));
            }
        }
        CloudTrailServiceKind::Rds => {
            out.field_str(
                "dBInstanceIdentifier",
                &format!("audit-db-{}", inputs.event_index % 128),
            );
            out.field_str(
                "dBInstanceClass",
                pick(rng, &["db.t3.micro", "db.t3.medium", "db.m6g.large"]),
            );
            out.field_str(
                "engine",
                pick(rng, &["postgres", "mysql", "aurora-postgresql"]),
            );
        }
        CloudTrailServiceKind::CloudTrail => {
            out.field_str("trailName", &format!("org-trail-{}", inputs.account_id));
            if inputs.action.event_name == "LookupEvents" {
                out.field_raw("maxResults", "50");
            }
        }
    }

    if chance(rng, inputs.optional_density / 3) {
        out.field_str("clientRequestToken", &cloudtrail_uuid_like(rng));
    }

    Some(out.finish())
}
fn build_response_elements_json(
    rng: &mut fastrand::Rng,
    inputs: &EventShapeInputs<'_>,
) -> Option<String> {
    if inputs.action.read_only && !chance(rng, inputs.optional_density / 2) {
        return None;
    }
    let mut out = JsonObjectWriter::new(256);
    match inputs.service_kind {
        CloudTrailServiceKind::Ec2 => {
            if inputs.action.event_name == "RunInstances" {
                out.field_str("reservationId", &format!("r-{:08x}", rng.u32(..)));
                out.field_raw(
                    "instancesSet",
                    &format!(
                        r#"{{"items":[{{"instanceId":"i-{:08x}","currentState":{{"code":16,"name":"running"}}}}]}}"#,
                        rng.u32(..)
                    ),
                );
            } else {
                out.field_bool("return", chance(rng, inputs.optional_density));
            }
        }
        CloudTrailServiceKind::S3 => {
            if inputs.action.data_event {
                out.field_str(
                    "x-amz-version-id",
                    &format!("{}-{:08x}", inputs.event_index, rng.u32(..)),
                );
                out.field_str("etag", &format!("\"{:08x}\"", rng.u32(..)));
            } else {
                out.field_str("bucketRegion", inputs.region);
            }
        }
        CloudTrailServiceKind::Iam => {
            out.field_raw(
                "user",
                &format!(
                    r#"{{"userName":"{}","arn":"arn:aws:iam::{}:user/{}"}}"#,
                    inputs.principal_name, inputs.account_id, inputs.principal_name
                ),
            );
        }
        CloudTrailServiceKind::Sts => {
            out.field_raw(
                "credentials",
                &format!(
                    r#"{{"accessKeyId":"AKIA{:08X}","sessionToken":"{:032x}","expiration":"2024-01-15T11:30:00Z"}}"#,
                    rng.u32(..),
                    rng.u128(..)
                ),
            );
        }
        CloudTrailServiceKind::Kms => {
            out.field_str(
                "keyId",
                &format!(
                    "arn:aws:kms:{}:{}:key/{:08x}",
                    inputs.region,
                    inputs.account_id,
                    rng.u32(..)
                ),
            );
            out.field_str(
                "encryptionAlgorithm",
                pick(rng, &["SYMMETRIC_DEFAULT", "RSAES_OAEP_SHA_256"]),
            );
        }
        CloudTrailServiceKind::Lambda => {
            out.field_str(
                "functionName",
                &format!(
                    "cloudtrail-{}-{}",
                    inputs.principal_name,
                    inputs.event_index % 16
                ),
            );
            out.field_str("responsePayload", "{\"status\":\"ok\"}");
        }
        CloudTrailServiceKind::Rds => {
            out.field_str(
                "dBInstanceIdentifier",
                &format!("audit-db-{}", inputs.event_index % 128),
            );
            out.field_str(
                "dBInstanceStatus",
                pick(rng, &["available", "modifying", "backing-up"]),
            );
        }
        CloudTrailServiceKind::CloudTrail => {
            out.field_str("trailName", &format!("org-trail-{}", inputs.account_id));
            out.field_bool("isLogging", chance(rng, inputs.optional_density));
        }
    }
    Some(out.finish())
}
