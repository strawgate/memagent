#[allow(clippy::too_many_arguments)]
fn build_resources_json(
    rng: &mut fastrand::Rng,
    service_kind: CloudTrailServiceKind,
    action: &CloudTrailActionSpec,
    account_id: &str,
    principal_name: &str,
    role_name: &str,
    session_name: &str,
    region: &str,
    event_index: usize,
    optional_density: u8,
) -> Option<String> {
    if !action.data_event && !chance(rng, optional_density / 2) {
        return None;
    }

    let mut items = Vec::with_capacity(2);
    items.push(build_resource_entry_json(
        rng,
        service_kind,
        action,
        account_id,
        principal_name,
        role_name,
        session_name,
        region,
        event_index,
        0,
    ));
    if action.data_event || chance(rng, optional_density / 2) {
        items.push(build_resource_entry_json(
            rng,
            service_kind,
            action,
            account_id,
            principal_name,
            role_name,
            session_name,
            region,
            event_index,
            1,
        ));
    }
    Some(format!("[{}]", items.join(",")))
}

#[allow(clippy::too_many_arguments)]
fn build_resource_entry_json(
    rng: &mut fastrand::Rng,
    service_kind: CloudTrailServiceKind,
    action: &CloudTrailActionSpec,
    account_id: &str,
    principal_name: &str,
    role_name: &str,
    session_name: &str,
    region: &str,
    event_index: usize,
    ordinal: usize,
) -> String {
    let arn = cloudtrail_resource_arn(
        service_kind,
        action,
        account_id,
        principal_name,
        role_name,
        session_name,
        region,
        event_index,
        ordinal,
    );
    let mut out = JsonObjectWriter::new(256);
    out.field_str("ARN", &arn);
    out.field_str("accountId", account_id);
    out.field_str("type", action.resource_type);
    if chance(rng, 35) {
        out.field_str(
            "resourceName",
            &cloudtrail_resource_name(service_kind, action, principal_name, event_index, ordinal),
        );
    }
    out.finish()
}

#[allow(clippy::too_many_arguments)]
fn cloudtrail_resource_arn(
    service_kind: CloudTrailServiceKind,
    action: &CloudTrailActionSpec,
    account_id: &str,
    principal_name: &str,
    role_name: &str,
    session_name: &str,
    region: &str,
    event_index: usize,
    ordinal: usize,
) -> String {
    match service_kind {
        CloudTrailServiceKind::Ec2 => format!(
            "arn:aws:ec2:{region}:{account_id}:instance/i-{:08x}",
            0x5000_0000u32
                .wrapping_add(event_index as u32)
                .wrapping_add(ordinal as u32)
        ),
        CloudTrailServiceKind::S3 => format!(
            "arn:aws:s3:::audit-{account_id}-{principal_name}/cloudtrail/{event_index:04}/{}",
            action.event_name.to_lowercase()
        ),
        CloudTrailServiceKind::Iam => {
            if action.resource_type.contains("Role") {
                format!("arn:aws:iam::{account_id}:role/{role_name}")
            } else {
                format!("arn:aws:iam::{account_id}:user/{principal_name}")
            }
        }
        CloudTrailServiceKind::Sts => {
            format!("arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}")
        }
        CloudTrailServiceKind::Kms => format!(
            "arn:aws:kms:{region}:{account_id}:key/{:08x}",
            0x6000_0000u32
                .wrapping_add(event_index as u32)
                .wrapping_add(ordinal as u32)
        ),
        CloudTrailServiceKind::Lambda => format!(
            "arn:aws:lambda:{region}:{account_id}:function:cloudtrail-{principal_name}-{}",
            event_index % 16
        ),
        CloudTrailServiceKind::Rds => format!(
            "arn:aws:rds:{region}:{account_id}:db:audit-db-{}",
            event_index % 128
        ),
        CloudTrailServiceKind::CloudTrail => {
            format!("arn:aws:cloudtrail:{region}:{account_id}:trail/org-trail-{account_id}")
        }
    }
}

fn cloudtrail_resource_name(
    service_kind: CloudTrailServiceKind,
    action: &CloudTrailActionSpec,
    principal_name: &str,
    event_index: usize,
    ordinal: usize,
) -> String {
    match service_kind {
        CloudTrailServiceKind::Ec2 => format!(
            "i-{:08x}",
            0x7000_0000u32 + event_index as u32 + ordinal as u32
        ),
        CloudTrailServiceKind::S3 => format!(
            "cloudtrail/{}/{}",
            principal_name,
            action.event_name.to_lowercase()
        ),
        CloudTrailServiceKind::Iam => principal_name.to_string(),
        CloudTrailServiceKind::Sts => format!("{}-{}", principal_name, event_index % 1_000),
        CloudTrailServiceKind::Kms => format!("key/{:08x}", event_index as u32 + ordinal as u32),
        CloudTrailServiceKind::Lambda => {
            format!("cloudtrail-{}-{}", principal_name, event_index % 16)
        }
        CloudTrailServiceKind::Rds => format!("audit-db-{}", event_index % 128),
        CloudTrailServiceKind::CloudTrail => format!("org-trail-{}", event_index % 16),
    }
}

fn build_additional_event_data_json(
    rng: &mut fastrand::Rng,
    service_kind: CloudTrailServiceKind,
    action: &CloudTrailActionSpec,
    region: &str,
    optional_density: u8,
) -> Option<String> {
    if !chance(rng, optional_density / 2) && !action.data_event {
        return None;
    }
    let mut out = JsonObjectWriter::new(192);
    out.field_str("SignatureVersion", pick(rng, &["SigV4", "SigV2"]));
    out.field_str(
        "CipherSuite",
        pick(
            rng,
            &["ECDHE-RSA-AES128-GCM-SHA256", "TLS_AES_128_GCM_SHA256"],
        ),
    );
    out.field_str("tlsVersion", pick(rng, &["TLSv1.3", "TLSv1.2"]));
    out.field_str("region", region);
    if matches!(
        service_kind,
        CloudTrailServiceKind::S3 | CloudTrailServiceKind::Lambda
    ) {
        out.field_str("bucketOwner", "cloudtrail-bench");
    }
    if action.data_event {
        out.field_raw(
            "bytesTransferred",
            &format!("{}", 512 + rng.usize(..16_384)),
        );
    }
    Some(out.finish())
}

fn cloudtrail_error_pair(
    rng: &mut fastrand::Rng,
    read_only: bool,
    data_event: bool,
) -> Option<(&'static str, &'static str)> {
    let base = if data_event {
        9
    } else if read_only {
        4
    } else {
        7
    };
    if !chance(rng, base) {
        return None;
    }
    match rng.usize(..5) {
        0 => Some((
            "AccessDenied",
            "User is not authorized to perform this action",
        )),
        1 => Some(("ThrottlingException", "Rate exceeded")),
        2 => Some(("InvalidParameter", "Invalid request parameter")),
        3 => Some(("ResourceNotFoundException", "Resource not found")),
        _ => Some((
            "InternalFailure",
            "The request processing failed because of an unknown error",
        )),
    }
}

fn build_tls_details_json(
    rng: &mut fastrand::Rng,
    service_kind: CloudTrailServiceKind,
    optional_density: u8,
) -> Option<String> {
    if matches!(service_kind, CloudTrailServiceKind::CloudTrail)
        || !chance(rng, optional_density / 3)
    {
        return None;
    }
    let mut out = JsonObjectWriter::new(240);
    out.field_str("tlsVersion", pick(rng, &["TLSv1.3", "TLSv1.2"]));
    out.field_str(
        "cipherSuite",
        pick(
            rng,
            &["TLS_AES_128_GCM_SHA256", "ECDHE-RSA-AES128-GCM-SHA256"],
        ),
    );
    out.field_str(
        "clientProvidedHostHeader",
        pick(
            rng,
            &[
                "console.aws.amazon.com",
                "api.aws.amazon.com",
                "s3.amazonaws.com",
            ],
        ),
    );
    out.field_str(
        "clientProvidedPrincipal",
        pick(rng, &["console", "cli", "sdk"]),
    );
    Some(out.finish())
}
