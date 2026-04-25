pub fn gen_cloudtrail_audit(count: usize, seed: u64) -> Vec<u8> {
    gen_cloudtrail_audit_with_profile(count, seed, CloudTrailProfile::benchmark_default())
}

/// Generate CloudTrail-like audit logs using a tunable realism profile.
pub fn gen_cloudtrail_audit_with_profile(
    count: usize,
    seed: u64,
    profile: CloudTrailProfile,
) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let state = CloudTrailState::new(profile);
    let services = cloudtrail_services(profile.service_mix);
    let mut buf = String::with_capacity(count.saturating_mul(1_400));

    for i in 0..count {
        let service = weighted_choice_pick(&mut rng, services);
        let action = pick_cloudtrail_action(&mut rng, service.actions);
        let account_slot =
            pick_tenure_slot(&mut rng, i, profile.account_tenure, state.accounts.len());
        let principal_slot = pick_tenure_slot(
            &mut rng,
            i + account_slot,
            profile.principal_tenure,
            state.principals.len(),
        );
        let account_id = state.account_id(account_slot);
        let principal_name = state.principal_name(principal_slot);
        let role_name = state.role_name(principal_slot);
        let session_name = state.session_name(principal_slot);
        let region = pick_cloudtrail_region(&mut rng, profile.region_mix, service.kind);
        let event_time = cloudtrail_event_time(i, &mut rng);
        let event_id = cloudtrail_uuid_like(&mut rng);
        let identity_kind = pick_cloudtrail_identity_kind(&mut rng, service.kind, action.read_only);
        let principal_id = state.principal_id(identity_kind, account_slot, principal_slot);
        let user_arn = state.arn(identity_kind, account_slot, principal_slot);
        let shared_event_id = (action.data_event
            || chance(&mut rng, profile.optional_field_density / 3))
        .then(|| {
            state.shared_event_id(shared_event_slot(
                i,
                account_slot,
                principal_slot,
                state.shared_event_ids.len(),
                profile,
            ))
        });
        let source_ip = cloudtrail_source_ip(
            &mut rng,
            &state,
            account_slot,
            principal_slot,
            service.kind,
            profile.optional_field_density,
        );
        let user_agent = cloudtrail_user_agent(&mut rng, service.kind, principal_slot);
        let identity_inputs = IdentityInputs {
            account_id,
            principal_name,
            role_name,
            session_name,
            principal_id,
            arn: user_arn,
            optional_density: profile.optional_field_density,
            service_kind: service.kind,
        };
        let user_identity = build_user_identity_json(
            &mut rng,
            identity_kind,
            &identity_inputs,
        );
        let event_shape = EventShapeInputs {
            service_kind: service.kind,
            action,
            account_id,
            principal_name,
            role_name,
            session_name,
            region,
            event_index: i,
            optional_density: profile.optional_field_density,
        };
        let request_parameters = build_request_parameters_json(
            &mut rng,
            &event_shape,
        );
        let response_elements = build_response_elements_json(
            &mut rng,
            &event_shape,
        );
        let resources = build_resources_json(
            &mut rng,
            service.kind,
            action,
            account_id,
            principal_name,
            role_name,
            session_name,
            region,
            i,
            profile.optional_field_density,
        );
        let additional_event_data = build_additional_event_data_json(
            &mut rng,
            service.kind,
            action,
            region,
            profile.optional_field_density,
        );
        let error = cloudtrail_error_pair(&mut rng, action.read_only, action.data_event);
        let event_type = cloudtrail_event_type(service.kind, identity_kind, action);
        let event_category = if action.data_event {
            "Data"
        } else {
            "Management"
        };
        let vpc_endpoint_id = (chance(&mut rng, profile.optional_field_density / 4)
            && !matches!(service.kind, CloudTrailServiceKind::CloudTrail))
        .then(|| format!("vpce-{:08x}", rng.u32(..)));
        let tls_details =
            build_tls_details_json(&mut rng, service.kind, profile.optional_field_density);

        let mut event = JsonObjectWriter::new(1_280);
        event.field_str("eventVersion", CLOUDTRAIL_EVENT_VERSION);
        event.field_str("eventTime", &event_time);
        event.field_str("eventSource", service.event_source);
        event.field_str("eventName", action.event_name);
        event.field_str("awsRegion", region);
        event.field_str("sourceIPAddress", source_ip);
        event.field_str("userAgent", &user_agent);
        event.field_str("eventID", &event_id);
        event.field_str("eventType", event_type);
        event.field_str("recipientAccountId", account_id);
        event.field_bool("readOnly", action.read_only);
        event.field_bool("managementEvent", !action.data_event);
        event.field_str("eventCategory", event_category);
        event.field_raw("userIdentity", &user_identity);
        if let Some(request_parameters) = request_parameters {
            event.field_raw("requestParameters", &request_parameters);
        }
        if let Some(response_elements) = response_elements {
            event.field_raw("responseElements", &response_elements);
        }
        if let Some(resources) = resources {
            event.field_raw("resources", &resources);
        }
        if let Some(additional_event_data) = additional_event_data {
            event.field_raw("additionalEventData", &additional_event_data);
        }
        if let Some(shared_event_id) = shared_event_id {
            event.field_str("sharedEventID", shared_event_id);
        }
        if let Some(vpc_endpoint_id) = vpc_endpoint_id {
            event.field_str("vpcEndpointId", &vpc_endpoint_id);
        }
        if let Some((error_code, error_message)) = error {
            event.field_str("errorCode", error_code);
            event.field_str("errorMessage", error_message);
        }
        if let Some(tls_details) = tls_details {
            event.field_raw("tlsDetails", &tls_details);
        }

        buf.push_str(&event.finish());
        buf.push('\n');
    }

    buf.into_bytes()
}

/// Generate CloudTrail-like audit logs directly as a detached Arrow
/// [`RecordBatch`].
///
/// This path uses the same deterministic workload model as
/// [`gen_cloudtrail_audit_with_profile`], but skips top-level NDJSON
/// materialization so downstream benchmarks can start at typed columns.
pub fn gen_cloudtrail_batch(count: usize, seed: u64) -> RecordBatch {
    gen_cloudtrail_batch_with_profile(count, seed, CloudTrailProfile::benchmark_default())
}

/// Generate CloudTrail-like audit logs as a detached Arrow [`RecordBatch`]
/// using a tunable realism profile.
pub fn gen_cloudtrail_batch_with_profile(
    count: usize,
    seed: u64,
    profile: CloudTrailProfile,
) -> RecordBatch {
    let mut rng = fastrand::Rng::with_seed(seed);
    let state = CloudTrailState::new(profile);
    let services = cloudtrail_services(profile.service_mix);
    let mut builders = CloudTrailBatchBuilders::new(count);

    for i in 0..count {
        let service = weighted_choice_pick(&mut rng, services);
        let action = pick_cloudtrail_action(&mut rng, service.actions);
        let account_slot =
            pick_tenure_slot(&mut rng, i, profile.account_tenure, state.accounts.len());
        let principal_slot = pick_tenure_slot(
            &mut rng,
            i + account_slot,
            profile.principal_tenure,
            state.principals.len(),
        );
        let account_id = state.account_id(account_slot);
        let principal_name = state.principal_name(principal_slot);
        let role_name = state.role_name(principal_slot);
        let session_name = state.session_name(principal_slot);
        let region = pick_cloudtrail_region(&mut rng, profile.region_mix, service.kind);
        let event_time = cloudtrail_event_time(i, &mut rng);
        let event_id = cloudtrail_uuid_like(&mut rng);
        let identity_kind = pick_cloudtrail_identity_kind(&mut rng, service.kind, action.read_only);
        let principal_id = state.principal_id(identity_kind, account_slot, principal_slot);
        let user_arn = state.arn(identity_kind, account_slot, principal_slot);
        let shared_event_id = (action.data_event
            || chance(&mut rng, profile.optional_field_density / 3))
        .then(|| {
            state.shared_event_id(shared_event_slot(
                i,
                account_slot,
                principal_slot,
                state.shared_event_ids.len(),
                profile,
            ))
        });
        let source_ip = cloudtrail_source_ip(
            &mut rng,
            &state,
            account_slot,
            principal_slot,
            service.kind,
            profile.optional_field_density,
        );
        let user_agent = cloudtrail_user_agent(&mut rng, service.kind, principal_slot);
        let request_parameters_present =
            !action.read_only || chance(&mut rng, profile.optional_field_density / 2);
        let response_elements_present =
            !action.read_only || chance(&mut rng, profile.optional_field_density / 2);
        let resources_present =
            action.data_event || chance(&mut rng, profile.optional_field_density / 2);
        let additional_event_data_present =
            action.data_event || chance(&mut rng, profile.optional_field_density / 2);
        let error = cloudtrail_error_pair(&mut rng, action.read_only, action.data_event);
        let event_type = cloudtrail_event_type(service.kind, identity_kind, action);
        let event_category = if action.data_event {
            "Data"
        } else {
            "Management"
        };
        let vpc_endpoint_id = (chance(&mut rng, profile.optional_field_density / 4)
            && !matches!(service.kind, CloudTrailServiceKind::CloudTrail))
        .then(|| format!("vpce-{:08x}", rng.u32(..)));
        let additional_event_data_tls_version =
            additional_event_data_present.then(|| pick(&mut rng, &["TLSv1.3", "TLSv1.2"]));
        let tls_details_present = !matches!(service.kind, CloudTrailServiceKind::CloudTrail)
            && chance(&mut rng, profile.optional_field_density / 3);
        let tls_details_version =
            tls_details_present.then(|| pick(&mut rng, &["TLSv1.3", "TLSv1.2"]));
        let tls_details_cipher = tls_details_present.then(|| {
            pick(
                &mut rng,
                &["TLS_AES_128_GCM_SHA256", "ECDHE-RSA-AES128-GCM-SHA256"],
            )
        });
        let tls_details_host = tls_details_present.then(|| {
            pick(
                &mut rng,
                &[
                    "console.aws.amazon.com",
                    "api.aws.amazon.com",
                    "s3.amazonaws.com",
                ],
            )
        });
        let tls_details_client_principal =
            tls_details_present.then(|| pick(&mut rng, &["console", "cli", "sdk"]));
        let resource_arn = resources_present.then(|| {
            cloudtrail_resource_arn(
                service.kind,
                action,
                account_id,
                principal_name,
                role_name,
                session_name,
                region,
                i,
                0,
            )
        });
        let resource_name = (resources_present && chance(&mut rng, 35))
            .then(|| cloudtrail_resource_name(service.kind, action, principal_name, i, 0));
        let invoked_by = (matches!(
            service.kind,
            CloudTrailServiceKind::Sts | CloudTrailServiceKind::CloudTrail
        ) || matches!(identity_kind, CloudTrailIdentityKind::AwsService))
        .then_some("cloudtrail.amazonaws.com");
        let user_name = match identity_kind {
            CloudTrailIdentityKind::IamUser
            | CloudTrailIdentityKind::AssumedRole
            | CloudTrailIdentityKind::FederatedUser => Some(principal_name),
            CloudTrailIdentityKind::Root | CloudTrailIdentityKind::AwsService => None,
        };

        builders
            .event_version
            .append_value(CLOUDTRAIL_EVENT_VERSION);
        builders.event_time.append_value(&event_time);
        builders.event_source.append_value(service.event_source);
        builders.event_name.append_value(action.event_name);
        builders.aws_region.append_value(region);
        builders.source_ip_address.append_value(source_ip);
        builders.user_agent.append_value(&user_agent);
        builders.event_id.append_value(&event_id);
        builders.event_type.append_value(event_type);
        builders.recipient_account_id.append_value(account_id);
        builders.read_only.append_value(action.read_only);
        builders.management_event.append_value(!action.data_event);
        builders.event_category.append_value(event_category);
        builders
            .user_identity_type
            .append_value(cloudtrail_identity_type(identity_kind));
        builders
            .user_identity_principal_id
            .append_value(principal_id);
        builders.user_identity_account_id.append_value(account_id);
        append_optional_string(&mut builders.user_identity_arn, user_arn);
        append_optional_string(&mut builders.user_identity_user_name, user_name);
        append_optional_string(&mut builders.user_identity_invoked_by, invoked_by);
        builders
            .request_parameters_present
            .append_value(request_parameters_present);
        builders
            .response_elements_present
            .append_value(response_elements_present);
        append_optional_string(&mut builders.resource_arn, resource_arn.as_deref());
        if resources_present {
            builders.resource_type.append_value(action.resource_type);
        } else {
            builders.resource_type.append_null();
        }
        append_optional_string(&mut builders.resource_name, resource_name.as_deref());
        builders
            .additional_event_data_present
            .append_value(additional_event_data_present);
        append_optional_string(
            &mut builders.additional_event_data_tls_version,
            additional_event_data_tls_version,
        );
        append_optional_string(&mut builders.shared_event_id, shared_event_id);
        append_optional_string(&mut builders.vpc_endpoint_id, vpc_endpoint_id.as_deref());
        append_optional_string(&mut builders.error_code, error.map(|(code, _)| code));
        append_optional_string(
            &mut builders.error_message,
            error.map(|(_, message)| message),
        );
        append_optional_string(&mut builders.tls_details_version, tls_details_version);
        append_optional_string(&mut builders.tls_details_cipher, tls_details_cipher);
        append_optional_string(&mut builders.tls_details_host, tls_details_host);
        append_optional_string(
            &mut builders.tls_details_client_provided_principal,
            tls_details_client_principal,
        );
    }

    builders.finish(count)
}

fn append_optional_string(builder: &mut StringBuilder, value: Option<&str>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

struct CloudTrailBatchBuilders {
    event_version: StringBuilder,
    event_time: StringBuilder,
    event_source: StringBuilder,
    event_name: StringBuilder,
    aws_region: StringBuilder,
    source_ip_address: StringBuilder,
    user_agent: StringBuilder,
    event_id: StringBuilder,
    event_type: StringBuilder,
    recipient_account_id: StringBuilder,
    read_only: BooleanBuilder,
    management_event: BooleanBuilder,
    event_category: StringBuilder,
    user_identity_type: StringBuilder,
    user_identity_principal_id: StringBuilder,
    user_identity_account_id: StringBuilder,
    user_identity_arn: StringBuilder,
    user_identity_user_name: StringBuilder,
    user_identity_invoked_by: StringBuilder,
    request_parameters_present: BooleanBuilder,
    response_elements_present: BooleanBuilder,
    resource_arn: StringBuilder,
    resource_type: StringBuilder,
    resource_name: StringBuilder,
    additional_event_data_present: BooleanBuilder,
    additional_event_data_tls_version: StringBuilder,
    shared_event_id: StringBuilder,
    vpc_endpoint_id: StringBuilder,
    error_code: StringBuilder,
    error_message: StringBuilder,
    tls_details_version: StringBuilder,
    tls_details_cipher: StringBuilder,
    tls_details_host: StringBuilder,
    tls_details_client_provided_principal: StringBuilder,
}

impl CloudTrailBatchBuilders {
    fn new(rows: usize) -> Self {
        Self {
            event_version: StringBuilder::with_capacity(rows, rows.saturating_mul(8)),
            event_time: StringBuilder::with_capacity(rows, rows.saturating_mul(32)),
            event_source: StringBuilder::with_capacity(rows, rows.saturating_mul(24)),
            event_name: StringBuilder::with_capacity(rows, rows.saturating_mul(24)),
            aws_region: StringBuilder::with_capacity(rows, rows.saturating_mul(16)),
            source_ip_address: StringBuilder::with_capacity(rows, rows.saturating_mul(20)),
            user_agent: StringBuilder::with_capacity(rows, rows.saturating_mul(48)),
            event_id: StringBuilder::with_capacity(rows, rows.saturating_mul(36)),
            event_type: StringBuilder::with_capacity(rows, rows.saturating_mul(20)),
            recipient_account_id: StringBuilder::with_capacity(rows, rows.saturating_mul(12)),
            read_only: BooleanBuilder::with_capacity(rows),
            management_event: BooleanBuilder::with_capacity(rows),
            event_category: StringBuilder::with_capacity(rows, rows.saturating_mul(12)),
            user_identity_type: StringBuilder::with_capacity(rows, rows.saturating_mul(16)),
            user_identity_principal_id: StringBuilder::with_capacity(rows, rows.saturating_mul(24)),
            user_identity_account_id: StringBuilder::with_capacity(rows, rows.saturating_mul(12)),
            user_identity_arn: StringBuilder::with_capacity(rows, rows.saturating_mul(72)),
            user_identity_user_name: StringBuilder::with_capacity(rows, rows.saturating_mul(16)),
            user_identity_invoked_by: StringBuilder::with_capacity(rows, rows.saturating_mul(28)),
            request_parameters_present: BooleanBuilder::with_capacity(rows),
            response_elements_present: BooleanBuilder::with_capacity(rows),
            resource_arn: StringBuilder::with_capacity(rows, rows.saturating_mul(72)),
            resource_type: StringBuilder::with_capacity(rows, rows.saturating_mul(24)),
            resource_name: StringBuilder::with_capacity(rows, rows.saturating_mul(24)),
            additional_event_data_present: BooleanBuilder::with_capacity(rows),
            additional_event_data_tls_version: StringBuilder::with_capacity(
                rows,
                rows.saturating_mul(8),
            ),
            shared_event_id: StringBuilder::with_capacity(rows, rows.saturating_mul(36)),
            vpc_endpoint_id: StringBuilder::with_capacity(rows, rows.saturating_mul(16)),
            error_code: StringBuilder::with_capacity(rows, rows.saturating_mul(16)),
            error_message: StringBuilder::with_capacity(rows, rows.saturating_mul(64)),
            tls_details_version: StringBuilder::with_capacity(rows, rows.saturating_mul(8)),
            tls_details_cipher: StringBuilder::with_capacity(rows, rows.saturating_mul(32)),
            tls_details_host: StringBuilder::with_capacity(rows, rows.saturating_mul(32)),
            tls_details_client_provided_principal: StringBuilder::with_capacity(
                rows,
                rows.saturating_mul(8),
            ),
        }
    }

    fn finish(mut self, rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("eventVersion", DataType::Utf8, true),
            Field::new("eventTime", DataType::Utf8, true),
            Field::new("eventSource", DataType::Utf8, true),
            Field::new("eventName", DataType::Utf8, true),
            Field::new("awsRegion", DataType::Utf8, true),
            Field::new("sourceIPAddress", DataType::Utf8, true),
            Field::new("userAgent", DataType::Utf8, true),
            Field::new("eventID", DataType::Utf8, true),
            Field::new("eventType", DataType::Utf8, true),
            Field::new("recipientAccountId", DataType::Utf8, true),
            Field::new("readOnly", DataType::Boolean, true),
            Field::new("managementEvent", DataType::Boolean, true),
            Field::new("eventCategory", DataType::Utf8, true),
            Field::new("userIdentity.type", DataType::Utf8, true),
            Field::new("userIdentity.principalId", DataType::Utf8, true),
            Field::new("userIdentity.accountId", DataType::Utf8, true),
            Field::new("userIdentity.arn", DataType::Utf8, true),
            Field::new("userIdentity.userName", DataType::Utf8, true),
            Field::new("userIdentity.invokedBy", DataType::Utf8, true),
            Field::new("requestParameters.present", DataType::Boolean, true),
            Field::new("responseElements.present", DataType::Boolean, true),
            Field::new("resources.0.ARN", DataType::Utf8, true),
            Field::new("resources.0.type", DataType::Utf8, true),
            Field::new("resources.0.resourceName", DataType::Utf8, true),
            Field::new("additionalEventData.present", DataType::Boolean, true),
            Field::new("additionalEventData.tlsVersion", DataType::Utf8, true),
            Field::new("sharedEventID", DataType::Utf8, true),
            Field::new("vpcEndpointId", DataType::Utf8, true),
            Field::new("errorCode", DataType::Utf8, true),
            Field::new("errorMessage", DataType::Utf8, true),
            Field::new("tlsDetails.tlsVersion", DataType::Utf8, true),
            Field::new("tlsDetails.cipherSuite", DataType::Utf8, true),
            Field::new("tlsDetails.clientProvidedHostHeader", DataType::Utf8, true),
            Field::new("tlsDetails.clientProvidedPrincipal", DataType::Utf8, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.event_version.finish()) as ArrayRef,
            Arc::new(self.event_time.finish()) as ArrayRef,
            Arc::new(self.event_source.finish()) as ArrayRef,
            Arc::new(self.event_name.finish()) as ArrayRef,
            Arc::new(self.aws_region.finish()) as ArrayRef,
            Arc::new(self.source_ip_address.finish()) as ArrayRef,
            Arc::new(self.user_agent.finish()) as ArrayRef,
            Arc::new(self.event_id.finish()) as ArrayRef,
            Arc::new(self.event_type.finish()) as ArrayRef,
            Arc::new(self.recipient_account_id.finish()) as ArrayRef,
            Arc::new(self.read_only.finish()) as ArrayRef,
            Arc::new(self.management_event.finish()) as ArrayRef,
            Arc::new(self.event_category.finish()) as ArrayRef,
            Arc::new(self.user_identity_type.finish()) as ArrayRef,
            Arc::new(self.user_identity_principal_id.finish()) as ArrayRef,
            Arc::new(self.user_identity_account_id.finish()) as ArrayRef,
            Arc::new(self.user_identity_arn.finish()) as ArrayRef,
            Arc::new(self.user_identity_user_name.finish()) as ArrayRef,
            Arc::new(self.user_identity_invoked_by.finish()) as ArrayRef,
            Arc::new(self.request_parameters_present.finish()) as ArrayRef,
            Arc::new(self.response_elements_present.finish()) as ArrayRef,
            Arc::new(self.resource_arn.finish()) as ArrayRef,
            Arc::new(self.resource_type.finish()) as ArrayRef,
            Arc::new(self.resource_name.finish()) as ArrayRef,
            Arc::new(self.additional_event_data_present.finish()) as ArrayRef,
            Arc::new(self.additional_event_data_tls_version.finish()) as ArrayRef,
            Arc::new(self.shared_event_id.finish()) as ArrayRef,
            Arc::new(self.vpc_endpoint_id.finish()) as ArrayRef,
            Arc::new(self.error_code.finish()) as ArrayRef,
            Arc::new(self.error_message.finish()) as ArrayRef,
            Arc::new(self.tls_details_version.finish()) as ArrayRef,
            Arc::new(self.tls_details_cipher.finish()) as ArrayRef,
            Arc::new(self.tls_details_host.finish()) as ArrayRef,
            Arc::new(self.tls_details_client_provided_principal.finish()) as ArrayRef,
        ];
        RecordBatch::try_new(schema, arrays).unwrap_or_else(|err| {
            panic!("cloudtrail batch generation failed for {rows} rows: {err}")
        })
    }
}

#[derive(Debug, Clone)]
struct CloudTrailState {
    accounts: Vec<String>,
    principals: Vec<String>,
    roles: Vec<String>,
    sessions: Vec<String>,
    iam_user_principal_ids: Vec<String>,
    assumed_role_principal_ids: Vec<String>,
    federated_principal_ids: Vec<String>,
    root_arns: Vec<String>,
    iam_user_arns: Vec<String>,
    assumed_role_arns: Vec<String>,
    federated_arns: Vec<String>,
    source_ips: Vec<String>,
    shared_event_ids: Vec<String>,
}

impl CloudTrailState {
    fn new(profile: CloudTrailProfile) -> Self {
        let account_count = profile.account_count.max(1);
        let principal_count = profile.principal_count.max(1);

        let mut accounts = Vec::with_capacity(account_count);
        for idx in 0..account_count {
            accounts.push(format!("{:012}", 100_000_000_000u64 + (idx as u64 * 137)));
        }

        let mut principals = Vec::with_capacity(principal_count);
        let mut roles = Vec::with_capacity(principal_count);
        let mut sessions = Vec::with_capacity(principal_count);
        let mut iam_user_principal_ids = Vec::with_capacity(principal_count);
        let mut assumed_role_principal_ids = Vec::with_capacity(principal_count);
        let mut federated_principal_ids = Vec::with_capacity(principal_count);
        for idx in 0..principal_count {
            principals.push(format!("user-{idx:03}"));
            roles.push(format!("cloudtrail-role-{idx:03}"));
            sessions.push(format!("session-{idx:03}"));
            iam_user_principal_ids.push(format!("AIDA{:08X}", 0x1000_0000 + idx as u32));
            assumed_role_principal_ids.push(format!(
                "AROA{:08X}:{}",
                0x2000_0000 + idx as u32,
                idx % 1_000
            ));
            federated_principal_ids.push(format!(
                "AROAFED{:08X}:{}",
                0x3000_0000 + idx as u32,
                idx % 1_000
            ));
        }

        let mut root_arns = Vec::with_capacity(account_count);
        let mut iam_user_arns = Vec::with_capacity(account_count * principal_count);
        let mut assumed_role_arns = Vec::with_capacity(account_count * principal_count);
        let mut federated_arns = Vec::with_capacity(account_count * principal_count);
        for account_id in &accounts {
            root_arns.push(format!("arn:aws:iam::{account_id}:root"));
            for idx in 0..principal_count {
                let principal_name = &principals[idx];
                let role_name = &roles[idx];
                let session_name = &sessions[idx];
                iam_user_arns.push(format!("arn:aws:iam::{account_id}:user/{principal_name}"));
                assumed_role_arns.push(format!(
                    "arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}"
                ));
                federated_arns.push(format!(
                    "arn:aws:sts::{account_id}:federated-user/{principal_name}"
                ));
            }
        }

        let source_ip_count = principal_count.clamp(CLOUDTRAIL_PUBLIC_IPS.len(), 128);
        let mut source_ips = Vec::with_capacity(source_ip_count);
        for idx in 0..source_ip_count {
            source_ips.push(cloudtrail_source_ip_seeded(idx));
        }

        let shared_event_count = (principal_count * 2).clamp(16, 256);
        let mut shared_event_ids = Vec::with_capacity(shared_event_count);
        for idx in 0..shared_event_count {
            shared_event_ids.push(uuid_like_from_value(
                0xfeed_face_cafe_babe_0000_0000_0000_0000u128 | idx as u128,
            ));
        }

        Self {
            accounts,
            principals,
            roles,
            sessions,
            iam_user_principal_ids,
            assumed_role_principal_ids,
            federated_principal_ids,
            root_arns,
            iam_user_arns,
            assumed_role_arns,
            federated_arns,
            source_ips,
            shared_event_ids,
        }
    }

    fn account_id(&self, idx: usize) -> &str {
        &self.accounts[idx % self.accounts.len()]
    }

    fn principal_name(&self, idx: usize) -> &str {
        &self.principals[idx % self.principals.len()]
    }

    fn role_name(&self, idx: usize) -> &str {
        &self.roles[idx % self.roles.len()]
    }

    fn session_name(&self, idx: usize) -> &str {
        &self.sessions[idx % self.sessions.len()]
    }

    fn principal_id(
        &self,
        identity_kind: CloudTrailIdentityKind,
        account_idx: usize,
        principal_idx: usize,
    ) -> &str {
        match identity_kind {
            CloudTrailIdentityKind::Root => &self.accounts[account_idx % self.accounts.len()],
            CloudTrailIdentityKind::IamUser => {
                &self.iam_user_principal_ids[principal_idx % self.iam_user_principal_ids.len()]
            }
            CloudTrailIdentityKind::AssumedRole => {
                &self.assumed_role_principal_ids
                    [principal_idx % self.assumed_role_principal_ids.len()]
            }
            CloudTrailIdentityKind::AwsService => "cloudtrail.amazonaws.com",
            CloudTrailIdentityKind::FederatedUser => {
                &self.federated_principal_ids[principal_idx % self.federated_principal_ids.len()]
            }
        }
    }

    fn arn(
        &self,
        identity_kind: CloudTrailIdentityKind,
        account_idx: usize,
        principal_idx: usize,
    ) -> Option<&str> {
        let account_idx = account_idx % self.accounts.len();
        let principal_idx = principal_idx % self.principals.len();
        let flat_idx = account_idx * self.principals.len() + principal_idx;
        match identity_kind {
            CloudTrailIdentityKind::Root => Some(&self.root_arns[account_idx]),
            CloudTrailIdentityKind::IamUser => Some(&self.iam_user_arns[flat_idx]),
            CloudTrailIdentityKind::AssumedRole => Some(&self.assumed_role_arns[flat_idx]),
            CloudTrailIdentityKind::AwsService => None,
            CloudTrailIdentityKind::FederatedUser => Some(&self.federated_arns[flat_idx]),
        }
    }

    fn source_ip(&self, idx: usize) -> &str {
        &self.source_ips[idx % self.source_ips.len()]
    }

    fn shared_event_id(&self, idx: usize) -> &str {
        &self.shared_event_ids[idx % self.shared_event_ids.len()]
    }
}
