//! CloudTrail-like synthetic audit log generator.
//!
//! The goal is to mimic the nested, partially sparse, high-cardinality shape
//! of real AWS CloudTrail records closely enough that compression, hashing,
//! and downstream parsing behave realistically in benchmarks.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::{
    CLOUDTRAIL_EVENT_VERSION, CLOUDTRAIL_PUBLIC_IPS, CLOUDTRAIL_REGIONS_GLOBAL,
    CLOUDTRAIL_REGIONS_MULTI, CLOUDTRAIL_REGIONS_REGIONAL, CLOUDTRAIL_USER_AGENTS,
    CloudTrailActionSpec, CloudTrailIdentityKind, CloudTrailProfile, CloudTrailRegionMix,
    CloudTrailServiceKind, CloudTrailServiceMix, CloudTrailServiceSpec, SERVICE_BALANCED,
    SERVICE_COMPUTE_HEAVY, SERVICE_SECURITY_HEAVY, SERVICE_STORAGE_HEAVY, json_escape, pick,
    uuid_like_from_value, weighted_choice_pick, weighted_pick,
};

/// Generate CloudTrail-like audit logs using the benchmark-default profile.
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
        let shared_event_id =
            if action.data_event || chance(&mut rng, profile.optional_field_density / 3) {
                Some(state.shared_event_id(shared_event_slot(
                    i,
                    account_slot,
                    principal_slot,
                    state.shared_event_ids.len(),
                    profile,
                )))
            } else {
                None
            };
        let source_ip = cloudtrail_source_ip(
            &mut rng,
            &state,
            account_slot,
            principal_slot,
            service.kind,
            profile.optional_field_density,
        );
        let user_agent = cloudtrail_user_agent(&mut rng, service.kind, principal_slot);
        let user_identity = build_user_identity_json(
            &mut rng,
            identity_kind,
            account_id,
            principal_name,
            role_name,
            session_name,
            principal_id,
            user_arn,
            profile.optional_field_density,
            service.kind,
        );
        let request_parameters = build_request_parameters_json(
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
        let response_elements = build_response_elements_json(
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
        let vpc_endpoint_id = if chance(&mut rng, profile.optional_field_density / 4)
            && !matches!(service.kind, CloudTrailServiceKind::CloudTrail)
        {
            Some(format!("vpce-{:08x}", rng.u32(..)))
        } else {
            None
        };
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
        let shared_event_id =
            if action.data_event || chance(&mut rng, profile.optional_field_density / 3) {
                Some(state.shared_event_id(shared_event_slot(
                    i,
                    account_slot,
                    principal_slot,
                    state.shared_event_ids.len(),
                    profile,
                )))
            } else {
                None
            };
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
        let vpc_endpoint_id = if chance(&mut rng, profile.optional_field_density / 4)
            && !matches!(service.kind, CloudTrailServiceKind::CloudTrail)
        {
            Some(format!("vpce-{:08x}", rng.u32(..)))
        } else {
            None
        };
        let additional_event_data_tls_version = if additional_event_data_present {
            Some(pick(&mut rng, &["TLSv1.3", "TLSv1.2"]))
        } else {
            None
        };
        let tls_details_present = !matches!(service.kind, CloudTrailServiceKind::CloudTrail)
            && chance(&mut rng, profile.optional_field_density / 3);
        let tls_details_version = if tls_details_present {
            Some(pick(&mut rng, &["TLSv1.3", "TLSv1.2"]))
        } else {
            None
        };
        let tls_details_cipher = if tls_details_present {
            Some(pick(
                &mut rng,
                &["TLS_AES_128_GCM_SHA256", "ECDHE-RSA-AES128-GCM-SHA256"],
            ))
        } else {
            None
        };
        let tls_details_host = if tls_details_present {
            Some(pick(
                &mut rng,
                &[
                    "console.aws.amazon.com",
                    "api.aws.amazon.com",
                    "s3.amazonaws.com",
                ],
            ))
        } else {
            None
        };
        let tls_details_client_principal = if tls_details_present {
            Some(pick(&mut rng, &["console", "cli", "sdk"]))
        } else {
            None
        };
        let resource_arn = if resources_present {
            Some(cloudtrail_resource_arn(
                service.kind,
                action,
                account_id,
                principal_name,
                role_name,
                session_name,
                region,
                i,
                0,
            ))
        } else {
            None
        };
        let resource_name = if resources_present && chance(&mut rng, 35) {
            Some(cloudtrail_resource_name(
                service.kind,
                action,
                principal_name,
                i,
                0,
            ))
        } else {
            None
        };
        let invoked_by = if matches!(
            service.kind,
            CloudTrailServiceKind::Sts | CloudTrailServiceKind::CloudTrail
        ) || matches!(identity_kind, CloudTrailIdentityKind::AwsService)
        {
            Some("cloudtrail.amazonaws.com")
        } else {
            None
        };
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
            principals.push(format!("user-{:03}", idx));
            roles.push(format!("cloudtrail-role-{:03}", idx));
            sessions.push(format!("session-{:03}", idx));
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
            if principal_slot % 4 == 0 {
                "console.amazonaws.com".to_string()
            } else {
                pick(rng, CLOUDTRAIL_USER_AGENTS).to_string()
            }
        }
        CloudTrailServiceKind::S3 | CloudTrailServiceKind::Kms => {
            pick(rng, CLOUDTRAIL_USER_AGENTS).to_string()
        }
        CloudTrailServiceKind::Ec2 | CloudTrailServiceKind::Lambda | CloudTrailServiceKind::Rds => {
            if principal_slot % 3 == 0 {
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

#[allow(clippy::too_many_arguments)]
fn build_user_identity_json(
    rng: &mut fastrand::Rng,
    identity_kind: CloudTrailIdentityKind,
    account_id: &str,
    principal_name: &str,
    role_name: &str,
    session_name: &str,
    principal_id: &str,
    arn: Option<&str>,
    optional_density: u8,
    service_kind: CloudTrailServiceKind,
) -> String {
    let mut out = JsonObjectWriter::new(384);
    out.field_str("type", cloudtrail_identity_type(identity_kind));
    out.field_str("principalId", principal_id);
    out.field_str("accountId", account_id);
    if let Some(arn) = arn {
        out.field_str("arn", arn);
    } else if chance(rng, optional_density / 2) {
        out.field_str("invokedBy", "cloudtrail.amazonaws.com");
    }

    match identity_kind {
        CloudTrailIdentityKind::Root => {}
        CloudTrailIdentityKind::IamUser => {
            out.field_str("userName", principal_name);
            if chance(rng, optional_density) {
                out.field_str("accessKeyId", &format!("AKIA{:08X}", rng.u32(..)));
            }
        }
        CloudTrailIdentityKind::AssumedRole | CloudTrailIdentityKind::FederatedUser => {
            out.field_str("userName", principal_name);
            if chance(rng, optional_density) {
                out.field_raw(
                    "sessionContext",
                    &build_session_context_json(
                        rng,
                        account_id,
                        principal_name,
                        role_name,
                        session_name,
                        principal_id,
                        arn,
                        identity_kind,
                        optional_density,
                    ),
                );
            }
        }
        CloudTrailIdentityKind::AwsService => {}
    }

    if matches!(
        service_kind,
        CloudTrailServiceKind::Lambda | CloudTrailServiceKind::Kms
    ) && chance(rng, optional_density / 2)
    {
        out.field_str("sourceIdentity", principal_name);
    }

    if matches!(
        service_kind,
        CloudTrailServiceKind::Sts | CloudTrailServiceKind::CloudTrail
    ) || matches!(identity_kind, CloudTrailIdentityKind::AwsService)
    {
        out.field_str("invokedBy", "cloudtrail.amazonaws.com");
    }

    out.finish()
}

#[allow(clippy::too_many_arguments)]
fn build_session_context_json(
    rng: &mut fastrand::Rng,
    account_id: &str,
    _principal_name: &str,
    role_name: &str,
    _session_name: &str,
    principal_id: &str,
    arn: Option<&str>,
    identity_kind: CloudTrailIdentityKind,
    optional_density: u8,
) -> String {
    let mut out = JsonObjectWriter::new(320);
    out.field_raw(
        "sessionIssuer",
        &build_session_issuer_json(identity_kind, account_id, role_name, principal_id, arn),
    );
    let mut attr = JsonObjectWriter::new(160);
    attr.field_str("creationDate", &cloudtrail_event_time(rng.usize(..60), rng));
    attr.field_str(
        "mfaAuthenticated",
        if chance(rng, optional_density) {
            "true"
        } else {
            "false"
        },
    );
    if chance(rng, optional_density / 2) {
        attr.field_str("sessionCredentialFromConsole", "true");
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

#[allow(clippy::too_many_arguments)]
fn build_request_parameters_json(
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
    if action.read_only && !chance(rng, optional_density / 2) {
        return None;
    }

    let mut out = JsonObjectWriter::new(384);
    match service_kind {
        CloudTrailServiceKind::Ec2 => {
            if action.event_name == "RunInstances" {
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
                out.field_str("keyName", &format!("cloudtrail-key-{event_index:04}"));
            } else {
                out.field_raw("instanceIds", &format!(r#"["i-{:08x}"]"#, rng.u32(..)));
                out.field_bool("dryRun", chance(rng, optional_density / 2));
            }
        }
        CloudTrailServiceKind::S3 => {
            out.field_str(
                "bucketName",
                &format!("audit-{account_id}-{principal_name}"),
            );
            out.field_str(
                "key",
                &format!(
                    "cloudtrail/{region}/{event_index:04}/{}.json",
                    action.event_name.to_lowercase()
                ),
            );
            if action.data_event {
                out.field_str(
                    "x-amz-server-side-encryption",
                    pick(rng, &["AES256", "aws:kms"]),
                );
            }
        }
        CloudTrailServiceKind::Iam => {
            out.field_str("userName", principal_name);
            if action.event_name.contains("Role") {
                out.field_str("roleName", role_name);
            }
            out.field_str("path", "/service/");
        }
        CloudTrailServiceKind::Sts => {
            out.field_str(
                "roleArn",
                &format!("arn:aws:iam::{account_id}:role/{role_name}"),
            );
            out.field_str("roleSessionName", session_name);
            out.field_raw("durationSeconds", "3600");
        }
        CloudTrailServiceKind::Kms => {
            out.field_str(
                "keyId",
                &format!("arn:aws:kms:{region}:{account_id}:key/{:08x}", rng.u32(..)),
            );
            out.field_raw(
                "encryptionContext",
                &format!(
                    r#"{{"env":"{}","service":"{}"}}"#,
                    pick(rng, &["prod", "stage", "dev"]),
                    action.resource_type
                ),
            );
        }
        CloudTrailServiceKind::Lambda => {
            out.field_str(
                "functionName",
                &format!("cloudtrail-{principal_name}-{}", event_index % 16),
            );
            if action.event_name == "Invoke" {
                out.field_str("qualifier", "$LATEST");
            } else {
                out.field_bool("publish", chance(rng, optional_density));
            }
        }
        CloudTrailServiceKind::Rds => {
            out.field_str(
                "dBInstanceIdentifier",
                &format!("audit-db-{}", event_index % 128),
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
            out.field_str("trailName", &format!("org-trail-{account_id}"));
            if action.event_name == "LookupEvents" {
                out.field_raw("maxResults", "50");
            }
        }
    }

    if chance(rng, optional_density / 3) {
        out.field_str("clientRequestToken", &cloudtrail_uuid_like(rng));
    }

    Some(out.finish())
}

#[allow(clippy::too_many_arguments)]
fn build_response_elements_json(
    rng: &mut fastrand::Rng,
    service_kind: CloudTrailServiceKind,
    action: &CloudTrailActionSpec,
    account_id: &str,
    principal_name: &str,
    _role_name: &str,
    _session_name: &str,
    region: &str,
    event_index: usize,
    optional_density: u8,
) -> Option<String> {
    if action.read_only && !chance(rng, optional_density / 2) {
        return None;
    }
    let mut out = JsonObjectWriter::new(256);
    match service_kind {
        CloudTrailServiceKind::Ec2 => {
            if action.event_name == "RunInstances" {
                out.field_str("reservationId", &format!("r-{:08x}", rng.u32(..)));
                out.field_raw(
                    "instancesSet",
                    &format!(
                        r#"{{"items":[{{"instanceId":"i-{:08x}","currentState":{{"code":16,"name":"running"}}}}]}}"#,
                        rng.u32(..)
                    ),
                );
            } else {
                out.field_bool("return", chance(rng, optional_density));
            }
        }
        CloudTrailServiceKind::S3 => {
            if action.data_event {
                out.field_str(
                    "x-amz-version-id",
                    &format!("{}-{:08x}", event_index, rng.u32(..)),
                );
                out.field_str("etag", &format!("\"{:08x}\"", rng.u32(..)));
            } else {
                out.field_str("bucketRegion", region);
            }
        }
        CloudTrailServiceKind::Iam => {
            out.field_raw(
                "user",
                &format!(
                    r#"{{"userName":"{}","arn":"arn:aws:iam::{}:user/{}"}}"#,
                    principal_name, account_id, principal_name
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
                &format!("arn:aws:kms:{region}:{account_id}:key/{:08x}", rng.u32(..)),
            );
            out.field_str(
                "encryptionAlgorithm",
                pick(rng, &["SYMMETRIC_DEFAULT", "RSAES_OAEP_SHA_256"]),
            );
        }
        CloudTrailServiceKind::Lambda => {
            out.field_str(
                "functionName",
                &format!("cloudtrail-{principal_name}-{}", event_index % 16),
            );
            out.field_str("responsePayload", "{\"status\":\"ok\"}");
        }
        CloudTrailServiceKind::Rds => {
            out.field_str(
                "dBInstanceIdentifier",
                &format!("audit-db-{}", event_index % 128),
            );
            out.field_str(
                "dBInstanceStatus",
                pick(rng, &["available", "modifying", "backing-up"]),
            );
        }
        CloudTrailServiceKind::CloudTrail => {
            out.field_str("trailName", &format!("org-trail-{account_id}"));
            out.field_bool("isLogging", chance(rng, optional_density));
        }
    }
    Some(out.finish())
}

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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::array::{Array, BooleanArray, StringArray};
    use serde_json::Value;

    use super::*;

    #[test]
    fn cloudtrail_deterministic() {
        let a = gen_cloudtrail_audit(100, 42);
        let b = gen_cloudtrail_audit(100, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn cloudtrail_valid_json() {
        let data = gen_cloudtrail_audit(120, 1);
        let text = std::str::from_utf8(&data).expect("valid utf-8");
        for line in text.lines() {
            let _: Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}: {line}"));
        }
    }

    #[test]
    fn cloudtrail_has_nested_optional_fields() {
        let data = gen_cloudtrail_audit_with_profile(
            400,
            7,
            CloudTrailProfile::benchmark_default()
                .with_service_mix(CloudTrailServiceMix::SecurityHeavy)
                .with_region_mix(CloudTrailRegionMix::MultiRegion)
                .with_optional_field_density(85),
        );
        let text = std::str::from_utf8(&data).expect("valid utf-8");
        let mut user_identity_types = HashSet::new();
        let mut saw_session_context = false;
        let mut saw_resources = false;
        let mut saw_error = false;
        let mut saw_additional = false;
        let mut saw_shared = false;
        for line in text.lines() {
            let value: Value = serde_json::from_str(line).expect("valid json");
            if let Some(user_identity) = value.get("userIdentity") {
                if let Some(kind) = user_identity.get("type").and_then(Value::as_str) {
                    user_identity_types.insert(kind.to_string());
                }
                if user_identity.get("sessionContext").is_some() {
                    saw_session_context = true;
                }
            }
            if value.get("resources").is_some() {
                saw_resources = true;
            }
            if value.get("errorCode").is_some() {
                saw_error = true;
            }
            if value.get("additionalEventData").is_some() {
                saw_additional = true;
            }
            if value.get("sharedEventID").is_some() {
                saw_shared = true;
            }
        }

        assert!(user_identity_types.contains("AssumedRole"));
        assert!(user_identity_types.contains("IAMUser"));
        assert!(user_identity_types.contains("AWSService"));
        assert!(saw_session_context, "expected at least one sessionContext");
        assert!(saw_resources, "expected at least one resources array");
        assert!(saw_error, "expected at least one errorCode");
        assert!(saw_additional, "expected at least one additionalEventData");
        assert!(saw_shared, "expected at least one sharedEventID");
    }

    #[test]
    fn cloudtrail_profile_changes_mix() {
        let security = gen_cloudtrail_audit_with_profile(
            120,
            8,
            CloudTrailProfile::benchmark_default()
                .with_service_mix(CloudTrailServiceMix::SecurityHeavy),
        );
        let storage = gen_cloudtrail_audit_with_profile(
            120,
            8,
            CloudTrailProfile::benchmark_default()
                .with_service_mix(CloudTrailServiceMix::StorageHeavy),
        );
        assert_ne!(
            security, storage,
            "changing service mix should change output"
        );
    }

    #[test]
    fn cloudtrail_batch_deterministic() {
        let a = gen_cloudtrail_batch_with_profile(
            64,
            42,
            CloudTrailProfile::benchmark_default().with_service_mix(CloudTrailServiceMix::Balanced),
        );
        let b = gen_cloudtrail_batch_with_profile(
            64,
            42,
            CloudTrailProfile::benchmark_default().with_service_mix(CloudTrailServiceMix::Balanced),
        );

        assert_eq!(a.schema(), b.schema());
        assert_eq!(a.num_rows(), b.num_rows());
        assert_eq!(a.num_columns(), b.num_columns());

        for idx in 0..a.num_columns() {
            let left = a.column(idx);
            let right = b.column(idx);
            assert_eq!(left.len(), right.len());
            match left.data_type() {
                DataType::Utf8 => {
                    let left = left
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("utf8 column");
                    let right = right
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("utf8 column");
                    for row in 0..left.len() {
                        assert_eq!(left.is_null(row), right.is_null(row));
                        if !left.is_null(row) {
                            assert_eq!(left.value(row), right.value(row));
                        }
                    }
                }
                DataType::Boolean => {
                    let left = left
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("bool column");
                    let right = right
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("bool column");
                    for row in 0..left.len() {
                        assert_eq!(left.value(row), right.value(row));
                    }
                }
                other => panic!("unexpected cloudtrail batch column type: {other:?}"),
            }
        }
    }

    #[test]
    fn cloudtrail_batch_has_expected_shape() {
        let batch = gen_cloudtrail_batch_with_profile(
            128,
            9,
            CloudTrailProfile::benchmark_default()
                .with_optional_field_density(85)
                .with_region_mix(CloudTrailRegionMix::MultiRegion),
        );

        assert_eq!(batch.num_rows(), 128);
        assert_eq!(batch.num_columns(), 34);
        assert!(batch.schema().field_with_name("userIdentity.type").is_ok());
        assert!(
            batch
                .schema()
                .field_with_name("tlsDetails.tlsVersion")
                .is_ok()
        );

        let identity_type = batch
            .column_by_name("userIdentity.type")
            .expect("userIdentity column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        let request_parameters_present = batch
            .column_by_name("requestParameters.present")
            .expect("requestParameters.present column")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool");

        assert!(identity_type.iter().flatten().next().is_some());
        assert!(request_parameters_present.values().iter().any(|v| v));
    }
}
