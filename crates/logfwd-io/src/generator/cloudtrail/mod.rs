//! CloudTrail-like synthetic audit log generator.
//!
//! The goal is to mimic the nested, partially sparse, high-cardinality shape
//! of real AWS CloudTrail records closely enough that compression, hashing,
//! and downstream parsing behave realistically in benchmarks.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::shared::{
    CLOUDTRAIL_EVENT_VERSION, CLOUDTRAIL_PUBLIC_IPS, CLOUDTRAIL_REGIONS_GLOBAL,
    CLOUDTRAIL_REGIONS_MULTI, CLOUDTRAIL_REGIONS_REGIONAL, CLOUDTRAIL_USER_AGENTS,
    CloudTrailActionSpec, CloudTrailIdentityKind, CloudTrailProfile, CloudTrailRegionMix,
    CloudTrailServiceKind, CloudTrailServiceMix, CloudTrailServiceSpec, SERVICE_BALANCED,
    SERVICE_COMPUTE_HEAVY, SERVICE_SECURITY_HEAVY, SERVICE_STORAGE_HEAVY, json_escape, pick,
    uuid_like_from_value, weighted_choice_pick, weighted_pick,
};

include!("engine.rs");
include!("field_builders.rs");
include!("resources.rs");

// ---------------------------------------------------------------------------
// Streaming support for GeneratorInput
// ---------------------------------------------------------------------------

/// Write a single CloudTrail event line into `buf`.
///
/// `event_index` is the monotonic event counter (equivalent to loop `i`) used
/// for tenure-based account and principal slot selection.
pub(crate) fn write_cloudtrail_line(
    buf: &mut Vec<u8>,
    rng: &mut fastrand::Rng,
    profile: &CloudTrailProfile,
    state: &CloudTrailState,
    event_index: usize,
) {
    let i = event_index;
    let services = cloudtrail_services(profile.service_mix);
    let service = weighted_choice_pick(rng, services);
    let action = pick_cloudtrail_action(rng, service.actions);
    let account_slot = pick_tenure_slot(rng, i, profile.account_tenure, state.accounts.len());
    let principal_slot = pick_tenure_slot(
        rng,
        i + account_slot,
        profile.principal_tenure,
        state.principals.len(),
    );
    let account_id = state.account_id(account_slot);
    let principal_name = state.principal_name(principal_slot);
    let role_name = state.role_name(principal_slot);
    let session_name = state.session_name(principal_slot);
    let region = pick_cloudtrail_region(rng, profile.region_mix, service.kind);
    let event_time = cloudtrail_event_time(i, rng);
    let event_id = cloudtrail_uuid_like(rng);
    let identity_kind = pick_cloudtrail_identity_kind(rng, service.kind, action.read_only);
    let principal_id = state.principal_id(identity_kind, account_slot, principal_slot);
    let user_arn = state.arn(identity_kind, account_slot, principal_slot);
    let shared_event_id = if action.data_event || chance(rng, profile.optional_field_density / 3) {
        Some(state.shared_event_id(shared_event_slot(
            i,
            account_slot,
            principal_slot,
            state.shared_event_ids.len(),
            *profile,
        )))
    } else {
        None
    };
    let source_ip = cloudtrail_source_ip(
        rng,
        state,
        account_slot,
        principal_slot,
        service.kind,
        profile.optional_field_density,
    );
    let user_agent = cloudtrail_user_agent(rng, service.kind, principal_slot);
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
    let user_identity = build_user_identity_json(rng, identity_kind, &identity_inputs);
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
    let request_parameters = build_request_parameters_json(rng, &event_shape);
    let response_elements = build_response_elements_json(rng, &event_shape);
    let resources = build_resources_json(
        rng,
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
        rng,
        service.kind,
        action,
        region,
        profile.optional_field_density,
    );
    let error = cloudtrail_error_pair(rng, action.read_only, action.data_event);
    let event_type = cloudtrail_event_type(service.kind, identity_kind, action);
    let event_category = if action.data_event {
        "Data"
    } else {
        "Management"
    };
    let vpc_endpoint_id = if chance(rng, profile.optional_field_density / 4)
        && !matches!(service.kind, CloudTrailServiceKind::CloudTrail)
    {
        Some(format!("vpce-{:08x}", rng.u32(..)))
    } else {
        None
    };
    let tls_details = build_tls_details_json(rng, service.kind, profile.optional_field_density);

    let mut event = JsonObjectWriter::new(1_536);
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

    buf.extend_from_slice(event.finish().as_bytes());
}

#[cfg(test)]
mod cloudtrail_tests;
