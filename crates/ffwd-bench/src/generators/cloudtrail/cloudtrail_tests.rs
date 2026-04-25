use std::collections::HashSet;

use arrow::array::{Array, BooleanArray, StringArray};
use serde_json::Value;

use super::*;
use crate::generators::test_support::scan_json;

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
        CloudTrailProfile::benchmark_default().with_service_mix(CloudTrailServiceMix::StorageHeavy),
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

#[test]
fn cloudtrail_batch_matches_scanned_top_level_schema() {
    let profile = CloudTrailProfile::benchmark_default();
    let scanned = scan_json(gen_cloudtrail_audit_with_profile(256, 42, profile));
    let direct = gen_cloudtrail_batch_with_profile(256, 42, profile);
    assert_eq!(scanned.num_rows(), direct.num_rows());

    let expected_shared_columns = [
        "eventVersion",
        "eventTime",
        "eventSource",
        "eventName",
        "awsRegion",
        "sourceIPAddress",
        "userAgent",
        "eventID",
        "eventType",
        "recipientAccountId",
        "readOnly",
        "managementEvent",
        "eventCategory",
        "sharedEventID",
        "vpcEndpointId",
        "errorCode",
        "errorMessage",
    ];

    for column_name in expected_shared_columns {
        let scanned_col = scanned
            .column_by_name(column_name)
            .unwrap_or_else(|| panic!("missing scanned column: {column_name}"));
        let direct_col = direct
            .column_by_name(column_name)
            .unwrap_or_else(|| panic!("missing direct column: {column_name}"));
        assert_eq!(
            scanned_col.data_type(),
            direct_col.data_type(),
            "top-level data type mismatch for {column_name}"
        );
    }
}
