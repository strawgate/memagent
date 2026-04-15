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

include!("engine.rs");
include!("field_builders.rs");
include!("resources_tls.rs");

#[cfg(test)]
mod cloudtrail_tests;
