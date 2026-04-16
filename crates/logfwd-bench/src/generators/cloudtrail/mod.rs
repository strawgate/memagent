//! CloudTrail-like synthetic audit log generator.
//!
//! Re-exported from `logfwd-io`. See that crate for implementation details.

pub use logfwd_io::generator::cloudtrail::{
    gen_cloudtrail_audit, gen_cloudtrail_audit_with_profile, gen_cloudtrail_batch,
    gen_cloudtrail_batch_with_profile,
};
