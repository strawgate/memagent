//! Pluggable trace validators encoding TLA+ safety properties.
//!
//! Each validator implements [`EventValidator`] and checks one or more
//! TLA+ invariants/temporal properties against a [`NormalizedTrace`].

pub mod delivery_retry;
pub mod pipeline_machine;
pub mod shutdown;
pub mod worker_pool;

use crate::trace_bridge::{EventValidator, NormalizedTrace};

/// Run all registered validators against a trace.
/// Returns the first failure or `Ok(())` if all pass.
pub fn run_all(trace: &NormalizedTrace) -> Result<(), String> {
    for v in all_validators() {
        v.validate(trace)?;
    }
    Ok(())
}

/// Collect every validator across all TLA+ specs.
pub fn all_validators() -> Vec<Box<dyn EventValidator>> {
    let mut vs: Vec<Box<dyn EventValidator>> = Vec::new();
    vs.extend(pipeline_machine::validators());
    vs.extend(worker_pool::validators());
    vs.extend(shutdown::validators());
    vs.extend(delivery_retry::validators());
    vs
}
