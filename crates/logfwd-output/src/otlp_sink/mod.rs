//! OTLP protobuf sink: encodes Arrow RecordBatches as OTLP LogRecords
//! and ships them to collectors over gRPC or HTTP.

mod columns;
mod encode;
mod factory;
mod send;
mod types;

// Re-export public items so external callers see them at `otlp_sink::*`.
pub use factory::OtlpSinkFactory;
pub use types::OtlpSink;

use crate::Compression;

#[cfg(test)]
mod otlp_sink_correctness_tests;

#[cfg(test)]
mod tests;
