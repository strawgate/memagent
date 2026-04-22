//! Elasticsearch sink: serializes Arrow RecordBatches as NDJSON bulk payloads
//! and ships them to Elasticsearch clusters via the `_bulk` API.

mod factory;
mod response;
mod send;
mod serialize;
mod sink_impl;
pub(crate) mod timestamp;
mod transport;
mod types;

// Re-export public items so external callers see them at `elasticsearch::*`.
pub use factory::ElasticsearchSinkFactory;
pub use types::{ElasticsearchRequestMode, ElasticsearchSink};

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod kani_proofs;
