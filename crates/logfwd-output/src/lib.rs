//! Output sink trait and implementations for serializing Arrow RecordBatches
//! to various formats: stdout JSON/text, JSON lines over HTTP, OTLP protobuf.

mod arrow_ipc_sink;
mod file_sink;
mod json_lines;
mod null;
mod otap_sink;
mod otlp_sink;
pub mod sink;
mod stdout;
mod tcp_sink;
mod udp_sink;

pub mod error;

pub(crate) mod http_classify;

mod conflict_columns;
mod elasticsearch;
mod factory;
mod metadata;
mod row_json;

mod loki;

pub use arrow_ipc_sink::{ArrowIpcSinkFactory, deserialize_ipc, serialize_ipc};
pub use elasticsearch::{ElasticsearchRequestMode, ElasticsearchSink, ElasticsearchSinkFactory};
pub use error::OutputError;
pub use factory::build_sink_factory;
pub use file_sink::{FileSink, FileSinkFactory};
pub use json_lines::{JsonLinesSink, JsonLinesSinkFactory};
pub use loki::{LokiSink, LokiSinkFactory};
pub use metadata::{BatchMetadata, Compression};
pub use null::{NullSink, NullSinkFactory};
pub use otap_sink::{
    ArrowPayloadType, BatchStatus, DecodedPayload, OtapSinkFactory, StatusCode,
    decode_batch_arrow_records, decode_batch_arrow_records_generated_fast, decode_batch_status,
    decode_batch_status_generated_fast, encode_batch_arrow_records,
    encode_batch_arrow_records_generated_fast,
};
pub use otlp_sink::{OtlpProtocol, OtlpSink, OtlpSinkFactory};
pub use sink::{
    AsyncFanoutFactory, AsyncFanoutSink, OnceAsyncFactory, SendResult, Sink, SinkFactory,
};
pub use stdout::StdoutSinkFactory;
#[cfg(test)]
use stdout::*;
pub use tcp_sink::{TcpSink, TcpSinkFactory};
pub use udp_sink::{UdpSink, UdpSinkFactory};

pub use conflict_columns::{ColInfo, ColVariant, build_col_infos};
pub(crate) use conflict_columns::{get_array, is_null};
#[cfg(any(test, kani))]
#[allow(unused_imports)]
pub(crate) use conflict_columns::{is_conflict_struct, json_priority, str_priority, variant_dt};
pub(crate) use metadata::build_auth_headers;
pub use row_json::write_row_json;
pub(crate) use row_json::{coalesce_as_str, str_value};

// ---------------------------------------------------------------------------
// HTTP retry helper
// ---------------------------------------------------------------------------

/// Returns `true` if the ureq error is transient and worth retrying.
///
/// Transient errors are: HTTP 429 Too Many Requests, 5xx server errors, and
/// network/transport failures (I/O, host not found, connection failed, timeout).
#[cfg(test)]
pub(crate) fn is_transient_error(e: &ureq::Error) -> bool {
    match e {
        ureq::Error::StatusCode(status) => *status == 429 || *status >= 500,
        ureq::Error::Io(_)
        | ureq::Error::HostNotFound
        | ureq::Error::ConnectionFailed
        | ureq::Error::Timeout(_) => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;

#[cfg(kani)]
mod kani_proofs {
    use arrow::datatypes::DataType;

    use super::{json_priority, str_priority};

    #[kani::proof]
    fn json_priority_orders_numeric_and_bool_before_text() {
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Float64));
        assert!(json_priority(&DataType::Float64) > json_priority(&DataType::Boolean));
        assert!(json_priority(&DataType::Boolean) > json_priority(&DataType::Utf8));
    }

    #[kani::proof]
    fn str_priority_prefers_text_over_numeric() {
        assert!(str_priority(&DataType::Utf8) > str_priority(&DataType::Boolean));
        assert!(str_priority(&DataType::Boolean) > str_priority(&DataType::Int64));
        assert!(str_priority(&DataType::Int64) > str_priority(&DataType::Float64));
    }
}
