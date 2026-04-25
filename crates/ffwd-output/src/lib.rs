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
mod internal_columns;
mod metadata;
mod row_json;

mod loki;

pub use arrow_ipc_sink::{ArrowIpcSinkFactory, deserialize_ipc, serialize_ipc};
pub use elasticsearch::{ElasticsearchRequestMode, ElasticsearchSink, ElasticsearchSinkFactory};
pub use error::OutputError;
pub use factory::build_sink_factory;
pub use ffwd_config::OtlpProtocol;
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
pub use otlp_sink::{OtlpSink, OtlpSinkFactory};
pub use sink::{
    AsyncFanoutFactory, AsyncFanoutSink, OnceAsyncFactory, SendResult, Sink, SinkFactory,
};
pub use stdout::{StdoutFormat, StdoutSink, StdoutSinkFactory};
pub use tcp_sink::{TcpSink, TcpSinkFactory};
pub use udp_sink::{UdpSink, UdpSinkFactory};

pub use conflict_columns::{
    ColInfo, ColVariant, ResolvedCol, TypedArrayRef, build_col_infos, resolve_col_infos,
};
pub(crate) use conflict_columns::{get_array, is_null};
#[cfg(any(test, kani))]
#[allow(unused_imports)]
pub(crate) use conflict_columns::{is_conflict_struct, json_priority, str_priority, variant_dt};
pub(crate) use metadata::build_auth_headers;
pub(crate) use row_json::{coalesce_as_str, write_json_value};
pub use row_json::{write_row_json, write_row_json_resolved};

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
    use arrow::datatypes::{DataType, Fields};

    use super::{ColVariant, is_conflict_struct, json_priority, str_priority, variant_dt};

    #[kani::proof]
    fn verify_is_conflict_struct_empty_returns_false() {
        let fields = Fields::empty();
        assert!(!is_conflict_struct(&fields));
        kani::cover!(true, "empty fields path exercised");
    }

    #[kani::proof]
    fn verify_json_priority_ordering() {
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Float64));
        assert!(json_priority(&DataType::Float64) > json_priority(&DataType::Boolean));
        assert!(json_priority(&DataType::Boolean) > json_priority(&DataType::Utf8));
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Utf8));
        kani::cover!(
            json_priority(&DataType::Int64) > json_priority(&DataType::Utf8),
            "int beats utf8"
        );
        kani::cover!(
            json_priority(&DataType::Boolean) > json_priority(&DataType::Utf8),
            "bool beats utf8"
        );
    }

    #[kani::proof]
    fn verify_str_priority_ordering() {
        assert!(str_priority(&DataType::Utf8) > str_priority(&DataType::Int64));
        assert!(str_priority(&DataType::Boolean) > str_priority(&DataType::Int64));
        assert!(str_priority(&DataType::Utf8View) > str_priority(&DataType::Float64));
        assert!(str_priority(&DataType::Utf8) == str_priority(&DataType::Utf8View));
        kani::cover!(
            str_priority(&DataType::Utf8) > str_priority(&DataType::Int64),
            "utf8 beats int64"
        );
    }

    #[kani::proof]
    fn verify_json_and_str_priority_differ_for_int_vs_utf8() {
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Utf8));
        assert!(str_priority(&DataType::Utf8) > str_priority(&DataType::Int64));
        kani::cover!(
            true,
            "ordering inversion between json and str priority verified"
        );
    }

    #[kani::proof]
    fn verify_col_variant_flat_preserves_idx_and_type() {
        let col_idx: usize = kani::any();
        let v = ColVariant::Flat {
            col_idx,
            dt: DataType::Int64,
        };
        if let ColVariant::Flat { col_idx: idx, dt } = v {
            assert_eq!(idx, col_idx);
            assert_eq!(dt, DataType::Int64);
        }
        kani::cover!(col_idx > 0, "non-zero col_idx");
        kani::cover!(col_idx == 0, "zero col_idx");
    }

    #[kani::proof]
    fn verify_col_variant_struct_field_preserves_indices() {
        let struct_col_idx: usize = kani::any();
        let field_idx: usize = kani::any();
        let v = ColVariant::StructField {
            struct_col_idx,
            field_idx,
            dt: DataType::Utf8,
        };
        if let ColVariant::StructField {
            struct_col_idx: sci,
            field_idx: fi,
            dt,
        } = v
        {
            assert_eq!(sci, struct_col_idx);
            assert_eq!(fi, field_idx);
            assert_eq!(dt, DataType::Utf8);
        }
        kani::cover!(struct_col_idx > 0, "non-zero struct_col_idx");
        kani::cover!(field_idx > 0, "non-zero field_idx");
    }

    #[kani::proof]
    fn verify_variant_dt_flat() {
        let v = ColVariant::Flat {
            col_idx: 0,
            dt: DataType::Int64,
        };
        assert_eq!(variant_dt(&v), &DataType::Int64);
        kani::cover!(true, "variant_dt(Flat) returns correct type");
    }

    #[kani::proof]
    fn verify_variant_dt_struct_field() {
        let v = ColVariant::StructField {
            struct_col_idx: 0,
            field_idx: 1,
            dt: DataType::Float64,
        };
        assert_eq!(variant_dt(&v), &DataType::Float64);
        kani::cover!(true, "variant_dt(StructField) returns correct type");
    }
}
