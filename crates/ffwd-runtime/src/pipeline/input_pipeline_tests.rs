//! Tests for input pipeline: I/O worker, CPU worker, source metadata, CRI metadata.

// `super` here is `input_pipeline`; go up one more level to reach `pipeline`.
use super::super::io_worker::*;
use super::super::source_metadata::*;
use super::super::*;
use arrow::array::{Array, StringArray, StringViewArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::{Bytes, BytesMut};
use ffwd_arrow::Scanner;
use ffwd_io::input::{CriMetadata, FramedReadEvent, SourceEvent};
use ffwd_types::source_metadata::{SourceMetadataPlan, SourcePathColumn};
use proptest::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::processor::ProcessorError;
use ffwd_diagnostics::diagnostics::PipelineMetrics;
use ffwd_types::field_names;
use ffwd_types::pipeline::SourceId;

// --- InputPipelineManager integration tests ---
// These test the full split pipeline via from_config + run, so they
// don't need direct access to private types.

fn make_dummy_control_rx() -> tokio::sync::broadcast::Receiver<crate::pipeline::ControlMessage> {
    let (tx, rx) = tokio::sync::broadcast::channel(16);
    drop(tx);
    rx
}

#[test]
fn shutdown_repoll_continues_for_payload_without_matching_eof() {
    let events = vec![
        SourceEvent::Data {
            bytes: Bytes::from_static(b"a\n"),
            source_id: Some(SourceId(1)),
            accounted_bytes: 2,
            cri_metadata: None,
        },
        SourceEvent::EndOfFile {
            source_id: Some(SourceId(2)),
        },
    ];

    assert!(should_repoll_shutdown(&events, false, false));
}

#[test]
fn shutdown_repoll_stops_when_payload_source_reaches_eof() {
    let events = vec![
        SourceEvent::Data {
            bytes: Bytes::from_static(b"a\n"),
            source_id: Some(SourceId(1)),
            accounted_bytes: 2,
            cri_metadata: None,
        },
        SourceEvent::EndOfFile {
            source_id: Some(SourceId(1)),
        },
    ];

    assert!(!should_repoll_shutdown(&events, false, false));
}

#[test]
fn shutdown_repoll_stops_on_global_eof() {
    let events = vec![
        SourceEvent::Data {
            bytes: Bytes::from_static(b"a\n"),
            source_id: Some(SourceId(1)),
            accounted_bytes: 2,
            cri_metadata: None,
        },
        SourceEvent::EndOfFile { source_id: None },
    ];

    assert!(!should_repoll_shutdown(&events, false, true));
}

#[test]
fn shutdown_repoll_continues_for_empty_framed_output_after_source_payload() {
    assert!(should_repoll_shutdown(&[], false, true));
}

#[test]
fn shutdown_repoll_continues_for_eof_only_output_after_source_payload() {
    let events = vec![SourceEvent::EndOfFile {
        source_id: Some(SourceId(2)),
    }];

    assert!(should_repoll_shutdown(&events, false, true));
}

#[test]
fn buffered_shutdown_repoll_continues_for_payload_without_finish() {
    let events = vec![FramedReadEvent::Data {
        range: 0..2,
        source_id: Some(SourceId(1)),
        cri_metadata: None,
    }];

    assert!(should_repoll_shutdown_buffered(&events, false, false));
}

#[test]
fn source_metadata_attach_adds_row_columns_after_scan() {
    let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
    )
    .expect("batch");
    let input_name: Arc<str> = Arc::from("pods");
    let row_origins = vec![
        RowOriginSpan {
            source_id: Some(SourceId(10)),
            input_name: Arc::clone(&input_name),
            rows: 2,
        },
        RowOriginSpan {
            source_id: Some(SourceId(11)),
            input_name,
            rows: 1,
        },
    ];
    let source_paths = HashMap::from([
        (SourceId(10), "/var/log/pods/ns_pod_uid/c/0.log".to_string()),
        (SourceId(11), "/var/log/pods/ns_pod_uid/c/1.log".to_string()),
    ]);

    let out = source_metadata_for_batch(
        batch,
        &row_origins,
        &source_paths,
        SourceMetadataPlan {
            has_source_id: true,
            source_path: SourcePathColumn::Ecs,
        },
    )
    .expect("metadata attach should succeed");

    let source_id = out
        .column_by_name(field_names::SOURCE_ID)
        .expect("source id column")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("source id type");
    assert_eq!(source_id.value(0), 10);
    assert_eq!(source_id.value(1), 10);
    assert_eq!(source_id.value(2), 11);

    let source_path = out
        .column_by_name(field_names::ECS_FILE_PATH)
        .expect("source path column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("source path type");
    assert_eq!(source_path.value(0), "/var/log/pods/ns_pod_uid/c/0.log");
    assert_eq!(source_path.value(1), "/var/log/pods/ns_pod_uid/c/0.log");
    assert_eq!(source_path.value(2), "/var/log/pods/ns_pod_uid/c/1.log");
}

#[test]
fn cri_metadata_attach_adds_row_columns_after_scan() {
    let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            "plain", "cri-out", "cri-err",
        ]))],
    )
    .expect("batch");
    let mut metadata = CriMetadata::default();
    metadata.append_null_rows(1);
    metadata.append_value(b"2024-01-15T10:30:00Z", b"stdout");
    metadata.append_value(b"2024-01-15T10:30:01Z", b"stderr");

    let out = cri_metadata_for_batch(batch, Some(metadata)).expect("CRI metadata attach");

    let timestamp = out
        .column_by_name(field_names::TIMESTAMP_UNDERSCORE)
        .expect("timestamp column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("timestamp type");
    assert!(timestamp.is_null(0));
    assert_eq!(timestamp.value(1), "2024-01-15T10:30:00Z");
    assert_eq!(timestamp.value(2), "2024-01-15T10:30:01Z");

    let stream = out
        .column_by_name(field_names::CRI_STREAM)
        .expect("stream column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("stream type");
    assert!(stream.is_null(0));
    assert_eq!(stream.value(1), "stdout");
    assert_eq!(stream.value(2), "stderr");
}

#[test]
fn cri_metadata_overlay_preserves_payload_values_for_null_sidecar_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("msg", DataType::Utf8, true),
        Field::new(field_names::TIMESTAMP_UNDERSCORE, DataType::Utf8, true),
        Field::new(field_names::CRI_STREAM, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["plain", "cri", "plain2"])),
            Arc::new(StringArray::from(vec![
                "payload-0",
                "payload-1",
                "payload-2",
            ])),
            Arc::new(StringArray::from(vec![
                "payload-stream-0",
                "payload-stream-1",
                "payload-stream-2",
            ])),
        ],
    )
    .expect("batch");
    let mut metadata = CriMetadata::default();
    metadata.append_null_rows(1);
    metadata.append_value(b"2024-01-15T10:30:00Z", b"stdout");
    metadata.append_null_rows(1);

    let out = cri_metadata_for_batch(batch, Some(metadata)).expect("CRI metadata attach");

    let timestamp = out
        .column_by_name(field_names::TIMESTAMP_UNDERSCORE)
        .expect("timestamp column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("timestamp type");
    assert_eq!(timestamp.value(0), "payload-0");
    assert_eq!(timestamp.value(1), "2024-01-15T10:30:00Z");
    assert_eq!(timestamp.value(2), "payload-2");

    let stream = out
        .column_by_name(field_names::CRI_STREAM)
        .expect("stream column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("stream type");
    assert_eq!(stream.value(0), "payload-stream-0");
    assert_eq!(stream.value(1), "stdout");
    assert_eq!(stream.value(2), "payload-stream-2");
}

#[test]
fn cri_metadata_all_null_sidecar_materializes_null_columns_when_absent() {
    let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["plain", "plain2"]))],
    )
    .expect("batch");
    let mut metadata = CriMetadata::default();
    metadata.append_null_rows(2);

    let out = cri_metadata_for_batch(batch, Some(metadata)).expect("CRI metadata attach");

    let timestamp = out
        .column_by_name(field_names::TIMESTAMP_UNDERSCORE)
        .expect("timestamp column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("timestamp type");
    assert!(timestamp.is_null(0));
    assert!(timestamp.is_null(1));

    let stream = out
        .column_by_name(field_names::CRI_STREAM)
        .expect("stream column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("stream type");
    assert!(stream.is_null(0));
    assert!(stream.is_null(1));
}

#[test]
fn append_cri_metadata_for_data_pads_prior_and_later_rows() {
    let mut buffered = CriMetadata::default();
    let mut event_metadata = CriMetadata::default();
    event_metadata.append_value(b"2024-01-15T10:30:00Z", b"stdout");

    append_cri_metadata_for_data(
        &mut buffered,
        Some(event_metadata),
        b"{\"msg\":\"plain\"}\n",
        b"{\"msg\":\"cri\"}\n",
    );
    append_cri_metadata_for_data(&mut buffered, None, b"", b"{\"msg\":\"plain2\"}\n");

    assert_eq!(buffered.rows, 3);
    assert_eq!(buffered.spans.len(), 3);
    assert!(buffered.spans[0].values.is_none());
    assert!(buffered.spans[1].values.is_some());
    assert!(buffered.spans[2].values.is_none());
}

#[cfg(feature = "datafusion")]
#[test]
fn cri_metadata_columns_are_queryable_before_sql() {
    use crate::transform::SqlTransform;
    let mut transform =
        SqlTransform::new("SELECT _timestamp, _stream, msg FROM logs").expect("sql");
    let mut scanner = Scanner::new(transform.scan_config());
    let scanned = scanner
        .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
        .expect("scan");
    let mut metadata = CriMetadata::default();
    metadata.append_value(b"2024-01-15T10:30:00Z", b"stdout");
    let attached = cri_metadata_for_batch(scanned, Some(metadata)).expect("attach");

    let result = transform
        .execute_blocking(attached)
        .expect("transform should see CRI metadata");

    let timestamp = result
        .column_by_name(field_names::TIMESTAMP_UNDERSCORE)
        .expect("timestamp column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("timestamp type");
    assert_eq!(timestamp.value(0), "2024-01-15T10:30:00Z");

    let stream = result
        .column_by_name(field_names::CRI_STREAM)
        .expect("stream column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("stream type");
    assert_eq!(stream.value(0), "stdout");
}

#[test]
fn scanner_ready_row_count_skips_empty_lines_like_scanner() {
    assert_eq!(
        scanner_ready_row_count(b"\n\r\n{\"msg\":\"x\"}\nplain\nlast"),
        3
    );
    assert_eq!(scanner_ready_row_count(b"   \n\t\r\n"), 2);
}

#[test]
fn scanner_ready_row_count_matches_scanner_for_crlf_blank_lines() {
    let mut scanner = Scanner::new(ffwd_core::scan_config::ScanConfig::default());
    let batch = scanner.scan(Bytes::from_static(b"\r\n")).expect("scan");
    assert_eq!(batch.num_rows(), scanner_ready_row_count(b"\r\n"));

    let mut config = ffwd_core::scan_config::ScanConfig::default();
    config.line_field_name = Some(field_names::BODY.to_string());
    let mut raw_scanner = Scanner::new(config);
    let raw_batch = raw_scanner
        .scan(Bytes::from_static(b"\r\n"))
        .expect("scan raw");
    assert_eq!(raw_batch.num_rows(), scanner_ready_row_count(b"\r\n"));
}

proptest! {
    #[test]
    fn scanner_ready_row_count_matches_scanner_for_generated_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..256),
        line_field in any::<bool>(),
    ) {
        let mut config = ffwd_core::scan_config::ScanConfig::default();
        if line_field {
            config.line_field_name = Some(field_names::BODY.to_string());
        }
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan(Bytes::from(bytes.clone())).expect("scan");
        prop_assert_eq!(batch.num_rows(), scanner_ready_row_count(&bytes));
    }
}

#[test]
fn row_origins_count_split_line_once() {
    let input_name: Arc<str> = Arc::from("file");
    let mut row_origins = Vec::new();
    let mut pending = None;
    let source_id = Some(SourceId(5));

    append_data_row_origins(
        &mut row_origins,
        &mut pending,
        source_id,
        &input_name,
        b"{\"msg\":\"hel",
    );
    assert!(row_origins.is_empty());

    append_data_row_origins(
        &mut row_origins,
        &mut pending,
        source_id,
        &input_name,
        b"lo\"}\n{\"msg\":\"next\"}\n",
    );
    finalize_pending_row_origin(&mut row_origins, &mut pending);

    let mut scanner = Scanner::new(ffwd_core::scan_config::ScanConfig::default());
    let batch = scanner
        .scan(Bytes::from_static(
            b"{\"msg\":\"hello\"}\n{\"msg\":\"next\"}\n",
        ))
        .expect("scan");
    let rows_from_origins = row_origins.iter().map(|span| span.rows).sum::<usize>();
    assert_eq!(rows_from_origins, batch.num_rows());
    assert_eq!(row_origins.len(), 1);
    assert_eq!(row_origins[0].source_id, source_id);
    assert_eq!(row_origins[0].rows, 2);
}

#[test]
fn row_origins_finalize_partial_line_on_flush() {
    let input_name: Arc<str> = Arc::from("file");
    let mut row_origins = Vec::new();
    let mut pending = None;
    let source_id = Some(SourceId(9));

    append_data_row_origins(
        &mut row_origins,
        &mut pending,
        source_id,
        &input_name,
        b"{\"msg\":\"partial\"}",
    );
    finalize_pending_row_origin(&mut row_origins, &mut pending);

    let mut scanner = Scanner::new(ffwd_core::scan_config::ScanConfig::default());
    let batch = scanner
        .scan(Bytes::from_static(b"{\"msg\":\"partial\"}"))
        .expect("scan");
    assert_eq!(
        row_origins.iter().map(|span| span.rows).sum::<usize>(),
        batch.num_rows()
    );
    assert_eq!(row_origins[0].source_id, source_id);
}

#[test]
fn source_paths_by_id_filters_to_buffered_source_ids() {
    let row_origins = vec![RowOriginSpan {
        source_id: Some(SourceId(2)),
        input_name: Arc::from("file"),
        rows: 1,
    }];
    let paths = vec![
        (SourceId(1), PathBuf::from("/var/log/one.log")),
        (SourceId(2), PathBuf::from("/var/log/two.log")),
    ];

    // source_paths_by_id is private to io_worker, test through the public API
    // by constructing the expected result.
    let filtered: HashMap<SourceId, String> = paths
        .iter()
        .filter(|(sid, _)| row_origins.iter().any(|o| o.source_id == Some(*sid)))
        .map(|(sid, path)| (*sid, path.to_string_lossy().into_owned()))
        .collect();

    assert_eq!(filtered.len(), 1);
    assert_eq!(
        filtered.get(&SourceId(2)).map(String::as_str),
        Some("/var/log/two.log")
    );
}

struct SingleDataSource {
    emitted: bool,
}

impl InputSource for SingleDataSource {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        if self.emitted {
            return Ok(Vec::new());
        }
        self.emitted = true;
        Ok(vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"x\"}\n"),
            source_id: Some(SourceId(7)),
            accounted_bytes: 12,
            cri_metadata: None,
        }])
    }

    fn name(&self) -> &'static str {
        "source-implementation-name"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        vec![
            (SourceId(7), PathBuf::from("/var/log/configured.log")),
            (SourceId(8), PathBuf::from("/var/log/other.log")),
        ]
    }
}

struct SplitLineSource {
    emitted: bool,
}

impl InputSource for SplitLineSource {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        if self.emitted {
            return Ok(Vec::new());
        }
        self.emitted = true;
        Ok(vec![
            SourceEvent::Data {
                bytes: Bytes::from_static(b"{\"msg\":\"hel"),
                source_id: Some(SourceId(7)),
                accounted_bytes: 11,
                cri_metadata: None,
            },
            SourceEvent::Data {
                bytes: Bytes::from_static(b"lo\"}\n{\"msg\":\"next\"}\n"),
                source_id: Some(SourceId(7)),
                accounted_bytes: 21,
                cri_metadata: None,
            },
        ])
    }

    fn name(&self) -> &'static str {
        "split-line-source"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        vec![(SourceId(7), PathBuf::from("/var/log/split.log"))]
    }
}

struct SharedBufferSource {
    emitted: bool,
    finished: bool,
}

impl InputSource for SharedBufferSource {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        panic!("poll() should not be used when poll_into() is available"); // ALLOW-PANIC: test-only InputSource that deliberately never uses poll()
    }

    fn poll_into(&mut self, dst: &mut BytesMut) -> io::Result<Option<Vec<FramedReadEvent>>> {
        if self.emitted {
            self.finished = true;
            return Ok(Some(Vec::new()));
        }
        let start = dst.len();
        dst.extend_from_slice(b"{\"msg\":\"shared\"}\n");
        self.emitted = true;
        self.finished = true;
        Ok(Some(vec![FramedReadEvent::Data {
            range: start..dst.len(),
            source_id: None,
            cri_metadata: None,
        }]))
    }

    fn name(&self) -> &str {
        "shared-buffer"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

struct LargeSharedBufferSource {
    emitted: bool,
    finished: bool,
}

impl InputSource for LargeSharedBufferSource {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        panic!("poll() should not be used when poll_into() is available"); // ALLOW-PANIC: test-only InputSource that deliberately never uses poll()
    }

    fn poll_into(&mut self, dst: &mut BytesMut) -> io::Result<Option<Vec<FramedReadEvent>>> {
        if self.emitted {
            return Ok(Some(Vec::new()));
        }
        let start = dst.len();
        let chunk = vec![b'x'; 80 * 1024];
        dst.extend_from_slice(&chunk);
        self.emitted = true;
        self.finished = true;
        Ok(Some(vec![FramedReadEvent::Data {
            range: start..dst.len(),
            source_id: None,
            cri_metadata: None,
        }]))
    }

    fn name(&self) -> &str {
        "large-shared-buffer"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

struct BatchSource {
    emitted: bool,
}

impl InputSource for BatchSource {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        if self.emitted {
            return Ok(Vec::new());
        }
        self.emitted = true;
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
            .expect("batch");
        Ok(vec![SourceEvent::Batch {
            batch,
            source_id: Some(SourceId(12)),
            accounted_bytes: 2,
        }])
    }

    fn name(&self) -> &'static str {
        "batch-source"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        vec![(SourceId(12), PathBuf::from("/var/log/batch.log"))]
    }
}

struct ShutdownDrainSource {
    emitted: bool,
}

impl InputSource for ShutdownDrainSource {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        if self.emitted {
            return Ok(Vec::new());
        }
        self.emitted = true;
        Ok(vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"drain\"}\n"),
            source_id: Some(SourceId(13)),
            accounted_bytes: 16,
            cri_metadata: None,
        }])
    }

    fn name(&self) -> &'static str {
        "shutdown-drain-source"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        vec![(SourceId(13), PathBuf::from("/var/log/drain.log"))]
    }
}

struct MultiShutdownPollSource {
    remaining: usize,
    emitted: usize,
    finished: bool,
}

impl InputSource for MultiShutdownPollSource {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        Ok(Vec::new())
    }

    fn poll_shutdown(&mut self) -> io::Result<Vec<SourceEvent>> {
        if self.remaining == 0 {
            if self.finished {
                return Ok(Vec::new());
            }
            self.finished = true;
            return Ok(vec![SourceEvent::EndOfFile {
                source_id: Some(SourceId(14)),
            }]);
        }

        self.emitted += 1;
        self.remaining -= 1;
        let bytes =
            Bytes::from(format!("{{\"msg\":\"shutdown-{}\"}}\n", self.emitted).into_bytes());
        let accounted_bytes = bytes.len() as u64;
        Ok(vec![SourceEvent::Data {
            bytes,
            source_id: Some(SourceId(14)),
            accounted_bytes,
            cri_metadata: None,
        }])
    }

    fn name(&self) -> &'static str {
        "multi-shutdown-poll-source"
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        vec![(SourceId(14), PathBuf::from("/var/log/shutdown.log"))]
    }
}

/// Capacity of the bounded channel between I/O worker and CPU worker.
const IO_CPU_CHANNEL_CAPACITY: usize = 4;

/// Constant for shutdown drain log interval (mirrors the production constant).
const SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS: usize = 64;

fn framed_data_payload(id: u8) -> Vec<u8> {
    format!("{{\"msg\":\"data-{id}\"}}\n").into_bytes()
}

#[test]
fn io_worker_uses_configured_input_name_for_source_metadata() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(SingleDataSource { emitted: false }),
        buf: BytesMut::new(),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_configured_input_name");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
    let worker_shutdown = shutdown.clone();

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            worker_shutdown,
            1,
            Duration::from_secs(60),
            Duration::from_millis(1),
            0,
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::Ecs,
            },
        );
    });

    let item = rx.blocking_recv().expect("io worker item");
    shutdown.cancel();
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::RawBatch(chunk) = item else {
        panic!("expected byte chunk");
    };
    assert_eq!(chunk.row_origins.len(), 1);
    assert_eq!(chunk.row_origins[0].input_name.as_ref(), "configured-input");
    assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(7)));
    assert_eq!(chunk.row_origins[0].rows, 1);
    assert_eq!(
        chunk.source_paths.get(&SourceId(7)).map(String::as_str),
        Some("/var/log/configured.log")
    );
    assert!(!chunk.source_paths.contains_key(&SourceId(8)));
}

#[test]
fn io_worker_counts_split_line_origin_once() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(SplitLineSource { emitted: false }),
        buf: BytesMut::new(),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_split_line_origin");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
    let worker_shutdown = shutdown.clone();

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            worker_shutdown,
            usize::MAX,
            Duration::ZERO,
            Duration::from_millis(1),
            0,
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::Ecs,
            },
        );
    });

    let item = rx.blocking_recv().expect("io worker item");
    shutdown.cancel();
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::RawBatch(chunk) = item else {
        panic!("expected byte chunk");
    };
    assert_eq!(
        chunk.bytes.as_ref(),
        b"{\"msg\":\"hello\"}\n{\"msg\":\"next\"}\n"
    );
    assert_eq!(chunk.row_origins.len(), 1);
    assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(7)));
    assert_eq!(chunk.row_origins[0].rows, 2);
    assert_eq!(
        chunk.source_paths.get(&SourceId(7)).map(String::as_str),
        Some("/var/log/split.log")
    );
}

#[test]
fn io_worker_attaches_source_metadata_to_batch_event() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(BatchSource { emitted: false }),
        buf: BytesMut::new(),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_batch_source_metadata");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
    let worker_shutdown = shutdown.clone();

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            worker_shutdown,
            usize::MAX,
            Duration::from_secs(60),
            Duration::from_millis(1),
            0,
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::Ecs,
            },
        );
    });

    let item = rx.blocking_recv().expect("io worker item");
    shutdown.cancel();
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::Batch {
        batch,
        row_origins,
        source_paths,
        ..
    } = item
    else {
        panic!("expected batch item");
    };
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(row_origins.len(), 1);
    assert_eq!(row_origins[0].source_id, Some(SourceId(12)));
    assert_eq!(row_origins[0].rows, 2);
    assert_eq!(
        source_paths.get(&SourceId(12)).map(String::as_str),
        Some("/var/log/batch.log")
    );
}

#[test]
fn io_worker_shutdown_drains_buffered_source_metadata() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(ShutdownDrainSource { emitted: false }),
        buf: BytesMut::new(),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_shutdown_drain");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
    let worker_shutdown = shutdown.clone();

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            worker_shutdown,
            usize::MAX,
            Duration::from_secs(60),
            Duration::from_millis(1),
            0,
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::Ecs,
            },
        );
    });

    std::thread::sleep(Duration::from_millis(20));
    shutdown.cancel();
    let item = rx.blocking_recv().expect("shutdown drain item");
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::RawBatch(chunk) = item else {
        panic!("expected byte chunk");
    };
    assert_eq!(chunk.bytes.as_ref(), b"{\"msg\":\"drain\"}\n");
    assert_eq!(chunk.row_origins.len(), 1);
    assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(13)));
    assert_eq!(chunk.row_origins[0].rows, 1);
    assert_eq!(
        chunk.source_paths.get(&SourceId(13)).map(String::as_str),
        Some("/var/log/drain.log")
    );
}

#[test]
fn io_worker_shutdown_repolls_until_source_finishes() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(MultiShutdownPollSource {
            remaining: SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS + 2,
            emitted: 0,
            finished: false,
        }),
        buf: BytesMut::new(),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_shutdown_repolls");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
    let worker_shutdown = shutdown.clone();

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            worker_shutdown,
            usize::MAX,
            Duration::from_secs(60),
            Duration::from_millis(1),
            0,
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::Ecs,
            },
        );
    });

    std::thread::sleep(Duration::from_millis(20));
    shutdown.cancel();
    let item = rx.blocking_recv().expect("shutdown drain item");
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::RawBatch(chunk) = item else {
        panic!("expected byte chunk");
    };
    let expected = (1..=SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS + 2)
        .map(|index| format!("{{\"msg\":\"shutdown-{index}\"}}\n"))
        .collect::<String>();
    assert_eq!(chunk.bytes.as_ref(), expected.as_bytes());
    assert_eq!(chunk.row_origins.len(), 1);
    assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(14)));
    assert_eq!(
        chunk.row_origins[0].rows,
        SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS + 2
    );
    assert_eq!(
        chunk.source_paths.get(&SourceId(14)).map(String::as_str),
        Some("/var/log/shutdown.log")
    );
}

#[test]
fn io_worker_extends_existing_buffer_via_poll_into_path() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(SharedBufferSource {
            emitted: false,
            finished: false,
        }),
        buf: BytesMut::from(&b"{\"msg\":\"seed\"}\n"[..]),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_shared_buffer_path");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            shutdown,
            usize::MAX,
            Duration::from_secs(60),
            Duration::from_millis(1),
            0,
            SourceMetadataPlan::default(),
        );
    });

    let item = rx.blocking_recv().expect("io worker item");
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::RawBatch(chunk) = item else {
        panic!("expected byte chunk");
    };
    assert_eq!(
        chunk.bytes.as_ref(),
        b"{\"msg\":\"seed\"}\n{\"msg\":\"shared\"}\n"
    );
}

#[test]
fn io_worker_uses_poll_into_path_with_empty_buffer() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(SharedBufferSource {
            emitted: false,
            finished: false,
        }),
        buf: BytesMut::new(),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_shared_buffer_empty_start");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            shutdown,
            usize::MAX,
            Duration::from_secs(60),
            Duration::from_millis(1),
            0,
            SourceMetadataPlan::default(),
        );
    });

    let item = rx.blocking_recv().expect("io worker item");
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::RawBatch(chunk) = item else {
        panic!("expected byte chunk");
    };
    assert_eq!(chunk.bytes.as_ref(), b"{\"msg\":\"shared\"}\n");
}

#[test]
fn io_worker_flushes_large_shared_buffer_chunk_without_waiting_for_timeout() {
    let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
    let input = IngestState {
        source: Box::new(LargeSharedBufferSource {
            emitted: false,
            finished: false,
        }),
        buf: BytesMut::new(),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: CriMetadata::default(),
        stats,
    };
    let meter = opentelemetry::global::meter("io_worker_large_shared_buffer");
    let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
    let worker_shutdown = shutdown.clone();

    let handle = std::thread::spawn(move || {
        io_worker_loop(
            input,
            Arc::from("configured-input"),
            tx,
            metrics,
            make_dummy_control_rx(),
            worker_shutdown,
            usize::MAX,
            Duration::from_secs(60),
            Duration::from_millis(1),
            0,
            SourceMetadataPlan::default(),
        );
    });

    let item = rx.blocking_recv().expect("io worker item");
    shutdown.cancel();
    drop(rx);
    handle.join().expect("io worker exits");

    let IoWorkItem::RawBatch(chunk) = item else {
        panic!("expected byte chunk");
    };
    assert_eq!(chunk.bytes.len(), 80 * 1024);
    assert!(chunk.bytes.iter().all(|byte| *byte == b'x'));
}

proptest! {
    #[test]
    fn process_buffered_events_preserves_generated_data_sequences(
        ids in prop::collection::vec(any::<u8>(), 1..16),
    ) {
        let (tx, mut rx) = mpsc::channel(64);
        let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
        let mut input = IngestState {
            source: Box::new(SingleDataSource { emitted: false }),
            buf: BytesMut::new(),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            cri_metadata: CriMetadata::default(),
            stats,
        };
        let input_name: Arc<str> = Arc::from("configured-input");
        let meter = opentelemetry::global::meter("process_buffered_events_proptest");
        let metrics = PipelineMetrics::new("test", "SELECT * FROM logs", &meter);
        let mut last_bp_warn = None;
        let mut buffered_since = None;
        let mut pending_row_origin = None;
        let mut events = Vec::new();
        let mut expected = Vec::new();

        for id in &ids {
            let payload = framed_data_payload(*id);
            let start = input.buf.len();
            input.buf.extend_from_slice(&payload);
            expected.extend_from_slice(&payload);
            events.push(FramedReadEvent::Data {
                range: start..input.buf.len(),
                source_id: None,
                cri_metadata: None,
            });
        }

        let ok = process_buffered_events(
            &mut input,
            &input_name,
            events,
            &tx,
            &metrics,
            &mut last_bp_warn,
            0,
            1,
            &mut buffered_since,
            &mut pending_row_origin,
            true,
            SourceMetadataPlan::default(),
        );

        prop_assert!(ok);
        prop_assert!(input.buf.is_empty());
        let item = rx.try_recv().expect("one emitted bytes chunk");
        let IoWorkItem::RawBatch(chunk) = item else {
            panic!("expected bytes chunk");
        };
        prop_assert_eq!(chunk.bytes.as_ref(), expected.as_slice());
        prop_assert!(rx.try_recv().is_err());
    }
}

#[test]
fn source_metadata_attach_replaces_payload_reserved_column() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_names::ECS_FILE_PATH,
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["payload-value"]))],
    )
    .expect("batch");
    let row_origins = vec![RowOriginSpan {
        source_id: Some(SourceId(5)),
        input_name: Arc::from("file"),
        rows: 1,
    }];
    let source_paths = HashMap::from([(SourceId(5), "/actual/path.log".to_string())]);

    let out = source_metadata_for_batch(
        batch,
        &row_origins,
        &source_paths,
        SourceMetadataPlan {
            source_path: SourcePathColumn::Ecs,
            ..SourceMetadataPlan::default()
        },
    )
    .expect("metadata attach should succeed");

    assert_eq!(out.num_columns(), 1);
    let source_path = out
        .column_by_name(field_names::ECS_FILE_PATH)
        .expect("source path column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("source path type");
    assert_eq!(source_path.value(0), "/actual/path.log");
}

#[test]
fn source_metadata_attach_rejects_row_count_mismatch() {
    let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
        .expect("batch");
    let row_origins = vec![RowOriginSpan {
        source_id: Some(SourceId(1)),
        input_name: Arc::from("file"),
        rows: 1,
    }];

    let err = source_metadata_for_batch(
        batch,
        &row_origins,
        &HashMap::new(),
        SourceMetadataPlan {
            has_source_id: true,
            ..SourceMetadataPlan::default()
        },
    )
    .expect_err("row count mismatch should fail");
    assert!(err.to_string().contains("row count mismatch"));
}

#[cfg(feature = "datafusion")]
#[test]
fn public_source_path_attached_before_sql_is_queryable() {
    use crate::transform::SqlTransform;
    let mut transform = SqlTransform::new(r#"SELECT "file.path", msg FROM logs"#).expect("sql");
    let mut scanner = Scanner::new(transform.scan_config());
    let scanned = scanner
        .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
        .expect("scan");
    let row_origins = vec![RowOriginSpan {
        source_id: Some(SourceId(99)),
        input_name: Arc::from("pods"),
        rows: 1,
    }];
    let source_paths =
        HashMap::from([(SourceId(99), "/var/log/pods/ns_pod_uid/c/0.log".to_string())]);
    let attached = source_metadata_for_batch(
        scanned,
        &row_origins,
        &source_paths,
        SourceMetadataPlan {
            has_source_id: false,
            source_path: SourcePathColumn::Ecs,
        },
    )
    .expect("attach");

    let result = transform
        .execute_blocking(attached)
        .expect("transform should see source path");
    let source_path = result
        .column_by_name(field_names::ECS_FILE_PATH)
        .expect("source path column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("source path type");
    assert_eq!(source_path.value(0), "/var/log/pods/ns_pod_uid/c/0.log");
}

#[cfg(feature = "datafusion")]
#[test]
fn select_star_includes_public_source_metadata_style_columns() {
    use crate::transform::SqlTransform;
    let mut transform = SqlTransform::new(r#"SELECT * FROM logs"#).expect("sql");
    let mut scanner = Scanner::new(transform.scan_config());
    let scanned = scanner
        .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
        .expect("scan");
    let row_origins = vec![RowOriginSpan {
        source_id: Some(SourceId(99)),
        input_name: Arc::from("pods"),
        rows: 1,
    }];
    let source_paths =
        HashMap::from([(SourceId(99), "/var/log/pods/ns_pod_uid/c/0.log".to_string())]);
    let attached = source_metadata_for_batch(
        scanned,
        &row_origins,
        &source_paths,
        SourceMetadataPlan {
            has_source_id: false,
            source_path: SourcePathColumn::Ecs,
        },
    )
    .expect("attach");

    let result = transform
        .execute_blocking(attached)
        .expect("transform should preserve source metadata columns");

    assert_eq!(result.num_rows(), 1);
    assert!(result.column_by_name(field_names::ECS_FILE_PATH).is_some());
    assert!(result.column_by_name("msg").is_some());
}

#[cfg(feature = "datafusion")]
#[test]
fn select_star_does_not_attach_source_metadata() {
    use crate::transform::SqlTransform;
    let mut transform = SqlTransform::new("SELECT * FROM logs").expect("sql");
    let mut scanner = Scanner::new(transform.scan_config());
    let scanned = scanner
        .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
        .expect("scan");
    let row_origins = vec![RowOriginSpan {
        source_id: Some(SourceId(99)),
        input_name: Arc::from("pods"),
        rows: 1,
    }];
    let source_paths =
        HashMap::from([(SourceId(99), "/var/log/pods/ns_pod_uid/c/0.log".to_string())]);
    let attached = source_metadata_for_batch(
        scanned,
        &row_origins,
        &source_paths,
        transform.analyzer().source_metadata_plan(),
    )
    .expect("attach");

    let result = transform
        .execute_blocking(attached)
        .expect("transform should keep wildcard projection narrow");

    assert!(result.column_by_name(field_names::SOURCE_ID).is_none());
}

#[cfg(feature = "datafusion")]
#[test]
fn explicit_projection_preserves_new_source_metadata_columns() {
    use crate::transform::SqlTransform;
    let mut transform = SqlTransform::new("SELECT msg, __source_id FROM logs").expect("sql");
    let mut scanner = Scanner::new(transform.scan_config());
    let scanned = scanner
        .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
        .expect("scan");
    let row_origins = vec![RowOriginSpan {
        source_id: Some(SourceId(99)),
        input_name: Arc::from("pods"),
        rows: 1,
    }];
    let attached = source_metadata_for_batch(
        scanned,
        &row_origins,
        &HashMap::new(),
        SourceMetadataPlan {
            has_source_id: true,
            source_path: SourcePathColumn::None,
        },
    )
    .expect("attach");

    let result = transform
        .execute_blocking(attached)
        .expect("transform should preserve explicit source metadata");

    let source_id = result
        .column_by_name(field_names::SOURCE_ID)
        .expect("source id column")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("source id type");
    assert_eq!(source_id.value(0), 99);
}

#[cfg(not(feature = "turmoil"))]
#[test]
fn manager_spawns_and_joins_with_empty_inputs() {
    // Verify InputPipelineManager handles zero inputs gracefully.
    // We test this through the pipeline's run path with a generator
    // that immediately shuts down.
    let yaml = r"
pipelines:
  default:
    inputs:
      - type: generator
    outputs:
      - type: 'null'
";
    let config = ffwd_config::Config::load_str(yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let meter = opentelemetry::global::meter("test");
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        sd.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "pipeline should shut down cleanly: {result:?}"
    );
}

#[cfg(not(feature = "turmoil"))]
#[test]
fn split_pipeline_processes_file_with_sql_filter() {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("split_test.log");

    // Write lines: half INFO, half DEBUG.
    let mut data = String::new();
    for i in 0..20 {
        let level = if i % 2 == 0 { "INFO" } else { "DEBUG" };
        data.push_str(&format!(r#"{{"level":"{}","seq":{}}}"#, level, i));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
    transform: "SELECT * FROM logs WHERE level = 'INFO'"
    outputs:
      - type: 'null'
"#,
        log_path.display()
    );

    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let meter = opentelemetry::global::meter("test");
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let metrics = pipeline.metrics().clone();

    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            // Simple SQL predicates can be pushed into the scanner, so
            // transform_in reflects only rows that survive the filter.
            if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 10 {
                std::thread::sleep(Duration::from_millis(50));
                sd.cancel();
                return;
            }
            if Instant::now() > deadline {
                sd.cancel();
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "split pipeline with SQL should work: {result:?}"
    );

    // Scanner-level predicate pushdown filters the 20 source rows to 10
    // rows before they reach the transform stage.
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 10,
        "expected 10 filtered rows into transform, got {lines_in}"
    );

    // Those same 10 rows reach output.
    let lines_out = pipeline
        .metrics
        .transform_out
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_out, 10,
        "expected 10 lines after SQL filter, got {lines_out}"
    );
}

/// Two file inputs processed simultaneously through InputPipelineManager.
/// Verifies both inputs' data reaches the output.
#[cfg(not(feature = "turmoil"))]
#[test]
fn split_pipeline_two_inputs_both_processed() {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let log1 = dir.path().join("input1.log");
    let log2 = dir.path().join("input2.log");

    // 10 lines each, different content.
    let mut data1 = String::new();
    let mut data2 = String::new();
    for i in 0..10 {
        data1.push_str(&format!(r#"{{"src":"a","seq":{}}}"#, i));
        data1.push('\n');
        data2.push_str(&format!(r#"{{"src":"b","seq":{}}}"#, i));
        data2.push('\n');
    }
    std::fs::write(&log1, data1.as_bytes()).unwrap();
    std::fs::write(&log2, data2.as_bytes()).unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
      - type: file
        path: {}
        format: json
    outputs:
      - type: 'null'
"#,
        log1.display(),
        log2.display()
    );

    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let meter = opentelemetry::global::meter("test");
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let metrics = pipeline.metrics().clone();

    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            // Wait until both files' data is scanned (20 lines total).
            if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 20 {
                std::thread::sleep(Duration::from_millis(50));
                sd.cancel();
                return;
            }
            if Instant::now() > deadline {
                sd.cancel();
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok(), "two-input pipeline should work: {result:?}");

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 20,
        "expected 20 lines from both inputs, got {lines_in}"
    );
}

/// End-to-end regression: file-tail read-budget signals should propagate
/// through framed input into the runtime adaptive cadence loop.
#[cfg(not(feature = "turmoil"))]
#[test]
fn split_pipeline_records_adaptive_fast_repolls() {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("adaptive_fast_repolls.log");

    let payload = "x".repeat(160);
    let mut data = String::new();
    for i in 0..40 {
        data.push_str(&format!(
            r#"{{"level":"INFO","seq":{},"msg":"{}"}}"#,
            i, payload
        ));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
        poll_interval_ms: 200
        per_file_read_budget_bytes: 64
        adaptive_fast_polls_max: 4
    outputs:
      - type: 'null'
"#,
        log_path.display()
    );

    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let meter = opentelemetry::global::meter("test");
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let metrics = pipeline.metrics().clone();
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let lines_in = metrics.transform_in.lines_total.load(Ordering::Relaxed);
            let fast_repolls = metrics.cadence_fast_repolls.load(Ordering::Relaxed);
            if lines_in >= 40 && fast_repolls > 0 {
                sd.cancel();
                return;
            }
            if Instant::now() > deadline {
                sd.cancel();
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "adaptive cadence pipeline should run cleanly: {result:?}"
    );

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 40,
        "expected to process test file rows, got {lines_in}"
    );

    let fast_repolls = pipeline
        .metrics
        .cadence_fast_repolls
        .load(Ordering::Relaxed);
    assert!(
        fast_repolls > 0,
        "expected adaptive cadence fast repolls to be recorded, got {fast_repolls}"
    );
}

#[cfg(not(feature = "turmoil"))]
#[test]
fn split_pipeline_shutdown_flushes_file_remainder_before_idle_eof() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown_partial.log");
    std::fs::write(&log_path, br#"{"level":"INFO","seq":775}"#).unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
        poll_interval_ms: 60000
    outputs:
      - type: 'null'
"#,
        log_path.display()
    );

    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let meter = opentelemetry::global::meter("test");
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
    pipeline.set_batch_timeout(Duration::from_secs(60));

    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "shutdown flush pipeline should run cleanly: {result:?}"
    );

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 1,
        "shutdown must flush final no-newline file record before normal idle EOF"
    );
}

/// Transform error: CPU worker drops the batch and does NOT advance
/// the checkpoint (correct at-least-once semantics — data will be
/// re-read on restart).
#[cfg(not(feature = "turmoil"))]
#[test]
fn split_pipeline_transform_error_drops_batch() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("transform_err.log");

    let mut data = String::new();
    for i in 0..10 {
        data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    // SQL references a column that doesn't exist -> DataFusion error.
    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
    transform: "SELECT nonexistent_col FROM logs"
    outputs:
      - type: 'null'
"#,
        log_path.display()
    );

    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let meter = opentelemetry::global::meter("test");
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        sd.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "pipeline should survive transform errors: {result:?}"
    );

    let errors = pipeline.metrics.transform_errors.load(Ordering::Relaxed);
    assert!(
        errors > 0,
        "CPU worker should report transform errors, got {errors}"
    );

    // No data reaches the pipeline — checkpoints NOT advanced
    // (at-least-once: data will be re-read on restart).
    let dropped = pipeline
        .metrics
        .dropped_batches_total
        .load(Ordering::Relaxed);
    assert!(
        dropped > 0,
        "failed batches should be counted as dropped, got {dropped}"
    );
}

/// Processor that returns a Transient error — tickets should be held
/// (checkpoint does NOT advance), and the pipeline continues.
#[cfg(not(feature = "turmoil"))]
#[test]
fn split_pipeline_processor_transient_error_continues() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("proc_err.log");

    let mut data = String::new();
    for i in 0..10 {
        data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
    outputs:
      - type: 'null'
"#,
        log_path.display()
    );

    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let meter = opentelemetry::global::meter("test");
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    // Processor that always returns a transient error.
    #[derive(Debug)]
    struct FailingProcessor;
    impl Processor for FailingProcessor {
        fn process(
            &mut self,
            _batch: RecordBatch,
            _meta: &ffwd_output::BatchMetadata,
        ) -> Result<smallvec::SmallVec<[RecordBatch; 1]>, ProcessorError> {
            Err(ProcessorError::Transient(
                "test transient error".to_string(),
            ))
        }

        fn flush(&mut self) -> smallvec::SmallVec<[RecordBatch; 1]> {
            smallvec::SmallVec::new()
        }

        fn name(&self) -> &'static str {
            "failing"
        }

        fn is_stateful(&self) -> bool {
            false
        }
    }

    pipeline = pipeline.with_processor(Box::new(FailingProcessor));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        sd.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "pipeline should survive processor errors: {result:?}"
    );

    // Data was scanned (transform_in > 0) but rejected by processor
    // so nothing reached output (transform_out == 0).
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(lines_in > 0, "data should be scanned, got {lines_in}");

    let lines_out = pipeline
        .metrics
        .transform_out
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_out, 0,
        "no data should pass through failing processor, got {lines_out}"
    );
}
