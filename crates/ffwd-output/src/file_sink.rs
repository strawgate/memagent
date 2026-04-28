use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use ffwd_types::diagnostics::ComponentStats;

use crate::BatchMetadata;
use crate::pipelined::{FileWriter, JsonBatchSerializer, PipelineConfig, PipelinedSink};
use crate::sink::{SendResult, Sink, SinkFactory};

use super::stdout::StdoutFormat;
use ffwd_types::field_names;

/// Append-only file sink with pipelined serialization and I/O.
///
/// Serialization (JSON/text) runs on the tokio worker thread while a dedicated
/// OS writer thread handles file I/O. This overlaps CPU and I/O for ~40% higher
/// sustained throughput at volume.
///
/// Uses the same row serialization logic as `StdoutSink` so `stdout` and
/// `file` stay behaviorally aligned.
pub struct FileSink {
    inner: PipelinedSink<JsonBatchSerializer>,
}

impl FileSink {
    pub fn new(
        name: String,
        format: StdoutFormat,
        file: std::fs::File,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self::with_message_field(name, format, field_names::BODY.to_string(), file, stats)
    }

    /// Create a file sink with a custom text/console message field fallback.
    pub fn with_message_field(
        name: String,
        format: StdoutFormat,
        message_field: String,
        file: std::fs::File,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let serializer = JsonBatchSerializer::with_message_field(
            name,
            format,
            message_field,
            Arc::clone(&stats),
        );
        let writer = FileWriter::new(file);
        let config = PipelineConfig::default();
        Self {
            inner: PipelinedSink::new(serializer, writer, stats, config),
        }
    }
}

impl Sink for FileSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        self.inner.send_batch(batch, metadata)
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        self.inner.flush()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        self.inner.shutdown()
    }
}

pub struct FileSinkFactory {
    name: String,
    format: StdoutFormat,
    message_field: String,
    path: String,
    stats: Arc<ComponentStats>,
}

impl FileSinkFactory {
    pub fn new(
        name: String,
        path: String,
        format: StdoutFormat,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::with_message_field(name, path, format, field_names::BODY.to_string(), stats)
    }

    /// Create a file sink factory with a custom text/console message field fallback.
    pub fn with_message_field(
        name: String,
        path: String,
        format: StdoutFormat,
        message_field: String,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        // Verify we can open the file (fail-fast on permission errors).
        let _file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            name,
            format,
            message_field,
            path,
            stats,
        })
    }
}

impl SinkFactory for FileSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(Box::new(FileSink::with_message_field(
            self.name.clone(),
            self.format,
            self.message_field.clone(),
            file,
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_use(&self) -> bool {
        // A single file can only safely have one writer — multiple fds in
        // append mode risk interleaving partial writes for large buffers.
        // The pipelined sink already overlaps serialize+I/O on one worker,
        // which is the optimal architecture for single-file output.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

    fn metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        }
    }

    fn temp_path(name: &str) -> PathBuf {
        let unique = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ffwd-output-{name}-{}-{unique}.tmp",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn file_sink_writes_json_lines() {
        let path = temp_path("capture-ndjson");
        let stats = Arc::new(ComponentStats::new());
        let factory = FileSinkFactory::new(
            "capture".to_string(),
            path.to_string_lossy().into_owned(),
            StdoutFormat::Json,
            Arc::clone(&stats),
        )
        .unwrap();
        let mut sink = factory.create().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("INFO"), Some("ERROR")])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("boom")])),
            ],
        )
        .unwrap();

        sink.send_batch(&batch, &metadata()).await.unwrap();
        sink.flush().await.unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains(r#""level":"INFO""#), "body: {body}");
        assert!(body.contains(r#""message":"boom""#), "body: {body}");
        assert_eq!(stats.lines_total.load(Ordering::Relaxed), 2);
        assert!(stats.bytes_total.load(Ordering::Relaxed) > 0);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn file_sink_text_mode_writes_message_lines() {
        let path = temp_path("capture-log");
        let stats = Arc::new(ComponentStats::new());
        let factory = FileSinkFactory::new(
            "capture".to_string(),
            path.to_string_lossy().into_owned(),
            StdoutFormat::Text,
            Arc::clone(&stats),
        )
        .unwrap();
        let mut sink = factory.create().unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("first line"),
                Some("second line"),
            ]))],
        )
        .unwrap();

        sink.send_batch(&batch, &metadata()).await.unwrap();
        sink.flush().await.unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body, "first line\nsecond line\n");
        assert_eq!(stats.lines_total.load(Ordering::Relaxed), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn file_sink_text_mode_skips_null_message_rows_in_stats() {
        let path = temp_path("capture-log-null");
        let stats = Arc::new(ComponentStats::new());
        let factory = FileSinkFactory::new(
            "capture".to_string(),
            path.to_string_lossy().into_owned(),
            StdoutFormat::Text,
            Arc::clone(&stats),
        )
        .unwrap();
        let mut sink = factory.create().unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("first line"),
                None,
                Some("third line"),
            ]))],
        )
        .unwrap();

        sink.send_batch(&batch, &metadata()).await.unwrap();
        sink.flush().await.unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body, "first line\nthird line\n");
        assert_eq!(stats.lines_total.load(Ordering::Relaxed), 2);
        let _ = std::fs::remove_file(&path);
    }
}
