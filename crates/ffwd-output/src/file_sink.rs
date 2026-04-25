use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use ffwd_types::diagnostics::ComponentStats;

use crate::BatchMetadata;
use crate::sink::{SendResult, Sink, SinkFactory};

use super::stdout::{StdoutFormat, StdoutSink};
use ffwd_types::field_names;

/// Append-only file sink.
///
/// Uses the same row serialization logic as `StdoutSink` so `stdout` and
/// `file` stay behaviorally aligned.
pub struct FileSink {
    serializer: StdoutSink,
    file: Arc<Mutex<tokio::fs::File>>,
    output_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl FileSink {
    pub fn new(
        name: String,
        format: StdoutFormat,
        file: Arc<Mutex<tokio::fs::File>>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self::with_message_field(name, format, field_names::BODY.to_string(), file, stats)
    }

    /// Create a file sink with a custom text/console message field fallback.
    ///
    /// The serializer prefers canonical `body` when present, then this
    /// configured field, then legacy aliases.
    pub fn with_message_field(
        name: String,
        format: StdoutFormat,
        message_field: String,
        file: Arc<Mutex<tokio::fs::File>>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self {
            serializer: StdoutSink::with_message_field(
                name,
                format,
                message_field,
                Arc::clone(&stats),
            ),
            file,
            output_buf: Vec::with_capacity(64 * 1024),
            stats,
        }
    }
}

impl Sink for FileSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            self.output_buf.clear();
            let lines_written =
                match self
                    .serializer
                    .write_batch_to(batch, metadata, &mut self.output_buf)
                {
                    Ok(n) => n as u64,
                    Err(e) => return SendResult::from_io_error(e),
                };

            let bytes_written = self.output_buf.len() as u64;
            let mut file = self.file.lock().await;
            if let Err(e) = file.write_all(&self.output_buf).await {
                return SendResult::from_io_error(e);
            }

            self.stats.inc_lines(lines_written);
            self.stats.inc_bytes(bytes_written);
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut file = self.file.lock().await;
            file.flush().await?;
            file.sync_data().await
        })
    }

    fn name(&self) -> &str {
        self.serializer.name()
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut file = self.file.lock().await;
            file.flush().await?;
            file.sync_data().await
        })
    }
}

pub struct FileSinkFactory {
    name: String,
    format: StdoutFormat,
    message_field: String,
    file: Arc<Mutex<tokio::fs::File>>,
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
        let std_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let file = tokio::fs::File::from_std(std_file);
        Ok(Self {
            name,
            format,
            message_field,
            file: Arc::new(Mutex::new(file)),
            stats,
        })
    }
}

impl SinkFactory for FileSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        Ok(Box::new(FileSink::with_message_field(
            self.name.clone(),
            self.format,
            self.message_field.clone(),
            Arc::clone(&self.file),
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_use(&self) -> bool {
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
