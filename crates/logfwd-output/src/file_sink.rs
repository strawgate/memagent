use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use logfwd_types::diagnostics::ComponentStats;

use crate::BatchMetadata;
use crate::sink::{SendResult, Sink, SinkFactory};

use super::stdout::{StdoutFormat, StdoutSink};
use logfwd_types::field_names;

use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
use logfwd_config::RotationConfig;
/// Append-only file sink.
///
/// Uses the same row serialization logic as `StdoutSink` so `stdout` and
/// `file` stay behaviorally aligned.
use tokio::io::AsyncWrite;

type AsyncWriter = Arc<Mutex<Option<Pin<Box<dyn AsyncWrite + Send + Sync>>>>>;

pub struct FileSink {
    serializer: StdoutSink,
    path_template: String,
    append: bool,
    compression: String,
    delimiter: String,
    rotation: Option<RotationConfig>,
    current_path: String,
    current_file_size: u64,
    created_at: std::time::Instant,
    writer: AsyncWriter,
    output_buf: Vec<u8>,
    replaced_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl FileSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        format: StdoutFormat,
        path_template: String,
        append: bool,
        compression: String,
        rotation: Option<RotationConfig>,
        delimiter: String,
        current_path: String,
        writer: Option<Pin<Box<dyn AsyncWrite + Send + Sync>>>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self::with_message_field(
            name,
            format,
            field_names::BODY.to_string(),
            path_template,
            append,
            compression,
            rotation,
            delimiter,
            current_path,
            writer,
            stats,
        )
    }

    /// Create a file sink with a custom text/console message field fallback.
    ///
    /// The serializer prefers canonical `body` when present, then this
    /// configured field, then legacy aliases.
    #[allow(clippy::too_many_arguments)]
    pub fn with_message_field(
        name: String,
        format: StdoutFormat,
        message_field: String,
        path_template: String,
        append: bool,
        compression: String,
        rotation: Option<RotationConfig>,
        delimiter: String,
        current_path: String,
        writer: Option<Pin<Box<dyn AsyncWrite + Send + Sync>>>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let current_file_size = std::fs::metadata(&current_path)
            .map(|m| m.len())
            .unwrap_or(0);

        Self {
            serializer: StdoutSink::with_message_field(
                name,
                format,
                message_field,
                Arc::clone(&stats),
            ),
            path_template,
            append,
            compression,
            rotation,
            delimiter,
            current_path,
            writer: Arc::new(Mutex::new(writer)),
            output_buf: Vec::with_capacity(64 * 1024),
            replaced_buf: Vec::with_capacity(64 * 1024),
            current_file_size,
            created_at: std::time::Instant::now(),
            stats,
        }
    }

    fn create_writer(
        file: tokio::fs::File,
        compression: &str,
    ) -> Pin<Box<dyn AsyncWrite + Send + Sync>> {
        match compression.to_lowercase().as_str() {
            "gzip" => Box::pin(GzipEncoder::new(file)),
            "zstd" => Box::pin(ZstdEncoder::new(file)),
            _ => Box::pin(file),
        }
    }

    async fn ensure_open(&mut self, next_write_size: u64) -> io::Result<()> {
        let mut force_rotate = false;

        if let Some(rot) = &self.rotation {
            if let Some(max_age) = rot.max_age_seconds
                && self.created_at.elapsed().as_secs() >= max_age
            {
                force_rotate = true;
            }
            if let Some(max_size) = rot.max_size_bytes
                && self.current_file_size + next_write_size > max_size
            {
                force_rotate = true;
            }
        }

        let now = chrono::Utc::now();
        let mut expected_path = now.format(&self.path_template).to_string();

        if force_rotate && expected_path == self.current_path {
            expected_path = format!("{}.{}", expected_path, now.timestamp_millis());
        }

        let mut writer_guard = self.writer.lock().await;

        if expected_path != self.current_path || writer_guard.is_none() {
            let mut opts = tokio::fs::OpenOptions::new();
            opts.create(true).write(true);
            if self.append {
                opts.append(true);
            } else {
                opts.truncate(true);
            }

            let new_file = opts.open(&expected_path).await?;
            let meta = new_file.metadata().await?;

            if let Some(mut old_writer) = writer_guard.take()
                && let Err(e) = old_writer.shutdown().await
            {
                tracing::warn!("Failed to shutdown previous file writer cleanly: {}", e);
                // It's a best-effort shutdown as we're rotating away, but we should not fail the new open
            }

            *writer_guard = Some(Self::create_writer(new_file, &self.compression));
            self.current_path = expected_path.clone();
            self.current_file_size = meta.len();
            self.created_at = std::time::Instant::now();

            if let Some(rot) = &self.rotation
                && let Some(max_files) = rot.max_files
            {
                self.prune_old_files(max_files).await?;
            }
        }
        Ok(())
    }

    async fn prune_old_files(&self, max_files: u64) -> io::Result<()> {
        if max_files == 0 {
            return Ok(());
        }

        let path = std::path::Path::new(&self.path_template);
        let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));

        // Find a constant prefix for the files to avoid deleting unrelated files.
        let file_name = path.file_name().unwrap_or_default().to_string_lossy();
        let prefix = file_name.split('%').next().unwrap_or(&file_name);

        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(entries) => entries,
            Err(_) => return Ok(()),
        };
        let mut files = Vec::new();

        while let Ok(Some(entry)) = entries.next_entry().await {
            let entry_name = entry.file_name().to_string_lossy().into_owned();
            if !entry_name.starts_with(prefix) {
                continue;
            }

            if let Ok(meta) = entry.metadata().await
                && meta.is_file()
            {
                files.push((
                    entry.path(),
                    meta.modified().unwrap_or(std::time::SystemTime::now()),
                ));
            }
        }

        files.sort_by_key(|(_, time)| *time);

        if files.len() as u64 > max_files {
            let to_remove = files.len() as u64 - max_files;
            for (file_path, _) in files.into_iter().take(to_remove as usize) {
                let _ = tokio::fs::remove_file(file_path).await;
            }
        }

        Ok(())
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
            if let Err(e) = self
                .serializer
                .write_batch_to(batch, metadata, &mut self.output_buf)
            {
                return SendResult::from_io_error(e);
            }

            let lines_written = memchr::memchr_iter(b'\n', &self.output_buf).count() as u64;

            let bytes_written = if self.delimiter == "\n" {
                self.output_buf.len() as u64
            } else {
                self.replaced_buf.clear();
                let delim_bytes = self.delimiter.as_bytes();

                let mut start = 0;
                for idx in memchr::memchr_iter(b'\n', &self.output_buf) {
                    self.replaced_buf
                        .extend_from_slice(&self.output_buf[start..idx]);
                    self.replaced_buf.extend_from_slice(delim_bytes);
                    start = idx + 1;
                }
                if start < self.output_buf.len() {
                    self.replaced_buf
                        .extend_from_slice(&self.output_buf[start..]);
                }
                self.replaced_buf.len() as u64
            };

            if let Err(e) = self.ensure_open(bytes_written).await {
                return SendResult::from_io_error(e);
            }

            let mut writer_guard = self.writer.lock().await;
            if let Some(writer) = writer_guard.as_mut() {
                let buf = if self.delimiter == "\n" {
                    &self.output_buf
                } else {
                    &self.replaced_buf
                };
                if let Err(e) = writer.write_all(buf).await {
                    return SendResult::from_io_error(e);
                }
            }

            self.current_file_size += bytes_written;
            self.stats.inc_lines(lines_written);
            self.stats.inc_bytes(bytes_written);
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut writer_guard = self.writer.lock().await;
            if let Some(writer) = writer_guard.as_mut() {
                AsyncWriteExt::flush(writer).await?;
            }
            Ok(())
        })
    }

    fn name(&self) -> &str {
        self.serializer.name()
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut writer_guard = self.writer.lock().await;
            if let Some(mut writer) = writer_guard.take() {
                writer.shutdown().await?;
            }
            Ok(())
        })
    }
}

pub struct FileSinkFactory {
    name: String,
    format: StdoutFormat,
    path_template: String,
    append: bool,
    compression: String,
    rotation: Option<RotationConfig>,
    delimiter: String,
    message_field: String,
    current_path: String,
    writer: AsyncWriter,
    stats: Arc<ComponentStats>,
}

impl FileSinkFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        path_template: String,
        format: StdoutFormat,
        append: bool,
        compression: String,
        rotation: Option<RotationConfig>,
        delimiter: String,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::with_message_field(
            name,
            path_template,
            format,
            append,
            compression,
            rotation,
            delimiter,
            field_names::BODY.to_string(),
            stats,
        )
    }

    /// Create a file sink factory with a custom text/console message field fallback.
    #[allow(clippy::too_many_arguments)]
    pub fn with_message_field(
        name: String,
        path_template: String,
        format: StdoutFormat,
        append: bool,
        compression: String,
        rotation: Option<RotationConfig>,
        delimiter: String,
        message_field: String,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let now = chrono::Utc::now();
        let current_path = now.format(&path_template).to_string();

        // Delay file opening to create() since SinkFactory construction may occur in sync context
        Ok(Self {
            name,
            format,
            path_template,
            append,
            compression,
            rotation,
            delimiter,
            message_field,
            current_path,
            writer: Arc::new(Mutex::new(None)),
            stats,
        })
    }
}

impl SinkFactory for FileSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let writer = self.writer.try_lock().ok().and_then(|mut lock| lock.take());

        Ok(Box::new(FileSink::with_message_field(
            self.name.clone(),
            self.format,
            self.message_field.clone(),
            self.path_template.clone(),
            self.append,
            self.compression.clone(),
            self.rotation.clone(),
            self.delimiter.clone(),
            self.current_path.clone(),
            writer,
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
            "logfwd-output-{name}-{}-{unique}.tmp",
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
            true,
            "none".to_string(),
            None,
            "\n".to_string(),
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
            true,
            "none".to_string(),
            None,
            "\n".to_string(),
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
            true,
            "none".to_string(),
            None,
            "\n".to_string(),
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
