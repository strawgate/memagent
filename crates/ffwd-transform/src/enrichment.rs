//! Enrichment table providers — background-refreshable Arrow tables
//! that get registered in DataFusion alongside the `logs` table.
//!
//! Each provider produces an Arrow RecordBatch representing a lookup table.
//! The SqlTransform registers these as MemTables so users can JOIN against them.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::array::{StringArray, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ffwd_arrow::columnar::builder::ColumnarBatchBuilder;
use ffwd_arrow::columnar::plan::{BatchPlan, FieldKind};

use crate::TransformError;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A named lookup table whose contents may be refreshed in the background.
///
/// The pipeline reads a snapshot once per batch (cheap RwLock read), not per row.
pub trait EnrichmentTable: Send + Sync {
    /// Table name for SQL (e.g., "k8s_pods", "host_info").
    fn name(&self) -> &str;

    /// Current snapshot. Returns None if data hasn't been loaded yet.
    fn snapshot(&self) -> Option<RecordBatch>;
}

// ---------------------------------------------------------------------------
// Static table (from config)
// ---------------------------------------------------------------------------

/// A one-row table with fixed key-value pairs from the YAML config.
///
/// SQL: `SELECT logs.*, e.* FROM logs CROSS JOIN env AS e`
pub struct StaticTable {
    table_name: String,
    batch: RecordBatch,
}

impl StaticTable {
    /// Create from key-value pairs.
    ///
    /// Returns an error if `labels` is empty (a table with no columns is
    /// meaningless) or if building the Arrow batch fails (e.g. schema/column
    /// count mismatch).  Both schema and columns are derived from the same
    /// `labels` slice, so the batch error path is not expected to trigger in
    /// practice, but the error is propagated for defensive correctness.
    ///
    /// The error type is `TransformError` for consistency with the rest of this
    /// module (all enrichment loaders use `Result<_, TransformError>`).
    pub fn new(
        table_name: impl Into<String>,
        labels: &[(String, String)],
    ) -> Result<Self, TransformError> {
        if labels.is_empty() {
            return Err(TransformError::Enrichment(
                "StaticTable requires at least one label".to_string(),
            ));
        }
        let fields: Vec<Field> = labels
            .iter()
            .map(|(k, _)| Field::new(k, DataType::Utf8, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<Arc<dyn arrow::array::Array>> = labels
            .iter()
            .map(|(_, v)| Arc::new(StringArray::from(vec![v.as_str()])) as _)
            .collect();
        let batch = RecordBatch::try_new(schema, columns).map_err(TransformError::Arrow)?;
        Ok(StaticTable {
            table_name: table_name.into(),
            batch,
        })
    }
}

impl EnrichmentTable for StaticTable {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        Some(self.batch.clone())
    }
}

// ---------------------------------------------------------------------------
// Host info (resolved once at startup)
// ---------------------------------------------------------------------------

/// Column naming convention for host enrichment.
///
/// Mirrors `ffwd_config::HostInfoStyle` — the runtime wiring layer
/// converts from config to this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostInfoStyle {
    /// Flat column names (FastForward default).
    #[default]
    Raw,
    /// ECS/Beats-style dotted column names.
    Ecs,
    /// OpenTelemetry semantic convention column names.
    Otel,
}

/// Column names for each host info style.
struct HostInfoColumns {
    hostname: &'static str,
    os_type: &'static str,
    os_arch: &'static str,
    os_name: &'static str,
    os_family: &'static str,
    os_version: &'static str,
    os_kernel: &'static str,
    host_id: &'static str,
    boot_id: &'static str,
}

const RAW_COLUMNS: HostInfoColumns = HostInfoColumns {
    hostname: "hostname",
    os_type: "os_type",
    os_arch: "os_arch",
    os_name: "os_name",
    os_family: "os_family",
    os_version: "os_version",
    os_kernel: "os_kernel",
    host_id: "host_id",
    boot_id: "boot_id",
};

const ECS_COLUMNS: HostInfoColumns = HostInfoColumns {
    hostname: "host.hostname",
    os_type: "host.os.type",
    os_arch: "host.architecture",
    os_name: "host.os.name",
    os_family: "host.os.family",
    os_version: "host.os.version",
    os_kernel: "host.os.kernel",
    host_id: "host.id",
    boot_id: "host.boot.id",
};

const OTEL_COLUMNS: HostInfoColumns = HostInfoColumns {
    hostname: "host.name",
    os_type: "os.type",
    os_arch: "host.arch",
    os_name: "os.name",
    os_family: "os.family",
    os_version: "os.version",
    os_kernel: "os.kernel",
    host_id: "host.id",
    boot_id: "host.boot.id",
};

fn host_info_columns(style: HostInfoStyle) -> &'static HostInfoColumns {
    match style {
        HostInfoStyle::Raw => &RAW_COLUMNS,
        HostInfoStyle::Ecs => &ECS_COLUMNS,
        HostInfoStyle::Otel => &OTEL_COLUMNS,
    }
}

/// System host metadata. One row, resolved at construction time.
///
/// Column names depend on the chosen [`HostInfoStyle`]:
///
/// | Semantic | `raw` | `ecs` / `beats` | `otel` |
/// |----------|-------|-----------------|--------|
/// | hostname | `hostname` | `host.hostname` | `host.name` |
/// | OS type | `os_type` | `host.os.type` | `os.type` |
/// | architecture | `os_arch` | `host.architecture` | `host.arch` |
/// | OS name | `os_name` | `host.os.name` | `os.name` |
/// | OS family | `os_family` | `host.os.family` | `os.family` |
/// | OS version | `os_version` | `host.os.version` | `os.version` |
/// | kernel | `os_kernel` | `host.os.kernel` | `os.kernel` |
/// | machine id | `host_id` | `host.id` | `host.id` |
/// | boot id | `boot_id` | `host.boot.id` | `host.boot.id` |
///
/// On Linux, extended fields are read from `/etc/os-release`,
/// `/proc/sys/kernel/osrelease`, `/etc/machine-id`, and
/// `/proc/sys/kernel/random/boot_id`.  On other platforms they degrade to
/// empty strings.
pub struct HostInfoTable {
    batch: RecordBatch,
}

impl Default for HostInfoTable {
    fn default() -> Self {
        Self::with_style(HostInfoStyle::default())
    }
}

impl HostInfoTable {
    /// Snapshot host metadata using the default `raw` column naming style.
    pub fn new() -> Self {
        Self::with_style(HostInfoStyle::default())
    }

    /// Snapshot host metadata using the specified column naming style.
    pub fn with_style(style: HostInfoStyle) -> Self {
        let cols = host_info_columns(style);

        let hostname = gethostname::gethostname().to_string_lossy().into_owned();
        let os_type = std::env::consts::OS.to_string();
        let os_arch = std::env::consts::ARCH.to_string();

        let os_release = read_os_release();
        let os_name = os_release_field(&os_release, "PRETTY_NAME");
        let os_family = os_release_field(&os_release, "ID_LIKE")
            .or_else(|| os_release_field(&os_release, "ID"))
            .unwrap_or_default();
        let os_version = os_release_field(&os_release, "VERSION_ID").unwrap_or_default();
        let os_name = os_name.unwrap_or_default();

        let os_kernel = read_trimmed_file("/proc/sys/kernel/osrelease");
        let host_id = read_trimmed_file("/etc/machine-id");
        let boot_id = read_trimmed_file("/proc/sys/kernel/random/boot_id");

        let schema = Arc::new(Schema::new(vec![
            Field::new(cols.hostname, DataType::Utf8, false),
            Field::new(cols.os_type, DataType::Utf8, false),
            Field::new(cols.os_arch, DataType::Utf8, false),
            Field::new(cols.os_name, DataType::Utf8, false),
            Field::new(cols.os_family, DataType::Utf8, false),
            Field::new(cols.os_version, DataType::Utf8, false),
            Field::new(cols.os_kernel, DataType::Utf8, false),
            Field::new(cols.host_id, DataType::Utf8, false),
            Field::new(cols.boot_id, DataType::Utf8, false),
        ]));
        #[expect(
            clippy::expect_used,
            reason = "host_info schema is constructed in lockstep"
        )]
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![hostname.as_str()])),
                Arc::new(StringArray::from(vec![os_type.as_str()])),
                Arc::new(StringArray::from(vec![os_arch.as_str()])),
                Arc::new(StringArray::from(vec![os_name.as_str()])),
                Arc::new(StringArray::from(vec![os_family.as_str()])),
                Arc::new(StringArray::from(vec![os_version.as_str()])),
                Arc::new(StringArray::from(vec![os_kernel.as_str()])),
                Arc::new(StringArray::from(vec![host_id.as_str()])),
                Arc::new(StringArray::from(vec![boot_id.as_str()])),
            ],
        )
        .expect("host_info schema mismatch");

        HostInfoTable { batch }
    }
}

impl EnrichmentTable for HostInfoTable {
    fn name(&self) -> &'static str {
        "host_info"
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        Some(self.batch.clone())
    }
}

/// Read a file and return its trimmed contents, or empty string on failure.
fn read_trimmed_file(path: &str) -> String {
    std::fs::read_to_string(path)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// Read and parse `/etc/os-release` (or `/usr/lib/os-release` fallback)
/// into key-value pairs.
fn read_os_release() -> Vec<(String, String)> {
    let content = std::fs::read_to_string("/etc/os-release")
        .or_else(|_| std::fs::read_to_string("/usr/lib/os-release"))
        .unwrap_or_default();
    parse_os_release(&content)
}

/// Parse os-release file contents into key-value pairs.
fn parse_os_release(content: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        let val = val.trim();
        let val = if val.len() >= 2
            && ((val.starts_with('"') && val.ends_with('"'))
                || (val.starts_with('\'') && val.ends_with('\'')))
        {
            &val[1..val.len() - 1]
        } else {
            val
        };
        pairs.push((key.to_string(), val.to_string()));
    }
    pairs
}

/// Look up a field from parsed os-release key-value pairs.
fn os_release_field(pairs: &[(String, String)], key: &str) -> Option<String> {
    pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

// ---------------------------------------------------------------------------
// K8s pod metadata (parsed from CRI log file paths)
// ---------------------------------------------------------------------------

/// Extracts Kubernetes metadata from CRI log file paths.
///
/// CRI log path format:
///   `/var/log/pods/<namespace>_<pod-name>_<pod-uid>/<container>/0.log`
///
/// This is zero-cost enrichment — no K8s API calls needed.
pub struct K8sPathTable {
    table_name: String,
    data: Arc<RwLock<Option<RecordBatch>>>,
}

/// One parsed K8s pod entry from a CRI log path.
#[derive(Debug, Clone)]
pub struct K8sPodEntry {
    pub log_path_prefix: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub container_name: String,
}

impl K8sPathTable {
    /// Create an empty K8s path table. Call [`update_from_paths`](Self::update_from_paths)
    /// to populate with CRI log paths.
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        // Start with an empty batch so SQL queries don't fail with "table not found".
        let empty = build_k8s_batch(&[]);
        K8sPathTable {
            table_name,
            data: Arc::new(RwLock::new(Some(empty))),
        }
    }

    /// Parse CRI log paths and update the table.
    pub fn update_from_paths(&self, paths: &[String]) {
        let mut entries = Vec::new();
        for path in paths {
            if let Some(entry) = parse_cri_log_path(path) {
                entries.push(entry);
            }
        }
        entries.sort_by(|a, b| {
            (&a.namespace, &a.pod_name, &a.pod_uid, &a.container_name).cmp(&(
                &b.namespace,
                &b.pod_name,
                &b.pod_uid,
                &b.container_name,
            ))
        });
        entries.dedup_by(|a, b| {
            a.namespace == b.namespace
                && a.pod_name == b.pod_name
                && a.pod_uid == b.pod_uid
                && a.container_name == b.container_name
        });

        let batch = build_k8s_batch(&entries);
        *self
            .data
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
    }
}

impl EnrichmentTable for K8sPathTable {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        self.data
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Parse a CRI log path into K8s metadata.
pub fn parse_cri_log_path(path: &str) -> Option<K8sPodEntry> {
    let pods_idx = path.find("/pods/")?;
    let after_pods = &path[pods_idx + 6..];

    let slash_idx = after_pods.find('/')?;
    let pod_dir = &after_pods[..slash_idx];
    let after_pod_dir = &after_pods[slash_idx + 1..];

    // UID is always 36 chars (UUID). Work backwards from the end of pod_dir.
    if pod_dir.len() < 38 {
        return None;
    }
    let uid_start = pod_dir.len() - 36;
    if pod_dir.as_bytes().get(uid_start.wrapping_sub(1))? != &b'_' {
        return None;
    }
    let pod_uid = &pod_dir[uid_start..];
    let name_and_ns = &pod_dir[..uid_start - 1];

    // namespace_podname — first underscore separates them.
    let ns_end = name_and_ns.find('_')?;
    let namespace = &name_and_ns[..ns_end];
    let pod_name = &name_and_ns[ns_end + 1..];

    if namespace.is_empty() || pod_name.is_empty() {
        return None;
    }

    let container_end = after_pod_dir.find('/').unwrap_or(after_pod_dir.len());
    let container_name = &after_pod_dir[..container_end];

    if container_name.is_empty() {
        return None;
    }

    // Build prefix up to and including "container/". Always end with '/'.
    let prefix_end = pods_idx + 6 + slash_idx + 1 + container_end;
    let mut log_path_prefix = path[..prefix_end.min(path.len())].to_string();
    if !log_path_prefix.ends_with('/') {
        log_path_prefix.push('/');
    }

    Some(K8sPodEntry {
        log_path_prefix,
        namespace: namespace.to_string(),
        pod_name: pod_name.to_string(),
        pod_uid: pod_uid.to_string(),
        container_name: container_name.to_string(),
    })
}

fn build_k8s_batch(entries: &[K8sPodEntry]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("log_path_prefix", DataType::Utf8, false),
        Field::new("namespace", DataType::Utf8, false),
        Field::new("pod_name", DataType::Utf8, false),
        Field::new("pod_uid", DataType::Utf8, false),
        Field::new("container_name", DataType::Utf8, false),
    ]));

    let prefixes: Vec<&str> = entries.iter().map(|e| e.log_path_prefix.as_str()).collect();
    let namespaces: Vec<&str> = entries.iter().map(|e| e.namespace.as_str()).collect();
    let pod_names: Vec<&str> = entries.iter().map(|e| e.pod_name.as_str()).collect();
    let uids: Vec<&str> = entries.iter().map(|e| e.pod_uid.as_str()).collect();
    let containers: Vec<&str> = entries.iter().map(|e| e.container_name.as_str()).collect();

    #[expect(
        clippy::expect_used,
        reason = "k8s table schema is constructed in lockstep"
    )]
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(prefixes)),
            Arc::new(StringArray::from(namespaces)),
            Arc::new(StringArray::from(pod_names)),
            Arc::new(StringArray::from(uids)),
            Arc::new(StringArray::from(containers)),
        ],
    )
    .expect("k8s batch schema mismatch")
}

// ---------------------------------------------------------------------------
// File-based lookup table (CSV)
// ---------------------------------------------------------------------------

/// A lookup table loaded from a CSV file. All columns are nullable Utf8View.
/// Supports periodic refresh via `reload()`.
///
/// ```yaml
/// enrichment:
///   - type: csv
///     table_name: assets
///     path: /etc/ffwd/assets.csv
/// ```
///
/// SQL: `SELECT logs.*, a.owner FROM logs LEFT JOIN assets AS a ON logs.host_str = a.hostname`
pub struct CsvFileTable {
    table_name: String,
    path: PathBuf,
    data: Arc<RwLock<Option<RecordBatch>>>,
}

impl CsvFileTable {
    /// Create a CSV file table. Call `reload()` to load the data.
    pub fn new(table_name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        CsvFileTable {
            table_name: table_name.into(),
            path: path.into(),
            data: Arc::new(RwLock::new(None)),
        }
    }

    /// Load the file from a reader (useful for testing).
    ///
    /// The stored batch preserves CSV header order as nullable `Utf8View`
    /// columns. Empty cells are empty strings; missing trailing cells are nulls.
    pub fn load_from_reader<R: io::Read>(&self, reader: R) -> Result<usize, TransformError> {
        let batch = read_csv_to_batch(reader)?;
        let num_rows = batch.num_rows();
        *self
            .data
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
        Ok(num_rows)
    }

    /// Reload the CSV file from disk. Returns the number of rows loaded.
    pub fn reload(&self) -> Result<usize, TransformError> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            TransformError::Enrichment(format!("failed to open {}: {e}", self.path.display()))
        })?;
        self.load_from_reader(io::BufReader::new(file))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl EnrichmentTable for CsvFileTable {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        self.data
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Read a CSV into an Arrow RecordBatch through the shared columnar builder.
///
/// CSV parsing semantics stay here: delimiter/quote/header handling and row
/// alignment are owned by the CSV reader. `ColumnarBatchBuilder` owns string
/// storage, sparse padding, and Arrow finalization into nullable Utf8View
/// columns.
fn read_csv_to_batch<R: io::Read>(reader: R) -> Result<RecordBatch, TransformError> {
    let mut csv_reader = csv::ReaderBuilder::new().flexible(true).from_reader(reader);

    let headers = read_csv_headers(&mut csv_reader)?;

    let num_cols = headers.len();
    let mut plan = BatchPlan::with_capacity(num_cols);
    let handles = headers
        .iter()
        .map(|header| {
            plan.declare_planned(header, FieldKind::Utf8View)
                .map_err(|e| TransformError::Enrichment(format!("CSV plan error: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = ColumnarBatchBuilder::new(plan);
    builder.set_dedup_enabled(false);
    builder.begin_batch();

    for (row_idx, result) in csv_reader.records().enumerate() {
        let record =
            result.map_err(|e| TransformError::Enrichment(format!("CSV parse error: {e}")))?;
        if record.len() > num_cols {
            return Err(TransformError::Enrichment(format!(
                "CSV row {} has {} fields, expected {} (header count)",
                row_idx + 1,
                record.len(),
                num_cols,
            )));
        }

        builder.begin_row();
        // Missing trailing CSV cells are represented by leaving the field unwritten.
        for (idx, handle) in handles.iter().enumerate() {
            if let Some(field) = record.get(idx) {
                builder
                    .write_str(*handle, field)
                    .map_err(|e| TransformError::Enrichment(format!("CSV builder error: {e}")))?;
            }
        }
        builder.end_row();
    }

    let batch = builder
        .finish_batch()
        .map_err(|e| TransformError::Enrichment(format!("CSV builder error: {e}")))?;
    restore_csv_header_columns(&headers, batch)
}

fn restore_csv_header_columns(
    headers: &[String],
    batch: RecordBatch,
) -> Result<RecordBatch, TransformError> {
    let schema = batch.schema();
    let index_by_name: HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().as_str(), idx))
        .collect();

    let mut fields = Vec::with_capacity(headers.len());
    let mut arrays = Vec::with_capacity(headers.len());
    for header in headers {
        match index_by_name.get(header.as_str()).copied() {
            Some(idx) => {
                let field = schema.fields().get(idx).ok_or_else(|| {
                    TransformError::Enrichment(format!(
                        "CSV header '{header}' resolved to invalid column index {idx}"
                    ))
                })?;
                let array = batch.columns().get(idx).ok_or_else(|| {
                    TransformError::Enrichment(format!(
                        "CSV header '{header}' resolved to missing batch column {idx}"
                    ))
                })?;
                fields.push(Arc::clone(field));
                arrays.push(Arc::clone(array));
            }
            None => {
                fields.push(Arc::new(Field::new(header, DataType::Utf8View, true)));
                arrays.push(new_null_array(&DataType::Utf8View, batch.num_rows()));
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(TransformError::Arrow)
}

#[cfg(test)]
fn read_csv_to_legacy_batch_for_test<R: io::Read>(
    reader: R,
) -> Result<RecordBatch, TransformError> {
    let mut csv_reader = csv::ReaderBuilder::new().flexible(true).from_reader(reader);

    let headers = read_csv_headers(&mut csv_reader)?;

    let num_cols = headers.len();
    let mut columns: Vec<Vec<Option<String>>> = vec![Vec::new(); num_cols];

    for (row_idx, result) in csv_reader.records().enumerate() {
        let record =
            result.map_err(|e| TransformError::Enrichment(format!("CSV parse error: {e}")))?;
        if record.len() > num_cols {
            return Err(TransformError::Enrichment(format!(
                "CSV row {} has {} fields, expected {} (header count)",
                row_idx + 1,
                record.len(),
                num_cols,
            )));
        }
        for (i, field) in record.iter().enumerate() {
            columns[i].push(Some(field.to_string()));
        }
        for col in columns.iter_mut().skip(record.len()) {
            col.push(None);
        }
    }

    let fields: Vec<Field> = headers
        .iter()
        .map(|h| Field::new(h, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|col| {
            let arr: StringArray = col.iter().map(|s| s.as_deref()).collect();
            Arc::new(arr) as ArrayRef
        })
        .collect();

    RecordBatch::try_new(schema, arrays).map_err(TransformError::Arrow)
}

fn read_csv_headers<R: io::Read>(
    csv_reader: &mut csv::Reader<R>,
) -> Result<Vec<String>, TransformError> {
    let headers: Vec<String> = csv_reader
        .headers()
        .map_err(|e| TransformError::Enrichment(format!("CSV header error: {e}")))?
        .iter()
        .map(ToString::to_string)
        .collect();
    validate_csv_headers(&headers)?;
    Ok(headers)
}

fn validate_csv_headers(headers: &[String]) -> Result<(), TransformError> {
    if headers.is_empty() {
        return Err(TransformError::Enrichment("CSV has no columns".to_string()));
    }

    let mut seen = HashSet::with_capacity(headers.len());
    for h in headers {
        if h.is_empty() {
            return Err(TransformError::Enrichment(
                "CSV has an empty header name".to_string(),
            ));
        }
        if !seen.insert(h) {
            return Err(TransformError::Enrichment(format!(
                "CSV has duplicate header name: {h}"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// JSON Lines file-based lookup table
// ---------------------------------------------------------------------------

/// A lookup table loaded from a JSON Lines file (one JSON object per line).
/// Discovers columns from all rows (union schema). All values stored as Utf8.
///
/// ```yaml
/// enrichment:
///   - type: jsonl
///     table_name: ip_owners
///     path: /etc/ffwd/ip-owners.jsonl
/// ```
pub struct JsonLinesFileTable {
    table_name: String,
    path: PathBuf,
    data: Arc<RwLock<Option<RecordBatch>>>,
}

impl JsonLinesFileTable {
    /// Create a JSONL file table. Call [`reload`](Self::reload) to load data from disk.
    pub fn new(table_name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        JsonLinesFileTable {
            table_name: table_name.into(),
            path: path.into(),
            data: Arc::new(RwLock::new(None)),
        }
    }

    /// Load from a reader (useful for testing).
    pub fn load_from_reader<R: io::BufRead>(&self, reader: R) -> Result<usize, TransformError> {
        let batch = read_jsonl_to_batch(reader)?;
        let num_rows = batch.num_rows();
        *self
            .data
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
        Ok(num_rows)
    }

    /// Reload from disk.
    pub fn reload(&self) -> Result<usize, TransformError> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            TransformError::Enrichment(format!("failed to open {}: {e}", self.path.display()))
        })?;
        self.load_from_reader(io::BufReader::new(file))
    }
}

impl EnrichmentTable for JsonLinesFileTable {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        self.data
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Read JSON Lines into a RecordBatch. Discovers union schema from all rows.
/// All values are stored as Utf8 (stringified).
fn read_jsonl_to_batch<R: io::BufRead>(reader: R) -> Result<RecordBatch, TransformError> {
    use std::collections::BTreeMap;

    // First pass: discover all keys and collect rows.
    let mut all_keys: Vec<String> = Vec::new();
    let mut key_set = HashSet::new();
    let mut rows: Vec<BTreeMap<String, String>> = Vec::new();

    for line in reader.lines() {
        let line =
            line.map_err(|e| TransformError::Enrichment(format!("JSONL read error: {e}")))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Minimal JSON object parsing — extract top-level string key-value pairs.
        // We use serde_json-style parsing but store everything as strings.
        let obj: BTreeMap<String, serde_json::Value> = serde_json::from_str(trimmed)
            .map_err(|e| TransformError::Enrichment(format!("JSONL parse error: {e}")))?;

        let mut row = BTreeMap::new();
        for (k, v) in obj {
            if key_set.insert(k.clone()) {
                all_keys.push(k.clone());
            }
            // Stringify all values.
            let s = match v {
                serde_json::Value::String(s) => s,
                serde_json::Value::Null => continue,
                other => other.to_string(),
            };
            row.insert(k, s);
        }
        rows.push(row);
    }

    if all_keys.is_empty() {
        return Err(TransformError::Enrichment(
            "JSONL has no fields".to_string(),
        ));
    }

    // Build columnar arrays.
    let fields: Vec<Field> = all_keys
        .iter()
        .map(|k| Field::new(k, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<Arc<dyn arrow::array::Array>> = all_keys
        .iter()
        .map(|key| {
            let arr: StringArray = rows
                .iter()
                .map(|row| row.get(key).map(String::as_str))
                .collect();
            Arc::new(arr) as _
        })
        .collect();

    RecordBatch::try_new(schema, arrays).map_err(TransformError::Arrow)
}

// ---------------------------------------------------------------------------
// Geo-IP database (IP-to-location lookup)
// ---------------------------------------------------------------------------

/// Result of a geo-IP address lookup.
///
/// All fields are optional — not every database contains every field, and
/// private-range or malformed IPs will produce `None` for all fields.
#[derive(Debug, Clone, Default)]
pub struct GeoResult {
    /// Two-letter ISO 3166-1 country code, e.g. `"US"`.
    pub country_code: Option<String>,
    /// Full English country name, e.g. `"United States"`.
    pub country_name: Option<String>,
    /// City name, e.g. `"Seattle"`.
    pub city: Option<String>,
    /// Region / state / subdivision name, e.g. `"Washington"`.
    pub region: Option<String>,
    /// Latitude in decimal degrees.
    pub latitude: Option<f64>,
    /// Longitude in decimal degrees.
    pub longitude: Option<f64>,
    /// Autonomous System Number.
    pub asn: Option<i64>,
    /// Organisation name associated with the ASN.
    pub org: Option<String>,
}

/// Pluggable backend for geo-IP lookups.
///
/// Implementations wrap a specific database format (MMDB, CSV, IP2Location, …).
/// The UDF holds an `Arc<dyn GeoDatabase>` so the same reader is shared across
/// all batch executions without reloading.
///
/// Returns `None` for any IP that cannot be resolved — private ranges,
/// malformed strings, or addresses absent from the database.
pub trait GeoDatabase: Send + Sync {
    /// Look up geographic location for an IP address string.
    ///
    /// Returns `None` for unresolvable IPs (private ranges, malformed, not in DB).
    fn lookup(&self, ip: &str) -> Option<GeoResult>;
}

// ---------------------------------------------------------------------------
// Reloadable geo database
// ---------------------------------------------------------------------------

/// Wraps a [`GeoDatabase`] with an atomic hot-swap handle.
///
/// The hot path takes a cheap shared read lock.  The reload task takes an
/// exclusive write lock only to swap the `Arc` pointer — the critical section
/// is a pointer copy, never an I/O operation.
///
/// ```rust,ignore
/// let db = Arc::new(MmdbDatabase::open(path)?);
/// let reloadable = Arc::new(ReloadableGeoDb::new(db));
/// let handle = reloadable.reload_handle();
///
/// tokio::spawn(async move {
///     let mut ticker = tokio::time::interval(Duration::from_secs(interval));
///     ticker.tick().await; // skip first immediate tick
///     loop {
///         ticker.tick().await;
///         if let Ok(new_db) = MmdbDatabase::open(&path) {
///             handle.replace(Arc::new(new_db));
///         }
///     }
/// });
/// ```
pub struct ReloadableGeoDb {
    inner: Arc<RwLock<Arc<dyn GeoDatabase>>>,
}

impl ReloadableGeoDb {
    /// Wrap an existing database.
    pub fn new(db: Arc<dyn GeoDatabase>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(db)),
        }
    }

    /// Return a lightweight handle for driving background reloads.
    pub fn reload_handle(&self) -> GeoReloadHandle {
        GeoReloadHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl GeoDatabase for ReloadableGeoDb {
    fn lookup(&self, ip: &str) -> Option<GeoResult> {
        let db = Arc::clone(
            &self
                .inner
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        // Lock is dropped here; the lookup runs without blocking writers.
        db.lookup(ip)
    }
}

/// A cloneable handle used to atomically replace the active database.
#[derive(Clone)]
pub struct GeoReloadHandle {
    inner: Arc<RwLock<Arc<dyn GeoDatabase>>>,
}

impl GeoReloadHandle {
    /// Atomically swap in a new database.  Safe to call from any thread.
    pub fn replace(&self, db: Arc<dyn GeoDatabase>) {
        *self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = db;
    }
}

// ---------------------------------------------------------------------------
// Environment variable enrichment table
// ---------------------------------------------------------------------------

/// A one-row enrichment table populated from environment variables matching a
/// name prefix.  The prefix is stripped and the remainder lower-cased to form
/// column names.
///
/// Useful for injecting deployment metadata that is already baked into the pod
/// environment without needing a separate config file.
///
/// ```yaml
/// enrichment:
///   - type: env_vars
///     table_name: deploy_meta
///     prefix: FFWD_META_
/// ```
///
/// With `FFWD_META_CLUSTER=prod` and `FFWD_META_REGION=us-east-1` set,
/// the table exposes `cluster` and `region` columns.
///
/// SQL: `SELECT logs.*, m.cluster, m.region FROM logs CROSS JOIN deploy_meta AS m`
pub struct EnvTable {
    table_name: String,
    batch: RecordBatch,
}

impl EnvTable {
    /// Build the table from all environment variables whose names begin with
    /// `prefix`.
    ///
    /// Returns an error if no matching variables are found, since an empty
    /// table would be misleading to wire into the pipeline.
    pub fn from_prefix(
        table_name: impl Into<String>,
        prefix: &str,
    ) -> Result<Self, TransformError> {
        let mut pairs: Vec<(String, String)> = std::env::vars_os()
            .filter_map(|(k, v)| {
                let k_str = k.to_str()?; // skip non-UTF8 keys
                let v_str = v.to_str()?; // skip non-UTF8 values
                k_str
                    .strip_prefix(prefix)
                    .filter(|s| !s.is_empty()) // skip exact prefix match (empty column name)
                    .map(|stripped| (stripped.to_lowercase(), v_str.to_owned()))
            })
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        if pairs.is_empty() {
            return Err(TransformError::Enrichment(format!(
                "EnvTable: no environment variables found with prefix '{prefix}'"
            )));
        }

        // Reject duplicate column names after lowercase normalization (e.g.
        // FFWD_META_REGION and FFWD_META_region both present would collide).
        for window in pairs.windows(2) {
            if let [first, second] = window
                && first.0 == second.0
            {
                return Err(TransformError::Enrichment(format!(
                    "EnvTable: duplicate column name '{}' after lowercasing (prefix '{prefix}')",
                    first.0
                )));
            }
        }

        let fields: Vec<Field> = pairs
            .iter()
            .map(|(k, _)| Field::new(k.as_str(), DataType::Utf8, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<Arc<dyn arrow::array::Array>> = pairs
            .iter()
            .map(|(_, v)| Arc::new(StringArray::from(vec![v.as_str()])) as _)
            .collect();
        let batch = RecordBatch::try_new(schema, columns).map_err(TransformError::Arrow)?;

        Ok(EnvTable {
            table_name: table_name.into(),
            batch,
        })
    }
}

impl EnrichmentTable for EnvTable {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        Some(self.batch.clone())
    }
}

// ---------------------------------------------------------------------------
// Process info (agent self-metadata, resolved once at startup)
// ---------------------------------------------------------------------------

/// Agent self-metadata table.  One row, resolved at construction time.
///
/// Columns: `agent_name`, `agent_version`, `pid`, `start_time`
/// Note: `start_time` captures the moment this table is constructed
/// (during pipeline startup), not the exact process start time.
///
/// ```yaml
/// enrichment:
///   - type: process_info
/// ```
///
/// SQL: `SELECT l.*, a.agent_version FROM logs l CROSS JOIN process_info a`
pub struct ProcessInfoTable {
    batch: RecordBatch,
}

impl Default for ProcessInfoTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessInfoTable {
    /// Snapshot agent self-metadata: name ("ffwd"), version, PID, start time.
    pub fn new() -> Self {
        let agent_name = "ffwd";
        let agent_version = env!("CARGO_PKG_VERSION");
        let pid = std::process::id().to_string();
        let start_time = {
            use std::time::{SystemTime, UNIX_EPOCH};
            let dur = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            // ISO 8601 UTC — good enough without pulling in chrono.
            let secs = dur.as_secs();
            let days = secs / 86400;
            let rem = secs % 86400;
            let hours = rem / 3600;
            let mins = (rem % 3600) / 60;
            let s = rem % 60;
            // Epoch day 0 = 1970-01-01. Simple civil-date conversion.
            let (y, m, d) = epoch_days_to_ymd(days as i64);
            format!("{y:04}-{m:02}-{d:02}T{hours:02}:{mins:02}:{s:02}Z")
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("agent_name", DataType::Utf8, false),
            Field::new("agent_version", DataType::Utf8, false),
            Field::new("pid", DataType::Utf8, false),
            Field::new("start_time", DataType::Utf8, false),
        ]));
        #[expect(
            clippy::expect_used,
            reason = "process_info schema is constructed in lockstep"
        )]
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![agent_name])),
                Arc::new(StringArray::from(vec![agent_version])),
                Arc::new(StringArray::from(vec![pid.as_str()])),
                Arc::new(StringArray::from(vec![start_time.as_str()])),
            ],
        )
        .expect("process_info schema mismatch");

        ProcessInfoTable { batch }
    }
}

impl EnrichmentTable for ProcessInfoTable {
    fn name(&self) -> &'static str {
        "process_info"
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        Some(self.batch.clone())
    }
}

/// Convert days since Unix epoch to (year, month, day).
fn epoch_days_to_ymd(days: i64) -> (i64, u32, u32) {
    // Algorithm from Howard Hinnant's chrono-compatible date library.
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Key-value file enrichment table
// ---------------------------------------------------------------------------

/// A one-row enrichment table populated from a `KEY=value` properties file.
///
/// Supported syntax:
/// - `KEY=value` — unquoted value (leading/trailing whitespace stripped)
/// - `KEY="quoted value"` — double-quoted value (quotes removed)
/// - `KEY='quoted value'` — single-quoted value (quotes removed)
/// - `# comment` and blank lines are ignored
///
/// Column names are the keys, lower-cased.  Reloadable via `reload()`.
///
/// ```yaml
/// enrichment:
///   - type: kv_file
///     table_name: os_release
///     path: /etc/os-release
///     refresh_interval: 3600
/// ```
pub struct KvFileTable {
    table_name: String,
    path: PathBuf,
    data: Arc<RwLock<Option<RecordBatch>>>,
}

impl KvFileTable {
    /// Create a KV file table. Call [`reload`](Self::reload) to load data from disk.
    pub fn new(table_name: impl Into<String>, path: &Path) -> Self {
        KvFileTable {
            table_name: table_name.into(),
            path: path.to_path_buf(),
            data: Arc::new(RwLock::new(None)),
        }
    }

    /// (Re)load the file from disk.  Returns the number of columns parsed.
    pub fn reload(&self) -> Result<usize, TransformError> {
        let pairs = parse_kv_file(&self.path)?;
        if pairs.is_empty() {
            return Err(TransformError::Enrichment(format!(
                "KvFileTable '{}': no key-value pairs found in '{}'",
                self.table_name,
                self.path.display()
            )));
        }
        let n = pairs.len();
        let batch = kv_pairs_to_batch(&pairs)?;
        *self
            .data
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
        Ok(n)
    }
}

impl EnrichmentTable for KvFileTable {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        self.data
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Parse a `KEY=value` file into sorted key-value pairs.
fn parse_kv_file(path: &Path) -> Result<Vec<(String, String)>, TransformError> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        TransformError::Enrichment(format!("failed to read '{}': {e}", path.display()))
    })?;
    let mut pairs = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((key, raw_val)) = trimmed.split_once('=') {
            let key = key.trim().to_lowercase();
            if key.is_empty() {
                continue;
            }
            let val = raw_val.trim();
            let val = if val.len() > 1
                && ((val.starts_with('"') && val.ends_with('"'))
                    || (val.starts_with('\'') && val.ends_with('\'')))
            {
                &val[1..val.len() - 1]
            } else {
                val
            };
            pairs.push((key, val.to_string()));
        }
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    for window in pairs.windows(2) {
        if let [first, second] = window
            && first.0 == second.0
        {
            return Err(TransformError::Enrichment(format!(
                "duplicate key '{}' in '{}'",
                first.0,
                path.display()
            )));
        }
    }
    Ok(pairs)
}

/// Build a one-row RecordBatch from key-value pairs.
fn kv_pairs_to_batch(pairs: &[(String, String)]) -> Result<RecordBatch, TransformError> {
    let fields: Vec<Field> = pairs
        .iter()
        .map(|(k, _)| Field::new(k.as_str(), DataType::Utf8, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<Arc<dyn arrow::array::Array>> = pairs
        .iter()
        .map(|(_, v)| Arc::new(StringArray::from(vec![v.as_str()])) as _)
        .collect();
    RecordBatch::try_new(schema, columns).map_err(TransformError::Arrow)
}

// ---------------------------------------------------------------------------
// Network info (resolved once at startup)
// ---------------------------------------------------------------------------

/// Network interface metadata.  One row, resolved at construction time.
///
/// Columns: `hostname`, `primary_ipv4`, `primary_ipv6`, `all_ipv4`, `all_ipv6`
///
/// IP addresses are discovered from `/proc/net/fib_trie` (IPv4) and
/// `/proc/net/if_inet6` (IPv6) on Linux.  On non-Linux systems the table
/// still provides `hostname` and empty IP columns.
///
/// ```yaml
/// enrichment:
///   - type: network_info
/// ```
pub struct NetworkInfoTable {
    batch: RecordBatch,
}

impl Default for NetworkInfoTable {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkInfoTable {
    /// Snapshot network metadata: hostname, IP addresses (sorted lexicographically).
    ///
    /// `primary_ipv4` / `primary_ipv6` are the lexicographically first non-loopback
    /// addresses. On multihomed hosts this may not match the default-route interface;
    /// use `all_ipv4` / `all_ipv6` if you need full coverage.
    pub fn new() -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().into_owned();

        let (ipv4_addrs, ipv6_addrs) = discover_local_ips();

        let primary_ipv4 = ipv4_addrs.first().cloned().unwrap_or_default();
        let primary_ipv6 = ipv6_addrs.first().cloned().unwrap_or_default();
        let all_ipv4 = ipv4_addrs.join(",");
        let all_ipv6 = ipv6_addrs.join(",");

        let schema = Arc::new(Schema::new(vec![
            Field::new("hostname", DataType::Utf8, false),
            Field::new("primary_ipv4", DataType::Utf8, false),
            Field::new("primary_ipv6", DataType::Utf8, false),
            Field::new("all_ipv4", DataType::Utf8, false),
            Field::new("all_ipv6", DataType::Utf8, false),
        ]));
        #[expect(
            clippy::expect_used,
            reason = "network_info schema is constructed in lockstep"
        )]
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![hostname.as_str()])),
                Arc::new(StringArray::from(vec![primary_ipv4.as_str()])),
                Arc::new(StringArray::from(vec![primary_ipv6.as_str()])),
                Arc::new(StringArray::from(vec![all_ipv4.as_str()])),
                Arc::new(StringArray::from(vec![all_ipv6.as_str()])),
            ],
        )
        .expect("network_info schema mismatch");

        NetworkInfoTable { batch }
    }
}

impl EnrichmentTable for NetworkInfoTable {
    fn name(&self) -> &'static str {
        "network_info"
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        Some(self.batch.clone())
    }
}

/// Discover non-loopback IPv4 and IPv6 addresses from procfs.
/// Returns `(ipv4_addrs, ipv6_addrs)`, each sorted.
fn discover_local_ips() -> (Vec<String>, Vec<String>) {
    let mut v4 = discover_ipv4_from_proc();
    let mut v6 = discover_ipv6_from_proc();
    v4.sort();
    v4.dedup();
    v6.sort();
    v6.dedup();
    (v4, v6)
}

/// Read IPv4 addresses from `/proc/net/fib_trie`.
///
/// We look for `/32 host LOCAL` entries and filter out `127.*`.
fn discover_ipv4_from_proc() -> Vec<String> {
    let Ok(content) = std::fs::read_to_string("/proc/net/fib_trie") else {
        return Vec::new();
    };
    let mut addrs = Vec::new();
    let mut prev_line = "";
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed == "/32 host LOCAL" {
            // The previous line has the IP like "  |-- 10.0.0.1"
            if let Some(ip) = prev_line.trim().strip_prefix("|-- ")
                && !ip.starts_with("127.")
            {
                addrs.push(ip.to_string());
            }
        }
        prev_line = trimmed;
    }
    addrs
}

/// Read IPv6 addresses from `/proc/net/if_inet6`.
///
/// Format: `<hex32> <idx> <prefix_len> <scope> <flags> <iface>`
/// Scope 0x20 = link-local; we skip those and loopback (::1).
fn discover_ipv6_from_proc() -> Vec<String> {
    let Ok(content) = std::fs::read_to_string("/proc/net/if_inet6") else {
        return Vec::new();
    };
    let mut addrs = Vec::new();
    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 4 {
            continue;
        }
        let Some(hex) = parts.first() else {
            continue;
        };
        let Some(scope) = parts.get(3) else {
            continue;
        };
        // Skip link-local (scope 20) and loopback (scope 10)
        if *scope == "20" || *scope == "10" {
            continue;
        }
        if hex.len() == 32
            && let Some(formatted) = format_ipv6_hex(hex)
        {
            addrs.push(formatted);
        }
    }
    addrs
}

/// Format 32-char hex string into standard IPv6 notation.
fn format_ipv6_hex(hex: &str) -> Option<String> {
    if hex.len() != 32 {
        return None;
    }
    let mut groups = Vec::with_capacity(8);
    for i in 0..8 {
        let start = i * 4;
        let group = &hex[start..start + 4];
        // Strip leading zeros for compactness
        let stripped = group.trim_start_matches('0');
        groups.push(if stripped.is_empty() {
            "0".to_string()
        } else {
            stripped.to_string()
        });
    }
    Some(groups.join(":"))
}

// ---------------------------------------------------------------------------
// Container info (resolved once at startup)
// ---------------------------------------------------------------------------

/// Container runtime detection.  One row, resolved at construction time.
///
/// Columns: `container_id`, `container_runtime`
///
/// Detection sources:
/// - `/.dockerenv` presence → runtime = "docker"
/// - `/proc/self/cgroup` parsing for container ID and runtime
/// - `/proc/self/mountinfo` parsing for cgroup v2 pure mode
///
/// Possible `container_runtime` values: `docker`, `containerd`, `cri-o`,
/// `kubernetes` (kubepods without specific runtime), `unknown`, or empty
/// string (not in a container).
///
/// ```yaml
/// enrichment:
///   - type: container_info
/// ```
pub struct ContainerInfoTable {
    batch: RecordBatch,
}

impl Default for ContainerInfoTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainerInfoTable {
    /// Detect container runtime and ID from `/proc/self/cgroup` and `/proc/self/mountinfo`.
    pub fn new() -> Self {
        let (container_id, container_runtime) = detect_container();

        let schema = Arc::new(Schema::new(vec![
            Field::new("container_id", DataType::Utf8, false),
            Field::new("container_runtime", DataType::Utf8, false),
        ]));
        #[expect(
            clippy::expect_used,
            reason = "container_info schema is constructed in lockstep"
        )]
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![container_id.as_str()])),
                Arc::new(StringArray::from(vec![container_runtime.as_str()])),
            ],
        )
        .expect("container_info schema mismatch");

        ContainerInfoTable { batch }
    }
}

impl EnrichmentTable for ContainerInfoTable {
    fn name(&self) -> &'static str {
        "container_info"
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        Some(self.batch.clone())
    }
}

/// Detect container runtime and extract container ID.
/// Returns `(container_id, runtime)`.
fn detect_container() -> (String, String) {
    // Try /proc/self/cgroup (works for cgroup v1 and hybrid v2)
    if let Ok(content) = std::fs::read_to_string("/proc/self/cgroup")
        && let Some((id, runtime)) = parse_cgroup_for_container(&content)
    {
        return (id, runtime);
    }

    // Try /proc/self/mountinfo for cgroup v2 pure mode
    if let Ok(content) = std::fs::read_to_string("/proc/self/mountinfo")
        && let Some((id, runtime)) = parse_mountinfo_for_container(&content)
    {
        return (id, runtime);
    }

    // Fallback: check for /.dockerenv
    if Path::new("/.dockerenv").exists() {
        return (String::new(), "docker".to_string());
    }

    (String::new(), String::new())
}

/// Parse cgroup file for container ID.
/// Cgroup v1 lines look like: `12:memory:/docker/<container-id>`
/// Cgroup v2 lines look like: `0::/system.slice/containerd-<id>.scope`
fn parse_cgroup_for_container(content: &str) -> Option<(String, String)> {
    for line in content.lines() {
        let path = line.rsplit_once(':').map(|(_, p)| p);
        let Some(path) = path else { continue };

        // Docker: /docker/<64-hex-chars> or /docker/buildkit/...
        if let Some(rest) = path.strip_prefix("/docker/") {
            let id = rest.split('/').next().unwrap_or("");
            if is_hex_container_id(id) {
                return Some((id.to_string(), "docker".to_string()));
            }
        }

        // Docker cgroup v2: docker-<id>.scope (e.g. /system.slice/docker-<id>.scope)
        if let Some(seg) = path.rsplit('/').find(|s| {
            s.starts_with("docker-")
                && Path::new(s)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("scope"))
        }) {
            // Strip prefix and extension case-insensitively via rsplit_once.
            let inner = seg
                .strip_prefix("docker-")
                .and_then(|s| s.rsplit_once('.').map(|(base, _)| base))
                .unwrap_or("");
            if is_hex_container_id(inner) {
                return Some((inner.to_string(), "docker".to_string()));
            }
        }

        // Containerd (K8s): /kubepods/...<64-hex-chars> or containerd-<id>.scope
        if (path.contains("/kubepods") || path.contains("containerd-"))
            && let Some(id) = extract_hex_id_from_path(path)
        {
            let runtime = if path.contains("containerd") {
                "containerd"
            } else {
                "kubernetes"
            };
            return Some((id, runtime.to_string()));
        }

        // CRI-O: /crio-<64-hex-chars>
        if let Some(rest) = path
            .strip_prefix("/crio-")
            .or_else(|| path.rsplit_once("/crio-").map(|(_, r)| r))
        {
            let id = rest.split('.').next().unwrap_or(rest);
            if is_hex_container_id(id) {
                return Some((id.to_string(), "cri-o".to_string()));
            }
        }
    }
    None
}

/// Parse mountinfo for container ID in cgroup v2 pure mode.
fn parse_mountinfo_for_container(content: &str) -> Option<(String, String)> {
    for line in content.lines() {
        if !line.contains("cgroup") {
            continue;
        }
        if let Some(id) = extract_hex_id_from_path(line) {
            let runtime = if line.contains("docker") {
                "docker"
            } else if line.contains("containerd") {
                "containerd"
            } else if line.contains("crio") {
                "cri-o"
            } else {
                "unknown"
            };
            return Some((id, runtime.to_string()));
        }
    }
    None
}

/// Check if a string looks like a 64-char hex container ID.
fn is_hex_container_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Extract a 64-char hex ID from anywhere in a path string.
fn extract_hex_id_from_path(path: &str) -> Option<String> {
    // Walk through segments separated by / or -
    for segment in path.split(['/', '-']) {
        let segment = segment.split('.').next().unwrap_or(segment);
        if is_hex_container_id(segment) {
            return Some(segment.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// K8s cluster info (resolved once at startup from downward API)
// ---------------------------------------------------------------------------

/// Kubernetes cluster metadata from the downward API and mounted secrets.
///
/// Columns: `namespace`, `pod_name`, `node_name`, `service_account`,
/// `cluster_name`
///
/// Detection sources:
/// - `KUBERNETES_SERVICE_HOST` env var (presence confirms K8s)
/// - `/var/run/secrets/kubernetes.io/serviceaccount/namespace`
/// - `HOSTNAME` env var (pod name in K8s)
/// - `K8S_NODE_NAME`, `NODE_NAME` env vars (set via downward API fieldRef)
/// - `K8S_CLUSTER_NAME`, `CLUSTER_NAME` env vars
///
/// If not running in Kubernetes, all columns are empty strings.
///
/// ```yaml
/// enrichment:
///   - type: k8s_cluster_info
/// ```
pub struct K8sClusterInfoTable {
    batch: RecordBatch,
}

impl Default for K8sClusterInfoTable {
    fn default() -> Self {
        Self::new()
    }
}

impl K8sClusterInfoTable {
    /// Read Kubernetes cluster metadata from the downward API environment variables
    /// and service account token path.
    pub fn new() -> Self {
        let in_k8s = std::env::var("KUBERNETES_SERVICE_HOST").is_ok();

        let namespace = if in_k8s {
            std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .unwrap_or_default()
                .trim()
                .to_string()
        } else {
            String::new()
        };

        let pod_name = if in_k8s {
            std::env::var("HOSTNAME").unwrap_or_default()
        } else {
            String::new()
        };

        let node_name = if in_k8s {
            std::env::var("K8S_NODE_NAME")
                .or_else(|_| std::env::var("NODE_NAME"))
                .unwrap_or_default()
        } else {
            String::new()
        };

        let service_account = if in_k8s {
            // Try the standard downward API env vars.
            std::env::var("K8S_SERVICE_ACCOUNT")
                .or_else(|_| std::env::var("SERVICE_ACCOUNT"))
                .unwrap_or_default()
        } else {
            String::new()
        };

        let cluster_name = if in_k8s {
            std::env::var("K8S_CLUSTER_NAME")
                .or_else(|_| std::env::var("CLUSTER_NAME"))
                .unwrap_or_default()
        } else {
            String::new()
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("pod_name", DataType::Utf8, false),
            Field::new("node_name", DataType::Utf8, false),
            Field::new("service_account", DataType::Utf8, false),
            Field::new("cluster_name", DataType::Utf8, false),
        ]));
        #[expect(
            clippy::expect_used,
            reason = "k8s_cluster_info schema is constructed in lockstep"
        )]
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![namespace.as_str()])),
                Arc::new(StringArray::from(vec![pod_name.as_str()])),
                Arc::new(StringArray::from(vec![node_name.as_str()])),
                Arc::new(StringArray::from(vec![service_account.as_str()])),
                Arc::new(StringArray::from(vec![cluster_name.as_str()])),
            ],
        )
        .expect("k8s_cluster_info schema mismatch");

        K8sClusterInfoTable { batch }
    }
}

impl EnrichmentTable for K8sClusterInfoTable {
    fn name(&self) -> &'static str {
        "k8s_cluster_info"
    }

    fn snapshot(&self) -> Option<RecordBatch> {
        Some(self.batch.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn csv_string_column<'a>(
        batch: &'a RecordBatch,
        name: &str,
    ) -> &'a arrow::array::StringViewArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing CSV column: {name}"))
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap_or_else(|| panic!("CSV column {name} should be Utf8View"))
    }

    // -- CRI path parsing ---------------------------------------------------

    #[test]
    fn parse_standard_cri_path() {
        let path = "/var/log/pods/default_myapp-abc-12345_a1b2c3d4-e5f6-7890-abcd-ef1234567890/nginx/0.log";
        let entry = parse_cri_log_path(path).expect("should parse");
        assert_eq!(entry.namespace, "default");
        assert_eq!(entry.pod_name, "myapp-abc-12345");
        assert_eq!(entry.pod_uid, "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert_eq!(entry.container_name, "nginx");
        assert!(entry.log_path_prefix.ends_with("nginx/"));
    }

    #[test]
    fn parse_kube_system_path() {
        let path = "/var/log/pods/kube-system_coredns-5d78c9869d-abc12_f1e2d3c4-b5a6-9870-fedc-ba0987654321/coredns/0.log";
        let entry = parse_cri_log_path(path).expect("should parse");
        assert_eq!(entry.namespace, "kube-system");
        assert_eq!(entry.pod_name, "coredns-5d78c9869d-abc12");
        assert_eq!(entry.container_name, "coredns");
    }

    #[test]
    fn parse_hyphenated_namespace() {
        let path = "/var/log/pods/my-team-prod_api-server-xyz_abcdefab-1234-5678-9abc-def012345678/app/0.log";
        let entry = parse_cri_log_path(path).expect("should parse");
        // Hyphens in namespace are fine — only underscores separate ns from pod name.
        assert_eq!(entry.namespace, "my-team-prod");
        assert_eq!(entry.pod_name, "api-server-xyz");
    }

    #[test]
    fn parse_pod_name_with_underscores() {
        // Pod names can contain underscores in some edge cases (StatefulSets, etc.)
        // Our parser uses the FIRST underscore as ns/pod separator.
        let path =
            "/var/log/pods/ns_pod_with_underscores_abcdefab-1234-5678-9abc-def012345678/c/0.log";
        let entry = parse_cri_log_path(path).expect("should parse");
        assert_eq!(entry.namespace, "ns");
        assert_eq!(entry.pod_name, "pod_with_underscores");
    }

    #[test]
    fn parse_invalid_paths() {
        assert!(parse_cri_log_path("/var/log/messages").is_none());
        assert!(parse_cri_log_path("/var/log/pods/").is_none());
        assert!(parse_cri_log_path("/var/log/pods/short/container/0.log").is_none());
        assert!(parse_cri_log_path("").is_none());
        assert!(parse_cri_log_path("/var/log/pods/a/b/0.log").is_none()); // no valid UUID
    }

    // -- Static table -------------------------------------------------------

    #[test]
    fn static_table_one_row() {
        let table = StaticTable::new(
            "env",
            &[
                ("environment".to_string(), "production".to_string()),
                ("cluster".to_string(), "us-east-1".to_string()),
            ],
        )
        .expect("valid labels");
        assert_eq!(table.name(), "env");
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
        let env = batch
            .column_by_name("environment")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(env.value(0), "production");
    }

    #[test]
    fn static_table_single_label() {
        let table =
            StaticTable::new("t", &[("key".to_string(), "value".to_string())]).expect("valid");
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn static_table_empty_labels_returns_error() {
        let result = StaticTable::new("t", &[]);
        let err = result.err().expect("empty labels should return Err");
        assert_eq!(
            err.to_string(),
            "enrichment error: StaticTable requires at least one label",
            "error message must identify the cause"
        );
    }

    // -- Host info ----------------------------------------------------------

    #[test]
    fn host_info_has_all_columns() {
        let table = HostInfoTable::new();
        assert_eq!(table.name(), "host_info");
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);

        // Core fields must always be non-empty.
        for col_name in &["hostname", "os_type", "os_arch"] {
            let col = batch
                .column_by_name(col_name)
                .unwrap_or_else(|| panic!("missing column: {col_name}"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(!col.value(0).is_empty(), "{col_name} should be non-empty");
        }

        // Extended fields must exist (may be empty on non-Linux).
        for col_name in &[
            "os_name",
            "os_family",
            "os_version",
            "os_kernel",
            "host_id",
            "boot_id",
        ] {
            assert!(
                batch.column_by_name(col_name).is_some(),
                "missing column: {col_name}"
            );
        }
    }

    #[test]
    fn host_info_linux_extended_fields_populated() {
        if std::env::consts::OS != "linux" {
            return; // extended fields rely on procfs / os-release
        }
        let table = HostInfoTable::new();
        let batch = table.snapshot().unwrap();

        let os_kernel = batch
            .column_by_name("os_kernel")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(
            !os_kernel.is_empty(),
            "os_kernel should be populated on Linux"
        );
    }

    #[test]
    fn parse_os_release_extracts_fields() {
        let content = r#"NAME="Ubuntu"
VERSION_ID="22.04"
ID=ubuntu
ID_LIKE=debian
PRETTY_NAME="Ubuntu 22.04.4 LTS"
"#;
        let pairs = parse_os_release(content);
        assert_eq!(
            os_release_field(&pairs, "PRETTY_NAME"),
            Some("Ubuntu 22.04.4 LTS".to_string()),
        );
        assert_eq!(
            os_release_field(&pairs, "ID_LIKE"),
            Some("debian".to_string()),
        );
        assert_eq!(
            os_release_field(&pairs, "VERSION_ID"),
            Some("22.04".to_string()),
        );
    }

    #[test]
    fn parse_os_release_skips_comments_and_blanks() {
        let content = "# comment\n\nID=alpine\n";
        let pairs = parse_os_release(content);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], ("ID".to_string(), "alpine".to_string()));
    }

    #[test]
    fn host_info_ecs_style_uses_dotted_names() {
        let table = HostInfoTable::with_style(HostInfoStyle::Ecs);
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_columns(), 9);
        assert!(batch.column_by_name("host.hostname").is_some());
        assert!(batch.column_by_name("host.os.type").is_some());
        assert!(batch.column_by_name("host.architecture").is_some());
        assert!(batch.column_by_name("host.os.name").is_some());
        assert!(batch.column_by_name("host.os.family").is_some());
        assert!(batch.column_by_name("host.os.version").is_some());
        assert!(batch.column_by_name("host.os.kernel").is_some());
        assert!(batch.column_by_name("host.id").is_some());
        assert!(batch.column_by_name("host.boot.id").is_some());
    }

    #[test]
    fn host_info_otel_style_uses_semantic_names() {
        let table = HostInfoTable::with_style(HostInfoStyle::Otel);
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_columns(), 9);
        assert!(batch.column_by_name("host.name").is_some());
        assert!(batch.column_by_name("os.type").is_some());
        assert!(batch.column_by_name("host.arch").is_some());
        assert!(batch.column_by_name("os.name").is_some());
        assert!(batch.column_by_name("os.family").is_some());
        assert!(batch.column_by_name("os.version").is_some());
        assert!(batch.column_by_name("os.kernel").is_some());
        assert!(batch.column_by_name("host.id").is_some());
        assert!(batch.column_by_name("host.boot.id").is_some());
    }

    // -- K8s path table -----------------------------------------------------

    #[test]
    fn k8s_path_table_starts_with_empty_batch() {
        let table = K8sPathTable::new("k8s_pods");
        let batch = table.snapshot().expect("should have empty batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 5); // log_path_prefix, namespace, pod_name, pod_uid, container_name
    }

    #[test]
    fn k8s_path_table_update_and_snapshot() {
        let table = K8sPathTable::new("k8s_pods");
        table.update_from_paths(&[
            "/var/log/pods/default_app-a-12345_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/main/0.log"
                .to_string(),
            "/var/log/pods/monitoring_prom-0_11111111-2222-3333-4444-555555555555/prometheus/0.log"
                .to_string(),
        ]);
        let batch = table.snapshot().expect("should have data");
        assert_eq!(batch.num_rows(), 2);
        let ns = batch
            .column_by_name("namespace")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ns.value(0), "default");
        assert_eq!(ns.value(1), "monitoring");
    }

    #[test]
    fn k8s_path_table_deduplicates() {
        let table = K8sPathTable::new("k8s_pods");
        table.update_from_paths(&[
            "/var/log/pods/default_app-12345_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/main/0.log"
                .to_string(),
            "/var/log/pods/default_app-12345_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/main/1.log"
                .to_string(),
        ]);
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn k8s_path_table_ignores_invalid_paths() {
        let table = K8sPathTable::new("k8s_pods");
        table.update_from_paths(&[
            "/var/log/messages".to_string(),
            "/var/log/pods/default_app-12345_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/main/0.log"
                .to_string(),
            "/not/a/cri/path".to_string(),
        ]);
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 1); // only the valid one
    }

    #[test]
    fn k8s_path_table_refresh_replaces_data() {
        let table = K8sPathTable::new("k8s_pods");
        table.update_from_paths(&[
            "/var/log/pods/default_app-a_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/c/0.log".to_string(),
        ]);
        assert_eq!(table.snapshot().unwrap().num_rows(), 1);

        // Refresh with different data.
        table.update_from_paths(&[
            "/var/log/pods/ns1_pod1_11111111-2222-3333-4444-555555555555/c/0.log".to_string(),
            "/var/log/pods/ns2_pod2_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/c/0.log".to_string(),
        ]);
        assert_eq!(table.snapshot().unwrap().num_rows(), 2);
    }

    // -- CSV file table -----------------------------------------------------

    #[test]
    fn csv_load_basic() {
        let csv_data = b"hostname,owner,team\nweb-1,alice,platform\napi-2,bob,backend\n";
        let table = CsvFileTable::new("assets", "/fake/path.csv");
        let rows = table.load_from_reader(&csv_data[..]).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(table.name(), "assets");

        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8View);

        let hostname = csv_string_column(&batch, "hostname");
        assert_eq!(hostname.value(0), "web-1");
        assert_eq!(hostname.value(1), "api-2");

        let team = csv_string_column(&batch, "team");
        assert_eq!(team.value(0), "platform");
        assert_eq!(team.value(1), "backend");
    }

    #[test]
    fn csv_with_missing_fields() {
        let csv_data = b"a,b,c\n1,2,3\n4,5\n"; // row 2 missing column c
        let table = CsvFileTable::new("t", "/fake");
        table.load_from_reader(&csv_data[..]).unwrap();
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let c = csv_string_column(&batch, "c");
        assert_eq!(c.value(0), "3");
        assert!(c.is_null(1)); // padded with NULL
    }

    #[test]
    fn csv_empty_cells_are_empty_strings() {
        let csv_data = b"a,b,c\n1,,3\n,5,\n";
        let table = CsvFileTable::new("t", "/fake");
        table.load_from_reader(&csv_data[..]).unwrap();
        let batch = table.snapshot().unwrap();

        let a = csv_string_column(&batch, "a");
        let b = csv_string_column(&batch, "b");
        let c = csv_string_column(&batch, "c");
        assert_eq!(a.value(1), "");
        assert_eq!(b.value(0), "");
        assert_eq!(c.value(1), "");
    }

    #[test]
    fn csv_quoted_commas_stay_in_cell() {
        let csv_data = b"host,note\nweb-1,\"hello,team\"\napi-2,\"x,y,z\"\n";
        let table = CsvFileTable::new("t", "/fake");
        table.load_from_reader(&csv_data[..]).unwrap();
        let batch = table.snapshot().unwrap();

        let note = csv_string_column(&batch, "note");
        assert_eq!(note.value(0), "hello,team");
        assert_eq!(note.value(1), "x,y,z");
    }

    #[test]
    fn csv_all_missing_column_preserves_header_as_nulls() {
        let csv_data = b"a,b\n1\n2\n";
        let table = CsvFileTable::new("t", "/fake");
        table.load_from_reader(&csv_data[..]).unwrap();
        let batch = table.snapshot().unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(1).name(), "b");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8View);
        let b = csv_string_column(&batch, "b");
        assert!(b.is_null(0));
        assert!(b.is_null(1));
    }

    #[test]
    fn csv_columnar_builder_matches_legacy_values() {
        let csv_data = b"a,b,c\n1,2,3\n4,,\n5\n";
        let columnar = read_csv_to_batch(&csv_data[..]).unwrap();
        let legacy = read_csv_to_legacy_batch_for_test(&csv_data[..]).unwrap();

        assert_eq!(columnar.num_rows(), legacy.num_rows());
        assert_eq!(columnar.num_columns(), legacy.num_columns());
        for field in legacy.schema().fields() {
            let name = field.name();
            let actual = csv_string_column(&columnar, name);
            let expected = legacy
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..legacy.num_rows() {
                assert_eq!(
                    actual.is_null(row),
                    expected.is_null(row),
                    "null mismatch for {name} row {row}",
                );
                if !expected.is_null(row) {
                    assert_eq!(actual.value(row), expected.value(row));
                }
            }
        }
    }

    #[test]
    fn csv_empty_file_fails() {
        let table = CsvFileTable::new("t", "/fake");
        let result = table.load_from_reader(&b""[..]);
        assert!(result.is_err());
    }

    #[test]
    fn csv_empty_header_fails() {
        let csv_data = b"host,,team\nweb-1,alice,platform\n";
        let table = CsvFileTable::new("t", "/fake");
        let result = table.load_from_reader(&csv_data[..]);
        let err = result.expect_err("empty header should return Err");
        assert!(err.to_string().contains("empty header name"));
    }

    #[test]
    fn csv_duplicate_header_fails() {
        let csv_data = b"host,host\nweb-1,web-2\n";
        let table = CsvFileTable::new("t", "/fake");
        let result = table.load_from_reader(&csv_data[..]);
        let err = result.expect_err("duplicate header should return Err");
        assert!(err.to_string().contains("duplicate header name"));
    }

    #[test]
    fn csv_reload_replaces_data() {
        let table = CsvFileTable::new("t", "/fake");
        table.load_from_reader(&b"col\nval1\n"[..]).unwrap();
        assert_eq!(table.snapshot().unwrap().num_rows(), 1);

        table
            .load_from_reader(&b"col\nval1\nval2\nval3\n"[..])
            .unwrap();
        assert_eq!(table.snapshot().unwrap().num_rows(), 3);
    }

    #[test]
    fn csv_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("test.csv");
        std::fs::write(&csv_path, "ip,region\n10.0.0.1,us-east\n10.0.0.2,eu-west\n").unwrap();

        let table = CsvFileTable::new("ips", &csv_path);
        let rows = table.reload().unwrap();
        assert_eq!(rows, 2);

        let batch = table.snapshot().unwrap();
        let region = csv_string_column(&batch, "region");
        assert_eq!(region.value(0), "us-east");
        assert_eq!(region.value(1), "eu-west");
    }

    // -- JSON Lines file table ----------------------------------------------

    #[test]
    fn jsonl_load_basic() {
        let data =
            b"{\"ip\":\"10.0.0.1\",\"owner\":\"alice\"}\n{\"ip\":\"10.0.0.2\",\"owner\":\"bob\"}\n";
        let table = JsonLinesFileTable::new("ip_owners", "/fake");
        let rows = table.load_from_reader(&data[..]).unwrap();
        assert_eq!(rows, 2);

        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let ip = batch
            .column_by_name("ip")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ip.value(0), "10.0.0.1");
        assert_eq!(ip.value(1), "10.0.0.2");
    }

    #[test]
    fn jsonl_union_schema() {
        // Row 1 has {a, b}, row 2 has {b, c} — result should have {a, b, c}.
        let data = b"{\"a\":\"1\",\"b\":\"2\"}\n{\"b\":\"3\",\"c\":\"4\"}\n";
        let table = JsonLinesFileTable::new("t", "/fake");
        table.load_from_reader(&data[..]).unwrap();

        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let a = batch
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "1");
        assert!(a.is_null(1)); // row 2 doesn't have "a"

        let c = batch
            .column_by_name("c")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(c.is_null(0)); // row 1 doesn't have "c"
        assert_eq!(c.value(1), "4");
    }

    #[test]
    fn jsonl_non_string_values_stringified() {
        let data = b"{\"name\":\"web\",\"port\":8080,\"active\":true}\n";
        let table = JsonLinesFileTable::new("t", "/fake");
        table.load_from_reader(&data[..]).unwrap();

        let batch = table.snapshot().unwrap();
        let port = batch
            .column_by_name("port")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(port.value(0), "8080");

        let active = batch
            .column_by_name("active")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(active.value(0), "true");
    }

    #[test]
    fn jsonl_skips_blank_lines() {
        let data = b"{\"a\":\"1\"}\n\n{\"a\":\"2\"}\n\n";
        let table = JsonLinesFileTable::new("t", "/fake");
        table.load_from_reader(&data[..]).unwrap();
        assert_eq!(table.snapshot().unwrap().num_rows(), 2);
    }

    #[test]
    fn jsonl_empty_fails() {
        let table = JsonLinesFileTable::new("t", "/fake");
        let result = table.load_from_reader(&b""[..]);
        assert!(result.is_err());
    }

    // -- Trait object dispatch -----------------------------------------------

    #[test]
    fn trait_object_dispatch() {
        let tables: Vec<Box<dyn EnrichmentTable>> = vec![
            Box::new(
                StaticTable::new("env", &[("k".to_string(), "v".to_string())])
                    .expect("valid labels"),
            ),
            Box::new(HostInfoTable::new()),
            Box::new(K8sPathTable::new("k8s_pods")),
        ];

        assert_eq!(tables[0].name(), "env");
        assert!(tables[0].snapshot().is_some());
        assert_eq!(tables[1].name(), "host_info");
        assert!(tables[1].snapshot().is_some());
        assert_eq!(tables[2].name(), "k8s_pods");
        let batch = tables[2].snapshot().expect("should have empty batch");
        assert_eq!(batch.num_rows(), 0); // empty until update_from_paths
    }

    // -- Concurrent access --------------------------------------------------

    #[test]
    fn concurrent_read_write() {
        let table = Arc::new(K8sPathTable::new("k8s_pods"));

        // Spawn a writer.
        let writer = Arc::clone(&table);
        let handle = std::thread::spawn(move || {
            for i in 0..10 {
                let path =
                    format!("/var/log/pods/ns_pod-{i}_aaaaaaaa-bbbb-cccc-dddd-{i:012x}/c/0.log");
                writer.update_from_paths(&[path]);
            }
        });

        // Read concurrently.
        for _ in 0..100 {
            let _ = table.snapshot(); // should not panic
        }

        handle.join().unwrap();
        // Final state should have data.
        assert!(table.snapshot().is_some());
    }

    // -- ReloadableGeoDb ----------------------------------------------------

    struct FixedGeoDb(GeoResult);
    impl GeoDatabase for FixedGeoDb {
        fn lookup(&self, _ip: &str) -> Option<GeoResult> {
            Some(self.0.clone())
        }
    }

    #[test]
    fn reloadable_geo_db_initial_lookup() {
        let result = GeoResult {
            country_code: Some("US".to_string()),
            ..Default::default()
        };
        let db = Arc::new(FixedGeoDb(result));
        let reloadable = Arc::new(ReloadableGeoDb::new(db));
        let got = reloadable.lookup("1.2.3.4").unwrap();
        assert_eq!(got.country_code.as_deref(), Some("US"));
    }

    #[test]
    fn reloadable_geo_db_swap_replaces_backend() {
        let first = Arc::new(FixedGeoDb(GeoResult {
            country_code: Some("US".to_string()),
            ..Default::default()
        }));
        let reloadable = Arc::new(ReloadableGeoDb::new(first));
        let handle = reloadable.reload_handle();

        let second = Arc::new(FixedGeoDb(GeoResult {
            country_code: Some("DE".to_string()),
            ..Default::default()
        }));
        handle.replace(second);

        let got = reloadable.lookup("1.2.3.4").unwrap();
        assert_eq!(got.country_code.as_deref(), Some("DE"));
    }

    #[test]
    fn reloadable_geo_db_concurrent_reads() {
        let db = Arc::new(FixedGeoDb(GeoResult {
            country_code: Some("AU".to_string()),
            ..Default::default()
        }));
        let reloadable = Arc::new(ReloadableGeoDb::new(db));
        let handle = reloadable.reload_handle();

        let reader = Arc::clone(&reloadable);
        let reader_thread = std::thread::spawn(move || {
            for _ in 0..100 {
                let _ = reader.lookup("8.8.8.8");
            }
        });

        // Swap pointer while reader is running.
        handle.replace(Arc::new(FixedGeoDb(GeoResult {
            country_code: Some("GB".to_string()),
            ..Default::default()
        })));

        reader_thread.join().unwrap();
    }

    // -- EnvTable -----------------------------------------------------------

    #[test]
    fn env_table_reads_prefix() {
        // SAFETY: test sets and clears env vars; must not run in parallel with other
        // tests that read the same vars.
        unsafe {
            std::env::set_var("FFWD_TEST_CLUSTER", "prod");
            std::env::set_var("FFWD_TEST_REGION", "us-east-1");
        }

        let table = EnvTable::from_prefix("deploy", "FFWD_TEST_").expect("should succeed");
        assert_eq!(table.name(), "deploy");
        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Columns exist for the two vars we set (plus any pre-existing matches).
        assert!(batch.column_by_name("cluster").is_some());
        assert!(batch.column_by_name("region").is_some());

        let cluster = batch
            .column_by_name("cluster")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cluster.value(0), "prod");

        // SAFETY: `remove_var` is unsafe because it mutates process-global
        // state and is unsound under concurrent access. This is safe here
        // because `cargo nextest` runs each test in its own process, and
        // these variables use a unique `FFWD_TEST_` prefix not read by
        // any other test.
        unsafe {
            std::env::remove_var("FFWD_TEST_CLUSTER");
            std::env::remove_var("FFWD_TEST_REGION");
        }
    }

    #[test]
    fn env_table_no_match_returns_error() {
        let result = EnvTable::from_prefix("nothing", "FFWD_NONEXISTENT_PREFIX_XYZZY_12345_");
        assert!(result.is_err());
    }

    #[test]
    #[cfg(unix)] // Windows env vars are case-insensitive; both names map to one var.
    fn env_table_rejects_duplicate_columns_after_lowercasing() {
        // Set env vars that collide after lowercasing the suffix.
        // SAFETY: test is run single-threaded via `cargo nextest` (which isolates
        // each test in its own process) or `--test-threads=1`. The vars use a
        // unique prefix (FFWD_DUPTEST_) to avoid collisions with real env.
        unsafe {
            std::env::set_var("FFWD_DUPTEST_FOO", "a");
            std::env::set_var("FFWD_DUPTEST_foo", "b");
        }
        let result = EnvTable::from_prefix("dup_test", "FFWD_DUPTEST_");
        // Clean up before asserting.
        // SAFETY: this removes only the unique test variables set above.
        unsafe {
            std::env::remove_var("FFWD_DUPTEST_FOO");
            std::env::remove_var("FFWD_DUPTEST_foo");
        }
        assert!(result.is_err());
        let msg = format!("{}", result.err().unwrap());
        assert!(msg.contains("duplicate column name"), "got: {msg}");
    }

    // -- ProcessInfoTable -------------------------------------------------------

    #[test]
    fn process_info_has_expected_columns() {
        let table = ProcessInfoTable::new();
        let batch = table.snapshot().expect("should have snapshot");
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("agent_name").is_some());
        assert!(batch.column_by_name("agent_version").is_some());
        assert!(batch.column_by_name("pid").is_some());
        assert!(batch.column_by_name("start_time").is_some());

        let name_col = batch
            .column_by_name("agent_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "ffwd");

        let pid_col = batch
            .column_by_name("pid")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pid: u32 = pid_col.value(0).parse().expect("pid should be numeric");
        assert!(pid > 0);
    }

    #[test]
    fn process_info_start_time_is_iso8601() {
        let table = ProcessInfoTable::new();
        let batch = table.snapshot().unwrap();
        let ts = batch
            .column_by_name("start_time")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        // Should look like "2026-04-12T06:20:13Z"
        assert!(ts.ends_with('Z'), "expected UTC: {ts}");
        assert_eq!(ts.len(), 20, "expected ISO 8601 length: {ts}");
    }

    #[test]
    fn process_info_table_name() {
        let table = ProcessInfoTable::new();
        assert_eq!(table.name(), "process_info");
    }

    // -- epoch_days_to_ymd -------------------------------------------------------

    #[test]
    fn epoch_days_known_dates() {
        // Unix epoch: 1970-01-01
        assert_eq!(epoch_days_to_ymd(0), (1970, 1, 1));
        // 2000-01-01 is day 10957
        assert_eq!(epoch_days_to_ymd(10957), (2000, 1, 1));
        // 2024-02-29 (leap day) is day 19782
        assert_eq!(epoch_days_to_ymd(19782), (2024, 2, 29));
    }

    // -- KvFileTable -------------------------------------------------------

    #[test]
    fn kv_file_parses_os_release_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("os-release");
        std::fs::write(
            &path,
            r#"# This is a comment
    NAME="Ubuntu"
    VERSION_ID="22.04"
    ID=ubuntu
    PRETTY_NAME="Ubuntu 22.04.3 LTS"
    "#,
        )
        .unwrap();

        let table = KvFileTable::new("os", &path);
        let n = table.reload().unwrap();
        assert_eq!(n, 4);

        let batch = table.snapshot().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let name_col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Ubuntu");

        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "ubuntu");
    }

    #[test]
    fn kv_file_handles_single_quotes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.env");
        std::fs::write(&path, "KEY='single quoted'\n").unwrap();

        let table = KvFileTable::new("test", &path);
        table.reload().unwrap();
        let batch = table.snapshot().unwrap();
        let val = batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(val, "single quoted");
    }

    #[test]
    fn kv_file_handles_single_char_quoted_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.env");
        // A single quote character as the entire value — previously panicked.
        std::fs::write(&path, "KEY=\"\nOTHER=ok\n").unwrap();

        let table = KvFileTable::new("test", &path);
        table.reload().unwrap();
        let batch = table.snapshot().unwrap();
        // The single `"` should be kept as-is (not stripped).
        let val = batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(val, "\"");
    }

    #[test]
    fn kv_file_empty_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.env");
        std::fs::write(&path, "# only comments\n\n").unwrap();

        let table = KvFileTable::new("empty", &path);
        assert!(table.reload().is_err());
    }

    #[test]
    fn kv_file_missing_file_returns_error() {
        let table = KvFileTable::new("missing", Path::new("/nonexistent/file.env"));
        assert!(table.reload().is_err());
    }

    #[test]
    fn kv_file_reload_updates_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("update.env");
        std::fs::write(&path, "VERSION=1\n").unwrap();

        let table = KvFileTable::new("ver", &path);
        table.reload().unwrap();
        let v1 = table
            .snapshot()
            .unwrap()
            .column_by_name("version")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(v1, "1");

        std::fs::write(&path, "VERSION=2\n").unwrap();
        table.reload().unwrap();
        let v2 = table
            .snapshot()
            .unwrap()
            .column_by_name("version")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(v2, "2");
    }

    // -- NetworkInfoTable -------------------------------------------------------

    #[test]
    fn network_info_has_expected_columns() {
        let table = NetworkInfoTable::new();
        let batch = table.snapshot().expect("should have snapshot");
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("hostname").is_some());
        assert!(batch.column_by_name("primary_ipv4").is_some());
        assert!(batch.column_by_name("primary_ipv6").is_some());
        assert!(batch.column_by_name("all_ipv4").is_some());
        assert!(batch.column_by_name("all_ipv6").is_some());

        // Hostname should be non-empty on any real system
        let hostname = batch
            .column_by_name("hostname")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(!hostname.is_empty());
    }

    #[test]
    fn network_info_table_name() {
        let table = NetworkInfoTable::new();
        assert_eq!(table.name(), "network_info");
    }

    // -- format_ipv6_hex -------------------------------------------------------

    #[test]
    fn format_ipv6_hex_known_address() {
        // 2001:0db8:0000:0000:0000:0000:0000:0001
        let hex = "20010db8000000000000000000000001";
        let formatted = format_ipv6_hex(hex).unwrap();
        assert_eq!(formatted, "2001:db8:0:0:0:0:0:1");
    }

    #[test]
    fn format_ipv6_hex_all_zeros() {
        let hex = "00000000000000000000000000000000";
        let formatted = format_ipv6_hex(hex).unwrap();
        assert_eq!(formatted, "0:0:0:0:0:0:0:0");
    }

    #[test]
    fn format_ipv6_hex_wrong_length() {
        assert!(format_ipv6_hex("abc").is_none());
    }

    // -- ContainerInfoTable -----------------------------------------------------

    #[test]
    fn container_info_has_expected_columns() {
        let table = ContainerInfoTable::new();
        let batch = table.snapshot().expect("should have snapshot");
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("container_id").is_some());
        assert!(batch.column_by_name("container_runtime").is_some());
    }

    #[test]
    fn container_info_table_name() {
        let table = ContainerInfoTable::new();
        assert_eq!(table.name(), "container_info");
    }

    #[test]
    fn parse_cgroup_v1_docker_path() {
        let content =
            "12:memory:/docker/abc123def456abc123def456abc123def456abc123def456abc123def456abc1\n";
        let result = parse_cgroup_for_container(content);
        assert!(result.is_some());
        let (id, runtime) = result.unwrap();
        assert_eq!(runtime, "docker");
        assert_eq!(id.len(), 64);
    }

    #[test]
    fn parse_cgroup_v2_docker_systemd_scope() {
        let id_hex = "a1b2c3d4".repeat(8); // 64 hex chars
        let content = format!("0::/system.slice/docker-{id_hex}.scope\n");
        let result = parse_cgroup_for_container(&content);
        assert!(result.is_some());
        let (id, runtime) = result.unwrap();
        assert_eq!(runtime, "docker");
        assert_eq!(id, id_hex);
    }

    #[test]
    fn parse_cgroup_not_container() {
        let content = "0::/init.scope\n";
        let result = parse_cgroup_for_container(content);
        assert!(result.is_none());
    }

    // -- K8sClusterInfoTable ----------------------------------------------------

    #[test]
    fn k8s_cluster_info_has_expected_columns() {
        let table = K8sClusterInfoTable::new();
        let batch = table.snapshot().expect("should have snapshot");
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("namespace").is_some());
        assert!(batch.column_by_name("pod_name").is_some());
        assert!(batch.column_by_name("node_name").is_some());
        assert!(batch.column_by_name("service_account").is_some());
        assert!(batch.column_by_name("cluster_name").is_some());
    }

    #[test]
    fn k8s_cluster_info_table_name() {
        let table = K8sClusterInfoTable::new();
        assert_eq!(table.name(), "k8s_cluster_info");
    }

    // -- is_hex_container_id ----------------------------------------------------

    #[test]
    fn hex_container_id_valid() {
        let id = "a".repeat(64);
        assert!(is_hex_container_id(&id));
    }

    #[test]
    fn hex_container_id_too_short() {
        assert!(!is_hex_container_id("abc123"));
    }

    #[test]
    fn hex_container_id_non_hex() {
        let id = "g".repeat(64);
        assert!(!is_hex_container_id(&id));
    }
}
