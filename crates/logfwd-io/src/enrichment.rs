//! Enrichment table providers — background-refreshable Arrow tables
//! that get registered in DataFusion alongside the `logs` table.
//!
//! Each provider produces an Arrow RecordBatch representing a lookup table.
//! The SqlTransform registers these as MemTables so users can JOIN against them.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::InputError;

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
    /// The error type is `InputError` for consistency with the rest of this
    /// module (all enrichment loaders use `Result<_, InputError>`).
    pub fn new(table_name: impl Into<String>, labels: &[(String, String)]) -> Result<Self, InputError> {
        if labels.is_empty() {
            return Err(InputError::Config(
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
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| InputError::Config(format!("Arrow batch error: {e}")))?;
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

/// System host metadata. One row, resolved at construction time.
///
/// Columns: `hostname`, `os_type`, `os_arch`, `os_name`, `os_family`,
/// `os_version`, `os_kernel`, `host_id`, `boot_id`
///
/// Fields align with Elastic Common Schema `host.*` / `host.os.*`, Fluent Bit
/// sysinfo, and Vector host metadata.  On Linux, extended fields are read from
/// `/etc/os-release`, `/proc/sys/kernel/osrelease`, `/etc/machine-id`, and
/// `/proc/sys/kernel/random/boot_id`.  On other platforms they degrade to empty
/// strings.
pub struct HostInfoTable {
    batch: RecordBatch,
}

impl Default for HostInfoTable {
    fn default() -> Self {
        Self::new()
    }
}

impl HostInfoTable {
    /// Snapshot host metadata at construction time.
    pub fn new() -> Self {
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
            Field::new("hostname", DataType::Utf8, false),
            Field::new("os_type", DataType::Utf8, false),
            Field::new("os_arch", DataType::Utf8, false),
            Field::new("os_name", DataType::Utf8, false),
            Field::new("os_family", DataType::Utf8, false),
            Field::new("os_version", DataType::Utf8, false),
            Field::new("os_kernel", DataType::Utf8, false),
            Field::new("host_id", DataType::Utf8, false),
            Field::new("boot_id", DataType::Utf8, false),
        ]));
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
        let val = if (val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\''))
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
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

// ---------------------------------------------------------------------------
// K8s pod metadata (parsed from CRI log file paths)
// ---------------------------------------------------------------------------

/// Extracts Kubernetes metadata from CRI log file paths.
///
/// CRI log path format:
///   /var/log/pods/<namespace>_<pod-name>_<pod-uid>/<container>/0.log
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
        let mut entries: Vec<_> = paths
            .iter()
            .filter_map(|path| parse_cri_log_path(path))
            .collect();
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

/// A lookup table loaded from a CSV file. All columns are Utf8.
/// Supports periodic refresh via `reload()`.
///
/// ```yaml
/// enrichment:
///   - type: csv
///     table_name: assets
///     path: /etc/logfwd/assets.csv
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
    pub fn load_from_reader<R: io::Read>(&self, reader: R) -> Result<usize, InputError> {
        let batch = read_csv_to_batch(reader)?;
        let num_rows = batch.num_rows();
        *self
            .data
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
        Ok(num_rows)
    }

    /// Reload the CSV file from disk. Returns the number of rows loaded.
    pub fn reload(&self) -> Result<usize, InputError> {
        let file = std::fs::File::open(&self.path)
            .map_err(|e| InputError::Config(format!("failed to open {}: {e}", self.path.display())))?;
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

/// Read a CSV into an Arrow RecordBatch. All columns are Utf8.
fn read_csv_to_batch<R: io::Read>(reader: R) -> Result<RecordBatch, InputError> {
    let mut csv_reader = csv::ReaderBuilder::new().flexible(true).from_reader(reader);

    let headers: Vec<String> = csv_reader
        .headers()
        .map_err(|e| InputError::Config(format!("CSV header error: {e}")))?
        .iter()
        .map(ToString::to_string)
        .collect();

    if headers.is_empty() {
        return Err(InputError::Config("CSV has no columns".to_string()));
    }

    let mut seen = HashSet::with_capacity(headers.len());
    for h in &headers {
        if h.is_empty() {
            return Err(InputError::Config(
                "CSV has an empty header name".to_string(),
            ));
        }
        if !seen.insert(h) {
            return Err(InputError::Config(format!(
                "CSV has duplicate header name: {h}"
            )));
        }
    }

    // Read all rows into column-oriented vecs.
    let num_cols = headers.len();
    let mut columns: Vec<Vec<Option<String>>> = vec![Vec::new(); num_cols];

    for (row_idx, result) in csv_reader.records().enumerate() {
        let record = result.map_err(|e| InputError::Config(format!("CSV parse error: {e}")))?;
        if record.len() > num_cols {
            return Err(InputError::Config(format!(
                "CSV row {} has {} fields, expected {} (header count)",
                row_idx + 1,
                record.len(),
                num_cols,
            )));
        }
        for (i, field) in record.iter().enumerate() {
            columns[i].push(Some(field.to_string()));
        }
        // Pad missing columns with NULL.
        for col in columns.iter_mut().skip(record.len()) {
            col.push(None);
        }
    }

    let fields: Vec<Field> = headers
        .iter()
        .map(|h| Field::new(h, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
        .iter()
        .map(|col| {
            let arr: StringArray = col.iter().map(|s| s.as_deref()).collect();
            Arc::new(arr) as _
        })
        .collect();

    RecordBatch::try_new(schema, arrays).map_err(|e| InputError::Config(format!("Arrow batch error: {e}")))
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
///     path: /etc/logfwd/ip-owners.jsonl
/// ```
pub struct JsonLinesFileTable {
    table_name: String,
    path: PathBuf,
    data: Arc<RwLock<Option<RecordBatch>>>,
}

impl JsonLinesFileTable {
    pub fn new(table_name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        JsonLinesFileTable {
            table_name: table_name.into(),
            path: path.into(),
            data: Arc::new(RwLock::new(None)),
        }
    }

    /// Load from a reader (useful for testing).
    pub fn load_from_reader<R: io::BufRead>(&self, reader: R) -> Result<usize, InputError> {
        let batch = read_jsonl_to_batch(reader)?;
        let num_rows = batch.num_rows();
        *self
            .data
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
        Ok(num_rows)
    }

    /// Reload from disk.
    pub fn reload(&self) -> Result<usize, InputError> {
        let file = std::fs::File::open(&self.path)
            .map_err(|e| InputError::Config(format!("failed to open {}: {e}", self.path.display())))?;
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
fn read_jsonl_to_batch<R: io::BufRead>(reader: R) -> Result<RecordBatch, InputError> {
    use std::collections::BTreeMap;

    // First pass: discover all keys and collect rows.
    let mut all_keys: Vec<String> = Vec::new();
    let mut key_set = HashSet::new();
    let mut rows: Vec<BTreeMap<String, String>> = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(|e| InputError::Config(format!("JSONL read error: {e}")))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Minimal JSON object parsing — extract top-level string key-value pairs.
        // We use serde_json-style parsing but store everything as strings.
        let obj: BTreeMap<String, serde_json::Value> =
            serde_json::from_str(trimmed).map_err(|e| InputError::Config(format!("JSONL parse error: {e}")))?;

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
        return Err(InputError::Config("JSONL has no fields".to_string()));
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

    RecordBatch::try_new(schema, arrays).map_err(|e| InputError::Config(format!("Arrow batch error: {e}")))
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn get_str_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column: {name}"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {name} is not StringArray"))
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
        let env = get_str_col(&batch, "environment");
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
        assert!(
            err.to_string().contains("StaticTable requires at least one label"),
            "error message must identify the cause, got: {err}"
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
            let col = get_str_col(&batch, col_name);
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
        let ns = get_str_col(&batch, "namespace");
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

        let hostname = get_str_col(&batch, "hostname");
        assert_eq!(hostname.value(0), "web-1");
        assert_eq!(hostname.value(1), "api-2");

        let team = get_str_col(&batch, "team");
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
        let c = get_str_col(&batch, "c");
        assert_eq!(c.value(0), "3");
        assert!(c.is_null(1)); // padded with NULL
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
        let region = get_str_col(&batch, "region");
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

        let ip = get_str_col(&batch, "ip");
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

        let a = get_str_col(&batch, "a");
        assert_eq!(a.value(0), "1");
        assert!(a.is_null(1)); // row 2 doesn't have "a"

        let c = get_str_col(&batch, "c");
        assert!(c.is_null(0)); // row 1 doesn't have "c"
        assert_eq!(c.value(1), "4");
    }

    #[test]
    fn jsonl_non_string_values_stringified() {
        let data = b"{\"name\":\"web\",\"port\":8080,\"active\":true}\n";
        let table = JsonLinesFileTable::new("t", "/fake");
        table.load_from_reader(&data[..]).unwrap();

        let batch = table.snapshot().unwrap();
        let port = get_str_col(&batch, "port");
        assert_eq!(port.value(0), "8080");

        let active = get_str_col(&batch, "active");
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
}
