use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveResponse {
    pub status: String,
    pub reason: String,
    pub observed_at_unix_ns: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub status: String,
    pub reason: String,
    pub observed_at_unix_ns: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealthResponse {
    pub status: String,
    pub reason: String,
    pub readiness_impact: String,
    pub observed_at_unix_ns: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryResponse {
    pub resident: usize,
    pub allocated: usize,
    pub active: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemResponse {
    pub uptime_seconds: u64,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputResponse {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub health: String,
    pub lines_total: u64,
    pub bytes_total: u64,
    pub errors: u64,
    pub rotations: u64,
    pub parse_errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputResponse {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub health: String,
    pub lines_total: u64,
    pub bytes_total: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformResponse {
    pub sql: String,
    pub health: String,
    pub lines_in: u64,
    pub lines_out: u64,
    pub errors: u64,
    pub filter_drop_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchesResponse {
    pub total: u64,
    pub avg_rows: f64,
    pub flush_by_size: u64,
    pub flush_by_timeout: u64,
    pub dropped_batches_total: u64,
    pub scan_errors_total: u64,
    pub parse_errors_total: u64,
    pub last_batch_time_ns: u64,
    pub batch_latency_avg_ns: u64,
    pub inflight: u64,
    pub rows_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageSecondsResponse {
    pub scan: f64,
    pub transform: f64,
    pub output: f64,
    pub queue_wait: f64,
    pub send: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineResponse {
    pub name: String,
    pub inputs: Vec<InputResponse>,
    pub transform: TransformResponse,
    pub outputs: Vec<OutputResponse>,
    pub batches: BatchesResponse,
    pub stage_seconds: StageSecondsResponse,
    pub backpressure_stalls: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub live: LiveResponse,
    pub ready: ReadyResponse,
    pub component_health: ComponentHealthResponse,
    pub pipelines: Vec<PipelineResponse>,
    pub system: SystemResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_version: Option<String>,
}

pub const DIAGNOSTICS_CONTRACT_VERSION: &str = "1";
