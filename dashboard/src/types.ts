// API response types from logfwd diagnostics server


export type MetricId =
  | "lps"
  | "bps"
  | "obps"
  | "err"
  | "cpu"
  | "mem"
  | "lat"
  | "inflight"
  | "batches"
  | "stalls";

export interface ComponentData {
  name: string;
  type: string;
  lines_total: number;
  bytes_total: number;
  errors: number;
  parse_errors?: number;
}

export interface TransformData {
  sql: string;
  lines_in: number;
  lines_out: number;
  filter_drop_rate: number;
}

export interface BatchesData {
  total: number;
  avg_rows: number;
  flush_by_size: number;
  flush_by_timeout: number;
  dropped_batches_total: number;
  scan_errors_total: number;
  last_batch_time_ns: number;
  batch_latency_avg_ns?: number;
  inflight?: number;
  rows_total?: number;
}

export interface BottleneckData {
  stage: "output" | "input" | "transform" | "scan" | "none";
  reason: string;
}

export interface PipelineData {
  name: string;
  inputs: ComponentData[];
  transform: TransformData;
  outputs: ComponentData[];
  batches?: BatchesData;
  stage_seconds?: {
    scan: number;
    transform: number;
    output: number;
    queue_wait?: number;
    send?: number;
  };
  backpressure_stalls?: number;
  bottleneck?: BottleneckData;
}

export type HealthState =
  | "starting"
  | "healthy"
  | "degraded"
  | "stopping"
  | "stopped"
  | "failed";

export interface StatusSnapshot {
  status: string;
  reason: string;
  observed_at_unix_ns: string;
}

export interface ComponentHealthSnapshot extends StatusSnapshot {
  status: HealthState;
  readiness_impact: "ready" | "non_blocking" | "gating";
}

export interface StatusResponse {
  contract_version: string;
  live: StatusSnapshot & {
    status: "live";
  };
  ready: StatusSnapshot & {
    status: "ready" | "not_ready";
  };
  component_health: ComponentHealthSnapshot;
  pipelines: PipelineData[];
  system: {
    uptime_seconds: number;
    version: string;
    memory?: {
      resident: number;
      allocated: number;
      active: number;
    };
  };
}

export interface LiveResponse {
  contract_version: string;
  status: "live";
  uptime_seconds: number;
  version: string;
}

export interface ReadyResponse {
  contract_version: string;
  status: "ready" | "not_ready";
  reason: string;
  observed_at_unix_ns: string;
}

export interface StatsResponse {
  uptime_sec: number;
  rss_bytes: number | null;
  cpu_user_ms: number | null;
  cpu_sys_ms: number | null;
  input_lines: number;
  input_bytes: number;
  output_lines: number;
  output_bytes: number;
  output_errors: number;
  batches: number;
  scan_sec: number;
  transform_sec: number;
  output_sec: number;
  backpressure_stalls: number;
  inflight_batches: number;
  mem_resident?: number;
  mem_allocated?: number;
  mem_active?: number;
}

export interface ConfigResponse {
  path: string;
  raw_yaml: string;
}

export interface TraceRecord {
  trace_id: string;
  pipeline: string;
  start_unix_ns: string;
  total_ns: string;
  scan_ns: string;
  transform_ns: string;
  output_ns: string;
  /** Absolute wall-clock start of the output span (ns). Use this to position the output bar. */
  output_start_unix_ns?: string;
  /** Rows extracted by the scanner (before SQL filter). */
  scan_rows: number;
  /** Rows into SQL transform (= scan_rows for non-empty scans). */
  input_rows: number;
  /** Rows after SQL filter, sent to output. */
  output_rows: number;
  /** Raw bytes fed to the scanner. */
  bytes_in: number;
  /** Time data waited in channel before processing, nanoseconds. */
  queue_wait_ns: string;
  /** Worker that processed this batch (`null`/`-1` if unassigned or unknown). */
  worker_id: number | null;
  /** Nanoseconds from request send start to response headers received. */
  send_ns?: string;
  /** Nanoseconds from response headers to body fully read. */
  recv_ns?: string;
  /** Milliseconds Elasticsearch spent processing (`took` field). */
  took_ms?: number;
  /** Number of retries before success or permanent failure. */
  retries?: number;
  /** Uncompressed NDJSON request body size in bytes. */
  req_bytes?: number;
  /** Compressed request body size (0 if compression disabled). */
  cmp_bytes?: number;
  /** Response body size in bytes. */
  resp_bytes?: number;
  /** "size" | "timeout" | "drain" */
  flush_reason: string;
  errors: number;
  status: "ok" | "error" | "unset";
  /**
   * Explicit trace lifecycle state used by TraceExplorer rendering.
   *
   * State contract:
   * - "scan_in_progress": scanner is executing (lifecycle_state_start_unix_ns required)
   * - "transform_in_progress": SQL transform is executing (lifecycle_state_start_unix_ns required)
   * - "queued_for_output": scan+transform are done and waiting for output worker assignment
   *   (lifecycle_state_start_unix_ns required; worker_id is typically null)
   * - "output_in_progress": output is executing on an assigned worker
   *   (lifecycle_state_start_unix_ns + output_start_unix_ns required)
   * - "completed": terminal state for both success and error outcomes
   */
  lifecycle_state:
    | "scan_in_progress"
    | "transform_in_progress"
    | "queued_for_output"
    | "output_in_progress"
    | "completed";
  /** Unix ns when the current lifecycle state started (required for in-progress states). */
  lifecycle_state_start_unix_ns?: string;
}

export interface TracesResponse {
  traces: TraceRecord[];
}
