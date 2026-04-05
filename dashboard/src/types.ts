// API response types from logfwd diagnostics server

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
}

export interface PipelinesResponse {
  pipelines: PipelineData[];
  system: {
    uptime_seconds: number;
    version: string;
  };
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
  start_unix_ns: number;
  total_ns: number;
  scan_ns: number;
  transform_ns: number;
  output_ns: number;
  /** Absolute wall-clock start of the output span (ns). Use this to position the output bar. */
  output_start_unix_ns?: number;
  /** Rows extracted by the scanner (before SQL filter). */
  scan_rows: number;
  /** Rows into SQL transform (= scan_rows for non-empty scans). */
  input_rows: number;
  /** Rows after SQL filter, sent to output. */
  output_rows: number;
  /** Raw bytes fed to the scanner. */
  bytes_in: number;
  /** Time data waited in channel before processing, nanoseconds. */
  queue_wait_ns: number;
  /** Worker that processed this batch (-1 if unknown). */
  worker_id: number;
  /** Nanoseconds from request send start to response headers received. */
  send_ns?: number;
  /** Nanoseconds from response headers to body fully read. */
  recv_ns?: number;
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
  /** True while the batch is still executing (scan/transform/output in progress). */
  in_progress?: boolean;
  /** Current stage when in_progress: "scan" | "transform" | "output" */
  stage?: string;
  /** Unix ns when the current in-progress stage started. */
  stage_start_unix_ns?: number;
}

export interface TracesResponse {
  traces: TraceRecord[];
}
