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

export interface PipelineData {
  name: string;
  inputs: ComponentData[];
  transform: TransformData;
  outputs: ComponentData[];
  batches?: number;
  stage_seconds?: { scan: number; transform: number; output: number };
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
  input_rows: number;
  output_rows: number;
  errors: number;
  status: "ok" | "error" | "unset";
}

export interface TracesResponse {
  traces: TraceRecord[];
}
