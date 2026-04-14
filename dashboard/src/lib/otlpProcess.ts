/**
 * Convert OTLP JSON metrics and spans into the dashboard's internal types.
 *
 * The diagnostics WebSocket pushes OTLP JSON (resourceMetrics, resourceSpans,
 * resourceLogs). This module bridges the gap between @otlpkit's parsed records
 * and our existing dashboard state (MetricSeries ring buffers, TraceRecord[]).
 */

import {
  collectMetricPoints,
  collectSpans,
  toNumber,
} from "@otlpkit/otlpjson";
import type { OtlpMetricsDocument, OtlpTracesDocument } from "@otlpkit/otlpjson";
import type { TraceRecord } from "../types";

// ---------------------------------------------------------------------------
// Metrics → dashboard metric map
// ---------------------------------------------------------------------------

/** Extracted counters/gauges from one OTLP metrics push. */
export interface OtlpMetricSnapshot {
  inputLines: number;
  inputBytes: number;
  outputBytes: number;
  outputErrors: number;
  batches: number;
  inflightBatches: number;
  backpressureStalls: number;
  cpuUserMs: number | null;
  cpuSysMs: number | null;
  rssBytes: number | null;
  memResident: number | null;
  memAllocated: number | null;
  memActive: number | null;
  scanNanos: number;
  transformNanos: number;
  outputNanos: number;
  uptimeSeconds: number;
}

/**
 * Extract a flat metric snapshot from an OTLP metrics document.
 * Sums across all pipelines for aggregate values.
 */
export function extractMetricSnapshot(doc: OtlpMetricsDocument): OtlpMetricSnapshot {
  const snap: OtlpMetricSnapshot = {
    inputLines: 0,
    inputBytes: 0,
    outputBytes: 0,
    outputErrors: 0,
    batches: 0,
    inflightBatches: 0,
    backpressureStalls: 0,
    cpuUserMs: null,
    cpuSysMs: null,
    rssBytes: null,
    memResident: null,
    memAllocated: null,
    memActive: null,
    scanNanos: 0,
    transformNanos: 0,
    outputNanos: 0,
    uptimeSeconds: 0,
  };

  for (const pt of collectMetricPoints(doc)) {
    const v = pt.point.kind === "number" ? (pt.point.value ?? 0) : 0;
    const stage = pt.point.attributes.stage as string | undefined;

    switch (pt.metric.name) {
      case "logfwd.input_lines":
        snap.inputLines += v;
        break;
      case "logfwd.input_bytes":
        snap.inputBytes += v;
        break;
      case "logfwd.output_bytes":
        snap.outputBytes += v;
        break;
      case "logfwd.output_errors":
        snap.outputErrors += v;
        break;
      case "logfwd.batches":
        snap.batches += v;
        break;
      case "logfwd.inflight_batches":
        snap.inflightBatches += v;
        break;
      case "logfwd.backpressure_stalls":
        snap.backpressureStalls += v;
        break;
      case "logfwd.stage_nanos":
        if (stage === "scan") snap.scanNanos += v;
        else if (stage === "transform") snap.transformNanos += v;
        else if (stage === "output") snap.outputNanos += v;
        break;
      case "logfwd.uptime_seconds":
        snap.uptimeSeconds = v;
        break;
      case "process.cpu.user_ms":
        snap.cpuUserMs = v;
        break;
      case "process.cpu.sys_ms":
        snap.cpuSysMs = v;
        break;
      case "process.memory.rss":
        snap.rssBytes = v;
        break;
      case "process.memory.resident":
        snap.memResident = v;
        break;
      case "process.memory.allocated":
        snap.memAllocated = v;
        break;
      case "process.memory.active":
        snap.memActive = v;
        break;
    }
  }

  return snap;
}

// ---------------------------------------------------------------------------
// Spans → TraceRecord[]
// ---------------------------------------------------------------------------

/**
 * Convert OTLP spans into the dashboard's TraceRecord format.
 *
 * The server sends both root "batch" spans and child spans (scan, transform,
 * output). We reconstruct the flat TraceRecord by matching root→children.
 */
export function extractTraceRecords(doc: OtlpTracesDocument): TraceRecord[] {
  const otlpSpans = collectSpans(doc);

  // Separate root spans (parentSpanId is zeroes or empty) from children.
  const roots: typeof otlpSpans = [];
  const childrenByTrace = new Map<string, typeof otlpSpans>();

  for (const s of otlpSpans) {
    const parentId = s.parentSpanId ?? "";
    if (parentId === "" || parentId === "0000000000000000") {
      roots.push(s);
    } else {
      const tid = s.traceId ?? "";
      const existing = childrenByTrace.get(tid);
      if (existing) existing.push(s);
      else childrenByTrace.set(tid, [s]);
    }
  }

  const records: TraceRecord[] = [];

  for (const root of roots) {
    const traceId = root.traceId ?? "";
    const attr = (key: string): string => String(root.attributes[key] ?? "");
    const attrNum = (key: string): number => toNumber(root.attributes[key]) ?? 0;

    const isInProgress = root.attributes.in_progress === true;
    const pipeline = attr("pipeline");
    const startNs = root.startTimeUnixNano ?? "0";
    const endNs = root.endTimeUnixNano ?? "0";
    const durationNs =
      BigInt(endNs) > BigInt(startNs)
        ? (BigInt(endNs) - BigInt(startNs)).toString()
        : "0";

    let scanNs = 0;
    let scanRows = 0;
    let transformNs = 0;
    let outputNs = 0;
    let outputStartUnixNs = 0;
    let workerId: number | null = null;
    let sendNs = 0;
    let recvNs = 0;
    let tookMs = 0;
    let retries = 0;
    let reqBytes = 0;
    let cmpBytes = 0;
    let respBytes = 0;

    const children = childrenByTrace.get(traceId) ?? [];
    for (const kid of children) {
      const kidNum = (key: string): number => toNumber(kid.attributes[key]) ?? 0;
      const kidStartNs = kid.startTimeUnixNano ?? "0";
      const kidEndNs = kid.endTimeUnixNano ?? "0";
      const kidDur =
        BigInt(kidEndNs) > BigInt(kidStartNs)
          ? Number(BigInt(kidEndNs) - BigInt(kidStartNs))
          : 0;

      switch (kid.name) {
        case "scan":
          scanNs = kidDur;
          scanRows = kidNum("rows");
          break;
        case "transform":
          transformNs = kidDur;
          break;
        case "output":
          outputNs = kidDur;
          outputStartUnixNs = Number(BigInt(kidStartNs));
          workerId = toNumber(kid.attributes.worker_id);
          sendNs = kidNum("send_ns");
          recvNs = kidNum("recv_ns");
          tookMs = kidNum("took_ms");
          retries = kidNum("retries");
          reqBytes = kidNum("req_bytes");
          cmpBytes = kidNum("cmp_bytes");
          respBytes = kidNum("resp_bytes");
          break;
      }
    }

    // Fallback to root attributes for scan/transform if no child spans.
    if (scanNs === 0) scanNs = attrNum("scan_ns");
    if (transformNs === 0) transformNs = attrNum("transform_ns");

    const errors = attrNum("errors");
    const inputRows = attrNum("input_rows");
    const outputRows = attrNum("output_rows");
    const bytesIn = attrNum("bytes_in");
    const flushReason = attr("flush_reason");
    const queueWaitNs = attr("queue_wait_ns");

    // Determine lifecycle state.
    let lifecycleState: TraceRecord["lifecycle_state"];
    let lifecycleStateStartUnixNs: string | undefined;

    if (isInProgress) {
      const stage = attr("stage");
      const stageStartNs = attr("stage_start_unix_ns");
      lifecycleStateStartUnixNs = stageStartNs || undefined;

      if (stage === "scan") {
        lifecycleState = "scan_in_progress";
      } else if (stage === "transform") {
        lifecycleState = "transform_in_progress";
      } else if (stage === "output") {
        lifecycleState = "output_in_progress";
      } else {
        // "queued" stage
        lifecycleState = "queued_for_output";
      }
    } else {
      lifecycleState = "completed";
    }

    const status: TraceRecord["status"] =
      root.status.code === 2 ? "error" : root.status.code === 1 ? "ok" : "unset";

    records.push({
      trace_id: traceId,
      pipeline,
      start_unix_ns: String(startNs),
      total_ns: durationNs,
      scan_ns: String(scanNs),
      transform_ns: String(transformNs),
      output_ns: String(outputNs),
      output_start_unix_ns: outputStartUnixNs > 0 ? String(outputStartUnixNs) : undefined,
      scan_rows: scanRows,
      input_rows: inputRows,
      output_rows: outputRows,
      bytes_in: bytesIn,
      queue_wait_ns: queueWaitNs || "0",
      worker_id: workerId,
      send_ns: sendNs > 0 ? String(sendNs) : undefined,
      recv_ns: recvNs > 0 ? String(recvNs) : undefined,
      took_ms: tookMs > 0 ? tookMs : undefined,
      retries: retries > 0 ? retries : undefined,
      req_bytes: reqBytes > 0 ? reqBytes : undefined,
      cmp_bytes: cmpBytes > 0 ? cmpBytes : undefined,
      resp_bytes: respBytes > 0 ? respBytes : undefined,
      flush_reason: flushReason,
      errors,
      status,
      lifecycle_state: lifecycleState,
      lifecycle_state_start_unix_ns: lifecycleStateStartUnixNs,
    });
  }

  return records;
}

/** Convert an OTLP uptime gauge to seconds for display. */
export function extractUptimeSeconds(doc: OtlpMetricsDocument): number {
  for (const pt of collectMetricPoints(doc)) {
    if (pt.metric.name === "logfwd.uptime_seconds" && pt.point.kind === "number") {
      return pt.point.value ?? 0;
    }
  }
  return 0;
}
