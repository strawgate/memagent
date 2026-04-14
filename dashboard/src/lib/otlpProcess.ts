/**
 * Convert OTLP JSON spans into the dashboard's internal TraceRecord type.
 *
 * The diagnostics WebSocket pushes OTLP JSON (resourceMetrics, resourceSpans,
 * resourceLogs). Metrics are now handled directly by the TelemetryStore from
 * @otlpkit/views. This module only bridges OTLP spans → TraceRecord[].
 */

import type { OtlpTracesDocument } from "@otlpkit/otlpjson";
import { collectSpans, toNumber } from "@otlpkit/otlpjson";
import type { TraceRecord } from "../types";

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
      BigInt(endNs) > BigInt(startNs) ? (BigInt(endNs) - BigInt(startNs)).toString() : "0";

    let scanNs = 0;
    let scanRows = 0;
    let transformNs = 0;
    let outputNs = 0;
    let outputStartUnixNs = "0";
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
        BigInt(kidEndNs) > BigInt(kidStartNs) ? Number(BigInt(kidEndNs) - BigInt(kidStartNs)) : 0;

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
          outputStartUnixNs = kidStartNs;
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

    // Fallback to root attributes for output/worker if no output child span
    // (e.g. in-progress batches that haven't reached the output stage yet).
    if (workerId === null) workerId = toNumber(root.attributes.worker_id) ?? null;
    if (outputNs === 0) outputNs = attrNum("output_ns");
    if (outputStartUnixNs === "0") {
      const rootVal = attr("output_start_unix_ns");
      if (rootVal) outputStartUnixNs = rootVal;
    }

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
      root.status?.code === 2 ? "error" : root.status?.code === 1 ? "ok" : "unset";

    records.push({
      trace_id: traceId,
      pipeline,
      start_unix_ns: String(startNs),
      total_ns: durationNs,
      scan_ns: String(scanNs),
      transform_ns: String(transformNs),
      output_ns: String(outputNs),
      output_start_unix_ns: outputStartUnixNs !== "0" ? outputStartUnixNs : undefined,
      scan_rows: scanRows,
      input_rows: inputRows,
      output_rows: outputRows,
      bytes_in: bytesIn,
      queue_wait_ns: queueWaitNs || "0",
      worker_id: workerId ?? -1,
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
