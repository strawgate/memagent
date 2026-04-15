/**
 * Pure functions extracted from TraceExplorer so they can be unit-tested
 * without a DOM / canvas environment.
 */
import type { TraceRecord } from "../types";

// ─── Lane types (mirrored from TraceExplorer) ────────────────────────────────

export const SCAN_LANE = -99;

export interface Lane {
  workerId: number; // SCAN_LANE | >=0 worker id
  traces: TraceRecord[];
}

export interface LanesResult {
  lanes: Lane[];
  pendingTraces: TraceRecord[]; // shown as icon strip between scan and workers
}

// ─── computeStats ─────────────────────────────────────────────────────────────

export interface TraceStats {
  batchPerMin: number;
  avgMs: number;
  p90Ms: number;
  scanPct: number;
  xfmPct: number;
  queuePct: number;
  outPct: number;
  errors: number;
}

/**
 * Compute aggregate stats for a set of traces.
 * Returns null when there are fewer than 2 completed (non-in-progress) traces
 * because a single trace provides no meaningful time window for a batch rate.
 */
export function computeStats(traces: TraceRecord[]): TraceStats | null {
  // Only use completed (non-in-progress) traces for timing stats
  const done = traces.filter((t) => !t.in_progress);
  if (done.length < 2) return null;

  let totalNs = 0,
    scanNs = 0,
    xfmNs = 0,
    outNs = 0,
    queueNs = 0,
    errors = 0;
  for (const t of done) {
    const sNs = Number(t.scan_ns) || 0;
    const xNs = Number(t.transform_ns) || 0;
    const qNs = Number(t.queue_wait_ns) || 0;
    const oNs = Number(t.output_ns) || 0;
    const e2e = sNs + xNs + qNs + oNs;
    totalNs += e2e;
    scanNs += sNs;
    xfmNs += xNs;
    queueNs += qNs;
    outNs += oNs;
    if (t.errors > 0) errors++;
  }

  // Use min/max of start times to get the true observation window
  let minT = Number(done[0].start_unix_ns) || 0,
    maxT = Number(done[0].start_unix_ns) || 0;
  for (const t of done) {
    const startT = Number(t.start_unix_ns) || 0;
    if (startT < minT) minT = startT;
    if (startT > maxT) maxT = startT;
  }
  const windowSec = Math.max(1, (maxT - minT) / 1e9);

  const sorted = done
    .map(
      (t) =>
        (Number(t.scan_ns) || 0) +
        (Number(t.transform_ns) || 0) +
        (Number(t.queue_wait_ns) || 0) +
        (Number(t.output_ns) || 0)
    )
    .sort((a, b) => a - b);
  const p90ns = sorted[Math.floor(sorted.length * 0.9)] ?? 0;

  return {
    batchPerMin: (done.length / windowSec) * 60,
    avgMs: totalNs / done.length / 1e6,
    p90Ms: p90ns / 1e6,
    scanPct: totalNs > 0 ? (scanNs / totalNs) * 100 : 0,
    xfmPct: totalNs > 0 ? (xfmNs / totalNs) * 100 : 0,
    queuePct: totalNs > 0 ? (queueNs / totalNs) * 100 : 0,
    outPct: totalNs > 0 ? (outNs / totalNs) * 100 : 0,
    errors,
  };
}

// ─── buildLanes ───────────────────────────────────────────────────────────────

/**
 * Partition traces into swim-lane groups:
 *   - lanes[0]: the scan lane (contains ALL traces for scan+transform rendering)
 *   - lanes[1..]: one lane per worker, sorted by worker id ascending
 *   - pendingTraces: in-progress output batches with no worker assigned yet
 */
export function buildLanes(traces: TraceRecord[]): LanesResult {
  const pendingTraces: TraceRecord[] = [];
  const byWorker = new Map<number, TraceRecord[]>();

  for (const t of traces) {
    if (t.worker_id >= 0) {
      if (!byWorker.has(t.worker_id)) byWorker.set(t.worker_id, []);
      byWorker.get(t.worker_id)?.push(t);
    } else if (t.in_progress && t.stage === "output") {
      pendingTraces.push(t);
    }
  }

  const scan: Lane = { workerId: SCAN_LANE, traces };
  const workers: Lane[] = [...byWorker.entries()]
    .sort(([a], [b]) => a - b)
    .map(([wid, ts]) => ({ workerId: wid, traces: ts }));

  return { lanes: [scan, ...workers], pendingTraces };
}
