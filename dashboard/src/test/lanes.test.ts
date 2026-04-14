import { describe, expect, it } from "vitest";
import { buildLanes, SCAN_LANE } from "../components/TraceExplorer";
import type { TraceRecord } from "../types";

// ─── helpers ──────────────────────────────────────────────────────────────────

const BASE_NOW_NS = 1_700_000_000_000 * 1e6;

function makeTr(overrides: Partial<TraceRecord> = {}): TraceRecord {
  return {
    trace_id: "trace-1",
    pipeline: "test",
    start_unix_ns: BASE_NOW_NS - 5_000_000_000,
    total_ns: 500_000_000,
    scan_ns: 150_000_000,
    transform_ns: 10_000_000,
    output_ns: 300_000_000,
    output_start_unix_ns: BASE_NOW_NS - 5_000_000_000 + 160_000_000,
    scan_rows: 1000,
    input_rows: 1000,
    output_rows: 1000,
    bytes_in: 3_000_000,
    queue_wait_ns: 40_000_000,
    worker_id: 0,
    send_ns: 0,
    recv_ns: 0,
    took_ms: 500,
    retries: 0,
    req_bytes: 4_000_000,
    cmp_bytes: 0,
    resp_bytes: 200_000,
    flush_reason: "size",
    errors: 0,
    status: "unset",
    lifecycle_state: "completed",
    ...overrides,
  };
}

// ─── tests ────────────────────────────────────────────────────────────────────

describe("buildLanes", () => {
  // 1. All traces with worker_id >= 0 go into worker lanes
  it("traces with worker_id >= 0 appear in worker lanes", () => {
    const t0 = makeTr({ trace_id: "a", worker_id: 0 });
    const t1 = makeTr({ trace_id: "b", worker_id: 1 });
    const { lanes } = buildLanes([t0, t1]);

    // lanes[0] = scan, lanes[1+] = workers
    const workerLanes = lanes.filter((l) => l.workerId >= 0);
    const allWorkerTraces = workerLanes.flatMap((l) => l.traces);
    const ids = allWorkerTraces.map((t) => t.trace_id);
    expect(ids).toContain("a");
    expect(ids).toContain("b");
  });

  // 2. In-progress traces with worker_id=-1 and stage="output" go into pendingTraces
  it("in-progress traces with worker_id=-1 and stage=output go to pendingTraces", () => {
    const pending = makeTr({
      trace_id: "pending-1",
      worker_id: -1,
      in_progress: true,
      lifecycle_state: "queued_for_output",
    });
    const { pendingTraces, lanes } = buildLanes([pending]);

    expect(pendingTraces).toHaveLength(1);
    expect(pendingTraces[0].trace_id).toBe("pending-1");

    // Should NOT appear in any worker lane (no worker_id >= 0)
    const workerLanes = lanes.filter((l) => l.workerId >= 0);
    expect(workerLanes).toHaveLength(0);
  });

  // 3. Worker lanes are sorted by worker_id ascending
  it("worker lanes are sorted by worker_id ascending", () => {
    const t2 = makeTr({ trace_id: "w2", worker_id: 2 });
    const t0 = makeTr({ trace_id: "w0", worker_id: 0 });
    const t1 = makeTr({ trace_id: "w1", worker_id: 1 });
    const { lanes } = buildLanes([t2, t0, t1]);

    const workerLanes = lanes.filter((l) => l.workerId >= 0);
    const ids = workerLanes.map((l) => l.workerId);
    expect(ids).toEqual([0, 1, 2]);
  });

  // 4. Scan lane contains ALL traces
  it("scan lane (workerId=SCAN_LANE) contains all traces", () => {
    const t0 = makeTr({ trace_id: "a", worker_id: 0 });
    const t1 = makeTr({ trace_id: "b", worker_id: 1 });
    const pending = makeTr({
      trace_id: "c",
      worker_id: -1,
      in_progress: true,
      lifecycle_state: "queued_for_output",
    });
    const { lanes } = buildLanes([t0, t1, pending]);

    const scanLane = lanes.find((l) => l.workerId === SCAN_LANE);
    expect(scanLane).toBeDefined();
    const scanIds = scanLane?.traces.map((t) => t.trace_id);
    expect(scanIds).toContain("a");
    expect(scanIds).toContain("b");
    expect(scanIds).toContain("c");
    expect(scanIds).toHaveLength(3);
  });

  // 5. Scan lane is always the first lane (index 0)
  it("scan lane is always first in lanes array", () => {
    const t0 = makeTr({ worker_id: 0 });
    const { lanes } = buildLanes([t0]);
    expect(lanes[0].workerId).toBe(SCAN_LANE);
  });

  // 6. Empty input → scan lane with empty traces, no workers
  it("empty input produces scan lane with no traces and no workers", () => {
    const { lanes, pendingTraces } = buildLanes([]);
    expect(lanes).toHaveLength(1);
    expect(lanes[0].workerId).toBe(SCAN_LANE);
    expect(lanes[0].traces).toHaveLength(0);
    expect(pendingTraces).toHaveLength(0);
  });

  // 7. In-progress trace with worker_id=-1 but stage != "output" does NOT go to pending
  it("in-progress with worker_id=-1 but stage=scan does not go to pending", () => {
    const tr = makeTr({
      worker_id: -1,
      in_progress: true,
      lifecycle_state: "scan_in_progress",
    });
    const { pendingTraces } = buildLanes([tr]);
    expect(pendingTraces).toHaveLength(0);
  });

  // 8. Mix of completed and in-progress traces
  it("handles a mix of completed and in-progress traces correctly", () => {
    const completed = makeTr({
      trace_id: "done",
      worker_id: 0,
    });
    const inProgressWorker = makeTr({
      trace_id: "live-worker",
      worker_id: 1,
      in_progress: true,
      lifecycle_state: "output_in_progress",
    });
    const inProgressPending = makeTr({
      trace_id: "live-pending",
      worker_id: -1,
      in_progress: true,
      lifecycle_state: "queued_for_output",
    });
    const { lanes, pendingTraces } = buildLanes([completed, inProgressWorker, inProgressPending]);

    // Pending gets the unassigned in-progress output
    expect(pendingTraces.map((t) => t.trace_id)).toContain("live-pending");

    // Worker lanes contain both completed and in-progress with known worker_id
    const workerLanes = lanes.filter((l) => l.workerId >= 0);
    const allWorkerIds = workerLanes.flatMap((l) => l.traces.map((t) => t.trace_id));
    expect(allWorkerIds).toContain("done");
    expect(allWorkerIds).toContain("live-worker");

    // Scan lane contains all three
    const scanLane = lanes.find((l) => l.workerId === SCAN_LANE);
    expect(scanLane?.traces).toHaveLength(3);
  });

  // 9. Multiple traces on the same worker go into one lane
  it("multiple traces on the same worker_id share one lane", () => {
    const t1 = makeTr({ trace_id: "a", worker_id: 0 });
    const t2 = makeTr({ trace_id: "b", worker_id: 0 });
    const { lanes } = buildLanes([t1, t2]);

    const w0Lane = lanes.find((l) => l.workerId === 0);
    expect(w0Lane).toBeDefined();
    expect(w0Lane?.traces).toHaveLength(2);
    const ids = w0Lane?.traces.map((t) => t.trace_id);
    expect(ids).toContain("a");
    expect(ids).toContain("b");
  });
});
