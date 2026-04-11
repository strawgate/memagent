import { buildLanes, computeStats, SCAN_LANE } from "../components/TraceExplorer";
import type { TraceRecord } from "../types";

// ─── helpers ─────────────────────────────────────────────────────────────────

let nextId = 1;

/**
 * Create a minimal valid TraceRecord. All timing values are in nanoseconds.
 * Defaults give a 100ms scan, 50ms transform, 10ms queue, 200ms output batch.
 */
function makeTr(overrides: Partial<TraceRecord> = {}): TraceRecord {
  const id = nextId++;
  return {
    trace_id: `trace-${id}`,
    pipeline: "default",
    start_unix_ns: 1_000_000_000 * id, // 1 second apart
    total_ns: 360_000_000,
    scan_ns: 100_000_000, // 100ms
    transform_ns: 50_000_000, // 50ms
    queue_wait_ns: 10_000_000, // 10ms
    output_ns: 200_000_000, // 200ms
    scan_rows: 100,
    input_rows: 100,
    output_rows: 90,
    bytes_in: 4096,
    flush_reason: "size",
    errors: 0,
    status: "ok",
    worker_id: 0,
    ...overrides,
  };
}

beforeEach(() => {
  nextId = 1;
});

// ─── computeStats ─────────────────────────────────────────────────────────────

describe("computeStats", () => {
  it("returns null for empty array", () => {
    expect(computeStats([])).toBeNull();
  });

  it("returns null for single completed trace", () => {
    expect(computeStats([makeTr()])).toBeNull();
  });

  it("returns null when only in-progress traces exist", () => {
    const traces = [
      makeTr({ in_progress: true }),
      makeTr({ in_progress: true }),
      makeTr({ in_progress: true }),
    ];
    expect(computeStats(traces)).toBeNull();
  });

  it("returns null when only one completed trace and rest are in-progress", () => {
    const traces = [
      makeTr(), // completed
      makeTr({ in_progress: true }),
      makeTr({ in_progress: true }),
    ];
    expect(computeStats(traces)).toBeNull();
  });

  it("excludes in-progress traces from stats", () => {
    // two completed traces 1 s apart → window = 1 s → 120 batches/min
    const t1 = makeTr({ start_unix_ns: 0 });
    const t2 = makeTr({ start_unix_ns: 1_000_000_000 }); // 1 second later
    const tInProgress = makeTr({ in_progress: true, start_unix_ns: 999_999_999_999 });
    const stats = computeStats([t1, t2, tInProgress]);
    expect(stats).not.toBeNull();
    // window is determined only by completed traces
    expect(stats?.batchPerMin).toBeCloseTo(120, 0);
  });

  describe("with two or more completed traces", () => {
    it("returns a non-null result", () => {
      expect(computeStats([makeTr(), makeTr()])).not.toBeNull();
    });

    it("computes correct avgMs", () => {
      // Both traces: scan=100ms, xfm=50ms, queue=10ms, out=200ms → e2e=360ms each
      const traces = [makeTr({ start_unix_ns: 0 }), makeTr({ start_unix_ns: 1_000_000_000 })];
      const stats = computeStats(traces)!;
      expect(stats.avgMs).toBeCloseTo(360, 5);
    });

    it("computes correct p90Ms for two-item array", () => {
      // sorted = [360ms, 360ms]; index = floor(2*0.9) = 1 → 360ms
      const traces = [makeTr({ start_unix_ns: 0 }), makeTr({ start_unix_ns: 1_000_000_000 })];
      const stats = computeStats(traces)!;
      expect(stats.p90Ms).toBeCloseTo(360, 5);
    });

    it("computes p90Ms correctly for 10 traces with varying latencies", () => {
      // 10 traces with e2e values 100ms..1000ms (step 100ms)
      const traces = Array.from({ length: 10 }, (_, i) => {
        const totalNs = (i + 1) * 100_000_000; // 100ms, 200ms, …, 1000ms
        return makeTr({
          start_unix_ns: i * 1_000_000_000,
          scan_ns: totalNs,
          transform_ns: 0,
          queue_wait_ns: 0,
          output_ns: 0,
        });
      });
      const stats = computeStats(traces)!;
      // sorted = [100,200,...,1000]; index = floor(10*0.9) = 9 → 1000ms
      expect(stats.p90Ms).toBeCloseTo(1000, 5);
    });

    it("computes correct time percentage breakdowns", () => {
      // scan=100, xfm=50, queue=10, out=200 → total e2e=360
      // scanPct = 100/360*100, xfmPct = 50/360*100, etc.
      const traces = [makeTr({ start_unix_ns: 0 }), makeTr({ start_unix_ns: 1_000_000_000 })];
      const stats = computeStats(traces)!;
      expect(stats.scanPct).toBeCloseTo((100 / 360) * 100, 3);
      expect(stats.xfmPct).toBeCloseTo((50 / 360) * 100, 3);
      expect(stats.queuePct).toBeCloseTo((10 / 360) * 100, 3);
      expect(stats.outPct).toBeCloseTo((200 / 360) * 100, 3);
    });

    it("percentages sum to approximately 100", () => {
      const traces = [makeTr({ start_unix_ns: 0 }), makeTr({ start_unix_ns: 2_000_000_000 })];
      const stats = computeStats(traces)!;
      const total = stats.scanPct + stats.xfmPct + stats.queuePct + stats.outPct;
      expect(total).toBeCloseTo(100, 5);
    });

    it("computes scanPct=0 when total time is zero", () => {
      const t1 = makeTr({
        start_unix_ns: 0,
        scan_ns: 0,
        transform_ns: 0,
        queue_wait_ns: 0,
        output_ns: 0,
      });
      const t2 = makeTr({
        start_unix_ns: 1_000_000_000,
        scan_ns: 0,
        transform_ns: 0,
        queue_wait_ns: 0,
        output_ns: 0,
      });
      const stats = computeStats([t1, t2])!;
      expect(stats.scanPct).toBe(0);
      expect(stats.xfmPct).toBe(0);
      expect(stats.queuePct).toBe(0);
      expect(stats.outPct).toBe(0);
    });

    it("computes batchPerMin — two traces 1 s apart → 120 batches/min", () => {
      const t1 = makeTr({ start_unix_ns: 0 });
      const t2 = makeTr({ start_unix_ns: 1_000_000_000 });
      const stats = computeStats([t1, t2])!;
      // window = 1s, batches = 2, rate = 2/1*60 = 120
      expect(stats.batchPerMin).toBeCloseTo(120, 5);
    });

    it("clamps window to at least 1 s when all start times are equal", () => {
      // Same start_unix_ns → window = 0 → clamped to 1s
      const t1 = makeTr({ start_unix_ns: 5_000_000_000 });
      const t2 = makeTr({ start_unix_ns: 5_000_000_000 });
      const stats = computeStats([t1, t2])!;
      // 2 batches / 1 sec * 60 = 120
      expect(stats.batchPerMin).toBeCloseTo(120, 5);
    });
  });

  describe("error counting", () => {
    it("counts zero errors when no traces have errors", () => {
      const stats = computeStats([makeTr({ errors: 0 }), makeTr({ errors: 0 })])!;
      expect(stats.errors).toBe(0);
    });

    it("counts each trace with errors > 0 as one error (not sum of error counts)", () => {
      const traces = [
        makeTr({ errors: 3 }), // counted as 1 errored trace
        makeTr({ errors: 0 }),
        makeTr({ errors: 1 }), // counted as 1 errored trace
      ];
      const stats = computeStats(traces)!;
      expect(stats.errors).toBe(2);
    });

    it("counts all traces as errors when all have errors", () => {
      const traces = [makeTr({ errors: 1 }), makeTr({ errors: 5 }), makeTr({ errors: 2 })];
      const stats = computeStats(traces)!;
      expect(stats.errors).toBe(3);
    });

    it("does not count errors from in-progress traces", () => {
      const traces = [
        makeTr({ errors: 1, start_unix_ns: 0 }),
        makeTr({ errors: 0, start_unix_ns: 1_000_000_000 }),
        makeTr({ errors: 99, in_progress: true }), // excluded
      ];
      const stats = computeStats(traces)!;
      expect(stats.errors).toBe(1);
    });
  });
});

// ─── buildLanes ───────────────────────────────────────────────────────────────

describe("buildLanes", () => {
  it("returns a single scan lane for empty input", () => {
    const { lanes, pendingTraces } = buildLanes([]);
    expect(lanes).toHaveLength(1);
    expect(lanes[0].workerId).toBe(SCAN_LANE);
    expect(lanes[0].traces).toHaveLength(0);
    expect(pendingTraces).toHaveLength(0);
  });

  it("scan lane (index 0) always contains ALL traces", () => {
    const traces = [makeTr({ worker_id: 0 }), makeTr({ worker_id: 1 }), makeTr({ worker_id: 0 })];
    const { lanes } = buildLanes(traces);
    expect(lanes[0].workerId).toBe(SCAN_LANE);
    expect(lanes[0].traces).toHaveLength(3);
  });

  it("creates one worker lane per unique worker_id", () => {
    const traces = [
      makeTr({ worker_id: 0 }),
      makeTr({ worker_id: 1 }),
      makeTr({ worker_id: 0 }),
      makeTr({ worker_id: 2 }),
    ];
    const { lanes } = buildLanes(traces);
    // 1 scan lane + 3 worker lanes
    expect(lanes).toHaveLength(4);
  });

  it("worker lanes are sorted by worker_id ascending", () => {
    const traces = [makeTr({ worker_id: 3 }), makeTr({ worker_id: 1 }), makeTr({ worker_id: 2 })];
    const { lanes } = buildLanes(traces);
    const workerIds = lanes.slice(1).map((l) => l.workerId);
    expect(workerIds).toEqual([1, 2, 3]);
  });

  it("each worker lane contains only traces for that worker", () => {
    const t0a = makeTr({ worker_id: 0 });
    const t0b = makeTr({ worker_id: 0 });
    const t1 = makeTr({ worker_id: 1 });
    const { lanes } = buildLanes([t0a, t0b, t1]);

    const lane0 = lanes.find((l) => l.workerId === 0)!;
    const lane1 = lanes.find((l) => l.workerId === 1)!;
    expect(lane0.traces).toHaveLength(2);
    expect(lane1.traces).toHaveLength(1);
  });

  describe("pending traces", () => {
    it("collects in-progress output batches with no worker (worker_id < 0) as pending", () => {
      const pending = makeTr({ worker_id: -1, in_progress: true, stage: "output" });
      const { pendingTraces } = buildLanes([pending]);
      expect(pendingTraces).toHaveLength(1);
      expect(pendingTraces[0]).toBe(pending);
    });

    it("does not add pending traces to any worker lane", () => {
      const pending = makeTr({ worker_id: -1, in_progress: true, stage: "output" });
      const { lanes } = buildLanes([pending]);
      // only scan lane, no worker lanes
      expect(lanes).toHaveLength(1);
    });

    it("ignores in-progress traces not in output stage for pending", () => {
      // worker_id < 0 but stage = "scan" — not pending, not in any worker lane
      const scanning = makeTr({ worker_id: -1, in_progress: true, stage: "scan" });
      const { pendingTraces, lanes } = buildLanes([scanning]);
      expect(pendingTraces).toHaveLength(0);
      expect(lanes).toHaveLength(1); // only scan lane
    });

    it("completed traces with worker_id < 0 are not added to pending or worker lanes", () => {
      // worker_id = -1 but not in_progress → neither pending nor worker lane
      const done = makeTr({ worker_id: -1, in_progress: false });
      const { pendingTraces, lanes } = buildLanes([done]);
      expect(pendingTraces).toHaveLength(0);
      expect(lanes).toHaveLength(1); // scan lane only
    });

    it("mixes worker and pending traces correctly", () => {
      const w0 = makeTr({ worker_id: 0 });
      const w1 = makeTr({ worker_id: 1 });
      const p = makeTr({ worker_id: -1, in_progress: true, stage: "output" });
      const { lanes, pendingTraces } = buildLanes([w0, w1, p]);

      expect(pendingTraces).toHaveLength(1);
      expect(lanes).toHaveLength(3); // scan + worker 0 + worker 1
    });
  });
});
