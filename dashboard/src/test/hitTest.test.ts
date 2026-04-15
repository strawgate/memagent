import { describe, expect, it } from "vitest";
import {
  hitTest,
  LABEL_W,
  LANE_H,
  type Lane,
  PEND_BOX_W,
  SCAN_H,
  SCAN_LANE,
  SECTION_GAP,
  STRIDE,
} from "../components/TraceExplorer";
import type { TraceRecord } from "../types";

// ─── helpers ──────────────────────────────────────────────────────────────────

const BASE_NOW_MS = 1_700_000_000_000;

function makeTr(overrides: Partial<TraceRecord> = {}): TraceRecord {
  return {
    trace_id: "test-1",
    pipeline: "test",
    start_unix_ns: String(BASE_NOW_MS * 1e6 - 5_000 * 1e6), // 5s before now
    total_ns: "500000000",
    scan_ns: "150000000",
    transform_ns: "10000000",
    output_ns: "300000000",
    output_start_unix_ns: String(BASE_NOW_MS * 1e6 - 5_000 * 1e6 + 160_000_000),
    scan_rows: 1000,
    input_rows: 1000,
    output_rows: 1000,
    bytes_in: 3_000_000,
    queue_wait_ns: "40000000",
    worker_id: 0,
    send_ns: "200000000",
    recv_ns: "100000000",
    took_ms: 500,
    retries: 0,
    req_bytes: 4_000_000,
    cmp_bytes: 0,
    resp_bytes: 200_000,
    flush_reason: "size",
    errors: 0,
    status: "ok",
    lifecycle_state: "completed",
    ...overrides,
  };
}

function makeLane(workerId: number, traces: TraceRecord[]): Lane {
  return { workerId, traces };
}

// Canvas dimensions: 1000px wide, tall enough for scan + gap + 3 worker lanes
const CANVAS_WIDTH = 1000;
const CANVAS_TOP = 100;
const CANVAS_LEFT = 50;

function makeRect(): DOMRect {
  return {
    top: CANVAS_TOP,
    left: CANVAS_LEFT,
    width: CANVAS_WIDTH,
    height: 200,
    bottom: 300,
    right: 1050,
    x: CANVAS_LEFT,
    y: CANVAS_TOP,
    toJSON: () => {},
  };
}

const W = CANVAS_WIDTH - LABEL_W - PEND_BOX_W; // usable chart width
const windowMs = 30_000;
const nowMs = BASE_NOW_MS;

// ─── tests ────────────────────────────────────────────────────────────────────

describe("hitTest", () => {
  const tr1 = makeTr({ trace_id: "t1" });
  const tr2 = makeTr({ trace_id: "t2", start_unix_ns: String(BASE_NOW_MS * 1e6 - 2_000 * 1e6) });
  const scanLane = makeLane(SCAN_LANE, [tr1, tr2]);
  const workerLane0 = makeLane(0, [tr1]);
  const workerLane1 = makeLane(1, [tr2]);
  const lanes: Lane[] = [scanLane, workerLane0, workerLane1];
  const rect = makeRect();

  it("returns null for empty lanes", () => {
    const result = hitTest([], rect, CANVAS_LEFT + LABEL_W + 100, CANVAS_TOP + 2, windowMs, nowMs);
    expect(result).toBeNull();
  });

  it("selects scan lane for clicks in the scan row region", () => {
    // Click in the scan row (y within [0, 1 + SCAN_H))
    const y = CANVAS_TOP + 5; // well within scan region
    // Place click near tr2 (more recent start) in time
    const tr2StartMs = Number(tr2.start_unix_ns) / 1e6;
    const minMs = nowMs - windowMs;
    const xFrac = (tr2StartMs - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;

    const result = hitTest(lanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("t2");
  });

  it("returns null for clicks in the section gap between scan and workers", () => {
    const scanBottom = 1 + SCAN_H;
    const workersTop = 1 + SCAN_H + SECTION_GAP;
    // Click in the gap between scanBottom and workersTop
    const y = CANVAS_TOP + (scanBottom + workersTop) / 2;
    const x = CANVAS_LEFT + LABEL_W + 100;

    const result = hitTest(lanes, rect, x, y, windowMs, nowMs);
    expect(result).toBeNull();
  });

  it("maps clicks below the gap to the correct worker lane", () => {
    const workersTop = 1 + SCAN_H + SECTION_GAP;
    // Click in worker lane 0 (li=1): y offset = workersTop + some pixels within first STRIDE
    const y = CANVAS_TOP + workersTop + LANE_H / 2;
    // Place click near tr1's time
    const tr1StartMs = Number(tr1.start_unix_ns) / 1e6;
    const minMs = nowMs - windowMs;
    const xFrac = (tr1StartMs - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;

    const result = hitTest(lanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("t1");
  });

  it("maps clicks in the second worker lane correctly", () => {
    const workersTop = 1 + SCAN_H + SECTION_GAP;
    // Click in worker lane 1 (li=2): y offset = workersTop + 1 stride + half lane
    const y = CANVAS_TOP + workersTop + STRIDE + LANE_H / 2;
    const tr2StartMs = Number(tr2.start_unix_ns) / 1e6;
    const minMs = nowMs - windowMs;
    const xFrac = (tr2StartMs - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;

    const result = hitTest(lanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("t2");
  });

  it("clamps lane index to last lane when clicking far below", () => {
    const workersTop = 1 + SCAN_H + SECTION_GAP;
    // Click very far below — should clamp to last worker lane (li = lanes.length - 1)
    const y = CANVAS_TOP + workersTop + 10 * STRIDE;
    const tr2StartMs = Number(tr2.start_unix_ns) / 1e6;
    const minMs = nowMs - windowMs;
    const xFrac = (tr2StartMs - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;

    const result = hitTest(lanes, rect, x, y, windowMs, nowMs);
    // Should hit the last lane (workerLane1 with tr2)
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("t2");
  });

  it("falls back to first lane when click is above canvas top", () => {
    // Click above the canvas top — relY would be negative
    const y = CANVAS_TOP - 50;
    const x = CANVAS_LEFT + LABEL_W + 100;

    const result = hitTest(lanes, rect, x, y, windowMs, nowMs);
    // relY < 0 → li = 0 (scan lane), so it will match scan unless lanes is empty
    // With non-empty lanes, relY < scanBottom (14) is true for relY < 0
    // So it picks scan lane — this tests that it doesn't crash
    expect(result).not.toBeNull();
  });

  it("picks the closest trace by midpoint distance in scan lane", () => {
    // Two traces far apart in time
    const trEarly = makeTr({
      trace_id: "early",
      start_unix_ns: String((nowMs - 25_000) * 1e6),
    });
    const trLate = makeTr({
      trace_id: "late",
      start_unix_ns: String((nowMs - 3_000) * 1e6),
    });
    const scanOnly = makeLane(SCAN_LANE, [trEarly, trLate]);
    const testLanes: Lane[] = [scanOnly];

    // Click at a time closer to trLate
    const minMs = nowMs - windowMs;
    const clickTimeMs = nowMs - 3_050; // 3050ms ago → closest to trLate's start
    const xFrac = (clickTimeMs - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;
    const y = CANVAS_TOP + 5;

    const result = hitTest(testLanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("late");
  });

  it("picks the closest trace by midpoint distance in worker lane", () => {
    const trEarly = makeTr({
      trace_id: "w-early",
      start_unix_ns: String((nowMs - 20_000) * 1e6),
      output_start_unix_ns: String((nowMs - 19_840) * 1e6),
      worker_id: 0,
    });
    const trLate = makeTr({
      trace_id: "w-late",
      start_unix_ns: String((nowMs - 5_000) * 1e6),
      output_start_unix_ns: String((nowMs - 4_840) * 1e6),
      worker_id: 0,
    });
    const workerLane = makeLane(0, [trEarly, trLate]);
    const testLanes: Lane[] = [makeLane(SCAN_LANE, []), workerLane];
    const workersTop = 1 + SCAN_H + SECTION_GAP;
    const y = CANVAS_TOP + workersTop + LANE_H / 2;

    // Click at time closer to trLate
    const minMs = nowMs - windowMs;
    const clickTimeMs = nowMs - 5_100;
    const xFrac = (clickTimeMs - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;

    const result = hitTest(testLanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("w-late");
  });

  it("uses scan+xfm midpoint for scan lane traces", () => {
    // Trace with large scan_ns, small transform_ns → midpoint biased toward start
    const tr = makeTr({
      trace_id: "scan-mid",
      start_unix_ns: String((nowMs - 10_000) * 1e6),
      scan_ns: "4000000000", // 4000ms scan
      transform_ns: "0",
    });
    const scanOnly = makeLane(SCAN_LANE, [tr]);
    const testLanes: Lane[] = [scanOnly];

    // Midpoint in scan lane = sMs + (scanMs + xfmMs) / 2 = (nowMs - 10000) + 2000 = nowMs - 8000
    const expectedMid = nowMs - 8_000;
    const minMs = nowMs - windowMs;
    const xFrac = (expectedMid - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;
    const y = CANVAS_TOP + 5;

    const result = hitTest(testLanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("scan-mid");
  });

  it("uses output_start_unix_ns for worker lane midpoint when available", () => {
    // output_start_unix_ns shifts the midpoint calculation
    const tr = makeTr({
      trace_id: "out-start",
      start_unix_ns: String((nowMs - 10_000) * 1e6),
      output_start_unix_ns: String((nowMs - 9_000) * 1e6),
      output_ns: "2000000000", // 2000ms output
    });
    const workerLane = makeLane(0, [tr]);
    const testLanes: Lane[] = [makeLane(SCAN_LANE, []), workerLane];
    const workersTop = 1 + SCAN_H + SECTION_GAP;
    const y = CANVAS_TOP + workersTop + LANE_H / 2;

    // Worker midpoint = (sMs + outStartMs + outMs) / 2
    // sMs = nowMs - 10000, outStartMs = nowMs - 9000, outMs = 2000
    // midMs = ((nowMs - 10000) + (nowMs - 9000) + 2000) / 2 = (2*nowMs - 17000) / 2
    const sMs = nowMs - 10_000;
    const outStartMs = nowMs - 9_000;
    const outMs = 2_000;
    const expectedMid = (sMs + outStartMs + outMs) / 2;
    const minMs = nowMs - windowMs;
    const xFrac = (expectedMid - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;

    const result = hitTest(testLanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("out-start");
  });

  it("falls back to scan+xfm for worker midpoint when output_start_unix_ns is missing", () => {
    const tr = makeTr({
      trace_id: "no-out-start",
      start_unix_ns: String((nowMs - 10_000) * 1e6),
      scan_ns: "200000000", // 200ms
      transform_ns: "100000000", // 100ms
      output_ns: "500000000", // 500ms
      output_start_unix_ns: undefined,
    });
    const workerLane = makeLane(0, [tr]);
    const testLanes: Lane[] = [makeLane(SCAN_LANE, []), workerLane];
    const workersTop = 1 + SCAN_H + SECTION_GAP;
    const y = CANVAS_TOP + workersTop + LANE_H / 2;

    // Fallback: outStartMs = sMs + scanMs + xfmMs = (nowMs - 10000) + 200 + 100
    const sMs = nowMs - 10_000;
    const outStartMs = sMs + 200 + 100;
    const outMs = 500;
    const expectedMid = (sMs + outStartMs + outMs) / 2;
    const minMs = nowMs - windowMs;
    const xFrac = (expectedMid - minMs) / windowMs;
    const x = CANVAS_LEFT + LABEL_W + xFrac * W;

    const result = hitTest(testLanes, rect, x, y, windowMs, nowMs);
    expect(result).not.toBeNull();
    expect(result?.trace_id).toBe("no-out-start");
  });

  it("returns null for a lane with no traces", () => {
    const emptyLanes: Lane[] = [makeLane(SCAN_LANE, [])];
    const y = CANVAS_TOP + 5; // scan region
    const x = CANVAS_LEFT + LABEL_W + 100;

    const result = hitTest(emptyLanes, rect, x, y, windowMs, nowMs);
    expect(result).toBeNull();
  });
});
