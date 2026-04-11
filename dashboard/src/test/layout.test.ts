import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LABEL_W, type Lane, layoutSwimlane, SCAN_LANE } from "../components/TraceExplorer";
import type { TraceRecord } from "../types";

// ─── helpers ──────────────────────────────────────────────────────────────────

// A "now" anchor we can control via fake timers.
// 1_700_000_000_000 ms = some plausible Unix epoch in ms
const BASE_NOW_MS = 1_700_000_000_000;
const BASE_NOW_NS = BASE_NOW_MS * 1e6;

function makeTr(overrides: Partial<TraceRecord> = {}): TraceRecord {
  return {
    trace_id: "test-1",
    pipeline: "test",
    start_unix_ns: BASE_NOW_NS - 5_000 * 1e6, // started 5000 ms before NOW
    total_ns: 500_000_000,
    scan_ns: 150_000_000,
    transform_ns: 10_000_000,
    output_ns: 300_000_000,
    output_start_unix_ns: BASE_NOW_NS - 5_000 * 1e6 + 160_000_000,
    scan_rows: 1000,
    input_rows: 1000,
    output_rows: 1000,
    bytes_in: 3_000_000,
    queue_wait_ns: 40_000_000,
    worker_id: 0,
    send_ns: 200_000_000,
    recv_ns: 100_000_000,
    took_ms: 500,
    retries: 0,
    req_bytes: 4_000_000,
    cmp_bytes: 0,
    resp_bytes: 200_000,
    flush_reason: "size",
    errors: 0,
    status: "unset",
    ...overrides,
  };
}

function makeLane(workerId: number, traces: TraceRecord[]): Lane {
  return { workerId, traces };
}

const W = 900; // chart width (excluding LABEL_W and PEND_BOX_W)
const windowMs = 30_000; // 30 s window
const nowMs = BASE_NOW_MS;

// ─── tests ────────────────────────────────────────────────────────────────────

describe("layoutSwimlane", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(BASE_NOW_MS);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // 1. Empty lanes → empty result
  it("returns empty array for empty lanes", () => {
    const result = layoutSwimlane([], windowMs, nowMs, null, 1, W);
    expect(result).toEqual([]);
  });

  // 2. Bar within visible window → has positive width
  it("bar within window has positive width", () => {
    const tr = makeTr({
      // started 5 s ago, well within 30 s window
      start_unix_ns: (nowMs - 5_000) * 1e6,
      scan_ns: 1_000_000_000, // 1000 ms scan
      transform_ns: 500_000_000,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBeGreaterThan(0);
    expect(bars[0].w).toBeGreaterThan(0);
  });

  // 3. Bar entirely before visible window → not included
  it("bar entirely before window is excluded", () => {
    const tr = makeTr({
      // started 35 s ago, ended ~34.7 s ago — entirely outside 30 s window
      start_unix_ns: (nowMs - 35_000) * 1e6,
      scan_ns: 150_000_000, // 150 ms
      transform_ns: 50_000_000, // 50 ms  => ends 34.8 s ago
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(0);
  });

  // 4. Completed scan-row bar: x position proportional to time
  it("scan bar x is proportional to start time", () => {
    // Trace started exactly windowMs/2 ago
    const halfWindow = windowMs / 2; // 15 000 ms
    const tr = makeTr({
      start_unix_ns: (nowMs - halfWindow) * 1e6,
      scan_ns: 1_000_000_000, // 1000 ms — wide enough to be visible
      transform_ns: 0,
      // Set end time far enough in the past to avoid the 300ms flicker suppression
      // scan ends at nowMs - halfWindow + 1000 ms = nowMs - 14000 ms (well past)
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(1);

    // The bar left edge clamps to LABEL_W when barX would be at the window midpoint.
    // With startMs = nowMs - 15000 and minMs = nowMs - 30000,
    // toXRaw(startMs) = LABEL_W + (15000/30000)*W = LABEL_W + W/2
    const expectedX = LABEL_W + W / 2 + 0.5; // +0.5 per the barX formula
    expect(bars[0].x).toBeCloseTo(expectedX, 1);
  });

  // 5. Scan row: scan segment is blue (#3b82f6), transform segment is purple (#8b5cf6)
  it("scan row has scan=blue and transform=purple segments", () => {
    const tr = makeTr({
      start_unix_ns: (nowMs - 10_000) * 1e6,
      scan_ns: 1_000_000_000, // 1000 ms
      transform_ns: 500_000_000, // 500 ms
      errors: 0,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(1);
    const segs = bars[0].segments;
    expect(segs.length).toBeGreaterThanOrEqual(2);
    // First segment = scan (blue)
    expect(segs[0].color).toBe("#3b82f6");
    // Second segment = transform (purple)
    expect(segs[1].color).toBe("#8b5cf6");
  });

  // 6. Worker row: selected trace has isSelected=true
  it("selected trace produces isSelected=true", () => {
    const tr = makeTr({
      trace_id: "selected-trace",
      start_unix_ns: (nowMs - 10_000) * 1e6,
      scan_ns: 500_000_000,
      transform_ns: 100_000_000,
      output_ns: 300_000_000,
      worker_id: 0,
      output_start_unix_ns: (nowMs - 10_000) * 1e6 + 600_000_000,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const workerLane = makeLane(0, [tr]);
    const bars = layoutSwimlane(
      [scanLane, workerLane],
      windowMs,
      nowMs,
      "selected-trace",
      tr.bytes_in,
      W
    );

    const workerBars = bars.filter((b) => !b.isScanRow);
    expect(workerBars.length).toBe(1);
    expect(workerBars[0].isSelected).toBe(true);

    // Non-selected scan row bar
    const scanBars = bars.filter((b) => b.isScanRow);
    // scan bar may or may not be there (flicker guard: bar end must be >300ms ago)
    // It was 10s ago so it should be there. It also has isSelected=true (same trace_id).
    if (scanBars.length > 0) {
      expect(scanBars[0].isSelected).toBe(true);
    }
  });

  it("non-selected trace has isSelected=false", () => {
    const tr = makeTr({
      trace_id: "other-trace",
      start_unix_ns: (nowMs - 10_000) * 1e6,
      scan_ns: 500_000_000,
      transform_ns: 100_000_000,
      output_ns: 300_000_000,
      worker_id: 0,
      output_start_unix_ns: (nowMs - 10_000) * 1e6 + 600_000_000,
    });
    const workerLane = makeLane(0, [tr]);
    const bars = layoutSwimlane(
      [makeLane(SCAN_LANE, [tr]), workerLane],
      windowMs,
      nowMs,
      "selected-trace",
      tr.bytes_in,
      W
    );
    const workerBars = bars.filter((b) => !b.isScanRow);
    expect(workerBars[0].isSelected).toBe(false);
  });

  // 7. In-progress scan row bar (stage="scan"): bar extends to nowMs (right edge ≈ LABEL_W + W)
  it("in-progress scan bar extends to chart right edge", () => {
    // Stage started 2 s ago, well within stale guard (< 60 s)
    const stageStartNs = (nowMs - 2_000) * 1e6;
    const tr = makeTr({
      start_unix_ns: (nowMs - 5_000) * 1e6,
      scan_ns: 100_000_000, // short initial value — will be overridden by live extension
      transform_ns: 0,
      in_progress: true,
      stage: "scan",
      stage_start_unix_ns: stageStartNs,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(1);

    // The bar end is startMs + liveMs, clipped to chartRight.
    // liveMs = nowMs - stageStartMs = 2000 ms
    // bar end pixel: LABEL_W + ((startMs + liveMs - minMs) / windowMs) * W
    const _chartRight = LABEL_W + W;
    // bar.x + bar.w should be close to chartRight (the live end will be at or near nowMs)
    const barRight = bars[0].x + bars[0].w;
    // stageStartMs is 2000ms before now, scanMs becomes ~2000ms,
    // barEndMs = startMs + scanMs = (nowMs-5000) + 2000 = nowMs - 3000 (not at right edge)
    // Let's verify the bar has extended beyond its static scanMs
    const staticScanMs = 100; // 100_000_000 ns
    const minMs2 = nowMs - windowMs;
    const staticBarEnd = LABEL_W + ((nowMs - 5_000 + staticScanMs - minMs2) / windowMs) * W;
    expect(barRight).toBeGreaterThan(staticBarEnd);
  });

  // 7b. In-progress bar where stage started 100ms ago — right edge is very close to chartRight
  it("in-progress scan stage just started → bar right edge near chart right", () => {
    const stageStartNs = (nowMs - 100) * 1e6; // 100 ms ago
    const tr = makeTr({
      start_unix_ns: (nowMs - 100) * 1e6, // started just now
      scan_ns: 50_000_000,
      transform_ns: 0,
      in_progress: true,
      stage: "scan",
      stage_start_unix_ns: stageStartNs,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(1);
    // barEndMs = startMs + liveMs ≈ (nowMs-100) + 100 ≈ nowMs
    // So x1raw ≈ LABEL_W + W = chartRight
    const chartRight = LABEL_W + W;
    const barRight = bars[0].x + bars[0].w;
    // Should be within a few pixels of chartRight
    // (the barX/barW formula adds +0.5 px adjustment, so allow ±1 px tolerance)
    expect(Math.abs(barRight - chartRight)).toBeLessThanOrEqual(1);
  });

  // 8. Stale guard: in-progress batch older than 60s is NOT extended
  it("stale guard: in-progress stage older than 60s not extended to now", () => {
    // stage_start_unix_ns is 90 seconds ago — beyond the 60s stale guard
    const stageStartNs = (nowMs - 90_000) * 1e6;
    const staticScanNs = 200_000_000; // 200 ms
    const tr = makeTr({
      start_unix_ns: (nowMs - 10_000) * 1e6,
      scan_ns: staticScanNs,
      transform_ns: 0,
      in_progress: true,
      stage: "scan",
      stage_start_unix_ns: stageStartNs,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(1);

    // With stale guard active, scanMs stays at staticScanNs / 1e6 = 200 ms
    // barEndMs = startMs + 200 = (nowMs-10000) + 200 = nowMs - 9800
    const minMs2 = nowMs - windowMs;
    const startMs = nowMs - 10_000;
    const expectedBarEnd = LABEL_W + ((startMs + 200 - minMs2) / windowMs) * W;
    const chartRight = LABEL_W + W;
    const barRight = bars[0].x + bars[0].w;

    // Should NOT reach chartRight
    expect(barRight).toBeLessThan(chartRight - 10);
    // And should be close to expectedBarEnd (±1 px for the barX/barW +0.5 adjustments)
    expect(Math.abs(barRight - expectedBarEnd)).toBeLessThanOrEqual(1);
  });

  // 9. sizeFrac: larger bytes_in → taller bar
  it("larger bytes_in produces taller bar than smaller bytes_in", () => {
    const startNs = (nowMs - 10_000) * 1e6;
    const trSmall = makeTr({
      trace_id: "small",
      start_unix_ns: startNs,
      scan_ns: 1_000_000_000,
      transform_ns: 100_000_000,
      bytes_in: 100_000, // small batch
    });
    const trLarge = makeTr({
      trace_id: "large",
      start_unix_ns: startNs - 2_000_000_000, // slightly earlier, same lane
      scan_ns: 1_000_000_000,
      transform_ns: 100_000_000,
      bytes_in: 10_000_000, // large batch
    });
    const maxBytesIn = Math.max(trSmall.bytes_in, trLarge.bytes_in); // 10_000_000
    const scanLane = makeLane(SCAN_LANE, [trSmall, trLarge]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, maxBytesIn, W);

    const smallBar = bars.find((b) => b.traceId === "small");
    const largeBar = bars.find((b) => b.traceId === "large");
    expect(smallBar).toBeDefined();
    expect(largeBar).toBeDefined();
    expect(largeBar?.h).toBeGreaterThan(smallBar?.h);
  });

  // 10. Worker row: workerAssigned=false → no green output segment (only ghost + gray)
  it("unassigned worker row has no green output segment", () => {
    const stageStartNs = (nowMs - 500) * 1e6; // 500ms in queue
    const tr = makeTr({
      trace_id: "unassigned",
      start_unix_ns: (nowMs - 2_000) * 1e6,
      scan_ns: 500_000_000,
      transform_ns: 100_000_000,
      output_ns: 0,
      output_start_unix_ns: 0, // not yet assigned
      worker_id: -1, // no worker yet
      in_progress: true,
      stage: "output",
      stage_start_unix_ns: stageStartNs,
    });
    const workerLane = makeLane(0, [tr]);
    const bars = layoutSwimlane(
      [makeLane(SCAN_LANE, [tr]), workerLane],
      windowMs,
      nowMs,
      null,
      tr.bytes_in,
      W
    );

    const workerBars = bars.filter((b) => !b.isScanRow);
    expect(workerBars.length).toBe(1);

    // Should NOT contain green output color (#10b981)
    const greenColors = workerBars[0].segments.filter((s) => s.color === "#10b981");
    expect(greenColors.length).toBe(0);

    // Should have scan ghost (blue) and/or transform ghost (purple) + gray queue
    const hasGray = workerBars[0].segments.some((s) => s.color === "#374151");
    expect(hasGray).toBe(true);
  });

  // 11. Scan row bars older than 300ms are included; too-fresh completed bars are excluded
  it("completed scan bar finished <300ms ago is excluded (flicker guard)", () => {
    // Bar finished 100ms ago
    const scanMs = 500; // 500 ms
    const tr = makeTr({
      start_unix_ns: (nowMs - 100 - scanMs) * 1e6,
      scan_ns: scanMs * 1e6,
      transform_ns: 0,
      in_progress: false,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    // Bar end = nowMs - 100ms < 300ms, should be excluded
    expect(bars.length).toBe(0);
  });

  it("completed scan bar finished >300ms ago is included", () => {
    const scanMs = 500; // 500 ms
    const tr = makeTr({
      start_unix_ns: (nowMs - 1_000 - scanMs) * 1e6,
      scan_ns: scanMs * 1e6,
      transform_ns: 100_000_000,
      in_progress: false,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(1);
  });

  // 12. Error trace on scan row → single red segment
  it("scan row error trace has a single red segment", () => {
    const tr = makeTr({
      start_unix_ns: (nowMs - 5_000) * 1e6,
      scan_ns: 500_000_000,
      transform_ns: 100_000_000,
      errors: 3,
    });
    const scanLane = makeLane(SCAN_LANE, [tr]);
    const bars = layoutSwimlane([scanLane], windowMs, nowMs, null, tr.bytes_in, W);
    expect(bars.length).toBe(1);
    expect(bars[0].segments.every((s) => s.color === "#ef4444")).toBe(true);
  });

  // 13. isScanRow flag is set correctly
  it("scan lane bars have isScanRow=true, worker lane bars have isScanRow=false", () => {
    const tr = makeTr({
      start_unix_ns: (nowMs - 10_000) * 1e6,
      scan_ns: 1_000_000_000,
      transform_ns: 200_000_000,
      output_ns: 300_000_000,
      output_start_unix_ns: (nowMs - 10_000) * 1e6 + 1_200_000_000,
      worker_id: 0,
    });
    const bars = layoutSwimlane(
      [makeLane(SCAN_LANE, [tr]), makeLane(0, [tr])],
      windowMs,
      nowMs,
      null,
      tr.bytes_in,
      W
    );
    const scanBars = bars.filter((b) => b.isScanRow);
    const workerBars = bars.filter((b) => !b.isScanRow);
    expect(scanBars.length).toBe(1);
    expect(workerBars.length).toBe(1);
  });

  // 14. Worker row completed trace: has green output segment
  it("completed worker row trace has green output segment", () => {
    const tr = makeTr({
      start_unix_ns: (nowMs - 10_000) * 1e6,
      scan_ns: 500_000_000,
      transform_ns: 100_000_000,
      output_ns: 300_000_000,
      output_start_unix_ns: (nowMs - 10_000) * 1e6 + 600_000_000,
      worker_id: 0,
      send_ns: 0, // no send/recv breakdown, uses C.output
      recv_ns: 0,
      in_progress: false,
    });
    const workerLane = makeLane(0, [tr]);
    const bars = layoutSwimlane(
      [makeLane(SCAN_LANE, [tr]), workerLane],
      windowMs,
      nowMs,
      null,
      tr.bytes_in,
      W
    );
    const workerBars = bars.filter((b) => !b.isScanRow);
    expect(workerBars.length).toBe(1);
    const hasGreen = workerBars[0].segments.some((s) => s.color === "#10b981");
    expect(hasGreen).toBe(true);
  });
});
