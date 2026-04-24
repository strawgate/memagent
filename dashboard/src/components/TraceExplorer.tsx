import { useEffect, useRef, useState } from "preact/hooks";
import { fmtNs } from "../lib/format";
import type { TraceRecord } from "../types";

// ─── colors ──────────────────────────────────────────────────────────────────

const C = {
  scan: "#3b82f6",
  transform: "#8b5cf6",
  output: "#10b981",
  send: "#34d399",
  recv: "#0891b2",
  error: "#ef4444",
  selected: "rgba(255,255,255,0.85)",
};

// ─── formatters ──────────────────────────────────────────────────────────────

function fmtRows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function fmtBytes(n: number): string {
  if (n >= 1_048_576) return `${(n / 1_048_576).toFixed(1)}MB`;
  if (n >= 1_024) return `${(n / 1_024).toFixed(0)}KB`;
  return `${n}B`;
}

// ─── stats ───────────────────────────────────────────────────────────────────

function fmtMs(ms: number): string {
  if (ms >= 10_000) return `${(ms / 1000).toFixed(0)}s`;
  if (ms >= 1_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${ms.toFixed(0)}ms`;
}

export function computeStats(traces: TraceRecord[]) {
  // Only use terminal traces for timing stats.
  const done = traces.filter((t) => t.lifecycle_state === "completed");
  if (done.length < 2) return null;
  let totalNs = 0,
    scanNs = 0,
    xfmNs = 0,
    outNs = 0,
    queueNs = 0,
    errors = 0;
  for (const t of done) {
    const e2e =
      Number(t.scan_ns) + Number(t.transform_ns) + Number(t.queue_wait_ns) + Number(t.output_ns);
    totalNs += e2e;
    scanNs += Number(t.scan_ns);
    xfmNs += Number(t.transform_ns);
    queueNs += Number(t.queue_wait_ns);
    outNs += Number(t.output_ns);
    if (t.errors > 0) errors++;
  }
  // Use min/max of start times to get the true observation window
  let minT = Number(done[0].start_unix_ns),
    maxT = Number(done[0].start_unix_ns);
  for (const t of done) {
    if (Number(t.start_unix_ns) < minT) minT = Number(t.start_unix_ns);
    if (Number(t.start_unix_ns) > maxT) maxT = Number(t.start_unix_ns);
  }
  const windowSec = Math.max(1, (Number(maxT) - Number(minT)) / 1e9);
  const sorted = done
    .map(
      (t) =>
        Number(t.scan_ns) + Number(t.transform_ns) + Number(t.queue_wait_ns) + Number(t.output_ns)
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

// ─── lanes ───────────────────────────────────────────────────────────────────
//
// Layout (top→bottom):
//   SCAN row (li=0)  — all traces, shows scan+transform history (persists)
//   [pending icon strip] — batches queued for a worker, not yet assigned
//   Workers (li=1..) — sorted W0, W1, … showing output phase

export const SCAN_LANE = -99;

export interface Lane {
  workerId: number; // SCAN_LANE | >=0 worker id
  traces: TraceRecord[];
}

interface LanesResult {
  lanes: Lane[];
  pendingTraces: TraceRecord[]; // shown as icon strip between scan and workers
}

export function buildLanes(traces: TraceRecord[]): LanesResult {
  const pendingTraces: TraceRecord[] = [];
  const byWorker = new Map<number, TraceRecord[]>();

  for (const t of traces) {
    const wid = t.worker_id ?? -1;
    if (wid >= 0) {
      if (!byWorker.has(wid)) byWorker.set(wid, []);
      byWorker.get(wid)?.push(t);
    } else if (t.lifecycle_state === "queued_for_output") {
      pendingTraces.push(t);
    }
  }

  // Scan lane gets ALL traces — it shows the scan+xfm segment for every batch.
  const scan: Lane = { workerId: SCAN_LANE, traces };
  const workers: Lane[] = [...byWorker.entries()]
    .sort(([a], [b]) => a - b)
    .map(([wid, ts]) => ({ workerId: wid, traces: ts }));

  return { lanes: [scan, ...workers], pendingTraces };
}

// ─── swimlane canvas ─────────────────────────────────────────────────────────

export const LANE_H = 13; // worker row height
export const SCAN_H = 13; // scan row height
const LANE_GAP = 3;
export const LABEL_W = 40; // left margin (worker / SCAN labels)
export const PEND_BOX_W = 72; // right column: pending-batch dot box
export const STRIDE = LANE_H + LANE_GAP;
const BAR_R = 3;
export const SECTION_GAP = 6;
const BAR_PAD = 3; // row padding; bar height is further scaled by batch size

// ─── layout types (exported for testing) ──────────────────────────────────────

export interface SegSpec {
  /** x offset from bar start in pixels */
  x: number;
  /** width in pixels */
  w: number;
  color: string;
  alpha: number;
  pulse: boolean;
}

export interface BarSpec {
  /** Lane index (0 = SCAN row, 1+ = worker rows) */
  li: number;
  /** Absolute x position in pixels (already includes LABEL_W) */
  x: number;
  /** y position */
  y: number;
  /** width */
  w: number;
  /** height */
  h: number;
  traceId: string;
  isSelected: boolean;
  isScanRow: boolean;
  /** Color segments to fill inside the clipped bar rect */
  segments: SegSpec[];
}

// li=0 : SCAN row  (y=1, h=SCAN_H)
// [SECTION_GAP]
// li=1+ : workers  (y = 1 + SCAN_H + SECTION_GAP + (li-1)*STRIDE)
//
// Right side: pending-batch dot box covers the workers area.

function laneY(li: number): number {
  if (li === 0) return 1;
  return 1 + SCAN_H + SECTION_GAP + (li - 1) * STRIDE;
}

function laneH(li: number): number {
  return li === 0 ? SCAN_H : LANE_H;
}

function canvasH(lanes: Lane[]): number {
  const workerCount = lanes.length - 1;
  return 1 + SCAN_H + SECTION_GAP + workerCount * STRIDE + 4;
}

/**
 * Pure layout function — computes bar positions and segments for the swimlane.
 * No canvas API calls. Exported for unit testing.
 *
 * All time values are in MILLISECONDS.
 */
export function layoutSwimlane(
  lanes: Lane[],
  windowMs: number,
  nowMs: number,
  selectedId: string | null,
  maxBytesIn: number,
  W: number // chart width = rect.width - LABEL_W - PEND_BOX_W
): BarSpec[] {
  if (lanes.length === 0) return [];

  const minMs = nowMs - windowMs;
  const chartRight = LABEL_W + W;
  const toXRaw = (ms: number) => LABEL_W + ((ms - minMs) / windowMs) * W;
  const isInProgress = (t: TraceRecord) => t.lifecycle_state !== "completed";
  const isScanLive = (t: TraceRecord) =>
    t.lifecycle_state === "scan_in_progress" || t.lifecycle_state === "transform_in_progress";
  const isOutputLive = (t: TraceRecord) => t.lifecycle_state === "output_in_progress";
  const isQueuedForOutput = (t: TraceRecord) => t.lifecycle_state === "queued_for_output";

  const sizeFrac = (t: TraceRecord) => {
    if (t.bytes_in > 0) return 0.5 + 0.5 * (t.bytes_in / maxBytesIn);
    return isInProgress(t) ? 1.0 : 0.75;
  };

  const bars: BarSpec[] = [];

  for (let li = 0; li < lanes.length; li++) {
    const lane = lanes[li];
    const isScan = lane.workerId === SCAN_LANE;
    const y = laneY(li);
    const h = laneH(li);

    for (const t of lane.traces) {
      const startMs = Number(t.start_unix_ns) / 1e6;
      let scanMs = Number(t.scan_ns) / 1e6;
      let xfmMs = Number(t.transform_ns) / 1e6;
      let outMs = Number(t.output_ns) / 1e6;

      if (isInProgress(t) && Number(t.lifecycle_state_start_unix_ns)) {
        const stageStartMs = Number(t.lifecycle_state_start_unix_ns) / 1e6;
        // Clamp: stageStart must be in the past and not older than 60s (stale guard)
        if (stageStartMs > 0 && stageStartMs <= nowMs && nowMs - stageStartMs < 60_000) {
          const live = Math.max(1, nowMs - stageStartMs);
          if (t.lifecycle_state === "scan_in_progress") scanMs = live;
          else if (t.lifecycle_state === "transform_in_progress") xfmMs = live;
          else if (t.lifecycle_state === "output_in_progress") outMs = live;
        }
      }

      if (isScan) {
        // Skip batches with no scan time yet, or that just finished (< 300ms old)
        // In-progress: clamp to nowMs so the bar grows with the clock.
        // Completed: use actual end position — no clamp. (Same reasoning as worker row:
        // clamping completed bars to nowMs pins them when the server clock is ahead.)
        const rawEndMs = startMs + scanMs + xfmMs;
        const barEndMs = isInProgress(t) ? Math.min(rawEndMs, nowMs) : rawEndMs;
        // Hide completed scan bars for 300ms after they finish to prevent flicker
        // during the active→completed transition. The barEndMs <= nowMs guard
        // avoids hiding bars whose end is in the future due to clock skew.
        if (!isInProgress(t) && barEndMs <= nowMs && nowMs - barEndMs < 300) continue;

        const isLive = isScanLive(t);
        const x0raw = toXRaw(startMs);
        const x1raw = toXRaw(barEndMs);
        if (x1raw < LABEL_W || x0raw > chartRight) continue;

        const barX = Math.max(LABEL_W, x0raw) + 0.5;
        const barW = Math.max(2, Math.min(chartRight, x1raw) - 0.5 - barX);
        const fullBarH = h - BAR_PAD * 2;
        const barH = Math.max(2, Math.round(fullBarH * sizeFrac(t)));
        const barY = y + Math.floor((h - barH) / 2);

        const segments: SegSpec[] = [];
        const baseAlpha = isLive ? 1 : 0.55;

        if (t.errors > 0) {
          // Error: single red segment spanning the whole bar
          segments.push({
            x: toXRaw(startMs),
            w: toXRaw(barEndMs) - toXRaw(startMs),
            color: C.error,
            alpha: baseAlpha,
            pulse: false,
          });
        } else {
          // Build scan + transform segments using the same cursor logic as drawSwimlane
          let curMs = startMs;
          const addSeg = (dur: number, color: string, pulse: boolean) => {
            const sx = toXRaw(curMs);
            const ex = toXRaw(curMs + dur);
            curMs += dur;
            if (ex <= sx) return;
            segments.push({ x: sx, w: ex - sx, color, alpha: baseAlpha, pulse });
          };
          addSeg(scanMs, C.scan, t.lifecycle_state === "scan_in_progress");
          addSeg(xfmMs, C.transform, t.lifecycle_state === "transform_in_progress");
        }

        bars.push({
          li,
          x: barX,
          y: barY,
          w: barW,
          h: barH,
          traceId: t.trace_id,
          isSelected: t.trace_id === selectedId,
          isScanRow: true,
          segments,
        });
      } else {
        // Worker row
        const sendMs = Number(t.send_ns ?? 0) / 1e6;
        const recvMs = Number(t.recv_ns ?? 0) / 1e6;

        const workerAssigned = isQueuedForOutput(t)
          ? false
          : (t.worker_id ?? -1) >= 0 && Number(t.output_start_unix_ns ?? "0") > 0;
        const outStartMs =
          Number(t.output_start_unix_ns ?? "0") > 0
            ? Number(t.output_start_unix_ns) / 1e6
            : startMs + scanMs + xfmMs;
        const actualQueueMs = Math.max(0, outStartMs - (startMs + scanMs + xfmMs));

        // Note: drawInFrac animation requires firstSeen state (not available here).
        // Worker-row bars are laid out without the draw-in animation — the
        // animation alpha is applied in drawSwimlane via fadeAlpha/drawInFrac.
        // TODO: thread firstSeen into layoutSwimlane if animation needs testing.
        let barEndMs: number;
        if (isQueuedForOutput(t) && !workerAssigned) {
          const stageStartMs = Number(t.lifecycle_state_start_unix_ns ?? "0") / 1e6;
          barEndMs =
            stageStartMs > 0
              ? Math.max(startMs + scanMs + xfmMs + 1, nowMs)
              : startMs + scanMs + xfmMs + 1;
        } else if (isInProgress(t)) {
          // In-progress: clamp to nowMs so the bar grows with the clock.
          barEndMs = Math.min(outStartMs + outMs, nowMs);
        } else {
          // Completed: use the actual end position — no nowMs clamp.
          // Clamping completed bars to nowMs pins them to the right edge when
          // the server clock is slightly ahead of the client clock.
          // The rendering layer (Math.min(chartRight, x1raw)) already handles
          // bars that extend past the visible area.
          barEndMs = outStartMs + outMs;
        }

        const x0raw = toXRaw(startMs);
        const x1raw = toXRaw(barEndMs);
        if (x1raw < LABEL_W || x0raw > chartRight) continue;

        const barX = Math.max(LABEL_W, x0raw) + 0.5;
        const barW = Math.max(2, Math.min(chartRight, x1raw) - 0.5 - barX);
        const fullBarH = h - BAR_PAD * 2;
        const barH = Math.max(2, Math.round(fullBarH * sizeFrac(t)));
        const barY = y + Math.floor((h - barH) / 2);

        const segments: SegSpec[] = [];

        if (isQueuedForOutput(t) && !workerAssigned) {
          // Queued: scan ghost → xfm ghost → growing gray queue wait
          let curMs = startMs;
          const addSeg = (dur: number, color: string, alpha: number, pulse: boolean) => {
            if (dur <= 0) return;
            const sx = toXRaw(curMs);
            const ex = toXRaw(curMs + dur);
            curMs += dur;
            const w = ex - sx;
            segments.push({ x: sx, w: Math.max(2, w), color, alpha, pulse });
          };
          addSeg(scanMs, C.scan, 0.38, false);
          addSeg(xfmMs, C.transform, 0.38, false);
          const queueNowMs = nowMs - (startMs + scanMs + xfmMs);
          addSeg(queueNowMs, "#374151", 0.7, true); // pulse handled by drawSwimlane
        } else if (t.errors > 0) {
          let curMs = startMs;
          const addSeg = (dur: number, color: string, alpha: number) => {
            if (dur <= 0) return;
            const sx = toXRaw(curMs);
            const ex = toXRaw(curMs + dur);
            curMs += dur;
            const w = ex - sx;
            segments.push({ x: sx, w: Math.max(2, w), color, alpha, pulse: false });
          };
          addSeg(scanMs, C.scan, 0.38);
          addSeg(xfmMs, C.transform, 0.38);
          if (actualQueueMs > 0) {
            const sx = toXRaw(startMs + scanMs + xfmMs);
            const ex = toXRaw(outStartMs);
            if (ex > sx)
              segments.push({
                x: sx,
                w: Math.max(1, ex - sx),
                color: "#374151",
                alpha: 0.55,
                pulse: false,
              });
          }
          curMs = outStartMs;
          addSeg(outMs, C.error, 1.0);
        } else {
          let curMs = startMs;
          const addSeg = (dur: number, color: string, alpha: number, pulse = false) => {
            if (dur <= 0) return;
            const sx = toXRaw(curMs);
            const ex = toXRaw(curMs + dur);
            curMs += dur;
            const w = ex - sx;
            segments.push({ x: sx, w: Math.max(2, w), color, alpha, pulse });
          };
          addSeg(scanMs, C.scan, 0.38);
          addSeg(xfmMs, C.transform, 0.38);
          if (actualQueueMs > 0) {
            const sx = toXRaw(startMs + scanMs + xfmMs);
            const ex = toXRaw(outStartMs);
            if (ex > sx)
              segments.push({
                x: sx,
                w: Math.max(1, ex - sx),
                color: "#374151",
                alpha: 0.55,
                pulse: false,
              });
          }
          curMs = outStartMs;
          if (sendMs > 0 || recvMs > 0) {
            const other = Math.max(0, outMs - sendMs - recvMs);
            if (other > 0) addSeg(other, C.output, 1.0);
            addSeg(sendMs, C.send, 1.0);
            addSeg(recvMs, C.recv, 1.0);
          } else {
            addSeg(outMs, C.output, 1.0, isOutputLive(t));
          }
        }

        bars.push({
          li,
          x: barX,
          y: barY,
          w: barW,
          h: barH,
          traceId: t.trace_id,
          isSelected: t.trace_id === selectedId,
          isScanRow: false,
          segments,
        });
      }
    }
  }

  return bars;
}

/**
 * Draw the butterfly swimlane.  All time values are in MILLISECONDS.
 *
 * Each batch lives in ONE row its whole life:
 *   - Worker rows: full bar scan(ghost)→xfm(ghost)→queue→send→recv
 *   - Pipeline center row: only the currently-in-progress scan/transform (single-threaded = at most 1)
 *   - Pending row: in-progress output with unknown worker (growing bar)
 *
 * firstSeen: mutated each frame — records when each trace_id first appeared in
 * a worker lane, used for 200ms fade-in.
 */
function drawSwimlane(
  canvas: HTMLCanvasElement,
  lanes: Lane[],
  windowMs: number,
  nowMs: number,
  selectedId: string | null,
  firstSeen: Map<string, number>,
  pendingTraces: TraceRecord[]
) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  if (rect.width === 0 || lanes.length === 0) return;

  const totalH = canvasH(lanes);
  const newW = Math.round(rect.width * dpr);
  const newH = Math.round(totalH * dpr);
  if (canvas.width !== newW) canvas.width = newW;
  if (canvas.height !== newH) {
    canvas.height = newH;
    canvas.style.height = `${totalH}px`;
  }

  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.clearRect(0, 0, newW, newH);
  ctx.save();
  ctx.scale(dpr, dpr);

  const W = rect.width - LABEL_W - PEND_BOX_W;
  const chartRight = LABEL_W + W;

  // Pre-compute max bytes_in for proportional bar-height scaling
  let maxBytesIn = 1;
  for (const lane of lanes) {
    for (const t of lane.traces) {
      if (t.bytes_in > maxBytesIn) maxBytesIn = t.bytes_in;
    }
  }

  // ── divider between scan and workers ──────────────────────────────────────
  if (lanes.length > 1) {
    const divY = laneY(1) - SECTION_GAP / 2;
    ctx.fillStyle = "rgba(255,255,255,0.07)";
    ctx.fillRect(LABEL_W, divY, W, 1);
  }

  // ── scan row background ───────────────────────────────────────────────────
  ctx.fillStyle = "rgba(255,255,255,0.025)";
  ctx.fillRect(LABEL_W, laneY(0), W, SCAN_H);

  // ── row backgrounds and labels ────────────────────────────────────────────
  for (let li = 0; li < lanes.length; li++) {
    const lane = lanes[li];
    const isScan = lane.workerId === SCAN_LANE;
    const y = laneY(li);
    const h = laneH(li);

    ctx.fillStyle = "rgba(255,255,255,0.02)";
    ctx.fillRect(LABEL_W, y, W, h);

    ctx.font = "9px monospace";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    if (isScan) {
      ctx.fillStyle = "rgba(100,116,139,0.6)";
      ctx.fillText("SCAN", LABEL_W - 4, y + h / 2);
    } else {
      const hasActive = lane.traces.some((t) => t.lifecycle_state !== "completed");
      ctx.fillStyle = hasActive
        ? `rgba(148,163,184,${0.45 + 0.3 * Math.abs(Math.sin(nowMs / 800))})`
        : "rgba(148,163,184,0.22)";
      ctx.fillText(`W${lane.workerId}`, LABEL_W - 4, y + h / 2);
    }
  }

  // ── per-bar rendering via pure layout function ────────────────────────────
  const bars = layoutSwimlane(lanes, windowMs, nowMs, selectedId, maxBytesIn, W);

  for (const bar of bars) {
    if (bar.isScanRow) {
      // Scan row: static alpha (isLive already baked into segment alphas via layoutSwimlane)
      // Re-derive isLive from whether any segment has alpha=1 (live bars) vs 0.55 (completed).
      // Actually layoutSwimlane sets baseAlpha on all segments. We apply it as ctx.globalAlpha.
      const baseAlpha = bar.segments.length > 0 ? bar.segments[0].alpha : 0.55;

      ctx.save();
      ctx.globalAlpha = baseAlpha;
      ctx.beginPath();
      ctx.roundRect(bar.x, bar.y, bar.w, bar.h, BAR_R);
      ctx.clip();

      for (const seg of bar.segments) {
        if (seg.pulse) {
          ctx.globalAlpha = baseAlpha > 0.9 ? 0.7 + 0.3 * Math.abs(Math.sin(nowMs / 400)) : 0.55;
        } else {
          ctx.globalAlpha = baseAlpha;
        }
        ctx.fillStyle = seg.color;
        ctx.fillRect(seg.x, bar.y, seg.w, bar.h);
      }
      ctx.restore();

      if (bar.isSelected) {
        ctx.save();
        ctx.strokeStyle = C.selected;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.roundRect(bar.x + 0.75, bar.y + 0.75, bar.w - 1.5, bar.h - 1.5, BAR_R);
        ctx.stroke();
        ctx.restore();
      }
    } else {
      // Worker row: apply fade-in animation (firstSeen state lives here in drawSwimlane)
      const traceId = bar.traceId;
      if (!firstSeen.has(traceId)) firstSeen.set(traceId, nowMs);
      const age = nowMs - (firstSeen.get(traceId) ?? nowMs);
      const fadeAlpha = Math.min(1, age / 150);

      ctx.save();
      ctx.globalAlpha = fadeAlpha;
      ctx.beginPath();
      ctx.roundRect(bar.x, bar.y, bar.w, bar.h, BAR_R);
      ctx.clip();

      for (const seg of bar.segments) {
        const pulseVal = seg.pulse ? 0.7 + 0.3 * Math.abs(Math.sin(nowMs / 400)) : 1;
        ctx.globalAlpha = fadeAlpha * seg.alpha * pulseVal;
        ctx.fillStyle = seg.color;
        ctx.fillRect(seg.x, bar.y, seg.w, bar.h);
      }
      ctx.restore();

      if (bar.isSelected) {
        ctx.save();
        ctx.strokeStyle = C.selected;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.roundRect(bar.x + 0.75, bar.y + 0.75, bar.w - 1.5, bar.h - 1.5, BAR_R);
        ctx.stroke();
        ctx.restore();
      }
    }
  }

  // ── pending-batch dot box (right column, spans scan row + workers) ────────
  if (lanes.length > 1) {
    const boxX = chartRight + 4;
    const boxW = PEND_BOX_W - 8;
    // Box extends from scan row top all the way to last worker row bottom
    const boxTop = 1;
    const boxBot = laneY(lanes.length - 1) + LANE_H;
    const boxH = boxBot - boxTop;

    // Box background
    ctx.fillStyle = pendingTraces.length > 0 ? "rgba(245,158,11,0.06)" : "rgba(255,255,255,0.015)";
    ctx.beginPath();
    ctx.roundRect(boxX, boxTop, boxW, boxH, 3);
    ctx.fill();

    // "PEND" header — vertically centered in the scan row
    ctx.font = "9px monospace";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillStyle = pendingTraces.length > 0 ? "rgba(245,158,11,0.4)" : "rgba(100,116,139,0.3)";
    ctx.fillText("PEND", boxX + boxW / 2, 1 + SCAN_H / 2);

    if (pendingTraces.length > 0) {
      const DOT = 5;
      const GAP = 2;
      const pulse = 0.45 + 0.3 * Math.abs(Math.sin(nowMs / 600));
      const perRow = Math.max(1, Math.floor((boxW - 4) / (DOT + GAP)));
      const dotsTop = laneY(1);
      const dotsH = boxBot - dotsTop;
      const OVERFLOW_H = 10; // reserve pixels for "+N" label when needed
      // First pass: how many rows fit without an overflow label?
      let maxRows = Math.max(1, Math.floor((dotsH - 2) / (DOT + GAP)));
      let maxDots = perRow * maxRows;
      // If there's overflow, shrink by one row to make room for the "+N" line
      if (pendingTraces.length > maxDots) {
        maxRows = Math.max(1, Math.floor((dotsH - 2 - OVERFLOW_H) / (DOT + GAP)));
        maxDots = perRow * maxRows;
      }
      const shown = Math.min(pendingTraces.length, maxDots);

      for (let i = 0; i < shown; i++) {
        const col = i % perRow;
        const row = Math.floor(i / perRow);
        const dotX = boxX + 2 + col * (DOT + GAP);
        const dotY = dotsTop + 1 + row * (DOT + GAP);
        const a = pulse * (1 - (row / (maxRows + 1)) * 0.25);
        ctx.fillStyle = `rgba(245,158,11,${a.toFixed(2)})`;
        ctx.beginPath();
        ctx.roundRect(dotX, dotY, DOT, DOT, 1);
        ctx.fill();
      }

      if (pendingTraces.length > maxDots) {
        ctx.font = "8px monospace";
        ctx.textAlign = "center";
        ctx.textBaseline = "bottom";
        ctx.fillStyle = `rgba(245,158,11,${pulse.toFixed(2)})`;
        ctx.fillText(`+${pendingTraces.length - maxDots}`, boxX + boxW / 2, boxBot - 2);
      }
    }
  }

  ctx.restore();
}

export function hitTest(
  lanes: Lane[],
  rect: DOMRect,
  clientX: number,
  clientY: number,
  windowMs: number,
  nowMs: number
): TraceRecord | null {
  const relY = clientY - rect.top;

  // Layout: scan (li=0), gap, workers (li=1+)
  const scanBottom = 1 + SCAN_H;
  const workersTop = 1 + SCAN_H + SECTION_GAP;

  let li: number;
  if (relY < scanBottom) {
    li = 0;
  } else if (relY < workersTop) {
    return null; // section gap
  } else {
    const offset = relY - workersTop;
    li = Math.min(lanes.length - 1, 1 + Math.max(0, Math.floor(offset / STRIDE)));
  }
  if (li < 0 || li >= lanes.length) return null;

  const W = rect.width - LABEL_W - PEND_BOX_W;
  const minMs = nowMs - windowMs;
  const clickMs = minMs + ((clientX - rect.left - LABEL_W) / W) * windowMs;
  const laneId = lanes[li].workerId;

  let best: TraceRecord | null = null;
  let bestDist = Infinity;
  for (const t of lanes[li].traces) {
    const scanMs = Number(t.scan_ns) / 1e6;
    const xfmMs = Number(t.transform_ns) / 1e6;
    const outMs = Number(t.output_ns) / 1e6;
    const sMs = Number(t.start_unix_ns) / 1e6;

    let midMs: number;
    if (laneId === SCAN_LANE) {
      midMs = sMs + (scanMs + xfmMs) / 2;
    } else {
      const outStartMs =
        Number(t.output_start_unix_ns ?? "0") > 0
          ? Number(t.output_start_unix_ns) / 1e6
          : sMs + scanMs + xfmMs;
      midMs = (sMs + outStartMs + outMs) / 2;
    }

    const d = Math.abs(midMs - clickMs);
    if (d < bestDist) {
      bestDist = d;
      best = t;
    }
  }
  return best;
}

// ─── detail panel ────────────────────────────────────────────────────────────

function DetailPanel({ t }: { t: TraceRecord }) {
  const e2e =
    Number(t.scan_ns) + Number(t.transform_ns) + Number(t.queue_wait_ns) + Number(t.output_ns);
  const pct = (ns: number) => (e2e > 0 ? `${((ns / e2e) * 100).toFixed(0)}%` : "–");
  const hasSendRecv = Number(t.send_ns ?? 0) > 0 || Number(t.recv_ns ?? 0) > 0;
  return (
    <div class="t2-detail">
      <div class="t2-stage-boxes">
        <div class="t2-stage-box" style={`border-top:2px solid ${C.scan}`}>
          <div class="t2-stage-label">scan</div>
          <div class="t2-stage-dur">{fmtNs(Number(t.scan_ns))}</div>
          <div class="t2-stage-sub">{pct(Number(t.scan_ns))}</div>
          {t.scan_rows > 0 && <div class="t2-stage-sub">{fmtRows(t.scan_rows)} rows</div>}
        </div>
        <div class="t2-stage-box" style={`border-top:2px solid ${C.transform}`}>
          <div class="t2-stage-label">transform</div>
          <div class="t2-stage-dur">{fmtNs(Number(t.transform_ns))}</div>
          <div class="t2-stage-sub">{pct(Number(t.transform_ns))}</div>
          <div class="t2-stage-sub">
            {fmtRows(t.input_rows)}→{fmtRows(t.output_rows)}
          </div>
        </div>
        {Number(t.queue_wait_ns) > 0 && (
          <div class="t2-stage-box" style="border-top:2px solid #6b7280">
            <div class="t2-stage-label">queue wait</div>
            <div class="t2-stage-dur">{fmtNs(Number(t.queue_wait_ns))}</div>
            <div class="t2-stage-sub">{pct(Number(t.queue_wait_ns))}</div>
          </div>
        )}
        {hasSendRecv ? (
          <>
            <div class="t2-stage-box" style={`border-top:2px solid ${C.send}`}>
              <div class="t2-stage-label">send req</div>
              <div class="t2-stage-dur">{fmtNs(Number(t.send_ns ?? 0))}</div>
              <div class="t2-stage-sub">{pct(Number(t.send_ns ?? 0))}</div>
              {(t.req_bytes ?? 0) > 0 && (
                <div class="t2-stage-sub">
                  {fmtBytes(t.req_bytes ?? 0)}
                  {(t.cmp_bytes ?? 0) > 0 &&
                    ` → ${fmtBytes(t.cmp_bytes ?? 0)} (${(((t.cmp_bytes ?? 0) / (t.req_bytes ?? 1)) * 100).toFixed(0)}%)`}
                </div>
              )}
            </div>
            <div class="t2-stage-box" style={`border-top:2px solid ${C.recv}`}>
              <div class="t2-stage-label">recv resp</div>
              <div class="t2-stage-dur">{fmtNs(Number(t.recv_ns ?? 0))}</div>
              <div class="t2-stage-sub">{pct(Number(t.recv_ns ?? 0))}</div>
              {(t.took_ms ?? 0) > 0 && <div class="t2-stage-sub">ES took {t.took_ms}ms</div>}
              {(t.resp_bytes ?? 0) > 0 && (
                <div class="t2-stage-sub">{fmtBytes(t.resp_bytes ?? 0)} received</div>
              )}
            </div>
          </>
        ) : (
          <div class="t2-stage-box" style={`border-top:2px solid ${C.output}`}>
            <div class="t2-stage-label">output</div>
            <div class="t2-stage-dur">{fmtNs(Number(t.output_ns))}</div>
            <div class="t2-stage-sub">{pct(Number(t.output_ns))}</div>
            <div class="t2-stage-sub">{fmtRows(t.output_rows)} rows sent</div>
          </div>
        )}
      </div>
      <div class="t2-detail-meta">
        {t.bytes_in > 0 && (
          <span>
            input <b>{fmtBytes(t.bytes_in)}</b>
          </span>
        )}
        <span>
          e2e <b>{fmtNs(e2e)}</b>
        </span>
        {(t.worker_id ?? -1) >= 0 && (
          <span>
            worker <b>{t.worker_id ?? -1}</b>
          </span>
        )}
        {(t.retries ?? 0) > 0 && (
          <span style="color:#f59e0b">
            retries <b>{t.retries}</b>
          </span>
        )}
        <span>
          flush <b>{t.flush_reason}</b>
        </span>
        {t.errors > 0 && (
          <span style={`color:${C.error}`}>
            errors <b>{t.errors}</b>
          </span>
        )}
        <span class="t2-traceid">id {t.trace_id.slice(0, 8)}…</span>
      </div>
    </div>
  );
}

// ─── main export ─────────────────────────────────────────────────────────────

const WINDOW_OPTIONS = [
  { label: "5s", ms: 5_000 },
  { label: "30s", ms: 30_000 },
  { label: "2m", ms: 120_000 },
  { label: "5m", ms: 300_000 },
];
const DEFAULT_WORKERS = 16;

/** Pick the tightest window that comfortably shows ~10 batches. */
function autoWindow(traces: TraceRecord[]): number {
  const done = traces.filter((t) => Number(t.total_ns) > 0);
  if (done.length === 0) return WINDOW_OPTIONS[1].ms; // default 30s until data arrives
  const avgMs = done.reduce((s, t) => s + Number(t.total_ns), 0) / done.length / 1e6;
  if (avgMs < 500) return WINDOW_OPTIONS[0].ms; // <0.5s batches → 5s window
  if (avgMs < 3_000) return WINDOW_OPTIONS[1].ms; // <3s batches → 30s window
  if (avgMs < 12_000) return WINDOW_OPTIONS[2].ms; // <12s batches → 2m window
  return WINDOW_OPTIONS[3].ms;
}

interface Props {
  traces: TraceRecord[];
}

export function TraceExplorer({ traces }: Props) {
  const [expanded, setExpanded] = useState<TraceRecord | null>(null);
  const [showAll, setShowAll] = useState(false);
  const [windowMs, setWindowMs] = useState(() => autoWindow(traces));
  const userPickedWindowRef = useRef(false);
  const autoWindowInitializedRef = useRef(traces.length > 0);

  const canvasRef = useRef<HTMLCanvasElement>(null);
  const lanesRef = useRef<Lane[]>([]);
  const allLanesRef = useRef<Lane[]>([]);
  const pendingTracesRef = useRef<TraceRecord[]>([]);
  const expandedIdRef = useRef<string | null>(null);
  const rafRef = useRef<number>(0);
  const windowMsRef = useRef<number>(windowMs);
  // Tracks first-seen time per trace_id in a worker lane, for fade-in animation.
  const firstSeenRef = useRef<Map<string, number>>(new Map());

  // Keep expanded detail fresh on each poll.
  useEffect(() => {
    if (!expanded) return;
    const refreshed = traces.find((t) => t.trace_id === expanded.trace_id);
    if (refreshed && refreshed !== expanded) setExpanded(refreshed);
  }, [traces, expanded?.trace_id, expanded]);

  // Auto-adjust window when new traces arrive, unless the user has picked one.
  useEffect(() => {
    if (userPickedWindowRef.current) return;
    if (autoWindowInitializedRef.current) return;
    if (traces.length === 0) return;
    const w = autoWindow(traces);
    autoWindowInitializedRef.current = true;
    if (w !== windowMs) setWindowMs(w);
  }, [traces, windowMs]);

  // Rebuild lanes: [scan, W0, W1, …].  Pending stored separately as icon strip.
  useEffect(() => {
    const { lanes: all, pendingTraces } = buildLanes(traces);
    allLanesRef.current = all;
    pendingTracesRef.current = pendingTraces;
    if (showAll) {
      lanesRef.current = all;
    } else {
      // all[0] = scan lane, all[1..] = workers
      const workers = all.slice(1);
      lanesRef.current = [all[0], ...workers.slice(0, DEFAULT_WORKERS)];
    }
    // Prune firstSeenRef: remove entries no longer in the active trace set.
    const activeIds = new Set(traces.map((t) => t.trace_id));
    for (const id of firstSeenRef.current.keys()) {
      if (!activeIds.has(id)) firstSeenRef.current.delete(id);
    }
  }, [traces, showAll]);

  useEffect(() => {
    expandedIdRef.current = expanded?.trace_id ?? null;
  }, [expanded]);
  useEffect(() => {
    windowMsRef.current = windowMs;
  }, [windowMs]);

  useEffect(() => {
    const draw = () => {
      const canvas = canvasRef.current;
      if (canvas && lanesRef.current.length > 0) {
        drawSwimlane(
          canvas,
          lanesRef.current,
          windowMsRef.current,
          Date.now(),
          expandedIdRef.current,
          firstSeenRef.current,
          pendingTracesRef.current
        );
      }
      rafRef.current = requestAnimationFrame(draw);
    };
    rafRef.current = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(rafRef.current);
  }, []);

  const stats = computeStats(traces);
  // allLanesRef = [scan, W0, W1, …]
  const allWorkers = Math.max(0, allLanesRef.current.length - 1); // exclude scan lane
  const shownWorkers = Math.max(0, lanesRef.current.length - 1);
  const hiddenCount = Math.max(0, allWorkers - shownWorkers);
  const hasSendRecv = traces.some((t) => Number(t.send_ns ?? 0) > 0);

  const handleClick = (e: MouseEvent) => {
    if (!canvasRef.current) return;
    const hit = hitTest(
      lanesRef.current,
      canvasRef.current.getBoundingClientRect(),
      e.clientX,
      e.clientY,
      windowMsRef.current,
      Date.now()
    );
    if (!hit) return;
    setExpanded(expanded?.trace_id === hit.trace_id ? null : hit);
  };

  if (traces.length === 0) {
    return <div class="log-empty">Waiting for batch spans…</div>;
  }

  return (
    <div class="t2-body">
      {/* Stats + window picker */}
      <div class="t2-topbar">
        {stats && (
          <div class="t2-stats">
            <span class="t2-stat">
              <b>{stats.batchPerMin.toFixed(0)}</b> batch/min
            </span>
            <span class="t2-stat">
              avg <b>{fmtMs(stats.avgMs)}</b> · p90 <b>{fmtMs(stats.p90Ms)}</b>
            </span>
            <div
              class="t2-breakdown-bar"
              title={`scan ${stats.scanPct.toFixed(0)}% · xfm ${stats.xfmPct.toFixed(0)}% · queue ${stats.queuePct.toFixed(0)}% · out ${stats.outPct.toFixed(0)}%`}
            >
              <div
                class="t2-bd-seg"
                style={`flex:${Math.max(stats.scanPct, 1)};background:${C.scan}`}
              >
                {stats.scanPct >= 6 && `${stats.scanPct.toFixed(0)}%`}
              </div>
              <div
                class="t2-bd-seg"
                style={`flex:${Math.max(stats.xfmPct, 0.5)};background:${C.transform}`}
              >
                {stats.xfmPct >= 6 && `${stats.xfmPct.toFixed(0)}%`}
              </div>
              <div
                class="t2-bd-seg"
                style={`flex:${Math.max(stats.queuePct, 1)};background:rgba(100,116,139,0.6)`}
              >
                {stats.queuePct >= 6 && `${stats.queuePct.toFixed(0)}%`}
              </div>
              <div
                class="t2-bd-seg"
                style={`flex:${Math.max(stats.outPct, 1)};background:${C.output}`}
              >
                {stats.outPct >= 6 && `${stats.outPct.toFixed(0)}%`}
              </div>
            </div>
            {stats.errors > 0 && (
              <span class="t2-stat" style={`color:${C.error}`}>
                {stats.errors} errors
              </span>
            )}
          </div>
        )}
        <div class="t2-window-picker">
          {WINDOW_OPTIONS.map((o) => (
            <button
              type="button"
              key={o.label}
              class={`t2-win-btn${windowMs === o.ms ? " active" : ""}`}
              onClick={() => {
                userPickedWindowRef.current = true;
                setWindowMs(o.ms);
              }}
            >
              {o.label}
            </button>
          ))}
        </div>
      </div>

      {/* Legend */}
      <div class="t2-legend">
        <span class="t2-legend-item">
          <span class="t2-swatch" style={`background:${C.scan}`} />
          scan
        </span>
        <span class="t2-legend-item">
          <span class="t2-swatch" style={`background:${C.transform}`} />
          transform
        </span>
        <span class="t2-legend-item">
          <span
            class="t2-swatch"
            style="background:rgba(107,114,128,0.45);border:1px solid rgba(107,114,128,0.6)"
          />
          queue wait
        </span>
        <span class="t2-legend-sep" />
        {hasSendRecv ? (
          <>
            <span class="t2-legend-item">
              <span class="t2-swatch" style={`background:${C.send}`} />
              send
            </span>
            <span class="t2-legend-item">
              <span class="t2-swatch" style={`background:${C.recv}`} />
              recv
            </span>
          </>
        ) : (
          <span class="t2-legend-item">
            <span class="t2-swatch" style={`background:${C.output}`} />
            output
          </span>
        )}
        <span class="t2-legend-note">click to inspect</span>
      </div>

      {/* Swimlane canvas */}
      <div class="t2-timeline-wrap">
        <canvas
          ref={canvasRef}
          class="t2-timeline"
          style="cursor:pointer;width:100%;height:auto"
          onClick={handleClick}
        />
      </div>

      {/* Show more/less worker rows */}
      {(showAll || hiddenCount > 0) && (
        <button type="button" class="t2-show-more" onClick={() => setShowAll((v) => !v)}>
          {showAll
            ? `▲ show fewer workers`
            : `▼ show ${hiddenCount} more worker${hiddenCount > 1 ? "s" : ""}`}
        </button>
      )}

      {/* Detail panel */}
      {expanded && <DetailPanel t={expanded} />}
    </div>
  );
}
