import { useState, useEffect, useLayoutEffect, useRef } from "preact/hooks";
import { api } from "../api";
import type { TraceRecord } from "../types";

// ─── colors ──────────────────────────────────────────────────────────────────

const C = {
  scan:      "#3b82f6",
  transform: "#8b5cf6",
  output:    "#10b981",
  gap:       "#374151",
  error:     "#ef4444",
  slow:      "#f59e0b",
};

// ─── formatters ──────────────────────────────────────────────────────────────

function fmtNs(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`;
  if (ns >= 1_000)     return `${(ns / 1_000).toFixed(0)}µs`;
  return `${ns}ns`;
}

function fmtRows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function fmtBytes(n: number): string {
  if (n >= 1_048_576) return `${(n / 1_048_576).toFixed(1)}MB`;
  if (n >= 1_024)     return `${(n / 1_024).toFixed(0)}KB`;
  return `${n}B`;
}

function fmtThroughput(bytes: number, ns: number): string {
  if (ns <= 0 || bytes <= 0) return "";
  const mbps = (bytes / 1_048_576) / (ns / 1e9);
  if (mbps >= 100) return `${mbps.toFixed(0)} MB/s`;
  if (mbps >= 1)   return `${mbps.toFixed(1)} MB/s`;
  return `${(mbps * 1024).toFixed(0)} KB/s`;
}

// ─── stats ───────────────────────────────────────────────────────────────────

interface Stats {
  batchPerMin: number;
  throughputMBps: number;
  avgMs: number;
  scanPct: number;
  xfmPct: number;
  outPct: number;
  timeoutPct: number;
  errors: number;
}

function computeStats(traces: TraceRecord[]): Stats | null {
  if (traces.length < 2) return null;
  let bytes = 0, totalNs = 0, scanNs = 0, xfmNs = 0, outNs = 0, errors = 0, timeouts = 0;
  for (const t of traces) {
    bytes   += t.bytes_in;
    totalNs += t.total_ns;
    scanNs  += t.scan_ns;
    xfmNs   += t.transform_ns;
    outNs   += t.output_ns;
    if (t.errors > 0) errors++;
    if (t.flush_reason === "timeout") timeouts++;
  }
  const windowSec = Math.max(1,
    (traces[0].start_unix_ns - traces[traces.length - 1].start_unix_ns) / 1e9,
  );
  return {
    batchPerMin:     (traces.length / windowSec) * 60,
    throughputMBps:  (bytes / 1_048_576) / (totalNs / 1e9),
    avgMs:           totalNs / traces.length / 1e6,
    scanPct:         totalNs > 0 ? (scanNs / totalNs) * 100 : 0,
    xfmPct:          totalNs > 0 ? (xfmNs  / totalNs) * 100 : 0,
    outPct:          totalNs > 0 ? (outNs   / totalNs) * 100 : 0,
    timeoutPct:      (timeouts / traces.length) * 100,
    errors,
  };
}

function computeP90(traces: TraceRecord[]): number {
  if (traces.length === 0) return 0;
  const sorted = traces.map(t => t.total_ns).sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length * 0.9)];
}

// ─── timeline canvas ─────────────────────────────────────────────────────────

function drawTimeline(
  canvas: HTMLCanvasElement,
  traces: TraceRecord[],
  selectedId: string | null,
  _p90: number,
) {
  const dpr  = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  if (rect.width === 0 || rect.height === 0) return;

  canvas.width  = rect.width  * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext("2d")!;
  ctx.scale(dpr, dpr);

  const W = rect.width;
  const H = rect.height - 1; // 1px reserved for axis line

  // traces[0] is newest, traces[n-1] is oldest — map to left→right.
  const minT     = traces[traces.length - 1].start_unix_ns;
  const maxT     = traces[0].start_unix_ns + traces[0].total_ns;
  const timeSpan = maxT - minT;
  if (timeSpan <= 0) return;

  const maxDur = traces.reduce((m, t) => Math.max(m, t.total_ns), 1);

  // Draw from oldest to newest so newer bars paint over older ones on overlap.
  for (let i = traces.length - 1; i >= 0; i--) {
    const t    = traces[i];
    const x    = ((t.start_unix_ns - minT) / timeSpan) * W;
    const barW = Math.max(2, (t.total_ns / timeSpan) * W);

    if (t.errors > 0) {
      ctx.fillStyle = C.error;
      ctx.fillRect(x, H - (t.total_ns / maxDur) * H, barW, (t.total_ns / maxDur) * H);
    } else {
      // Stacked segments, drawn bottom-up: output → transform → scan → gap.
      let yOff = 0;
      const seg = (ns: number, color: string) => {
        const h = (ns / maxDur) * H;
        ctx.fillStyle = color;
        ctx.fillRect(x, H - yOff - h, barW, h);
        yOff += h;
      };
      seg(t.output_ns,    C.output);
      seg(t.transform_ns, C.transform);
      seg(t.scan_ns,      C.scan);
      const gap = Math.max(0, t.total_ns - t.scan_ns - t.transform_ns - t.output_ns);
      if (gap > 0) seg(gap, C.gap);
    }

    // White outline for selected batch.
    if (t.trace_id === selectedId) {
      const h = (t.total_ns / maxDur) * H;
      ctx.strokeStyle = "rgba(255,255,255,0.9)";
      ctx.lineWidth   = 1.5;
      ctx.strokeRect(x + 0.5, H - h + 0.5, Math.max(2, barW - 1), h - 1);
    }
  }

  // Axis line.
  ctx.fillStyle = "rgba(75,85,99,0.5)";
  ctx.fillRect(0, H, W, 1);
}

function hitTestTimeline(
  traces: TraceRecord[],
  canvasRect: DOMRect,
  clientX: number,
): TraceRecord | null {
  if (traces.length === 0) return null;
  const x      = clientX - canvasRect.left;
  const minT   = traces[traces.length - 1].start_unix_ns;
  const maxT   = traces[0].start_unix_ns + traces[0].total_ns;
  const clickT = minT + (x / canvasRect.width) * (maxT - minT);
  let best = traces[0];
  let bestDist = Math.abs(best.start_unix_ns - clickT);
  for (const t of traces) {
    const d = Math.abs(t.start_unix_ns - clickT);
    if (d < bestDist) { bestDist = d; best = t; }
  }
  return best;
}

// ─── detail panel ────────────────────────────────────────────────────────────

function DetailPanel({ t }: { t: TraceRecord }) {
  const gap = Math.max(0, t.total_ns - t.scan_ns - t.transform_ns - t.output_ns);

  return (
    <div class="t2-detail">
      <div class="t2-stage-boxes">
        <div class="t2-stage-box" style={`border-top:2px solid ${C.scan}`}>
          <div class="t2-stage-label">scan</div>
          <div class="t2-stage-dur">{fmtNs(t.scan_ns)}</div>
          {t.scan_rows > 0 && <div class="t2-stage-sub">{fmtRows(t.scan_rows)} rows</div>}
          {t.bytes_in > 0 && t.scan_ns > 0 && (
            <div class="t2-stage-sub">{fmtThroughput(t.bytes_in, t.scan_ns)}</div>
          )}
        </div>

        <div class="t2-stage-box" style={`border-top:2px solid ${C.transform}`}>
          <div class="t2-stage-label">transform</div>
          <div class="t2-stage-dur">{fmtNs(t.transform_ns)}</div>
          <div class="t2-stage-sub">{fmtRows(t.input_rows)}→{fmtRows(t.output_rows)} rows</div>
          {t.input_rows > 0 && t.output_rows < t.input_rows && (
            <div class="t2-stage-sub">
              {((1 - t.output_rows / t.input_rows) * 100).toFixed(0)}% filtered
            </div>
          )}
        </div>

        <div class="t2-stage-box" style={`border-top:2px solid ${C.output}`}>
          <div class="t2-stage-label">output</div>
          <div class="t2-stage-dur">{fmtNs(t.output_ns)}</div>
          <div class="t2-stage-sub">{fmtRows(t.output_rows)} rows sent</div>
        </div>

        {gap > t.total_ns * 0.05 && (
          <div class="t2-stage-box" style={`border-top:2px solid ${C.gap}`}>
            <div class="t2-stage-label">overhead</div>
            <div class="t2-stage-dur">{fmtNs(gap)}</div>
            <div class="t2-stage-sub">{((gap / t.total_ns) * 100).toFixed(0)}% of total</div>
          </div>
        )}
      </div>

      <div class="t2-detail-meta">
        {t.bytes_in > 0 && <span>input <b>{fmtBytes(t.bytes_in)}</b></span>}
        {t.queue_wait_ns > 0 && <span>queued <b>{fmtNs(t.queue_wait_ns)}</b></span>}
        <span>flush <b>{t.flush_reason}</b></span>
        {t.errors > 0 && <span style={`color:${C.error}`}>errors <b>{t.errors}</b></span>}
        <span class="t2-traceid">id {t.trace_id.slice(0, 8)}…</span>
      </div>
    </div>
  );
}

// ─── main component ───────────────────────────────────────────────────────────

const LIST_LIMIT = 100;

export function TraceExplorer() {
  const [traces, setTraces]   = useState<TraceRecord[]>([]);
  const [open, setOpen]       = useState(true);
  const [selected, setSelected] = useState<string | null>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    if (!open) return;
    let alive = true;
    const poll = async () => {
      const data = await api.traces();
      if (alive && data) setTraces(data.traces);
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => { alive = false; clearInterval(id); };
  }, [open]);

  useLayoutEffect(() => {
    if (!canvasRef.current || traces.length === 0) return;
    drawTimeline(canvasRef.current, traces, selected, computeP90(traces));
  }, [traces, selected]);

  const stats   = computeStats(traces);
  const p90     = computeP90(traces);
  const visible = traces.slice(0, LIST_LIMIT);

  const handleCanvasClick = (e: MouseEvent) => {
    if (!canvasRef.current) return;
    const hit = hitTestTimeline(traces, canvasRef.current.getBoundingClientRect(), e.clientX);
    if (hit) setSelected(hit.trace_id === selected ? null : hit.trace_id);
  };

  const icon = (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" style="flex-shrink:0">
      <rect x="1" y="3" width="5" height="3" rx="1" fill="currentColor" opacity="0.5" />
      <rect x="1" y="8" width="9" height="3" rx="1" fill="currentColor" opacity="0.5" />
      <rect x="7" y="3" width="8" height="3" rx="1" fill="currentColor" opacity="0.8" />
      <rect x="11" y="8" width="4" height="3" rx="1" fill="currentColor" opacity="0.8" />
    </svg>
  );

  return (
    <div class="t2-box">
      {/* Header */}
      <div class="t2-header">
        <div class="t2-header-left">
          {icon}
          <span>Batch Traces</span>
          {open && traces.length > 0 && (
            <span class="t2-count">{traces.length} buffered</span>
          )}
        </div>
        <button
          class="log-close"
          onClick={() => setOpen(!open)}
          aria-label={open ? "Collapse trace explorer" : "Expand trace explorer"}
        >
          {open ? "−" : "+"}
        </button>
      </div>

      {open && (
        <div class="t2-body">
          {traces.length === 0 ? (
            <div class="log-empty">Waiting for batch spans…</div>
          ) : (
            <>
              {/* Stats strip */}
              {stats && (
                <div class="t2-stats">
                  <span class="t2-stat"><b>{stats.batchPerMin.toFixed(0)}</b> batch/min</span>
                  {stats.throughputMBps >= 0.01 && (
                    <span class="t2-stat">
                      <b>{stats.throughputMBps >= 1
                        ? stats.throughputMBps.toFixed(0)
                        : stats.throughputMBps.toFixed(2)
                      }</b> MB/s
                    </span>
                  )}
                  <span class="t2-stat">avg <b>{stats.avgMs.toFixed(1)}ms</b></span>
                  <span class="t2-stat t2-stat-breakdown">
                    <span style={`color:${C.scan}`}>{stats.scanPct.toFixed(0)}%</span>
                    {" scan · "}
                    <span style={`color:${C.transform}`}>{stats.xfmPct.toFixed(0)}%</span>
                    {" xfm · "}
                    <span style={`color:${C.output}`}>{stats.outPct.toFixed(0)}%</span>
                    {" out"}
                  </span>
                  {stats.timeoutPct > 5 && (
                    <span class="t2-stat" style={`color:${C.slow}`}>
                      {stats.timeoutPct.toFixed(0)}% idle flush
                    </span>
                  )}
                  {stats.errors > 0 && (
                    <span class="t2-stat" style={`color:${C.error}`}>
                      {stats.errors} error{stats.errors > 1 ? "s" : ""}
                    </span>
                  )}
                </div>
              )}

              {/* Timeline */}
              <div class="t2-timeline-wrap">
                <canvas
                  ref={canvasRef}
                  class="t2-timeline"
                  onClick={handleCanvasClick}
                  title="Click to select a batch"
                />
                <div class="t2-legend">
                  {(["scan", "transform", "output"] as const).map(k => (
                    <span key={k} class="t2-legend-item">
                      <span class="t2-swatch" style={`background:${C[k]}`} />
                      {k}
                    </span>
                  ))}
                  <span class="t2-legend-note">height = duration</span>
                </div>
              </div>

              {/* Batch list */}
              <div class="t2-list">
                {visible.map((t) => {
                  const isSlow     = p90 > 0 && t.total_ns > p90;
                  const isSelected = selected === t.trace_id;
                  const gap        = Math.max(0, t.total_ns - t.scan_ns - t.transform_ns - t.output_ns);
                  const pct        = (ns: number) => `${((ns / t.total_ns) * 100).toFixed(1)}%`;

                  return (
                    <div
                      key={t.trace_id}
                      class={[
                        "t2-row",
                        isSelected && "t2-row-selected",
                        isSlow     && "t2-row-slow",
                      ].filter(Boolean).join(" ")}
                      onClick={() => setSelected(isSelected ? null : t.trace_id)}
                    >
                      {/* Row summary */}
                      <div class="t2-row-meta">
                        <span class="t2-pipeline">{t.pipeline}</span>
                        <span class="t2-dur">{fmtNs(t.total_ns)}</span>
                        <span class="t2-flow">{fmtRows(t.input_rows)}→{fmtRows(t.output_rows)}</span>
                        {t.bytes_in > 0 && (
                          <span class="t2-bytes">{fmtBytes(t.bytes_in)}</span>
                        )}
                        {t.errors > 0         && <span class="t2-badge t2-badge-err">err</span>}
                        {t.flush_reason === "timeout" && <span class="t2-badge t2-badge-idle">idle</span>}
                        {isSlow               && <span class="t2-badge t2-badge-slow">slow</span>}
                      </div>

                      {/* 100% stacked breakdown bar */}
                      <div class="t2-bar">
                        {t.scan_ns > 0 && (
                          <div class="t2-seg" style={`width:${pct(t.scan_ns)};background:${C.scan}`}
                            title={`scan ${fmtNs(t.scan_ns)}`} />
                        )}
                        {t.transform_ns > 0 && (
                          <div class="t2-seg" style={`width:${pct(t.transform_ns)};background:${C.transform}`}
                            title={`transform ${fmtNs(t.transform_ns)}`} />
                        )}
                        {t.output_ns > 0 && (
                          <div class="t2-seg" style={`width:${pct(t.output_ns)};background:${C.output}`}
                            title={`output ${fmtNs(t.output_ns)}`} />
                        )}
                        {gap > 0 && (
                          <div class="t2-seg" style={`width:${pct(gap)};background:${C.gap}`}
                            title={`overhead ${fmtNs(gap)}`} />
                        )}
                      </div>

                      {isSelected && <DetailPanel t={t} />}
                    </div>
                  );
                })}

                {traces.length > LIST_LIMIT && (
                  <div class="t2-overflow">
                    +{traces.length - LIST_LIMIT} older batches visible in timeline above
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
