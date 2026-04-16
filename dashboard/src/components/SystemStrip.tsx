import type { TelemetryStore } from "@otlpkit/views";
import { fmtBytesCompact, fmtDuration } from "../lib/format";
import { Sparkline } from "./Sparkline";

interface Props {
  store: TelemetryStore;
  tick: number;
  uptimeSec: number;
}

/**
 * Compact horizontal strip showing system-level metrics with sparklines.
 * Replaces the 3-panel System Metrics chart grid.
 */
export function SystemStrip({ store, tick: _tick, uptimeSec }: Props) {
  // CPU percentage time series.
  const cpuFrame = store.selectTimeSeries({
    metricName: "logfwd.cpu_percent",
    intervalMs: 2000,
    reduce: "last",
  });
  const cpuPoints = cpuFrame.series[0]?.points.map((p) => p.value) ?? [];
  const cpuLatest = cpuPoints.length > 0 ? cpuPoints[cpuPoints.length - 1] : 0;

  // Memory (prefer jemalloc allocated, fallback to RSS).
  const memFrame = store.selectTimeSeries({
    metricName: "logfwd.memory.allocated",
    intervalMs: 2000,
    reduce: "last",
  });
  const memPoints = memFrame.series[0]?.points.map((p) => p.value) ?? [];
  const memLatest = memPoints.length > 0 ? memPoints[memPoints.length - 1] : 0;

  // RSS for comparison.
  const rssFrame = store.selectTimeSeries({
    metricName: "logfwd.memory.resident",
    intervalMs: 2000,
    reduce: "last",
  });
  const rssPoints = rssFrame.series[0]?.points.map((p) => p.value) ?? [];
  const rssLatest = rssPoints.length > 0 ? rssPoints[rssPoints.length - 1] : 0;

  // Display: show allocated if available, else RSS.
  let memLabel: string;
  let displayPoints: number[];
  let displayLatest: number;
  let memSecondary = "";
  if (memPoints.length > 0 && memLatest > 0) {
    memLabel = "Alloc";
    displayPoints = memPoints;
    displayLatest = memLatest;
    if (rssLatest > 0) memSecondary = ` / ${fmtBytesCompact(rssLatest)} RSS`;
  } else {
    memLabel = "RSS";
    displayPoints = rssPoints;
    displayLatest = rssLatest;
  }

  // Inflight batches.
  const inflightFrame = store.selectTimeSeries({
    metricName: "logfwd.batch.inflight",
    intervalMs: 2000,
    reduce: "last",
  });
  const inflightPoints = inflightFrame.series[0]?.points.map((p) => p.value) ?? [];
  const inflightLatest = inflightPoints.length > 0 ? inflightPoints[inflightPoints.length - 1] : 0;

  return (
    <div class="system-strip">
      <div class="sys-cell">
        <span class="sys-label">CPU</span>
        <Sparkline
          values={cpuPoints}
          width={48}
          height={16}
          color="var(--accent)"
          warnThreshold={70}
          errThreshold={90}
          style="area"
        />
        <span class="sys-value">{cpuLatest.toFixed(1)}%</span>
      </div>

      <div class="sys-divider" />

      <div class="sys-cell">
        <span class="sys-label">{memLabel}</span>
        <Sparkline
          values={displayPoints}
          width={48}
          height={16}
          color="var(--purple)"
          style="area"
        />
        <span class="sys-value">
          {fmtBytesCompact(displayLatest)}
          {memSecondary && <span class="sys-secondary">{memSecondary}</span>}
        </span>
      </div>

      <div class="sys-divider" />

      <div class="sys-cell">
        <span class="sys-label">Inflight</span>
        <Sparkline
          values={inflightPoints}
          width={48}
          height={16}
          color="var(--accent)"
          warnThreshold={6}
          errThreshold={8}
        />
        <span class="sys-value">{Math.round(inflightLatest)}</span>
      </div>

      <div class="sys-divider" />

      <div class="sys-cell">
        <span class="sys-label">Uptime</span>
        <span class="sys-value">{fmtDuration(uptimeSec)}</span>
      </div>
    </div>
  );
}
