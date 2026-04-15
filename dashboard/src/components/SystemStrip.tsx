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
    metricName: "process.memory.allocated",
    intervalMs: 2000,
    reduce: "last",
  });
  const memPoints = memFrame.series[0]?.points.map((p) => p.value) ?? [];
  const memLatest = memPoints.length > 0 ? memPoints[memPoints.length - 1] : 0;

  // Fallback to RSS if jemalloc not available.
  let memLabel = "Alloc";
  let displayPoints = memPoints;
  let displayLatest = memLatest;
  if (memPoints.length === 0) {
    const rssFrame = store.selectTimeSeries({
      metricName: "process.memory.rss",
      intervalMs: 2000,
      reduce: "last",
    });
    displayPoints = rssFrame.series[0]?.points.map((p) => p.value) ?? [];
    displayLatest = displayPoints.length > 0 ? displayPoints[displayPoints.length - 1] : 0;
    memLabel = "RSS";
  }

  // Inflight batches.
  const inflightFrame = store.selectTimeSeries({
    metricName: "logfwd.inflight_batches",
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
        <span class="sys-value">{fmtBytesCompact(displayLatest)}</span>
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
