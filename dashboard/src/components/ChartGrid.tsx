import type { TelemetryStore } from "@otlpkit/views";
import { Chart, type ChartConfig, SERIES_PALETTE } from "./Chart";

/** Interval for time-series bucketing (ms). */
const INTERVAL_MS = 2000;

interface Props {
  store: TelemetryStore;
  charts: ChartConfig[];
  tick: number;
  /** Pipeline keys to hide (toggled off in the legend). */
  hiddenPipelines?: Set<string>;
}

export function ChartGrid({ store, charts, tick: _tick, hiddenPipelines }: Props) {
  return (
    <div class="chart-grid">
      {charts.map((cfg) => {
        let frame = store.selectTimeSeries({
          metricName: cfg.metricName,
          intervalMs: INTERVAL_MS,
          reduce: "last",
        });
        // Filter hidden pipelines while preserving series index → color mapping.
        // We null-out hidden series' points so their palette slot is stable.
        if (hiddenPipelines && hiddenPipelines.size > 0 && cfg.splitBy) {
          frame = {
            ...frame,
            series: frame.series.map((s) =>
              hiddenPipelines.has(s.key) ? { ...s, points: [] } : s
            ),
          };
        }
        // Sum latest values across visible series.
        let displayVal = "-";
        let total = 0;
        let hasValue = false;
        for (const s of frame.series) {
          if (hiddenPipelines?.has(s.key)) continue;
          const last = s.points[s.points.length - 1];
          if (last != null) {
            total += last.value;
            hasValue = true;
          }
        }
        if (hasValue) {
          displayVal = cfg.fmtAxis ? cfg.fmtAxis(total) : String(Math.round(total));
        }
        return (
          <div class="chart-card" key={cfg.metricName}>
            <div class="chart-head">
              <span class="chart-label">{cfg.label}</span>
              <span class="chart-val">
                {displayVal}
                <span class="unit">{cfg.unit}</span>
              </span>
            </div>
            <Chart frame={frame} config={cfg} />
          </div>
        );
      })}
    </div>
  );
}

/** Discover pipeline names from the first splitBy chart's frame. */
export function discoverPipelines(store: TelemetryStore, charts: ChartConfig[]): string[] {
  const cfg = charts.find((c) => c.splitBy);
  if (!cfg) return [];
  const frame = store.selectTimeSeries({
    metricName: cfg.metricName,
    intervalMs: INTERVAL_MS,
    reduce: "last",
    splitBy: cfg.splitBy,
  });
  return frame.series.map((s) => s.key);
}

/** Clickable legend bar for toggling pipeline visibility. */
export function PipelineLegend({
  pipelines,
  hidden,
  onToggle,
}: {
  pipelines: string[];
  hidden: Set<string>;
  onToggle: (key: string) => void;
}) {
  if (pipelines.length <= 1) return null;
  return (
    <div class="pipeline-legend">
      {pipelines.map((name, i) => {
        const color = SERIES_PALETTE[i % SERIES_PALETTE.length];
        const off = hidden.has(name);
        return (
          <button
            type="button"
            key={name}
            class={`legend-pill ${off ? "off" : ""}`}
            onClick={() => onToggle(name)}
          >
            <span class="legend-swatch" style={`background:${off ? "var(--t4)" : color}`} />
            {name}
          </button>
        );
      })}
    </div>
  );
}
