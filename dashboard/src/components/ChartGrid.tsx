import type { TelemetryStore } from "@otlpkit/views";
import { Chart, type ChartConfig } from "./Chart";

/** Interval for time-series bucketing (ms). */
const INTERVAL_MS = 2000;

interface Props {
  store: TelemetryStore;
  charts: ChartConfig[];
  tick: number;
}

export function ChartGrid({ store, charts, tick: _tick }: Props) {
  return (
    <div class="chart-grid">
      {charts.map((cfg) => {
        const frame = store.selectTimeSeries({
          metricName: cfg.metricName,
          intervalMs: INTERVAL_MS,
          reduce: "last",
          ...(cfg.splitBy ? { splitBy: cfg.splitBy } : {}),
        });
        // Sum latest values across all series (e.g. multiple pipelines).
        let displayVal = "-";
        let total = 0;
        let hasValue = false;
        for (const s of frame.series) {
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
