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
        });
        const latest = frame.series[0]?.points;
        const lastPt = latest?.[latest.length - 1];
        const displayVal =
          lastPt != null
            ? cfg.fmtAxis
              ? cfg.fmtAxis(lastPt.value)
              : String(Math.round(lastPt.value))
            : "-";
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
