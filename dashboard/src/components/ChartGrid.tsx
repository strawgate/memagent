import type { MetricSeries } from "../app";
import { Chart } from "./Chart";

interface Props {
  series: MetricSeries[];
}

export function ChartGrid({ series }: Props) {
  return (
    <div class="chart-grid">
      {series.map((s) => (
        <div class="chart-card" key={s.id}>
          <div class="chart-head">
            <span class="chart-label">{s.label}</span>
            <span class="chart-val">
              {s.value}
              <span class="unit">{s.unit}</span>
              {s.limit && <span class="lim">{s.limit}</span>}
            </span>
          </div>
          <Chart series={s} />
        </div>
      ))}
    </div>
  );
}
