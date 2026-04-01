import { fmtBytes, fmt } from "../lib/format";
import type { StatsResponse } from "../types";

interface Props {
  stats: StatsResponse | null;
}

export function MetricBadges({ stats }: Props) {
  if (!stats) return null;

  const badges: Array<{ label: string; value: string; limit?: string }> = [];

  if (stats.batches > 0) {
    badges.push({ label: "Batches", value: fmt(stats.batches) });
  }
  if (stats.backpressure_stalls > 0) {
    badges.push({ label: "Backpressure", value: fmt(stats.backpressure_stalls) });
  }
  if (stats.mem_allocated) {
    badges.push({
      label: "Heap",
      value: fmtBytes(stats.mem_allocated),
      limit: stats.mem_resident ? `/ ${fmtBytes(stats.mem_resident)}` : undefined,
    });
  }

  if (badges.length === 0) return null;

  return (
    <div class="badge-row">
      {badges.map((b) => (
        <div class="mbadge" key={b.label}>
          <span class="mbadge-label">{b.label}</span>
          <span class="mbadge-val">{b.value}</span>
          {b.limit && <span class="mbadge-lim">{b.limit}</span>}
        </div>
      ))}
    </div>
  );
}
