import { fmt } from "../lib/format";
import type { StatsResponse } from "../types";

interface Props {
  stats: StatsResponse | null;
}

export function MetricBadges({ stats }: Props) {
  if (!stats) return null;

  const badges: Array<{ label: string; value: string; limit?: string; colorStyle?: string }> = [];

  if (stats.batches > 0) {
    badges.push({ label: "Batches", value: fmt(stats.batches) });
  }
  if (stats.backpressure_stalls > 0) {
    badges.push({ label: "Scan Stalls", value: fmt(stats.backpressure_stalls) });
  }

  if (stats.channel_capacity != null && stats.channel_capacity > 0 && stats.channel_depth != null) {
    const ratio = stats.channel_depth / stats.channel_capacity;
    let colorStyle = "";
    if (ratio > 0.8) {
      colorStyle = "color: var(--err)"; // Red
    } else if (ratio > 0.5) {
      colorStyle = "color: var(--warn)"; // Amber
    }
    badges.push({
      label: "Queue",
      value: `${stats.channel_depth}/${stats.channel_capacity}`,
      colorStyle,
    });
  }

  if (badges.length === 0) return null;

  return (
    <div class="badge-row">
      {badges.map((b) => (
        <div class="mbadge" key={b.label}>
          <span class="mbadge-label">{b.label}</span>
          <span class="mbadge-val" style={b.colorStyle}>
            {b.value}
          </span>
          {b.limit && <span class="mbadge-lim">{b.limit}</span>}
        </div>
      ))}
    </div>
  );
}
