import type { BottleneckData } from "../types";

const STAGE_CONFIG: Record<
  BottleneckData["stage"],
  { emoji: string; label: string; cls: string }
> = {
  output: { emoji: "🔴", label: "output-bound", cls: "bottleneck-output" },
  input: { emoji: "🟠", label: "input-bound", cls: "bottleneck-input" },
  transform: { emoji: "🟡", label: "transform-bound", cls: "bottleneck-transform" },
  scan: { emoji: "🟡", label: "scan-bound", cls: "bottleneck-scan" },
  none: { emoji: "🟢", label: "healthy", cls: "bottleneck-none" },
};

export function BottleneckBadge({ b }: { b: BottleneckData }) {
  const { emoji, label, cls } = STAGE_CONFIG[b.stage] ?? STAGE_CONFIG.none;
  return (
    <span title={b.reason} class={`badge bottleneck-badge ${cls}`}>
      {emoji} {label}
    </span>
  );
}
