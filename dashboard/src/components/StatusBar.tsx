import { fmtDuration } from "../lib/format";
import type { HealthState } from "../types";

interface Props {
  connected: boolean;
  wsConnected: boolean;
  componentHealth: HealthState;
  ready: "ready" | "not_ready";
  statusReason: string;
  totalErrors: number;
  version: string;
  uptime: number;
}

export function StatusBar({
  connected,
  wsConnected,
  componentHealth,
  ready,
  statusReason,
  totalErrors,
  version,
  uptime,
}: Props) {
  const pillClass = connected
    ? ready !== "ready" || componentHealth === "failed" || componentHealth === "stopped"
      ? "pill pill-off"
      : totalErrors > 0 || componentHealth === "degraded"
        ? "pill pill-err"
        : "pill pill-ok"
    : "pill pill-off";
  const pillText = connected
    ? ready !== "ready"
      ? "not ready"
      : componentHealth === "failed"
        ? "failed"
        : componentHealth === "stopped"
          ? "stopped"
          : totalErrors > 0
            ? `${totalErrors} errors`
            : componentHealth === "degraded"
              ? "degraded"
              : "healthy"
    : "disconnected";

  return (
    <div class="bar">
      <h1>logfwd</h1>
      <div class={pillClass} title={statusReason || undefined}>
        <span class="dot" />
        <span>{pillText}</span>
      </div>
      {connected && !wsConnected && (
        <div class="pill pill-warn" title="WebSocket disconnected — live metrics paused">
          <span>⚡ live data paused</span>
        </div>
      )}
      <div class="spacer" />
      <span class="bar-meta">
        v{version} &middot; {fmtDuration(uptime)}
      </span>
    </div>
  );
}
