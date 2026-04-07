import { fmtDuration } from "../lib/format";

interface Props {
  connected: boolean;
  componentHealth: string;
  ready: "ready" | "not_ready";
  totalErrors: number;
  version: string;
  uptime: number;
}

export function StatusBar({ connected, componentHealth, ready, totalErrors, version, uptime }: Props) {
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
      <div class={pillClass}>
        <span class="dot" />
        <span>{pillText}</span>
      </div>
      <div class="spacer" />
      <span class="bar-meta">
        v{version} &middot; {fmtDuration(uptime)}
      </span>
    </div>
  );
}
