import { fmtDuration } from "../lib/format";

interface Props {
  connected: boolean;
  totalErrors: number;
  version: string;
  uptime: number;
}

export function StatusBar({ connected, totalErrors, version, uptime }: Props) {
  const pillClass = connected
    ? totalErrors > 0 ? "pill pill-err" : "pill pill-ok"
    : "pill pill-off";
  const pillText = connected
    ? totalErrors > 0 ? `${totalErrors} errors` : "healthy"
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
