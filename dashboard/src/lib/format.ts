export function fmt(n: number | null | undefined): string {
  if (n == null) return "-";
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}G`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return String(Math.round(n));
}

export function fmtCompact(n: number): string {
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(0)}K`;
  if (n >= 1) return n.toFixed(0);
  return n < 0.01 ? "0" : n.toFixed(1);
}

export function fmtBytes(b: number | null | undefined): string {
  if (b == null || b === 0) return "-";
  if (b >= 1073741824) return `${(b / 1073741824).toFixed(1)} GB`;
  if (b >= 1048576) return `${(b / 1048576).toFixed(1)} MB`;
  if (b >= 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${b} B`;
}

export function fmtBytesCompact(b: number): string {
  if (b >= 1073741824) return `${(b / 1073741824).toFixed(1)}G`;
  if (b >= 1048576) return `${(b / 1048576).toFixed(0)}M`;
  if (b >= 1024) return `${(b / 1024).toFixed(0)}K`;
  return `${Math.round(b)}B`;
}

export function fmtNs(ns: number | null | undefined): string {
  if (ns == null || !Number.isFinite(ns)) return "-";
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`;
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(0)}µs`;
  return `${Math.round(ns)}ns`;
}

export function fmtDuration(s: number | null | undefined): string {
  if (s == null) return "";
  if (s >= 86400) return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
  if (s >= 3600) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  if (s >= 60) return `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`;
  return `${s}s`;
}
