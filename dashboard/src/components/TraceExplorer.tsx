import { useState, useEffect, useRef } from "preact/hooks";
import { api } from "../api";
import type { TraceRecord } from "../types";

const COLORS = {
  scan:      "#3b82f6", // blue
  transform: "#8b5cf6", // purple
  output:    "#10b981", // green
  gap:       "#374151", // dark gray (unaccounted time)
};

function fmt_ns(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`;
  if (ns >= 1_000)     return `${(ns / 1_000).toFixed(0)}µs`;
  return `${ns}ns`;
}

function fmt_rows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(0)}K`;
  return String(n);
}

export function TraceExplorer() {
  const [traces, setTraces] = useState<TraceRecord[]>([]);
  const [open, setOpen] = useState(true);
  const [selected, setSelected] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const poll = async () => {
      const data = await api.traces();
      if (data) setTraces(data.traces);
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => clearInterval(id);
  }, [open]);

  // Max duration for proportional bar sizing.
  const maxNs = traces.reduce((m, t) => Math.max(m, t.total_ns), 1);

  const icon = (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" style="flex-shrink:0">
      <rect x="1" y="3" width="5" height="3" rx="1" fill="currentColor" opacity="0.5"/>
      <rect x="1" y="8" width="9" height="3" rx="1" fill="currentColor" opacity="0.5"/>
      <rect x="7" y="3" width="8" height="3" rx="1" fill="currentColor" opacity="0.8"/>
      <rect x="11" y="8" width="4" height="3" rx="1" fill="currentColor" opacity="0.8"/>
    </svg>
  );

  return (
    <div class="trace-box">
      <div class="trace-header">
        <div class="trace-header-left">
          {icon}
          <span>Batch Traces</span>
          {open && traces.length > 0 && (
            <span class="trace-count">{traces.length} recent</span>
          )}
        </div>
        <button
          class="log-close"
          onClick={() => setOpen(!open)}
          aria-label={open ? "Collapse trace explorer" : "Expand trace explorer"}
        >
          {open ? "−" : "+"}
        </button>
      </div>

      {open && (
        <div class="trace-body">
          {traces.length === 0 ? (
            <div class="log-empty">Waiting for batch spans…</div>
          ) : (
            <>
              <div class="trace-legend">
                {(["scan", "transform", "output"] as const).map(k => (
                  <span key={k} class="trace-legend-item">
                    <span class="trace-swatch" style={`background:${COLORS[k]}`} />
                    {k}
                  </span>
                ))}
              </div>
              <div ref={containerRef} class="trace-list">
                {traces.map((t) => {
                  const pct = (ns: number) => `${Math.max(0.5, (ns / maxNs) * 100).toFixed(2)}%`;
                  const accounted = t.scan_ns + t.transform_ns + t.output_ns;
                  const gap_ns = Math.max(0, t.total_ns - accounted);
                  const isSelected = selected === t.trace_id;

                  return (
                    <div
                      key={t.trace_id}
                      class={`trace-row${isSelected ? " trace-row-selected" : ""}`}
                      onClick={() => setSelected(isSelected ? null : t.trace_id)}
                    >
                      <div class="trace-meta">
                        <span class="trace-pipeline">{t.pipeline}</span>
                        <span class="trace-duration">{fmt_ns(t.total_ns)}</span>
                        <span class="trace-rows">
                          {fmt_rows(t.input_rows)}→{fmt_rows(t.output_rows)}
                        </span>
                        {t.errors > 0 && <span class="trace-error-badge">err</span>}
                      </div>

                      <div class="trace-bar-track">
                        {t.scan_ns > 0 && (
                          <div
                            class="trace-segment"
                            style={`width:${pct(t.scan_ns)};background:${COLORS.scan}`}
                            title={`scan: ${fmt_ns(t.scan_ns)}`}
                          />
                        )}
                        {t.transform_ns > 0 && (
                          <div
                            class="trace-segment"
                            style={`width:${pct(t.transform_ns)};background:${COLORS.transform}`}
                            title={`transform: ${fmt_ns(t.transform_ns)}`}
                          />
                        )}
                        {t.output_ns > 0 && (
                          <div
                            class="trace-segment"
                            style={`width:${pct(t.output_ns)};background:${COLORS.output}`}
                            title={`output: ${fmt_ns(t.output_ns)}`}
                          />
                        )}
                        {gap_ns > 0 && (
                          <div
                            class="trace-segment"
                            style={`width:${pct(gap_ns)};background:${COLORS.gap}`}
                            title={`other: ${fmt_ns(gap_ns)}`}
                          />
                        )}
                      </div>

                      {isSelected && (
                        <div class="trace-detail">
                          <span>scan <b>{fmt_ns(t.scan_ns)}</b></span>
                          <span>transform <b>{fmt_ns(t.transform_ns)}</b></span>
                          <span>output <b>{fmt_ns(t.output_ns)}</b></span>
                          <span>total <b>{fmt_ns(t.total_ns)}</b></span>
                          <span>in <b>{t.input_rows.toLocaleString()}</b> rows</span>
                          <span>out <b>{t.output_rows.toLocaleString()}</b> rows</span>
                          {t.input_rows > 0 && (
                            <span>filter ratio <b>{((1 - t.output_rows / t.input_rows) * 100).toFixed(1)}%</b> dropped</span>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
