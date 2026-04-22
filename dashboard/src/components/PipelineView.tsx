import { useRef, useState } from "preact/hooks";
import { fmt, fmtBytes, fmtNs } from "../lib/format";
import { RateTracker } from "../lib/rates";
import type { ComponentData, PipelineData, TraceRecord } from "../types";
import { BottleneckBadge } from "./BottleneckBadge";
import { TraceExplorer } from "./TraceExplorer";

const POLL_OPTIONS = [
  { label: "500ms", ms: 500 },
  { label: "1s", ms: 1000 },
  { label: "2s", ms: 2000 },
  { label: "5s", ms: 5000 },
];

interface Props {
  pipeline: PipelineData;
  traces: TraceRecord[];
  pollMs: number;
  setPollMs: (ms: number) => void;
}

const Arrow = () => (
  <div class="pipe-conn">
    <svg viewBox="0 0 28 14" aria-hidden="true">
      <line x1="0" y1="7" x2="20" y2="7" />
      <polygon points="20,3 28,7 20,11" />
    </svg>
  </div>
);

// Human-readable label for component types coming from the server.
function typeLabel(type: string): string {
  const map: Record<string, string> = {
    file: "File",
    generator: "Generator",
    tcp: "TCP",
    udp: "UDP",
    otlp: "OTLP",
    otlpgrpc: "OTLP/gRPC",
    loki: "Loki",
    elasticsearch: "Elasticsearch",
    parquet: "Parquet",
    stdout: "Stdout",
    null: "Null",
  };
  return map[type.toLowerCase()] ?? type;
}

// Names like "input_0", "output_1" are auto-generated and not worth showing.
function isGenericName(name: string): boolean {
  return /^(input|output)_\d+$/.test(name);
}

export function PipelineView({ pipeline: p, traces, pollMs, setPollMs }: Props) {
  const [sel, setSel] = useState<string | null>(null);
  const ratesRef = useRef(new RateTracker());

  const toggle = (id: string) => setSel(sel === id ? null : id);

  const compRate = (comp: ComponentData, field: "lines_total" | "bytes_total") => {
    const v = ratesRef.current.rate(`${comp.name}_${field}`, comp[field]);
    return v != null ? `${fmt(v)}/s` : "-";
  };

  return (
    <div class="section">
      <div class="heading heading-flex">
        Pipeline: {p.name}
        {p.bottleneck && <BottleneckBadge b={p.bottleneck} />}
      </div>
      <div class="pipe-flow">
        {p.inputs.map((inp, i) => (
          <>
            {i > 0 && <Arrow />}
            <button
              type="button"
              class={`pn inp ${sel === `i${i}` ? "selected" : ""}`}
              onClick={() => toggle(`i${i}`)}
            >
              <span class="pn-type">{typeLabel(inp.type)}</span>
              {!isGenericName(inp.name) && <span class="pn-name">{inp.name}</span>}
              <span class="pn-row">
                <span>lines</span>
                <b>{fmt(inp.lines_total)}</b>
              </span>
              <span class="pn-row">
                <span>bytes</span>
                <b>{fmtBytes(inp.bytes_total)}</b>
              </span>
              <span class="pn-row">
                <span>rate</span>
                <b>{compRate(inp, "lines_total")}</b>
              </span>
            </button>
          </>
        ))}
        <Arrow />
        <button
          type="button"
          class={`pn xfm ${sel === "t" ? "selected" : ""}`}
          onClick={() => toggle("t")}
        >
          <span class="pn-type">SQL</span>
          <span class="pn-row">
            <span>in</span>
            <b>{fmt(p.transform.lines_in)}</b>
          </span>
          <span class="pn-row">
            <span>out</span>
            <b>{fmt(p.transform.lines_out)}</b>
          </span>
          <span class="pn-row">
            <span>drop</span>
            <b style={p.transform.filter_drop_rate > 0.05 ? "color:var(--warn)" : ""}>
              {(p.transform.filter_drop_rate * 100).toFixed(1)}%
            </b>
          </span>
        </button>
        <Arrow />
        {p.outputs.map((out, i) => (
          <>
            {i > 0 && <Arrow />}
            <button
              type="button"
              class={`pn out ${sel === `o${i}` ? "selected" : ""}`}
              onClick={() => toggle(`o${i}`)}
            >
              <span class="pn-type">{typeLabel(out.type)}</span>
              {!isGenericName(out.name) && <span class="pn-name">{out.name}</span>}
              <span class="pn-row">
                <span>lines</span>
                <b>{fmt(out.lines_total)}</b>
              </span>
              <span class="pn-row">
                <span>bytes</span>
                <b>{fmtBytes(out.bytes_total)}</b>
              </span>
              <span class="pn-row">
                <span>errors</span>
                <b style={out.errors > 0 ? "color:var(--err)" : ""}>{out.errors}</b>
              </span>
              {out.send_ns_total != null && out.send_count != null && out.send_count > 0 && (
                <span class="pn-row">
                  <span>latency</span>
                  <b>{fmtNs(out.send_ns_total / Math.max(1, out.send_count))}</b>
                </span>
              )}
            </button>
          </>
        ))}
      </div>

      {/* Inspector panels */}
      {sel === "t" && (
        <div class="inspector">
          <div class="insp-header">
            <span class="insp-title">Transform &mdash; SQL</span>
            <button type="button" class="insp-close" onClick={() => setSel(null)}>
              &times; close
            </button>
          </div>
          <div class="sql-box">{p.transform.sql || "SELECT * FROM logs"}</div>
          <div class="insp-grid">
            <div class="insp-kv">
              <div class="ik-l">Lines In</div>
              <div class="ik-v">{fmt(p.transform.lines_in)}</div>
            </div>
            <div class="insp-kv">
              <div class="ik-l">Lines Out</div>
              <div class="ik-v">{fmt(p.transform.lines_out)}</div>
            </div>
            <div class="insp-kv">
              <div class="ik-l">Drop Rate</div>
              <div class="ik-v">{(p.transform.filter_drop_rate * 100).toFixed(1)}%</div>
            </div>
            {p.stage_seconds?.scan != null && (
              <div class="insp-kv">
                <div class="ik-l">Scan Time</div>
                <div class="ik-v">{p.stage_seconds.scan.toFixed(3)}s</div>
              </div>
            )}
            {p.stage_seconds?.transform != null && (
              <div class="insp-kv">
                <div class="ik-l">Transform Time</div>
                <div class="ik-v">{p.stage_seconds.transform.toFixed(3)}s</div>
              </div>
            )}
          </div>
        </div>
      )}

      {sel?.startsWith("i") &&
        (() => {
          const idx = parseInt(sel.slice(1), 10);
          const inp = p.inputs[idx];
          if (!inp) return null;
          return (
            <div class="inspector">
              <div class="insp-header">
                <span class="insp-title">
                  Input &mdash; {typeLabel(inp.type)}
                  {!isGenericName(inp.name) ? ` · ${inp.name}` : ""}
                </span>
                <button type="button" class="insp-close" onClick={() => setSel(null)}>
                  &times; close
                </button>
              </div>
              <div class="insp-grid">
                <div class="insp-kv">
                  <div class="ik-l">Type</div>
                  <div class="ik-v">{typeLabel(inp.type)}</div>
                </div>
                {!isGenericName(inp.name) && (
                  <div class="insp-kv">
                    <div class="ik-l">Name</div>
                    <div class="ik-v">{inp.name}</div>
                  </div>
                )}
                <div class="insp-kv">
                  <div class="ik-l">Lines</div>
                  <div class="ik-v">{fmt(inp.lines_total)}</div>
                </div>
                <div class="insp-kv">
                  <div class="ik-l">Bytes</div>
                  <div class="ik-v">{fmtBytes(inp.bytes_total)}</div>
                </div>
                <div class="insp-kv">
                  <div class="ik-l">Errors</div>
                  <div class="ik-v">{inp.errors}</div>
                </div>
              </div>
            </div>
          );
        })()}

      {sel?.startsWith("o") &&
        (() => {
          const idx = parseInt(sel.slice(1), 10);
          const out = p.outputs[idx];
          if (!out) return null;
          return (
            <div class="inspector">
              <div class="insp-header">
                <span class="insp-title">
                  Output &mdash; {typeLabel(out.type)}
                  {!isGenericName(out.name) ? ` · ${out.name}` : ""}
                </span>
                <button type="button" class="insp-close" onClick={() => setSel(null)}>
                  &times; close
                </button>
              </div>
              <div class="insp-grid">
                <div class="insp-kv">
                  <div class="ik-l">Type</div>
                  <div class="ik-v">{typeLabel(out.type)}</div>
                </div>
                {!isGenericName(out.name) && (
                  <div class="insp-kv">
                    <div class="ik-l">Name</div>
                    <div class="ik-v">{out.name}</div>
                  </div>
                )}
                <div class="insp-kv">
                  <div class="ik-l">Lines</div>
                  <div class="ik-v">{fmt(out.lines_total)}</div>
                </div>
                <div class="insp-kv">
                  <div class="ik-l">Bytes</div>
                  <div class="ik-v">{fmtBytes(out.bytes_total)}</div>
                </div>
                <div class="insp-kv">
                  <div class="ik-l">Errors</div>
                  <div class="ik-v" style={out.errors > 0 ? "color:var(--err)" : ""}>
                    {out.errors}
                  </div>
                </div>
                {out.send_ns_total != null && out.send_count != null && out.send_count > 0 && (
                  <div class="insp-kv">
                    <div class="ik-l">Latency</div>
                    <div class="ik-v">{fmtNs(out.send_ns_total / Math.max(1, out.send_count))}</div>
                  </div>
                )}
              </div>
            </div>
          );
        })()}

      {/* Batch traces — always visible */}
      {traces.length > 0 && (
        <div class="pipe-traces">
          <div class="pipe-traces-header">
            <span>Batch Traces</span>
            <span class="pipe-traces-count">{traces.length} recent</span>
            <div class="t2-window-picker" style="margin-left:auto">
              {POLL_OPTIONS.map((o) => (
                <button
                  type="button"
                  key={o.label}
                  class={`t2-win-btn${pollMs === o.ms ? " active" : ""}`}
                  onClick={() => setPollMs(o.ms)}
                >
                  {o.label}
                </button>
              ))}
            </div>
          </div>
          <TraceExplorer traces={traces} />
        </div>
      )}
    </div>
  );
}
