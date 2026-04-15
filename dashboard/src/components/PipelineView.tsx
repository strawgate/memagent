import type { TelemetryStore } from "@otlpkit/views";
import { Fragment } from "preact";
import { useRef, useState } from "preact/hooks";
import { fmt, fmtBytes } from "../lib/format";
import { RateTracker } from "../lib/rates";
import type { ComponentData, PipelineData, TraceRecord } from "../types";
import { BottleneckBadge } from "./BottleneckBadge";
import { Sparkline } from "./Sparkline";
import { TraceExplorer } from "./TraceExplorer";

interface Props {
  pipeline: PipelineData;
  traces: TraceRecord[];
  store: TelemetryStore;
  tick: number;
  /** Whether this pipeline section starts expanded. */
  defaultExpanded: boolean;
  /** Total number of pipelines. */
  pipelineCount: number;
}

const Arrow = () => (
  <div class="pipe-conn">
    <svg viewBox="0 0 28 14" aria-hidden="true">
      <line x1="0" y1="7" x2="20" y2="7" />
      <polygon points="20,3 28,7 20,11" />
    </svg>
  </div>
);

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

function isGenericName(name: string): boolean {
  return /^(input|output)_\d+$/.test(name);
}

/** Health dot: green/yellow/red based on error presence. */
function HealthDot({ errors, rate }: { errors: number; rate?: number | null }) {
  const color = errors > 0 ? "var(--err)" : rate != null && rate === 0 ? "var(--t4)" : "var(--ok)";
  return (
    <span
      class="health-dot"
      style={{ backgroundColor: color }}
      title={errors > 0 ? `${errors} errors` : "healthy"}
    />
  );
}

/** Extract sparkline data for a metric, optionally filtered by pipeline. */
function selectSparkline(store: TelemetryStore, metricName: string, pipeline?: string): number[] {
  const frame = store.selectTimeSeries({
    metricName,
    intervalMs: 2000,
    reduce: "last",
    ...(pipeline ? { splitBy: "pipeline" } : {}),
  });
  if (!pipeline) return frame.series[0]?.points.map((p) => p.value) ?? [];
  // Only return the matching series — don't fall back to a different pipeline's data.
  const series = frame.series.find((s) => s.key === pipeline);
  return series?.points.map((p) => p.value) ?? [];
}

/** Build a friendly "Input → Output" summary for the collapsed header. */
function pipelineSummary(p: PipelineData): string {
  const inputName =
    p.inputs.length === 1
      ? isGenericName(p.inputs[0].name)
        ? typeLabel(p.inputs[0].type)
        : p.inputs[0].name
      : `${p.inputs.length} inputs`;
  const outputName =
    p.outputs.length === 1
      ? isGenericName(p.outputs[0].name)
        ? typeLabel(p.outputs[0].type)
        : p.outputs[0].name
      : `${p.outputs.length} outputs`;
  return `${inputName} → ${outputName}`;
}

export function PipelineView({ pipeline: p, traces, store, tick: _tick, defaultExpanded }: Props) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const [sel, setSel] = useState<string | null>(null);
  const ratesRef = useRef(new RateTracker());

  const toggle = (id: string) => setSel(sel === id ? null : id);

  const compRate = (comp: ComponentData, field: "lines_total" | "bytes_total") => {
    const v = ratesRef.current.rate(`${comp.name}_${field}`, comp[field]);
    return v != null ? `${fmt(v)}/s` : "-";
  };

  // Compute current EPS (events/lines per second) across all inputs.
  const inputEps = p.inputs.reduce((sum, inp) => {
    const v = ratesRef.current.rate(`${inp.name}_lines_total`, inp.lines_total);
    return sum + (v ?? 0);
  }, 0);

  // Sparkline data for this pipeline.
  const inputRateSpark = selectSparkline(store, "logfwd.input_lines_per_sec", p.name);
  const outputRateSpark = selectSparkline(store, "logfwd.output_lines_per_sec", p.name);
  const stallsSpark = selectSparkline(store, "logfwd.backpressure_stalls_per_sec", p.name);

  // Stage breakdown from status data.
  const stages = p.stage_seconds;
  const totalStageTime = stages ? stages.scan + stages.transform + stages.output : 0;
  const stageBar =
    stages && totalStageTime > 0
      ? {
          scanPct: (stages.scan / totalStageTime) * 100,
          xfmPct: (stages.transform / totalStageTime) * 100,
          outPct: (stages.output / totalStageTime) * 100,
        }
      : null;

  const totalErrors = p.outputs.reduce((s, o) => s + o.errors, 0);
  const latencyMs = p.batches?.batch_latency_avg_ns ? p.batches.batch_latency_avg_ns / 1e6 : null;

  return (
    <div class={`pipeline-card ${expanded ? "open" : ""}`}>
      {/* ── Collapsible header ── */}
      <button
        type="button"
        class="pipeline-header"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        <span class="collapsible-arrow">{expanded ? "▾" : "▸"}</span>
        <span class="pipeline-name">{p.name}</span>
        {!expanded && (
          <span class="pipeline-summary">
            <span class="pipeline-flow-label">{pipelineSummary(p)}</span>
            <span class="pipeline-eps">{fmt(inputEps)} EPS</span>
          </span>
        )}
        {p.bottleneck && p.bottleneck.stage !== "none" && <BottleneckBadge b={p.bottleneck} />}
        {stageBar && (
          <span class="pipeline-stage-pcts">
            <span class="stage-pct scan">Scan {stageBar.scanPct.toFixed(0)}%</span>
            <span class="stage-pct xfm">Transform {stageBar.xfmPct.toFixed(0)}%</span>
            <span class="stage-pct out">Output {stageBar.outPct.toFixed(0)}%</span>
          </span>
        )}
      </button>

      {expanded && (
        <>
          {/* ── Flow diagram with inline sparklines ── */}
          <div class="pipe-flow">
            {p.inputs.map((inp, i) => (
              <Fragment key={inp.name}>
                {i > 0 && <Arrow />}
                <button
                  type="button"
                  class={`pn inp ${sel === `i${i}` ? "selected" : ""}`}
                  onClick={() => toggle(`i${i}`)}
                >
                  <span class="pn-top">
                    <HealthDot errors={inp.errors + (inp.parse_errors ?? 0)} />
                    <span class="pn-type">{typeLabel(inp.type)}</span>
                  </span>
                  {!isGenericName(inp.name) && <span class="pn-name">{inp.name}</span>}
                  <span class="pn-row">
                    <span>rate</span>
                    <b>{compRate(inp, "lines_total")}</b>
                    <Sparkline
                      values={inputRateSpark}
                      width={40}
                      height={12}
                      color="var(--ok)"
                      style="area"
                    />
                  </span>
                  {i === 0 && (() => {
                    const sr = ratesRef.current.rate(
                      `${p.name}_stalls`,
                      p.backpressure_stalls ?? 0
                    );
                    return sr != null && sr > 0 ? (
                      <span class="pn-row">
                        <span>stalls</span>
                        <b class="text-warn">{sr.toFixed(1)}/s</b>
                        <Sparkline
                          values={stallsSpark}
                          width={40}
                          height={12}
                          color="var(--warn)"
                          style="area"
                        />
                      </span>
                    ) : null;
                  })()}
                </button>
              </Fragment>
            ))}
            <Arrow />
            <button
              type="button"
              class={`pn xfm ${sel === "t" ? "selected" : ""}`}
              onClick={() => toggle("t")}
            >
              <span class="pn-top">
                <HealthDot errors={0} rate={p.transform.lines_in > 0 ? 1 : null} />
                <span class="pn-type">SQL</span>
              </span>
              <span class="pn-row">
                <span>pass</span>
                <b>
                  {p.transform.lines_in > 0
                    ? `${((1 - p.transform.filter_drop_rate) * 100).toFixed(1)}%`
                    : "-"}
                </b>
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
              <Fragment key={out.name}>
                {i > 0 && <Arrow />}
                <button
                  type="button"
                  class={`pn out ${sel === `o${i}` ? "selected" : ""}`}
                  onClick={() => toggle(`o${i}`)}
                >
                  <span class="pn-top">
                    <HealthDot errors={out.errors} />
                    <span class="pn-type">{typeLabel(out.type)}</span>
                  </span>
                  {!isGenericName(out.name) && <span class="pn-name">{out.name}</span>}
                  <span class="pn-row">
                    <span>rate</span>
                    <b>{compRate(out, "lines_total")}</b>
                    <Sparkline
                      values={outputRateSpark}
                      width={40}
                      height={12}
                      color="var(--purple)"
                      style="area"
                    />
                  </span>
                </button>
              </Fragment>
            ))}
          </div>

          {/* ── Compact stats strip ── */}
          <div class="pipe-stats">
            {latencyMs != null && (
              <span class="pipe-stat">
                <span class="pipe-stat-label">Latency</span>
                <b>{latencyMs.toFixed(1)}ms</b>
              </span>
            )}
            {p.batches?.inflight != null && (
              <span class="pipe-stat">
                <span class="pipe-stat-label">Inflight</span>
                <b>{p.batches.inflight}</b>
              </span>
            )}
            {totalErrors > 0 && (
              <span class="pipe-stat">
                <span class="pipe-stat-label">Errors</span>
                <b class="text-err">{totalErrors}</b>
              </span>
            )}
          </div>

          {/* ── Inspector panels (click a node) ── */}
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
                {totalStageTime > 0 && stages?.transform != null && (
                  <div class="insp-kv">
                    <div class="ik-l">Time Share</div>
                    <div class="ik-v">
                      {((stages.transform / totalStageTime) * 100).toFixed(1)}%
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {sel?.startsWith("i") &&
            (() => {
              const idx = Number.parseInt(sel.slice(1), 10);
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
                    {inp.parse_errors != null && inp.parse_errors > 0 && (
                      <div class="insp-kv">
                        <div class="ik-l">Parse Errors</div>
                        <div class="ik-v text-warn">{inp.parse_errors}</div>
                      </div>
                    )}
                  </div>
                </div>
              );
            })()}

          {sel?.startsWith("o") &&
            (() => {
              const idx = Number.parseInt(sel.slice(1), 10);
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
                  </div>
                </div>
              );
            })()}

          {/* ── Batch monitor — directly visible, no nested collapse ── */}
          {traces.length > 0 && (
            <div class="pipe-traces">
              <div class="pipe-traces-header">
                <span>Batch Monitor</span>
                <span class="trace-count">{traces.length} recent</span>
              </div>
              <TraceExplorer traces={traces} />
            </div>
          )}
        </>
      )}
    </div>
  );
}
