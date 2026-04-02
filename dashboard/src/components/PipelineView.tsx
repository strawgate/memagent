import { useState, useRef } from "preact/hooks";
import { fmt, fmtBytes } from "../lib/format";
import type { PipelineData, ComponentData } from "../types";
import { RateTracker } from "../lib/rates";

interface Props {
  pipeline: PipelineData;
}

const Arrow = () => (
  <div class="pipe-conn">
    <svg viewBox="0 0 28 14">
      <line x1="0" y1="7" x2="20" y2="7" />
      <polygon points="20,3 28,7 20,11" />
    </svg>
  </div>
);

export function PipelineView({ pipeline: p }: Props) {
  const [sel, setSel] = useState<string | null>(null);
  const ratesRef = useRef(new RateTracker());

  const toggle = (id: string) => setSel(sel === id ? null : id);

  const compRate = (comp: ComponentData, field: "lines_total" | "bytes_total") => {
    const v = ratesRef.current.rate(`${comp.name}_${field}`, comp[field]);
    return v != null ? fmt(v) + "/s" : "-";
  };

  return (
    <div class="section">
      <div class="heading">Pipeline: {p.name}</div>
      <div class="pipe-flow">
        {p.inputs.map((inp, i) => (
          <>
            {i > 0 && <Arrow />}
            <div
              class={`pn inp ${sel === `i${i}` ? "selected" : ""}`}
              onClick={() => toggle(`i${i}`)}
            >
              <div class="pn-type">Input</div>
              <div class="pn-name">{inp.name}</div>
              <div class="pn-row"><span>lines</span><b>{fmt(inp.lines_total)}</b></div>
              <div class="pn-row"><span>bytes</span><b>{fmtBytes(inp.bytes_total)}</b></div>
              <div class="pn-row"><span>rate</span><b>{compRate(inp, "lines_total")}</b></div>
            </div>
          </>
        ))}
        <Arrow />
        <div
          class={`pn xfm ${sel === "t" ? "selected" : ""}`}
          onClick={() => toggle("t")}
        >
          <div class="pn-type">Transform</div>
          <div class="pn-name">SQL</div>
          <div class="pn-row"><span>in</span><b>{fmt(p.transform.lines_in)}</b></div>
          <div class="pn-row"><span>out</span><b>{fmt(p.transform.lines_out)}</b></div>
          <div class="pn-row">
            <span>drop</span>
            <b style={p.transform.filter_drop_rate > 0.05 ? "color:var(--warn)" : ""}>
              {(p.transform.filter_drop_rate * 100).toFixed(1)}%
            </b>
          </div>
        </div>
        <Arrow />
        {p.outputs.map((out, i) => (
          <>
            {i > 0 && <Arrow />}
            <div
              class={`pn out ${sel === `o${i}` ? "selected" : ""}`}
              onClick={() => toggle(`o${i}`)}
            >
              <div class="pn-type">Output</div>
              <div class="pn-name">{out.name}</div>
              <div class="pn-row"><span>lines</span><b>{fmt(out.lines_total)}</b></div>
              <div class="pn-row"><span>bytes</span><b>{fmtBytes(out.bytes_total)}</b></div>
              <div class="pn-row">
                <span>errors</span>
                <b style={out.errors > 0 ? "color:var(--err)" : ""}>{out.errors}</b>
              </div>
            </div>
          </>
        ))}
      </div>

      {/* Inspector panel */}
      {sel === "t" && (
        <div class="inspector">
          <div class="insp-header">
            <span class="insp-title">Transform &mdash; SQL</span>
            <button class="insp-close" onClick={() => setSel(null)}>&times; close</button>
          </div>
          <div class="sql-box">{p.transform.sql || "SELECT * FROM logs"}</div>
          <div class="insp-grid">
            <div class="insp-kv"><div class="ik-l">Lines In</div><div class="ik-v">{fmt(p.transform.lines_in)}</div></div>
            <div class="insp-kv"><div class="ik-l">Lines Out</div><div class="ik-v">{fmt(p.transform.lines_out)}</div></div>
            <div class="insp-kv"><div class="ik-l">Drop Rate</div><div class="ik-v">{(p.transform.filter_drop_rate * 100).toFixed(1)}%</div></div>
            {p.stage_seconds?.scan != null && <div class="insp-kv"><div class="ik-l">Scan Time</div><div class="ik-v">{p.stage_seconds.scan.toFixed(3)}s</div></div>}
            {p.stage_seconds?.transform != null && <div class="insp-kv"><div class="ik-l">Transform Time</div><div class="ik-v">{p.stage_seconds.transform.toFixed(3)}s</div></div>}
          </div>
        </div>
      )}

      {sel?.startsWith("i") && (() => {
        const idx = parseInt(sel.slice(1));
        const inp = p.inputs[idx];
        if (!inp) return null;
        return (
          <div class="inspector">
            <div class="insp-header">
              <span class="insp-title">Input &mdash; {inp.name}</span>
              <button class="insp-close" onClick={() => setSel(null)}>&times; close</button>
            </div>
            <div class="insp-grid">
              <div class="insp-kv"><div class="ik-l">Name</div><div class="ik-v">{inp.name}</div></div>
              <div class="insp-kv"><div class="ik-l">Type</div><div class="ik-v">{inp.type}</div></div>
              <div class="insp-kv"><div class="ik-l">Lines</div><div class="ik-v">{fmt(inp.lines_total)}</div></div>
              <div class="insp-kv"><div class="ik-l">Bytes</div><div class="ik-v">{fmtBytes(inp.bytes_total)}</div></div>
              <div class="insp-kv"><div class="ik-l">Errors</div><div class="ik-v">{inp.errors}</div></div>
            </div>
          </div>
        );
      })()}

      {sel?.startsWith("o") && (() => {
        const idx = parseInt(sel.slice(1));
        const out = p.outputs[idx];
        if (!out) return null;
        return (
          <div class="inspector">
            <div class="insp-header">
              <span class="insp-title">Output &mdash; {out.name}</span>
              <button class="insp-close" onClick={() => setSel(null)}>&times; close</button>
            </div>
            <div class="insp-grid">
              <div class="insp-kv"><div class="ik-l">Name</div><div class="ik-v">{out.name}</div></div>
              <div class="insp-kv"><div class="ik-l">Type</div><div class="ik-v">{out.type}</div></div>
              <div class="insp-kv"><div class="ik-l">Lines</div><div class="ik-v">{fmt(out.lines_total)}</div></div>
              <div class="insp-kv"><div class="ik-l">Bytes</div><div class="ik-v">{fmtBytes(out.bytes_total)}</div></div>
              <div class="insp-kv"><div class="ik-l">Errors</div><div class="ik-v" style={out.errors > 0 ? "color:var(--err)" : ""}>{out.errors}</div></div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
