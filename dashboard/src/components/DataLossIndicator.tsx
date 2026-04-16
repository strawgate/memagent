import { useState } from "preact/hooks";
import { fmtCompact } from "../lib/format";
import type { PipelineData } from "../types";

interface Props {
  pipelines: PipelineData[];
}

interface PipelineLoss {
  name: string;
  linesIn: number;
  linesOut: number;
  dropped: number;
  errors: number;
  dropRate: number;
}

/**
 * Prominent data loss indicator — the first thing operators look at.
 *
 * Shows a single percentage with color coding:
 * - Green: 0% loss
 * - Yellow: > 0% but < 1%
 * - Red: >= 1% loss
 *
 * Click to expand per-pipeline breakdown.
 */
export function DataLossIndicator({ pipelines }: Props) {
  const [expanded, setExpanded] = useState(false);

  // Aggregate across all pipelines.
  let totalIn = 0;
  let totalOut = 0;
  let totalErrors = 0;
  const perPipeline: PipelineLoss[] = [];

  for (const p of pipelines) {
    const linesIn = p.transform.lines_in ?? sumLines(p.inputs);
    // Use transform.lines_out as canonical count — summing outputs double-counts in fan-out.
    const linesOut = p.transform.lines_out ?? sumLines(p.outputs);
    const errors = sumErrors(p.outputs);
    const dropped = Math.max(0, linesIn - linesOut);

    totalIn += linesIn;
    totalOut += linesOut;
    totalErrors += errors;

    perPipeline.push({
      name: p.name,
      linesIn,
      linesOut,
      dropped,
      errors,
      dropRate: linesIn > 0 ? dropped / linesIn : 0,
    });
  }

  const totalDropped = Math.max(0, totalIn - totalOut);
  const lossRate = totalIn > 0 ? totalDropped / totalIn : 0;
  const lossPct = (lossRate * 100).toFixed(lossRate < 0.001 ? 3 : 1);

  const severity = lossRate >= 0.01 ? "err" : lossRate > 0 ? "warn" : "ok";

  return (
    <div class="data-loss">
      <button
        type="button"
        class={`data-loss-summary data-loss-${severity}`}
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        <span class="data-loss-pct">{lossPct}%</span>
        <span class="data-loss-label">data loss</span>
        <span class="data-loss-flow">
          {fmtCompact(totalIn)} in → {fmtCompact(totalOut)} out
        </span>
        {totalErrors > 0 && <span class="data-loss-errors">{totalErrors} errors</span>}
        <span class="data-loss-expand">{expanded ? "▾" : "▸"}</span>
      </button>

      {expanded && perPipeline.length > 0 && (
        <div class="data-loss-detail">
          <table class="data-loss-table">
            <thead>
              <tr>
                <th>Pipeline</th>
                <th>In</th>
                <th>Out</th>
                <th>Dropped</th>
                <th>Errors</th>
                <th>Loss</th>
              </tr>
            </thead>
            <tbody>
              {perPipeline.map((p) => (
                <tr key={p.name}>
                  <td class="mono">{p.name}</td>
                  <td class="num">{fmtCompact(p.linesIn)}</td>
                  <td class="num">{fmtCompact(p.linesOut)}</td>
                  <td class={`num ${p.dropped > 0 ? "text-warn" : ""}`}>{fmtCompact(p.dropped)}</td>
                  <td class={`num ${p.errors > 0 ? "text-err" : ""}`}>{p.errors}</td>
                  <td
                    class={`num ${p.dropRate >= 0.01 ? "text-err" : p.dropRate > 0 ? "text-warn" : ""}`}
                  >
                    {(p.dropRate * 100).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function sumLines(components: { lines_total: number }[]): number {
  return components.reduce((s, c) => s + c.lines_total, 0);
}

function sumErrors(components: { errors: number }[]): number {
  return components.reduce((s, c) => s + c.errors, 0);
}
