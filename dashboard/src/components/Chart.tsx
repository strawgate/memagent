import type { TimeSeriesFrame } from "@otlpkit/views";
import { useEffect, useRef } from "preact/hooks";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { CHART_AXIS, CHART_GRID } from "../lib/theme";

export const CHART_CONSTANTS = {
  CHART_HEIGHT: 130,
  CHART_PADDING: [4, 4, 6, 0] as const,
  CHART_FONT: '9px "SF Mono", "Cascadia Code", monospace',
  MAX_WINDOW_SEC: 300,
  MIN_WINDOW_SEC: 10,
} as const;

/** Per-chart visual configuration (independent of data). */
export interface ChartConfig {
  readonly metricName: string;
  readonly label: string;
  readonly color: string;
  readonly unit: string;
  readonly fmtAxis?: (v: number) => string;
  readonly yRange?: [number, number];
}

/** Build uPlot options for a chart. */
function buildOpts(cfg: ChartConfig, width: number, now: number, age: number): uPlot.Options {
  const { CHART_HEIGHT, CHART_PADDING, CHART_FONT, MAX_WINDOW_SEC, MIN_WINDOW_SEC } =
    CHART_CONSTANTS;
  const win = Math.max(MIN_WINDOW_SEC, Math.min(age, MAX_WINDOW_SEC));
  return {
    width,
    height: CHART_HEIGHT,
    padding: [...CHART_PADDING],
    cursor: {
      show: true,
      x: true,
      y: false,
      points: { show: true, size: 6, fill: cfg.color },
    },
    legend: { show: false },
    axes: [
      {
        stroke: CHART_AXIS,
        grid: { stroke: CHART_GRID, width: 1 },
        ticks: { show: false },
        gap: 4,
        size: 18,
        font: CHART_FONT,
        values: (_, ticks) =>
          ticks.map((t, i, arr) => {
            if (i === arr.length - 1) return "now";
            const ago = Math.round(Date.now() / 1000 - t);
            if (ago < 60) return `${ago}s`;
            return `${Math.floor(ago / 60)}m`;
          }),
      },
      {
        stroke: CHART_AXIS,
        grid: { stroke: CHART_GRID, width: 1 },
        ticks: { show: false },
        gap: 4,
        size: 32,
        font: CHART_FONT,
        values: (_, ticks) =>
          ticks.map((v) => (cfg.fmtAxis ? cfg.fmtAxis(v) : String(Math.round(v)))),
      },
    ],
    scales: {
      x: { min: now - win, max: now },
      y: {
        range: (_u, dataMin, dataMax) => {
          const [yMin, yMax] = cfg.yRange ?? [0, 100];
          return [Math.min(yMin, dataMin ?? 0), Math.max(yMax, (dataMax ?? 0) * 1.1)];
        },
      },
    },
    series: [
      {},
      {
        stroke: cfg.color,
        fill: `${cfg.color}33`,
        width: 2,
        points: { show: false },
        paths: uPlot.paths.spline?.(),
      },
      {
        stroke: `${cfg.color}66`,
        width: 1.5,
        dash: [5, 4],
        points: { show: false },
      },
    ],
  };
}

interface Props {
  frame: TimeSeriesFrame;
  config: ChartConfig;
}

export function Chart({ frame, config }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);
  const roRef = useRef<ResizeObserver | null>(null);
  const rafRef = useRef<number>(0);
  // Store frame in a ref so the RAF loop reads latest data without
  // tearing down the effect (and uPlot instance) on every update.
  const frameRef = useRef(frame);
  frameRef.current = frame;

  // RAF loop: creates the plot when data arrives, updates each frame.
  useEffect(() => {
    const el = containerRef.current;
    const { CHART_HEIGHT, MIN_WINDOW_SEC, MAX_WINDOW_SEC } = CHART_CONSTANTS;

    const tick = () => {
      const pts = frameRef.current.series[0]?.points ?? [];

      if (pts.length < 2) {
        if (plotRef.current) {
          plotRef.current.destroy();
          plotRef.current = null;
          roRef.current?.disconnect();
          roRef.current = null;
        }
      } else {
        const now = Date.now() / 1000;
        const firstTimeMs = pts[0].timeMs ?? Date.now();
        const age = now - firstTimeMs / 1000;

        if (!plotRef.current && el?.offsetWidth) {
          const times = pts.map((p) => (p.timeMs ?? 0) / 1000);
          const vals = pts.map((p) => p.value);
          const lastVal = vals[vals.length - 1];
          const initYe = vals.map((_, i) => (i === vals.length - 1 ? lastVal : null));
          const plot = new uPlot(
            buildOpts(config, el.offsetWidth, now, age),
            [times, vals, initYe],
            el
          );
          plotRef.current = plot;

          const ro = new ResizeObserver(() => {
            if (el.offsetWidth > 0) plot.setSize({ width: el.offsetWidth, height: CHART_HEIGHT });
          });
          ro.observe(el);
          roRef.current = ro;
        }

        if (plotRef.current) {
          const n = pts.length;
          const lastVal = pts[n - 1].value;
          const win = Math.max(MIN_WINDOW_SEC, Math.min(age, MAX_WINDOW_SEC));

          const xs: number[] = new Array(n + 1);
          const ys: (number | null)[] = new Array(n + 1);
          const ye: (number | null)[] = new Array(n + 1);
          for (let i = 0; i < n; i++) {
            xs[i] = (pts[i].timeMs ?? 0) / 1000;
            ys[i] = pts[i].value;
            ye[i] = i === n - 1 ? lastVal : null;
          }
          xs[n] = now;
          ys[n] = null;
          ye[n] = lastVal;

          plotRef.current.batch(() => {
            plotRef.current?.setData([xs, ys, ye]);
            plotRef.current?.setScale("x", { min: now - win, max: now });
          });
        }
      }

      rafRef.current = requestAnimationFrame(tick);
    };

    rafRef.current = requestAnimationFrame(tick);
    return () => {
      cancelAnimationFrame(rafRef.current);
      plotRef.current?.destroy();
      plotRef.current = null;
      roRef.current?.disconnect();
      roRef.current = null;
    };
  }, [config]);

  const hasData = (frame.series[0]?.points.length ?? 0) >= 2;

  return (
    <div ref={containerRef} class="chart-container">
      {!hasData && <div class="chart-placeholder">waiting for data…</div>}
    </div>
  );
}
