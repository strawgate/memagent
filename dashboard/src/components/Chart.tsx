import { useEffect, useRef } from "preact/hooks";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { MetricSeries } from "../app";
import { CHART_AXIS, CHART_GRID } from "../lib/theme";

export const CHART_CONSTANTS = {
  CHART_HEIGHT: 130,
  CHART_PADDING: [4, 4, 6, 0] as const,
  CHART_FONT: '9px "SF Mono", "Cascadia Code", monospace',
  MAX_WINDOW_SEC: 300,
  MIN_WINDOW_SEC: 10,
  BUCKET_MS: 5000,
} as const;

/** Build uPlot options for the given series, container width, and bucketed points. */
export function buildChartOpts(
  series: MetricSeries,
  width: number,
  points: ReturnType<MetricSeries["ring"]["bucket"]>,
): uPlot.Options {
  const { CHART_HEIGHT, CHART_PADDING, CHART_FONT, MAX_WINDOW_SEC, MIN_WINDOW_SEC } =
    CHART_CONSTANTS;
  const now = Date.now() / 1000;
  const age = now - points[0].t / 1000;
  const win = Math.max(MIN_WINDOW_SEC, Math.min(age, MAX_WINDOW_SEC));
  return {
    width,
    height: CHART_HEIGHT,
    padding: [...CHART_PADDING],
    cursor: {
      show: true,
      x: true,
      y: false,
      points: { show: true, size: 6, fill: series.color },
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
          ticks.map((v) => (series.fmtAxis ? series.fmtAxis(v) : String(Math.round(v)))),
      },
    ],
    scales: {
      x: { min: now - win, max: now },
      y: {
        range: (_u, dataMin, dataMax) => {
          const [yMin, yMax] = series.yRange ?? [0, 100];
          return [Math.min(yMin, dataMin ?? 0), Math.max(yMax, (dataMax ?? 0) * 1.1)];
        },
      },
    },
    series: [
      {},
      {
        stroke: series.color,
        fill: `${series.color}33`,
        width: 2,
        points: { show: false },
        paths: uPlot.paths.spline?.(),
      },
      {
        stroke: `${series.color}66`,
        width: 1.5,
        dash: [5, 4],
        points: { show: false },
      },
    ],
  };
}

interface Props {
  series: MetricSeries;
}

export function Chart({ series }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);
  const roRef = useRef<ResizeObserver | null>(null);
  const rafRef = useRef<number>(0);

  // Single RAF loop: creates the plot when data first arrives, updates it every frame,
  // and destroys it if data dries up. Using RAF for creation avoids the stale-closure
  // problem where the init useEffect only fires once (on mount, when the ring is still
  // empty) because `series` is a mutable object whose reference never changes.
  useEffect(() => {
    const el = containerRef.current;
    const { CHART_HEIGHT, MIN_WINDOW_SEC, MAX_WINDOW_SEC, BUCKET_MS } = CHART_CONSTANTS;

    const tick = () => {
      const pts = series.ring.bucket(BUCKET_MS);

      if (pts.length < 2) {
        if (plotRef.current) {
          plotRef.current.destroy();
          plotRef.current = null;
          roRef.current?.disconnect();
          roRef.current = null;
        }
      } else {
        if (!plotRef.current && el?.offsetWidth) {
          const times = pts.map((p) => p.t / 1000);
          const vals = pts.map((p) => p.v);
          const lastVal = vals[vals.length - 1];
          const initYe = vals.map((_, i) => (i === vals.length - 1 ? lastVal : null));
          const plot = new uPlot(buildChartOpts(series, el.offsetWidth, pts), [times, vals, initYe], el);
          plotRef.current = plot;

          const ro = new ResizeObserver(() => {
            if (el.offsetWidth > 0) plot.setSize({ width: el.offsetWidth, height: CHART_HEIGHT });
          });
          ro.observe(el);
          roRef.current = ro;
        }

        if (plotRef.current) {
          const now2 = Date.now() / 1000;
          const age = now2 - pts[0].t / 1000;
          const win2 = Math.max(MIN_WINDOW_SEC, Math.min(age, MAX_WINDOW_SEC));
          const n = pts.length;
          const lastVal = pts[n - 1].v;

          const xs: number[] = new Array(n + 1);
          const ys: (number | null)[] = new Array(n + 1);
          const ye: (number | null)[] = new Array(n + 1);
          for (let i = 0; i < n; i++) {
            xs[i] = pts[i].t / 1000;
            ys[i] = pts[i].v;
            ye[i] = i === n - 1 ? lastVal : null;
          }
          xs[n] = now2;
          ys[n] = null;
          ye[n] = lastVal;

          plotRef.current.batch(() => {
            plotRef.current!.setData([xs, ys, ye]);
            plotRef.current!.setScale("x", { min: now2 - win2, max: now2 });
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
  }, [series]);

  const hasData = series.ring.bucket(CHART_CONSTANTS.BUCKET_MS).length >= 2;

  return (
    <div ref={containerRef} class="chart-container">
      {!hasData && <div class="chart-placeholder">waiting for data…</div>}
    </div>
  );
}
