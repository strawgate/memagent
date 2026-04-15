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
  /** When set, `selectTimeSeries` splits by this attribute (e.g. "pipeline"). */
  readonly splitBy?: string;
}

/** Palette for multi-pipeline series. Cycles through these colors. */
const SERIES_PALETTE = [
  "#3b82f6",
  "#22c55e",
  "#f59e0b",
  "#ef4444",
  "#a78bfa",
  "#ec4899",
  "#14b8a6",
  "#f97316",
];

/** Build uPlot options for a chart. `seriesCount` is the number of data series (pipelines). */
function buildOpts(
  cfg: ChartConfig,
  width: number,
  now: number,
  age: number,
  seriesCount: number
): uPlot.Options {
  const { CHART_HEIGHT, CHART_PADDING, CHART_FONT, MAX_WINDOW_SEC, MIN_WINDOW_SEC } =
    CHART_CONSTANTS;
  const win = Math.max(MIN_WINDOW_SEC, Math.min(age, MAX_WINDOW_SEC));

  // Build series definitions: for each data series, add a solid line + a dashed projection.
  const series: uPlot.Series[] = [{}]; // index 0 = x-axis
  for (let i = 0; i < seriesCount; i++) {
    const color = seriesCount === 1 ? cfg.color : SERIES_PALETTE[i % SERIES_PALETTE.length];
    series.push({
      stroke: color,
      fill: seriesCount === 1 ? `${color}33` : undefined,
      width: 2,
      points: { show: false },
      paths: uPlot.paths.spline?.(),
    });
    series.push({
      stroke: `${color}66`,
      width: 1.5,
      dash: [5, 4],
      points: { show: false },
    });
  }

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
    series,
  };
}

interface Props {
  frame: TimeSeriesFrame;
  config: ChartConfig;
}

/**
 * Build aligned uPlot data from a multi-series TimeSeriesFrame.
 * Returns [xs, ys1, ye1, ys2, ye2, ...] where each series pair is (values, projection).
 */
function buildAlignedData(
  frame: TimeSeriesFrame,
  now: number
): { data: uPlot.AlignedData; seriesCount: number; age: number } {
  const allSeries = frame.series;
  const seriesCount = allSeries.length;
  if (seriesCount === 0) return { data: [[]], seriesCount: 0, age: 0 };

  // Collect all unique timestamps across all series.
  const timeSet = new Set<number>();
  for (const s of allSeries) {
    for (const p of s.points) {
      if (p.timeMs != null) timeSet.add(p.timeMs);
    }
  }
  const times = Array.from(timeSet).sort((a, b) => a - b);
  const n = times.length;
  if (n === 0) return { data: [[]], seriesCount, age: 0 };

  const age = now - times[0] / 1000;

  // Build lookup per series: timeMs → value.
  const xs = new Array<number>(n + 1);
  for (let i = 0; i < n; i++) xs[i] = times[i] / 1000;
  xs[n] = now;

  const data: (number | null)[][] = [xs];
  for (const s of allSeries) {
    const lookup = new Map<number, number>();
    for (const p of s.points) {
      if (p.timeMs != null) lookup.set(p.timeMs, p.value);
    }
    const lastVal = s.points.length > 0 ? s.points[s.points.length - 1].value : null;

    // Find the last timestamp index where this series has actual data.
    let lastIdx = -1;
    for (let i = n - 1; i >= 0; i--) {
      if (lookup.has(times[i])) {
        lastIdx = i;
        break;
      }
    }

    const ys = new Array<number | null>(n + 1);
    const ye = new Array<number | null>(n + 1);
    for (let i = 0; i < n; i++) {
      ys[i] = lookup.get(times[i]) ?? null;
      // Anchor projection at this series' own last data point.
      ye[i] = i === lastIdx ? lastVal : null;
    }
    ys[n] = null;
    ye[n] = lastVal;

    data.push(ys, ye);
  }

  return { data: data as uPlot.AlignedData, seriesCount, age };
}

export function Chart({ frame, config }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);
  const roRef = useRef<ResizeObserver | null>(null);
  const rafRef = useRef<number>(0);
  const seriesCountRef = useRef(0);
  // Store frame in a ref so the RAF loop reads latest data without
  // tearing down the effect (and uPlot instance) on every update.
  const frameRef = useRef(frame);
  frameRef.current = frame;

  // Cache buildAlignedData result — only rebuild when frame identity changes.
  const cachedAlignedRef = useRef<{
    frame: TimeSeriesFrame;
    result: ReturnType<typeof buildAlignedData>;
  } | null>(null);

  // RAF loop: creates the plot when data arrives, updates each frame.
  useEffect(() => {
    const el = containerRef.current;
    const { CHART_HEIGHT, MIN_WINDOW_SEC, MAX_WINDOW_SEC } = CHART_CONSTANTS;

    const tick = () => {
      const now = Date.now() / 1000;
      const currentFrame = frameRef.current;

      // Reuse cached aligned data if the frame hasn't changed.
      let aligned: ReturnType<typeof buildAlignedData>;
      if (cachedAlignedRef.current && cachedAlignedRef.current.frame === currentFrame) {
        aligned = cachedAlignedRef.current.result;
        // Update only the synthetic trailing point (x-scale "now" marker).
        const xs = aligned.data[0] as number[];
        if (xs.length > 0) xs[xs.length - 1] = now;
      } else {
        aligned = buildAlignedData(currentFrame, now);
        cachedAlignedRef.current = { frame: currentFrame, result: aligned };
      }
      const { data, seriesCount } = aligned;
      // Recompute age from live `now` so the x-axis window stays current
      // even when the cached aligned data is reused across ticks.
      const xs = data[0] as number[];
      const age = xs.length > 0 ? now - xs[0] : 0;
      const totalPoints = xs.length;

      if (totalPoints < 2) {
        if (plotRef.current) {
          plotRef.current.destroy();
          plotRef.current = null;
          seriesCountRef.current = 0;
          roRef.current?.disconnect();
          roRef.current = null;
        }
      } else {
        // Rebuild uPlot if series count changed (pipelines added/removed).
        if (plotRef.current && seriesCount !== seriesCountRef.current) {
          plotRef.current.destroy();
          plotRef.current = null;
          roRef.current?.disconnect();
          roRef.current = null;
        }

        if (!plotRef.current && el?.offsetWidth) {
          const plot = new uPlot(
            buildOpts(config, el.offsetWidth, now, age, seriesCount),
            data,
            el
          );
          plotRef.current = plot;
          seriesCountRef.current = seriesCount;

          const ro = new ResizeObserver(() => {
            if (el.offsetWidth > 0) plot.setSize({ width: el.offsetWidth, height: CHART_HEIGHT });
          });
          ro.observe(el);
          roRef.current = ro;
        }

        if (plotRef.current) {
          const win = Math.max(MIN_WINDOW_SEC, Math.min(age, MAX_WINDOW_SEC));
          plotRef.current.batch(() => {
            plotRef.current?.setData(data);
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
      seriesCountRef.current = 0;
      roRef.current?.disconnect();
      roRef.current = null;
    };
  }, [config]);

  // Count unique timestamps — aligns with buildAlignedData's data[0].length check.
  const timeSet = new Set<number>();
  for (const s of frame.series) {
    for (const p of s.points) {
      if (p.timeMs != null) timeSet.add(p.timeMs);
    }
  }

  return (
    <div ref={containerRef} class="chart-container">
      {timeSet.size < 2 && <div class="chart-placeholder">waiting for data…</div>}
    </div>
  );
}
