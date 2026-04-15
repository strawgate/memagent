import { useEffect, useRef } from "preact/hooks";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { MetricSeries } from "../app";

interface Props {
  series: MetricSeries;
}

export function Chart({ series }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);
  const roRef = useRef<ResizeObserver | null>(null);
  const rafRef = useRef<number>(0);

  // RAF: re-render at 60fps.
  // Real data (solid line) ends at the last polled point.
  // Estimated tail (dashed, lighter) extends from there to now.
  useEffect(() => {
    const tick = () => {
      const plot = plotRef.current;
      if (plot) {
        const pts = series.ring.bucket(5000);
        if (pts.length >= 2) {
          const now2 = Date.now() / 1000;
          const age = now2 - pts[0].t / 1000;
          const win2 = Math.max(10, Math.min(age, 300));
          const lastVal = pts[pts.length - 1].v;
          const n = pts.length;

          // xs: all real timestamps + now
          // ys: real values + null  (solid line stops at last real point)
          // ye: nulls…lastVal, lastVal  (dashed tail: last real → now)
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

          plot.batch(() => {
            plot.setData([xs, ys, ye]);
            plot.setScale("x", { min: now2 - win2, max: now2 });
          });
        }
      }
      rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
  }, [series]);

  // Create the plot when data is first available; destroy if data dries up.
  // All data updates are handled by the RAF loop above.
  useEffect(() => {
    const el = containerRef.current;
    if (!el?.offsetWidth) return;

    const points = series.ring.bucket(5000);
    if (points.length < 2) {
      if (plotRef.current) {
        plotRef.current.destroy();
        plotRef.current = null;
      }
      return;
    }

    // Plot already exists — RAF handles updates.
    if (plotRef.current) return;

    const now = Date.now() / 1000;
    const times = points.map((p) => p.t / 1000);
    const vals = points.map((p) => p.v);
    const age = now - times[0];
    const win = Math.max(10, Math.min(age, 300));

    const opts: uPlot.Options = {
      width: el.offsetWidth,
      height: 130,
      padding: [4, 4, 6, 0],
      cursor: {
        show: true,
        x: true,
        y: false,
        points: { show: true, size: 6, fill: series.color },
      },
      legend: { show: false },
      axes: [
        {
          stroke: "#94a3b8",
          grid: { stroke: "#252d40", width: 1 },
          ticks: { show: false },
          gap: 4,
          size: 18,
          font: '9px "SF Mono", "Cascadia Code", monospace',
          values: (_, ticks) =>
            ticks.map((t, i, arr) => {
              if (i === arr.length - 1) return "now";
              const ago = Math.round(Date.now() / 1000 - t);
              if (ago < 60) return `${ago}s`;
              return `${Math.floor(ago / 60)}m`;
            }),
        },
        {
          stroke: "#94a3b8",
          grid: { stroke: "#252d40", width: 1 },
          ticks: { show: false },
          gap: 4,
          size: 32,
          font: '9px "SF Mono", "Cascadia Code", monospace',
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
          // Real data — solid line + fill
          stroke: series.color,
          fill: `${series.color}33`,
          width: 2,
          points: { show: false },
          paths: uPlot.paths.spline?.(),
        },
        {
          // Estimated tail — dashed, lighter, no fill
          stroke: `${series.color}66`,
          width: 1.5,
          dash: [5, 4],
          points: { show: false },
        },
      ],
    };

    const lastVal = vals[vals.length - 1];
    const initYe = vals.map((_, i) => (i === vals.length - 1 ? lastVal : null));
    const plot = new uPlot(opts, [times, vals, initYe], el);
    plotRef.current = plot;

    const ro = new ResizeObserver(() => {
      if (el.offsetWidth > 0) {
        plot.setSize({ width: el.offsetWidth, height: 130 });
      }
    });
    ro.observe(el);
    roRef.current = ro;

    return () => {
      ro.disconnect();
      plot.destroy();
      plotRef.current = null;
      roRef.current = null;
    };
  }, [series]);

  const hasData = series.ring.bucket(5000).length >= 2;

  return (
    <div ref={containerRef} style="width:100%;min-height:130px;overflow:hidden">
      {!hasData && (
        <div style="display:flex;align-items:center;justify-content:center;height:130px;color:var(--t4);font-size:11px">
          waiting for data…
        </div>
      )}
    </div>
  );
}
