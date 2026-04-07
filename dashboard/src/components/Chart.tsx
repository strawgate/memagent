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

  // Single RAF loop: creates the plot when data first arrives, updates it every frame,
  // and destroys it if data dries up. Using RAF for creation avoids the stale-closure
  // problem where the init useEffect only fires once (on mount, when the ring is still
  // empty) because `series` is a mutable object whose reference never changes.
  useEffect(() => {
    const el = containerRef.current;

    const buildOpts = (pts: ReturnType<typeof series.ring.bucket>): uPlot.Options => {
      const now = Date.now() / 1000;
      const age = now - pts[0].t / 1000;
      const win = Math.max(10, Math.min(age, 300));
      return {
        width: el?.offsetWidth ?? 300,
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
    };

    const tick = () => {
      const pts = series.ring.bucket(5000);

      if (pts.length < 2) {
        // Destroy plot if data dried up.
        if (plotRef.current) {
          plotRef.current.destroy();
          plotRef.current = null;
          roRef.current?.disconnect();
          roRef.current = null;
        }
      } else {
        // Create plot on first frame that has data.
        if (!plotRef.current && el?.offsetWidth) {
          const times = pts.map((p) => p.t / 1000);
          const vals = pts.map((p) => p.v);
          const lastVal = vals[vals.length - 1];
          const initYe = vals.map((_, i) => (i === vals.length - 1 ? lastVal : null));
          const plot = new uPlot(buildOpts(pts), [times, vals, initYe], el);
          plotRef.current = plot;

          const ro = new ResizeObserver(() => {
            if (el.offsetWidth > 0) plot.setSize({ width: el.offsetWidth, height: 130 });
          });
          ro.observe(el);
          roRef.current = ro;
        }

        // Update data every frame.
        if (plotRef.current) {
          const now2 = Date.now() / 1000;
          const age = now2 - pts[0].t / 1000;
          const win2 = Math.max(10, Math.min(age, 300));
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
