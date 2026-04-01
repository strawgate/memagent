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

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !el.offsetWidth) return;

    const points = series.ring.points();
    if (points.length < 2) {
      // Destroy stale plot if data dried up.
      if (plotRef.current) {
        plotRef.current.destroy();
        plotRef.current = null;
      }
      return;
    }

    const times = points.map((p) => p.t / 1000);
    const vals = points.map((p) => p.v);
    const now = Date.now() / 1000;
    const age = now - times[0];
    const win = Math.max(10, Math.min(age, 300));

    if (plotRef.current) {
      plotRef.current.batch(() => {
        plotRef.current!.setData([times, vals]);
        plotRef.current!.setScale("x", { min: now - win, max: now });
      });
      return;
    }

    // First render — create the plot.
    const opts: uPlot.Options = {
      width: el.offsetWidth,
      height: 130,
      padding: [8, 8, 0, 0],
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
          size: 24,
          font: '9px "SF Mono", "Cascadia Code", monospace',
          values: (_, ticks) =>
            ticks.map((t) => {
              const ago = Math.round(now - t);
              if (ago <= 1) return "now";
              if (ago < 60) return ago + "s";
              return Math.floor(ago / 60) + "m";
            }),
        },
        {
          stroke: "#94a3b8",
          grid: { stroke: "#252d40", width: 1 },
          ticks: { show: false },
          gap: 4,
          size: 46,
          font: '9px "SF Mono", "Cascadia Code", monospace',
          values: (_, ticks) =>
            ticks.map((v) =>
              series.fmtAxis ? series.fmtAxis(v) : String(Math.round(v)),
            ),
        },
      ],
      scales: {
        x: { min: now - win, max: now },
        y: {
          range: (_u, dataMin, dataMax) => {
            const [yMin, yMax] = series.yRange ?? [0, 100];
            // Use the configured range as minimum, but expand if data exceeds it.
            return [
              Math.min(yMin, dataMin ?? 0),
              Math.max(yMax, (dataMax ?? 0) * 1.1),
            ];
          },
        },
      },
      series: [
        {},
        {
          stroke: series.color,
          fill: series.color + "40",
          width: 2.5,
          points: { show: false },
        },
      ],
    };

    const plot = new uPlot(opts, [times, vals], el);
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
  });

  const hasData = series.ring.points().length >= 2;

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
