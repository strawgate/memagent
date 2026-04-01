import { useState, useEffect, useCallback, useRef } from "preact/hooks";
import { api } from "./api";
import { RateTracker } from "./lib/rates";
import { RingBuffer } from "./lib/ring";
import { fmt, fmtBytes, fmtCompact, fmtBytesCompact } from "./lib/format";
import type { PipelinesResponse, StatsResponse } from "./types";
import { StatusBar } from "./components/StatusBar";
import { MetricBadges } from "./components/MetricBadges";
import { ChartGrid } from "./components/ChartGrid";
import { PipelineView } from "./components/PipelineView";
import { ConfigView } from "./components/ConfigView";
import { LogViewer } from "./components/LogViewer";

const POLL_MS = 2000;

export interface MetricSeries {
  id: string;
  label: string;
  color: string;
  ring: RingBuffer;
  value: string;
  unit: string;
  limit?: string;
  fmtAxis?: (v: number) => string;
  /** Minimum Y-axis range [min, max]. Auto-scales beyond this. */
  yRange?: [number, number];
}

const rates = new RateTracker();

function createSeries(): MetricSeries[] {
  return [
    { id: "lps", label: "Lines / sec", color: "#3b82f6", ring: new RingBuffer(), value: "-", unit: "/s", fmtAxis: fmtCompact, yRange: [0, 1000] },
    { id: "bps", label: "Input Bytes", color: "#8b5cf6", ring: new RingBuffer(), value: "-", unit: "/s", fmtAxis: fmtBytesCompact, yRange: [0, 102400] },
    { id: "err", label: "Errors / sec", color: "#ef4444", ring: new RingBuffer(), value: "-", unit: "/s", fmtAxis: fmtCompact, yRange: [0, 10] },
    { id: "cpu", label: "Process CPU", color: "#f59e0b", ring: new RingBuffer(), value: "-", unit: "%", fmtAxis: (v) => v.toFixed(1), yRange: [0, 10] },
    { id: "mem", label: "Memory", color: "#10b981", ring: new RingBuffer(), value: "-", unit: "", fmtAxis: fmtBytesCompact, yRange: [0, 67108864] },
    { id: "lat", label: "Batch Latency", color: "#06b6d4", ring: new RingBuffer(), value: "-", unit: "ms", fmtAxis: (v) => v.toFixed(1), yRange: [0, 100] },
  ];
}

/** Map server history counters → dashboard series. */
const HISTORY_MAP: Record<string, { series: string; mode: "gauge" | "counter" }> = {
  input_lines: { series: "lps", mode: "counter" },
  input_bytes: { series: "bps", mode: "counter" },
  output_errors: { series: "err", mode: "counter" },
  cpu_user_ms: { series: "cpu", mode: "counter" },
  mem_allocated: { series: "mem", mode: "gauge" },
};

export function App() {
  const [connected, setConnected] = useState(false);
  const [pipes, setPipes] = useState<PipelinesResponse | null>(null);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [totalErrors, setTotalErrors] = useState(0);
  const seriesRef = useRef(createSeries());
  const [, forceUpdate] = useState(0);
  const historyLoaded = useRef(false);

  // Load server-side history on mount — gives charts data from before page open.
  useEffect(() => {
    if (historyLoaded.current) return;
    historyLoaded.current = true;
    api.history().then((hist) => {
      if (!hist) return;
      const series = seriesRef.current;
      const now = Date.now();

      for (const [metricName, points] of Object.entries(hist)) {
        const mapping = HISTORY_MAP[metricName];
        if (!mapping || points.length < 2) continue;
        const s = series.find((s) => s.id === mapping.series);
        if (!s) continue;

        // Server times are seconds-since-start. Convert to epoch ms
        // by anchoring the latest point to "now".
        const latestServerT = points[points.length - 1][0];

        if (mapping.mode === "gauge") {
          for (const [t, v] of points) {
            s.ring.pushRaw(now - (latestServerT - t) * 1000, v);
          }
        } else {
          // Counter → compute deltas as rates.
          for (let i = 1; i < points.length; i++) {
            const dt = points[i][0] - points[i - 1][0];
            if (dt <= 0) continue;
            let rate = (points[i][1] - points[i - 1][1]) / dt;
            if (metricName === "cpu_user_ms") rate /= 10; // ms/s → %
            if (rate < 0) rate = 0;
            s.ring.pushRaw(now - (latestServerT - points[i][0]) * 1000, rate);
          }
        }
      }
      forceUpdate((n) => n + 1);
    });
  }, []);

  const poll = useCallback(async () => {
    const [pipeData, statsData] = await Promise.all([api.pipelines(), api.stats()]);

    if (pipeData) {
      setConnected(true);
      setPipes(pipeData);

      let tl = 0, tb = 0, te = 0;
      for (const p of pipeData.pipelines) {
        tl += p.transform.lines_in;
        for (const i of p.inputs) tb += i.bytes_total;
        for (const o of p.outputs) te += o.errors;
      }
      setTotalErrors(te);

      const series = seriesRef.current;
      const lps = rates.rate("lps", tl);
      const bps = rates.rate("bps", tb);
      const eps = rates.rate("eps", te);

      if (lps != null) { series[0].ring.push(lps); series[0].value = fmt(lps); }
      if (bps != null) { series[1].ring.push(bps); series[1].value = fmtBytes(bps); }
      if (eps != null) { series[2].ring.push(eps); series[2].value = fmt(eps); }
    } else {
      setConnected(false);
    }

    if (statsData) {
      setStats(statsData);
      const series = seriesRef.current;

      if (statsData.cpu_user_ms != null && statsData.cpu_sys_ms != null) {
        const cpuMs = statsData.cpu_user_ms + statsData.cpu_sys_ms;
        const cpuRate = rates.rate("cpu_ms", cpuMs);
        if (cpuRate != null) {
          const cpuPct = cpuRate / 10;
          series[3].ring.push(cpuPct);
          series[3].value = cpuPct.toFixed(1);
        }
      }

      const memBytes = statsData.mem_allocated ?? statsData.rss_bytes;
      if (memBytes != null) {
        series[4].ring.push(memBytes);
        series[4].value = fmtBytes(memBytes);
        if (statsData.mem_resident) {
          series[4].limit = "/ " + fmtBytes(statsData.mem_resident) + " resident";
        }
      }

      if (statsData.batches > 0) {
        const totalSec = statsData.scan_sec + statsData.transform_sec + statsData.output_sec;
        const latRate = rates.rate("lat", totalSec * 1000);
        if (latRate != null) {
          series[5].ring.push(latRate);
          series[5].value = latRate.toFixed(1);
        }
      }
    }

    forceUpdate((n) => n + 1);
  }, []);

  useEffect(() => {
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => clearInterval(id);
  }, [poll]);

  const version = pipes?.system?.version ?? "?";
  const uptime = stats?.uptime_sec ?? pipes?.system?.uptime_seconds ?? 0;

  return (
    <>
      <StatusBar
        connected={connected}
        totalErrors={totalErrors}
        version={version}
        uptime={uptime}
      />
      <main>
        <MetricBadges stats={stats} />

        <div class="section">
          <div class="heading">Metrics</div>
          <ChartGrid series={seriesRef.current} />
        </div>

        <LogViewer />

        {pipes?.pipelines.map((p) => (
          <PipelineView key={p.name} pipeline={p} />
        ))}

        <ConfigView />
      </main>
    </>
  );
}
