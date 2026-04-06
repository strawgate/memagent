import { useCallback, useEffect, useMemo, useRef, useState } from "preact/hooks";
import { api } from "./api";
import { ChartGrid } from "./components/ChartGrid";
import { ConfigView } from "./components/ConfigView";
import { LogViewer } from "./components/LogViewer";
import { MetricBadges } from "./components/MetricBadges";
import { PipelineView } from "./components/PipelineView";
import { StatusBar } from "./components/StatusBar";
import { fmt, fmtBytes, fmtBytesCompact, fmtCompact } from "./lib/format";
import { RateTracker } from "./lib/rates";
import { RingBuffer } from "./lib/ring";
import type { PipelinesResponse, StatsResponse, TraceRecord } from "./types";

const POLL_OPTIONS = [
  { label: "500ms", ms: 500 },
  { label: "1s", ms: 1000 },
  { label: "2s", ms: 2000 },
  { label: "5s", ms: 5000 },
];

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
    {
      id: "lps",
      label: "Lines / sec",
      color: "#3b82f6",
      ring: new RingBuffer(),
      value: "-",
      unit: "/s",
      fmtAxis: fmtCompact,
      yRange: [0, 1000],
    },
    {
      id: "bps",
      label: "Input Bytes/s",
      color: "#8b5cf6",
      ring: new RingBuffer(),
      value: "-",
      unit: "/s",
      fmtAxis: fmtBytesCompact,
      yRange: [0, 102400],
    },
    {
      id: "obps",
      label: "Output Bytes/s",
      color: "#22c55e",
      ring: new RingBuffer(),
      value: "-",
      unit: "/s",
      fmtAxis: fmtBytesCompact,
      yRange: [0, 102400],
    },
    {
      id: "err",
      label: "Errors / sec",
      color: "#ef4444",
      ring: new RingBuffer(),
      value: "-",
      unit: "/s",
      fmtAxis: fmtCompact,
      yRange: [0, 10],
    },
    {
      id: "cpu",
      label: "Process CPU",
      color: "#f59e0b",
      ring: new RingBuffer(),
      value: "-",
      unit: "%",
      fmtAxis: (v) => v.toFixed(1),
      yRange: [0, 10],
    },
    {
      id: "mem",
      label: "Memory",
      color: "#10b981",
      ring: new RingBuffer(),
      value: "-",
      unit: "",
      fmtAxis: fmtBytesCompact,
      yRange: [0, 67108864],
    },
    {
      id: "lat",
      label: "Batch Latency",
      color: "#06b6d4",
      ring: new RingBuffer(),
      value: "-",
      unit: "",
      fmtAxis: (v) => (v >= 1000 ? `${(v / 1000).toFixed(1)}s` : `${v.toFixed(0)}ms`),
      yRange: [0, 100],
    },
    {
      id: "inflight",
      label: "Inflight Batches",
      color: "#f97316",
      ring: new RingBuffer(),
      value: "-",
      unit: "",
      fmtAxis: (v) => v.toFixed(0),
      yRange: [0, 10],
    },
    {
      id: "batches",
      label: "Batch Rate",
      color: "#a78bfa",
      ring: new RingBuffer(),
      value: "-",
      unit: "/min",
      fmtAxis: fmtCompact,
      yRange: [0, 10],
    },
    {
      id: "stalls",
      label: "Scan Stalls",
      color: "#fb7185",
      ring: new RingBuffer(),
      value: "-",
      unit: "/s",
      fmtAxis: (v) => v.toFixed(1),
      yRange: [0, 1],
    },
  ];
}

/** Map server history counters → dashboard series. */
const HISTORY_MAP: Record<string, { series: string; mode: "gauge" | "counter" }> = {
  input_lines: { series: "lps", mode: "counter" },
  input_bytes: { series: "bps", mode: "counter" },
  output_bytes: { series: "obps", mode: "counter" },
  output_errors: { series: "err", mode: "counter" },
  mem_allocated: { series: "mem", mode: "gauge" },
  inflight_batches: { series: "inflight", mode: "gauge" },
};

export function App() {
  const [connected, setConnected] = useState(false);
  const [pipes, setPipes] = useState<PipelinesResponse | null>(null);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [traces, setTraces] = useState<TraceRecord[]>([]);
  const [totalErrors, setTotalErrors] = useState(0);
  const [pollMs, setPollMs] = useState(POLL_OPTIONS[2].ms); // default 2s
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
      // CPU: sum cpu_user_ms + cpu_sys_ms deltas to match live path.
      const cpuUser = hist.cpu_user_ms;
      const cpuSys = hist.cpu_sys_ms;
      if (cpuUser && cpuSys && cpuUser.length >= 2 && cpuSys.length >= 2) {
        const cpuS = series.find((s) => s.id === "cpu");
        if (cpuS) {
          // Build a map from time → value for cpuSys for quick lookup.
          const sysMap = new Map<number, number>(cpuSys.map(([t, v]) => [t, v]));
          const latestT = cpuUser[cpuUser.length - 1][0];
          for (let i = 1; i < cpuUser.length; i++) {
            const dt = cpuUser[i][0] - cpuUser[i - 1][0];
            if (dt <= 0) continue;
            const prevSys = sysMap.get(cpuUser[i - 1][0]);
            const curSys = sysMap.get(cpuUser[i][0]);
            if (prevSys == null || curSys == null) continue;
            const totalMs = cpuUser[i][1] - cpuUser[i - 1][1] + (curSys - prevSys);
            let rate = totalMs / dt / 10; // ms/s → %
            if (rate < 0) rate = 0;
            cpuS.ring.pushRaw(now - (latestT - cpuUser[i][0]) * 1000, rate);
          }
        }
      }

      // Batch latency: delta(scan+transform+output seconds) / delta(batches) * 1000ms.
      const batchesHist = hist.batches;
      const scanHist = hist.scan_sec;
      const transformHist = hist.transform_sec;
      const outputHistSec = hist.output_sec;
      if (batchesHist && scanHist && transformHist && outputHistSec && batchesHist.length >= 2) {
        const latS = series.find((s) => s.id === "lat");
        if (latS) {
          const scanMap = new Map<number, number>(scanHist.map(([t, v]) => [t, v]));
          const trMap = new Map<number, number>(transformHist.map(([t, v]) => [t, v]));
          const outMap = new Map<number, number>(outputHistSec.map(([t, v]) => [t, v]));
          const latestT = batchesHist[batchesHist.length - 1][0];
          for (let i = 1; i < batchesHist.length; i++) {
            const t0 = batchesHist[i - 1][0],
              t1 = batchesHist[i][0];
            const dBatches = batchesHist[i][1] - batchesHist[i - 1][1];
            if (dBatches <= 0) continue;
            const s0 = scanMap.get(t0),
              s1 = scanMap.get(t1);
            const tr0 = trMap.get(t0),
              tr1 = trMap.get(t1);
            const o0 = outMap.get(t0),
              o1 = outMap.get(t1);
            if (s0 == null || s1 == null || tr0 == null || tr1 == null || o0 == null || o1 == null)
              continue;
            const totalSec = s1 - s0 + (tr1 - tr0) + (o1 - o0);
            const avgMs = (totalSec / dBatches) * 1000;
            if (avgMs >= 0) latS.ring.pushRaw(now - (latestT - t1) * 1000, avgMs);
          }
        }
      }

      forceUpdate((n) => n + 1);
    });
  }, []);

  const poll = useCallback(async () => {
    const [pipeData, statsData, tracesData] = await Promise.all([
      api.pipelines(),
      api.stats(),
      api.traces(),
    ]);
    if (tracesData) setTraces(tracesData.traces);

    if (pipeData) {
      setConnected(true);
      setPipes(pipeData);

      let tl = 0,
        tb = 0,
        te = 0;
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

      if (lps != null) {
        series[0].ring.push(lps);
        series[0].value = fmt(lps);
      }
      if (bps != null) {
        series[1].ring.push(bps);
        series[1].value = fmtBytes(bps);
      }
      if (eps != null) {
        series[3].ring.push(eps);
        series[3].value = fmt(eps);
      }
    } else {
      setConnected(false);
    }

    if (statsData) {
      setStats(statsData);
      const series = seriesRef.current;

      // Output bytes/sec (series[2])
      const obpsRate = rates.rate("obps", statsData.output_bytes);
      if (obpsRate != null) {
        series[2].ring.push(obpsRate);
        series[2].value = fmtBytes(obpsRate);
      }

      if (statsData.cpu_user_ms != null && statsData.cpu_sys_ms != null) {
        const cpuMs = statsData.cpu_user_ms + statsData.cpu_sys_ms;
        const cpuRate = rates.rate("cpu_ms", cpuMs);
        if (cpuRate != null) {
          const cpuPct = cpuRate / 10;
          series[4].ring.push(cpuPct);
          series[4].value = cpuPct.toFixed(1);
        }
      }

      const memBytes = statsData.mem_allocated ?? statsData.rss_bytes;
      if (memBytes != null) {
        series[5].ring.push(memBytes);
        series[5].value = fmtBytes(memBytes);
        if (statsData.mem_resident) {
          series[5].limit = `/ ${fmtBytes(statsData.mem_resident)} resident`;
        }
      }

      // Batch latency: rolling average of total_ns from recent traces.
      // This gives true ms/batch rather than the cumulative-rate approximation.
      if (tracesData && tracesData.traces.length > 0) {
        const done = tracesData.traces.filter((t) => !t.in_progress).slice(0, 50);
        if (done.length > 0) {
          const avgMs = done.reduce((s, t) => s + (Number(t.total_ns ?? "0") || 0), 0) / done.length / 1e6;
          series[6].ring.push(avgMs);
          series[6].value =
            avgMs >= 1000 ? `${(avgMs / 1000).toFixed(1)}s` : `${avgMs.toFixed(0)}ms`;
        }
      }

      // Inflight batches (series[7])
      series[7].ring.push(statsData.inflight_batches);
      series[7].value = statsData.inflight_batches.toFixed(0);

      // Batch rate: batches/min (series[8])
      const batchRate = rates.rate("batches", statsData.batches);
      if (batchRate != null) {
        const bpm = batchRate * 60;
        series[8].ring.push(bpm);
        series[8].value = fmtCompact(bpm);
      }

      // Scan stalls: stalls/sec (series[9])
      const stallRate = rates.rate("stalls", statsData.backpressure_stalls);
      if (stallRate != null) {
        series[9].ring.push(stallRate);
        series[9].value = stallRate.toFixed(1);
      }
    }

    forceUpdate((n) => n + 1);
  }, []);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout>;
    let backoff = pollMs;

    const loop = () => {
      poll()
        .then(
          () => {
            backoff = pollMs;
          }, // success — reset backoff
          () => {
            backoff = Math.min(backoff * 2, 30_000);
          } // error — exponential backoff
        )
        .finally(() => {
          if (!cancelled) timer = setTimeout(loop, backoff);
        });
    };

    loop();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [poll, pollMs]);

  const version = pipes?.system?.version ?? "?";
  const uptime = stats?.uptime_sec ?? pipes?.system?.uptime_seconds ?? 0;

  // Stable references — series composition never changes, only data mutated in place.
  const pipelineSeries = useMemo(
    () =>
      seriesRef.current.filter((s) =>
        ["lps", "bps", "obps", "err", "lat", "inflight", "batches", "stalls"].includes(s.id)
      ),
    []
  );
  const systemSeries = useMemo(
    () => seriesRef.current.filter((s) => ["cpu", "mem"].includes(s.id)),
    []
  );

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
          <div class="heading">Pipeline Metrics</div>
          <ChartGrid series={pipelineSeries} />
        </div>

        <div class="section">
          <div class="heading">System Metrics</div>
          <ChartGrid series={systemSeries} />
        </div>

        <LogViewer />

        {pipes?.pipelines.map((p) => (
          <PipelineView
            key={p.name}
            pipeline={p}
            traces={traces.filter((t) => t.pipeline === p.name)}
            pollMs={pollMs}
            setPollMs={setPollMs}
          />
        ))}

        <ConfigView />
      </main>
    </>
  );
}
