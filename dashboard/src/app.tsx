import { useCallback, useEffect, useMemo, useRef, useState } from "preact/hooks";
import { api } from "./api";
import { ChartGrid } from "./components/ChartGrid";
import { ConfigView } from "./components/ConfigView";
import { LogViewer } from "./components/LogViewer";
import { MetricBadges } from "./components/MetricBadges";
import { PipelineView } from "./components/PipelineView";
import { StatusBar } from "./components/StatusBar";
import { fmt, fmtBytes, fmtBytesCompact, fmtCompact } from "./lib/format";
import {
  createMetricRegistry,
  orderedMetrics,
  PIPELINE_METRIC_ORDER,
  pushMetricHistorySample,
  pushMetricSample,
  SYSTEM_METRIC_ORDER,
} from "./lib/metricRegistry";
import { extractMetricSnapshot, extractTraceRecords } from "./lib/otlpProcess";
import { RateTracker } from "./lib/rates";
import { RingBuffer } from "./lib/ring";
import { useTelemetryWebSocket } from "./lib/useTelemetryWebSocket";
import type { MetricId, StatsResponse, StatusResponse, TraceRecord } from "./types";

const POLL_OPTIONS = [
  { label: "500ms", ms: 500 },
  { label: "1s", ms: 1000 },
  { label: "2s", ms: 2000 },
  { label: "5s", ms: 5000 },
];

export interface MetricSeries {
  id: MetricId;
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
const HISTORY_MAP: Record<string, { series: MetricId; mode: "gauge" | "counter" }> = {
  input_lines: { series: "lps", mode: "counter" },
  input_bytes: { series: "bps", mode: "counter" },
  output_bytes: { series: "obps", mode: "counter" },
  output_errors: { series: "err", mode: "counter" },
  mem_allocated: { series: "mem", mode: "gauge" },
  inflight_batches: { series: "inflight", mode: "gauge" },
};

export function App() {
  const [connected, setConnected] = useState(false);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [traces, setTraces] = useState<TraceRecord[]>([]);
  const [totalErrors, setTotalErrors] = useState(0);
  const [pollMs, setPollMs] = useState(POLL_OPTIONS[2].ms); // default 2s
  const metricRegistryRef = useRef(createMetricRegistry(createSeries()));
  const [, forceUpdate] = useState(0);
  const historyLoaded = useRef(false);

  // Load server-side history on mount — gives charts data from before page open.
  useEffect(() => {
    if (historyLoaded.current) return;
    historyLoaded.current = true;
    api.history().then((hist) => {
      if (!hist) return;
      const metricRegistry = metricRegistryRef.current;
      const now = Date.now();

      for (const [metricName, points] of Object.entries(hist)) {
        const mapping = HISTORY_MAP[metricName];
        if (!mapping || points.length < 2) continue;
        // Server times are seconds-since-start. Convert to epoch ms
        // by anchoring the latest point to "now".
        const latestServerT = points[points.length - 1][0];

        if (mapping.mode === "gauge") {
          for (const [t, v] of points) {
            pushMetricHistorySample(
              metricRegistry,
              mapping.series,
              now - (latestServerT - t) * 1000,
              v
            );
          }
        } else {
          // Counter → compute deltas as rates.
          for (let i = 1; i < points.length; i++) {
            const dt = points[i][0] - points[i - 1][0];
            if (dt <= 0) continue;
            let rate = (points[i][1] - points[i - 1][1]) / dt;
            if (metricName === "cpu_user_ms") rate /= 10; // ms/s → %
            if (rate < 0) rate = 0;
            pushMetricHistorySample(
              metricRegistry,
              mapping.series,
              now - (latestServerT - points[i][0]) * 1000,
              rate
            );
          }
        }
      }
      // CPU: sum cpu_user_ms + cpu_sys_ms deltas to match live path.
      const cpuUser = hist.cpu_user_ms;
      const cpuSys = hist.cpu_sys_ms;
      if (cpuUser && cpuSys && cpuUser.length >= 2 && cpuSys.length >= 2) {
        {
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
            pushMetricHistorySample(
              metricRegistry,
              "cpu",
              now - (latestT - cpuUser[i][0]) * 1000,
              rate
            );
          }
        }
      }

      // Batch latency: delta(scan+transform+output seconds) / delta(batches) * 1000ms.
      const batchesHist = hist.batches;
      const scanHist = hist.scan_sec;
      const transformHist = hist.transform_sec;
      const outputHistSec = hist.output_sec;
      if (batchesHist && scanHist && transformHist && outputHistSec && batchesHist.length >= 2) {
        {
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
            if (avgMs >= 0) {
              pushMetricHistorySample(metricRegistry, "lat", now - (latestT - t1) * 1000, avgMs);
            }
          }
        }
      }

      // Batch rate (batches/min) from server history.
      if (batchesHist && batchesHist.length >= 2) {
        {
          const latestT = batchesHist[batchesHist.length - 1][0];
          for (let i = 1; i < batchesHist.length; i++) {
            const dt = batchesHist[i][0] - batchesHist[i - 1][0];
            if (dt <= 0) continue;
            const rate = ((batchesHist[i][1] - batchesHist[i - 1][1]) / dt) * 60; // batches/min
            if (rate >= 0) {
              pushMetricHistorySample(
                metricRegistry,
                "batches",
                now - (latestT - batchesHist[i][0]) * 1000,
                rate
              );
            }
          }
        }
      }

      // Backpressure stalls/sec from server history.
      const stallsHist = hist.backpressure_stalls;
      if (stallsHist && stallsHist.length >= 2) {
        {
          const latestT = stallsHist[stallsHist.length - 1][0];
          for (let i = 1; i < stallsHist.length; i++) {
            const dt = stallsHist[i][0] - stallsHist[i - 1][0];
            if (dt <= 0) continue;
            const rate = (stallsHist[i][1] - stallsHist[i - 1][1]) / dt; // stalls/sec
            if (rate >= 0) {
              pushMetricHistorySample(
                metricRegistry,
                "stalls",
                now - (latestT - stallsHist[i][0]) * 1000,
                rate
              );
            }
          }
        }
      }

      forceUpdate((n) => n + 1);
    });
  }, []);

  // ── WebSocket telemetry (preferred) ────────────────────────────────────────
  const { lastMessage } = useTelemetryWebSocket();

  // Process OTLP metrics from WebSocket push.
  const processOtlpMetrics = useCallback(
    (doc: import("@otlpkit/otlpjson").OtlpMetricsDocument) => {
      const snap = extractMetricSnapshot(doc);
      setConnected(true);

      const metricRegistry = metricRegistryRef.current;

      const lps = rates.rate("lps", snap.inputLines);
      const bps = rates.rate("bps", snap.inputBytes);
      const eps = rates.rate("eps", snap.outputErrors);
      const obpsRate = rates.rate("obps", snap.outputBytes);

      if (lps != null) pushMetricSample(metricRegistry, "lps", lps, fmt(lps));
      if (bps != null) pushMetricSample(metricRegistry, "bps", bps, fmtBytes(bps));
      if (eps != null) pushMetricSample(metricRegistry, "err", eps, fmt(eps));
      if (obpsRate != null) pushMetricSample(metricRegistry, "obps", obpsRate, fmtBytes(obpsRate));

      if (snap.cpuUserMs != null && snap.cpuSysMs != null) {
        const cpuMs = snap.cpuUserMs + snap.cpuSysMs;
        const cpuRate = rates.rate("cpu_ms", cpuMs);
        if (cpuRate != null) {
          const cpuPct = cpuRate / 10;
          pushMetricSample(metricRegistry, "cpu", cpuPct, cpuPct.toFixed(1));
        }
      }

      const memBytes = snap.memAllocated ?? snap.rssBytes;
      if (memBytes != null) {
        const limit = snap.memResident
          ? `/ ${fmtBytes(snap.memResident)} resident`
          : undefined;
        pushMetricSample(metricRegistry, "mem", memBytes, fmtBytes(memBytes), limit);
      }

      pushMetricSample(
        metricRegistry,
        "inflight",
        snap.inflightBatches,
        snap.inflightBatches.toFixed(0)
      );

      const batchRate = rates.rate("batches", snap.batches);
      if (batchRate != null) {
        const bpm = batchRate * 60;
        pushMetricSample(metricRegistry, "batches", bpm, fmtCompact(bpm));
      }

      const stallRate = rates.rate("stalls", snap.backpressureStalls);
      if (stallRate != null) {
        pushMetricSample(metricRegistry, "stalls", stallRate, stallRate.toFixed(1));
      }

      // Build a synthetic StatsResponse for MetricBadges and latency.
      setStats({
        uptime_sec: snap.uptimeSeconds,
        rss_bytes: snap.rssBytes,
        cpu_user_ms: snap.cpuUserMs,
        cpu_sys_ms: snap.cpuSysMs,
        input_lines: snap.inputLines,
        input_bytes: snap.inputBytes,
        output_lines: 0,
        output_bytes: snap.outputBytes,
        output_errors: snap.outputErrors,
        batches: snap.batches,
        scan_sec: snap.scanNanos / 1e9,
        transform_sec: snap.transformNanos / 1e9,
        output_sec: snap.outputNanos / 1e9,
        backpressure_stalls: snap.backpressureStalls,
        inflight_batches: snap.inflightBatches,
        mem_resident: snap.memResident ?? undefined,
        mem_allocated: snap.memAllocated ?? undefined,
        mem_active: snap.memActive ?? undefined,
      });

      setTotalErrors(snap.outputErrors);
      forceUpdate((n) => n + 1);
    },
    []
  );

  // Max traces to retain in the dashboard (prevents unbounded growth).
  const MAX_TRACES = 1000;

  // Process OTLP spans from WebSocket push (delta delivery).
  // Server sends only NEW completed spans + all current in-progress spans.
  const processOtlpTraces = useCallback(
    (doc: import("@otlpkit/otlpjson").OtlpTracesDocument) => {
      const incoming = extractTraceRecords(doc);
      setTraces((prev) => {
        // Separate incoming into completed and in-progress.
        const incomingCompleted: TraceRecord[] = [];
        const incomingInProgress = new Map<string, TraceRecord>();
        for (const t of incoming) {
          if (t.lifecycle_state === "completed") {
            incomingCompleted.push(t);
          } else {
            incomingInProgress.set(t.trace_id, t);
          }
        }

        // Start with all previously completed traces (stable, won't be re-sent).
        const merged: TraceRecord[] = [];
        for (const t of prev) {
          if (t.lifecycle_state === "completed") {
            merged.push(t);
          }
          // Drop old in-progress entries — they'll be replaced by the fresh set.
        }

        // Add new completed traces from this tick.
        const seen = new Set(merged.map((t) => t.trace_id));
        for (const t of incomingCompleted) {
          if (!seen.has(t.trace_id)) {
            merged.push(t);
            seen.add(t.trace_id);
          }
        }

        // Append fresh in-progress traces.
        for (const t of incomingInProgress.values()) {
          if (!seen.has(t.trace_id)) {
            merged.push(t);
          }
        }

        // Cap to most recent traces (in-progress always kept, oldest completed trimmed).
        if (merged.length > MAX_TRACES) {
          // Sort: in-progress first, then by start time descending.
          merged.sort((a, b) => {
            const aIp = a.lifecycle_state !== "completed" ? 1 : 0;
            const bIp = b.lifecycle_state !== "completed" ? 1 : 0;
            if (aIp !== bIp) return bIp - aIp;
            return Number(BigInt(b.start_unix_ns) - BigInt(a.start_unix_ns));
          });
          merged.length = MAX_TRACES;
        }

        return merged;
      });

      // Compute batch latency from completed traces.
      const done = incoming
        .filter((t) => t.lifecycle_state === "completed" && Number(t.total_ns) > 0)
        .slice(0, 50);
      if (done.length > 0) {
        const avgMs =
          done.reduce((s, t) => s + (Number(t.total_ns ?? "0") || 0), 0) / done.length / 1e6;
        const formatted =
          avgMs >= 1000
            ? `${(avgMs / 1000).toFixed(1)}s`
            : avgMs >= 1
              ? `${avgMs.toFixed(0)}ms`
              : `${avgMs.toFixed(1)}ms`;
        pushMetricSample(metricRegistryRef.current, "lat", avgMs, formatted);
      }

      forceUpdate((n) => n + 1);
    },
    []
  );

  // ── Route WebSocket messages to the correct handler ────────────────────────
  useEffect(() => {
    if (!lastMessage) return;
    switch (lastMessage.signal) {
      case "metrics":
        processOtlpMetrics(lastMessage.data);
        break;
      case "traces":
        processOtlpTraces(lastMessage.data);
        break;
      case "logs":
        // Logs are handled by LogViewer via REST polling for now.
        break;
    }
  }, [lastMessage, processOtlpMetrics, processOtlpTraces]);

  // ── Status polling (always runs — OTLP metrics don't carry pipeline topology) ──
  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout>;
    let backoff = pollMs;

    const loop = () => {
      api
        .status()
        .then(
          (statusData) => {
            if (statusData) {
              setStatus(statusData);
              setConnected(true);
            }
            backoff = pollMs;
          },
          () => {
            setConnected(false);
            backoff = Math.min(backoff * 2, 30_000);
          }
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
  }, [pollMs]);

  const version = status?.system?.version ?? "?";
  const uptime = stats?.uptime_sec ?? status?.system?.uptime_seconds ?? 0;
  const componentHealth = status?.component_health.status ?? "failed";
  const ready = status?.ready.status ?? "not_ready";
  const statusReason = status?.ready.reason ?? status?.component_health.reason ?? "";

  // Stable references — series composition never changes, only data mutated in place.
  const pipelineSeries = useMemo(
    () => orderedMetrics(metricRegistryRef.current, PIPELINE_METRIC_ORDER),
    []
  );
  const systemSeries = useMemo(
    () => orderedMetrics(metricRegistryRef.current, SYSTEM_METRIC_ORDER),
    []
  );

  return (
    <>
      <StatusBar
        connected={connected}
        componentHealth={componentHealth}
        ready={ready}
        statusReason={statusReason}
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

        {status?.pipelines.map((p) => (
          <PipelineView
            key={p.name}
            pipeline={p}
            traces={traces.filter((t) => t.pipeline === p.name || t.pipeline === "")}
            pollMs={pollMs}
            setPollMs={setPollMs}
          />
        ))}

        <ConfigView />
      </main>
    </>
  );
}
