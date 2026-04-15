import { useCallback, useEffect, useState } from "preact/hooks";
import { api } from "./api";
import type { ChartConfig } from "./components/Chart";
import { ChartGrid } from "./components/ChartGrid";
import { ConfigView } from "./components/ConfigView";
import { LogViewer } from "./components/LogViewer";
import { MetricBadges } from "./components/MetricBadges";
import { PipelineView } from "./components/PipelineView";
import { StatusBar } from "./components/StatusBar";
import { fmtBytesCompact, fmtCompact } from "./lib/format";
import { mergeTraces } from "./lib/mergeTraces";
import { extractTraceRecords } from "./lib/otlpProcess";
import { useTelemetryStore } from "./lib/useTelemetryStore";
import { useTelemetryWebSocket } from "./lib/useTelemetryWebSocket";
import type { StatsResponse, StatusResponse, TraceRecord } from "./types";

const POLL_OPTIONS = [
  { label: "500ms", ms: 500 },
  { label: "1s", ms: 1000 },
  { label: "2s", ms: 2000 },
  { label: "5s", ms: 5000 },
];

// ── Chart configurations (pure data — no mutable state) ────────────────────

const PIPELINE_CHARTS: ChartConfig[] = [
  {
    metricName: "logfwd.input_lines_per_sec",
    label: "Lines / sec",
    color: "#3b82f6",
    unit: "/s",
    fmtAxis: fmtCompact,
    yRange: [0, 1000],
    splitBy: "pipeline",
  },
  {
    metricName: "logfwd.input_bytes_per_sec",
    label: "Input Bytes/s",
    color: "#8b5cf6",
    unit: "/s",
    fmtAxis: fmtBytesCompact,
    yRange: [0, 102400],
    splitBy: "pipeline",
  },
  {
    metricName: "logfwd.output_bytes_per_sec",
    label: "Output Bytes/s",
    color: "#22c55e",
    unit: "/s",
    fmtAxis: fmtBytesCompact,
    yRange: [0, 102400],
    splitBy: "pipeline",
  },
  {
    metricName: "logfwd.output_errors_per_sec",
    label: "Errors / sec",
    color: "#ef4444",
    unit: "/s",
    fmtAxis: fmtCompact,
    yRange: [0, 10],
    splitBy: "pipeline",
  },
  {
    metricName: "logfwd.batches_per_min",
    label: "Batch Rate",
    color: "#a78bfa",
    unit: "/min",
    fmtAxis: fmtCompact,
    yRange: [0, 10],
    splitBy: "pipeline",
  },
  {
    metricName: "logfwd.backpressure_stalls_per_sec",
    label: "Scan Stalls",
    color: "#fb7185",
    unit: "/s",
    fmtAxis: (v) => v.toFixed(1),
    yRange: [0, 1],
    splitBy: "pipeline",
  },
];

const SYSTEM_CHARTS: ChartConfig[] = [
  {
    metricName: "logfwd.cpu_percent",
    label: "Process CPU",
    color: "#f59e0b",
    unit: "%",
    fmtAxis: (v) => v.toFixed(1),
    yRange: [0, 10],
  },
  {
    metricName: "process.memory.allocated",
    label: "Memory",
    color: "#10b981",
    unit: "",
    fmtAxis: fmtBytesCompact,
    yRange: [0, 67108864],
  },
  {
    metricName: "logfwd.inflight_batches",
    label: "Inflight Batches",
    color: "#f97316",
    unit: "",
    fmtAxis: (v) => v.toFixed(0),
    yRange: [0, 10],
  },
];

export function App() {
  const [connected, setConnected] = useState(false);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [traces, setTraces] = useState<TraceRecord[]>([]);
  const [totalErrors, setTotalErrors] = useState(0);
  const [pollMs, setPollMs] = useState(POLL_OPTIONS[2].ms); // default 2s

  // ── WebSocket telemetry → TelemetryStore ─────────────────────────────────
  const { store, tick, ingest } = useTelemetryStore();

  // Build stats from latest OTLP metrics in the store.
  const updateStats = useCallback(() => {
    /** Get latest value for a metric (single series, no pipeline split). */
    const val = (name: string): number => {
      const frame = store.selectLatestValues({ metricName: name });
      return frame.rows[0]?.value ?? 0;
    };
    /** Sum latest values across all pipeline series for a metric. */
    const sum = (name: string): number => {
      const frame = store.selectLatestValues({ metricName: name, splitBy: "pipeline" });
      return frame.rows.reduce((acc, r) => acc + r.value, 0);
    };

    setStats({
      uptime_sec: val("logfwd.uptime_seconds"),
      rss_bytes: val("process.memory.rss"),
      cpu_user_ms: null,
      cpu_sys_ms: null,
      input_lines: sum("logfwd.input_lines"),
      input_bytes: sum("logfwd.input_bytes"),
      output_lines: 0,
      output_bytes: sum("logfwd.output_bytes"),
      output_errors: sum("logfwd.output_errors"),
      batches: sum("logfwd.batches"),
      scan_sec: sum("logfwd.stage_nanos") / 1e9,
      transform_sec: 0,
      output_sec: 0,
      backpressure_stalls: sum("logfwd.backpressure_stalls"),
      inflight_batches: sum("logfwd.inflight_batches"),
      mem_resident: val("process.memory.resident") || undefined,
      mem_allocated: val("process.memory.allocated") || undefined,
      mem_active: val("process.memory.active") || undefined,
    });
    setTotalErrors(sum("logfwd.output_errors"));
  }, [store]);

  // Max traces to retain in the dashboard (prevents unbounded growth).
  const MAX_TRACES = 1000;

  // Process OTLP spans from WebSocket push (delta delivery).
  const processOtlpTraces = useCallback((doc: import("@otlpkit/otlpjson").OtlpTracesDocument) => {
    const incoming = extractTraceRecords(doc);
    setTraces((prev) => mergeTraces(prev, incoming, MAX_TRACES));
  }, []);

  // Dispatch each WS frame synchronously — no frames are dropped.
  const handleMessage = useCallback(
    (msg: import("./lib/useTelemetryWebSocket").OtlpMessage) => {
      if (msg.signal === "metrics") {
        ingest(msg.data);
        updateStats();
      } else if (msg.signal === "traces") {
        processOtlpTraces(msg.data);
      }
      // Logs are handled by LogViewer via REST polling.
    },
    [ingest, updateStats, processOtlpTraces]
  );

  const { wsConnected } = useTelemetryWebSocket(handleMessage);

  // `connected` means the status API is reachable. `wsConnected` tracks the
  // live WebSocket independently. The StatusBar shows a separate pill when
  // the WS drops while the API is still up.

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
              backoff = pollMs;
            } else {
              setConnected(false);
              setStatus(null);
            }
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

  return (
    <>
      <StatusBar
        connected={connected}
        wsConnected={wsConnected}
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
          <ChartGrid store={store} charts={PIPELINE_CHARTS} tick={tick} />
        </div>

        <div class="section">
          <div class="heading">System Metrics</div>
          <ChartGrid store={store} charts={SYSTEM_CHARTS} tick={tick} />
        </div>

        <LogViewer />

        {status?.pipelines.map((p, i) => (
          <PipelineView
            key={p.name}
            pipeline={p}
            traces={traces.filter((t) => t.pipeline === p.name || (t.pipeline === "" && i === 0))}
            pollMs={pollMs}
            setPollMs={setPollMs}
          />
        ))}

        <ConfigView />
      </main>
    </>
  );
}
