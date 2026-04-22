import { useCallback, useEffect, useRef, useState } from "preact/hooks";
import { api } from "./api";
import type { ChartConfig } from "./components/Chart";
import { ChartGrid, discoverPipelines, PipelineLegend } from "./components/ChartGrid";
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

/** Always-visible primary charts. */
const PRIMARY_CHARTS: ChartConfig[] = [
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
];

/** Charts shown only when "Show More" is toggled or they have non-zero values. */
const EXTRA_CHARTS: ChartConfig[] = [
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
  const [showMoreCharts, setShowMoreCharts] = useState(false);
  const [hiddenPipelines, setHiddenPipelines] = useState<Set<string>>(new Set());
  const [pollMs, setPollMs] = useState(POLL_OPTIONS[1].ms); // default 1s

  // ── WebSocket telemetry → TelemetryStore ─────────────────────────────────
  const { store, tick, ingest } = useTelemetryStore();

  // Build stats from latest OTLP metrics in the store.
  const updateStats = useCallback(() => {
    const val = (name: string): number => {
      const frame = store.selectLatestValues({ metricName: name });
      return frame.rows[0]?.value ?? 0;
    };

    setStats({
      uptime_sec: val("logfwd.uptime_seconds"),
      rss_bytes: val("process.memory.rss"),
      cpu_user_ms: null,
      cpu_sys_ms: null,
      input_lines: val("logfwd.input_lines"),
      input_bytes: val("logfwd.input_bytes"),
      output_lines: 0,
      output_bytes: val("logfwd.output_bytes"),
      output_errors: val("logfwd.output_errors"),
      batches: val("logfwd.batches"),
      scan_sec: val("logfwd.stage_nanos") / 1e9,
      transform_sec: 0,
      output_sec: 0,
      backpressure_stalls: val("logfwd.backpressure_stalls"),
      inflight_batches: val("logfwd.inflight_batches"),
      channel_depth: val("logfwd.channel_depth") ?? undefined,
      channel_capacity: val("logfwd.channel_capacity") ?? undefined,
      mem_resident: val("process.memory.resident") ?? undefined,
      mem_allocated: val("process.memory.allocated") ?? undefined,
      mem_active: val("process.memory.active") ?? undefined,
    });
    setTotalErrors(val("logfwd.output_errors"));
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

  // Track WS connectivity — dashboard shows connected only when both
  // the WebSocket and the status poll are healthy.
  const wsConnectedRef = useRef(wsConnected);
  wsConnectedRef.current = wsConnected;

  useEffect(() => {
    setConnected((prev) => (wsConnected ? prev : false));
  }, [wsConnected]);

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
              setConnected(wsConnectedRef.current);
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

  // Decide which extra charts to show: always show charts with non-zero data,
  // or show all when user clicks "Show More".
  const visibleExtras = showMoreCharts
    ? EXTRA_CHARTS
    : EXTRA_CHARTS.filter((cfg) => {
        const frame = store.selectTimeSeries({
          metricName: cfg.metricName,
          intervalMs: 1000,
          reduce: "last",
          ...(cfg.splitBy ? { splitBy: cfg.splitBy } : {}),
        });
        return frame.series.some((s) => s.points.some((pt) => pt.value > 0));
      });

  const hasHiddenCharts = !showMoreCharts && visibleExtras.length < EXTRA_CHARTS.length;

  // Discover pipeline names for the legend from chart data.
  const allPipelineCharts = [...PRIMARY_CHARTS, ...EXTRA_CHARTS];
  const pipelineNames = discoverPipelines(store, allPipelineCharts);
  const togglePipeline = useCallback((key: string) => {
    setHiddenPipelines((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

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
          <PipelineLegend
            pipelines={pipelineNames}
            hidden={hiddenPipelines}
            onToggle={togglePipeline}
          />
          <ChartGrid
            store={store}
            charts={[...PRIMARY_CHARTS, ...visibleExtras]}
            tick={tick}
            hiddenPipelines={hiddenPipelines}
          />
          {hasHiddenCharts && (
            <button type="button" class="show-more-btn" onClick={() => setShowMoreCharts(true)}>
              Show More Charts
            </button>
          )}
          {showMoreCharts && visibleExtras.length === EXTRA_CHARTS.length && (
            <button type="button" class="show-more-btn" onClick={() => setShowMoreCharts(false)}>
              Show Less
            </button>
          )}
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
