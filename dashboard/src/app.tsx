import { useCallback, useEffect, useState } from "preact/hooks";
import { api } from "./api";
import { Collapsible } from "./components/Collapsible";
import { ConfigView } from "./components/ConfigView";
import { DataLossIndicator } from "./components/DataLossIndicator";
import { LogViewer } from "./components/LogViewer";
import { PipelineView } from "./components/PipelineView";
import { StatusBar } from "./components/StatusBar";
import { SystemStrip } from "./components/SystemStrip";
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
    const val = (name: string): number => {
      const frame = store.selectLatestValues({ metricName: name });
      return frame.rows[0]?.value ?? 0;
    };
    const sum = (name: string): number => {
      const frame = store.selectLatestValues({
        metricName: name,
        splitBy: "pipeline",
      });
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

  const MAX_TRACES = 1000;

  const processOtlpTraces = useCallback((doc: import("@otlpkit/otlpjson").OtlpTracesDocument) => {
    const incoming = extractTraceRecords(doc);
    setTraces((prev) => mergeTraces(prev, incoming, MAX_TRACES));
  }, []);

  const handleMessage = useCallback(
    (msg: import("./lib/useTelemetryWebSocket").OtlpMessage) => {
      if (msg.signal === "metrics") {
        ingest(msg.data);
        updateStats();
      } else if (msg.signal === "traces") {
        processOtlpTraces(msg.data);
      }
    },
    [ingest, updateStats, processOtlpTraces]
  );

  const { wsConnected } = useTelemetryWebSocket(handleMessage);

  // ── Status polling ──
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
        {/* ── Data Loss — the #1 question: "Am I losing data?" ── */}
        {status?.pipelines && status.pipelines.length > 0 && (
          <DataLossIndicator pipelines={status.pipelines} />
        )}

        {/* ── Pipelines — one card per pipeline with inline sparklines ── */}
        {status?.pipelines.map((p, i) => (
          <PipelineView
            key={p.name}
            pipeline={p}
            traces={traces.filter((t) => t.pipeline === p.name || (t.pipeline === "" && i === 0))}
            pollMs={pollMs}
            setPollMs={setPollMs}
            store={store}
            tick={tick}
          />
        ))}

        {/* ── System strip — compact CPU / Memory / Inflight / Uptime ── */}
        <SystemStrip store={store} tick={tick} uptimeSec={uptime} />

        {/* ── Collapsible utility sections ── */}
        <Collapsible title="Logs">
          <LogViewer />
        </Collapsible>

        <Collapsible title="Configuration">
          <ConfigView />
        </Collapsible>
      </main>
    </>
  );
}
