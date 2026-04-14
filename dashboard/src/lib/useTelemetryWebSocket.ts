import { useEffect, useRef, useState } from "preact/hooks";
import type {
  OtlpDocument,
  OtlpLogsDocument,
  OtlpMetricsDocument,
  OtlpTracesDocument,
} from "@otlpkit/otlpjson";
import {
  isLogsDocument,
  isMetricsDocument,
  isTracesDocument,
  parseOtlpJson,
} from "@otlpkit/otlpjson";

/** Minimum delay between reconnect attempts (ms). */
const RECONNECT_BASE_MS = 1_000;
/** Maximum delay between reconnect attempts (ms). */
const RECONNECT_MAX_MS = 30_000;

/** Discriminated union of parsed OTLP messages from the WebSocket. */
export type OtlpMessage =
  | { readonly signal: "metrics"; readonly data: OtlpMetricsDocument }
  | { readonly signal: "traces"; readonly data: OtlpTracesDocument }
  | { readonly signal: "logs"; readonly data: OtlpLogsDocument };

/**
 * Connect to the diagnostics WebSocket at `/admin/v1/telemetry`.
 *
 * The server pushes OTLP JSON messages (resourceMetrics, resourceSpans,
 * resourceLogs). Each incoming message is parsed via `@otlpkit/otlpjson`
 * and exposed as `lastMessage`.
 *
 * Automatically reconnects with exponential backoff on close or error.
 * The caller should fall back to HTTP polling when `wsConnected` is false.
 */
export function useTelemetryWebSocket(): {
  wsConnected: boolean;
  lastMessage: OtlpMessage | null;
} {
  const [wsConnected, setWsConnected] = useState(false);
  const [lastMessage, setLastMessage] = useState<OtlpMessage | null>(null);
  const backoffRef = useRef(RECONNECT_BASE_MS);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let cancelled = false;

    function connect() {
      if (cancelled) return;

      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const url = `${proto}//${location.host}/admin/v1/telemetry`;
      ws = new WebSocket(url);

      ws.onopen = () => {
        setWsConnected(true);
        backoffRef.current = RECONNECT_BASE_MS;
      };

      ws.onmessage = (ev) => {
        try {
          const raw = JSON.parse(ev.data);
          const doc: OtlpDocument = parseOtlpJson(raw);
          if (isMetricsDocument(doc)) {
            setLastMessage({ signal: "metrics", data: doc });
          } else if (isTracesDocument(doc)) {
            setLastMessage({ signal: "traces", data: doc });
          } else if (isLogsDocument(doc)) {
            setLastMessage({ signal: "logs", data: doc });
          }
        } catch {
          // Ignore malformed messages.
        }
      };

      ws.onclose = () => {
        setWsConnected(false);
        ws = null;
        scheduleReconnect();
      };

      ws.onerror = () => {
        // onclose fires after onerror, so reconnect is handled there.
      };
    }

    function scheduleReconnect() {
      if (cancelled) return;
      const delay = backoffRef.current;
      backoffRef.current = Math.min(delay * 2, RECONNECT_MAX_MS);
      timer = setTimeout(connect, delay);
    }

    connect();

    return () => {
      cancelled = true;
      if (timer != null) clearTimeout(timer);
      if (ws) {
        ws.onclose = null; // prevent reconnect in cleanup
        ws.close();
      }
      setWsConnected(false);
    };
  }, []);

  return { wsConnected, lastMessage };
}
