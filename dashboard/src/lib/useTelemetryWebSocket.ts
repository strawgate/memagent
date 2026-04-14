import { useEffect, useRef, useState } from "preact/hooks";
import type { WsEnvelope } from "../types";

/** Minimum delay between reconnect attempts (ms). */
const RECONNECT_BASE_MS = 1_000;
/** Maximum delay between reconnect attempts (ms). */
const RECONNECT_MAX_MS = 30_000;

/**
 * Connect to the diagnostics WebSocket at `/admin/v1/telemetry`.
 *
 * Returns `{ wsConnected, lastEnvelope }`:
 * - `wsConnected`: true when the socket is open
 * - `lastEnvelope`: most recent parsed envelope (null until first message)
 *
 * Automatically reconnects with exponential backoff on close or error.
 * The caller should fall back to HTTP polling when `wsConnected` is false.
 */
export function useTelemetryWebSocket(): {
  wsConnected: boolean;
  lastEnvelope: WsEnvelope | null;
} {
  const [wsConnected, setWsConnected] = useState(false);
  const [lastEnvelope, setLastEnvelope] = useState<WsEnvelope | null>(null);
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
          const data = JSON.parse(ev.data) as WsEnvelope;
          setLastEnvelope(data);
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

  return { wsConnected, lastEnvelope };
}
