import { createTelemetryStore, type TelemetryStore } from "@otlpkit/views";
import { useEffect, useRef, useState } from "preact/hooks";
import type { OtlpMessage } from "./useTelemetryWebSocket";

/** 5 minutes of data at 2 s intervals ≈ 150 samples per metric. */
const MAX_AGE_MS = 5 * 60 * 1000;

/**
 * Wraps `@otlpkit/views` `TelemetryStore` in a Preact-friendly hook.
 *
 * Call `ingest()` with each OTLP message from the WebSocket.
 * The store accumulates metric/trace/log records and provides selectors
 * (`selectTimeSeries`, `selectLatestValues`, etc.) for chart rendering.
 *
 * Returns:
 * - `store`: stable TelemetryStore reference (never changes)
 * - `tick`: counter that increments on each ingest (use as render dependency)
 */
export function useTelemetryStore(lastMessage: OtlpMessage | null): {
  store: TelemetryStore;
  tick: number;
} {
  const storeRef = useRef<TelemetryStore | null>(null);
  if (!storeRef.current) {
    storeRef.current = createTelemetryStore({ maxAgeMs: MAX_AGE_MS });
  }
  const store = storeRef.current;
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (!lastMessage) return;
    store.ingest(lastMessage.data);
    setTick((n) => n + 1);
  }, [lastMessage, store]);

  return { store, tick };
}
