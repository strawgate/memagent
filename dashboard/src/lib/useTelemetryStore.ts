import type { OtlpDocument } from "@otlpkit/otlpjson";
import { createTelemetryStore, type TelemetryStore } from "@otlpkit/views";
import { useRef, useState } from "preact/hooks";

/** 5 minutes of data at 2 s intervals ≈ 150 samples per metric. */
const MAX_AGE_MS = 5 * 60 * 1000;

/**
 * Wraps `@otlpkit/views` `TelemetryStore` in a Preact-friendly hook.
 *
 * Returns:
 * - `store`: stable TelemetryStore reference (never changes)
 * - `tick`: counter that increments on each ingest (use as render dependency)
 * - `ingest`: callback to feed an OTLP document into the store
 */
export function useTelemetryStore(): {
  store: TelemetryStore;
  tick: number;
  ingest: (doc: OtlpDocument) => void;
} {
  const storeRef = useRef<TelemetryStore | null>(null);
  if (!storeRef.current) {
    storeRef.current = createTelemetryStore({ maxAgeMs: MAX_AGE_MS });
  }
  const store = storeRef.current;
  const [tick, setTick] = useState(0);

  const ingestRef = useRef((doc: OtlpDocument) => {
    store.ingest(doc);
    setTick((n) => n + 1);
  });

  return { store, tick, ingest: ingestRef.current };
}
