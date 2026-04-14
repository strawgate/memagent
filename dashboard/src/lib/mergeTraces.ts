/**
 * Pure merge logic for delta trace delivery.
 *
 * Extracted from app.tsx so it can be unit-tested without Preact hooks.
 *
 * The server sends: (a) only NEW completed spans since last cursor, plus
 * (b) ALL currently in-progress batches every tick. So we:
 *   1. Keep all completed traces from prev (they're immutable)
 *   2. Drop old in-progress from prev IF incoming has a fresher version
 *   3. Merge new completed from incoming (dedup by trace_id)
 *   4. Merge in-progress from incoming (always latest state)
 */

import type { TraceRecord } from "../types";

export function mergeTraces(
  prev: TraceRecord[],
  incoming: TraceRecord[],
  maxTraces: number
): TraceRecord[] {
  const incomingCompleted: TraceRecord[] = [];
  const incomingInProgress = new Map<string, TraceRecord>();
  for (const t of incoming) {
    if (t.lifecycle_state === "completed") {
      incomingCompleted.push(t);
    } else {
      incomingInProgress.set(t.trace_id, t);
    }
  }

  // Build the set of all trace_ids present in this incoming update.
  const incomingIds = new Set<string>([
    ...incomingCompleted.map((t) => t.trace_id),
    ...incomingInProgress.keys(),
  ]);

  const merged: TraceRecord[] = [];
  for (const t of prev) {
    if (t.lifecycle_state === "completed") {
      // Always keep completed traces (immutable).
      merged.push(t);
    } else if (!incomingIds.has(t.trace_id)) {
      // Defensive: keep in-progress traces not superseded by incoming.
      merged.push(t);
    }
    // Otherwise drop stale in-progress — incoming has a fresher version.
  }

  const seen = new Set(merged.map((t) => t.trace_id));
  for (const t of incomingCompleted) {
    if (!seen.has(t.trace_id)) {
      merged.push(t);
      seen.add(t.trace_id);
    }
  }

  for (const t of incomingInProgress.values()) {
    if (!seen.has(t.trace_id)) {
      merged.push(t);
    }
  }

  if (merged.length > maxTraces) {
    merged.sort((a, b) => {
      const aIp = a.lifecycle_state !== "completed" ? 1 : 0;
      const bIp = b.lifecycle_state !== "completed" ? 1 : 0;
      if (aIp !== bIp) return bIp - aIp;
      const diff = BigInt(b.start_unix_ns) - BigInt(a.start_unix_ns);
      return diff > 0n ? 1 : diff < 0n ? -1 : 0;
    });
    merged.length = maxTraces;
  }

  return merged;
}
