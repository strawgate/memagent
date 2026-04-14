import { describe, expect, it } from "vitest";
import { mergeTraces } from "../lib/mergeTraces";
import type { TraceRecord } from "../types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal TraceRecord for testing. */
function trace(
  id: string,
  state: TraceRecord["lifecycle_state"],
  startNs = "1000000000"
): TraceRecord {
  return {
    trace_id: id,
    pipeline: "default",
    start_unix_ns: startNs,
    total_ns: "1000000",
    scan_ns: "100000",
    transform_ns: "200000",
    output_ns: "300000",
    scan_rows: 10,
    input_rows: 10,
    output_rows: 10,
    bytes_in: 1024,
    queue_wait_ns: "50000",
    worker_id: 0,
    flush_reason: "size",
    errors: 0,
    status: "ok",
    lifecycle_state: state,
  };
}

const completed = (id: string, startNs?: string) => trace(id, "completed", startNs);
const scanning = (id: string, startNs?: string) => trace(id, "scan_in_progress", startNs);
const outputting = (id: string, startNs?: string) => trace(id, "output_in_progress", startNs);
const queued = (id: string, startNs?: string) => trace(id, "queued_for_output", startNs);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("mergeTraces", () => {
  // ── Basic merge semantics ──────────────────────────────────────────────

  describe("basic merge", () => {
    it("returns incoming when prev is empty", () => {
      const result = mergeTraces([], [completed("a"), scanning("b")], 100);
      expect(result).toHaveLength(2);
      expect(result.map((t) => t.trace_id)).toEqual(["a", "b"]);
    });

    it("keeps completed from prev when incoming is empty", () => {
      const result = mergeTraces([completed("a"), completed("b")], [], 100);
      expect(result).toHaveLength(2);
    });

    it("returns empty when both are empty", () => {
      expect(mergeTraces([], [], 100)).toEqual([]);
    });
  });

  // ── Completed trace handling ───────────────────────────────────────────

  describe("completed traces", () => {
    it("retains all completed from prev", () => {
      const prev = [completed("a"), completed("b")];
      const result = mergeTraces(prev, [], 100);
      expect(result.map((t) => t.trace_id)).toEqual(["a", "b"]);
    });

    it("adds new completed from incoming", () => {
      const prev = [completed("a")];
      const incoming = [completed("b")];
      const result = mergeTraces(prev, incoming, 100);
      expect(result.map((t) => t.trace_id)).toEqual(["a", "b"]);
    });

    it("deduplicates completed by trace_id (prev wins)", () => {
      const prevA = completed("a");
      prevA.errors = 0;
      const incomingA = completed("a");
      incomingA.errors = 5; // different value

      const result = mergeTraces([prevA], [incomingA], 100);
      expect(result).toHaveLength(1);
      expect(result[0].errors).toBe(0); // prev version kept
    });

    it("does not downgrade completed to in-progress", () => {
      const prev = [completed("a")];
      const incoming = [scanning("a")]; // same trace, but in-progress
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(1);
      expect(result[0].lifecycle_state).toBe("completed");
    });
  });

  // ── In-progress trace handling ─────────────────────────────────────────

  describe("in-progress traces", () => {
    it("replaces stale in-progress with fresher incoming version", () => {
      const prev = [scanning("a")];
      const incoming = [outputting("a")]; // progressed to output
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(1);
      expect(result[0].lifecycle_state).toBe("output_in_progress");
    });

    it("replaces in-progress with completed version", () => {
      const prev = [outputting("a")];
      const incoming = [completed("a")];
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(1);
      expect(result[0].lifecycle_state).toBe("completed");
    });

    it("drops ALL in-progress from prev (server re-sends active each tick)", () => {
      // This is the key design: synthetic in-progress IDs differ from real
      // completed IDs, so keeping stale entries would create ghost traces.
      const prev = [scanning("a"), outputting("b")];
      const incoming = [outputting("a")]; // only 'a' updated, 'b' gone
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(1);
      expect(result[0].trace_id).toBe("a");
      expect(result[0].lifecycle_state).toBe("output_in_progress");
    });

    it("drops stale in-progress when incoming has same trace_id", () => {
      const prev = [scanning("a")];
      prev[0].scan_ns = "old";
      const incoming = [scanning("a")];
      incoming[0].scan_ns = "new";
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(1);
      expect(result[0].scan_ns).toBe("new");
    });
  });

  // ── Mixed scenarios ────────────────────────────────────────────────────

  describe("mixed completed + in-progress", () => {
    it("handles typical server tick (completed + active batches)", () => {
      const prev = [completed("old1"), completed("old2"), scanning("active1")];
      const incoming = [
        completed("new1"), // new completed span
        outputting("active1"), // active1 progressed
        scanning("active2"), // new active batch
      ];
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(5);

      const ids = result.map((t) => t.trace_id);
      expect(ids).toContain("old1");
      expect(ids).toContain("old2");
      expect(ids).toContain("new1");
      expect(ids).toContain("active1");
      expect(ids).toContain("active2");

      // active1 should be the updated version
      expect(result.find((t) => t.trace_id === "active1")?.lifecycle_state).toBe(
        "output_in_progress"
      );
    });

    it("handles batch completing between ticks", () => {
      const prev = [scanning("a")]; // was in-progress
      const incoming = [completed("a")]; // now completed
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(1);
      expect(result[0].lifecycle_state).toBe("completed");
    });

    it("handles batch completing with different trace_id (synthetic vs real)", () => {
      // In-progress uses synthetic ID, completed uses real OTLP trace ID.
      // The in-progress entry must NOT persist as a ghost.
      const prev = [scanning("synthetic-001")];
      const incoming = [completed("real-otlp-abc")]; // different ID, same logical batch
      const result = mergeTraces(prev, incoming, 100);
      // Only the completed trace survives — synthetic in-progress is dropped
      expect(result).toHaveLength(1);
      expect(result[0].trace_id).toBe("real-otlp-abc");
      expect(result[0].lifecycle_state).toBe("completed");
    });

    it("handles all batches completing (no more active)", () => {
      const prev = [completed("old"), scanning("a"), outputting("b")];
      const incoming = [completed("a"), completed("b")]; // both finished
      const result = mergeTraces(prev, incoming, 100);
      expect(result).toHaveLength(3);
      expect(result.every((t) => t.lifecycle_state === "completed")).toBe(true);
    });
  });

  // ── MAX_TRACES eviction ────────────────────────────────────────────────

  describe("eviction (maxTraces)", () => {
    it("trims to maxTraces when exceeded", () => {
      const prev = Array.from({ length: 5 }, (_, i) => completed(`c${i}`, String(1000 + i)));
      const incoming = Array.from({ length: 5 }, (_, i) => completed(`n${i}`, String(2000 + i)));
      const result = mergeTraces(prev, incoming, 8);
      expect(result).toHaveLength(8);
    });

    it("preserves in-progress traces over old completed during eviction", () => {
      // In-progress should sort before (higher priority) completed
      const prev = Array.from({ length: 10 }, (_, i) => completed(`c${i}`, String(1000 + i)));
      const incoming = [scanning("active", "5000")];
      const result = mergeTraces(prev, incoming, 5);
      expect(result).toHaveLength(5);
      // The active trace should survive eviction
      expect(result.some((t) => t.trace_id === "active")).toBe(true);
    });

    it("among completed traces, keeps newest (highest start_unix_ns)", () => {
      const prev = [completed("old", "1000"), completed("mid", "5000"), completed("new", "9000")];
      const result = mergeTraces(prev, [], 2);
      expect(result).toHaveLength(2);
      const ids = result.map((t) => t.trace_id);
      expect(ids).toContain("new");
      expect(ids).toContain("mid");
      expect(ids).not.toContain("old");
    });

    it("handles BigInt sort correctly for large nanosecond timestamps", () => {
      const prev = [
        completed("a", "1776000000000000000"), // realistic unix ns
        completed("b", "1776000001000000000"), // 1 second later
        completed("c", "1776000002000000000"), // 2 seconds later
      ];
      const result = mergeTraces(prev, [], 2);
      expect(result).toHaveLength(2);
      // Should keep the two newest
      const ids = result.map((t) => t.trace_id);
      expect(ids).toContain("c");
      expect(ids).toContain("b");
    });
  });

  // ── Edge cases ─────────────────────────────────────────────────────────

  describe("edge cases", () => {
    it("handles same trace_id as both completed (prev) and in-progress (incoming)", () => {
      // Shouldn't happen normally, but be defensive
      const prev = [completed("a")];
      const incoming = [scanning("a")];
      const result = mergeTraces(prev, incoming, 100);
      // Completed in prev takes priority (seen set blocks in-progress)
      expect(result).toHaveLength(1);
      expect(result[0].lifecycle_state).toBe("completed");
    });

    it("handles duplicate trace_ids in incoming", () => {
      const incoming = [completed("a"), completed("a")];
      const result = mergeTraces([], incoming, 100);
      // Second dupe should be filtered by seen set
      expect(result).toHaveLength(1);
    });

    it("drops all in-progress from prev when incoming is empty", () => {
      const prev = [
        scanning("a"),
        trace("b", "transform_in_progress"),
        queued("c"),
        outputting("d"),
        completed("e"),
      ];
      const result = mergeTraces(prev, [], 100);
      // Only completed survives — all in-progress dropped (server didn't re-send them)
      expect(result).toHaveLength(1);
      expect(result[0].trace_id).toBe("e");
    });

    it("maxTraces = 0 returns empty", () => {
      const result = mergeTraces([completed("a")], [completed("b")], 0);
      expect(result).toHaveLength(0);
    });

    it("maxTraces = 1 keeps the most important trace", () => {
      const prev = [completed("old", "1000")];
      const incoming = [scanning("active", "2000")];
      const result = mergeTraces(prev, incoming, 1);
      expect(result).toHaveLength(1);
      // In-progress sorts first (higher priority)
      expect(result[0].trace_id).toBe("active");
    });
  });

  // ── Simulated real-world sequences ─────────────────────────────────────

  describe("multi-tick simulation", () => {
    it("simulates 3 server ticks with batch lifecycle", () => {
      // Tick 1: batch starts
      let state = mergeTraces([], [scanning("batch1")], 100);
      expect(state).toHaveLength(1);
      expect(state[0].lifecycle_state).toBe("scan_in_progress");

      // Tick 2: batch progresses to output, new batch starts
      state = mergeTraces(state, [outputting("batch1"), scanning("batch2")], 100);
      expect(state).toHaveLength(2);
      expect(state.find((t) => t.trace_id === "batch1")?.lifecycle_state).toBe(
        "output_in_progress"
      );
      expect(state.find((t) => t.trace_id === "batch2")?.lifecycle_state).toBe("scan_in_progress");

      // Tick 3: batch1 completes, batch2 progresses
      state = mergeTraces(state, [completed("batch1"), outputting("batch2")], 100);
      expect(state).toHaveLength(2);
      expect(state.find((t) => t.trace_id === "batch1")?.lifecycle_state).toBe("completed");
      expect(state.find((t) => t.trace_id === "batch2")?.lifecycle_state).toBe(
        "output_in_progress"
      );
    });

    it("simulates WS message drop (in-progress lost, recovers next tick)", () => {
      // Tick 1: two active batches
      let state = mergeTraces([], [scanning("a"), scanning("b")], 100);
      expect(state).toHaveLength(2);

      // Tick 2: WS only delivers 'a' update (b dropped from server = completed/gone)
      state = mergeTraces(state, [outputting("a")], 100);
      expect(state).toHaveLength(1);
      expect(state[0].trace_id).toBe("a");
      expect(state[0].lifecycle_state).toBe("output_in_progress");

      // Tick 3: if 'b' is still active, server resends it
      state = mergeTraces(state, [completed("a"), scanning("b")], 100);
      expect(state).toHaveLength(2);
      expect(state.find((t) => t.trace_id === "a")?.lifecycle_state).toBe("completed");
      expect(state.find((t) => t.trace_id === "b")?.lifecycle_state).toBe("scan_in_progress");
    });

    it("completed traces accumulate across ticks", () => {
      let state: TraceRecord[] = [];
      for (let i = 0; i < 10; i++) {
        state = mergeTraces(state, [completed(`batch${i}`, String(1000 + i))], 100);
      }
      expect(state).toHaveLength(10);
      expect(state.every((t) => t.lifecycle_state === "completed")).toBe(true);
    });
  });
});
