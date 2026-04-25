import type { OtlpTracesDocument } from "@otlpkit/otlpjson";
import { describe, expect, it } from "vitest";
import { extractTraceRecords } from "../lib/otlpProcess";

// ---------------------------------------------------------------------------
// Helpers to build OTLP JSON documents
// ---------------------------------------------------------------------------

function tracesDoc(spans: Array<Record<string, unknown>>): OtlpTracesDocument {
  return {
    resourceSpans: [
      {
        resource: { attributes: [] },
        scopeSpans: [
          {
            scope: { name: "ffwd.diagnostics" },
            spans,
          },
        ],
      },
    ],
  } as unknown as OtlpTracesDocument;
}

function rootSpan(overrides: Record<string, unknown> = {}) {
  return {
    traceId: "aaaa0000bbbb1111cccc2222dddd3333",
    spanId: "0000000000000001",
    parentSpanId: "",
    name: "batch",
    startTimeUnixNano: "1000000000",
    endTimeUnixNano: "2000000000",
    status: { code: 0 },
    attributes: [],
    ...overrides,
  };
}

function childSpan(name: string, overrides: Record<string, unknown> = {}) {
  return {
    traceId: "aaaa0000bbbb1111cccc2222dddd3333",
    spanId: "0000000000000002",
    parentSpanId: "0000000000000001",
    name,
    startTimeUnixNano: "1100000000",
    endTimeUnixNano: "1500000000",
    status: { code: 0 },
    attributes: [],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// extractTraceRecords
// ---------------------------------------------------------------------------

describe("extractTraceRecords", () => {
  it("extracts a completed root span", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "input_rows", value: { intValue: "50" } },
          { key: "output_rows", value: { intValue: "48" } },
          { key: "bytes_in", value: { intValue: "1024" } },
          { key: "errors", value: { intValue: "0" } },
          { key: "flush_reason", value: { stringValue: "size" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records).toHaveLength(1);
    expect(records[0].trace_id).toBe("aaaa0000bbbb1111cccc2222dddd3333");
    expect(records[0].pipeline).toBe("main");
    expect(records[0].lifecycle_state).toBe("completed");
    expect(records[0].total_ns).toBe("1000000000");
    expect(records[0].flush_reason).toBe("size");
  });

  it("detects in-progress spans with stage", () => {
    const doc = tracesDoc([
      rootSpan({
        endTimeUnixNano: "0",
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "in_progress", value: { boolValue: true } },
          { key: "stage", value: { stringValue: "output" } },
          { key: "stage_start_unix_ns", value: { stringValue: "1200000000" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records).toHaveLength(1);
    expect(records[0].lifecycle_state).toBe("output_in_progress");
    expect(records[0].lifecycle_state_start_unix_ns).toBe("1200000000");
  });

  it("reconstructs child spans into root TraceRecord", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
      childSpan("scan", {
        startTimeUnixNano: "1000000000",
        endTimeUnixNano: "1200000000",
        attributes: [{ key: "rows", value: { intValue: "50" } }],
      }),
      childSpan("transform", {
        spanId: "0000000000000003",
        startTimeUnixNano: "1200000000",
        endTimeUnixNano: "1300000000",
      }),
      childSpan("output", {
        spanId: "0000000000000004",
        startTimeUnixNano: "1300000000",
        endTimeUnixNano: "1800000000",
        attributes: [
          { key: "worker_id", value: { intValue: "2" } },
          { key: "took_ms", value: { intValue: "45" } },
          { key: "req_bytes", value: { intValue: "512" } },
          { key: "cmp_bytes", value: { intValue: "256" } },
          { key: "resp_bytes", value: { intValue: "64" } },
          { key: "retries", value: { intValue: "1" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records).toHaveLength(1);

    const r = records[0];
    expect(r.scan_ns).toBe("200000000");
    expect(r.scan_rows).toBe(50);
    expect(r.transform_ns).toBe("100000000");
    expect(r.output_ns).toBe("500000000");
    expect(r.worker_id).toBe(2);
    expect(r.took_ms).toBe(45);
    expect(r.req_bytes).toBe(512);
    expect(r.cmp_bytes).toBe(256);
    expect(r.resp_bytes).toBe(64);
    expect(r.retries).toBe(1);
  });

  it("falls back to root attributes when no child spans", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "scan_ns", value: { intValue: "150000000" } },
          { key: "transform_ns", value: { intValue: "50000000" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records[0].scan_ns).toBe("150000000");
    expect(records[0].transform_ns).toBe("50000000");
  });

  it("handles error status", () => {
    const doc = tracesDoc([
      rootSpan({
        status: { code: 2, message: "output failed" },
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "errors", value: { intValue: "1" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records[0].status).toBe("error");
    expect(records[0].errors).toBe(1);
  });

  it("handles empty traces document", () => {
    const doc = tracesDoc([]);
    expect(extractTraceRecords(doc)).toEqual([]);
  });

  it("handles multiple root spans", () => {
    const doc = tracesDoc([
      rootSpan({
        traceId: "aaaa0000000000000000000000000001",
        attributes: [{ key: "pipeline", value: { stringValue: "p1" } }],
      }),
      rootSpan({
        traceId: "aaaa0000000000000000000000000002",
        spanId: "0000000000000099",
        attributes: [{ key: "pipeline", value: { stringValue: "p2" } }],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records).toHaveLength(2);
    expect(records[0].pipeline).toBe("p1");
    expect(records[1].pipeline).toBe("p2");
  });

  // ── Root-attribute fallback for output/worker ─────────────────────────

  it("falls back to root worker_id when no output child span", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "in_progress", value: { boolValue: true } },
          { key: "stage", value: { stringValue: "scan" } },
          { key: "worker_id", value: { intValue: "7" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records).toHaveLength(1);
    expect(records[0].worker_id).toBe(7);
  });

  it("falls back to root output_ns when no output child span", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "output_ns", value: { intValue: "999000" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records[0].output_ns).toBe("999000");
  });

  it("prefers child output span over root attributes", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "worker_id", value: { intValue: "1" } },
          { key: "output_ns", value: { intValue: "100" } },
        ],
      }),
      childSpan("output", {
        spanId: "0000000000000004",
        startTimeUnixNano: "1300000000",
        endTimeUnixNano: "1800000000",
        attributes: [{ key: "worker_id", value: { intValue: "5" } }],
      }),
    ]);

    const records = extractTraceRecords(doc);
    // Child output span takes priority
    expect(records[0].worker_id).toBe(5);
    expect(records[0].output_ns).toBe("500000000");
  });

  // ── Lifecycle state derivation ────────────────────────────────────────

  it("derives scan_in_progress for stage=scan", () => {
    const doc = tracesDoc([
      rootSpan({
        endTimeUnixNano: "0",
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "in_progress", value: { boolValue: true } },
          { key: "stage", value: { stringValue: "scan" } },
        ],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].lifecycle_state).toBe("scan_in_progress");
  });

  it("derives transform_in_progress for stage=transform", () => {
    const doc = tracesDoc([
      rootSpan({
        endTimeUnixNano: "0",
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "in_progress", value: { boolValue: true } },
          { key: "stage", value: { stringValue: "transform" } },
        ],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].lifecycle_state).toBe("transform_in_progress");
  });

  it("derives queued_for_output for stage=queued", () => {
    const doc = tracesDoc([
      rootSpan({
        endTimeUnixNano: "0",
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "in_progress", value: { boolValue: true } },
          { key: "stage", value: { stringValue: "queued" } },
        ],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].lifecycle_state).toBe("queued_for_output");
  });

  it("derives queued_for_output for unknown/missing stage", () => {
    const doc = tracesDoc([
      rootSpan({
        endTimeUnixNano: "0",
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "in_progress", value: { boolValue: true } },
          // no stage attribute
        ],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].lifecycle_state).toBe("queued_for_output");
  });

  // ── Status code mapping ───────────────────────────────────────────────

  it("maps status code 1 to 'ok'", () => {
    const doc = tracesDoc([
      rootSpan({
        status: { code: 1 },
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].status).toBe("ok");
  });

  it("maps status code 0 to 'unset'", () => {
    const doc = tracesDoc([
      rootSpan({
        status: { code: 0 },
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].status).toBe("unset");
  });

  // ── Default values for missing fields ─────────────────────────────────

  it("defaults numeric fields to 0 when missing", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
    ]);
    const r = extractTraceRecords(doc)[0];
    expect(r.input_rows).toBe(0);
    expect(r.output_rows).toBe(0);
    expect(r.bytes_in).toBe(0);
    expect(r.errors).toBe(0);
    expect(r.scan_rows).toBe(0);
  });

  it("defaults worker_id to -1 when not set anywhere", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
    ]);
    // No worker_id on root or child — uses null ?? null → null, then ?? -1
    // Actually: toNumber(root.attributes.worker_id) where worker_id is missing → null
    // Then: workerId ?? -1 → -1
    expect(extractTraceRecords(doc)[0].worker_id).toBe(-1);
  });

  it("defaults pipeline to empty string when missing", () => {
    const doc = tracesDoc([rootSpan({ attributes: [] })]);
    expect(extractTraceRecords(doc)[0].pipeline).toBe("");
  });

  // ── Duration calculation ──────────────────────────────────────────────

  it("computes duration as 0 when end <= start", () => {
    const doc = tracesDoc([
      rootSpan({
        startTimeUnixNano: "5000000000",
        endTimeUnixNano: "5000000000", // same
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].total_ns).toBe("0");
  });

  it("computes child span duration as 0 when end <= start", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
      childSpan("scan", {
        startTimeUnixNano: "1500000000",
        endTimeUnixNano: "1200000000", // end before start
        attributes: [{ key: "rows", value: { intValue: "10" } }],
      }),
    ]);
    expect(extractTraceRecords(doc)[0].scan_ns).toBe("0");
  });

  // ── Optional output span fields ───────────────────────────────────────

  it("omits optional output fields when they are zero", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
      childSpan("output", {
        spanId: "0000000000000004",
        startTimeUnixNano: "1300000000",
        endTimeUnixNano: "1800000000",
        attributes: [
          { key: "worker_id", value: { intValue: "2" } },
          // All other output fields default to 0
        ],
      }),
    ]);

    const r = extractTraceRecords(doc)[0];
    expect(r.send_ns).toBeUndefined();
    expect(r.recv_ns).toBeUndefined();
    expect(r.took_ms).toBeUndefined();
    expect(r.retries).toBeUndefined();
    expect(r.req_bytes).toBeUndefined();
    expect(r.cmp_bytes).toBeUndefined();
    expect(r.resp_bytes).toBeUndefined();
  });

  // ── Multiple resource/scope spans ─────────────────────────────────────

  it("handles resourceSpans with no scopeSpans", () => {
    const doc: OtlpTracesDocument = {
      resourceSpans: [
        {
          resource: { attributes: [] },
          scopeSpans: [],
        },
      ],
    } as unknown as OtlpTracesDocument;
    expect(extractTraceRecords(doc)).toEqual([]);
  });

  it("handles missing resourceSpans gracefully", () => {
    const doc = { resourceSpans: [] } as unknown as OtlpTracesDocument;
    expect(extractTraceRecords(doc)).toEqual([]);
  });

  // ── output_start_unix_ns ──────────────────────────────────────────────

  it("sets output_start_unix_ns from output child span", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
      childSpan("output", {
        spanId: "0000000000000004",
        startTimeUnixNano: "1300000000",
        endTimeUnixNano: "1800000000",
        attributes: [{ key: "worker_id", value: { intValue: "0" } }],
      }),
    ]);

    expect(extractTraceRecords(doc)[0].output_start_unix_ns).toBe("1300000000");
  });

  it("omits output_start_unix_ns when no output child and no root attr", () => {
    const doc = tracesDoc([
      rootSpan({
        attributes: [{ key: "pipeline", value: { stringValue: "main" } }],
      }),
    ]);

    expect(extractTraceRecords(doc)[0].output_start_unix_ns).toBeUndefined();
  });

  it("falls back to root output_start_unix_ns when no output child span", () => {
    const doc = tracesDoc([
      rootSpan({
        endTimeUnixNano: "0",
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
          { key: "in_progress", value: { boolValue: true } },
          { key: "stage", value: { stringValue: "output" } },
          { key: "output_start_unix_ns", value: { stringValue: "1400000000" } },
        ],
      }),
    ]);

    expect(extractTraceRecords(doc)[0].output_start_unix_ns).toBe("1400000000");
  });
});
