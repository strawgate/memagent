import { describe, expect, it } from "vitest";
import type { OtlpMetricsDocument, OtlpTracesDocument } from "@otlpkit/otlpjson";
import { extractMetricSnapshot, extractTraceRecords, extractUptimeSeconds } from "../lib/otlpProcess";

// ---------------------------------------------------------------------------
// Helpers to build OTLP JSON documents
// ---------------------------------------------------------------------------

function metricsDoc(
  metrics: Array<Record<string, unknown>>
): OtlpMetricsDocument {
  return {
    resourceMetrics: [
      {
        resource: { attributes: [] },
        scopeMetrics: [
          {
            scope: { name: "logfwd.diagnostics" },
            metrics,
          },
        ],
      },
    ],
  } as unknown as OtlpMetricsDocument;
}

function gauge(name: string, value: number, attrs?: Record<string, unknown>) {
  return {
    name,
    gauge: {
      dataPoints: [
        {
          timeUnixNano: "1000000000",
          asDouble: value,
          attributes: attrs
            ? Object.entries(attrs).map(([k, v]) => ({
                key: k,
                value: { stringValue: String(v) },
              }))
            : [],
        },
      ],
    },
  };
}

function sum(name: string, value: number, attrs?: Record<string, unknown>) {
  return {
    name,
    sum: {
      isMonotonic: true,
      dataPoints: [
        {
          timeUnixNano: "1000000000",
          asDouble: value,
          attributes: attrs
            ? Object.entries(attrs).map(([k, v]) => ({
                key: k,
                value: { stringValue: String(v) },
              }))
            : [],
        },
      ],
    },
  };
}

function tracesDoc(
  spans: Array<Record<string, unknown>>
): OtlpTracesDocument {
  return {
    resourceSpans: [
      {
        resource: { attributes: [] },
        scopeSpans: [
          {
            scope: { name: "logfwd.diagnostics" },
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
// extractMetricSnapshot
// ---------------------------------------------------------------------------

describe("extractMetricSnapshot", () => {
  it("extracts basic counters from OTLP metrics", () => {
    const doc = metricsDoc([
      sum("logfwd.input_lines", 100),
      sum("logfwd.input_bytes", 2048),
      sum("logfwd.output_bytes", 1024),
      sum("logfwd.output_errors", 3),
      sum("logfwd.batches", 10),
      gauge("logfwd.inflight_batches", 2),
      sum("logfwd.backpressure_stalls", 1),
      gauge("logfwd.uptime_seconds", 42.5),
    ]);

    const snap = extractMetricSnapshot(doc);
    expect(snap.inputLines).toBe(100);
    expect(snap.inputBytes).toBe(2048);
    expect(snap.outputBytes).toBe(1024);
    expect(snap.outputErrors).toBe(3);
    expect(snap.batches).toBe(10);
    expect(snap.inflightBatches).toBe(2);
    expect(snap.backpressureStalls).toBe(1);
    expect(snap.uptimeSeconds).toBe(42.5);
  });

  it("extracts stage nanos by attribute", () => {
    const doc = metricsDoc([
      sum("logfwd.stage_nanos", 500, { stage: "scan" }),
      sum("logfwd.stage_nanos", 300, { stage: "transform" }),
      sum("logfwd.stage_nanos", 200, { stage: "output" }),
    ]);

    const snap = extractMetricSnapshot(doc);
    expect(snap.scanNanos).toBe(500);
    expect(snap.transformNanos).toBe(300);
    expect(snap.outputNanos).toBe(200);
  });

  it("extracts process metrics", () => {
    const doc = metricsDoc([
      gauge("process.cpu.user_ms", 1500),
      gauge("process.cpu.sys_ms", 300),
      gauge("process.memory.rss", 50_000_000),
      gauge("process.memory.resident", 80_000_000),
      gauge("process.memory.allocated", 45_000_000),
      gauge("process.memory.active", 40_000_000),
    ]);

    const snap = extractMetricSnapshot(doc);
    expect(snap.cpuUserMs).toBe(1500);
    expect(snap.cpuSysMs).toBe(300);
    expect(snap.rssBytes).toBe(50_000_000);
    expect(snap.memResident).toBe(80_000_000);
    expect(snap.memAllocated).toBe(45_000_000);
    expect(snap.memActive).toBe(40_000_000);
  });

  it("returns null for missing process metrics", () => {
    const doc = metricsDoc([gauge("logfwd.uptime_seconds", 10)]);
    const snap = extractMetricSnapshot(doc);
    expect(snap.cpuUserMs).toBeNull();
    expect(snap.cpuSysMs).toBeNull();
    expect(snap.rssBytes).toBeNull();
    expect(snap.memResident).toBeNull();
  });

  it("handles empty metrics document", () => {
    const doc = metricsDoc([]);
    const snap = extractMetricSnapshot(doc);
    expect(snap.inputLines).toBe(0);
    expect(snap.uptimeSeconds).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// extractUptimeSeconds
// ---------------------------------------------------------------------------

describe("extractUptimeSeconds", () => {
  it("returns uptime from gauge", () => {
    const doc = metricsDoc([gauge("logfwd.uptime_seconds", 123.4)]);
    expect(extractUptimeSeconds(doc)).toBe(123.4);
  });

  it("returns 0 when no uptime metric", () => {
    const doc = metricsDoc([gauge("logfwd.input_lines", 5)]);
    expect(extractUptimeSeconds(doc)).toBe(0);
  });
});

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
        attributes: [
          { key: "pipeline", value: { stringValue: "main" } },
        ],
      }),
      childSpan("scan", {
        startTimeUnixNano: "1000000000",
        endTimeUnixNano: "1200000000",
        attributes: [
          { key: "rows", value: { intValue: "50" } },
        ],
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
        attributes: [
          { key: "pipeline", value: { stringValue: "p1" } },
        ],
      }),
      rootSpan({
        traceId: "aaaa0000000000000000000000000002",
        spanId: "0000000000000099",
        attributes: [
          { key: "pipeline", value: { stringValue: "p2" } },
        ],
      }),
    ]);

    const records = extractTraceRecords(doc);
    expect(records).toHaveLength(2);
    expect(records[0].pipeline).toBe("p1");
    expect(records[1].pipeline).toBe("p2");
  });
});
