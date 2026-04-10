import {
  PIPELINE_METRIC_ORDER,
  SYSTEM_METRIC_ORDER,
  createMetricRegistry,
  orderedMetrics,
  pushMetricHistorySample,
  pushMetricSample,
} from "../lib/metricRegistry";
import type { MetricId } from "../types";

class FakeRing {
  public pushed: number[] = [];
  public pushedRaw: Array<{ ts: number; value: number }> = [];

  push(value: number): void {
    this.pushed.push(value);
  }

  pushRaw(ts: number, value: number): void {
    this.pushedRaw.push({ ts, value });
  }
}

function makeSeries(id: MetricId) {
  return {
    id,
    label: id,
    color: "#000",
    ring: new FakeRing(),
    value: "-",
    unit: "",
  };
}

const ALL_IDS: MetricId[] = [
  "lps",
  "bps",
  "obps",
  "err",
  "cpu",
  "mem",
  "lat",
  "inflight",
  "batches",
  "stalls",
];

describe("metric registry", () => {
  it("updates metrics by id even when declaration order changes", () => {
    const shuffled = [...ALL_IDS].reverse().map(makeSeries);
    const registry = createMetricRegistry(shuffled);

    pushMetricSample(registry, "obps", 42, "42 B/s");
    pushMetricHistorySample(registry, "lat", 10_000, 17);

    expect(registry.byId.obps.ring.pushed).toEqual([42]);
    expect(registry.byId.obps.value).toBe("42 B/s");
    expect(registry.byId.lat.ring.pushedRaw).toEqual([{ ts: 10_000, value: 17 }]);

    for (const id of ALL_IDS) {
      if (id === "obps") continue;
      expect(registry.byId[id].ring.pushed).toEqual([]);
    }
  });

  it("uses explicit render order independent of registry insertion order", () => {
    const shuffled = [...ALL_IDS].reverse().map(makeSeries);
    const registry = createMetricRegistry(shuffled);

    expect(orderedMetrics(registry, PIPELINE_METRIC_ORDER).map((m) => m.id)).toEqual(
      PIPELINE_METRIC_ORDER
    );
    expect(orderedMetrics(registry, SYSTEM_METRIC_ORDER).map((m) => m.id)).toEqual(
      SYSTEM_METRIC_ORDER
    );
  });

  it("throws if any metric id is missing", () => {
    const incomplete = ALL_IDS.filter((id) => id !== "stalls").map(makeSeries);
    expect(() => createMetricRegistry(incomplete)).toThrow("missing metric id: stalls");
  });
});
