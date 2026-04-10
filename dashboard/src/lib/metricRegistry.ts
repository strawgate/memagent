import type { MetricId } from "../types";

export const METRIC_IDS = [
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
] as const;

export const PIPELINE_METRIC_ORDER: readonly MetricId[] = [
  "lps",
  "bps",
  "obps",
  "err",
  "lat",
  "inflight",
  "batches",
  "stalls",
];

export const SYSTEM_METRIC_ORDER: readonly MetricId[] = ["cpu", "mem"];

interface MetricRing {
  push(value: number): void;
  pushRaw(timestampMs: number, value: number): void;
}

export interface MetricSeriesRecord {
  id: MetricId;
  ring: MetricRing;
  value: string;
  limit?: string;
}

export interface MetricRegistry<TSeries extends MetricSeriesRecord> {
  ordered: TSeries[];
  byId: Record<MetricId, TSeries>;
}

export function createMetricRegistry<TSeries extends MetricSeriesRecord>(
  series: TSeries[]
): MetricRegistry<TSeries> {
  const byId = {} as Record<MetricId, TSeries>;

  for (const metric of series) {
    if (byId[metric.id]) {
      throw new Error(`duplicate metric id: ${metric.id}`);
    }
    byId[metric.id] = metric;
  }

  for (const id of METRIC_IDS) {
    if (!byId[id]) {
      throw new Error(`missing metric id: ${id}`);
    }
  }

  return { ordered: series, byId };
}

export function orderedMetrics<TSeries extends MetricSeriesRecord>(
  registry: MetricRegistry<TSeries>,
  order: readonly MetricId[]
): TSeries[] {
  return order.map((id) => registry.byId[id]);
}

export function pushMetricSample<TSeries extends MetricSeriesRecord>(
  registry: MetricRegistry<TSeries>,
  id: MetricId,
  value: number,
  formatted: string,
  limit?: string
): void {
  const metric = registry.byId[id];
  metric.ring.push(value);
  metric.value = formatted;
  if (limit !== undefined) {
    metric.limit = limit;
  }
}

export function pushMetricHistorySample<TSeries extends MetricSeriesRecord>(
  registry: MetricRegistry<TSeries>,
  id: MetricId,
  timestampMs: number,
  value: number
): void {
  registry.byId[id].ring.pushRaw(timestampMs, value);
}
