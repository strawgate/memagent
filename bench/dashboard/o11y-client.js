import {
  fetchIndex,
  fetchRun,
  rawUrl,
} from "https://cdn.jsdelivr.net/npm/@benchkit/chart@0.2.2/dist/fetch.js/+esm";
import { trendChartDataset } from "https://cdn.jsdelivr.net/npm/@benchkit/adapters@0.2.2/dist/chartjs.js/+esm";

export const benchDataSource = {
  owner: "strawgate",
  repo: "memagent",
  branch: "bench-data",
};

function withDataPrefix(filePath) {
  const cleaned = String(filePath || "").replace(/^\/+/, "");
  return cleaned.startsWith("data/") ? cleaned : `data/${cleaned}`;
}

export function benchDataUrl(filePath) {
  return rawUrl(benchDataSource, withDataPrefix(filePath));
}

async function fetchJsonAtPath(filePath, signal) {
  const url = rawUrl(benchDataSource, filePath);
  try {
    const res = await fetch(url, { signal });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function fetchBenchJson(filePath, signal) {
  return fetchJsonAtPath(withDataPrefix(filePath), signal);
}

export async function fetchBenchIndex(signal) {
  try {
    return await fetchIndex(benchDataSource, signal);
  } catch {
    return fetchJsonAtPath("data/index.json", signal);
  }
}

export async function fetchBenchRun(runId, signal) {
  try {
    return await fetchRun(benchDataSource, runId, signal);
  } catch {
    return fetchJsonAtPath(`data/runs/${runId}.json`, signal);
  }
}

export function toTrendDataset(metricName, points, options = {}) {
  const entry = {
    tags: {},
    points: points
      .filter((p) => p && p.timestamp && Number.isFinite(p.value))
      .map((p) => ({ timestamp: p.timestamp, value: p.value })),
  };
  return trendChartDataset(metricName, entry, options);
}

export function shortId(value) {
  const s = String(value ?? "");
  return s.length > 8 ? `#${s.slice(-8)}` : `#${s}`;
}
