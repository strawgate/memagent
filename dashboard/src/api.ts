import type { PipelinesResponse, StatsResponse, ConfigResponse, TracesResponse } from "./types";

async function get<T>(url: string): Promise<T | null> {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

export type HistoryResponse = Record<string, [number, number][]>;

export const api = {
  pipelines: () => get<PipelinesResponse>("/api/pipelines"),
  stats: () => get<StatsResponse>("/api/stats"),
  config: () => get<ConfigResponse>("/api/config"),
  history: () => get<HistoryResponse>("/api/history"),
  traces: () => get<TracesResponse>("/api/traces"),
};
