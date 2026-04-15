import type { ConfigResponse, StatsResponse, StatusResponse, TracesResponse } from "./types";

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
  status: () => get<StatusResponse>("/admin/v1/status"),
  stats: () => get<StatsResponse>("/api/stats"),
  config: () => get<ConfigResponse>("/api/config"),
  history: () => get<HistoryResponse>("/api/history"),
  traces: () => get<TracesResponse>("/api/traces"),
};
