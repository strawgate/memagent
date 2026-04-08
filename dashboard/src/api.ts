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
  stats: () => get<StatsResponse>("/admin/v1/stats"),
  config: () => get<ConfigResponse>("/admin/v1/config"),
  history: () => get<HistoryResponse>("/admin/v1/history"),
  traces: () => get<TracesResponse>("/admin/v1/traces"),
};
