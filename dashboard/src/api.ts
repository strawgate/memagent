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

/** Fetch JSON, returning { data, errorMessage } so callers can show why. */
async function getWithReason<T>(
  url: string
): Promise<{ data: T | null; errorMessage: string | null }> {
  try {
    const res = await fetch(url);
    if (!res.ok) {
      try {
        const body: unknown = await res.json();
        const msg =
          typeof body === "object" &&
          body !== null &&
          "message" in body &&
          typeof (body as { message?: unknown }).message === "string"
            ? (body as { message: string }).message
            : `HTTP ${res.status}`;
        return { data: null, errorMessage: msg };
      } catch {
        return { data: null, errorMessage: `HTTP ${res.status}` };
      }
    }
    return { data: (await res.json()) as T, errorMessage: null };
  } catch {
    return { data: null, errorMessage: "Network error" };
  }
}

export type HistoryResponse = Record<string, [number, number][]>;

export const api = {
  status: () => get<StatusResponse>("/admin/v1/status"),
  stats: () => get<StatsResponse>("/admin/v1/stats"),
  config: () => getWithReason<ConfigResponse>("/admin/v1/config"),
  history: () => get<HistoryResponse>("/admin/v1/history"),
  traces: () => get<TracesResponse>("/admin/v1/traces"),
};
