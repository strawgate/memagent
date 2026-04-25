import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "../api";

// ─── helpers ──────────────────────────────────────────────────────────────────

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  mockFetch.mockReset();
  vi.unstubAllGlobals();
});

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
  } as Response;
}

// ─── tests ────────────────────────────────────────────────────────────────────

describe("api", () => {
  describe("successful requests", () => {
    it("status() fetches /admin/v1/status and returns parsed JSON", async () => {
      const body = { contract_version: "1", pipelines: [] };
      mockFetch.mockResolvedValue(jsonResponse(body));

      const result = await api.status();
      expect(result).toEqual(body);
      expect(mockFetch).toHaveBeenCalledWith("/admin/v1/status");
    });

    it("stats() fetches /admin/v1/stats", async () => {
      const body = { uptime_sec: 100, input_lines: 50 };
      mockFetch.mockResolvedValue(jsonResponse(body));

      const result = await api.stats();
      expect(result).toEqual(body);
      expect(mockFetch).toHaveBeenCalledWith("/admin/v1/stats");
    });

    it("config() fetches /admin/v1/config", async () => {
      const body = { path: "/etc/ffwd.yaml", raw_yaml: "inputs: []" };
      mockFetch.mockResolvedValue(jsonResponse(body));

      const result = await api.config();
      expect(result).toEqual({ data: body, errorMessage: null });
      expect(mockFetch).toHaveBeenCalledWith("/admin/v1/config");
    });

    it("history() fetches /admin/v1/history", async () => {
      const body = { lps: [[1000, 42]] };
      mockFetch.mockResolvedValue(jsonResponse(body));

      const result = await api.history();
      expect(result).toEqual(body);
      expect(mockFetch).toHaveBeenCalledWith("/admin/v1/history");
    });

    it("traces() fetches /admin/v1/traces", async () => {
      const body = { traces: [] };
      mockFetch.mockResolvedValue(jsonResponse(body));

      const result = await api.traces();
      expect(result).toEqual(body);
      expect(mockFetch).toHaveBeenCalledWith("/admin/v1/traces");
    });
  });

  describe("error handling", () => {
    it("returns null on non-ok HTTP status (404)", async () => {
      mockFetch.mockResolvedValue(jsonResponse({}, 404));
      const result = await api.status();
      expect(result).toBeNull();
    });

    it("returns null on non-ok HTTP status (500)", async () => {
      mockFetch.mockResolvedValue(jsonResponse({}, 500));
      const result = await api.stats();
      expect(result).toBeNull();
    });

    it("returns null on network error (fetch throws)", async () => {
      mockFetch.mockRejectedValue(new TypeError("Failed to fetch"));
      const result = await api.traces();
      expect(result).toBeNull();
    });

    it("returns null on JSON parse error", async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.reject(new SyntaxError("Unexpected token")),
      } as Response);

      // config uses getWithReason — returns { data: null, errorMessage }
      const result = await api.config();
      expect(result).toEqual({ data: null, errorMessage: "Network error" });
    });

    it("returns null on abort/timeout", async () => {
      mockFetch.mockRejectedValue(new DOMException("The operation was aborted", "AbortError"));
      const result = await api.history();
      expect(result).toBeNull();
    });

    it("config() returns server error message on 403", async () => {
      mockFetch.mockResolvedValue(
        jsonResponse(
          {
            error: "config_endpoint_disabled",
            message: "set FFWD_UNSAFE_EXPOSE_CONFIG=1 to enable /admin/v1/config",
          },
          403
        )
      );
      const result = await api.config();
      expect(result).toEqual({
        data: null,
        errorMessage: "set FFWD_UNSAFE_EXPOSE_CONFIG=1 to enable /admin/v1/config",
      });
    });
  });
});
