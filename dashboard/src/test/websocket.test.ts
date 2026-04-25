import { act, renderHook } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useTelemetryWebSocket } from "../lib/useTelemetryWebSocket";

// ─── mock WebSocket ──────────────────────────────────────────────────────────

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  url: string;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readyState = 0; // CONNECTING
  closed = false;

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }

  close() {
    this.closed = true;
    this.readyState = 3;
  }

  // Test helpers
  simulateOpen() {
    this.readyState = 1; // OPEN
    this.onopen?.();
  }

  simulateMessage(data: string) {
    this.onmessage?.({ data });
  }

  simulateClose() {
    this.readyState = 3;
    this.onclose?.();
  }
}

// ─── setup ───────────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.useFakeTimers();
  MockWebSocket.instances = [];
  vi.stubGlobal("WebSocket", MockWebSocket);
  vi.stubGlobal("location", { protocol: "http:", host: "localhost:9090" });
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  MockWebSocket.instances = [];
});

// ─── tests ───────────────────────────────────────────────────────────────────

describe("useTelemetryWebSocket", () => {
  it("starts disconnected", () => {
    const handler = vi.fn();
    const { result } = renderHook(() => useTelemetryWebSocket(handler));
    expect(result.current.wsConnected).toBe(false);
  });

  it("connects to ws://host/admin/v1/telemetry", () => {
    renderHook(() => useTelemetryWebSocket(vi.fn()));
    expect(MockWebSocket.instances).toHaveLength(1);
    expect(MockWebSocket.instances[0].url).toBe("ws://localhost:9090/admin/v1/telemetry");
  });

  it("sets wsConnected=true on open", () => {
    const { result } = renderHook(() => useTelemetryWebSocket(vi.fn()));
    act(() => {
      MockWebSocket.instances[0].simulateOpen();
    });
    expect(result.current.wsConnected).toBe(true);
  });

  it("calls onMessage with parsed OTLP metrics", () => {
    const handler = vi.fn();
    renderHook(() => useTelemetryWebSocket(handler));
    const ws = MockWebSocket.instances[0];

    act(() => ws.simulateOpen());

    const metricsDoc = {
      resourceMetrics: [
        {
          resource: { attributes: [] },
          scopeMetrics: [
            {
              scope: { name: "ffwd.diagnostics" },
              metrics: [
                {
                  name: "ffwd.uptime_seconds",
                  gauge: {
                    dataPoints: [{ timeUnixNano: "1000", asDouble: 42.5 }],
                  },
                },
              ],
            },
          ],
        },
      ],
    };

    act(() => ws.simulateMessage(JSON.stringify(metricsDoc)));
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0].signal).toBe("metrics");
  });

  it("calls onMessage with parsed OTLP traces", () => {
    const handler = vi.fn();
    renderHook(() => useTelemetryWebSocket(handler));
    const ws = MockWebSocket.instances[0];

    act(() => ws.simulateOpen());

    const tracesDoc = {
      resourceSpans: [
        {
          resource: { attributes: [] },
          scopeSpans: [
            {
              scope: { name: "ffwd.diagnostics" },
              spans: [],
            },
          ],
        },
      ],
    };

    act(() => ws.simulateMessage(JSON.stringify(tracesDoc)));
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0].signal).toBe("traces");
  });

  it("ignores malformed messages", () => {
    const handler = vi.fn();
    renderHook(() => useTelemetryWebSocket(handler));
    const ws = MockWebSocket.instances[0];

    act(() => ws.simulateOpen());
    act(() => ws.simulateMessage("not json"));
    expect(handler).not.toHaveBeenCalled();
  });

  it("sets wsConnected=false on close and schedules reconnect", () => {
    const { result } = renderHook(() => useTelemetryWebSocket(vi.fn()));
    const ws = MockWebSocket.instances[0];

    act(() => ws.simulateOpen());
    expect(result.current.wsConnected).toBe(true);

    act(() => ws.simulateClose());
    expect(result.current.wsConnected).toBe(false);

    // Advance past the 1s reconnect delay.
    act(() => vi.advanceTimersByTime(1100));
    expect(MockWebSocket.instances).toHaveLength(2);
  });

  it("uses exponential backoff on repeated disconnects", () => {
    renderHook(() => useTelemetryWebSocket(vi.fn()));

    // First disconnect → reconnect after 1s
    act(() => MockWebSocket.instances[0].simulateClose());
    act(() => vi.advanceTimersByTime(1100));
    expect(MockWebSocket.instances).toHaveLength(2);

    // Second disconnect → reconnect after 2s
    act(() => MockWebSocket.instances[1].simulateClose());
    act(() => vi.advanceTimersByTime(1100));
    expect(MockWebSocket.instances).toHaveLength(2); // not yet
    act(() => vi.advanceTimersByTime(1000));
    expect(MockWebSocket.instances).toHaveLength(3);
  });

  it("resets backoff on successful connection", () => {
    renderHook(() => useTelemetryWebSocket(vi.fn()));

    // Disconnect → reconnect after 1s
    act(() => MockWebSocket.instances[0].simulateClose());
    act(() => vi.advanceTimersByTime(1100));
    expect(MockWebSocket.instances).toHaveLength(2);

    // Second disconnect → would be 2s, but open resets it
    act(() => MockWebSocket.instances[1].simulateOpen());
    act(() => MockWebSocket.instances[1].simulateClose());
    act(() => vi.advanceTimersByTime(1100));
    expect(MockWebSocket.instances).toHaveLength(3); // reset to 1s
  });

  it("closes WebSocket on unmount", () => {
    const { unmount } = renderHook(() => useTelemetryWebSocket(vi.fn()));
    const ws = MockWebSocket.instances[0];
    act(() => ws.simulateOpen());

    unmount();
    expect(ws.closed).toBe(true);
  });
});
