import type { TimeSeriesFrame, TimeSeriesPoint } from "@otlpkit/views";
import { cleanup, render, screen } from "@testing-library/preact";
import type { ChartConfig } from "../components/Chart";
import { Chart } from "../components/Chart";

// ─── uPlot mock ──────────────────────────────────────────────────────────────

const mockSetData = vi.fn();
const mockSetScale = vi.fn();
const mockDestroy = vi.fn();
const mockBatch = vi.fn((cb: () => void) => cb());
const mockSetSize = vi.fn();

let constructorCallCount = 0;

vi.mock("uplot", () => {
  const UPlot = vi.fn().mockImplementation(function (this: Record<string, unknown>) {
    constructorCallCount++;
    this.setData = mockSetData;
    this.setScale = mockSetScale;
    this.destroy = mockDestroy;
    this.batch = mockBatch;
    this.setSize = mockSetSize;
  });

  (UPlot as Record<string, unknown>).paths = { spline: () => () => null };

  return { default: UPlot };
});

// ─── Helpers ──────────────────────────────────────────────────────────────────

const DEFAULT_CONFIG: ChartConfig = {
  metricName: "ffwd.input_lines_per_sec",
  label: "Lines / sec",
  color: "#3b82f6",
  unit: "/s",
  yRange: [0, 100],
};

function makePoint(timeMs: number, value: number): TimeSeriesPoint {
  const ns = String(Math.floor(timeMs * 1_000_000));
  return {
    timeUnixNano: ns,
    timeMs,
    isoTime: new Date(timeMs).toISOString(),
    value,
    samples: 1,
  };
}

function makeFrame(points: TimeSeriesPoint[]): TimeSeriesFrame {
  return {
    kind: "time-series",
    signal: "metrics",
    title: "Test",
    unit: "/s",
    intervalMs: 2000,
    series: [{ key: "test", label: "Test", points }],
  };
}

function emptyFrame(): TimeSeriesFrame {
  return makeFrame([]);
}

function filledFrame(count = 4): TimeSeriesFrame {
  const now = Date.now();
  const pts = Array.from({ length: count }, (_, i) => makePoint(now - (count - i) * 2000, 100 + i));
  return makeFrame(pts);
}

// ─── RAF control ──────────────────────────────────────────────────────────────

let rafCallbacks: FrameRequestCallback[] = [];
let rafId = 1;

function installFakeRaf() {
  rafCallbacks = [];
  rafId = 1;
  vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => {
    rafCallbacks.push(cb);
    return rafId++;
  });
  vi.stubGlobal("cancelAnimationFrame", (_id: number) => {
    rafCallbacks = [];
  });
}

function flushRaf() {
  const cbs = rafCallbacks.splice(0);
  for (const cb of cbs) cb(performance.now());
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe("Chart", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000_000_000);
    installFakeRaf();
    constructorCallCount = 0;
    mockSetData.mockClear();
    mockSetScale.mockClear();
    mockDestroy.mockClear();
    mockBatch.mockClear();
    mockSetSize.mockClear();
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("shows 'waiting for data' when frame is empty", () => {
    render(<Chart frame={emptyFrame()} config={DEFAULT_CONFIG} />);
    flushRaf();
    expect(screen.getByText("waiting for data…")).toBeInTheDocument();
  });

  it("creates plot when frame has ≥2 points", () => {
    const { container } = render(<Chart frame={filledFrame()} config={DEFAULT_CONFIG} />);
    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    flushRaf();
    expect(constructorCallCount).toBe(1);
  });

  it("destroys plot when frame becomes empty", () => {
    const { container, rerender } = render(<Chart frame={filledFrame()} config={DEFAULT_CONFIG} />);
    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    flushRaf();
    expect(constructorCallCount).toBe(1);

    rerender(<Chart frame={emptyFrame()} config={DEFAULT_CONFIG} />);
    flushRaf();
    expect(mockDestroy).toHaveBeenCalledTimes(1);
  });

  it("renders immediately if frame has data on mount", () => {
    const { container } = render(<Chart frame={filledFrame()} config={DEFAULT_CONFIG} />);
    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    flushRaf();
    expect(constructorCallCount).toBe(1);
  });

  it("updates plot data on each animation frame", () => {
    const { container } = render(<Chart frame={filledFrame()} config={DEFAULT_CONFIG} />);
    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    flushRaf();
    expect(constructorCallCount).toBe(1);

    mockBatch.mockClear();
    mockSetData.mockClear();
    mockSetScale.mockClear();
    flushRaf();
    expect(mockBatch).toHaveBeenCalledTimes(1);
    expect(mockSetData).toHaveBeenCalledTimes(1);
    expect(mockSetScale).toHaveBeenCalledTimes(1);
  });
});
