import { cleanup, render, screen } from "@testing-library/preact";
import { RingBuffer } from "../lib/ring";
import { Chart, CHART_CONSTANTS } from "../components/Chart";
import type { MetricSeries } from "../app";

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

  // uPlot.paths.spline is used in buildChartOpts
  (UPlot as Record<string, unknown>).paths = { spline: () => () => null };

  return { default: UPlot };
});

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Build a MetricSeries with a fresh RingBuffer. */
function makeSeries(overrides?: Partial<MetricSeries>): MetricSeries {
  return {
    id: "test",
    label: "Test",
    color: "#3b82f6",
    ring: new RingBuffer(),
    value: "-",
    unit: "/s",
    yRange: [0, 100],
    ...overrides,
  };
}

/** Push enough data into a ring so bucket() returns ≥2 points. */
function fillRing(ring: RingBuffer, count = 4) {
  const now = Date.now();
  const bucketMs = CHART_CONSTANTS.BUCKET_MS;
  for (let i = 0; i < count; i++) {
    ring.pushRaw(now - (count - i) * bucketMs, 100 + i);
  }
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

/** Run one pending RAF tick. */
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
    // Cleanup component while RAF stubs are still alive, then restore globals
    cleanup();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("shows 'waiting for data' when ring is empty", () => {
    const s = makeSeries();
    render(<Chart series={s} />);
    flushRaf();

    expect(screen.getByText("waiting for data…")).toBeInTheDocument();
  });

  it("creates plot when ring gets ≥2 bucketed points", () => {
    const s = makeSeries();
    const { container } = render(<Chart series={s} />);

    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    // First frame: no data — should not create
    flushRaf();
    expect(constructorCallCount).toBe(0);

    // Add data
    fillRing(s.ring);

    // Next frame: data available — should create plot
    flushRaf();
    expect(constructorCallCount).toBe(1);
  });

  it("destroys plot when ring data is cleared", () => {
    const ring = new RingBuffer();
    fillRing(ring);
    const s = makeSeries({ ring });

    const { container } = render(<Chart series={s} />);
    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    // First frame: has data → creates plot
    flushRaf();
    expect(constructorCallCount).toBe(1);

    // Advance time beyond max age (5 min) so all data is stale
    vi.setSystemTime(Date.now() + 10 * 60 * 1000);

    // Next frame: no bucketed data → should destroy
    flushRaf();
    expect(mockDestroy).toHaveBeenCalledTimes(1);
  });

  it("renders immediately if ring has data on mount", () => {
    const ring = new RingBuffer();
    fillRing(ring);
    const s = makeSeries({ ring });

    const { container } = render(<Chart series={s} />);
    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    // First RAF tick creates plot since data is already present
    flushRaf();
    expect(constructorCallCount).toBe(1);
  });

  it("updates plot data on each animation frame", () => {
    const ring = new RingBuffer();
    fillRing(ring);
    const s = makeSeries({ ring });

    const { container } = render(<Chart series={s} />);
    const el = container.firstElementChild as HTMLElement;
    Object.defineProperty(el, "offsetWidth", { value: 400, configurable: true });

    // First frame: creates plot
    flushRaf();
    expect(constructorCallCount).toBe(1);

    // Second frame: should update data (batch calls setData + setScale)
    mockBatch.mockClear();
    mockSetData.mockClear();
    mockSetScale.mockClear();
    flushRaf();
    expect(mockBatch).toHaveBeenCalledTimes(1);
    expect(mockSetData).toHaveBeenCalledTimes(1);
    expect(mockSetScale).toHaveBeenCalledTimes(1);
  });
});
