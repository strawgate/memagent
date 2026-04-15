import { test as fcTest } from "@fast-check/vitest";
import fc from "fast-check";
import { RateTracker } from "../lib/rates";

// ─── helpers ─────────────────────────────────────────────────────────────────

/** Advance fake time by `ms` milliseconds. */
function advance(ms: number) {
  vi.setSystemTime(vi.getMockedSystemTime()?.valueOf() + ms);
}

// ─── first call returns null ──────────────────────────────────────────────────

describe("RateTracker — first call", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns null on the first call for a key (no prior sample to diff)", () => {
    const tracker = new RateTracker();
    expect(tracker.rate("key", 100)).toBeNull();
  });

  it("returns null on the first call for each distinct key independently", () => {
    const tracker = new RateTracker();
    tracker.rate("a", 10);
    advance(1000);
    tracker.rate("a", 20); // second call for "a" — not null
    // First call for "b" is still null
    expect(tracker.rate("b", 50)).toBeNull();
  });
});

// ─── basic rate calculation ───────────────────────────────────────────────────

describe("RateTracker — rate calculation", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("computes raw rate correctly for a simple two-sample sequence", () => {
    // alpha=1 means EMA = raw (no smoothing), useful for deterministic tests
    const tracker = new RateTracker(1.0);
    tracker.rate("k", 0); // first call — seeds prev
    advance(1000); // 1 second passes
    const r = tracker.rate("k", 100); // delta=100, dt=1s → 100/s
    expect(r).not.toBeNull();
    expect(r).toBeCloseTo(100, 5);
  });

  it("scales correctly for sub-second intervals", () => {
    const tracker = new RateTracker(1.0);
    tracker.rate("k", 0);
    advance(500); // 0.5 seconds
    const r = tracker.rate("k", 50); // 50 units / 0.5 s = 100/s
    expect(r).toBeCloseTo(100, 5);
  });

  it("returns 0 (not negative) when counter decreases (counter reset)", () => {
    const tracker = new RateTracker(1.0);
    tracker.rate("k", 1000);
    advance(1000);
    const r = tracker.rate("k", 500); // negative delta → clamped to 0
    expect(r).toBe(0);
  });

  it("returns 0 for no change in counter", () => {
    const tracker = new RateTracker(1.0);
    tracker.rate("k", 100);
    advance(1000);
    const r = tracker.rate("k", 100); // delta=0 → rate=0
    expect(r).toBe(0);
  });

  it("handles large counter deltas correctly", () => {
    const tracker = new RateTracker(1.0);
    tracker.rate("k", 0);
    advance(1000);
    const r = tracker.rate("k", 1_000_000);
    expect(r).toBeCloseTo(1_000_000, 3);
  });
});

// ─── EMA smoothing ───────────────────────────────────────────────────────────

describe("RateTracker — EMA smoothing", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("with default alpha=0.3, EMA converges toward actual rate over many calls", () => {
    const tracker = new RateTracker(0.3);
    // Constant rate of 100/s. After enough samples the EMA should approach 100.
    let counter = 0;
    let lastRate: number | null = null;
    for (let i = 0; i < 30; i++) {
      counter += 100;
      advance(1000);
      lastRate = tracker.rate("k", counter);
    }
    expect(lastRate).not.toBeNull();
    // EMA should be very close to 100 after 30 constant samples
    expect(lastRate!).toBeCloseTo(100, 1);
  });

  it("EMA blends previous smoothed value with new raw rate (alpha=0.5)", () => {
    const tracker = new RateTracker(0.5);

    // Seed with first sample
    tracker.rate("k", 0);
    advance(1000);
    // First real sample: raw=100, prevEma=null → EMA=100
    const r1 = tracker.rate("k", 100);
    expect(r1).toBeCloseTo(100, 5);

    advance(1000);
    // Second sample: raw=0 (counter stays at 100), prevEma=100 → EMA = 100*0.5 + 0*0.5 = 50
    const r2 = tracker.rate("k", 100);
    expect(r2).toBeCloseTo(50, 5);
  });

  it("first real sample initializes EMA to raw rate (no prior EMA)", () => {
    const tracker = new RateTracker(0.3);
    tracker.rate("k", 0); // seed
    advance(2000);
    // delta=200 over 2s → raw=100/s; no prior EMA → EMA=100
    const r = tracker.rate("k", 200);
    expect(r).toBeCloseTo(100, 5);
  });

  it("independent keys maintain separate EMA state", () => {
    const tracker = new RateTracker(1.0); // alpha=1 → EMA = raw
    tracker.rate("fast", 0);
    tracker.rate("slow", 0);
    advance(1000);
    const fast = tracker.rate("fast", 1000); // 1000/s
    const slow = tracker.rate("slow", 10); // 10/s
    expect(fast).toBeCloseTo(1000, 5);
    expect(slow).toBeCloseTo(10, 5);
  });
});

// ─── dt = 0 edge case ────────────────────────────────────────────────────────

describe("RateTracker — same-timestamp edge case (dt=0)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not produce NaN or Infinity when called twice at the same timestamp", () => {
    const tracker = new RateTracker();
    tracker.rate("k", 0); // seed
    advance(1000);
    tracker.rate("k", 100); // establishes EMA
    // Now call again without advancing time (dt=0)
    const r = tracker.rate("k", 200);
    expect(r).not.toBeNull();
    expect(Number.isFinite(r!)).toBe(true);
    expect(Number.isNaN(r!)).toBe(false);
  });

  it("returns the last EMA when dt=0 (no new rate calculated)", () => {
    const tracker = new RateTracker(1.0);
    tracker.rate("k", 0);
    advance(1000);
    // EMA is set to 100 here
    const ema = tracker.rate("k", 100);
    // Same timestamp — should return the cached EMA
    const same = tracker.rate("k", 999);
    expect(same).toBeCloseTo(ema!, 5);
  });

  it("returns null when dt=0 on first real sample (no EMA yet)", () => {
    const tracker = new RateTracker();
    tracker.rate("k", 0); // seed (no advance)
    // Same time as seed — dt=0, no EMA established
    const r = tracker.rate("k", 100);
    // dt=0 path: return ema.get(key) ?? null — EMA not set yet → null
    expect(r).toBeNull();
  });
});

// ─── tick() deprecation ───────────────────────────────────────────────────────

describe("RateTracker — tick() is a no-op", () => {
  it("tick() does not throw", () => {
    const tracker = new RateTracker();
    expect(() => tracker.tick()).not.toThrow();
  });

  it("calling tick() does not affect rate calculation", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const tracker = new RateTracker(1.0);
    tracker.rate("k", 0);
    advance(1000);
    tracker.tick(); // should do nothing
    tracker.tick();
    const r = tracker.rate("k", 100);
    expect(r).toBeCloseTo(100, 5);
    vi.useRealTimers();
  });
});

// ─── property-based tests ─────────────────────────────────────────────────────

describe("RateTracker — property-based invariants", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  fcTest.prop([
    fc.array(
      fc.record({
        dt: fc.integer({ min: 1, max: 10_000 }), // ms between samples
        value: fc.integer({ min: 0, max: 1_000_000 }),
      }),
      { minLength: 2, maxLength: 20 }
    ),
    fc.float({ min: Math.fround(0.01), max: Math.fround(1), noNaN: true }),
  ])("rate is always a finite non-negative number or null", (samples, alpha) => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const tracker = new RateTracker(alpha);
    for (const { dt, value } of samples) {
      advance(dt);
      const r = tracker.rate("k", value);
      if (r !== null) {
        expect(Number.isFinite(r)).toBe(true);
        expect(r).toBeGreaterThanOrEqual(0);
      }
    }
    vi.useRealTimers();
  });

  fcTest.prop([
    fc.integer({ min: 1, max: 100 }), // number of keys
    fc.integer({ min: 2, max: 10 }), // samples per key
  ])("each key tracks its own independent EMA state", (keyCount, samplesPerKey) => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const tracker = new RateTracker(1.0);
    const results: Record<string, number[]> = {};

    for (let s = 0; s < samplesPerKey; s++) {
      advance(1000);
      for (let k = 0; k < keyCount; k++) {
        const key = `key-${k}`;
        // Each key gets a distinct counter value: k * 10 per second
        const r = tracker.rate(key, s * k * 10);
        if (r !== null) {
          if (!results[key]) results[key] = [];
          results[key].push(r);
        }
      }
    }
    // Just verify no key's rate bled into another (no NaN/Infinity)
    for (const rates of Object.values(results)) {
      for (const r of rates) {
        expect(Number.isFinite(r)).toBe(true);
      }
    }
    vi.useRealTimers();
  });
});
