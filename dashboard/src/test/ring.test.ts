import { test as fcTest } from "@fast-check/vitest";
import fc from "fast-check";
import { RingBuffer } from "../lib/ring";

// ─── helpers ─────────────────────────────────────────────────────────────────

/** Push n values at evenly-spaced timestamps starting at t0, stepping by stepMs. */
function _pushAt(rb: RingBuffer, t0: number, stepMs: number, values: number[]) {
  for (let i = 0; i < values.length; i++) rb.pushRaw(t0 + i * stepMs, values[i]!);
}

// ─── push & capacity ─────────────────────────────────────────────────────────

describe("RingBuffer — push and capacity", () => {
  it("starts empty", () => {
    const rb = new RingBuffer(5);
    expect(rb.length).toBe(0);
  });

  it("length grows with each push up to capacity", () => {
    const rb = new RingBuffer(3);
    rb.push(1);
    expect(rb.length).toBe(1);
    rb.push(2);
    expect(rb.length).toBe(2);
    rb.push(3);
    expect(rb.length).toBe(3);
  });

  it("length stays at capacity after overflow", () => {
    const rb = new RingBuffer(3);
    rb.push(1);
    rb.push(2);
    rb.push(3);
    rb.push(4); // evicts oldest
    expect(rb.length).toBe(3);
  });

  it("evicts oldest entry when over capacity (FIFO order)", () => {
    vi.useFakeTimers();
    const rb = new RingBuffer(3);
    vi.setSystemTime(1000);
    rb.push(10);
    vi.setSystemTime(2000);
    rb.push(20);
    vi.setSystemTime(3000);
    rb.push(30);
    vi.setSystemTime(4000);
    rb.push(40); // should evict the t=1000, v=10 entry

    const pts = rb.points(60_000);
    expect(pts).toHaveLength(3);
    expect(pts.map((p) => p.v)).toEqual([20, 30, 40]);
    vi.useRealTimers();
  });

  it("pushRaw stores exact timestamp and value", () => {
    const rb = new RingBuffer(10);
    rb.pushRaw(12345, 99);
    const pts = rb.points(Number.MAX_SAFE_INTEGER);
    expect(pts).toHaveLength(1);
    expect(pts[0]).toEqual({ t: 12345, v: 99 });
  });

  it("pushRaw also respects capacity limit", () => {
    const rb = new RingBuffer(2);
    rb.pushRaw(100, 1);
    rb.pushRaw(200, 2);
    rb.pushRaw(300, 3);
    expect(rb.length).toBe(2);
    const pts = rb.points(Number.MAX_SAFE_INTEGER);
    expect(pts.map((p) => p.v)).toEqual([2, 3]);
  });
});

// ─── points(maxAgeMs) ────────────────────────────────────────────────────────

describe("RingBuffer — points(maxAgeMs)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns empty array for empty buffer", () => {
    const rb = new RingBuffer();
    expect(rb.points(60_000)).toEqual([]);
  });

  it("returns all points within the age window", () => {
    const now = 100_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(10);
    rb.pushRaw(now - 5_000, 1); // 5 s ago — within 10 s window
    rb.pushRaw(now - 8_000, 2); // 8 s ago — within 10 s window

    const pts = rb.points(10_000);
    expect(pts).toHaveLength(2);
  });

  it("excludes points older than maxAgeMs", () => {
    const now = 100_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(10);
    rb.pushRaw(now - 15_000, 1); // too old
    rb.pushRaw(now - 5_000, 2); // within window

    const pts = rb.points(10_000);
    expect(pts).toHaveLength(1);
    expect(pts[0].v).toBe(2);
  });

  it("a point exactly at the cutoff boundary is included", () => {
    const now = 100_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(10);
    rb.pushRaw(now - 10_000, 42); // exactly at cutoff (t >= cutoff is inclusive)

    const pts = rb.points(10_000);
    expect(pts).toHaveLength(1);
  });

  it("returns empty array when all points are older than window", () => {
    const now = 100_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(10);
    rb.pushRaw(now - 60_000, 1);
    rb.pushRaw(now - 30_000, 2);

    expect(rb.points(1_000)).toEqual([]);
  });

  it("returns points in chronological (ascending timestamp) order", () => {
    const now = 100_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(10);
    // Push in order — ring maintains insertion order
    rb.pushRaw(now - 9_000, 1);
    rb.pushRaw(now - 6_000, 2);
    rb.pushRaw(now - 3_000, 3);

    const pts = rb.points(10_000);
    const ts = pts.map((p) => p.t);
    expect(ts).toEqual([...ts].sort((a, b) => a - b));
  });

  it("single-entry buffer returns that entry if within window", () => {
    const now = 50_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(5);
    rb.pushRaw(now - 1_000, 7);

    const pts = rb.points(5_000);
    expect(pts).toHaveLength(1);
    expect(pts[0].v).toBe(7);
  });

  it("single-entry buffer returns empty if outside window", () => {
    const now = 50_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(5);
    rb.pushRaw(now - 10_000, 7);

    expect(rb.points(5_000)).toEqual([]);
  });
});

// ─── bucket(bucketMs, maxAgeMs) ──────────────────────────────────────────────

describe("RingBuffer — bucket()", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns empty array for empty buffer", () => {
    const rb = new RingBuffer();
    vi.setSystemTime(100_000);
    expect(rb.bucket(5_000)).toEqual([]);
  });

  it("averages multiple entries in the same bucket", () => {
    // Put 3 points all inside the same 10-second bucket [0, 10000)
    const rb = new RingBuffer(10);
    const now = 1_000_000;
    vi.setSystemTime(now);

    rb.pushRaw(now - 9_500, 10); // bucket key = floor(t/10000)*10000
    rb.pushRaw(now - 9_000, 20);
    rb.pushRaw(now - 8_500, 30);

    const pts = rb.bucket(10_000, 15_000);
    // All three fall in the same 10-second bucket → single averaged point
    expect(pts).toHaveLength(1);
    expect(pts[0].v).toBeCloseTo(20, 5); // (10+20+30)/3 = 20
  });

  it("bucket timestamp is the midpoint of the bucket interval", () => {
    const rb = new RingBuffer(10);
    const now = 1_000_000;
    vi.setSystemTime(now);
    // bucket = 5000 ms; key = floor(t/5000)*5000; midpoint = key + 2500
    const t = 100_000; // floor(100000/5000)*5000 = 100000; midpoint = 102500
    rb.pushRaw(t, 1);

    const pts = rb.bucket(5_000, now - t + 1000);
    expect(pts).toHaveLength(1);
    // midpoint = bucket_key + bucketMs/2
    const expectedT = Math.floor(t / 5000) * 5000 + 2500;
    expect(pts[0].t).toBe(expectedT);
  });

  it("separates entries into distinct buckets", () => {
    const rb = new RingBuffer(20);
    const now = 2_000_000;
    vi.setSystemTime(now);
    const bucketMs = 5_000;

    // Two points in bucket A (t=1_990_000-1_994_999 → key=1_990_000)
    rb.pushRaw(1_990_000, 10);
    rb.pushRaw(1_992_000, 20);
    // One point in bucket B (t=1_995_000-1_999_999 → key=1_995_000)
    rb.pushRaw(1_997_000, 30);

    const pts = rb.bucket(bucketMs, 15_000);
    expect(pts).toHaveLength(2);
    expect(pts[0].v).toBeCloseTo(15); // (10+20)/2
    expect(pts[1].v).toBeCloseTo(30);
  });

  it("returns buckets in chronological order", () => {
    const rb = new RingBuffer(20);
    const now = 2_000_000;
    vi.setSystemTime(now);

    rb.pushRaw(1_985_000, 5);
    rb.pushRaw(1_990_000, 10);
    rb.pushRaw(1_995_000, 15);

    const pts = rb.bucket(5_000, 20_000);
    const ts = pts.map((p) => p.t);
    expect(ts).toEqual([...ts].sort((a, b) => a - b));
  });

  it("single entry produces one bucket point with that value", () => {
    const rb = new RingBuffer(5);
    const now = 500_000;
    vi.setSystemTime(now);
    rb.pushRaw(now - 2_000, 42);

    const pts = rb.bucket(5_000, 10_000);
    expect(pts).toHaveLength(1);
    expect(pts[0].v).toBe(42);
  });

  it("all entries in same bucket — returns single averaged point", () => {
    const rb = new RingBuffer(10);
    // Use now=99_999 so it's not on a 10_000 boundary — all 5 points land in bucket 90000
    const now = 99_999;
    vi.setSystemTime(now);

    for (let i = 0; i < 5; i++) {
      rb.pushRaw(now - (4 - i) * 100, i * 10); // t=99599,99699,99799,99899,99999 → all in [90000,100000) bucket
    }

    const pts = rb.bucket(10_000, 15_000);
    expect(pts).toHaveLength(1);
    expect(pts[0].v).toBeCloseTo(20); // (0+10+20+30+40)/5 = 20
  });
});

// ─── length property ─────────────────────────────────────────────────────────

describe("RingBuffer — length property", () => {
  it("never exceeds capacity", () => {
    const rb = new RingBuffer(10);
    for (let i = 0; i < 50; i++) {
      rb.pushRaw(i * 1000, i);
    }
    expect(rb.length).toBeLessThanOrEqual(10);
  });
});

// ─── property-based tests ────────────────────────────────────────────────────

describe("RingBuffer — property-based invariants", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  fcTest.prop([
    fc.integer({ min: 1, max: 100 }),
    fc.array(fc.float({ noNaN: true, noDefaultInfinity: true }), { minLength: 0, maxLength: 300 }),
  ])("length never exceeds capacity regardless of push count", (capacity, values) => {
    const rb = new RingBuffer(capacity);
    for (let i = 0; i < values.length; i++) rb.pushRaw(i * 100, values[i]!);
    expect(rb.length).toBeLessThanOrEqual(capacity);
  });

  // points() returns insertion order, not sorted — don't assert ordering on it.
  // bucket() sorts by bucket key, so it always returns ascending timestamps.
  fcTest.prop([
    fc.array(
      fc.record({
        t: fc.integer({ min: 1_000_000, max: 2_000_000 }),
        v: fc.float({ noNaN: true, noDefaultInfinity: true }),
      }),
      { minLength: 1, maxLength: 50 }
    ),
  ])("bucket() always returns entries in ascending timestamp order", (rawPts) => {
    vi.useFakeTimers();
    vi.setSystemTime(3_000_000);
    const rb = new RingBuffer(200);
    for (const { t, v } of rawPts) {
      rb.pushRaw(t, v);
    }
    const pts = rb.bucket(10_000, 2_000_000);
    const ts = pts.map((p) => p.t);
    for (let i = 1; i < ts.length; i++) {
      expect(ts[i]).toBeGreaterThanOrEqual(ts[i - 1]);
    }
    vi.useRealTimers();
  });

  fcTest.prop([
    fc.integer({ min: 1, max: 50 }),
    fc.array(fc.float({ min: 0, noNaN: true, noDefaultInfinity: true }), {
      minLength: 0,
      maxLength: 100,
    }),
  ])("bucket() always returns fewer or equal entries than points()", (bucketMs, values) => {
    vi.useFakeTimers();
    const now = 5_000_000;
    vi.setSystemTime(now);
    const rb = new RingBuffer(200);
    for (let i = 0; i < values.length; i++)
      rb.pushRaw(now - (values.length - i) * 1000, values[i]!);
    const maxAge = values.length * 1100;
    const pts = rb.points(maxAge);
    const buckets = rb.bucket(bucketMs * 1000, maxAge);
    expect(buckets.length).toBeLessThanOrEqual(pts.length);
    vi.useRealTimers();
  });
});
