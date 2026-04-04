export interface DataPoint {
  t: number; // timestamp ms
  v: number; // value
}

export class RingBuffer {
  private data: DataPoint[] = [];
  private capacity: number;

  constructor(capacity: number = 500) {
    this.capacity = capacity;
  }

  push(value: number) {
    this.data.push({ t: Date.now(), v: value });
    if (this.data.length > this.capacity) this.data.shift();
  }

  /** Push with explicit timestamp (for loading server-side history). */
  pushRaw(timestampMs: number, value: number) {
    this.data.push({ t: timestampMs, v: value });
    if (this.data.length > this.capacity) this.data.shift();
  }

  /** Get all points, trimmed to max age */
  points(maxAgeMs: number = 5 * 60 * 1000): DataPoint[] {
    const cutoff = Date.now() - maxAgeMs;
    return this.data.filter((d) => d.t >= cutoff);
  }

  /**
   * Return points averaged into fixed-width time buckets.
   * e.g. bucketMs=5000 → one averaged point per 5 seconds.
   */
  bucket(bucketMs: number, maxAgeMs: number = 5 * 60 * 1000): DataPoint[] {
    if (!Number.isFinite(bucketMs) || bucketMs <= 0) return [];
    const raw = this.points(maxAgeMs);
    if (raw.length === 0) return [];
    const map = new Map<number, { sum: number; count: number }>();
    for (const p of raw) {
      const key = Math.floor(p.t / bucketMs) * bucketMs;
      const entry = map.get(key);
      if (entry) {
        entry.sum += p.v;
        entry.count++;
      } else map.set(key, { sum: p.v, count: 1 });
    }
    return [...map.entries()]
      .sort(([a], [b]) => a - b)
      .map(([t, { sum, count }]) => ({ t: t + bucketMs / 2, v: sum / count }));
  }

  get length(): number {
    return this.data.length;
  }
}
