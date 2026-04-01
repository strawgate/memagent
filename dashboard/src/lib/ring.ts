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

  get length(): number {
    return this.data.length;
  }
}
