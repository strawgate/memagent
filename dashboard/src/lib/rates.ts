/** EMA-smoothed rate tracker. Tracks counter deltas over time. */
export class RateTracker {
  private prev = new Map<string, { value: number; time: number }>();
  private ema = new Map<string, number>();
  private alpha: number;

  constructor(alpha: number = 0.3) {
    this.alpha = alpha;
  }

  /** Record a counter value. Returns the smoothed rate (units/sec) or null if first sample. */
  rate(key: string, value: number): number | null {
    const now = Date.now();
    const prev = this.prev.get(key);
    this.prev.set(key, { value, time: now });

    if (prev == null) return null;
    const dt = (now - prev.time) / 1000;
    if (dt <= 0) return this.ema.get(key) ?? null; // same render cycle — return last EMA

    const raw = Math.max(0, (value - prev.value) / dt);
    const prevEma = this.ema.get(key);
    const smoothed = prevEma == null ? raw : prevEma * (1 - this.alpha) + raw * this.alpha;
    this.ema.set(key, smoothed);
    return smoothed;
  }

  /** @deprecated No longer needed — each key tracks its own timestamp. */
  tick() {}
}
