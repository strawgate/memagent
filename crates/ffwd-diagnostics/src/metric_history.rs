//! Server-side metric history with reducing precision.
//!
//! Stores the last hour of metric snapshots:
//! - Last 5 min: every 2s (150 points)
//! - 5-15 min: every 10s (60 points)
//! - 15-60 min: every 30s (90 points)
//!
//! Total: ~300 points per metric. At ~8 bytes each = ~2.4KB per metric.
//! With 10 metrics = ~24KB total. Negligible.
//!
//! The diagnostics thread samples metrics every 2s and pushes to this buffer.
//! On each push, old high-resolution points are downsampled to lower tiers.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Instant;

/// A single data point: timestamp (seconds since server start) + value.
#[derive(Clone, Copy, Debug)]
pub struct Point {
    pub t: f64, // seconds since server start
    pub v: f64,
}

/// One tier of the reducing-precision buffer.
struct Tier {
    interval_secs: f64,
    max_age_secs: f64,
    points: VecDeque<Point>,
}

impl Tier {
    fn new(interval_secs: f64, max_age_secs: f64) -> Self {
        let capacity = (max_age_secs / interval_secs) as usize + 2;
        Self {
            interval_secs,
            max_age_secs,
            points: VecDeque::with_capacity(capacity),
        }
    }

    fn push(&mut self, p: Point) {
        self.points.push_back(p);
    }

    fn trim(&mut self, now_secs: f64) {
        let cutoff = now_secs - self.max_age_secs;
        while let Some(front) = self.points.front() {
            if front.t < cutoff {
                self.points.pop_front();
            } else {
                break;
            }
        }
    }

    fn last_time(&self) -> Option<f64> {
        self.points.back().map(|p| p.t)
    }
}

/// A single metric's history across all tiers.
struct MetricBuffer {
    name: String,
    tiers: Vec<Tier>,
}

impl MetricBuffer {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            tiers: vec![
                Tier::new(2.0, 300.0),   // 2s intervals, 5 min
                Tier::new(10.0, 900.0),  // 10s intervals, contributes coverage up to 15 min
                Tier::new(30.0, 3600.0), // 30s intervals, contributes coverage up to 60 min
            ],
        }
    }

    /// Push a new high-resolution point. Automatically downsamples to lower tiers.
    fn push(&mut self, t: f64, v: f64) {
        if !t.is_finite() {
            return; // reject NaN and Infinity timestamps — they break tier trimming
        }
        if !v.is_finite() {
            // Value is non-finite — don't record it, but still trim all tiers
            // so that stale points are evicted even during NaN bursts. (#1464)
            for tier in &mut self.tiers {
                tier.trim(t);
            }
            return;
        }
        let p = Point { t, v };

        // Always push to tier 0 (highest resolution).
        #[allow(clippy::indexing_slicing)]
        {
            self.tiers[0].push(p);
            self.tiers[0].trim(t);
        }

        // Downsample to lower tiers if enough time has passed.
        #[allow(clippy::indexing_slicing)]
        for i in 1..self.tiers.len() {
            let interval = self.tiers[i].interval_secs;
            let should_push = match self.tiers[i].last_time() {
                None => true,
                Some(last_t) => t - last_t >= interval,
            };
            if should_push {
                self.tiers[i].push(p);
                self.tiers[i].trim(t);
            }
        }
    }

    /// Get all points across all tiers, merged and sorted by time.
    /// Lower tiers fill in history beyond what tier 0 covers.
    fn points(&self) -> Vec<Point> {
        let mut all = Vec::new();

        // Add points from lowest resolution first, then overlay higher.
        // Use a set of time windows to avoid duplicates.
        for tier in self.tiers.iter().rev() {
            for p in &tier.points {
                all.push(*p);
            }
        }

        // Sort by time and deduplicate close timestamps.
        all.retain(|p| !p.t.is_nan());
        all.sort_by(|a, b| a.t.total_cmp(&b.t));
        all.dedup_by(|a, b| (a.t - b.t).abs() < 1.0);
        all
    }
}

/// Collection of metric histories.
///
/// The diagnostics sampler records into this buffer, and HTTP handlers read
/// snapshots out of it. Internal synchronization is handled with a `Mutex`
/// because writes are infrequent and isolated to the diagnostics path.
pub struct MetricHistory {
    start: Instant,
    metrics: Mutex<Vec<MetricBuffer>>,
}

impl Default for MetricHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricHistory {
    /// Creates an empty history buffer anchored to the current instant.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            metrics: Mutex::new(Vec::new()),
        }
    }

    /// Record a value for a named metric.
    pub fn record(&self, name: &str, value: f64) {
        let t = self.start.elapsed().as_secs_f64();
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let buf = match metrics.iter_mut().find(|m| m.name == name) {
            Some(b) => b,
            None => {
                metrics.push(MetricBuffer::new(name));
                match metrics.last_mut() {
                    Some(last) => last,
                    None => return,
                }
            }
        };
        buf.push(t, value);
    }

    /// Get all history as JSON-ready data.
    /// Returns a map of metric name → list of [t, v] pairs.
    pub fn to_json(&self) -> String {
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut root = serde_json::Map::with_capacity(metrics.len());
        for metric in metrics.iter() {
            let points = metric
                .points()
                .into_iter()
                .map(|p| serde_json::json!([p.t, p.v]))
                .collect::<Vec<_>>();
            root.insert(metric.name.clone(), serde_json::Value::Array(points));
        }
        serde_json::to_string(&root).unwrap_or_else(|_| "{}".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_recording() {
        let h = MetricHistory::new();
        h.record("cpu", 1.0);
        h.record("cpu", 2.0);
        h.record("rss", 100.0);
        let json = h.to_json();
        assert!(json.contains("\"cpu\""));
        assert!(json.contains("\"rss\""));
    }

    #[test]
    fn test_tier_trim() {
        let mut tier = Tier::new(2.0, 10.0);
        for i in 0..20 {
            tier.push(Point {
                t: i as f64 * 2.0,
                v: i as f64,
            });
        }
        tier.trim(38.0); // now=38, cutoff=28
        assert!(tier.points.front().unwrap().t >= 28.0);
    }

    #[test]
    fn test_downsample() {
        let mut buf = MetricBuffer::new("test");
        // Push 100 points at 2s intervals (200 seconds)
        for i in 0..100 {
            buf.push(i as f64 * 2.0, i as f64);
        }
        let raw_points: usize = buf.tiers.iter().map(|tier| tier.points.len()).sum();
        let points = buf.points();
        // Should have points from all tiers, deduplicated
        assert!(!points.is_empty());
        assert!(raw_points > points.len()); // duplicates across tiers were removed
        assert_eq!(points.len(), 100); // highest-resolution tier preserves all unique timestamps
    }

    #[test]
    fn test_nan_timestamps_do_not_panic() {
        let mut buf = MetricBuffer::new("test");
        buf.push(1.0, 10.0);
        buf.push(f64::NAN, 20.0);
        buf.push(2.0, 30.0);
        let points = buf.points();
        assert_eq!(points.len(), 2);
        assert!((points[0].t - 1.0).abs() < f64::EPSILON);
        assert!((points[1].t - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_infinity_and_nan_rejection() {
        let mut buf = MetricBuffer::new("test");
        buf.push(1.0, 10.0);

        // Push non-finite timestamps
        buf.push(f64::INFINITY, 20.0);
        buf.push(f64::NEG_INFINITY, 30.0);
        buf.push(f64::NAN, 40.0);

        // Push non-finite values
        buf.push(2.0, f64::INFINITY);
        buf.push(3.0, f64::NEG_INFINITY);
        buf.push(4.0, f64::NAN);

        // Push a valid point to ensure it still works
        buf.push(5.0, 50.0);

        let points = buf.points();
        // Only 1.0 and 5.0 should be accepted
        assert_eq!(points.len(), 2);
        assert!((points[0].t - 1.0).abs() < f64::EPSILON);
        assert!((points[1].t - 5.0).abs() < f64::EPSILON);
        assert!((points[0].v - 10.0).abs() < f64::EPSILON);
        assert!((points[1].v - 50.0).abs() < f64::EPSILON);
    }
}
