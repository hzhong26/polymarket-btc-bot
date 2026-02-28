use std::collections::VecDeque;

/// Maximum buffer size: 10 minutes of sub-second data is ~600 entries.
const MAX_BUFFER: usize = 1200;

/// Minimum observations needed before producing a z-score.
const MIN_OBS: usize = 20;

/// Tracks recent prices for computing short-term momentum/mean-reversion z-scores.
///
/// Stores a ring buffer of (price, timestamp_secs) pairs and computes:
///   z = return_over_window / rolling_std_of_returns
///
/// Negative z means price dropped → mean reversion predicts bounce up.
/// Positive z means price rose → mean reversion predicts pullback down.
#[derive(Clone)]
pub struct MomentumTracker {
    /// Ring buffer of (price, timestamp_secs).
    prices: VecDeque<(f64, f64)>,
}

impl MomentumTracker {
    pub fn new() -> Self {
        Self {
            prices: VecDeque::with_capacity(MAX_BUFFER),
        }
    }

    /// Feed a new price observation.
    pub fn add_price(&mut self, price: f64, timestamp_secs: f64) {
        if price <= 0.0 {
            return;
        }
        // Reject out-of-order or duplicate timestamps
        if let Some(&(_, last_ts)) = self.prices.back() {
            if timestamp_secs <= last_ts {
                return;
            }
        }
        self.prices.push_back((price, timestamp_secs));
        while self.prices.len() > MAX_BUFFER {
            self.prices.pop_front();
        }
    }

    /// Compute the z-score of the return over the last `lookback_secs`.
    ///
    /// z = log_return_over_window / rolling_std_of_log_returns
    ///
    /// Returns None if insufficient data.
    pub fn z_score(&self, lookback_secs: f64) -> Option<f64> {
        if self.prices.len() < MIN_OBS {
            return None;
        }

        let (current_price, current_ts) = *self.prices.back()?;
        let cutoff_ts = current_ts - lookback_secs;

        // Find the price closest to `cutoff_ts` ago
        let anchor_price = self.price_at_or_before(cutoff_ts)?;

        // Log return over the lookback window
        let window_return = (current_price / anchor_price).ln();

        // Compute rolling std of log returns over the same window
        let std = self.rolling_std(cutoff_ts)?;
        if std < 1e-10 {
            return None; // Avoid division by zero
        }

        Some(window_return / std)
    }

    /// Get the log return over the last `lookback_secs`.
    /// Returns None if insufficient data.
    pub fn log_return(&self, lookback_secs: f64) -> Option<f64> {
        let (current_price, current_ts) = *self.prices.back()?;
        let cutoff_ts = current_ts - lookback_secs;
        let anchor_price = self.price_at_or_before(cutoff_ts)?;
        Some((current_price / anchor_price).ln())
    }

    /// Number of price observations stored.
    pub fn len(&self) -> usize {
        self.prices.len()
    }

    /// Find the price at or just before a given timestamp.
    fn price_at_or_before(&self, ts: f64) -> Option<f64> {
        // Binary search for the last entry with timestamp <= ts
        let mut lo = 0usize;
        let mut hi = self.prices.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.prices[mid].1 <= ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // lo is now the first index AFTER ts
        if lo == 0 {
            return None; // No data before cutoff
        }
        Some(self.prices[lo - 1].0)
    }

    /// Compute the standard deviation of tick-to-tick log returns
    /// for observations after `since_ts`.
    fn rolling_std(&self, since_ts: f64) -> Option<f64> {
        // Find starting index
        let start = self.prices.iter().position(|(_, ts)| *ts >= since_ts)?;
        if start + 2 > self.prices.len() {
            return None; // Need at least 2 observations after cutoff
        }

        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut n = 0u32;

        for i in (start + 1)..self.prices.len() {
            let r = (self.prices[i].0 / self.prices[i - 1].0).ln();
            sum += r;
            sum_sq += r * r;
            n += 1;
        }

        if n < 2 {
            return None;
        }

        let mean = sum / n as f64;
        let variance = (sum_sq / n as f64) - mean * mean;
        if variance < 0.0 {
            return None;
        }

        // Scale from per-tick std to per-window std:
        // std_window = std_tick * sqrt(n_ticks)
        Some(variance.sqrt() * (n as f64).sqrt())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tracker() {
        let tracker = MomentumTracker::new();
        assert!(tracker.z_score(180.0).is_none());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_insufficient_data() {
        let mut tracker = MomentumTracker::new();
        for i in 0..10 {
            tracker.add_price(100.0 + i as f64, 1000.0 + i as f64);
        }
        // Less than MIN_OBS (20)
        assert!(tracker.z_score(5.0).is_none());
    }

    #[test]
    fn test_flat_price_zero_zscore() {
        let mut tracker = MomentumTracker::new();
        for i in 0..50 {
            tracker.add_price(100.0, 1000.0 + i as f64);
        }
        // Flat price → zero return → z should be None (zero std)
        assert!(tracker.z_score(30.0).is_none());
    }

    #[test]
    fn test_rising_price_positive_zscore() {
        let mut tracker = MomentumTracker::new();
        // Rising price with some noise (realistic: trend + noise)
        for i in 0..50 {
            let noise = if i % 3 == 0 { -0.02 } else if i % 3 == 1 { 0.03 } else { -0.01 };
            let price = 100.0 + (i as f64) * 0.10 + noise;
            tracker.add_price(price, 1000.0 + i as f64);
        }
        let z = tracker.z_score(30.0);
        assert!(z.is_some());
        assert!(z.unwrap() > 0.0, "rising price should give positive z, got {}", z.unwrap());
    }

    #[test]
    fn test_falling_price_negative_zscore() {
        let mut tracker = MomentumTracker::new();
        // Falling price with noise
        for i in 0..50 {
            let noise = if i % 3 == 0 { 0.02 } else if i % 3 == 1 { -0.03 } else { 0.01 };
            let price = 100.0 - (i as f64) * 0.10 + noise;
            tracker.add_price(price, 1000.0 + i as f64);
        }
        let z = tracker.z_score(30.0);
        assert!(z.is_some());
        assert!(z.unwrap() < 0.0, "falling price should give negative z, got {}", z.unwrap());
    }

    #[test]
    fn test_sharp_drop_large_negative_z() {
        let mut tracker = MomentumTracker::new();
        // Normal random walk, then a sharp drop
        for i in 0..40 {
            tracker.add_price(68000.0, 1000.0 + i as f64 * 3.0);
        }
        // Sharp 1% drop over 3 ticks
        tracker.add_price(67660.0, 1121.0);
        tracker.add_price(67340.0, 1124.0);
        tracker.add_price(67320.0, 1127.0);

        let z = tracker.z_score(60.0);
        assert!(z.is_some());
        assert!(
            z.unwrap() < -1.0,
            "1% drop should give z < -1, got {}",
            z.unwrap()
        );
    }

    #[test]
    fn test_rejects_out_of_order() {
        let mut tracker = MomentumTracker::new();
        tracker.add_price(100.0, 1000.0);
        tracker.add_price(101.0, 999.0); // out of order → rejected
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn test_rejects_zero_price() {
        let mut tracker = MomentumTracker::new();
        tracker.add_price(0.0, 1000.0);
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_log_return() {
        let mut tracker = MomentumTracker::new();
        for i in 0..30 {
            tracker.add_price(100.0, 1000.0 + i as f64);
        }
        tracker.add_price(101.0, 1030.0); // 1% up
        let ret = tracker.log_return(5.0);
        assert!(ret.is_some());
        let r = ret.unwrap();
        assert!((r - 0.01).abs() < 0.001, "expected ~0.01, got {}", r);
    }

    #[test]
    fn test_buffer_eviction() {
        let mut tracker = MomentumTracker::new();
        for i in 0..(MAX_BUFFER + 100) {
            tracker.add_price(100.0 + (i as f64 * 0.01), i as f64);
        }
        assert_eq!(tracker.len(), MAX_BUFFER);
    }
}
