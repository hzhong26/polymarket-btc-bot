use std::collections::VecDeque;

/// Regime-aware volatility estimator using EWMA (Exponentially Weighted Moving Average).
///
/// Uses the RiskMetrics-style recursive update:
///   σ²_t = λ × σ²_(t-1) + (1-λ) × r²_t
///
/// This weights recent returns exponentially more than older ones, allowing the
/// vol estimate to react to regime changes within seconds rather than waiting
/// for the full rolling window to turn over.
#[derive(Clone)]
pub struct VolatilityEstimator {
    /// Rolling window of log-returns (kept for mean_log_return_per_tick drift calc)
    log_returns: VecDeque<f64>,
    /// Maximum window size (number of observations to keep)
    window_size: usize,
    /// Threshold for regime detection (in annualized vol units)
    high_vol_threshold: f64,
    /// Rolling prices for computing log-returns
    prices: VecDeque<f64>,
    /// EWMA decay factor (e.g. 0.94)
    ewma_lambda: f64,
    /// Running EWMA variance (per-tick, not annualized). None until warmup completes.
    ewma_variance: Option<f64>,
    /// Total observations seen (for warmup logic)
    total_observations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VolRegime {
    Low,
    High,
}

impl VolatilityEstimator {
    /// Create a new volatility estimator.
    ///
    /// - `window_size`: number of price observations to keep (e.g. 100 for ~5 min at 3s intervals)
    /// - `high_vol_threshold`: annualized vol threshold for high-vol regime (e.g. 1.0 = 100%)
    /// - `ewma_lambda`: EWMA decay factor (0.94 = RiskMetrics standard); clamped to (0.5, 0.999)
    pub fn new(window_size: usize, high_vol_threshold: f64, ewma_lambda: f64) -> Self {
        let lambda = ewma_lambda.clamp(0.5, 0.999);
        Self {
            log_returns: VecDeque::with_capacity(window_size),
            window_size,
            high_vol_threshold,
            prices: VecDeque::with_capacity(window_size + 1),
            ewma_lambda: lambda,
            ewma_variance: None,
            total_observations: 0,
        }
    }

    /// Warmup threshold: 25% of window, minimum 2.
    fn warmup_threshold(&self) -> usize {
        (self.window_size / 4).max(2)
    }

    /// Add a new price observation and update rolling log-returns + EWMA variance.
    pub fn add_price(&mut self, price: f64) {
        if price <= 0.0 {
            return;
        }

        if let Some(&last_price) = self.prices.back() {
            if last_price > 0.0 {
                let log_return = (price / last_price).ln();
                self.log_returns.push_back(log_return);
                if self.log_returns.len() > self.window_size {
                    self.log_returns.pop_front();
                }

                let r_sq = log_return * log_return;

                if let Some(var) = self.ewma_variance {
                    // Recursive EWMA update
                    self.ewma_variance = Some(self.ewma_lambda * var + (1.0 - self.ewma_lambda) * r_sq);
                } else if self.total_observations + 1 >= self.warmup_threshold() {
                    // Seed EWMA from simple zero-mean variance of accumulated returns
                    let sum_r_sq: f64 = self.log_returns.iter().map(|r| r * r).sum();
                    let n = self.log_returns.len() as f64;
                    self.ewma_variance = Some(sum_r_sq / n);
                }

                self.total_observations += 1;
            }
        } else {
            // First price — no return yet, but count as an observation
            self.total_observations += 1;
        }

        self.prices.push_back(price);
        if self.prices.len() > self.window_size + 1 {
            self.prices.pop_front();
        }
    }

    /// Compute annualized volatility.
    ///
    /// Uses EWMA variance when available (post-warmup), otherwise falls back to
    /// simple zero-mean variance (Σr²/n) during warmup.
    ///
    /// σ_annual = √(variance_per_tick) × √(ticks_per_year)
    ///
    /// Returns None if insufficient data (< 2 observations).
    pub fn annualized_volatility(&self, tick_interval_secs: f64) -> Option<f64> {
        if self.log_returns.len() < 2 {
            return None;
        }

        let variance = if let Some(v) = self.ewma_variance {
            v
        } else {
            // Warmup fallback: simple zero-mean variance
            let n = self.log_returns.len() as f64;
            let sum_r_sq: f64 = self.log_returns.iter().map(|r| r * r).sum();
            sum_r_sq / n
        };

        let sigma_per_tick = variance.sqrt();
        let ticks_per_year = (365.25 * 24.0 * 3600.0) / tick_interval_secs;
        let annualized = sigma_per_tick * ticks_per_year.sqrt();

        if annualized.is_finite() && annualized > 0.0 {
            Some(annualized)
        } else {
            None
        }
    }

    /// Mean log-return per tick over the rolling window.
    pub fn mean_log_return_per_tick(&self) -> Option<f64> {
        if self.log_returns.is_empty() {
            return None;
        }
        let n = self.log_returns.len() as f64;
        Some(self.log_returns.iter().sum::<f64>() / n)
    }

    /// Detect the current volatility regime.
    pub fn regime(&self, tick_interval_secs: f64) -> VolRegime {
        match self.annualized_volatility(tick_interval_secs) {
            Some(vol) if vol > self.high_vol_threshold => VolRegime::High,
            _ => VolRegime::Low,
        }
    }

    /// Get the number of observations currently in the window.
    pub fn observation_count(&self) -> usize {
        self.log_returns.len()
    }

    /// Check if we have enough data for a reliable estimate.
    /// Requires at least half the window to be filled.
    pub fn is_ready(&self) -> bool {
        self.log_returns.len() >= self.window_size / 2
    }

    /// Get the current EWMA variance (per-tick, not annualized). None if not yet warmed up.
    pub fn ewma_variance(&self) -> Option<f64> {
        self.ewma_variance
    }

    /// Total observations seen (for warmup tracking).
    pub fn total_observations(&self) -> usize {
        self.total_observations
    }

    /// Whether the EWMA estimator has completed warmup and is producing recursive updates.
    pub fn is_warmed_up(&self) -> bool {
        self.ewma_variance.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_estimator() {
        let est = VolatilityEstimator::new(60, 1.0, 0.94);
        assert_eq!(est.annualized_volatility(2.0), None);
        assert!(!est.is_ready());
    }

    #[test]
    fn test_single_price() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94);
        est.add_price(68000.0);
        assert_eq!(est.annualized_volatility(2.0), None);
    }

    #[test]
    fn test_constant_price_zero_vol() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94);
        for _ in 0..30 {
            est.add_price(68000.0);
        }
        let vol = est.annualized_volatility(2.0);
        // Constant price → zero log-returns → zero vol
        assert!(vol.is_none() || vol.unwrap() < 1e-10);
    }

    #[test]
    fn test_volatile_prices() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94);
        // Simulate BTC bouncing between 67000 and 69000
        for i in 0..30 {
            let price = if i % 2 == 0 { 67000.0 } else { 69000.0 };
            est.add_price(price);
        }
        let vol = est.annualized_volatility(2.0);
        assert!(vol.is_some());
        assert!(vol.unwrap() > 0.0);
    }

    #[test]
    fn test_regime_detection() {
        let mut est = VolatilityEstimator::new(60, 0.5, 0.94);
        // Low-vol: tiny moves
        for i in 0..30 {
            est.add_price(68000.0 + (i as f64) * 0.01);
        }
        assert_eq!(est.regime(2.0), VolRegime::Low);
    }

    #[test]
    fn test_window_eviction() {
        let mut est = VolatilityEstimator::new(10, 1.0, 0.94);
        for i in 0..20 {
            est.add_price(68000.0 + (i as f64) * 10.0);
        }
        assert_eq!(est.observation_count(), 10);
    }

    #[test]
    fn test_mean_log_return_per_tick() {
        let mut est = VolatilityEstimator::new(10, 1.0, 0.94);
        est.add_price(100.0);
        est.add_price(101.0);
        est.add_price(102.0);
        let mean = est.mean_log_return_per_tick().unwrap();
        assert!(mean > 0.0);
    }

    #[test]
    fn test_invalid_prices_ignored() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94);
        est.add_price(68000.0);
        est.add_price(0.0); // ignored
        est.add_price(-1.0); // ignored
        est.add_price(68100.0);
        // Only one valid log-return (68000 → 68100)
        assert_eq!(est.observation_count(), 1);
    }

    // ── EWMA-specific tests ──

    #[test]
    fn test_ewma_warmup_seeds_from_simple_variance() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94);
        // warmup_threshold = 100/4 = 25
        // Feed 24 prices (23 returns) — should NOT have EWMA yet
        for i in 0..24 {
            est.add_price(68000.0 + (i as f64) * 5.0);
        }
        assert!(est.ewma_variance.is_none());

        // 25th price (24th return) triggers warmup (total_observations+1 >= 25) → EWMA seeded
        est.add_price(68125.0);
        assert!(est.ewma_variance.is_some());
        assert!(est.ewma_variance.unwrap() > 0.0);
    }

    #[test]
    fn test_ewma_reacts_fast_to_vol_spike() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94);
        // Build a calm baseline: tiny moves
        for i in 0..60 {
            est.add_price(68000.0 + (i as f64) * 0.10);
        }
        let calm_vol = est.annualized_volatility(3.0).unwrap();

        // Single 1% spike
        let last = 68000.0 + 59.0 * 0.10;
        est.add_price(last * 1.01);

        let spike_vol = est.annualized_volatility(3.0).unwrap();
        // Vol should jump >2x after the spike
        assert!(
            spike_vol > calm_vol * 2.0,
            "spike_vol={spike_vol} should be > 2x calm_vol={calm_vol}"
        );
    }

    #[test]
    fn test_ewma_decays_after_spike() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94);
        // Calm baseline
        for i in 0..60 {
            est.add_price(68000.0 + (i as f64) * 0.10);
        }

        // Spike
        let last = 68000.0 + 59.0 * 0.10;
        let spiked = last * 1.01;
        est.add_price(spiked);
        let spike_vol = est.annualized_volatility(3.0).unwrap();

        // 20 calm ticks after spike
        for i in 0..20 {
            est.add_price(spiked + (i as f64) * 0.10);
        }
        let decayed_vol = est.annualized_volatility(3.0).unwrap();

        // Should decay to <70% of spike
        assert!(
            decayed_vol < spike_vol * 0.70,
            "decayed_vol={decayed_vol} should be < 70% of spike_vol={spike_vol}"
        );
    }

    #[test]
    fn test_ewma_lambda_sensitivity() {
        // Lower lambda → faster reaction → higher vol after same spike
        let mut est_high = VolatilityEstimator::new(100, 1.0, 0.97);
        let mut est_low = VolatilityEstimator::new(100, 1.0, 0.85);

        for i in 0..60 {
            let p = 68000.0 + (i as f64) * 0.10;
            est_high.add_price(p);
            est_low.add_price(p);
        }

        // Same spike
        let last = 68000.0 + 59.0 * 0.10;
        est_high.add_price(last * 1.01);
        est_low.add_price(last * 1.01);

        let vol_high_lambda = est_high.annualized_volatility(3.0).unwrap();
        let vol_low_lambda = est_low.annualized_volatility(3.0).unwrap();

        assert!(
            vol_low_lambda > vol_high_lambda,
            "lower lambda ({vol_low_lambda}) should produce higher vol than higher lambda ({vol_high_lambda})"
        );
    }

    #[test]
    fn test_ewma_constant_price_still_zero_vol() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94);
        for _ in 0..60 {
            est.add_price(68000.0);
        }
        // EWMA should be seeded (60 > warmup_threshold=25), but variance is 0
        assert!(est.ewma_variance.is_some());
        let vol = est.annualized_volatility(3.0);
        assert!(vol.is_none() || vol.unwrap() < 1e-10);
    }
}
