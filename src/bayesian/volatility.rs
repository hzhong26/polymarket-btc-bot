use std::collections::VecDeque;

/// Regime-aware volatility estimator.
///
/// Computes BTC volatility from rolling log-returns, with regime detection
/// to differentiate between low-vol and high-vol market states.
/// Outputs annualized volatility for use in the GBM/Black-Scholes model.
#[derive(Clone)]
pub struct VolatilityEstimator {
    /// Rolling window of log-returns
    log_returns: VecDeque<f64>,
    /// Maximum window size (number of observations to keep)
    window_size: usize,
    /// Threshold for regime detection (in annualized vol units)
    /// If estimated vol > high_vol_threshold, we're in high-vol regime
    high_vol_threshold: f64,
    /// Rolling prices for computing log-returns
    prices: VecDeque<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VolRegime {
    Low,
    High,
}

impl VolatilityEstimator {
    /// Create a new volatility estimator.
    ///
    /// - `window_size`: number of price observations to keep (e.g. 60 for 2 minutes at 2s intervals)
    /// - `high_vol_threshold`: annualized vol threshold for high-vol regime (e.g. 1.0 = 100%)
    pub fn new(window_size: usize, high_vol_threshold: f64) -> Self {
        Self {
            log_returns: VecDeque::with_capacity(window_size),
            window_size,
            high_vol_threshold,
            prices: VecDeque::with_capacity(window_size + 1),
        }
    }

    /// Add a new price observation and update rolling log-returns.
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
            }
        }

        self.prices.push_back(price);
        if self.prices.len() > self.window_size + 1 {
            self.prices.pop_front();
        }
    }

    /// Compute annualized volatility from rolling log-returns.
    ///
    /// σ_annual = σ_per_tick * √(ticks_per_year)
    ///
    /// For 2-second price ticks:
    /// ticks_per_year = 365.25 * 24 * 3600 / 2 ≈ 15,778,800
    ///
    /// Returns None if insufficient data (< 2 observations).
    pub fn annualized_volatility(&self, tick_interval_secs: f64) -> Option<f64> {
        if self.log_returns.len() < 2 {
            return None;
        }

        let n = self.log_returns.len() as f64;
        let mean = self.log_returns.iter().sum::<f64>() / n;
        let variance = self
            .log_returns
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / (n - 1.0);

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_estimator() {
        let est = VolatilityEstimator::new(60, 1.0);
        assert_eq!(est.annualized_volatility(2.0), None);
        assert!(!est.is_ready());
    }

    #[test]
    fn test_single_price() {
        let mut est = VolatilityEstimator::new(60, 1.0);
        est.add_price(68000.0);
        assert_eq!(est.annualized_volatility(2.0), None);
    }

    #[test]
    fn test_constant_price_zero_vol() {
        let mut est = VolatilityEstimator::new(60, 1.0);
        for _ in 0..30 {
            est.add_price(68000.0);
        }
        let vol = est.annualized_volatility(2.0);
        // Constant price → zero log-returns → zero vol
        assert!(vol.is_none() || vol.unwrap() < 1e-10);
    }

    #[test]
    fn test_volatile_prices() {
        let mut est = VolatilityEstimator::new(60, 1.0);
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
        let mut est = VolatilityEstimator::new(60, 0.5);
        // Low-vol: tiny moves
        for i in 0..30 {
            est.add_price(68000.0 + (i as f64) * 0.01);
        }
        assert_eq!(est.regime(2.0), VolRegime::Low);
    }

    #[test]
    fn test_window_eviction() {
        let mut est = VolatilityEstimator::new(10, 1.0);
        for i in 0..20 {
            est.add_price(68000.0 + (i as f64) * 10.0);
        }
        assert_eq!(est.observation_count(), 10);
    }

    #[test]
    fn test_mean_log_return_per_tick() {
        let mut est = VolatilityEstimator::new(10, 1.0);
        est.add_price(100.0);
        est.add_price(101.0);
        est.add_price(102.0);
        let mean = est.mean_log_return_per_tick().unwrap();
        assert!(mean > 0.0);
    }

    #[test]
    fn test_invalid_prices_ignored() {
        let mut est = VolatilityEstimator::new(60, 1.0);
        est.add_price(68000.0);
        est.add_price(0.0); // ignored
        est.add_price(-1.0); // ignored
        est.add_price(68100.0);
        // Only one valid log-return (68000 → 68100)
        assert_eq!(est.observation_count(), 1);
    }
}
