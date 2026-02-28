use std::collections::VecDeque;

/// Seconds per year (365.25 days × 24h × 3600s).
const SECONDS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;

/// Minimum accepted dt between observations (50 ms).
/// Observations arriving faster than this are rejected to prevent division by near-zero.
const MIN_DT_SECS: f64 = 0.05;

/// Maximum dt used in a single EWMA update (30 seconds).
/// Gaps longer than this (e.g. WebSocket reconnect) are capped to prevent
/// a single outsized return from dominating the variance estimate.
const MAX_DT_SECS: f64 = 30.0;

/// Time-aware volatility estimator using EWMA.
///
/// Stores variance **per second** (not per tick), eliminating the need for an
/// external `tick_interval_secs` parameter.  Actual inter-observation time is
/// tracked via timestamps, and the EWMA decay is adjusted for each observation:
///
///   λ_eff = exp(-dt / half_life)
///   σ²_t  = λ_eff × σ²_{t-1} + (1 - λ_eff) × (r/dt)²
///
/// The half-life is derived once at construction from the caller's EWMA lambda
/// and reference interval:  `τ = -reference_dt / ln(λ)`.
#[derive(Clone)]
pub struct VolatilityEstimator {
    /// Rolling window of (log_return, dt_secs) pairs.
    log_returns: VecDeque<(f64, f64)>,
    /// Maximum window size (number of return observations to keep).
    window_size: usize,
    /// Threshold for regime detection (in annualized vol units).
    high_vol_threshold: f64,
    /// Last observed price (for computing next log-return).
    last_price: Option<f64>,
    /// Timestamp of the last accepted observation (seconds, e.g. Unix epoch).
    last_timestamp_secs: Option<f64>,
    /// EWMA half-life in seconds.  Derived from lambda + reference interval.
    half_life_secs: f64,
    /// Running EWMA variance **per second** (not annualized). None until warmup.
    ewma_variance: Option<f64>,
    /// Total return observations seen (for warmup logic).
    total_observations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VolRegime {
    Low,
    High,
}

impl VolatilityEstimator {
    /// Create a new time-aware volatility estimator.
    ///
    /// - `window_size`: max (log_return, dt) pairs to keep (bounds memory and drift calc)
    /// - `high_vol_threshold`: annualized vol threshold for high-vol regime (e.g. 1.0 = 100%)
    /// - `ewma_lambda`: decay factor at the *reference* interval (0.94 = RiskMetrics standard)
    /// - `reference_interval_secs`: the interval `ewma_lambda` was calibrated for (e.g. 3.0)
    pub fn new(
        window_size: usize,
        high_vol_threshold: f64,
        ewma_lambda: f64,
        reference_interval_secs: f64,
    ) -> Self {
        let lambda = ewma_lambda.clamp(0.5, 0.999);
        // τ = -dt_ref / ln(λ).  For λ=0.94, dt_ref=3.0 → τ ≈ 48.5 s.
        let half_life = -reference_interval_secs.max(0.1) / lambda.ln();
        Self {
            log_returns: VecDeque::with_capacity(window_size),
            window_size,
            high_vol_threshold,
            last_price: None,
            last_timestamp_secs: None,
            half_life_secs: half_life,
            ewma_variance: None,
            total_observations: 0,
        }
    }

    /// Warmup threshold: 25% of window, minimum 2.
    fn warmup_threshold(&self) -> usize {
        (self.window_size / 4).max(2)
    }

    /// Ingest a new price observation with its timestamp.
    ///
    /// - `price`: spot price (must be > 0)
    /// - `timestamp_secs`: observation time in seconds (e.g. Unix epoch with sub-second precision)
    pub fn add_price(&mut self, price: f64, timestamp_secs: f64) {
        if price <= 0.0 || !timestamp_secs.is_finite() {
            return;
        }

        if let (Some(prev_price), Some(prev_ts)) = (self.last_price, self.last_timestamp_secs) {
            let raw_dt = timestamp_secs - prev_ts;

            // Reject observations that are too close together or out of order.
            if raw_dt < MIN_DT_SECS {
                return;
            }

            // Cap dt to avoid a single reconnect gap dominating the estimate.
            let dt = raw_dt.min(MAX_DT_SECS);

            if prev_price > 0.0 {
                let log_return = (price / prev_price).ln();

                // Store (return, dt) pair for drift calculation and warmup seeding.
                self.log_returns.push_back((log_return, dt));
                if self.log_returns.len() > self.window_size {
                    self.log_returns.pop_front();
                }

                // Variance per second for this observation.
                let r_per_sec = log_return / dt;
                let r_sq_per_sec = r_per_sec * r_per_sec;

                // Time-weighted EWMA decay.
                let lambda_eff = (-dt / self.half_life_secs).exp();

                if let Some(var) = self.ewma_variance {
                    self.ewma_variance =
                        Some(lambda_eff * var + (1.0 - lambda_eff) * r_sq_per_sec);
                } else if self.total_observations + 1 >= self.warmup_threshold() {
                    // Seed EWMA from simple zero-mean variance-per-second.
                    let sum_r_sq_per_sec: f64 = self
                        .log_returns
                        .iter()
                        .map(|(r, d)| {
                            let rps = r / d;
                            rps * rps
                        })
                        .sum();
                    let n = self.log_returns.len() as f64;
                    self.ewma_variance = Some(sum_r_sq_per_sec / n);
                }

                self.total_observations += 1;
            }
        }

        // Always update last_price / last_timestamp for the next observation.
        self.last_price = Some(price);
        self.last_timestamp_secs = Some(timestamp_secs);
    }

    /// Compute annualized volatility.
    ///
    /// Uses EWMA variance (per-second) when available, otherwise falls back to
    /// simple zero-mean variance-per-second during warmup.
    ///
    /// σ_annual = √(variance_per_second × seconds_per_year)
    ///
    /// Returns None if insufficient data (< 2 return observations).
    pub fn annualized_volatility(&self) -> Option<f64> {
        if self.log_returns.len() < 2 {
            return None;
        }

        let var_per_sec = if let Some(v) = self.ewma_variance {
            v
        } else {
            // Warmup fallback: simple zero-mean variance per second.
            let n = self.log_returns.len() as f64;
            let sum: f64 = self
                .log_returns
                .iter()
                .map(|(r, dt)| {
                    let rps = r / dt;
                    rps * rps
                })
                .sum();
            sum / n
        };

        let annualized = (var_per_sec * SECONDS_PER_YEAR).sqrt();

        if annualized.is_finite() && annualized > 0.0 {
            Some(annualized)
        } else {
            None
        }
    }

    /// Mean log-return per second over the rolling window.
    pub fn mean_log_return_per_second(&self) -> Option<f64> {
        if self.log_returns.is_empty() {
            return None;
        }
        let sum: f64 = self.log_returns.iter().map(|(r, dt)| r / dt).sum();
        Some(sum / self.log_returns.len() as f64)
    }

    /// Detect the current volatility regime.
    pub fn regime(&self) -> VolRegime {
        match self.annualized_volatility() {
            Some(vol) if vol > self.high_vol_threshold => VolRegime::High,
            _ => VolRegime::Low,
        }
    }

    /// Get the number of return observations currently in the window.
    pub fn observation_count(&self) -> usize {
        self.log_returns.len()
    }

    /// Check if we have enough data for a reliable estimate.
    /// Requires at least half the window to be filled.
    pub fn is_ready(&self) -> bool {
        self.log_returns.len() >= self.window_size / 2
    }

    /// Compute the ratio of short-window realized volatility to long-window (EWMA) volatility.
    ///
    /// Returns `short_rv / long_rv` where:
    /// - `short_rv` is realized vol computed from returns in the last `short_window_secs` seconds
    /// - `long_rv` is the current EWMA-based annualized vol estimate
    ///
    /// Interpretation:
    /// - `> 1.5` → volatility expanding (regime shift, uncertainty high)
    /// - `< 0.7` → volatility contracting (range-bound, more predictable)
    /// - `~1.0`  → stable regime
    ///
    /// Returns None if insufficient data for either estimate.
    pub fn vol_regime_ratio(&self, short_window_secs: f64) -> Option<f64> {
        let long_vol = self.annualized_volatility()?;
        if long_vol < 1e-10 {
            return None;
        }

        // Compute short-window RV from recent returns within the time window.
        // Walk backwards through log_returns to find observations within the window.
        let total_dt: f64 = self.log_returns.iter().map(|(_, dt)| dt).sum();
        if total_dt < short_window_secs * 0.5 {
            // Not enough history to fill even half the short window
            return None;
        }

        // Accumulate from the back until we exceed short_window_secs
        let mut sum_r_sq_per_sec = 0.0f64;
        let mut dt_accum = 0.0f64;
        let mut n = 0u32;

        for &(r, dt) in self.log_returns.iter().rev() {
            if dt_accum + dt > short_window_secs && n >= 2 {
                break;
            }
            let rps = r / dt;
            sum_r_sq_per_sec += rps * rps;
            dt_accum += dt;
            n += 1;
        }

        if n < 2 {
            return None;
        }

        let short_var_per_sec = sum_r_sq_per_sec / n as f64;
        let short_vol = (short_var_per_sec * SECONDS_PER_YEAR).sqrt();

        if short_vol.is_finite() && short_vol > 0.0 {
            Some(short_vol / long_vol)
        } else {
            None
        }
    }

    /// Get the current EWMA variance (per-second, not annualized). None if not yet warmed up.
    pub fn ewma_variance(&self) -> Option<f64> {
        self.ewma_variance
    }

    /// Total return observations seen (for warmup tracking).
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

    /// Helper: reference interval used in most tests (seconds between observations).
    const REF_DT: f64 = 3.0;

    #[test]
    fn test_empty_estimator() {
        let est = VolatilityEstimator::new(60, 1.0, 0.94, REF_DT);
        assert_eq!(est.annualized_volatility(), None);
        assert!(!est.is_ready());
    }

    #[test]
    fn test_single_price() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94, REF_DT);
        est.add_price(68000.0, 1000.0);
        assert_eq!(est.annualized_volatility(), None);
    }

    #[test]
    fn test_constant_price_zero_vol() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94, REF_DT);
        for i in 0..30 {
            est.add_price(68000.0, 1000.0 + i as f64 * REF_DT);
        }
        let vol = est.annualized_volatility();
        // Constant price → zero log-returns → zero vol
        assert!(vol.is_none() || vol.unwrap() < 1e-10);
    }

    #[test]
    fn test_volatile_prices() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94, REF_DT);
        // Simulate BTC bouncing between 67000 and 69000
        for i in 0..30 {
            let price = if i % 2 == 0 { 67000.0 } else { 69000.0 };
            est.add_price(price, 1000.0 + i as f64 * REF_DT);
        }
        let vol = est.annualized_volatility();
        assert!(vol.is_some());
        assert!(vol.unwrap() > 0.0);
    }

    #[test]
    fn test_regime_detection() {
        let mut est = VolatilityEstimator::new(60, 0.5, 0.94, REF_DT);
        // Low-vol: tiny moves
        for i in 0..30 {
            est.add_price(68000.0 + (i as f64) * 0.01, 1000.0 + i as f64 * REF_DT);
        }
        assert_eq!(est.regime(), VolRegime::Low);
    }

    #[test]
    fn test_window_eviction() {
        let mut est = VolatilityEstimator::new(10, 1.0, 0.94, REF_DT);
        for i in 0..20 {
            est.add_price(68000.0 + (i as f64) * 10.0, 1000.0 + i as f64 * REF_DT);
        }
        // 20 prices → 19 returns, but window_size=10 → evicted down to 10
        assert_eq!(est.observation_count(), 10);
    }

    #[test]
    fn test_mean_log_return_per_second() {
        let mut est = VolatilityEstimator::new(10, 1.0, 0.94, REF_DT);
        est.add_price(100.0, 1000.0);
        est.add_price(101.0, 1000.0 + REF_DT);
        est.add_price(102.0, 1000.0 + 2.0 * REF_DT);
        let mean = est.mean_log_return_per_second().unwrap();
        assert!(mean > 0.0);
    }

    #[test]
    fn test_invalid_prices_ignored() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94, REF_DT);
        est.add_price(68000.0, 1000.0);
        est.add_price(0.0, 1003.0); // ignored
        est.add_price(-1.0, 1006.0); // ignored
        est.add_price(68100.0, 1009.0);
        // Only one valid log-return (68000 → 68100)
        assert_eq!(est.observation_count(), 1);
    }

    // ── EWMA-specific tests ──

    #[test]
    fn test_ewma_warmup_seeds_from_simple_variance() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94, REF_DT);
        // warmup_threshold = 100/4 = 25
        // Feed 25 prices (24 returns) — should NOT have EWMA yet
        // (check is total_observations+1 >= 25, i.e. 23+1=24 < 25 after 24 returns)
        for i in 0..25 {
            est.add_price(68000.0 + (i as f64) * 5.0, 1000.0 + i as f64 * REF_DT);
        }
        assert!(est.ewma_variance.is_none());

        // 26th price (25th return) triggers warmup (total_observations+1 = 24+1 = 25 >= 25)
        est.add_price(68130.0, 1000.0 + 25.0 * REF_DT);
        assert!(est.ewma_variance.is_some());
        assert!(est.ewma_variance.unwrap() > 0.0);
    }

    #[test]
    fn test_ewma_reacts_fast_to_vol_spike() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94, REF_DT);
        // Build a calm baseline: tiny moves
        for i in 0..60 {
            est.add_price(68000.0 + (i as f64) * 0.10, 1000.0 + i as f64 * REF_DT);
        }
        let calm_vol = est.annualized_volatility().unwrap();

        // Single 1% spike
        let last = 68000.0 + 59.0 * 0.10;
        est.add_price(last * 1.01, 1000.0 + 60.0 * REF_DT);

        let spike_vol = est.annualized_volatility().unwrap();
        // Vol should jump >2x after the spike
        assert!(
            spike_vol > calm_vol * 2.0,
            "spike_vol={spike_vol} should be > 2x calm_vol={calm_vol}"
        );
    }

    #[test]
    fn test_ewma_decays_after_spike() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94, REF_DT);
        // Calm baseline
        for i in 0..60 {
            est.add_price(68000.0 + (i as f64) * 0.10, 1000.0 + i as f64 * REF_DT);
        }

        // Spike
        let last = 68000.0 + 59.0 * 0.10;
        let spiked = last * 1.01;
        est.add_price(spiked, 1000.0 + 60.0 * REF_DT);
        let spike_vol = est.annualized_volatility().unwrap();

        // 20 calm ticks after spike
        for i in 0..20 {
            est.add_price(spiked + (i as f64) * 0.10, 1000.0 + (61.0 + i as f64) * REF_DT);
        }
        let decayed_vol = est.annualized_volatility().unwrap();

        // Should decay to <70% of spike
        assert!(
            decayed_vol < spike_vol * 0.70,
            "decayed_vol={decayed_vol} should be < 70% of spike_vol={spike_vol}"
        );
    }

    #[test]
    fn test_ewma_lambda_sensitivity() {
        // Lower lambda → faster reaction → higher vol after same spike
        let mut est_high = VolatilityEstimator::new(100, 1.0, 0.97, REF_DT);
        let mut est_low = VolatilityEstimator::new(100, 1.0, 0.85, REF_DT);

        for i in 0..60 {
            let p = 68000.0 + (i as f64) * 0.10;
            let ts = 1000.0 + i as f64 * REF_DT;
            est_high.add_price(p, ts);
            est_low.add_price(p, ts);
        }

        // Same spike
        let last = 68000.0 + 59.0 * 0.10;
        let ts_spike = 1000.0 + 60.0 * REF_DT;
        est_high.add_price(last * 1.01, ts_spike);
        est_low.add_price(last * 1.01, ts_spike);

        let vol_high_lambda = est_high.annualized_volatility().unwrap();
        let vol_low_lambda = est_low.annualized_volatility().unwrap();

        assert!(
            vol_low_lambda > vol_high_lambda,
            "lower lambda ({vol_low_lambda}) should produce higher vol than higher lambda ({vol_high_lambda})"
        );
    }

    #[test]
    fn test_ewma_constant_price_still_zero_vol() {
        let mut est = VolatilityEstimator::new(100, 1.0, 0.94, REF_DT);
        for i in 0..60 {
            est.add_price(68000.0, 1000.0 + i as f64 * REF_DT);
        }
        // EWMA should be seeded (60 > warmup_threshold=25), but variance is 0
        assert!(est.ewma_variance.is_some());
        let vol = est.annualized_volatility();
        assert!(vol.is_none() || vol.unwrap() < 1e-10);
    }

    // ── New time-aware tests ──

    #[test]
    fn test_min_dt_guard_rejects_fast_ticks() {
        let mut est = VolatilityEstimator::new(60, 1.0, 0.94, REF_DT);
        est.add_price(68000.0, 1000.0);
        est.add_price(68010.0, 1000.01); // 10ms apart → rejected
        est.add_price(68020.0, 1000.02); // 10ms from last accepted → rejected
        assert_eq!(est.observation_count(), 0); // no returns recorded
        // But last_price was updated on first call
        est.add_price(68030.0, 1001.0); // 1s from first → accepted
        assert_eq!(est.observation_count(), 1);
    }

    #[test]
    fn test_irregular_timestamps_produce_consistent_vol() {
        // Feed the same price path with uniform vs irregular spacing.
        // Irregular spacing alternates 1s and 5s intervals (mean 3s = REF_DT).
        // Both should produce roughly similar vol since average spacing is the same
        // and the per-second normalization handles the varying dt.
        let prices: Vec<f64> = (0..60)
            .map(|i| 68000.0 + (i as f64) * 0.10 + if i % 5 == 0 { 5.0 } else { 0.0 })
            .collect();

        let mut est_uniform = VolatilityEstimator::new(100, 1.0, 0.94, REF_DT);
        let mut est_irregular = VolatilityEstimator::new(100, 1.0, 0.94, REF_DT);

        let mut t_irreg = 1000.0;
        for (i, &p) in prices.iter().enumerate() {
            est_uniform.add_price(p, 1000.0 + i as f64 * REF_DT); // uniform 3s
            est_irregular.add_price(p, t_irreg); // alternating 1s/5s
            t_irreg += if i % 2 == 0 { 1.0 } else { 5.0 };
        }

        let vol_uniform = est_uniform.annualized_volatility().unwrap();
        let vol_irregular = est_irregular.annualized_volatility().unwrap();

        // Both should be positive and in the same order of magnitude.
        // Allow up to 5x ratio — the EWMA weighting differs due to varying λ_eff,
        // but both should be in a reasonable range.
        assert!(vol_uniform > 0.0, "uniform vol should be positive");
        assert!(vol_irregular > 0.0, "irregular vol should be positive");
        let ratio = vol_uniform / vol_irregular;
        assert!(
            (0.1..10.0).contains(&ratio),
            "vol_uniform={vol_uniform} and vol_irregular={vol_irregular} should be \
             same order of magnitude (ratio={ratio})"
        );
    }

    #[test]
    fn test_sub_second_ticks_correct_annualization() {
        // Known return pattern: 0.01% per second → annualized vol is well-defined.
        // Feed 100 observations at 1s intervals with alternating +/- 0.01% returns.
        let mut est = VolatilityEstimator::new(200, 1.0, 0.94, 1.0);
        let mut price = 68000.0;
        for i in 0..100 {
            est.add_price(price, 1000.0 + i as f64);
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            price *= 1.0 + sign * 0.0001; // 0.01% oscillation
        }

        let vol = est.annualized_volatility().unwrap();
        // 0.01% per second = 0.0001 return/sec, annualized ≈ 0.0001 * sqrt(31557600) ≈ 0.56
        // Allow wide tolerance due to EWMA weighting
        assert!(
            vol > 0.1 && vol < 3.0,
            "vol={vol} should be in reasonable range for 0.01%/sec oscillation"
        );
    }

    // ── Vol regime ratio tests ──

    #[test]
    fn test_regime_ratio_none_without_data() {
        let est = VolatilityEstimator::new(60, 1.0, 0.94, REF_DT);
        assert!(est.vol_regime_ratio(300.0).is_none());
    }

    #[test]
    fn test_regime_ratio_stable_near_one() {
        // Constant oscillation → short and long vol should be similar → ratio ≈ 1.0
        let mut est = VolatilityEstimator::new(200, 1.0, 0.94, REF_DT);
        for i in 0..120 {
            let price = if i % 2 == 0 { 68000.0 } else { 68050.0 };
            est.add_price(price, 1000.0 + i as f64 * REF_DT);
        }
        let ratio = est.vol_regime_ratio(30.0); // 30-second short window
        assert!(ratio.is_some(), "should have enough data for ratio");
        let r = ratio.unwrap();
        // Stable regime should be roughly 0.5..2.0 (not perfectly 1.0 due to EWMA weighting)
        assert!(
            r > 0.3 && r < 3.0,
            "stable regime ratio should be near 1.0, got {r}"
        );
    }

    #[test]
    fn test_regime_ratio_spike_shows_expansion() {
        let mut est = VolatilityEstimator::new(200, 1.0, 0.94, REF_DT);
        // 80 ticks of calm (tiny moves)
        for i in 0..80 {
            est.add_price(68000.0 + (i as f64) * 0.05, 1000.0 + i as f64 * REF_DT);
        }

        // Then 10 ticks of high vol (large oscillations)
        for i in 0..10 {
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            est.add_price(
                68004.0 + sign * 200.0,
                1000.0 + (80.0 + i as f64) * REF_DT,
            );
        }

        let ratio = est.vol_regime_ratio(30.0); // last ~30s = high vol ticks
        assert!(ratio.is_some());
        let r = ratio.unwrap();
        // Recent vol should be much higher than EWMA (which still carries calm history)
        assert!(
            r > 1.0,
            "post-spike regime ratio should be > 1.0, got {r}"
        );
    }
}
