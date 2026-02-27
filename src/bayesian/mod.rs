pub mod calibration;
pub mod models;
pub mod volatility;

use rust_decimal::prelude::ToPrimitive;

use crate::market_scanner::TargetMarket;
use crate::price_feeds::{AggregatedPrice, FeedSource};

pub use calibration::CalibrationTracker;
pub use volatility::VolatilityEstimator;

/// Seconds per year (365.25 days × 24h × 3600s).
const SECONDS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;

#[derive(Debug, thiserror::Error)]
pub enum BayesianError {
    #[error("invalid probability: {0}")]
    InvalidProbability(f64),
    #[error("no price data available for update")]
    NoPriceData,
    #[error("volatility estimate unavailable")]
    NoVolatility,
    #[error("missing official Polymarket price-to-beat for up/down market")]
    MissingOfficialReference,
}

/// Full posterior computation details for one market evaluation.
#[derive(Debug, Clone)]
pub struct PosteriorComputation {
    pub spot: f64,
    pub strike: f64,
    pub reference_spot: Option<f64>,
    pub time_to_expiry_secs: f64,
    pub volatility: f64,
    pub model_posterior: f64,
    pub market_implied: f64,
    pub confidence: f64,
    pub adjusted_posterior: f64,
    pub model_type: &'static str,
}

/// Bayesian updater for binary market probability estimation.
///
/// Uses a GBM-based model to estimate P(spot > strike at T) for strike markets,
/// and a directional forecast model for Up/Down markets.
/// Applies confidence-based shrinkage toward market-implied probability.
#[derive(Clone)]
pub struct BayesianUpdater {
    /// Volatility estimator (time-aware EWMA, stores variance per-second)
    vol_estimator: VolatilityEstimator,
    /// Default annualized volatility when estimator is not ready
    default_volatility: f64,
}

impl BayesianUpdater {
    /// Create a new Bayesian updater.
    ///
    /// - `vol_window`: number of price observations for vol estimation
    /// - `high_vol_threshold`: annualized vol threshold for regime detection
    /// - `lambda_reference_interval_secs`: the interval `ewma_lambda` was calibrated for
    ///   (e.g. 3.0 if lambda=0.94 was intended for 3-second observation intervals)
    /// - `default_volatility`: fallback annualized vol (e.g. 0.80 = 80%)
    /// - `ewma_lambda`: EWMA decay factor for volatility (0.94 = RiskMetrics standard)
    pub fn new(
        vol_window: usize,
        high_vol_threshold: f64,
        lambda_reference_interval_secs: f64,
        default_volatility: f64,
        ewma_lambda: f64,
    ) -> Self {
        Self {
            vol_estimator: VolatilityEstimator::new(
                vol_window,
                high_vol_threshold,
                ewma_lambda,
                lambda_reference_interval_secs,
            ),
            default_volatility,
        }
    }

    /// Feed a new aggregated price into the volatility estimator.
    ///
    /// Prefers the raw Binance observation (fastest, undampened by median) for vol
    /// estimation.  Falls back to the aggregated median if no Binance observation
    /// is present.  The aggregated median (`price.spot_usd`) is still used for the
    /// actual GBM d2 computation in `update_with_details`.
    pub fn feed_price(&mut self, price: &AggregatedPrice) {
        let (vol_price, ts_secs) = extract_binance_obs(price).unwrap_or_else(|| {
            let spot = price.spot_usd.to_f64().unwrap_or(0.0);
            let ts = price.timestamp.timestamp() as f64
                + price.timestamp.timestamp_subsec_millis() as f64 / 1000.0;
            (spot, ts)
        });

        if vol_price > 0.0 {
            self.vol_estimator.add_price(vol_price, ts_secs);
        }
    }

    /// Compute the posterior probability for a market given the latest price data.
    ///
    /// Returns the probability P(BTC > strike at expiry), adjusted for:
    /// 1. Current BTC price vs strike
    /// 2. Time to expiry
    /// 3. Regime-aware volatility
    /// 4. Price feed confidence (low confidence → shrink toward market-implied)
    pub fn update_with_details(
        &mut self,
        price: &AggregatedPrice,
        market: &TargetMarket,
    ) -> Result<PosteriorComputation, BayesianError> {
        let spot = price.spot_usd.to_f64().ok_or(BayesianError::NoPriceData)?;
        let strike = market
            .strike_price
            .to_f64()
            .ok_or(BayesianError::NoPriceData)?;
        let confidence = price.confidence.to_f64().unwrap_or(0.5).clamp(0.0, 1.0);
        let market_implied = market
            .current_yes_price
            .to_f64()
            .unwrap_or(0.5)
            .clamp(0.01, 0.99);

        if spot <= 0.0 {
            return Err(BayesianError::NoPriceData);
        }

        // Time to expiry in seconds — always remaining time from now, not total window duration.
        let now = chrono::Utc::now();
        let time_to_expiry = (market.end_time - now).num_seconds() as f64;
        if time_to_expiry <= 0.0 {
            // Market has expired; if spot > strike, probability is 1, else 0
            let adjusted = if strike > 0.0 {
                if spot > strike {
                    1.0
                } else {
                    0.0
                }
            } else if is_up_down_market(&market.question) {
                match up_down_reference_price(market) {
                    Some(reference_spot) => {
                        if spot >= reference_spot {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    None => return Err(BayesianError::MissingOfficialReference),
                }
            } else {
                return Err(BayesianError::NoPriceData);
            };
            return Ok(PosteriorComputation {
                spot,
                strike,
                reference_spot: None,
                time_to_expiry_secs: time_to_expiry,
                volatility: self.default_volatility,
                model_posterior: adjusted,
                market_implied: adjusted,
                confidence,
                adjusted_posterior: adjusted,
                model_type: if strike > 0.0 {
                    "strike_gbm"
                } else {
                    "updown_directional"
                },
            });
        }

        // Get volatility estimate (time-aware EWMA or default)
        let volatility = self
            .vol_estimator
            .annualized_volatility()
            .unwrap_or(self.default_volatility);

        if strike <= 0.0 {
            if !is_up_down_market(&market.question) {
                return Err(BayesianError::NoPriceData);
            }
            let reference_spot =
                up_down_reference_price(market).ok_or(BayesianError::MissingOfficialReference)?;
            let annualized_drift = self.current_annualized_log_drift();
            let model_posterior = models::directional_up_probability(
                spot,
                reference_spot,
                time_to_expiry,
                volatility,
                annualized_drift,
            );
            let adjusted_posterior = (confidence * model_posterior
                + (1.0 - confidence) * market_implied)
                .clamp(0.001, 0.999);
            return Ok(PosteriorComputation {
                spot,
                strike,
                reference_spot: Some(reference_spot),
                time_to_expiry_secs: time_to_expiry,
                volatility,
                model_posterior,
                market_implied,
                confidence,
                adjusted_posterior,
                model_type: "updown_directional",
            });
        }

        // Compute model posterior from GBM
        let model_posterior =
            models::binary_call_probability(spot, strike, time_to_expiry, volatility);

        let adjusted_posterior = confidence * model_posterior + (1.0 - confidence) * market_implied;

        // Clamp to valid probability range
        let adjusted_posterior = adjusted_posterior.clamp(0.001, 0.999);

        Ok(PosteriorComputation {
            spot,
            strike,
            reference_spot: None,
            time_to_expiry_secs: time_to_expiry,
            volatility,
            model_posterior,
            market_implied,
            confidence,
            adjusted_posterior,
            model_type: "strike_gbm",
        })
    }

    pub fn update(
        &mut self,
        price: &AggregatedPrice,
        market: &TargetMarket,
    ) -> Result<f64, BayesianError> {
        Ok(self.update_with_details(price, market)?.adjusted_posterior)
    }

    /// Get the current volatility regime.
    pub fn regime(&self) -> volatility::VolRegime {
        self.vol_estimator.regime()
    }

    /// Check if the volatility estimator has sufficient data.
    pub fn is_ready(&self) -> bool {
        self.vol_estimator.is_ready()
    }

    /// Get the current annualized volatility estimate.
    pub fn current_volatility(&self) -> f64 {
        self.vol_estimator
            .annualized_volatility()
            .unwrap_or(self.default_volatility)
    }

    /// Get the current EWMA variance (per-second, not annualized). None if not warmed up.
    pub fn ewma_variance(&self) -> Option<f64> {
        self.vol_estimator.ewma_variance()
    }

    /// Total observations fed into the volatility estimator.
    pub fn ewma_total_observations(&self) -> usize {
        self.vol_estimator.total_observations()
    }

    /// Whether the EWMA estimator has completed warmup.
    pub fn ewma_warmed_up(&self) -> bool {
        self.vol_estimator.is_warmed_up()
    }

    fn current_annualized_log_drift(&self) -> f64 {
        self.vol_estimator
            .mean_log_return_per_second()
            .map(|mu_per_sec| (mu_per_sec * SECONDS_PER_YEAR).clamp(-5.0, 5.0))
            .unwrap_or(0.0)
    }
}

/// Extract the Binance observation from an AggregatedPrice, if present.
/// Returns (price_f64, timestamp_secs_f64) for the highest-fidelity vol signal.
fn extract_binance_obs(price: &AggregatedPrice) -> Option<(f64, f64)> {
    price
        .observations
        .iter()
        .find(|obs| obs.source == FeedSource::Binance)
        .and_then(|obs| {
            let p = obs.price.to_f64()?;
            let ts = obs.timestamp.timestamp() as f64
                + obs.timestamp.timestamp_subsec_millis() as f64 / 1000.0;
            Some((p, ts))
        })
}

fn up_down_reference_price(market: &TargetMarket) -> Option<f64> {
    market.polymarket_price_to_beat.and_then(|v| v.to_f64())
}

fn is_up_down_market(question: &str) -> bool {
    let q = question.to_ascii_lowercase();
    q.contains("up or down") || q.contains("up/down")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;

    fn make_price(btc_usd: f64, confidence: f64) -> AggregatedPrice {
        AggregatedPrice {
            asset: crate::price_feeds::PriceAsset::Btc,
            spot_usd: Decimal::try_from(btc_usd).unwrap(),
            timestamp: Utc::now(),
            num_sources: 4,
            spread: Decimal::ZERO,
            confidence: Decimal::try_from(confidence).unwrap(),
            quorum_met: true,
            observations: Vec::new(),
        }
    }

    fn make_market(strike: f64, yes_price: f64, secs_to_expiry: i64) -> TargetMarket {
        TargetMarket {
            condition_id: "test".into(),
            token_id_yes: "yes_token".into(),
            token_id_no: "no_token".into(),
            question: format!("Will BTC be above ${}?", strike),
            strike_price: Decimal::try_from(strike).unwrap(),
            start_time: None,
            end_time: Utc::now() + chrono::Duration::seconds(secs_to_expiry),
            current_yes_price: Decimal::try_from(yes_price).unwrap(),
            current_no_price: Decimal::try_from(1.0 - yes_price).unwrap(),
            polymarket_price_to_beat: None,
            polymarket_price_to_beat_source: None,
            liquidity: Decimal::from(10000),
            volume: Decimal::from(50000),
        }
    }

    fn make_up_down_market(yes_price: f64, secs_to_expiry: i64) -> TargetMarket {
        TargetMarket {
            condition_id: "test".into(),
            token_id_yes: "yes_token".into(),
            token_id_no: "no_token".into(),
            question: "Ethereum Up or Down - test".into(),
            strike_price: Decimal::ZERO,
            start_time: None,
            end_time: Utc::now() + chrono::Duration::seconds(secs_to_expiry),
            current_yes_price: Decimal::try_from(yes_price).unwrap(),
            current_no_price: Decimal::try_from(1.0 - yes_price).unwrap(),
            polymarket_price_to_beat: Some(Decimal::new(100, 0)),
            polymarket_price_to_beat_source: Some("field:test".to_string()),
            liquidity: Decimal::from(10000),
            volume: Decimal::from(50000),
        }
    }

    fn make_price_at(spot_usd: f64, confidence: f64, timestamp: DateTime<Utc>) -> AggregatedPrice {
        AggregatedPrice {
            asset: crate::price_feeds::PriceAsset::Eth,
            spot_usd: Decimal::try_from(spot_usd).unwrap(),
            timestamp,
            num_sources: 4,
            spread: Decimal::ZERO,
            confidence: Decimal::try_from(confidence).unwrap(),
            quorum_met: true,
            observations: Vec::new(),
        }
    }

    #[test]
    fn test_update_atm_market() {
        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let price = make_price(68000.0, 1.0);
        let market = make_market(68000.0, 0.50, 300);

        let posterior = updater.update(&price, &market).unwrap();
        // ATM with 5 min to expiry → should be close to 0.5
        assert!((0.4..0.6).contains(&posterior));
    }

    #[test]
    fn test_update_itm_market() {
        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let price = make_price(72000.0, 1.0);
        let market = make_market(68000.0, 0.60, 300);

        let posterior = updater.update(&price, &market).unwrap();
        // BTC well above strike → high probability
        assert!(posterior > 0.8);
    }

    #[test]
    fn test_low_confidence_shrinks_to_market() {
        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let price = make_price(72000.0, 0.0); // Zero confidence!
        let market = make_market(68000.0, 0.60, 300);

        let posterior = updater.update(&price, &market).unwrap();
        // With zero confidence, should match market-implied price
        assert!((posterior - 0.60).abs() < 0.05);
    }

    #[test]
    fn test_expired_market() {
        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let price = make_price(72000.0, 1.0);
        let market = make_market(68000.0, 0.60, -10); // Already expired

        let posterior = updater.update(&price, &market).unwrap();
        // BTC > strike at expiry → 1.0
        assert_eq!(posterior, 1.0);
    }

    #[test]
    fn test_update_up_down_market_without_official_reference_rejected() {
        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let price = make_price(2800.0, 0.8);
        let mut market = make_up_down_market(0.70, 300);
        market.polymarket_price_to_beat = None;
        market.polymarket_price_to_beat_source = None;

        // No official Polymarket reference => reject.
        let err = updater.update_with_details(&price, &market).unwrap_err();
        assert!(matches!(err, BayesianError::MissingOfficialReference));
    }

    #[test]
    fn test_zero_strike_non_updown_market_rejected() {
        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let price = make_price(68000.0, 1.0);
        let mut market = make_market(68000.0, 0.60, 300);
        market.strike_price = Decimal::ZERO;
        market.question = "Will BTC move soon?".into();
        let err = updater.update_with_details(&price, &market).unwrap_err();
        assert!(matches!(err, BayesianError::NoPriceData));
    }

    #[test]
    fn test_up_down_directional_model_uses_official_reference() {
        let mut updater = BayesianUpdater::new(120, 1.0, 2.0, 0.80, 0.94);
        let now = Utc::now();
        for i in 0..90 {
            let ts = now - chrono::Duration::seconds(180 - (i as i64 * 2));
            let px = 100.0 + (i as f64) * 0.10;
            updater.feed_price(&make_price_at(px, 1.0, ts));
        }

        let mut market = make_up_down_market(0.50, 300);
        market.polymarket_price_to_beat = Some(Decimal::new(108, 0));
        let live = make_price_at(109.0, 0.9, now);

        let details = updater.update_with_details(&live, &market).unwrap();
        assert!(details.model_posterior > 0.5);
        assert_eq!(details.reference_spot, Some(108.0));
    }

    #[test]
    fn test_up_down_directional_model_marks_below_reference_as_lower_prob() {
        let mut updater = BayesianUpdater::new(120, 1.0, 2.0, 0.80, 0.94);
        let now = Utc::now();
        for i in 0..90 {
            let ts = now - chrono::Duration::seconds(180 - (i as i64 * 2));
            let px = 120.0 - (i as f64) * 0.10;
            updater.feed_price(&make_price_at(px, 1.0, ts));
        }

        let mut market = make_up_down_market(0.50, 300);
        market.polymarket_price_to_beat = Some(Decimal::new(115, 0));
        let live = make_price_at(111.0, 0.9, now);

        let details = updater.update_with_details(&live, &market).unwrap();
        assert!(details.model_posterior < 0.5);
        assert_eq!(details.reference_spot, Some(115.0));
    }

    #[test]
    fn test_feed_price_extracts_binance_observation() {
        use crate::price_feeds::{PriceAsset, PriceObservation};

        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let now = Utc::now();

        // First price with Binance observation
        let price1 = AggregatedPrice {
            asset: PriceAsset::Btc,
            spot_usd: Decimal::from(68000), // median
            timestamp: now,
            num_sources: 2,
            spread: Decimal::ZERO,
            confidence: Decimal::ONE,
            quorum_met: true,
            observations: vec![PriceObservation {
                asset: PriceAsset::Btc,
                source: FeedSource::Binance,
                price: Decimal::from(68050), // Binance is different from median
                timestamp: now,
                bid: None,
                ask: None,
            }],
        };
        updater.feed_price(&price1);

        // Second price 2 seconds later
        let later = now + chrono::Duration::seconds(2);
        let price2 = AggregatedPrice {
            asset: PriceAsset::Btc,
            spot_usd: Decimal::from(68100), // median
            timestamp: later,
            num_sources: 2,
            spread: Decimal::ZERO,
            confidence: Decimal::ONE,
            quorum_met: true,
            observations: vec![PriceObservation {
                asset: PriceAsset::Btc,
                source: FeedSource::Binance,
                price: Decimal::from(68150), // Binance raw
                timestamp: later,
                bid: None,
                ask: None,
            }],
        };
        updater.feed_price(&price2);

        // Should have 1 return observation from Binance prices (68050 → 68150)
        assert_eq!(updater.ewma_total_observations(), 1);
    }

    #[test]
    fn test_feed_price_falls_back_to_median_without_binance() {
        let mut updater = BayesianUpdater::new(60, 1.0, 2.0, 0.80, 0.94);
        let now = Utc::now();

        // Feed prices with no Binance observation (observations: Vec::new())
        let price1 = AggregatedPrice {
            asset: crate::price_feeds::PriceAsset::Btc,
            spot_usd: Decimal::from(68000),
            timestamp: now,
            num_sources: 2,
            spread: Decimal::ZERO,
            confidence: Decimal::ONE,
            quorum_met: true,
            observations: Vec::new(),
        };
        updater.feed_price(&price1);

        let later = now + chrono::Duration::seconds(3);
        let price2 = AggregatedPrice {
            asset: crate::price_feeds::PriceAsset::Btc,
            spot_usd: Decimal::from(68100),
            timestamp: later,
            num_sources: 2,
            spread: Decimal::ZERO,
            confidence: Decimal::ONE,
            quorum_met: true,
            observations: Vec::new(),
        };
        updater.feed_price(&price2);

        // Should still work using the median spot_usd + timestamp
        assert_eq!(updater.ewma_total_observations(), 1);
    }
}
