use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{debug, warn};

use super::{AggregatedPrice, FeedSource, PriceAsset, PriceObservation};

/// Aggregates price observations from multiple feeds into a single canonical price.
///
/// Design:
/// - Chainlink is the conditional PRIMARY feed (resolution-aligned) for BTC
/// - Other feeds serve as cross-checks
/// - Anomaly detection: reject feeds deviating > threshold from median
/// - Confidence: based on agreement between sources
pub struct PriceAggregator {
    staleness_threshold: Duration,
    coingecko_staleness_threshold: Duration,
    anomaly_deviation_pct: f64,
    min_sources_per_asset: usize,
    latest: HashMap<(FeedSource, PriceAsset), PriceObservation>,
}

impl PriceAggregator {
    pub fn new(
        staleness_threshold: Duration,
        coingecko_staleness_threshold: Duration,
        anomaly_deviation_pct: Decimal,
        min_sources_per_asset: usize,
    ) -> Self {
        Self {
            staleness_threshold,
            coingecko_staleness_threshold,
            anomaly_deviation_pct: anomaly_deviation_pct.to_f64().unwrap_or(2.0),
            min_sources_per_asset: min_sources_per_asset.max(1),
            latest: HashMap::new(),
        }
    }

    /// Ingest a new observation. Returns an aggregated price if enough
    /// non-stale, non-anomalous sources are available.
    pub fn update(&mut self, obs: PriceObservation) -> Option<AggregatedPrice> {
        let key = (obs.source, obs.asset);
        self.latest.insert(key, obs);
        self.aggregate_for_asset(key.1)
    }

    /// Compute the aggregated price.
    ///
    /// Algorithm:
    /// 1. Discard observations with non-positive prices or older than staleness_threshold
    /// 2. Iterative median filtering: compute median, reject outliers, recompute from survivors
    /// 3. If Chainlink is available and valid, use it as BTC primary price
    /// 4. Confidence = based on agreement and number of sources (scaled to min_sources quorum)
    /// 5. Timestamp = freshest observation timestamp (not wall clock)
    fn aggregate_for_asset(&self, asset: PriceAsset) -> Option<AggregatedPrice> {
        let now = Utc::now();

        // Issue 6: Filter non-positive prices alongside staleness
        let fresh: Vec<&PriceObservation> = self
            .latest
            .values()
            .filter(|obs| obs.asset == asset)
            .filter(|obs| obs.price > Decimal::ZERO)
            .filter(|obs| {
                let age = now - obs.timestamp;
                // Issue 7: expect() instead of unwrap_or_default() — staleness thresholds must be valid
                age < chrono::Duration::from_std(self.staleness_for_source(obs.source))
                    .expect("staleness threshold must be non-negative")
            })
            .collect();

        if fresh.is_empty() {
            return None;
        }

        // Issue 2: Iterative median filtering — two passes to handle correlated feed failures.
        // Pass 1: compute median from all fresh, reject outliers.
        // Pass 2: recompute median from survivors, reject again.
        let threshold_pct = Decimal::try_from(self.anomaly_deviation_pct).unwrap_or(Decimal::TWO);

        let mut candidates: Vec<&PriceObservation> = fresh;

        for pass in 0..2 {
            let mut prices: Vec<Decimal> = candidates.iter().map(|obs| obs.price).collect();
            prices.sort();

            let median = if prices.len() % 2 == 0 {
                let mid = prices.len() / 2;
                (prices[mid - 1] + prices[mid]) / Decimal::TWO
            } else {
                prices[prices.len() / 2]
            };

            let deviation_threshold = median * threshold_pct / Decimal::from(100);

            let before_count = candidates.len();
            candidates.retain(|obs| {
                let deviation = (obs.price - median).abs();
                if deviation > deviation_threshold {
                    warn!(
                        source = %obs.source,
                        price = %obs.price,
                        median = %median,
                        deviation = %deviation,
                        pass,
                        "anomalous price feed rejected"
                    );
                    false
                } else {
                    true
                }
            });

            // If nothing was filtered this pass, or all were filtered, stop
            if candidates.len() == before_count || candidates.is_empty() {
                break;
            }
        }

        let valid = candidates;

        if valid.len() < self.min_sources_per_asset {
            debug!(
                asset = %asset,
                valid_sources = valid.len(),
                required_sources = self.min_sources_per_asset,
                "insufficient valid feed quorum for aggregation"
            );
            return None;
        }

        // Compute final median from the clean set (Issue 4 resolved — single consistent set)
        let mut valid_prices: Vec<Decimal> = valid.iter().map(|obs| obs.price).collect();
        valid_prices.sort();
        let valid_median = if valid_prices.len() % 2 == 0 {
            let mid = valid_prices.len() / 2;
            (valid_prices[mid - 1] + valid_prices[mid]) / Decimal::TWO
        } else {
            valid_prices[valid_prices.len() / 2]
        };

        // Use median for all assets uniformly. When continuous Chainlink polling is enabled,
        // Chainlink still contributes to the median. When off, no Chainlink observations arrive.
        let spot_usd = valid_median;

        // Spread = max - min across valid sources
        let min_price = valid.iter().map(|o| o.price).min().unwrap_or(spot_usd);
        let max_price = valid.iter().map(|o| o.price).max().unwrap_or(spot_usd);
        let spread = max_price - min_price;

        // Issue 3: Confidence scaled to min_sources quorum, gentler spread multiplier
        let confidence = if valid_median > Decimal::ZERO {
            let spread_pct = spread / valid_median;
            // Reduced multiplier: 1% spread → 75% agreement (was 50%)
            let agreement = (Decimal::ONE - spread_pct * Decimal::from(25))
                .max(Decimal::ZERO)
                .min(Decimal::ONE);
            // Scale count_factor relative to min_sources_per_asset (meeting quorum = full score)
            let count_factor = (Decimal::from(valid.len() as i64)
                / Decimal::from(self.min_sources_per_asset as i64))
            .min(Decimal::ONE);
            (agreement * count_factor)
                .max(Decimal::ZERO)
                .min(Decimal::ONE)
        } else {
            Decimal::ZERO
        };

        let observations: Vec<PriceObservation> = valid.into_iter().cloned().collect();
        let num_sources = observations.len();

        // Issue 1: Use freshest observation timestamp instead of Utc::now()
        let freshest_timestamp = observations
            .iter()
            .map(|o| o.timestamp)
            .max()
            .unwrap_or(now);

        debug!(
            asset = %asset,
            %spot_usd,
            num_sources,
            %spread,
            %confidence,
            quorum_required = self.min_sources_per_asset,
            quorum_met = num_sources >= self.min_sources_per_asset,
            "price aggregated"
        );

        Some(AggregatedPrice {
            asset,
            spot_usd,
            timestamp: freshest_timestamp,
            num_sources,
            spread,
            confidence,
            quorum_met: num_sources >= self.min_sources_per_asset,
            observations,
        })
    }

    fn staleness_for_source(&self, source: FeedSource) -> Duration {
        match source {
            FeedSource::CoinGecko => self.coingecko_staleness_threshold,
            _ => self.staleness_threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn obs(asset: PriceAsset, source: FeedSource, price: f64) -> PriceObservation {
        PriceObservation {
            asset,
            source,
            price: Decimal::try_from(price).unwrap(),
            timestamp: Utc::now(),
            bid: None,
            ask: None,
        }
    }

    #[test]
    fn test_single_feed() {
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );
        let result = agg.update(obs(PriceAsset::Btc, FeedSource::Chainlink, 68000.0));
        assert!(result.is_some());
        let p = result.unwrap();
        assert_eq!(p.spot_usd, Decimal::try_from(68000.0).unwrap());
        assert_eq!(p.asset, PriceAsset::Btc);
        assert_eq!(p.num_sources, 1);
        assert!(p.quorum_met);
    }

    #[test]
    fn test_btc_uses_median_uniformly() {
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );
        agg.update(obs(PriceAsset::Btc, FeedSource::Binance, 68010.0));
        agg.update(obs(PriceAsset::Btc, FeedSource::Coinbase, 67990.0));
        let result = agg.update(obs(PriceAsset::Btc, FeedSource::Chainlink, 68005.0));

        let p = result.unwrap();
        // All assets use median uniformly now
        assert_eq!(p.spot_usd, Decimal::try_from(68005.0).unwrap()); // median of 3
        assert_eq!(p.num_sources, 3);
    }

    #[test]
    fn test_anomaly_rejection() {
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );
        agg.update(obs(PriceAsset::Btc, FeedSource::Chainlink, 68000.0));
        agg.update(obs(PriceAsset::Btc, FeedSource::Binance, 68010.0));
        agg.update(obs(PriceAsset::Btc, FeedSource::Coinbase, 68005.0));
        // CoinGecko reports wildly different price (> 2% deviation)
        let result = agg.update(obs(PriceAsset::Btc, FeedSource::CoinGecko, 72000.0));

        let p = result.unwrap();
        // Anomalous CoinGecko should be rejected
        assert!(p.num_sources < 4);
    }

    #[test]
    fn test_confidence_high_agreement() {
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );
        agg.update(obs(PriceAsset::Btc, FeedSource::Chainlink, 68000.0));
        agg.update(obs(PriceAsset::Btc, FeedSource::Binance, 68001.0));
        agg.update(obs(PriceAsset::Btc, FeedSource::Coinbase, 67999.0));
        let result = agg.update(obs(PriceAsset::Btc, FeedSource::CryptoCompare, 68002.0));

        let p = result.unwrap();
        // Very close prices → high confidence
        assert!(p.confidence > Decimal::new(9, 1)); // > 0.9
    }

    #[test]
    fn test_empty_aggregator() {
        let agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );
        // No observations → None
        assert!(agg.aggregate_for_asset(PriceAsset::Btc).is_none());
    }

    #[test]
    fn test_quorum_requires_two_sources() {
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            2,
        );
        assert!(agg
            .update(obs(PriceAsset::Eth, FeedSource::Coinbase, 2800.0))
            .is_none());
        let p = agg
            .update(obs(PriceAsset::Eth, FeedSource::CoinGecko, 2801.0))
            .unwrap();
        assert_eq!(p.asset, PriceAsset::Eth);
        assert!(p.quorum_met);
    }

    #[test]
    fn test_btc_outlier_rejected_uses_median() {
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            2,
        );
        agg.update(obs(PriceAsset::Btc, FeedSource::Binance, 68000.0));
        agg.update(obs(PriceAsset::Btc, FeedSource::Coinbase, 68010.0));
        let p = agg
            .update(obs(PriceAsset::Btc, FeedSource::Chainlink, 73000.0))
            .unwrap();
        // Chainlink outlier rejected, median of Binance+Coinbase
        assert_eq!(p.spot_usd, Decimal::try_from(68005.0).unwrap());
    }

    #[test]
    fn test_iterative_filtering_removes_outlier_cluster() {
        // Issue 2: Iterative filtering cleans up after first pass shifts the median.
        // Scenario: 3 feeds agree near 68000, 1 outlier at 72000.
        // Without iterative filtering, the outlier shifts the median upward and
        // a borderline feed near 68000 could be rejected. With iterative filtering,
        // after pass 1 removes 72000, pass 2 recomputes a tighter median from survivors.
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO, // 2% threshold
            1,
        );
        agg.update(obs(PriceAsset::Eth, FeedSource::Chainlink, 68000.0));
        agg.update(obs(PriceAsset::Eth, FeedSource::Coinbase, 68010.0));
        agg.update(obs(PriceAsset::Eth, FeedSource::Binance, 68005.0));
        // One outlier beyond 2% of median
        let result = agg
            .update(obs(PriceAsset::Eth, FeedSource::CoinGecko, 72000.0))
            .unwrap();

        // Outlier should be rejected, leaving 3 sources near 68000
        assert_eq!(result.num_sources, 3);
        assert!(
            result.spot_usd < Decimal::from(69000),
            "spot {} should be near 68000",
            result.spot_usd
        );
    }

    #[test]
    fn test_iterative_filtering_all_rejected_returns_none() {
        // Edge case: prices in two widely separated clusters where median falls between.
        // First pass rejects all → should not panic, should return None (quorum unmet).
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            2, // require at least 2 sources
        );
        // Cluster A around 100
        agg.update(obs(PriceAsset::Eth, FeedSource::Chainlink, 100.0));
        agg.update(obs(PriceAsset::Eth, FeedSource::Coinbase, 101.0));
        // Cluster B around 500 — median of all 4 is (101+500)/2 = 300.5, everything is >2% away
        agg.update(obs(PriceAsset::Eth, FeedSource::Binance, 500.0));
        let result = agg.update(obs(PriceAsset::Eth, FeedSource::CoinGecko, 510.0));
        // All get rejected → quorum not met → None
        assert!(result.is_none());
    }

    #[test]
    fn test_confidence_single_source_meets_quorum() {
        // Issue 3: With min_sources=1, a single source should get full count_factor (1.0)
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1, // min_sources = 1
        );
        let result = agg
            .update(obs(PriceAsset::Btc, FeedSource::Chainlink, 68000.0))
            .unwrap();
        // Single source, zero spread → confidence should be 1.0 (not 0.33 like before)
        assert_eq!(result.confidence, Decimal::ONE);
    }

    #[test]
    fn test_confidence_two_sources_meets_quorum() {
        // Issue 3: With min_sources=2, two agreeing sources should get full count_factor
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            2,
        );
        agg.update(obs(PriceAsset::Eth, FeedSource::Coinbase, 2800.0));
        let result = agg
            .update(obs(PriceAsset::Eth, FeedSource::Binance, 2800.0))
            .unwrap();
        // Two sources meeting quorum of 2 with zero spread → confidence = 1.0
        assert_eq!(result.confidence, Decimal::ONE);
    }

    #[test]
    fn test_timestamp_uses_freshest_observation() {
        // Issue 1: Aggregated timestamp should be the freshest observation, not Utc::now()
        let mut agg = PriceAggregator::new(
            Duration::from_secs(30),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );
        let obs_time = Utc::now() - chrono::Duration::seconds(5);
        let observation = PriceObservation {
            asset: PriceAsset::Btc,
            source: FeedSource::Chainlink,
            price: Decimal::try_from(68000.0).unwrap(),
            timestamp: obs_time,
            bid: None,
            ask: None,
        };
        let result = agg.update(observation).unwrap();
        assert_eq!(result.timestamp, obs_time);
    }

    #[test]
    fn test_zero_price_rejected() {
        // Issue 6: Zero or negative prices should be filtered out
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );
        let zero_obs = PriceObservation {
            asset: PriceAsset::Btc,
            source: FeedSource::Chainlink,
            price: Decimal::ZERO,
            timestamp: Utc::now(),
            bid: None,
            ask: None,
        };
        assert!(agg.update(zero_obs).is_none());

        let neg_obs = PriceObservation {
            asset: PriceAsset::Btc,
            source: FeedSource::Binance,
            price: Decimal::from(-100),
            timestamp: Utc::now(),
            bid: None,
            ask: None,
        };
        assert!(agg.update(neg_obs).is_none());
    }

    #[test]
    fn test_coingecko_uses_longer_staleness_window() {
        let mut agg = PriceAggregator::new(
            Duration::from_secs(10),
            Duration::from_secs(45),
            Decimal::TWO,
            1,
        );

        let stale_time = Utc::now() - chrono::Duration::seconds(20);

        // Coinbase should be considered stale under the 10s default threshold.
        let coinbase_obs = PriceObservation {
            asset: PriceAsset::Sol,
            source: FeedSource::Coinbase,
            price: Decimal::try_from(130.0).unwrap(),
            timestamp: stale_time,
            bid: None,
            ask: None,
        };
        assert!(agg.update(coinbase_obs).is_none());

        // CoinGecko should still be considered fresh under the 45s override.
        let coingecko_obs = PriceObservation {
            asset: PriceAsset::Sol,
            source: FeedSource::CoinGecko,
            price: Decimal::try_from(130.2).unwrap(),
            timestamp: stale_time,
            bid: None,
            ask: None,
        };
        assert!(agg.update(coingecko_obs).is_some());
    }
}
