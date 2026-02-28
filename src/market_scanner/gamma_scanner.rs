use std::collections::HashSet;

use chrono::{Duration, Utc};

use crate::config::TradingConfig;
use crate::price_feeds::PriceAsset;
use crate::timezone::{et_abbreviation, to_et};
use crate::venue::{MarketFilter, VenueClient, VenueError};

use super::{ScannerError, TargetMarket};

/// Scans Polymarket for active crypto prediction markets
/// using the venue client (which queries the Gamma API).
#[derive(Clone)]
pub struct GammaScanner {
    config: TradingConfig,
}

impl GammaScanner {
    pub fn new(config: TradingConfig) -> Self {
        Self { config }
    }

    /// Scan for active markets that match configured tags and duration filters.
    /// Delegates to the venue client's get_markets method.
    pub async fn scan(&self, venue: &dyn VenueClient) -> Result<Vec<TargetMarket>, ScannerError> {
        let normalized_tags = normalize_tags(&self.config.market_tags);
        let effective_max_duration_secs = self.config.market_duration_max_secs.max(900);
        let filter = MarketFilter {
            tags: normalized_tags,
            max_duration_secs: effective_max_duration_secs,
            active_only: true,
        };

        let raw_markets = venue.get_markets(&filter).await.map_err(|e| match e {
            VenueError::Http(e) => ScannerError::Http(e),
            other => ScannerError::GammaApi(other.to_string()),
        })?;
        let now = Utc::now();
        let mut rejected_unsupported_asset = 0usize;
        let mut rejected_expired = 0usize;
        let mut rejected_missing_start = 0usize;
        let mut rejected_future = 0usize;
        let mut rejected_window = 0usize;
        let mut missing_official_reference = 0usize;
        let mut missing_reference_samples: Vec<String> = Vec::new();
        let mut markets: Vec<TargetMarket> = Vec::new();

        for market in raw_markets {
            if PriceAsset::from_market_question(&market.question).is_none() {
                rejected_unsupported_asset += 1;
                continue;
            }
            if market.end_time <= now {
                rejected_expired += 1;
                continue;
            }
            let Some(start_time) = market.start_time else {
                rejected_missing_start += 1;
                continue;
            };
            if start_time > now + Duration::seconds(30) {
                rejected_future += 1;
                continue;
            }
            let duration_secs = (market.end_time - start_time).num_seconds();
            if !matches_supported_window(duration_secs) {
                rejected_window += 1;
                continue;
            }
            if is_up_down_question(&market.question) && market.polymarket_price_to_beat.is_none() {
                // Keep market discovery independent from reference availability.
                // Trading remains fail-closed in Bayesian/pipeline when reference is missing.
                missing_official_reference += 1;
                if missing_reference_samples.len() < 4 {
                    missing_reference_samples.push(format!(
                        "{} | start={} end={} | source={}",
                        market.question,
                        market
                            .start_time
                            .map(fmt_et_timestamp)
                            .unwrap_or_else(|| "n/a".to_string()),
                        fmt_et_timestamp(market.end_time),
                        market
                            .polymarket_price_to_beat_source
                            .as_deref()
                            .unwrap_or("none")
                    ));
                }
            }
            markets.push(market);
        }

        if markets.is_empty() {
            tracing::warn!(
                tags = ?filter.tags,
                max_duration_secs = filter.max_duration_secs,
                rejected_unsupported_asset,
                rejected_expired,
                rejected_missing_start,
                rejected_future,
                rejected_window,
                missing_official_reference,
                missing_reference_samples = ?missing_reference_samples,
                "no active current 5m/15m matching markets found"
            );
            return Err(ScannerError::NoMarkets);
        }

        tracing::info!(
            count = markets.len(),
            tags = ?filter.tags,
            max_duration_secs = filter.max_duration_secs,
            missing_official_reference,
            missing_reference_samples = ?missing_reference_samples,
            "found active current 5m/15m matching markets"
        );
        Ok(markets)
    }
}

fn normalize_tags(tags: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for tag in tags {
        let normalized = tag.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }

    out
}

#[cfg(test)]
fn is_current_short_window_market(market: &TargetMarket, now: chrono::DateTime<Utc>) -> bool {
    if PriceAsset::from_market_question(&market.question).is_none() {
        return false;
    }

    if market.end_time <= now {
        return false;
    }

    let Some(start_time) = market.start_time else {
        return false;
    };

    // Accept tiny clock skews, but reject future time slots.
    if start_time > now + Duration::seconds(30) {
        return false;
    }

    let duration_secs = (market.end_time - start_time).num_seconds();
    matches_supported_window(duration_secs)
}

fn matches_supported_window(duration_secs: i64) -> bool {
    const WINDOWS: [i64; 2] = [300, 900];
    const TOLERANCE_SECS: i64 = 30;
    WINDOWS
        .iter()
        .any(|window| (duration_secs - *window).abs() <= TOLERANCE_SECS)
}

fn is_up_down_question(question: &str) -> bool {
    let q = question.to_ascii_lowercase();
    q.contains("up or down") || q.contains("up/down")
}

fn fmt_et_timestamp(ts: chrono::DateTime<Utc>) -> String {
    let et = to_et(ts);
    format!("{} {}", et.format("%Y-%m-%dT%H:%M:%S%.3f"), et_abbreviation(ts))
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use rust_decimal::Decimal;

    use super::*;
    use crate::venue::{
        FeeSchedule, MarketFilter, OrderBook, OrderEvent, OrderId, OrderRequest, VenueClient,
        VenueError, VenueType,
    };

    struct MockVenue {
        fee_schedule: FeeSchedule,
        captured_filter: Mutex<Option<MarketFilter>>,
        next_markets_result: Mutex<Option<Result<Vec<TargetMarket>, VenueError>>>,
    }

    impl MockVenue {
        fn new(result: Result<Vec<TargetMarket>, VenueError>) -> Self {
            Self {
                fee_schedule: FeeSchedule {
                    taker_fee_bps: Decimal::from(200),
                    maker_rebate_bps: Decimal::ZERO,
                },
                captured_filter: Mutex::new(None),
                next_markets_result: Mutex::new(Some(result)),
            }
        }

        fn captured_filter(&self) -> MutexGuard<'_, Option<MarketFilter>> {
            self.captured_filter
                .lock()
                .expect("captured_filter mutex poisoned")
        }
    }

    #[async_trait]
    impl VenueClient for MockVenue {
        async fn get_markets(
            &self,
            filter: &MarketFilter,
        ) -> Result<Vec<TargetMarket>, VenueError> {
            *self
                .captured_filter
                .lock()
                .expect("captured_filter mutex poisoned") = Some(filter.clone());
            self.next_markets_result
                .lock()
                .expect("next_markets_result mutex poisoned")
                .take()
                .expect("expected one get_markets response")
        }

        async fn get_market_resolution(
            &self,
            _condition_id: &str,
        ) -> Result<Option<crate::venue::MarketResolution>, VenueError> {
            Ok(None)
        }

        async fn get_orderbook(&self, _token_id: &str) -> Result<OrderBook, VenueError> {
            Err(VenueError::Unsupported(
                "not needed for scanner tests".into(),
            ))
        }

        async fn place_order(&self, _order: &OrderRequest) -> Result<OrderId, VenueError> {
            Err(VenueError::Unsupported(
                "not needed for scanner tests".into(),
            ))
        }

        async fn cancel_order(&self, _order_id: &OrderId) -> Result<(), VenueError> {
            Err(VenueError::Unsupported(
                "not needed for scanner tests".into(),
            ))
        }

        async fn cancel_all(&self) -> Result<(), VenueError> {
            Err(VenueError::Unsupported(
                "not needed for scanner tests".into(),
            ))
        }

        async fn subscribe_fills(
            &self,
            _tx: tokio::sync::mpsc::Sender<OrderEvent>,
            _market_rx: tokio::sync::mpsc::Receiver<String>,
            _shutdown: tokio_util::sync::CancellationToken,
        ) -> Result<(), VenueError> {
            Ok(())
        }

        async fn get_balance(&self) -> Result<Decimal, VenueError> {
            Ok(Decimal::ZERO)
        }

        fn fee_schedule(&self) -> &FeeSchedule {
            &self.fee_schedule
        }

        fn venue_type(&self) -> VenueType {
            VenueType::International
        }
    }

    fn trading_config(market_tags: Vec<&str>) -> TradingConfig {
        TradingConfig {
            scan_interval_ms: 30_000,
            tick_interval_ms: 500,
            edge_threshold: Decimal::new(8, 2),
            kelly_fraction: Decimal::new(25, 2),
            uncertainty_haircut_max: Decimal::new(5, 1),
            latency_penalty: Decimal::new(5, 3),
            market_tags: market_tags.into_iter().map(str::to_string).collect(),
            market_duration_max_secs: 900,
            min_confidence: Decimal::new(80, 2),
            max_spread_ratio: Decimal::new(1, 2),
            min_liquidity: Decimal::from(1000),
            min_time_to_expiry_secs: 60,
            paper_bankroll_usdc: Decimal::from(1000),
            dashboard_enabled: true,
            dashboard_live_interval_ms: 5_000,
            calibration_min_samples: 30,
            vol_window_secs: 300,
            ewma_lambda: 0.94,
            fee_rate: Decimal::new(25, 2),
            fee_exponent: 2,
            asset_params: Default::default(),
            max_net_edge: None,
            yes_edge_threshold_mult: Decimal::ONE,
            no_edge_threshold_mult: Decimal::ONE,
            prob_shrinkage_alpha: None,
            min_conviction: 0.0,
            min_annualized_vol: 0.0,
            obi_weight: 0.0,
            obi_depth_levels: 5,
            reversion_lookback_secs: 0.0,
            reversion_weight: 0.0,
            reversion_negative_mult: 1.0,
        }
    }

    fn sample_market() -> TargetMarket {
        let start_time = Utc::now() - Duration::minutes(1);
        TargetMarket {
            condition_id: "cond-1".to_string(),
            token_id_yes: "yes-1".to_string(),
            token_id_no: "no-1".to_string(),
            question: "Bitcoin Up or Down - current slot".to_string(),
            strike_price: Decimal::from(100_000),
            start_time: Some(start_time),
            end_time: start_time + Duration::minutes(5),
            current_yes_price: Decimal::new(52, 2),
            current_no_price: Decimal::new(48, 2),
            polymarket_price_to_beat: Some(Decimal::from(100_000)),
            polymarket_price_to_beat_source: Some("field:test".to_string()),
            liquidity: Decimal::from(1000),
            volume: Decimal::from(2000),
        }
    }

    #[tokio::test]
    async fn scan_normalizes_tags_and_uses_duration_filter() {
        let config = trading_config(vec![" Bitcoin ", "ETH", "bitcoin", "", "   "]);
        let scanner = GammaScanner::new(config);
        let venue = MockVenue::new(Ok(vec![sample_market()]));

        let markets = scanner.scan(&venue).await.expect("scan should succeed");
        assert_eq!(markets.len(), 1);

        let captured = venue.captured_filter();
        let filter = captured.as_ref().expect("expected captured filter");
        assert_eq!(filter.tags, vec!["bitcoin".to_string(), "eth".to_string()]);
        assert_eq!(filter.max_duration_secs, 900);
        assert!(filter.active_only);
    }

    #[test]
    fn current_short_window_filter_accepts_only_5m_or_15m_live_markets() {
        let now = Utc::now();
        let current_5m_start = now - Duration::minutes(1);
        let current_15m_start = now - Duration::minutes(2);
        let future_start = now + Duration::minutes(10);

        let current_5m = TargetMarket {
            question: "BTC Up or Down - current 5m".to_string(),
            start_time: Some(current_5m_start),
            end_time: current_5m_start + Duration::minutes(5),
            ..sample_market()
        };
        let current_15m = TargetMarket {
            question: "ETH Up or Down - current 15m".to_string(),
            start_time: Some(current_15m_start),
            end_time: current_15m_start + Duration::minutes(15),
            ..sample_market()
        };
        let future_5m = TargetMarket {
            question: "XRP Up or Down - future 5m".to_string(),
            start_time: Some(future_start),
            end_time: future_start + Duration::minutes(5),
            ..sample_market()
        };
        let current_10m = TargetMarket {
            question: "SOL Up or Down - current 10m".to_string(),
            start_time: Some(now - Duration::minutes(1)),
            end_time: now + Duration::minutes(9),
            ..sample_market()
        };

        assert!(is_current_short_window_market(&current_5m, now));
        assert!(is_current_short_window_market(&current_15m, now));
        assert!(!is_current_short_window_market(&future_5m, now));
        assert!(!is_current_short_window_market(&current_10m, now));
    }

    #[tokio::test]
    async fn scan_returns_only_current_5m_and_15m_markets() {
        let config = trading_config(vec!["bitcoin", "ethereum", "xrp", "solana"]);
        let scanner = GammaScanner::new(config);

        let now = Utc::now();
        let start_5m = now - Duration::minutes(1);
        let start_15m = now - Duration::minutes(2);
        let future_start = now + Duration::minutes(30);

        let current_btc_5m = TargetMarket {
            question: "BTC Up or Down - live 5m".to_string(),
            start_time: Some(start_5m),
            end_time: start_5m + Duration::minutes(5),
            condition_id: "btc-5m".to_string(),
            ..sample_market()
        };
        let current_eth_15m = TargetMarket {
            question: "ETH Up or Down - live 15m".to_string(),
            start_time: Some(start_15m),
            end_time: start_15m + Duration::minutes(15),
            condition_id: "eth-15m".to_string(),
            ..sample_market()
        };
        let future_xrp_5m = TargetMarket {
            question: "XRP Up or Down - future 5m".to_string(),
            start_time: Some(future_start),
            end_time: future_start + Duration::minutes(5),
            condition_id: "xrp-future".to_string(),
            ..sample_market()
        };
        let current_sol_10m = TargetMarket {
            question: "SOL Up or Down - live 10m".to_string(),
            start_time: Some(now - Duration::minutes(1)),
            end_time: now + Duration::minutes(9),
            condition_id: "sol-10m".to_string(),
            ..sample_market()
        };

        let venue = MockVenue::new(Ok(vec![
            current_btc_5m,
            current_eth_15m,
            future_xrp_5m,
            current_sol_10m,
        ]));

        let markets = scanner.scan(&venue).await.expect("scan should succeed");
        let ids: Vec<&str> = markets.iter().map(|m| m.condition_id.as_str()).collect();
        assert_eq!(ids, vec!["btc-5m", "eth-15m"]);
    }

    #[tokio::test]
    async fn scan_keeps_updown_markets_without_official_price_to_beat_for_observation() {
        let config = trading_config(vec!["bitcoin"]);
        let scanner = GammaScanner::new(config);

        let mut market = sample_market();
        market.polymarket_price_to_beat = None;
        market.polymarket_price_to_beat_source = None;

        let venue = MockVenue::new(Ok(vec![market]));
        let markets = scanner.scan(&venue).await.expect("scan should succeed");
        assert_eq!(markets.len(), 1);
        assert!(markets[0].polymarket_price_to_beat.is_none());
    }

    #[tokio::test]
    async fn scan_returns_no_markets_error_for_empty_results() {
        let config = trading_config(vec!["bitcoin"]);
        let scanner = GammaScanner::new(config);
        let venue = MockVenue::new(Ok(Vec::new()));

        let err = scanner.scan(&venue).await.expect_err("scan should fail");
        assert!(matches!(err, ScannerError::NoMarkets));
    }

    #[tokio::test]
    async fn scan_maps_non_http_venue_errors_to_gamma_api_error() {
        let config = trading_config(vec!["bitcoin"]);
        let scanner = GammaScanner::new(config);
        let venue = MockVenue::new(Err(VenueError::Unsupported(
            "scanner test unsupported path".into(),
        )));

        let err = scanner.scan(&venue).await.expect_err("scan should fail");
        match err {
            ScannerError::GammaApi(msg) => {
                assert!(msg.contains("unsupported venue operation"));
            }
            other => panic!("expected ScannerError::GammaApi, got {other:?}"),
        }
    }
}
