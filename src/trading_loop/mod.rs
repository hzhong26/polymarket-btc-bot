pub mod dashboard;
pub mod event_logger;
pub mod paper_mode;
pub mod pipeline;
pub mod pnl;
pub mod reference_cache;
pub mod trade_sheet;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::join_all;

use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{debug, error, info, warn};

use crate::bayesian::{BayesianUpdater, CalibrationTracker};
use crate::config::{PriceFeedConfig, TradingAssetTuning, TradingConfig};
use crate::dashboard_server::SharedState;
use crate::edge::FeeModel;
use crate::executor::order_manager::OrderManager;
use crate::kelly::KellySizer;
use crate::market_scanner::gamma_scanner::GammaScanner;
use crate::market_scanner::{ScannerError, TargetMarket};
use crate::price_feeds::chainlink::fetch_chainlink_price_oneshot;
use crate::price_feeds::{AggregatedPrice, PriceAsset};
use crate::risk::clock::ClockGuard;
use crate::risk::kill_switch::KillSwitch;
use crate::risk::RiskManager;
use crate::timezone::{et_abbreviation, to_et};
use crate::venue::{MarketResolution, OrderEvent, Side, VenueClient, VenueError};

use self::reference_cache::{CapturedReference, ReferencePriceCache, ReferenceSource};

use self::dashboard::print_pnl_dashboard;
use self::event_logger::{
    action_payload, closed_trade_payload, fill_applied_payload, market_scan_payload,
    order_event_payload, pipeline_payload, pnl_snapshot_payload, price_update_payload,
    reference_captured_payload,
    run_ended_payload, run_started_payload, traded_decision_payload, trading_eligibility_payload,
    EventLoggerHandle,
};
use self::paper_mode::{PaperPendingOrder, PaperTrader};
use self::pipeline::PipelineAction;
use self::pnl::{FillContext, OutcomeTokenSide, PnlTracker};

const SETTLEMENT_GRACE_SECS: i64 = 10;
const SETTLEMENT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const REFERENCE_CAPTURE_PROBE_INTERVAL: Duration = Duration::from_millis(500);
const MARKET_WINDOW_TOLERANCE_SECS: i64 = 30;
const PRICE_STARVATION_TIMEOUT: Duration = Duration::from_secs(30);
const SCAN_TIMEOUT: Duration = Duration::from_secs(15);
const ORDERBOOK_FETCH_TIMEOUT: Duration = Duration::from_secs(3);
const RESOLUTION_POLL_INTERVAL_SECS: i64 = 5;
type ScanResultMsg = (
    chrono::DateTime<Utc>,
    Result<Vec<TargetMarket>, ScannerError>,
);
type SettlementPollMsg = Vec<(String, Result<Option<MarketResolution>, VenueError>)>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EligibilitySnapshot {
    active_markets_for_asset: usize,
    blocked_missing_reference: usize,
    blocked_idempotency: usize,
    eligible_markets_for_trading: usize,
}

/// Main trading loop orchestrator.
pub struct TradingLoop {
    config: TradingConfig,
    price_feed_config: PriceFeedConfig,
    scanner: GammaScanner,
    bayesian_by_asset: HashMap<PriceAsset, BayesianUpdater>,
    calibration_by_asset: HashMap<PriceAsset, CalibrationTracker>,
    latest_spot_by_asset: HashMap<PriceAsset, (chrono::DateTime<Utc>, rust_decimal::Decimal)>,
    kelly: KellySizer,
    executor: OrderManager,
    risk: RiskManager,
    fee_model: FeeModel,
    clock_guard: ClockGuard,
    kill_switch: KillSwitch,
    active_markets: Vec<TargetMarket>,
    reference_cache: ReferencePriceCache,
    http_client: reqwest::Client,
    last_scan: chrono::DateTime<Utc>,
    paper_mode: bool,
    paper_bankroll: rust_decimal::Decimal,
    paper_trader: PaperTrader,
    pnl_tracker: PnlTracker,
    dashboard_enabled: bool,
    dashboard_live_interval: Duration,
    last_pnl_dashboard: chrono::DateTime<Utc>,
    last_settlement_check: chrono::DateTime<Utc>,
    last_reference_capture_probe: chrono::DateTime<Utc>,
    last_boundary_capture: chrono::DateTime<Utc>,
    resolution_poll_not_before: HashMap<String, chrono::DateTime<Utc>>,
    scan_in_flight: bool,
    scan_result_tx: tokio::sync::mpsc::Sender<ScanResultMsg>,
    scan_result_rx: tokio::sync::mpsc::Receiver<ScanResultMsg>,
    pending_scan_handle: Option<tokio::task::JoinHandle<()>>,
    settlement_in_flight: bool,
    settlement_result_tx: tokio::sync::mpsc::Sender<SettlementPollMsg>,
    settlement_result_rx: tokio::sync::mpsc::Receiver<SettlementPollMsg>,
    pending_settlement_handle: Option<tokio::task::JoinHandle<()>>,
    event_logger: EventLoggerHandle,
    last_eligibility_snapshot_by_asset: HashMap<PriceAsset, EligibilitySnapshot>,
    drawdown_halt: Option<String>,
    dashboard_state: Option<SharedState>,
    trade_sheet: Option<trade_sheet::TradeSheetWriter>,
    last_price_received: tokio::time::Instant,
}

impl TradingLoop {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: TradingConfig,
        price_feed_config: PriceFeedConfig,
        scanner: GammaScanner,
        bayesian_by_asset: HashMap<PriceAsset, BayesianUpdater>,
        kelly: KellySizer,
        executor: OrderManager,
        risk: RiskManager,
        fee_model: FeeModel,
        clock_guard: ClockGuard,
        kill_switch: KillSwitch,
        paper_mode: bool,
        paper_bankroll: rust_decimal::Decimal,
        starting_capital: rust_decimal::Decimal,
        event_logger: EventLoggerHandle,
        dashboard_state: SharedState,
    ) -> Self {
        let dashboard_enabled = config.dashboard_enabled;
        let dashboard_live_interval = Duration::from_millis(config.dashboard_live_interval_ms);
        let fee_regime = fee_model.fee_regime;
        let (scan_result_tx, scan_result_rx) = tokio::sync::mpsc::channel(4);
        let (settlement_result_tx, settlement_result_rx) = tokio::sync::mpsc::channel(4);

        Self {
            config,
            price_feed_config,
            scanner,
            bayesian_by_asset,
            calibration_by_asset: PriceAsset::ALL
                .into_iter()
                .map(|asset| {
                    let path = std::path::PathBuf::from("logs")
                        .join(format!("calibration_{}.json", asset.symbol().to_lowercase()));
                    let tracker = CalibrationTracker::load(&path, 500).unwrap_or_else(|_| {
                        info!(
                            asset = %asset,
                            path = %path.display(),
                            "no persisted calibration found, starting fresh"
                        );
                        CalibrationTracker::new(500)
                    });
                    (asset, tracker)
                })
                .collect(),
            latest_spot_by_asset: HashMap::new(),
            kelly,
            executor,
            risk,
            fee_model,
            clock_guard,
            kill_switch,
            active_markets: Vec::new(),
            reference_cache: ReferencePriceCache::new(),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            last_scan: Utc::now() - chrono::Duration::hours(1), // Force immediate scan
            paper_mode,
            paper_bankroll,
            paper_trader: PaperTrader::new(dashboard_enabled),
            pnl_tracker: PnlTracker::new(starting_capital, fee_regime),
            dashboard_enabled,
            dashboard_live_interval,
            last_pnl_dashboard: Utc::now() - chrono::Duration::hours(1),
            last_settlement_check: Utc::now() - chrono::Duration::hours(1),
            last_reference_capture_probe: Utc::now() - chrono::Duration::hours(1),
            last_boundary_capture: chrono::DateTime::UNIX_EPOCH,
            resolution_poll_not_before: HashMap::new(),
            scan_in_flight: false,
            scan_result_tx,
            scan_result_rx,
            pending_scan_handle: None,
            settlement_in_flight: false,
            settlement_result_tx,
            settlement_result_rx,
            pending_settlement_handle: None,
            trade_sheet: event_logger.is_enabled().then(|| {
                trade_sheet::TradeSheetWriter::new(
                    event_logger.log_dir(),
                    &event_logger.run_metadata().bot_version,
                )
            }),
            event_logger,
            last_eligibility_snapshot_by_asset: HashMap::new(),
            drawdown_halt: None,
            dashboard_state: Some(dashboard_state),
            last_price_received: tokio::time::Instant::now(),
        }
    }

    /// Push a pnl snapshot (with `t`, `ts`, and `positions`) to the dashboard.
    fn push_dashboard_snap(
        &self,
        payload: &serde_json::Value,
        positions: Vec<serde_json::Value>,
    ) {
        if let Some(ref state) = self.dashboard_state {
            let ts = Utc::now().timestamp_millis();
            let mut msg = payload.clone();
            if let serde_json::Value::Object(ref mut m) = msg {
                m.insert("t".into(), serde_json::json!("snap"));
                m.insert("ts".into(), serde_json::json!(ts));
                m.insert(
                    "positions".into(),
                    serde_json::Value::Array(positions),
                );
            }
            state.push_snap(msg.to_string());
        }
    }

    /// Push a trade-closed event to the dashboard.
    fn push_dashboard_closed(&self, record: serde_json::Value) {
        if let Some(ref state) = self.dashboard_state {
            state.push_closed(record.to_string());
        }
    }

    /// Remove a pending order from the dashboard (filled or expired).
    fn remove_dashboard_pending(&self, condition_id: &str, reason: &str) {
        if let Some(ref state) = self.dashboard_state {
            state.remove_pending(condition_id, reason);
        }
    }

    /// Main loop. Runs until shutdown signal.
    pub async fn run(
        &mut self,
        venue: Arc<dyn VenueClient>,
        mut price_rx: tokio::sync::broadcast::Receiver<AggregatedPrice>,
        mut order_event_rx: tokio::sync::mpsc::Receiver<OrderEvent>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), crate::Error> {
        info!(
            paper_mode = self.paper_mode,
            configured_paper_bankroll = %self.paper_bankroll,
            edge_threshold = %self.config.edge_threshold,
            kelly_fraction = %self.config.kelly_fraction,
            dashboard_enabled = self.dashboard_enabled,
            dashboard_live_interval_ms = self.dashboard_live_interval.as_millis(),
            "trading loop starting"
        );
        self.event_logger.try_log_value(
            "run_started",
            run_started_payload(
                self.paper_mode,
                self.config.edge_threshold,
                self.config.kelly_fraction,
                self.config.min_confidence,
                self.config.max_spread_ratio,
                self.config.min_liquidity,
                self.config.min_time_to_expiry_secs,
                &self.config.market_tags,
                self.config.market_duration_max_secs,
                self.event_logger.no_trade_sample_rate(),
            ),
        );

        let scan_interval = Duration::from_millis(self.config.scan_interval_ms);

        // Seed trade sheet from today's events log (catches trades placed in earlier
        // runs of the same day before this process started).
        let log_dir = self.event_logger.log_dir().to_string();
        if let Some(ref mut ts) = self.trade_sheet {
            ts.seed_from_today_log(&log_dir).await;
        }

        let run_exit_reason = loop {
            tokio::select! {
                // Branch 1: New price data
                result = price_rx.recv() => {
                    match result {
                        Ok(price) => {
                            self.last_price_received = tokio::time::Instant::now();
                            self.handle_price_update(venue.as_ref(), &price).await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(skipped = n, "price receiver lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            error!("price channel closed");
                            break "price_channel_closed";
                        }
                    }
                }

                // Branch 2: Order events from WebSocket
                Some(event) = order_event_rx.recv() => {
                    self.event_logger
                        .try_log_value("order_event", order_event_payload(&event));
                    let fill_update = self.executor.handle_event(&event);
                    if let Some(update) = fill_update {
                        let applied = self.pnl_tracker.apply_live_fill(
                            &update.order_id,
                            update.fill_price,
                            update.fill_amount,
                        );
                        self.event_logger.try_log_value(
                            "fill_applied",
                            fill_applied_payload(
                                Some(&update.order_id),
                                Side::Buy,
                                update.fill_price,
                                update.fill_amount,
                                applied,
                            ),
                        );
                        if update.is_terminal {
                            self.pnl_tracker.clear_order_context(&update.order_id);
                        }
                        if self.dashboard_enabled && !self.paper_mode {
                            let snapshot = self.pnl_tracker.snapshot();
                            print_pnl_dashboard(&snapshot, "live", "fill", None);
                            let snap_payload = pnl_snapshot_payload(&snapshot, "fill");
                            self.push_dashboard_snap(
                                &snap_payload,
                                self.pnl_tracker.open_position_cards(),
                            );
                            self.event_logger.try_log_value("pnl_snapshot", snap_payload);
                            self.last_pnl_dashboard = Utc::now();
                        }
                    }

                    match &event {
                        OrderEvent::Cancelled { order_id } | OrderEvent::Rejected { order_id, .. } => {
                            self.pnl_tracker.clear_order_context(order_id);
                        }
                        _ => {}
                    }
                }

                // Branch 3: Periodic market scan
                _ = tokio::time::sleep(time_until_next_scan(self.last_scan, scan_interval)), if !self.scan_in_flight => {
                    self.dispatch_scan(venue.clone());
                }

                // Branch 3b: Scan results
                Some((scanned_at, scan_result)) = self.scan_result_rx.recv() => {
                    self.scan_in_flight = false;
                    self.pending_scan_handle.take();
                    self.apply_scan_result(scanned_at, scan_result);
                }

                // Branch 4: Periodic P&L dashboard (live mode)
                _ = tokio::time::sleep(time_until_next_dashboard(self.last_pnl_dashboard, self.dashboard_live_interval)),
                    if self.dashboard_enabled && !self.paper_mode => {
                    let snapshot = self.pnl_tracker.snapshot();
                    print_pnl_dashboard(&snapshot, "live", "periodic", None);
                    let snap_payload = pnl_snapshot_payload(&snapshot, "periodic");
                    self.push_dashboard_snap(
                        &snap_payload,
                        self.pnl_tracker.open_position_cards(),
                    );
                    self.event_logger.try_log_value("pnl_snapshot", snap_payload);
                    self.last_pnl_dashboard = Utc::now();
                }

                // Branch 5: Dispatch settlement resolution polling (non-blocking)
                _ = tokio::time::sleep(time_until_next_settlement(self.last_settlement_check, SETTLEMENT_CHECK_INTERVAL)),
                    if !self.settlement_in_flight => {
                    self.dispatch_settlement(venue.clone());
                }

                // Branch 5b: Settlement poll results
                Some(results) = self.settlement_result_rx.recv() => {
                    self.settlement_in_flight = false;
                    self.pending_settlement_handle.take();
                    self.apply_settlement_results(results);
                }

                // Branch 6: Periodic strict reference capture probe
                _ = tokio::time::sleep(time_until_next_reference_capture(
                    self.last_reference_capture_probe,
                    REFERENCE_CAPTURE_PROBE_INTERVAL,
                )), if self.price_feed_config.captured_reference_for_entry => {
                    let now = Utc::now();
                    self.capture_reference_prices_at(now).await;
                    self.last_reference_capture_probe = now;
                }

                // Branch 6b: Price feed starvation watchdog
                _ = tokio::time::sleep_until(self.last_price_received + PRICE_STARVATION_TIMEOUT) => {
                    error!(
                        timeout_secs = PRICE_STARVATION_TIMEOUT.as_secs(),
                        "no price update received — price feed may be dead"
                    );
                    self.event_logger.try_log_value(
                        "price_starvation",
                        serde_json::json!({
                            "timeout_secs": PRICE_STARVATION_TIMEOUT.as_secs(),
                            "last_price_age_secs": self.last_price_received.elapsed().as_secs(),
                        }),
                    );
                    break "price_feed_starvation";
                }

                // Branch 7: Shutdown
                _ = shutdown.cancelled() => {
                    info!("shutdown signal received");
                    if let Some(handle) = self.pending_scan_handle.take() {
                        handle.abort();
                    }
                    if let Some(handle) = self.pending_settlement_handle.take() {
                        handle.abort();
                    }
                    if !self.paper_mode {
                        if let Err(e) = self.executor.cancel_all(venue.as_ref()).await {
                            error!(error = %e, "failed to cancel all orders on shutdown");
                        }
                    }
                    break "shutdown_signal";
                }
            }

            // Proactive reference capture at 5-minute window boundaries.
            // Runs after every select iteration (not inside a select branch) so it
            // can never be starved by other branches winning the select race.
            if self.price_feed_config.captured_reference_for_entry {
                let current_boundary = floor_to_five_minute_boundary(Utc::now());
                if current_boundary > self.last_boundary_capture {
                    self.last_boundary_capture = current_boundary;
                    info!(
                        boundary_et = %fmt_et_log_time(current_boundary),
                        "5-minute boundary reached, capturing reference prices"
                    );
                    self.capture_reference_at_boundary(current_boundary).await;
                }
            }

            // Check kill switch
            if self.kill_switch.is_triggered() {
                error!("kill switch active — halting trading loop");
                if let Some(handle) = self.pending_scan_handle.take() {
                    handle.abort();
                }
                if let Some(handle) = self.pending_settlement_handle.take() {
                    handle.abort();
                }
                if !self.paper_mode {
                    let _ = self.executor.cancel_all(venue.as_ref()).await;
                }
                self.risk.halt();
                break "kill_switch";
            }

            // Check drawdown halt
            if let Some(ref reason) = self.drawdown_halt {
                let msg = format!("HALTED: max drawdown limit reached — {reason}");
                error!("{msg}");
                eprintln!("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                eprintln!("!! {msg}");
                eprintln!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
                self.event_logger.try_log_value(
                    "drawdown_halt",
                    serde_json::json!({ "reason": reason }),
                );
                if let Some(handle) = self.pending_scan_handle.take() {
                    handle.abort();
                }
                if let Some(handle) = self.pending_settlement_handle.take() {
                    handle.abort();
                }
                if !self.paper_mode {
                    let _ = self.executor.cancel_all(venue.as_ref()).await;
                }
                break "drawdown_limit";
            }
        };

        if self.paper_mode {
            info!(
                trades = self.paper_trader.trade_count(),
                "paper trading session complete"
            );
        }

        let final_snapshot = self.pnl_tracker.snapshot();
        let health = self.event_logger.health_snapshot();
        self.event_logger.try_log_value(
            "logger_health",
            serde_json::json!({
                "dropped_count": health.dropped_count,
                "backpressure_incidents": health.backpressure_incidents,
                "write_error_count": health.write_error_count,
            }),
        );
        self.event_logger.try_log_value(
            "run_ended",
            run_ended_payload(&final_snapshot, health, run_exit_reason),
        );

        Ok(())
    }

    /// Strict capture window after market start for self-captured fallback references.
    /// Markets are eligible only in `[start_time, start_time + 3s]`.
    const CAPTURE_GRACE_SECS: i64 = 3;

    async fn capture_reference_prices_at(&mut self, now: chrono::DateTime<Utc>) {
        if !self.price_feed_config.captured_reference_for_entry {
            return;
        }

        let grace = chrono::Duration::seconds(Self::CAPTURE_GRACE_SECS);

        for market in &self.active_markets {
            let Some((asset, start)) =
                should_attempt_reference_capture(market, now, grace, &self.reference_cache)
            else {
                continue;
            };

            // Try Chainlink one-shot first
            let (ref_price, source) = match self
                .price_feed_config
                .chainlink_address_for_asset(asset)
            {
                Some(address) => {
                    let cl_result = tokio::time::timeout(
                        ORDERBOOK_FETCH_TIMEOUT,
                        fetch_chainlink_price_oneshot(
                            &self.http_client,
                            &self.price_feed_config.chainlink_rpc_url,
                            address,
                        ),
                    ).await;
                    match cl_result {
                        Ok(Ok((cl_price, _ts))) => (cl_price, ReferenceSource::Chainlink),
                        Ok(Err(e)) => {
                            let fallback_spot = self
                                .latest_spot_by_asset
                                .get(&asset)
                                .map(|(_, spot)| *spot);
                            if let Some(spot) = fallback_spot {
                                warn!(
                                    asset = %asset,
                                    error = %e,
                                    "chainlink oneshot failed, falling back to latest aggregated spot"
                                );
                                (spot, ReferenceSource::AggregatedSpot)
                            } else {
                                debug!(
                                    asset = %asset,
                                    error = %e,
                                    "chainlink oneshot failed and no aggregated spot fallback available yet"
                                );
                                continue;
                            }
                        }
                        Err(_) => {
                            let fallback_spot = self
                                .latest_spot_by_asset
                                .get(&asset)
                                .map(|(_, spot)| *spot);
                            if let Some(spot) = fallback_spot {
                                warn!(
                                    asset = %asset,
                                    "chainlink oneshot timed out, falling back to latest aggregated spot"
                                );
                                (spot, ReferenceSource::AggregatedSpot)
                            } else {
                                warn!(
                                    asset = %asset,
                                    "chainlink oneshot timed out and no aggregated spot fallback available"
                                );
                                continue;
                            }
                        }
                    }
                }
                None => {
                    let Some(spot) = self
                        .latest_spot_by_asset
                        .get(&asset)
                        .map(|(_, spot)| *spot)
                    else {
                        debug!(
                            asset = %asset,
                            "no chainlink address and no aggregated spot fallback available yet"
                        );
                        continue;
                    };
                    (spot, ReferenceSource::AggregatedSpot)
                }
            };

            let capture_delay_secs = (now - start).num_seconds();
            info!(
                asset = %asset,
                window_start_et = %fmt_et_log_time(start),
                captured_price = %ref_price,
                source = ?source,
                capture_delay_secs,
                "captured reference price at window open"
            );

            self.reference_cache.insert(
                asset,
                start,
                CapturedReference {
                    price: ref_price,
                    source,
                    captured_at: now,
                    window_start: start,
                },
            );

            self.event_logger.try_log_value(
                "reference_captured",
                reference_captured_payload(
                    asset,
                    source.as_str(),
                    ref_price,
                    start,
                    now,
                    "market_open",
                    Some(capture_delay_secs),
                ),
            );
        }
    }

    /// Proactively fetch and cache reference prices for all assets at a 5-minute boundary.
    /// Does not depend on active_markets — runs unconditionally for all 4 assets so that
    /// the reference is ready before the scanner even picks up the new window's markets.
    /// All Chainlink RPC calls are fired in parallel to minimize select-loop blocking time.
    async fn capture_reference_at_boundary(&mut self, boundary: chrono::DateTime<Utc>) {
        if !self.price_feed_config.captured_reference_for_entry {
            return;
        }

        // Collect assets that need fetching and spawn parallel Chainlink calls.
        let mut handles: Vec<(
            PriceAsset,
            Option<tokio::task::JoinHandle<Result<(Decimal, chrono::DateTime<Utc>), crate::price_feeds::PriceFeedError>>>,
            Option<Decimal>, // spot fallback
        )> = Vec::new();

        for asset in PriceAsset::ALL {
            if self.reference_cache.get(asset, boundary).is_some() {
                continue;
            }

            let spot_fallback = self
                .latest_spot_by_asset
                .get(&asset)
                .map(|(_, s)| *s);

            match self.price_feed_config.chainlink_address_for_asset(asset) {
                Some(address) => {
                    let http = self.http_client.clone();
                    let rpc_url = self.price_feed_config.chainlink_rpc_url.clone();
                    let addr = address.to_string();
                    let handle = tokio::spawn(async move {
                        fetch_chainlink_price_oneshot(&http, &rpc_url, &addr).await
                    });
                    handles.push((asset, Some(handle), spot_fallback));
                }
                None => {
                    handles.push((asset, None, spot_fallback));
                }
            }
        }

        // Await all results and apply to cache.
        for (asset, handle, spot_fallback) in handles {
            let (ref_price, source) = match handle {
                Some(h) => match h.await {
                    Ok(Ok((cl_price, _ts))) => (cl_price, ReferenceSource::Chainlink),
                    Ok(Err(e)) => {
                        if let Some(spot) = spot_fallback {
                            warn!(
                                asset = %asset,
                                error = %e,
                                "chainlink failed at boundary, falling back to latest spot"
                            );
                            (spot, ReferenceSource::AggregatedSpot)
                        } else {
                            warn!(
                                asset = %asset,
                                error = %e,
                                "chainlink failed at boundary and no spot available, skipping"
                            );
                            continue;
                        }
                    }
                    Err(e) => {
                        if let Some(spot) = spot_fallback {
                            warn!(
                                asset = %asset,
                                error = %e,
                                "chainlink task panicked at boundary, falling back to latest spot"
                            );
                            (spot, ReferenceSource::AggregatedSpot)
                        } else {
                            warn!(
                                asset = %asset,
                                error = %e,
                                "chainlink task panicked at boundary and no spot available, skipping"
                            );
                            continue;
                        }
                    }
                },
                None => {
                    if let Some(spot) = spot_fallback {
                        (spot, ReferenceSource::AggregatedSpot)
                    } else {
                        debug!(
                            asset = %asset,
                            "no chainlink address and no spot available for boundary capture"
                        );
                        continue;
                    }
                }
            };

            info!(
                asset = %asset,
                boundary_et = %fmt_et_log_time(boundary),
                price = %ref_price,
                source = ?source,
                "boundary reference price captured"
            );

            let captured_at = Utc::now();
            self.reference_cache.insert(
                asset,
                boundary,
                CapturedReference {
                    price: ref_price,
                    source,
                    captured_at,
                    window_start: boundary,
                },
            );

            self.event_logger.try_log_value(
                "reference_captured",
                reference_captured_payload(
                    asset,
                    source.as_str(),
                    ref_price,
                    boundary,
                    captured_at,
                    "boundary",
                    None,
                ),
            );
        }
    }

    /// Fallback: for any up/down market that still lacks a reference price after the
    /// strict capture window has passed, inject the latest aggregated spot price.
    /// Less accurate than a capture at market open, but far better than not trading.
    fn late_capture_from_spot_if_blocked(&mut self) {
        if !self.price_feed_config.captured_reference_for_entry {
            return;
        }
        for market in &self.active_markets {
            if market.strike_price > Decimal::ZERO {
                continue; // strike markets don't need a reference
            }
            if market.polymarket_price_to_beat.is_some() {
                continue; // already has an official reference
            }
            let Some(asset) = PriceAsset::from_market_question(&market.question) else {
                continue;
            };
            let Some(start) = market.start_time else {
                continue;
            };
            if self.reference_cache.get(asset, start).is_some() {
                continue; // already captured
            }
            let Some((_, spot)) = self.latest_spot_by_asset.get(&asset) else {
                continue;
            };
            warn!(
                asset = %asset,
                window_start_et = %fmt_et_log_time(start),
                spot = %spot,
                "late reference capture from aggregated spot (missed capture window)"
            );
            let captured_at = Utc::now();
            self.reference_cache.insert(
                asset,
                start,
                reference_cache::CapturedReference {
                    price: *spot,
                    source: reference_cache::ReferenceSource::AggregatedSpot,
                    captured_at,
                    window_start: start,
                },
            );

            self.event_logger.try_log_value(
                "reference_captured",
                reference_captured_payload(
                    asset,
                    "aggregated_spot",
                    *spot,
                    start,
                    captured_at,
                    "late_fallback",
                    None,
                ),
            );
        }
    }

    async fn handle_price_update(&mut self, venue: &dyn VenueClient, price: &AggregatedPrice) {
        self.event_logger
            .try_log_value("price_update", price_update_payload(price));
        self.latest_spot_by_asset
            .insert(price.asset, (price.timestamp, price.spot_usd));
        self.capture_reference_prices_at(Utc::now()).await;
        self.late_capture_from_spot_if_blocked();
        self.prune_expired_markets(Utc::now(), "price_update");

        // Expire unfilled paper orders whose markets have ended
        if self.paper_mode {
            let expired = self.paper_trader.expire_stale(Utc::now());
            for order in &expired {
                self.pnl_tracker.clear_order_context(&order.paper_order_id);
                self.remove_dashboard_pending(&order.condition_id, "expired");
                // Compute hypothetical outcome fields for expired orders
                let outcome_side = order.fill_context.outcome_side;
                let spot_at_expiry = self
                    .latest_spot_by_asset
                    .get(&order.fill_context.asset)
                    .map(|(_, s)| s.to_f64().unwrap_or(0.0));
                let strike_price = order
                    .fill_context
                    .strike_price
                    .and_then(|s| s.to_f64());
                let would_have_won = match (outcome_side, spot_at_expiry, strike_price) {
                    (Some(pnl::OutcomeTokenSide::Yes), Some(spot), Some(strike)) => {
                        Some(spot >= strike)
                    }
                    (Some(pnl::OutcomeTokenSide::No), Some(spot), Some(strike)) => {
                        Some(spot < strike)
                    }
                    _ => None,
                };
                let entry_price = order.limit_price.to_f64().unwrap_or(0.0);
                let amount_f = order.amount.to_f64().unwrap_or(0.0);
                let hypothetical_pnl = would_have_won.map(|won| {
                    if won {
                        (1.0 - entry_price) * amount_f
                    } else {
                        -entry_price * amount_f
                    }
                });

                self.event_logger.try_log_value(
                    "paper_order_expired",
                    serde_json::json!({
                        "paper_order_id": order.paper_order_id,
                        "condition_id": order.condition_id,
                        "token_id": order.token_id,
                        "limit_price": order.limit_price.to_f64(),
                        "amount": order.amount.to_f64(),
                        "market_question": order.market_question,
                        "placed_at": order.placed_at.to_rfc3339(),
                        "end_time": order.end_time.to_rfc3339(),
                        "asset": order.fill_context.asset.symbol(),
                        "outcome_side": outcome_side.map(|s| match s {
                            pnl::OutcomeTokenSide::Yes => "YES",
                            pnl::OutcomeTokenSide::No  => "NO",
                        }),
                        "net_edge": order.diagnostics.net_edge,
                        "posterior": order.diagnostics.posterior,
                        "spot_at_expiry": spot_at_expiry,
                        "strike_price": strike_price,
                        "would_have_won": would_have_won,
                        "hypothetical_pnl": hypothetical_pnl,
                    }),
                );
                info!(
                    paper_order_id = %order.paper_order_id,
                    condition_id = %order.condition_id,
                    market = %order.market_question,
                    "paper order expired unfilled"
                );
            }
        }

        self.maybe_log_eligibility_snapshot(price.asset);

        let asset_tuning = self.asset_tuning_for(price.asset);
        let edge_threshold = self.config.edge_threshold.to_f64().unwrap_or(0.08)
            * asset_tuning.edge_threshold_mult.to_f64().unwrap_or(1.0);

        let base_uncertainty = self
            .calibration_by_asset
            .get(&price.asset)
            .map(|c| c.model_uncertainty_warmup(self.config.calibration_min_samples))
            .unwrap_or(0.15);
        let model_uncertainty = (base_uncertainty
            * asset_tuning.uncertainty_mult.to_f64().unwrap_or(1.0))
        .clamp(0.0, 1.0);

        // Clone the dashboard handle before the mutable borrow of bayesian_by_asset
        // so that we can push snapshots inside the market loop without conflicting borrows.
        let dash = self.dashboard_state.clone();

        let Some(bayesian) = self.bayesian_by_asset.get_mut(&price.asset) else {
            debug!(asset = %price.asset, "no bayesian state for asset, skipping");
            return;
        };

        // Feed price to the asset-specific volatility estimator.
        bayesian.feed_price(price);

        // Stale data check
        if !self.clock_guard.is_fresh(price.timestamp) {
            debug!(
                age_ms = self.clock_guard.quote_age_ms(price.timestamp),
                "stale price data, skipping"
            );
            return;
        }

        // Drawdown halt check — set the flag; the main loop will break and exit.
        if self.paper_mode && self.drawdown_halt.is_none() {
            let bankroll = self.pnl_tracker.available_capital();
            if self.risk.is_drawdown_breached(bankroll) {
                let (dd, max) = self.risk.drawdown_status(bankroll);
                self.drawdown_halt = Some(format!(
                    "drawdown {:.1}% exceeds max_drawdown_pct {:.1}%",
                    dd.to_f64().unwrap_or(0.0) * 100.0,
                    max.to_f64().unwrap_or(0.0) * 100.0,
                ));
            }
        }
        if self.drawdown_halt.is_some() {
            return;
        }

        // ── Phase 1: Collect markets that need orderbook fetches ──
        let active_markets = self.active_markets.clone();
        struct BookFetchPlan {
            market_idx: usize,
            market_asset: PriceAsset,
            effective_entry_reference: Option<Decimal>,
            needs_no_book: bool,
        }
        let mut fetch_plans: Vec<BookFetchPlan> = Vec::new();

        for (idx, market) in active_markets.iter().enumerate() {
            let Some(market_asset) = PriceAsset::from_market_question(&market.question) else {
                continue;
            };
            if market_asset != price.asset {
                continue;
            }

            let effective_entry_reference = effective_entry_reference_for_market(
                market,
                self.price_feed_config.captured_reference_for_entry,
                &self.reference_cache,
            );

            if should_skip_for_missing_reference(market, effective_entry_reference) {
                debug!(
                    condition_id = %market.condition_id,
                    market = %market.question,
                    "skipping up/down market without any reference before orderbook fetch"
                );
                continue;
            }

            let needs_no_book = self.pnl_tracker.has_active_no_position(market)
                || (self.paper_mode && self.paper_trader.has_pending_no_order(&market.token_id_no));

            fetch_plans.push(BookFetchPlan {
                market_idx: idx,
                market_asset,
                effective_entry_reference,
                needs_no_book,
            });
        }

        // ── Phase 2: Fetch all orderbooks concurrently (3s timeout per request) ──
        let fetch_futures: Vec<_> = fetch_plans.iter().map(|plan| {
            let yes_token = active_markets[plan.market_idx].token_id_yes.clone();
            let no_token = active_markets[plan.market_idx].token_id_no.clone();
            let needs_no = plan.needs_no_book;
            async move {
                let yes_book = tokio::time::timeout(
                    ORDERBOOK_FETCH_TIMEOUT,
                    venue.get_orderbook(&yes_token),
                ).await;
                let no_book = if needs_no {
                    Some(tokio::time::timeout(
                        ORDERBOOK_FETCH_TIMEOUT,
                        venue.get_orderbook(&no_token),
                    ).await)
                } else {
                    None
                };
                (yes_book, no_book)
            }
        }).collect();

        let fetched_books = join_all(fetch_futures).await;

        // ── Phase 3: Process each market with pre-fetched orderbooks ──
        for (plan, (yes_result, no_result)) in fetch_plans.into_iter().zip(fetched_books) {
            let market = &active_markets[plan.market_idx];
            let market_asset = plan.market_asset;
            let effective_entry_reference = plan.effective_entry_reference;

            // Unwrap YES orderbook (required)
            let book = match yes_result {
                Ok(Ok(b)) => b,
                Ok(Err(e)) => {
                    debug!(market = %market.question, error = %e, "failed to get orderbook");
                    continue;
                }
                Err(_) => {
                    warn!(market = %market.question, "orderbook fetch timed out (YES)");
                    continue;
                }
            };
            // Unwrap optional NO orderbook
            let no_book = match no_result {
                Some(Ok(Ok(b))) => Some(b),
                Some(Ok(Err(e))) => {
                    debug!(market = %market.question, error = %e, "failed to get NO orderbook");
                    None
                }
                Some(Err(_)) => {
                    warn!(market = %market.question, "orderbook fetch timed out (NO)");
                    None
                }
                None => None,
            };
            self.pnl_tracker.update_market_marks(market, &book, no_book.as_ref());

            // Paper mode: check if a pending order can now be filled against this book
            if self.paper_mode && self.paper_trader.has_pending(&market.condition_id) {
                // Determine which book to check: use the token-specific book
                let pending_is_no = self.paper_trader.has_pending_no_order(&market.token_id_no);
                let fill_book = if pending_is_no {
                    no_book.as_ref().unwrap_or(&book)
                } else {
                    &book
                };
                if let Some(order) = self.paper_trader.check_fill(&market.condition_id, fill_book) {
                    self.pnl_tracker.apply_live_fill(
                        &order.paper_order_id,
                        order.limit_price,
                        order.amount,
                    );
                    self.pnl_tracker.clear_order_context(&order.paper_order_id);
                    self.event_logger.try_log_value(
                        "fill_applied",
                        fill_applied_payload(
                            Some(&order.paper_order_id),
                            order.side,
                            order.limit_price,
                            order.amount,
                            true,
                        ),
                    );
                    self.event_logger.try_log_value(
                        "traded_decision",
                        traded_decision_payload(
                            Some(&order.paper_order_id),
                            &order.condition_id,
                            &order.token_id,
                            order.side,
                            order.limit_price,
                            order.amount,
                            &order.market_question,
                            order.diagnostics.model_type,
                            order.diagnostics.posterior,
                            order.diagnostics.net_edge,
                        ),
                    );
                    // Dashboard: remove from pending, push opened event
                    if let Some(ref state) = dash {
                        state.remove_pending(&order.condition_id, "filled");
                        let d = &order.diagnostics;
                        let outcome_side = order
                            .fill_context
                            .outcome_side
                            .unwrap_or(pnl::OutcomeTokenSide::Yes);
                        let opened_msg = serde_json::json!({
                            "t": "opened",
                            "ts": Utc::now().timestamp_millis(),
                            "token_id": order.token_id,
                            "condition_id": order.condition_id,
                            "asset": d.market_asset,
                            "question": order.market_question,
                            "opened_at": Utc::now().to_rfc3339(),
                            "model_type": d.model_type,
                            "p_model": d.p_model,
                            "market_implied_p": d.market_implied_p,
                            "posterior": d.posterior,
                            "gross_edge": d.gross_edge,
                            "net_edge": d.net_edge,
                            "cost_fee": d.cost_fee,
                            "cost_slippage": d.cost_slippage,
                            "cost_total": d.cost_total,
                            "kelly_raw": d.kelly_raw,
                            "kelly_adjusted": d.kelly_adjusted,
                            "kelly_final": d.kelly_final,
                            "kelly_stake_usdc": d.kelly_stake_usdc.and_then(|v| v.to_f64()),
                            "entry_price": order.limit_price.to_f64(),
                            "quantity": order.amount.to_f64(),
                            "end_time": order.end_time.to_rfc3339(),
                            "side": match outcome_side {
                                pnl::OutcomeTokenSide::Yes => "YES",
                                pnl::OutcomeTokenSide::No  => "NO",
                            },
                        });
                        state.push_opened(opened_msg.to_string());
                    }
                    if let Some(ref mut ts) = self.trade_sheet {
                        let outcome_side = order
                            .fill_context
                            .outcome_side
                            .unwrap_or(pnl::OutcomeTokenSide::Yes);
                        ts.record_opened(
                            order.token_id.clone(),
                            order.fill_context.asset.symbol().to_string(),
                            Utc::now(),
                            Some(order.end_time),
                            order.fill_context.window_start,
                            match outcome_side {
                                pnl::OutcomeTokenSide::Yes => "YES".to_string(),
                                pnl::OutcomeTokenSide::No => "NO".to_string(),
                            },
                            order.diagnostics.kelly_stake_usdc.and_then(|v| v.to_f64()),
                            order.diagnostics.cost_fee,
                        );
                    }
                    info!(
                        paper_order_id = %order.paper_order_id,
                        condition_id = %order.condition_id,
                        market = %order.market_question,
                        price = %order.limit_price,
                        amount = %order.amount,
                        "paper order filled on subsequent tick"
                    );
                }
                // Whether filled or still pending, skip new pipeline eval for this market
                continue;
            }

            if self
                .pnl_tracker
                .has_active_or_settled_position(&market.condition_id)
            {
                debug!(
                    condition_id = %market.condition_id,
                    market = %market.question,
                    "skipping market: active/pending/settled position already exists"
                );
                continue;
            }

            let (bankroll, exposure, open_count) = if self.paper_mode {
                (
                    self.pnl_tracker.available_capital(),
                    self.pnl_tracker.deployed_capital(),
                    self.pnl_tracker.open_position_count(),
                )
            } else {
                (
                    venue.get_balance().await.unwrap_or_default(),
                    self.executor.total_exposure(),
                    self.executor.open_orders().len() as u32,
                )
            };

            // Clone market for model, injecting captured reference if official is missing.
            let market_for_model = if market.polymarket_price_to_beat.is_none() && effective_entry_reference.is_some() {
                let mut m = market.clone();
                m.polymarket_price_to_beat = effective_entry_reference;
                m.polymarket_price_to_beat_source = Some("self_captured".into());
                m
            } else {
                market.clone()
            };

            let result = pipeline::evaluate_market(
                &market_for_model,
                price,
                &book,
                bayesian,
                &self.fee_model,
                &self.kelly,
                &self.risk,
                exposure,
                bankroll,
                open_count,
                model_uncertainty,
                edge_threshold,
                self.config.min_confidence.to_f64().unwrap_or(0.8),
                self.config.max_spread_ratio.to_f64().unwrap_or(0.01),
                self.config.min_liquidity.to_f64().unwrap_or(0.0),
                self.config.min_time_to_expiry_secs as i64,
                self.config.max_net_edge.and_then(|d| d.to_f64()),
                self.config.yes_edge_threshold_mult.to_f64().unwrap_or(1.0),
                self.config.no_edge_threshold_mult.to_f64().unwrap_or(1.0),
                self.config.prob_shrinkage_alpha,
                self.config.obi_weight,
                self.config.obi_depth_levels,
            );
            let is_no_trade = matches!(result.action, PipelineAction::NoTrade { .. });
            let was_sampled = if is_no_trade {
                self.event_logger.should_log_no_trade()
            } else {
                true
            };
            let sample_rate = if is_no_trade {
                Some(self.event_logger.no_trade_sample_rate())
            } else {
                None
            };
            if !is_no_trade || was_sampled {
                self.event_logger.try_log_value(
                    "pipeline_evaluation",
                    pipeline_payload(
                        &result,
                        market_asset,
                        price,
                        &book,
                        model_uncertainty,
                        edge_threshold,
                        bankroll,
                        exposure,
                        open_count,
                        self.event_logger.orderbook_depth_levels(),
                        sample_rate,
                        was_sampled,
                    ),
                );
                self.event_logger.try_log_value(
                    "pipeline_action",
                    action_payload(&result, sample_rate, was_sampled),
                );
            }

            if self.paper_mode {
                if let PipelineAction::PlaceOrder {
                    token_id,
                    side,
                    price: order_price,
                    amount,
                } = &result.action
                {
                    // Create a pending order instead of filling instantly.
                    // The order will be checked for fill on subsequent ticks.
                    let paper_order_id = format!(
                        "paper-{}-{}",
                        &market.condition_id[..8.min(market.condition_id.len())],
                        Utc::now().timestamp_millis()
                    );
                    let fill_context = FillContext {
                        token_id: token_id.clone(),
                        asset: market_asset,
                        market_question: market.question.clone(),
                        side: *side,
                        condition_id: Some(market.condition_id.clone()),
                        end_time: Some(market.end_time),
                        window_start: market.start_time,
                        strike_price: Some(market.strike_price),
                        reference_price: effective_entry_reference.or_else(|| {
                            result
                                .diagnostics
                                .reference_price
                                .and_then(|v| Decimal::try_from(v).ok())
                        }),
                        official_price_to_beat: market.polymarket_price_to_beat,
                        model_type: Some(result.diagnostics.model_type.to_string()),
                        predicted_yes_prob: result.diagnostics.posterior,
                        model_yes_prob: result.diagnostics.p_model,
                        outcome_side: Some(outcome_side_for_token(market, token_id)),
                    };
                    let pending = PaperPendingOrder {
                        paper_order_id: paper_order_id.clone(),
                        token_id: token_id.clone(),
                        condition_id: market.condition_id.clone(),
                        side: *side,
                        limit_price: *order_price,
                        amount: *amount,
                        placed_at: Utc::now(),
                        end_time: market.end_time,
                        fill_context: fill_context.clone(),
                        diagnostics: result.diagnostics.clone(),
                        market_question: market.question.clone(),
                    };
                    self.pnl_tracker
                        .register_order_context(paper_order_id.clone(), fill_context);
                    self.paper_trader.place_pending(pending);
                    self.event_logger.try_log_value(
                        "paper_order_placed",
                        serde_json::json!({
                            "paper_order_id": paper_order_id,
                            "condition_id": market.condition_id,
                            "token_id": token_id,
                            "side": match side { Side::Buy => "BUY", Side::Sell => "SELL" },
                            "limit_price": order_price.to_f64(),
                            "amount": amount.to_f64(),
                            "market_question": market.question,
                            "end_time": market.end_time.to_rfc3339(),
                            "asset": result.diagnostics.market_asset,
                            "outcome_side": match outcome_side_for_token(market, token_id) {
                                pnl::OutcomeTokenSide::Yes => "YES",
                                pnl::OutcomeTokenSide::No  => "NO",
                            },
                            "net_edge": result.diagnostics.net_edge,
                            "posterior": result.diagnostics.posterior,
                            "p_model": result.diagnostics.p_model,
                            "kelly_stake_usdc": result.diagnostics.kelly_stake_usdc.and_then(|v| v.to_f64()),
                        }),
                    );
                    info!(
                        paper_order_id = %paper_order_id,
                        condition_id = %market.condition_id,
                        market = %market.question,
                        side = ?side,
                        price = %order_price,
                        amount = %amount,
                        "paper order placed (pending fill on next tick)"
                    );
                    // Dashboard: pending event
                    if let Some(ref state) = dash {
                        let d = &result.diagnostics;
                        let outcome_side = outcome_side_for_token(market, token_id);
                        let pending_msg = serde_json::json!({
                            "t": "pending",
                            "ts": Utc::now().timestamp_millis(),
                            "condition_id": market.condition_id,
                            "token_id": token_id,
                            "asset": d.market_asset,
                            "question": market.question,
                            "side": match outcome_side {
                                pnl::OutcomeTokenSide::Yes => "YES",
                                pnl::OutcomeTokenSide::No  => "NO",
                            },
                            "limit_price": order_price.to_f64(),
                            "amount": amount.to_f64(),
                            "placed_at": Utc::now().to_rfc3339(),
                            "end_time": market.end_time.to_rfc3339(),
                            "window_start": market.start_time.map(|t| t.to_rfc3339()),
                            "diagnostics": {
                                "p_model": d.p_model,
                                "market_implied_p": d.market_implied_p,
                                "posterior": d.posterior,
                                "gross_edge": d.gross_edge,
                                "net_edge": d.net_edge,
                                "cost_fee": d.cost_fee,
                                "cost_slippage": d.cost_slippage,
                                "cost_total": d.cost_total,
                                "kelly_raw": d.kelly_raw,
                                "kelly_adjusted": d.kelly_adjusted,
                                "kelly_final": d.kelly_final,
                                "kelly_stake_usdc": d.kelly_stake_usdc.and_then(|v| v.to_f64()),
                            },
                        });
                        state.push_pending(pending_msg.to_string());
                    }
                }
                let snapshot = self.pnl_tracker.snapshot();
                self.paper_trader.process(&result, &snapshot);
                let snap_payload = pnl_snapshot_payload(&snapshot, "evaluation");
                // Throttle paper-mode dashboard snaps to dashboard_live_interval
                // so the ring buffer covers hours of history, not minutes.
                let snap_due = (Utc::now() - self.last_pnl_dashboard)
                    .to_std()
                    .unwrap_or(self.dashboard_live_interval)
                    >= self.dashboard_live_interval;
                if snap_due {
                    if let Some(ref state) = dash {
                        let positions = self.pnl_tracker.open_position_cards();
                        let ts = Utc::now().timestamp_millis();
                        let mut snap_msg = snap_payload.clone();
                        if let serde_json::Value::Object(ref mut m) = snap_msg {
                            m.insert("t".into(), serde_json::json!("snap"));
                            m.insert("ts".into(), serde_json::json!(ts));
                            m.insert("positions".into(), serde_json::Value::Array(positions));
                        }
                        state.push_snap(snap_msg.to_string());
                    }
                    self.last_pnl_dashboard = Utc::now();
                }
                self.event_logger.try_log_value("pnl_snapshot", snap_payload);
                continue;
            }

            // Execute the action
            match &result.action {
                PipelineAction::NoTrade { reason } => {
                    debug!(market = %market.question, reason, "no trade");
                }
                PipelineAction::PlaceOrder {
                    token_id,
                    side,
                    price: order_price,
                    amount,
                } => {
                    match self
                        .executor
                        .place_limit_order(venue, token_id, *side, *order_price, *amount, &book)
                        .await
                    {
                        Ok(order_id) => {
                            self.event_logger.try_log_value(
                                "order_submitted",
                                serde_json::json!({
                                    "order_id": order_id.as_str(),
                                    "token_id": token_id,
                                    "side": match side { Side::Buy => "BUY", Side::Sell => "SELL" },
                                    "price": order_price.to_f64(),
                                    "amount": amount.to_f64(),
                                    "market_question": market.question.as_str(),
                                }),
                            );
                            self.event_logger.try_log_value(
                                "traded_decision",
                                traded_decision_payload(
                                    Some(order_id.as_str()),
                                    &market.condition_id,
                                    token_id,
                                    *side,
                                    *order_price,
                                    *amount,
                                    &market.question,
                                    result.diagnostics.model_type,
                                    result.diagnostics.posterior,
                                    result.diagnostics.net_edge,
                                ),
                            );
                            self.pnl_tracker.register_order_context(
                                order_id.clone(),
                                FillContext {
                                    token_id: token_id.clone(),
                                    asset: market_asset,
                                    market_question: market.question.clone(),
                                    side: *side,
                                    condition_id: Some(market.condition_id.clone()),
                                    end_time: Some(market.end_time),
                                    window_start: market.start_time,
                                    strike_price: Some(market.strike_price),
                                    reference_price: effective_entry_reference.or_else(
                                        || {
                                            result
                                                .diagnostics
                                                .reference_price
                                                .and_then(|v| Decimal::try_from(v).ok())
                                        },
                                    ),
                                    official_price_to_beat: market.polymarket_price_to_beat,
                                    model_type: Some(result.diagnostics.model_type.to_string()),
                                    predicted_yes_prob: result.diagnostics.posterior,
                                    model_yes_prob: result.diagnostics.p_model,
                                    outcome_side: Some(outcome_side_for_token(market, token_id)),
                                },
                            );
                            info!(
                                order_id,
                                market = %market.question,
                                side = ?side,
                                price = %order_price,
                                amount = %amount,
                                "order executed"
                            );
                            // Dashboard: opened event (live mode)
                            if let Some(ref state) = dash {
                                let d = &result.diagnostics;
                                let opened_msg = serde_json::json!({
                                    "t": "opened",
                                    "ts": Utc::now().timestamp_millis(),
                                    "token_id": token_id,
                                    "condition_id": market.condition_id,
                                    "asset": market_asset.symbol(),
                                    "question": market.question,
                                    "opened_at": Utc::now().to_rfc3339(),
                                    "model_type": d.model_type,
                                    "p_model": d.p_model,
                                    "market_implied_p": d.market_implied_p,
                                    "posterior": d.posterior,
                                    "gross_edge": d.gross_edge,
                                    "net_edge": d.net_edge,
                                    "cost_fee": d.cost_fee,
                                    "cost_slippage": d.cost_slippage,
                                    "cost_total": d.cost_total,
                                    "kelly_raw": d.kelly_raw,
                                    "kelly_adjusted": d.kelly_adjusted,
                                    "kelly_final": d.kelly_final,
                                    "kelly_stake_usdc": d.kelly_stake_usdc.and_then(|v| v.to_f64()),
                                    "entry_price": order_price.to_f64(),
                                    "quantity": amount.to_f64(),
                                    "end_time": market.end_time.to_rfc3339(),
                                    "window_start": market.start_time.map(|t| t.to_rfc3339()),
                                    "side": match outcome_side_for_token(market, token_id) {
                                        pnl::OutcomeTokenSide::Yes => "YES",
                                        pnl::OutcomeTokenSide::No  => "NO",
                                    },
                                });
                                state.push_opened(opened_msg.to_string());
                            }
                            if let Some(ref mut ts) = self.trade_sheet {
                                ts.record_opened(
                                    token_id.clone(),
                                    market_asset.symbol().to_string(),
                                    Utc::now(),
                                    Some(market.end_time),
                                    market.start_time,
                                    match outcome_side_for_token(market, token_id) {
                                        pnl::OutcomeTokenSide::Yes => "YES".to_string(),
                                        pnl::OutcomeTokenSide::No => "NO".to_string(),
                                    },
                                    result.diagnostics.kelly_stake_usdc.and_then(|v| v.to_f64()),
                                    result.diagnostics.cost_fee,
                                );
                            }
                            if self.dashboard_enabled {
                                let snapshot = self.pnl_tracker.snapshot();
                                print_pnl_dashboard(&snapshot, "live", "trade", None);
                                let snap_payload = pnl_snapshot_payload(&snapshot, "trade");
                                if let Some(ref state) = dash {
                                    let positions = self.pnl_tracker.open_position_cards();
                                    let ts = Utc::now().timestamp_millis();
                                    let mut snap_msg = snap_payload.clone();
                                    if let serde_json::Value::Object(ref mut m) = snap_msg {
                                        m.insert("t".into(), serde_json::json!("snap"));
                                        m.insert("ts".into(), serde_json::json!(ts));
                                        m.insert(
                                            "positions".into(),
                                            serde_json::Value::Array(positions),
                                        );
                                    }
                                    state.push_snap(snap_msg.to_string());
                                }
                                self.event_logger.try_log_value("pnl_snapshot", snap_payload);
                                self.last_pnl_dashboard = Utc::now();
                            }
                        }
                        Err(e) => {
                            self.event_logger.try_log_value(
                                "order_submit_failed",
                                serde_json::json!({
                                    "token_id": token_id,
                                    "side": match side { Side::Buy => "BUY", Side::Sell => "SELL" },
                                    "price": order_price.to_f64(),
                                    "amount": amount.to_f64(),
                                    "market_question": market.question.as_str(),
                                    "error": e.to_string(),
                                }),
                            );
                            error!(
                                market = %market.question,
                                error = %e,
                                "order execution failed"
                            );
                        }
                    }
                }
            }
        }
    }

    fn compute_eligibility_snapshot(&self, asset: PriceAsset) -> EligibilitySnapshot {
        let mut snapshot = EligibilitySnapshot::default();

        for market in &self.active_markets {
            if PriceAsset::from_market_question(&market.question) != Some(asset) {
                continue;
            }

            snapshot.active_markets_for_asset += 1;
            let effective_entry_reference = effective_entry_reference_for_market(
                market,
                self.price_feed_config.captured_reference_for_entry,
                &self.reference_cache,
            );
            if should_skip_for_missing_reference(market, effective_entry_reference) {
                snapshot.blocked_missing_reference += 1;
                continue;
            }
            if self
                .pnl_tracker
                .has_active_or_settled_position(&market.condition_id)
                || self.paper_trader.has_pending(&market.condition_id)
            {
                snapshot.blocked_idempotency += 1;
                continue;
            }
            snapshot.eligible_markets_for_trading += 1;
        }

        snapshot
    }

    fn maybe_log_eligibility_snapshot(&mut self, asset: PriceAsset) {
        let snapshot = self.compute_eligibility_snapshot(asset);
        let changed = self
            .last_eligibility_snapshot_by_asset
            .get(&asset)
            .copied()
            != Some(snapshot);
        if !changed {
            return;
        }
        self.last_eligibility_snapshot_by_asset.insert(asset, snapshot);

        info!(
            asset = %asset,
            active_markets_for_asset = snapshot.active_markets_for_asset,
            blocked_missing_reference = snapshot.blocked_missing_reference,
            blocked_idempotency = snapshot.blocked_idempotency,
            eligible_markets_for_trading = snapshot.eligible_markets_for_trading,
            captured_reference_for_entry = self.price_feed_config.captured_reference_for_entry,
            "trading eligibility snapshot"
        );
        self.event_logger.try_log_value(
            "trading_eligibility",
            trading_eligibility_payload(
                asset,
                snapshot.active_markets_for_asset,
                snapshot.blocked_missing_reference,
                snapshot.blocked_idempotency,
                snapshot.eligible_markets_for_trading,
                self.price_feed_config.captured_reference_for_entry,
            ),
        );
    }

    /// Dispatch settlement resolution polling to a background task.
    /// Only the Gamma API network calls are spawned; everything else stays in the main loop.
    fn dispatch_settlement(&mut self, venue: Arc<dyn VenueClient>) {
        if self.settlement_in_flight {
            return;
        }

        let now = Utc::now();
        let due = self
            .pnl_tracker
            .due_condition_ids(now, SETTLEMENT_GRACE_SECS);

        // Filter by backoff — only poll conditions whose backoff has expired
        let to_poll: Vec<String> = due
            .into_iter()
            .filter(|cid| {
                !self
                    .resolution_poll_not_before
                    .get(cid)
                    .is_some_and(|next| now < *next)
            })
            .collect();

        if to_poll.is_empty() {
            return;
        }

        self.settlement_in_flight = true;
        self.last_settlement_check = now;

        let tx = self.settlement_result_tx.clone();
        self.pending_settlement_handle = Some(tokio::spawn(async move {
            let result = tokio::time::timeout(SCAN_TIMEOUT, async {
                let mut results = Vec::new();
                for cid in &to_poll {
                    let result = venue.get_market_resolution(cid).await;
                    results.push((cid.clone(), result));
                }
                results
            })
            .await;

            let results = match result {
                Ok(r) => r,
                Err(_) => {
                    warn!(
                        timeout_secs = SCAN_TIMEOUT.as_secs(),
                        conditions = to_poll.len(),
                        "settlement poll timed out"
                    );
                    Vec::new()
                }
            };
            if tx.send(results).await.is_err() {
                debug!("settlement poll result dropped: receiver closed");
            }
        }));
    }

    /// Process the pre-fetched settlement poll results in the main loop.
    /// Settlement uses only official Polymarket Gamma API resolutions — never
    /// self-resolved from spot prices. Positions wait until Gamma resolves them.
    fn apply_settlement_results(&mut self, poll_results: SettlementPollMsg) {
        let now = Utc::now();
        let mut resolutions_by_condition: HashMap<String, MarketResolution> = HashMap::new();

        for (condition_id, result) in poll_results {
            match result {
                Ok(Some(resolution)) => {
                    // Post-hoc validation: compare captured reference to official price_to_beat
                    if let Some(official) = resolution.official_price_to_beat {
                        if let Some((asset, window_start)) =
                            self.pnl_tracker.asset_and_window(&condition_id)
                        {
                            if let Some(captured) = self.reference_cache.get(asset, window_start) {
                                let drift = (captured.price - official).abs();
                                let drift_pct = if official > Decimal::ZERO {
                                    drift / official * Decimal::from(100)
                                } else {
                                    Decimal::ZERO
                                };
                                info!(
                                    condition_id = %condition_id,
                                    captured = %captured.price,
                                    official = %official,
                                    drift_pct = %drift_pct,
                                    source = ?captured.source,
                                    "reference price validation: captured vs official"
                                );
                            }
                        }
                    }

                    if resolution.resolved_yes_outcome.is_some() {
                        self.resolution_poll_not_before.remove(&condition_id);
                        resolutions_by_condition.insert(condition_id, resolution);
                    } else {
                        info!(
                            condition_id = %condition_id,
                            now_et = %fmt_et_log_time(now),
                            "resolution pending after expiry; waiting for official Polymarket outcome"
                        );
                        self.resolution_poll_not_before.insert(
                            condition_id,
                            now + chrono::Duration::seconds(RESOLUTION_POLL_INTERVAL_SECS),
                        );
                    }
                }
                Ok(None) => {
                    info!(
                        condition_id = %condition_id,
                        now_et = %fmt_et_log_time(now),
                        "resolution pending after expiry; market not yet found on closed pages"
                    );
                    self.resolution_poll_not_before.insert(
                        condition_id,
                        now + chrono::Duration::seconds(RESOLUTION_POLL_INTERVAL_SECS),
                    );
                }
                Err(e) => {
                    warn!(
                        condition_id = %condition_id,
                        error = %e,
                        "failed to fetch official market resolution"
                    );
                    self.resolution_poll_not_before.insert(
                        condition_id,
                        now + chrono::Duration::seconds(RESOLUTION_POLL_INTERVAL_SECS),
                    );
                }
            }
        }

        let settled = self.pnl_tracker.settle_expired_positions(
            now,
            SETTLEMENT_GRACE_SECS,
            &resolutions_by_condition,
        );
        if settled.is_empty() {
            return;
        }

        let mode_label = if self.paper_mode { "paper" } else { "live" };
        for rec in &settled {
            if let Some(cal) = self.calibration_by_asset.get_mut(&rec.asset) {
                cal.record(
                    rec.model_yes_prob,
                    rec.actual_yes_outcome,
                    rec.market_question.clone(),
                );
                let path = std::path::PathBuf::from("logs")
                    .join(format!("calibration_{}.json", rec.asset.symbol().to_lowercase()));
                if let Err(e) = cal.save(&path) {
                    warn!(
                        asset = %rec.asset,
                        error = %e,
                        "failed to persist calibration history"
                    );
                }
            }
            let trade_payload = closed_trade_payload(rec);
            self.event_logger
                .try_log_value("position_settled", trade_payload.clone());
            // Dashboard: closed event
            let mut closed_msg = trade_payload.clone();
            if let serde_json::Value::Object(ref mut m) = closed_msg {
                m.insert("t".into(), serde_json::json!("closed"));
                m.insert("ts".into(), serde_json::json!(Utc::now().timestamp_millis()));
            }
            self.push_dashboard_closed(closed_msg);
            if let Some(ref mut ts) = self.trade_sheet {
                ts.record_closed(rec);
            }
            info!(
                asset = %rec.asset,
                token_id = %rec.token_id,
                realized = %rec.realized_pnl,
                actual_yes = rec.actual_yes_outcome,
                mode = mode_label,
                "position settled"
            );
        }

        // Prune reference cache for settled markets
        let settled_keys: Vec<_> = settled
            .iter()
            .filter_map(|s| {
                let asset = s.asset;
                Some((asset, s.window_start?))
            })
            .collect();
        self.reference_cache.prune_settled(&settled_keys);

        if self.dashboard_enabled {
            let snapshot = self.pnl_tracker.snapshot();
            print_pnl_dashboard(&snapshot, mode_label, "settlement", Some(&settled));
            let snap_payload = pnl_snapshot_payload(&snapshot, "settlement");
            self.push_dashboard_snap(
                &snap_payload,
                self.pnl_tracker.open_position_cards(),
            );
            self.event_logger
                .try_log_value("pnl_snapshot", snap_payload);
        }
    }

    fn asset_tuning_for(&self, asset: PriceAsset) -> &TradingAssetTuning {
        match asset {
            PriceAsset::Btc => &self.config.asset_params.btc,
            PriceAsset::Eth => &self.config.asset_params.eth,
            PriceAsset::Xrp => &self.config.asset_params.xrp,
            PriceAsset::Sol => &self.config.asset_params.sol,
        }
    }

    fn prune_expired_markets(&mut self, now: chrono::DateTime<Utc>, phase: &str) -> usize {
        let open_cids = self.pnl_tracker.open_condition_ids();
        let pending_cids = self.paper_trader.pending_condition_ids();
        let before = self.active_markets.len();
        self.active_markets.retain(|market| {
            market.end_time > now
                || open_cids.contains(&market.condition_id)
                || pending_cids.contains(&market.condition_id)
        });
        let removed = before.saturating_sub(self.active_markets.len());
        if removed > 0 {
            info!(
                phase,
                removed,
                remaining = self.active_markets.len(),
                now_et = %fmt_et_log_time(now),
                "expired markets dropped from active set"
            );
        }
        removed
    }

    fn log_market_time_trace(
        &self,
        phase: &str,
        scanned_at: chrono::DateTime<Utc>,
        added_markets: usize,
        removed_markets: usize,
    ) {
        let mut window_5m = 0usize;
        let mut window_15m = 0usize;
        let mut other_windows = 0usize;
        let mut missing_start_time = 0usize;
        let mut expired_in_set = 0usize;

        for market in &self.active_markets {
            if market.end_time <= scanned_at {
                expired_in_set += 1;
                continue;
            }
            match market_window_bucket(market) {
                MarketWindowBucket::FiveMinute => window_5m += 1,
                MarketWindowBucket::FifteenMinute => window_15m += 1,
                MarketWindowBucket::Other => other_windows += 1,
                MarketWindowBucket::Unknown => missing_start_time += 1,
            }
        }

        info!(
            phase,
            scanned_at_et = %fmt_et_log_time(scanned_at),
            active_markets = self.active_markets.len(),
            added_markets,
            removed_markets,
            window_5m,
            window_15m,
            other_windows,
            missing_start_time,
            expired_in_set,
            "market time trace"
        );
    }

    fn dispatch_scan(&mut self, venue: Arc<dyn VenueClient>) {
        if self.scan_in_flight {
            return;
        }

        let scanned_at = Utc::now();
        self.last_scan = scanned_at;
        self.scan_in_flight = true;

        let scanner = self.scanner.clone();
        let tx = self.scan_result_tx.clone();
        self.pending_scan_handle = Some(tokio::spawn(async move {
            let result = match tokio::time::timeout(SCAN_TIMEOUT, scanner.scan(venue.as_ref()))
                .await
            {
                Ok(r) => r,
                Err(_) => {
                    warn!(
                        timeout_secs = SCAN_TIMEOUT.as_secs(),
                        "market scan timed out"
                    );
                    Err(ScannerError::GammaApi(format!(
                        "scan timed out after {}s",
                        SCAN_TIMEOUT.as_secs()
                    )))
                }
            };
            if tx.send((scanned_at, result)).await.is_err() {
                debug!("scan result dropped: receiver closed");
            }
        }));
    }

    fn apply_scan_result(
        &mut self,
        scanned_at: chrono::DateTime<Utc>,
        scan_result: Result<Vec<TargetMarket>, ScannerError>,
    ) {
        self.prune_expired_markets(scanned_at, "scan_pre");
        let previous_ids: HashSet<String> = self
            .active_markets
            .iter()
            .map(|m| m.condition_id.clone())
            .collect();

        match scan_result {
            Ok(markets) => {
                let incoming_ids: HashSet<String> =
                    markets.iter().map(|m| m.condition_id.clone()).collect();
                let added = incoming_ids.difference(&previous_ids).count();
                let removed = previous_ids.difference(&incoming_ids).count();
                self.active_markets = markets;
                let (btc, eth, xrp, sol, unknown) = market_asset_counts(&self.active_markets);
                self.event_logger.try_log_value(
                    "market_scan",
                    market_scan_payload(
                        &self.config.market_tags,
                        self.config.market_duration_max_secs,
                        self.active_markets.len(),
                        btc,
                        eth,
                        xrp,
                        sol,
                        unknown,
                        None,
                    ),
                );
                info!(
                    count = self.active_markets.len(),
                    btc, eth, xrp, sol, unknown, added, removed, "markets refreshed"
                );
                self.log_market_time_trace("scan_success", scanned_at, added, removed);
            }
            Err(e) => {
                let mut scan_error: Option<String> = None;
                match &e {
                    ScannerError::NoMarkets => {
                        // Important: do not keep stale markets if current-window scan is empty.
                        let cleared_stale = self.active_markets.len();
                        self.active_markets.clear();
                        warn!(
                            cleared_stale,
                            "scan returned no current markets; cleared active market set"
                        );
                        self.log_market_time_trace("scan_no_markets", scanned_at, 0, cleared_stale);
                    }
                    _ => {
                        let pruned = self.prune_expired_markets(scanned_at, "scan_error");
                        scan_error = Some(e.to_string());
                        error!(
                            error = %e,
                            active_markets = self.active_markets.len(),
                            pruned_expired = pruned,
                            "market scan failed, keeping non-expired markets"
                        );
                        self.log_market_time_trace("scan_error", scanned_at, 0, pruned);
                    }
                }
                let (btc, eth, xrp, sol, unknown) = market_asset_counts(&self.active_markets);
                self.event_logger.try_log_value(
                    "market_scan",
                    market_scan_payload(
                        &self.config.market_tags,
                        self.config.market_duration_max_secs,
                        self.active_markets.len(),
                        btc,
                        eth,
                        xrp,
                        sol,
                        unknown,
                        scan_error.as_deref(),
                    ),
                );
            }
        }
    }
}

fn market_asset_counts(markets: &[TargetMarket]) -> (usize, usize, usize, usize, usize) {
    let mut btc = 0usize;
    let mut eth = 0usize;
    let mut xrp = 0usize;
    let mut sol = 0usize;
    let mut unknown = 0usize;

    for market in markets {
        match PriceAsset::from_market_question(&market.question) {
            Some(PriceAsset::Btc) => btc += 1,
            Some(PriceAsset::Eth) => eth += 1,
            Some(PriceAsset::Xrp) => xrp += 1,
            Some(PriceAsset::Sol) => sol += 1,
            None => unknown += 1,
        }
    }

    (btc, eth, xrp, sol, unknown)
}

#[derive(Clone, Copy)]
enum MarketWindowBucket {
    FiveMinute,
    FifteenMinute,
    Other,
    Unknown,
}

fn market_window_bucket(market: &TargetMarket) -> MarketWindowBucket {
    let Some(start_time) = market.start_time else {
        return MarketWindowBucket::Unknown;
    };
    let duration_secs = (market.end_time - start_time).num_seconds();
    if (duration_secs - 300).abs() <= MARKET_WINDOW_TOLERANCE_SECS {
        return MarketWindowBucket::FiveMinute;
    }
    if (duration_secs - 900).abs() <= MARKET_WINDOW_TOLERANCE_SECS {
        return MarketWindowBucket::FifteenMinute;
    }
    MarketWindowBucket::Other
}

fn outcome_side_for_token(market: &TargetMarket, token_id: &str) -> OutcomeTokenSide {
    if token_id == market.token_id_yes {
        OutcomeTokenSide::Yes
    } else {
        OutcomeTokenSide::No
    }
}

fn effective_entry_reference_for_market(
    market: &TargetMarket,
    captured_reference_for_entry: bool,
    reference_cache: &ReferencePriceCache,
) -> Option<Decimal> {
    market.polymarket_price_to_beat.or_else(|| {
        if !captured_reference_for_entry {
            return None;
        }
        let asset = PriceAsset::from_market_question(&market.question)?;
        let start = market.start_time?;
        reference_cache.get(asset, start).map(|r| r.price)
    })
}

fn should_attempt_reference_capture(
    market: &TargetMarket,
    now: chrono::DateTime<Utc>,
    grace: chrono::Duration,
    reference_cache: &ReferencePriceCache,
) -> Option<(PriceAsset, chrono::DateTime<Utc>)> {
    let start = market.start_time?;
    if !is_within_capture_window(start, now, grace) {
        return None;
    }
    if market.polymarket_price_to_beat.is_some() {
        return None;
    }
    let asset = PriceAsset::from_market_question(&market.question)?;
    if reference_cache.get(asset, start).is_some() {
        return None;
    }
    Some((asset, start))
}

fn is_within_capture_window(
    start: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
    grace: chrono::Duration,
) -> bool {
    now >= start && now <= start + grace
}

fn should_skip_for_missing_reference(market: &TargetMarket, effective_ref: Option<Decimal>) -> bool {
    market.strike_price <= Decimal::ZERO && effective_ref.is_none()
}

fn time_until_next_scan(last_scan: chrono::DateTime<Utc>, interval: Duration) -> Duration {
    let elapsed = (Utc::now() - last_scan).to_std().unwrap_or(interval);
    interval.saturating_sub(elapsed)
}

fn time_until_next_dashboard(
    last_dashboard: chrono::DateTime<Utc>,
    interval: Duration,
) -> Duration {
    let elapsed = (Utc::now() - last_dashboard).to_std().unwrap_or(interval);
    interval.saturating_sub(elapsed)
}

fn time_until_next_settlement(
    last_settlement: chrono::DateTime<Utc>,
    interval: Duration,
) -> Duration {
    let elapsed = (Utc::now() - last_settlement).to_std().unwrap_or(interval);
    interval.saturating_sub(elapsed)
}

fn time_until_next_reference_capture(
    last_capture_probe: chrono::DateTime<Utc>,
    interval: Duration,
) -> Duration {
    let elapsed = (Utc::now() - last_capture_probe).to_std().unwrap_or(interval);
    interval.saturating_sub(elapsed)
}

fn fmt_et_log_time(ts: chrono::DateTime<Utc>) -> String {
    let et = to_et(ts);
    format!("{} {}", et.format("%Y-%m-%d %H:%M:%S"), et_abbreviation(ts))
}

/// Floor a timestamp down to the nearest 5-minute boundary (e.g. 9:27:34 → 9:25:00).
fn floor_to_five_minute_boundary(now: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    let secs = now.timestamp();
    let boundary_secs = (secs / 300) * 300;
    chrono::DateTime::from_timestamp(boundary_secs, 0).unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_market() -> TargetMarket {
        let now = Utc::now();
        TargetMarket {
            condition_id: "cond".to_string(),
            token_id_yes: "yes".to_string(),
            token_id_no: "no".to_string(),
            question: "Bitcoin Up or Down".to_string(),
            strike_price: Decimal::ZERO,
            start_time: Some(now - chrono::Duration::minutes(1)),
            end_time: now + chrono::Duration::minutes(5),
            current_yes_price: Decimal::new(50, 2),
            current_no_price: Decimal::new(50, 2),
            polymarket_price_to_beat: None,
            polymarket_price_to_beat_source: None,
            liquidity: Decimal::from(1_000),
            volume: Decimal::from(1_000),
        }
    }

    #[test]
    fn skip_when_updown_reference_missing_and_no_effective() {
        let market = sample_market();
        assert!(should_skip_for_missing_reference(&market, None));
    }

    #[test]
    fn do_not_skip_when_official_reference_present() {
        let mut market = sample_market();
        market.polymarket_price_to_beat = Some(Decimal::new(668996, 2));
        assert!(!should_skip_for_missing_reference(
            &market,
            market.polymarket_price_to_beat
        ));
    }

    #[test]
    fn do_not_skip_when_captured_reference_provides_effective() {
        let market = sample_market();
        // No official reference, but effective reference from cache
        assert!(!should_skip_for_missing_reference(
            &market,
            Some(Decimal::new(98000, 0))
        ));
    }

    #[test]
    fn capture_window_allows_exact_start() {
        let start = Utc::now();
        let now = start;
        assert!(is_within_capture_window(
            start,
            now,
            chrono::Duration::seconds(3)
        ));
    }

    #[test]
    fn capture_window_allows_exact_grace_boundary() {
        let start = Utc::now();
        let now = start + chrono::Duration::seconds(3);
        assert!(is_within_capture_window(
            start,
            now,
            chrono::Duration::seconds(3)
        ));
    }

    #[test]
    fn capture_window_rejects_before_start() {
        let start = Utc::now();
        let now = start - chrono::Duration::milliseconds(1);
        assert!(!is_within_capture_window(
            start,
            now,
            chrono::Duration::seconds(3)
        ));
    }

    #[test]
    fn capture_window_rejects_after_grace() {
        let start = Utc::now();
        let now = start + chrono::Duration::seconds(3) + chrono::Duration::milliseconds(1);
        assert!(!is_within_capture_window(
            start,
            now,
            chrono::Duration::seconds(3)
        ));
    }

    #[test]
    fn should_not_attempt_capture_when_official_reference_present() {
        let mut market = sample_market();
        market.start_time = Some(Utc::now());
        market.polymarket_price_to_beat = Some(Decimal::new(668996, 2));
        let cache = ReferencePriceCache::new();
        let out = should_attempt_reference_capture(
            &market,
            market.start_time.expect("start_time"),
            chrono::Duration::seconds(3),
            &cache,
        );
        assert!(out.is_none());
    }

    #[test]
    fn should_not_attempt_capture_when_cache_has_entry() {
        let mut market = sample_market();
        let start = Utc::now();
        market.start_time = Some(start);
        market.question = "Bitcoin Up or Down".to_string();
        let mut cache = ReferencePriceCache::new();
        cache.insert(
            PriceAsset::Btc,
            start,
            CapturedReference {
                price: Decimal::new(98000, 0),
                source: ReferenceSource::Chainlink,
                captured_at: start,
                window_start: start,
            },
        );
        let out = should_attempt_reference_capture(
            &market,
            start,
            chrono::Duration::seconds(3),
            &cache,
        );
        assert!(out.is_none());
    }
}
