use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context};
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

use std::collections::HashMap;

use polymarket_btc_bot::bayesian::BayesianUpdater;
use polymarket_btc_bot::config;
use polymarket_btc_bot::edge::FeeModel;
use polymarket_btc_bot::executor::order_manager::OrderManager;
use polymarket_btc_bot::kelly::KellySizer;
use polymarket_btc_bot::market_scanner::gamma_scanner::GammaScanner;
use polymarket_btc_bot::price_feeds::aggregator::PriceAggregator;
use polymarket_btc_bot::price_feeds::{
    binance::BinanceFeed, chainlink::ChainlinkFeed, coinbase::CoinbaseFeed,
    coingecko::CoinGeckoFeed, cryptocompare::CryptoCompareFeed, PriceAsset, PriceFeed,
};
use polymarket_btc_bot::risk::clock::ClockGuard;
use polymarket_btc_bot::risk::kill_switch::KillSwitch;
use polymarket_btc_bot::risk::RiskManager;
use polymarket_btc_bot::timezone::{et_abbreviation, to_et};
use polymarket_btc_bot::dashboard_server::{make_dashboard_state, run_dashboard_server};
use polymarket_btc_bot::trading_loop::event_logger::{EventLoggerHandle, RunMetadata};
use polymarket_btc_bot::trading_loop::TradingLoop;
use polymarket_btc_bot::venue::international::InternationalClient;
use polymarket_btc_bot::venue::us::UsClient;
use polymarket_btc_bot::venue::VenueClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install rustls crypto provider (required by rustls 0.23+)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // Load environment
    dotenvy::dotenv().ok();

    let cli = parse_cli_args();

    if let Some(dirs) = cli.runs_dirs {
        return polymarket_btc_bot::analyze::list_runs(&dirs);
    }
    if let Some(dirs) = cli.backtest_dirs {
        return polymarket_btc_bot::backtest::run(&dirs, cli.run_filter.as_deref());
    }
    if let Some(dirs) = cli.analyze_dirs {
        return polymarket_btc_bot::analyze::run(&dirs, cli.run_filter.as_deref());
    }

    // Parse config
    let mut app_config = config::load_config(&cli.config_path).context("failed to load config")?;
    if cli.no_log {
        app_config.logging.jsonl.enabled = false;
    }
    validate_startup_config(&app_config)?;
    let config_debug = format!("{:?}", app_config);

    // Initialize tracing
    init_tracing(&app_config.logging);

    info!("polymarket-btc-bot v{}", env!("CARGO_PKG_VERSION"));

    // Check for --dry-run / --paper flag
    let paper_mode = cli.paper_mode;
    let paper_bankroll = app_config.trading.paper_bankroll_usdc;
    if paper_mode {
        info!(
            paper_bankroll_usdc = %paper_bankroll,
            "PAPER TRADING MODE — no real orders will be placed"
        );
    }

    // Create venue client
    let venue: Arc<dyn VenueClient> = match app_config.venue.venue_type.as_str() {
        "international" => {
            let venue_config = app_config
                .venue
                .international
                .context("missing [venue.international] config")?;
            Arc::new(InternationalClient::new(venue_config)?)
        }
        "us" => {
            let venue_config = app_config.venue.us.context("missing [venue.us] config")?;
            Arc::new(UsClient::new(venue_config)?)
        }
        other => anyhow::bail!("unknown venue type: {}", other),
    };

    // Create shutdown token
    let shutdown = tokio_util::sync::CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    // Handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("ctrl+c received, initiating shutdown");
        shutdown_clone.cancel();
    });

    // Create channels
    let (price_obs_tx, price_obs_rx) = tokio::sync::mpsc::channel(256);
    let (aggregated_tx, aggregated_rx) = tokio::sync::broadcast::channel(128);
    let (order_event_tx, order_event_rx) = tokio::sync::mpsc::channel(64);

    // Spawn price feeds
    let (feeds, has_cryptocompare_key) = build_price_feeds(&app_config.price_feeds);
    if !has_cryptocompare_key {
        info!("CryptoCompare API key not configured — CryptoCompare feeds disabled");
    }

    info!(
        feed_tasks = feeds.len(),
        min_sources_per_asset = app_config.price_feeds.min_sources_per_asset,
        staleness_ms = app_config.price_feeds.staleness_threshold_ms,
        coingecko_staleness_ms = app_config.price_feeds.coingecko_staleness_threshold_ms,
        cryptocompare_enabled = has_cryptocompare_key,
        "price feed wiring initialized"
    );

    for feed in feeds {
        let tx = price_obs_tx.clone();
        let token = shutdown.clone();
        let source = feed.source();
        tokio::spawn(async move {
            info!(source = %source, "starting price feed");
            if let Err(e) = feed.run(tx, token).await {
                error!(source = %source, error = %e, "price feed exited with error");
            }
        });
    }
    drop(price_obs_tx); // Drop sender so aggregator can detect all feeds closed

    // Spawn price aggregator
    let staleness = Duration::from_millis(app_config.price_feeds.staleness_threshold_ms);
    let coingecko_staleness =
        Duration::from_millis(app_config.price_feeds.coingecko_staleness_threshold_ms);
    let anomaly_pct = app_config.price_feeds.anomaly_deviation_pct;
    let min_sources = app_config.price_feeds.min_sources_per_asset as usize;
    let non_btc_max_sources = if has_cryptocompare_key { 4 } else { 3 };
    let btc_max_sources = non_btc_max_sources + 1;
    if min_sources > non_btc_max_sources {
        warn!(
            min_sources_per_asset = min_sources,
            non_btc_max_sources,
            "min_sources_per_asset exceeds configured non-BTC feed count; ETH/XRP/SOL aggregation may stay empty"
        );
    }
    if min_sources > btc_max_sources {
        warn!(
            min_sources_per_asset = min_sources,
            btc_max_sources,
            "min_sources_per_asset exceeds configured BTC feed count; BTC aggregation may stay empty"
        );
    }
    let aggregated_tx_clone = aggregated_tx.clone();
    drop(aggregated_tx); // Only the aggregator task owns the sender — if it exits, the channel closes
    let shutdown_agg = shutdown.clone();
    tokio::spawn(async move {
        let mut aggregator =
            PriceAggregator::new(staleness, coingecko_staleness, anomaly_pct, min_sources);
        let mut rx = price_obs_rx;
        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Some(obs) => {
                            if let Some(agg) = aggregator.update(obs) {
                                let _ = aggregated_tx_clone.send(agg);
                            }
                        }
                        None => {
                            warn!("all price feed channels closed");
                            break;
                        }
                    }
                }
                _ = shutdown_agg.cancelled() => {
                    break;
                }
            }
        }
    });

    // Subscribe to venue fill events
    let venue_ref: &dyn VenueClient = venue.as_ref();
    if !paper_mode {
        let fill_tx = order_event_tx.clone();
        let fill_shutdown = shutdown.clone();
        let fill_venue = Arc::clone(&venue);
        tokio::spawn(async move {
            if let Err(e) = fill_venue.subscribe_fills(fill_tx, fill_shutdown).await {
                error!(error = %e, "failed to subscribe to fill events");
            }
        });
    }
    drop(order_event_tx);

    // Build trading components
    let scanner = GammaScanner::new(app_config.trading.clone());

    let tick_secs = app_config.price_feeds.poll_interval_ms as f64 / 1000.0;
    let vol_window_obs = (app_config.trading.vol_window_secs as f64 / tick_secs).ceil() as usize;
    let bayesian_by_asset: HashMap<PriceAsset, BayesianUpdater> = PriceAsset::ALL
        .into_iter()
        .map(|asset| {
            let lambda = app_config
                .trading
                .asset_params
                .for_asset(asset)
                .ewma_lambda
                .unwrap_or(app_config.trading.ewma_lambda);
            (asset, BayesianUpdater::new(vol_window_obs, 1.0, tick_secs, 0.80, lambda))
        })
        .collect();

    let kelly = KellySizer::new(
        app_config.trading.kelly_fraction.to_f64().unwrap_or(0.25),
        app_config.trading.edge_threshold.to_f64().unwrap_or(0.08),
        app_config
            .trading
            .uncertainty_haircut_max
            .to_f64()
            .unwrap_or(0.5),
    );

    let executor = OrderManager::new();

    let starting_balance = if paper_mode {
        paper_bankroll
    } else {
        venue_ref.get_balance().await.unwrap_or_default()
    };
    let mut risk_config = app_config.risk.clone();
    if !paper_mode {
        risk_config.apply_live_overrides();
    }
    let risk_manager = RiskManager::new(risk_config, starting_balance);

    let fee_model = FeeModel::with_dynamic(
        app_config.trading.fee_rate,
        app_config.trading.fee_exponent,
        app_config.trading.latency_penalty,
    );

    let clock_guard = ClockGuard::new(app_config.risk.max_quote_age_ms);
    let kill_switch = KillSwitch::new(&app_config.risk.ws_disconnect_action);
    let run_metadata = RunMetadata::new(paper_mode, venue_ref.venue_type(), &config_debug);
    let mut jsonl_config = app_config.logging.jsonl.clone();
    if !paper_mode {
        jsonl_config.dir = format!("{}/live", jsonl_config.dir);
    }
    let event_logger = EventLoggerHandle::new(jsonl_config, run_metadata);

    // Spawn browser dashboard server
    let dashboard_state = make_dashboard_state();
    {
        let state_clone = Arc::clone(&dashboard_state);
        let port = if paper_mode {
            app_config.logging.dashboard_port
        } else {
            app_config.logging.live_dashboard_port
        };
        tokio::spawn(async move {
            run_dashboard_server(state_clone, port).await;
        });
    }

    // Build and run the trading loop
    let mut trading_loop = TradingLoop::new(
        app_config.trading,
        app_config.price_feeds,
        scanner,
        bayesian_by_asset,
        kelly,
        executor,
        risk_manager,
        fee_model,
        clock_guard,
        kill_switch,
        paper_mode,
        paper_bankroll,
        starting_balance,
        event_logger,
        dashboard_state,
    );

    info!("starting trading loop");
    if let Err(e) = trading_loop
        .run(venue.clone(), aggregated_rx, order_event_rx, shutdown)
        .await
    {
        error!(error = %e, "trading loop exited with error");
    }

    info!("bot shut down cleanly");
    Ok(())
}

fn init_tracing(config: &config::LoggingConfig) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.level));
    let timer = EasternTimeFormatter;

    match config.format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_timer(timer)
                .with_env_filter(env_filter)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .pretty()
                .with_timer(timer)
                .with_env_filter(env_filter)
                .init();
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct EasternTimeFormatter;

impl FormatTime for EasternTimeFormatter {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        let now_utc = Utc::now();
        let now_et = to_et(now_utc);
        write!(
            w,
            "{} {}",
            now_et.format("%Y-%m-%dT%H:%M:%S%.6f"),
            et_abbreviation(now_utc)
        )
    }
}

#[derive(Debug)]
struct CliArgs {
    config_path: String,
    paper_mode: bool,
    no_log: bool,
    analyze_dirs: Option<Vec<String>>,
    runs_dirs: Option<Vec<String>>,
    backtest_dirs: Option<Vec<String>>,
    run_filter: Option<String>,
}

fn parse_cli_args() -> CliArgs {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // Extract --run <value> if present (used with --analyze or --runs)
    let mut run_filter: Option<String> = None;
    let mut skip_next = false;
    let mut args_without_run: Vec<String> = Vec::new();
    for (i, arg) in args.iter().enumerate() {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "--run" {
            if let Some(val) = args.get(i + 1) {
                run_filter = Some(val.clone());
                skip_next = true;
                continue;
            }
        }
        args_without_run.push(arg.clone());
    }

    let runs_mode = args_without_run.iter().any(|a| a == "--runs");
    if runs_mode {
        let dirs: Vec<String> = args_without_run
            .iter()
            .filter(|a| !a.starts_with("--"))
            .cloned()
            .collect();
        return CliArgs {
            config_path: String::new(),
            paper_mode: false,
            no_log: false,
            analyze_dirs: None,
            runs_dirs: Some(dirs),
            backtest_dirs: None,
            run_filter: None,
        };
    }

    let backtest_mode = args_without_run.iter().any(|a| a == "--backtest");
    if backtest_mode {
        let dirs: Vec<String> = args_without_run
            .iter()
            .filter(|a| !a.starts_with("--"))
            .cloned()
            .collect();
        return CliArgs {
            config_path: String::new(),
            paper_mode: false,
            no_log: false,
            analyze_dirs: None,
            runs_dirs: None,
            backtest_dirs: Some(dirs),
            run_filter,
        };
    }

    let analyze_mode = args_without_run.iter().any(|a| a == "--analyze");
    if analyze_mode {
        // Collect non-flag args as log dirs
        let dirs: Vec<String> = args_without_run
            .iter()
            .filter(|a| !a.starts_with("--"))
            .cloned()
            .collect();
        return CliArgs {
            config_path: String::new(),
            paper_mode: false,
            no_log: false,
            analyze_dirs: Some(dirs),
            runs_dirs: None,
            backtest_dirs: None,
            run_filter,
        };
    }

    let config_path = args_without_run
        .iter()
        .find(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "config.toml".into());
    let paper_mode = args_without_run.iter().any(|a| a == "--dry-run" || a == "--paper");
    let no_log = args_without_run.iter().any(|a| a == "--no-log");

    CliArgs {
        config_path,
        paper_mode,
        no_log,
        analyze_dirs: None,
        runs_dirs: None,
        backtest_dirs: None,
        run_filter: None,
    }
}

fn validate_startup_config(cfg: &config::AppConfig) -> anyhow::Result<()> {
    ensure!(
        cfg.price_feeds.poll_interval_ms > 0,
        "price_feeds.poll_interval_ms must be > 0"
    );
    ensure!(
        cfg.price_feeds.staleness_threshold_ms > 0,
        "price_feeds.staleness_threshold_ms must be > 0"
    );
    ensure!(
        cfg.price_feeds.coingecko_staleness_threshold_ms > 0,
        "price_feeds.coingecko_staleness_threshold_ms must be > 0"
    );
    ensure!(
        cfg.price_feeds.min_sources_per_asset > 0,
        "price_feeds.min_sources_per_asset must be > 0"
    );
    ensure!(
        cfg.price_feeds.min_sources_per_asset <= 5,
        "price_feeds.min_sources_per_asset must be <= 5"
    );
    ensure!(
        cfg.price_feeds.anomaly_deviation_pct > Decimal::ZERO,
        "price_feeds.anomaly_deviation_pct must be > 0"
    );
    ensure!(
        cfg.trading.scan_interval_ms > 0,
        "trading.scan_interval_ms must be > 0"
    );
    ensure!(
        cfg.trading.tick_interval_ms > 0,
        "trading.tick_interval_ms must be > 0"
    );
    ensure!(
        cfg.trading.market_duration_max_secs > 0,
        "trading.market_duration_max_secs must be > 0"
    );
    ensure!(
        !cfg.trading.market_tags.is_empty(),
        "trading.market_tags must contain at least one tag"
    );
    for (idx, tag) in cfg.trading.market_tags.iter().enumerate() {
        ensure!(
            !tag.trim().is_empty(),
            "trading.market_tags[{idx}] must not be empty or whitespace"
        );
    }
    ensure!(
        cfg.trading.min_time_to_expiry_secs > 0,
        "trading.min_time_to_expiry_secs must be > 0"
    );
    ensure!(
        cfg.trading.min_confidence >= rust_decimal::Decimal::ZERO
            && cfg.trading.min_confidence <= rust_decimal::Decimal::ONE,
        "trading.min_confidence must be in [0, 1]"
    );
    ensure!(
        cfg.trading.max_spread_ratio >= rust_decimal::Decimal::ZERO
            && cfg.trading.max_spread_ratio <= rust_decimal::Decimal::ONE,
        "trading.max_spread_ratio must be in [0, 1]"
    );
    ensure!(
        cfg.trading.min_liquidity >= rust_decimal::Decimal::ZERO,
        "trading.min_liquidity must be >= 0"
    );
    for (asset, params) in [
        ("btc", &cfg.trading.asset_params.btc),
        ("eth", &cfg.trading.asset_params.eth),
        ("xrp", &cfg.trading.asset_params.xrp),
        ("sol", &cfg.trading.asset_params.sol),
    ] {
        ensure!(
            params.edge_threshold_mult > rust_decimal::Decimal::ZERO,
            "trading.asset_params.{}.edge_threshold_mult must be > 0",
            asset
        );
        ensure!(
            params.uncertainty_mult >= rust_decimal::Decimal::ZERO,
            "trading.asset_params.{}.uncertainty_mult must be >= 0",
            asset
        );
        if let Some(lambda) = params.ewma_lambda {
            ensure!(
                lambda > 0.0 && lambda < 1.0,
                "trading.asset_params.{}.ewma_lambda must be in (0, 1)",
                asset
            );
        }
    }
    ensure!(
        cfg.trading.paper_bankroll_usdc > rust_decimal::Decimal::ZERO,
        "trading.paper_bankroll_usdc must be > 0"
    );
    ensure!(
        cfg.trading.ewma_lambda > 0.0 && cfg.trading.ewma_lambda < 1.0,
        "trading.ewma_lambda must be in (0, 1)"
    );
    if cfg.trading.dashboard_enabled {
        ensure!(
            cfg.trading.dashboard_live_interval_ms > 0,
            "trading.dashboard_live_interval_ms must be > 0 when dashboard_enabled=true"
        );
    }
    if cfg.logging.jsonl.enabled {
        ensure!(
            !cfg.logging.jsonl.dir.trim().is_empty(),
            "logging.jsonl.dir must not be empty when logging.jsonl.enabled=true"
        );
        ensure!(
            cfg.logging.jsonl.flush_interval_ms > 0,
            "logging.jsonl.flush_interval_ms must be > 0 when logging.jsonl.enabled=true"
        );
        ensure!(
            cfg.logging.jsonl.channel_capacity > 0,
            "logging.jsonl.channel_capacity must be > 0 when logging.jsonl.enabled=true"
        );
        ensure!(
            (0.0..=1.0).contains(&cfg.logging.jsonl.no_trade_sample_rate),
            "logging.jsonl.no_trade_sample_rate must be in [0, 1]"
        );
        ensure!(
            cfg.logging.jsonl.orderbook_depth_levels > 0,
            "logging.jsonl.orderbook_depth_levels must be > 0 when logging.jsonl.enabled=true"
        );
    }

    ensure!(
        !cfg.price_feeds.chainlink_rpc_url.trim().is_empty(),
        "price_feeds.chainlink_rpc_url is empty"
    );
    ensure!(
        !cfg.price_feeds.binance_ws_url.trim().is_empty(),
        "price_feeds.binance_ws_url is empty"
    );
    ensure!(
        !cfg.price_feeds.chainlink_btc_usd_address.trim().is_empty(),
        "price_feeds.chainlink_btc_usd_address is empty"
    );
    ensure!(
        !cfg.price_feeds.coinbase_url.trim().is_empty(),
        "price_feeds.coinbase_url is empty"
    );
    ensure!(
        cfg.price_feeds.coinbase_url.contains("BTC-USD"),
        "price_feeds.coinbase_url must contain BTC-USD placeholder"
    );
    ensure!(
        !cfg.price_feeds.coingecko_url.trim().is_empty(),
        "price_feeds.coingecko_url is empty"
    );
    ensure!(
        !cfg.price_feeds.cryptocompare_url.trim().is_empty(),
        "price_feeds.cryptocompare_url is empty"
    );
    ensure!(
        cfg.price_feeds.cryptocompare_url.contains("fsym=BTC"),
        "price_feeds.cryptocompare_url must contain fsym=BTC placeholder"
    );
    if let Some(api_key) = cfg.price_feeds.cryptocompare_api_key.as_deref() {
        ensure!(
            !api_key.trim().is_empty(),
            "price_feeds.cryptocompare_api_key must not be empty when provided"
        );
    }

    Ok(())
}

fn cryptocompare_api_key(price_feeds: &config::PriceFeedConfig) -> Option<&str> {
    price_feeds
        .cryptocompare_api_key
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn build_price_feeds(price_feeds: &config::PriceFeedConfig) -> (Vec<Box<dyn PriceFeed>>, bool) {
    let poll_interval = Duration::from_millis(price_feeds.poll_interval_ms);
    let cryptocompare_api_key = cryptocompare_api_key(price_feeds);

    let mut feeds: Vec<Box<dyn PriceFeed>> = Vec::new();

    if price_feeds.chainlink_continuous_enabled {
        for asset in PriceAsset::ALL {
            if let Some(address) = price_feeds.chainlink_address_for_asset(asset) {
                feeds.push(Box::new(ChainlinkFeed::new(
                    asset,
                    &price_feeds.chainlink_rpc_url,
                    address,
                    poll_interval,
                )));
            }
        }
    }

    feeds.push(Box::new(BinanceFeed::new(&price_feeds.binance_ws_url)));
    // Single batched CoinGecko feed for all assets (1 request per poll)
    feeds.push(Box::new(CoinGeckoFeed::new(
        &price_feeds.coingecko_base_url(),
        poll_interval,
    )));

    for asset in PriceAsset::ALL {
        feeds.push(Box::new(CoinbaseFeed::new(
            asset,
            &coinbase_url_for_asset(&price_feeds.coinbase_url, asset),
            poll_interval,
        )));
    }

    if let Some(api_key) = cryptocompare_api_key {
        for asset in PriceAsset::ALL {
            let url = cryptocompare_url_for_asset(&price_feeds.cryptocompare_url, asset);
            let url_with_key = format!("{}&api_key={}", url, api_key);
            feeds.push(Box::new(CryptoCompareFeed::new(
                asset,
                &url_with_key,
                poll_interval,
            )));
        }
    }

    (feeds, cryptocompare_api_key.is_some())
}

fn coinbase_url_for_asset(base: &str, asset: PriceAsset) -> String {
    base.replace("BTC-USD", asset.coinbase_product())
}

fn cryptocompare_url_for_asset(base: &str, asset: PriceAsset) -> String {
    base.replace("fsym=BTC", &format!("fsym={}", asset.cryptocompare_fsym()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_btc_bot::price_feeds::FeedSource;

    fn valid_app_config() -> config::AppConfig {
        config::AppConfig {
            venue: config::VenueConfig {
                venue_type: "international".into(),
                international: Some(config::InternationalVenueConfig {
                    clob_url: "https://clob.polymarket.com".into(),
                    gamma_url: "https://gamma-api.polymarket.com".into(),
                    ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/".into(),
                    chain_id: 137,
                    proxy_wallet: None,
                }),
                us: Some(config::UsVenueConfig {
                    api_url: "https://api.polymarket.com".into(),
                }),
            },
            price_feeds: valid_price_feed_config(None),
            trading: config::TradingConfig {
                scan_interval_ms: 30_000,
                tick_interval_ms: 500,
                edge_threshold: Decimal::new(11, 2),
                kelly_fraction: Decimal::new(12, 2),
                uncertainty_haircut_max: Decimal::new(5, 1),
                latency_penalty: Decimal::new(5, 3),
                market_tags: vec![
                    "bitcoin".to_string(),
                    "ethereum".to_string(),
                    "xrp".to_string(),
                    "solana".to_string(),
                ],
                market_duration_max_secs: 300,
                min_confidence: Decimal::new(80, 2),
                max_spread_ratio: Decimal::new(1, 2),
                min_liquidity: Decimal::from(1_000),
                min_time_to_expiry_secs: 60,
                paper_bankroll_usdc: Decimal::from(1_000),
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
                obi_weight: 0.0,
                obi_depth_levels: 5,
            },
            risk: config::RiskConfig {
                max_position_usdc: Decimal::from(500),
                max_total_exposure: Decimal::from(2_000),
                max_drawdown_pct: Decimal::new(15, 2),
                cooldown_after_loss_ms: 60_000,
                max_consecutive_losses: 3,
                max_open_orders: 10,
                max_quote_age_ms: 5_000,
                ws_disconnect_action: "cancel-all".into(),
                live: None,
            },
            logging: config::LoggingConfig {
                level: "info".into(),
                format: "pretty".into(),
                metrics_port: 9090,
                jsonl: config::JsonlLoggingConfig::default(),
                dashboard_port: 8080,
                live_dashboard_port: 8081,
            },
        }
    }

    fn valid_price_feed_config(cryptocompare_api_key: Option<&str>) -> config::PriceFeedConfig {
        config::PriceFeedConfig {
            chainlink_rpc_url: "https://1rpc.io/matic".into(),
            chainlink_btc_usd_address: "0xc907E116054Ad103354f2D350FD2514433D57F6f".into(),
            chainlink_eth_usd_address: None,
            chainlink_sol_usd_address: None,
            chainlink_xrp_usd_address: None,
            captured_reference_for_entry: true,
            chainlink_continuous_enabled: false,
            binance_ws_url:
                "wss://stream.binance.us:9443/stream?streams=btcusdt@ticker/ethusdt@ticker/xrpusdt@ticker/solusdt@ticker"
                    .into(),
            coinbase_url: "https://api.coinbase.com/v2/prices/BTC-USD/spot".into(),
            coingecko_url:
                "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
                    .into(),
            cryptocompare_url: "https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD"
                .into(),
            cryptocompare_api_key: cryptocompare_api_key.map(str::to_string),
            poll_interval_ms: 3_000,
            staleness_threshold_ms: 10_000,
            coingecko_staleness_threshold_ms: 45_000,
            anomaly_deviation_pct: Decimal::new(20, 1),
            min_sources_per_asset: 2,
        }
    }

    #[test]
    fn build_price_feeds_without_cryptocompare_key_no_continuous_chainlink() {
        let config = valid_price_feed_config(None);
        assert!(!config.chainlink_continuous_enabled);
        let (feeds, has_cryptocompare_key) = build_price_feeds(&config);
        assert!(!has_cryptocompare_key);

        let sources: Vec<FeedSource> = feeds.iter().map(|f| f.source()).collect();
        // Chainlink continuous is off, so no Chainlink feed
        assert_eq!(
            sources,
            vec![
                FeedSource::Binance,
                FeedSource::CoinGecko,
                FeedSource::Coinbase,
                FeedSource::Coinbase,
                FeedSource::Coinbase,
                FeedSource::Coinbase,
            ]
        );
    }

    #[test]
    fn build_price_feeds_with_continuous_chainlink_enabled() {
        let mut config = valid_price_feed_config(None);
        config.chainlink_continuous_enabled = true;
        let (feeds, _) = build_price_feeds(&config);

        let sources: Vec<FeedSource> = feeds.iter().map(|f| f.source()).collect();
        assert_eq!(sources[0], FeedSource::Chainlink);
    }

    #[test]
    fn build_price_feeds_with_cryptocompare_key_has_expected_order() {
        let mut config = valid_price_feed_config(Some("api-key"));
        config.chainlink_continuous_enabled = true;
        let (feeds, has_cryptocompare_key) = build_price_feeds(&config);
        assert!(has_cryptocompare_key);

        let sources: Vec<FeedSource> = feeds.iter().map(|f| f.source()).collect();
        assert_eq!(
            sources,
            vec![
                FeedSource::Chainlink,
                FeedSource::Binance,
                FeedSource::CoinGecko,
                FeedSource::Coinbase,
                FeedSource::Coinbase,
                FeedSource::Coinbase,
                FeedSource::Coinbase,
                FeedSource::CryptoCompare,
                FeedSource::CryptoCompare,
                FeedSource::CryptoCompare,
                FeedSource::CryptoCompare,
            ]
        );
        assert_eq!(
            sources
                .iter()
                .filter(|source| **source == FeedSource::CoinGecko)
                .count(),
            1
        );
    }

    #[test]
    fn validate_startup_config_rejects_non_positive_anomaly_threshold() {
        let mut config = valid_app_config();
        config.price_feeds.anomaly_deviation_pct = Decimal::ZERO;
        let err = validate_startup_config(&config).expect_err("validation should fail");
        assert!(err
            .to_string()
            .contains("price_feeds.anomaly_deviation_pct must be > 0"));

        config.price_feeds.anomaly_deviation_pct = Decimal::new(-1, 0);
        let err = validate_startup_config(&config).expect_err("validation should fail");
        assert!(err
            .to_string()
            .contains("price_feeds.anomaly_deviation_pct must be > 0"));
    }

    #[test]
    fn validate_startup_config_rejects_empty_cryptocompare_api_key() {
        let mut config = valid_app_config();
        config.price_feeds.cryptocompare_api_key = Some("   ".to_string());
        let err = validate_startup_config(&config).expect_err("validation should fail");
        assert!(err
            .to_string()
            .contains("price_feeds.cryptocompare_api_key must not be empty when provided"));
    }

    #[test]
    fn url_helpers_replace_asset_placeholders() {
        assert_eq!(
            coinbase_url_for_asset(
                "https://api.coinbase.com/v2/prices/BTC-USD/spot",
                PriceAsset::Eth
            ),
            "https://api.coinbase.com/v2/prices/ETH-USD/spot"
        );
        assert_eq!(
            cryptocompare_url_for_asset(
                "https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD",
                PriceAsset::Sol
            ),
            "https://min-api.cryptocompare.com/data/price?fsym=SOL&tsyms=USD"
        );
    }
}
