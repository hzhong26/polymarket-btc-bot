use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub venue: VenueConfig,
    pub price_feeds: PriceFeedConfig,
    pub trading: TradingConfig,
    pub risk: RiskConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
pub struct VenueConfig {
    pub venue_type: String,
    pub international: Option<InternationalVenueConfig>,
    pub us: Option<UsVenueConfig>,
}

#[derive(Debug, Deserialize)]
pub struct InternationalVenueConfig {
    pub clob_url: String,
    pub gamma_url: String,
    pub ws_url: String,
    pub chain_id: u64,
    pub proxy_wallet: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UsVenueConfig {
    pub api_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceFeedConfig {
    pub chainlink_rpc_url: String,
    pub chainlink_btc_usd_address: String,
    #[serde(default)]
    pub chainlink_eth_usd_address: Option<String>,
    #[serde(default)]
    pub chainlink_sol_usd_address: Option<String>,
    #[serde(default)]
    pub chainlink_xrp_usd_address: Option<String>,
    #[serde(default = "default_true")]
    pub captured_reference_for_entry: bool,
    #[serde(default)]
    pub chainlink_continuous_enabled: bool,
    pub binance_ws_url: String,
    pub coinbase_url: String,
    pub coingecko_url: String,
    pub cryptocompare_url: String,
    pub cryptocompare_api_key: Option<String>,
    pub poll_interval_ms: u64,
    pub staleness_threshold_ms: u64,
    #[serde(default = "default_coingecko_staleness_threshold_ms")]
    pub coingecko_staleness_threshold_ms: u64,
    pub anomaly_deviation_pct: Decimal,
    #[serde(default = "default_min_sources_per_asset")]
    pub min_sources_per_asset: u8,
}

impl PriceFeedConfig {
    /// Return the CoinGecko base URL (without query params) for the batched feed.
    /// Strips any existing query string so the feed can build its own `ids=...` param.
    pub fn coingecko_base_url(&self) -> String {
        match self.coingecko_url.split_once('?') {
            Some((base, _)) => base.to_string(),
            None => self.coingecko_url.clone(),
        }
    }

    /// Return the Chainlink aggregator contract address for a given asset, if configured.
    pub fn chainlink_address_for_asset(&self, asset: crate::price_feeds::PriceAsset) -> Option<&str> {
        use crate::price_feeds::PriceAsset;
        match asset {
            PriceAsset::Btc => Some(self.chainlink_btc_usd_address.as_str()),
            PriceAsset::Eth => self.chainlink_eth_usd_address.as_deref(),
            PriceAsset::Sol => self.chainlink_sol_usd_address.as_deref(),
            PriceAsset::Xrp => self.chainlink_xrp_usd_address.as_deref(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_coingecko_staleness_threshold_ms() -> u64 {
    45_000
}

fn default_min_sources_per_asset() -> u8 {
    2
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradingConfig {
    pub scan_interval_ms: u64,
    pub tick_interval_ms: u64,
    pub edge_threshold: Decimal,
    pub kelly_fraction: Decimal,
    pub uncertainty_haircut_max: Decimal,
    pub latency_penalty: Decimal,
    pub market_tags: Vec<String>,
    pub market_duration_max_secs: u64,
    #[serde(default = "default_min_confidence")]
    pub min_confidence: Decimal,
    #[serde(default = "default_max_spread_ratio")]
    pub max_spread_ratio: Decimal,
    #[serde(default = "default_min_liquidity")]
    pub min_liquidity: Decimal,
    #[serde(default = "default_min_time_to_expiry_secs")]
    pub min_time_to_expiry_secs: u64,
    #[serde(default = "default_paper_bankroll_usdc")]
    pub paper_bankroll_usdc: Decimal,
    #[serde(default = "default_dashboard_enabled")]
    pub dashboard_enabled: bool,
    #[serde(default = "default_dashboard_live_interval_ms")]
    pub dashboard_live_interval_ms: u64,
    #[serde(default = "default_calibration_min_samples")]
    pub calibration_min_samples: usize,
    #[serde(default = "default_vol_window_secs")]
    pub vol_window_secs: u64,
    #[serde(default = "default_ewma_lambda")]
    pub ewma_lambda: f64,
    #[serde(default = "default_fee_rate")]
    pub fee_rate: Decimal,
    #[serde(default = "default_fee_exponent")]
    pub fee_exponent: u32,
    #[serde(default)]
    pub asset_params: TradingAssetParams,
    /// Reject trades with net edge above this cap (None = no cap).
    #[serde(default)]
    pub max_net_edge: Option<Decimal>,
    /// Multiply edge threshold for YES-side trades (default 1.0 = no penalty).
    #[serde(default = "default_multiplier")]
    pub yes_edge_threshold_mult: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradingAssetTuning {
    #[serde(default = "default_multiplier")]
    pub edge_threshold_mult: Decimal,
    #[serde(default = "default_multiplier")]
    pub uncertainty_mult: Decimal,
    /// Per-asset EWMA lambda override. None = use global `ewma_lambda`.
    #[serde(default)]
    pub ewma_lambda: Option<f64>,
}

impl Default for TradingAssetTuning {
    fn default() -> Self {
        Self {
            edge_threshold_mult: Decimal::ONE,
            uncertainty_mult: Decimal::ONE,
            ewma_lambda: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradingAssetParams {
    #[serde(default)]
    pub btc: TradingAssetTuning,
    #[serde(default)]
    pub eth: TradingAssetTuning,
    #[serde(default)]
    pub xrp: TradingAssetTuning,
    #[serde(default)]
    pub sol: TradingAssetTuning,
}

impl TradingAssetParams {
    pub fn for_asset(&self, asset: crate::price_feeds::PriceAsset) -> &TradingAssetTuning {
        use crate::price_feeds::PriceAsset;
        match asset {
            PriceAsset::Btc => &self.btc,
            PriceAsset::Eth => &self.eth,
            PriceAsset::Xrp => &self.xrp,
            PriceAsset::Sol => &self.sol,
        }
    }
}

impl Default for TradingAssetParams {
    fn default() -> Self {
        Self {
            btc: TradingAssetTuning::default(),
            eth: TradingAssetTuning::default(),
            xrp: TradingAssetTuning::default(),
            sol: TradingAssetTuning::default(),
        }
    }
}

fn default_paper_bankroll_usdc() -> Decimal {
    Decimal::from(1000)
}

fn default_dashboard_enabled() -> bool {
    true
}

fn default_dashboard_live_interval_ms() -> u64 {
    5_000
}

fn default_min_confidence() -> Decimal {
    Decimal::new(80, 2)
}

fn default_max_spread_ratio() -> Decimal {
    Decimal::new(1, 2)
}

fn default_min_liquidity() -> Decimal {
    Decimal::from(1_000)
}

fn default_min_time_to_expiry_secs() -> u64 {
    60
}

fn default_calibration_min_samples() -> usize {
    30
}

fn default_vol_window_secs() -> u64 {
    300
}

fn default_ewma_lambda() -> f64 {
    0.94
}

fn default_fee_rate() -> Decimal {
    Decimal::new(25, 2) // 0.25
}

fn default_fee_exponent() -> u32 {
    2
}

fn default_multiplier() -> Decimal {
    Decimal::ONE
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_position_usdc: Decimal,
    pub max_total_exposure: Decimal,
    pub max_drawdown_pct: Decimal,
    pub cooldown_after_loss_ms: u64,
    pub max_consecutive_losses: u32,
    pub max_open_orders: u32,
    pub max_quote_age_ms: u64,
    pub ws_disconnect_action: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
    pub metrics_port: u16,
    #[serde(default)]
    pub jsonl: JsonlLoggingConfig,
    #[serde(default = "default_dashboard_port")]
    pub dashboard_port: u16,
}

fn default_dashboard_port() -> u16 {
    8080
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonlLoggingConfig {
    #[serde(default = "default_jsonl_enabled")]
    pub enabled: bool,
    #[serde(default = "default_jsonl_dir")]
    pub dir: String,
    #[serde(default = "default_jsonl_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_jsonl_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_no_trade_sample_rate")]
    pub no_trade_sample_rate: f64,
    #[serde(default = "default_orderbook_depth_levels")]
    pub orderbook_depth_levels: usize,
    #[serde(default = "default_jsonl_gzip_on_roll")]
    pub gzip_on_roll: bool,
    #[serde(default = "default_jsonl_prune_days")]
    pub prune_days: u32,
}

impl Default for JsonlLoggingConfig {
    fn default() -> Self {
        Self {
            enabled: default_jsonl_enabled(),
            dir: default_jsonl_dir(),
            flush_interval_ms: default_jsonl_flush_interval_ms(),
            channel_capacity: default_jsonl_channel_capacity(),
            no_trade_sample_rate: default_no_trade_sample_rate(),
            orderbook_depth_levels: default_orderbook_depth_levels(),
            gzip_on_roll: default_jsonl_gzip_on_roll(),
            prune_days: default_jsonl_prune_days(),
        }
    }
}

fn default_jsonl_enabled() -> bool {
    true
}

fn default_jsonl_dir() -> String {
    "logs".to_string()
}

fn default_jsonl_flush_interval_ms() -> u64 {
    1_000
}

fn default_jsonl_channel_capacity() -> usize {
    8_192
}

fn default_no_trade_sample_rate() -> f64 {
    0.15
}

fn default_orderbook_depth_levels() -> usize {
    3
}

fn default_jsonl_gzip_on_roll() -> bool {
    true
}

fn default_jsonl_prune_days() -> u32 {
    0
}
