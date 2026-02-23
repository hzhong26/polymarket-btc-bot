pub mod gamma_scanner;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

#[derive(Debug, thiserror::Error)]
pub enum ScannerError {
    #[error("Gamma API error: {0}")]
    GammaApi(String),
    #[error("no matching markets found")]
    NoMarkets,
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}

/// A tradeable crypto prediction market on Polymarket.
#[derive(Debug, Clone)]
pub struct TargetMarket {
    /// Condition ID for the market (used for WS subscriptions)
    pub condition_id: String,
    /// CLOB token ID for the Yes outcome
    pub token_id_yes: String,
    /// CLOB token ID for the No outcome
    pub token_id_no: String,
    /// Market question text from Gamma.
    pub question: String,
    /// Parsed strike threshold when present.
    /// For Up/Down markets this can be zero.
    pub strike_price: Decimal,
    /// Market interval start time when available (used by Up/Down modeling).
    pub start_time: Option<DateTime<Utc>>,
    /// Market resolution time
    pub end_time: DateTime<Utc>,
    /// Current Yes outcome price from Gamma
    pub current_yes_price: Decimal,
    /// Current No outcome price from Gamma
    pub current_no_price: Decimal,
    /// Official Polymarket "price to beat" (when published).
    pub polymarket_price_to_beat: Option<Decimal>,
    /// Where `polymarket_price_to_beat` was extracted from.
    pub polymarket_price_to_beat_source: Option<String>,
    /// Total liquidity in USDC
    pub liquidity: Decimal,
    /// Total trading volume
    pub volume: Decimal,
}
