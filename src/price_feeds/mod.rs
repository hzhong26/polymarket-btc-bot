pub mod aggregator;
pub mod binance;
pub mod chainlink;
pub mod coinbase;
pub mod coingecko;
pub mod cryptocompare;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum PriceFeedError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("WebSocket error: {0}")]
    WebSocket(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("all feeds stale or unavailable")]
    AllFeedsDown,
}

/// A single price observation from one source.
#[derive(Debug, Clone)]
pub struct PriceObservation {
    pub asset: PriceAsset,
    pub source: FeedSource,
    pub price: Decimal,
    pub timestamp: DateTime<Utc>,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
}

/// Supported crypto assets for market evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PriceAsset {
    Btc,
    Eth,
    Xrp,
    Sol,
}

impl PriceAsset {
    pub const ALL: [Self; 4] = [Self::Btc, Self::Eth, Self::Xrp, Self::Sol];

    pub fn symbol(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Eth => "ETH",
            Self::Xrp => "XRP",
            Self::Sol => "SOL",
        }
    }

    pub fn coinbase_product(self) -> &'static str {
        match self {
            Self::Btc => "BTC-USD",
            Self::Eth => "ETH-USD",
            Self::Xrp => "XRP-USD",
            Self::Sol => "SOL-USD",
        }
    }

    pub fn coingecko_id(self) -> &'static str {
        match self {
            Self::Btc => "bitcoin",
            Self::Eth => "ethereum",
            Self::Xrp => "ripple",
            Self::Sol => "solana",
        }
    }

    pub fn cryptocompare_fsym(self) -> &'static str {
        self.symbol()
    }

    pub fn from_market_question(question: &str) -> Option<Self> {
        let q = question.to_ascii_lowercase();
        if q.contains("bitcoin") || q.contains("btc") {
            Some(Self::Btc)
        } else if q.contains("ethereum") || q.contains(" eth ") || q.starts_with("eth ") {
            Some(Self::Eth)
        } else if q.contains("xrp") || q.contains("ripple") {
            Some(Self::Xrp)
        } else if q.contains("solana") || q.contains(" sol ") || q.starts_with("sol ") {
            Some(Self::Sol)
        } else {
            None
        }
    }
}

impl std::fmt::Display for PriceAsset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.symbol())
    }
}

impl FromStr for PriceAsset {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "BTC" => Ok(Self::Btc),
            "ETH" => Ok(Self::Eth),
            "XRP" => Ok(Self::Xrp),
            "SOL" => Ok(Self::Sol),
            _ => Err(()),
        }
    }
}

/// Identifies which price feed produced an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedSource {
    Chainlink,
    Binance,
    Coinbase,
    CoinGecko,
    CryptoCompare,
}

impl std::fmt::Display for FeedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chainlink => write!(f, "chainlink"),
            Self::Binance => write!(f, "binance"),
            Self::Coinbase => write!(f, "coinbase"),
            Self::CoinGecko => write!(f, "coingecko"),
            Self::CryptoCompare => write!(f, "cryptocompare"),
        }
    }
}

/// Aggregated price after combining multiple feeds.
/// Chainlink is the conditional primary (resolution-aligned) source for BTC.
/// Others serve as cross-checks.
#[derive(Debug, Clone)]
pub struct AggregatedPrice {
    /// Which asset this aggregate belongs to
    pub asset: PriceAsset,
    /// Canonical asset/USD spot price after aggregation
    pub spot_usd: Decimal,
    /// Timestamp of the aggregation
    pub timestamp: DateTime<Utc>,
    /// Number of non-stale sources that contributed
    pub num_sources: usize,
    /// Max - min across sources
    pub spread: Decimal,
    /// Confidence score: 1.0 = all agree, lower = divergence
    pub confidence: Decimal,
    /// Whether minimum source quorum was met for publishing this aggregate
    pub quorum_met: bool,
    /// Raw observations from each source
    pub observations: Vec<PriceObservation>,
}

/// Build an HTTP client with a proper User-Agent header.
/// Many APIs (e.g. CoinGecko) reject requests without one.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent("polymarket-btc-bot/0.1")
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build HTTP client")
}

/// Trait for individual price feed implementations.
#[async_trait]
pub trait PriceFeed: Send + Sync {
    fn source(&self) -> FeedSource;

    /// Start the feed. Sends observations to the provided sender.
    /// Runs until the cancellation token is triggered.
    async fn run(
        &self,
        tx: tokio::sync::mpsc::Sender<PriceObservation>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), PriceFeedError>;
}
