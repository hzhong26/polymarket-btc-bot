pub mod international;
pub mod us;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::market_scanner::TargetMarket;

#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("order rejected: {0}")]
    OrderRejected(String),
    #[error("WebSocket error: {0}")]
    WebSocket(String),
    #[error("unsupported venue operation: {0}")]
    Unsupported(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueType {
    International,
    US,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub order_type: OrderType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    /// Good-til-cancelled limit order
    Gtc,
    /// Fill-or-kill market order
    Fok,
}

#[derive(Debug, Clone)]
pub struct OrderId(pub String);

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub token_id: String,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub tick_size: Decimal,
    pub min_order_size: Decimal,
    pub neg_risk: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct OrderBookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

impl OrderBook {
    /// Best bid price (highest bid).
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|l| l.price)
    }

    /// Best ask price (lowest ask).
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|l| l.price)
    }

    /// Midpoint price.
    pub fn midpoint(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
            _ => None,
        }
    }

    /// Estimate slippage cost for buying `size` units.
    /// Walks up the ask side of the book.
    pub fn estimate_buy_slippage(&self, size: Decimal) -> Option<Decimal> {
        if self.asks.is_empty() {
            return None;
        }
        let best_ask = self.asks[0].price;
        let mut remaining = size;
        let mut total_cost = Decimal::ZERO;

        for level in &self.asks {
            let fill = remaining.min(level.size);
            total_cost += fill * level.price;
            remaining -= fill;
            if remaining <= Decimal::ZERO {
                break;
            }
        }

        if remaining > Decimal::ZERO {
            return None; // Not enough liquidity
        }

        let avg_price = total_cost / size;
        Some(avg_price - best_ask)
    }

    /// Estimate slippage cost for selling `size` units.
    /// Walks down the bid side of the book.
    pub fn estimate_sell_slippage(&self, size: Decimal) -> Option<Decimal> {
        if self.bids.is_empty() {
            return None;
        }
        let best_bid = self.bids[0].price;
        let mut remaining = size;
        let mut total_cost = Decimal::ZERO;

        for level in &self.bids {
            let fill = remaining.min(level.size);
            total_cost += fill * level.price;
            remaining -= fill;
            if remaining <= Decimal::ZERO {
                break;
            }
        }

        if remaining > Decimal::ZERO {
            return None;
        }

        let avg_price = total_cost / size;
        Some(best_bid - avg_price)
    }
}

#[derive(Debug, Clone)]
pub struct FeeSchedule {
    pub taker_fee_bps: Decimal,
    pub maker_rebate_bps: Decimal,
}

impl FeeSchedule {
    pub fn taker_fee_rate(&self) -> Decimal {
        self.taker_fee_bps / Decimal::from(10_000)
    }

    pub fn maker_rebate_rate(&self) -> Decimal {
        self.maker_rebate_bps / Decimal::from(10_000)
    }
}

#[derive(Debug, Clone)]
pub enum OrderEvent {
    Filled {
        order_id: String,
        fill_price: Decimal,
        fill_amount: Decimal,
    },
    PartialFill {
        order_id: String,
        fill_price: Decimal,
        fill_amount: Decimal,
    },
    Cancelled {
        order_id: String,
    },
    Rejected {
        order_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct MarketFilter {
    pub tags: Vec<String>,
    pub max_duration_secs: u64,
    pub active_only: bool,
}

#[derive(Debug, Clone)]
pub struct MarketResolution {
    pub condition_id: String,
    pub resolved_yes_outcome: Option<bool>,
    pub official_final_price: Option<Decimal>,
    pub official_price_to_beat: Option<Decimal>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolution_source: String,
}

/// Venue-agnostic trading client interface.
/// Implemented for both International (CLOB + EIP-712) and US (Ed25519) venues.
#[async_trait]
pub trait VenueClient: Send + Sync {
    /// Get markets matching the filter criteria.
    async fn get_markets(&self, filter: &MarketFilter) -> Result<Vec<TargetMarket>, VenueError>;

    /// Get official settlement state for a market condition.
    async fn get_market_resolution(
        &self,
        condition_id: &str,
    ) -> Result<Option<MarketResolution>, VenueError>;

    /// Get the current orderbook for a token.
    async fn get_orderbook(&self, token_id: &str) -> Result<OrderBook, VenueError>;

    /// Place an order. Returns the order ID.
    async fn place_order(&self, order: &OrderRequest) -> Result<OrderId, VenueError>;

    /// Cancel a specific order.
    async fn cancel_order(&self, order_id: &OrderId) -> Result<(), VenueError>;

    /// Cancel all open orders.
    async fn cancel_all(&self) -> Result<(), VenueError>;

    /// Subscribe to fill events. Sends events to the provided channel.
    async fn subscribe_fills(
        &self,
        tx: tokio::sync::mpsc::Sender<OrderEvent>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), VenueError>;

    /// Get the current USDC balance.
    async fn get_balance(&self) -> Result<Decimal, VenueError>;

    /// Get the fee schedule for this venue.
    fn fee_schedule(&self) -> &FeeSchedule;

    /// Which venue type this client represents.
    fn venue_type(&self) -> VenueType;
}
