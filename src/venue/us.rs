use async_trait::async_trait;
use rust_decimal::Decimal;
use tracing::warn;

use crate::config::UsVenueConfig;
use crate::market_scanner::TargetMarket;

use super::{
    FeeSchedule, MarketFilter, MarketResolution, OrderBook, OrderEvent, OrderId, OrderRequest,
    VenueClient, VenueError, VenueType,
};

/// Polymarket US venue client.
/// Uses Ed25519 API key authentication against the US portal API.
pub struct UsClient {
    _http: reqwest::Client,
    _config: UsVenueConfig,
    fee_schedule: FeeSchedule,
    // Ed25519 signing key loaded from env
}

impl UsClient {
    pub fn new(config: UsVenueConfig) -> Result<Self, VenueError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(VenueError::Http)?;

        let fee_schedule = FeeSchedule {
            taker_fee_bps: Decimal::from(200),
            maker_rebate_bps: Decimal::ZERO,
        };

        Ok(Self {
            _http: http,
            _config: config,
            fee_schedule,
        })
    }
}

#[async_trait]
impl VenueClient for UsClient {
    async fn get_markets(&self, _filter: &MarketFilter) -> Result<Vec<TargetMarket>, VenueError> {
        // TODO: Implement US portal market discovery
        warn!("US venue get_markets not yet implemented");
        Ok(Vec::new())
    }

    async fn get_market_resolution(
        &self,
        _condition_id: &str,
    ) -> Result<Option<MarketResolution>, VenueError> {
        Err(VenueError::Unsupported(
            "US venue market resolution not yet implemented".into(),
        ))
    }

    async fn get_orderbook(&self, _token_id: &str) -> Result<OrderBook, VenueError> {
        Err(VenueError::Unsupported(
            "US venue orderbook not yet implemented".into(),
        ))
    }

    async fn place_order(&self, _order: &OrderRequest) -> Result<OrderId, VenueError> {
        // TODO: Implement Ed25519-signed order placement
        Err(VenueError::Auth(
            "US venue order placement not yet implemented".into(),
        ))
    }

    async fn cancel_order(&self, _order_id: &OrderId) -> Result<(), VenueError> {
        Err(VenueError::Unsupported(
            "US venue cancel not yet implemented".into(),
        ))
    }

    async fn cancel_all(&self) -> Result<(), VenueError> {
        Err(VenueError::Unsupported(
            "US venue cancel-all not yet implemented".into(),
        ))
    }

    async fn subscribe_fills(
        &self,
        _tx: tokio::sync::mpsc::Sender<OrderEvent>,
        _shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), VenueError> {
        warn!("US venue fill subscription not yet implemented");
        Ok(())
    }

    async fn get_balance(&self) -> Result<Decimal, VenueError> {
        Ok(Decimal::ZERO)
    }

    fn fee_schedule(&self) -> &FeeSchedule {
        &self.fee_schedule
    }

    fn venue_type(&self) -> VenueType {
        VenueType::US
    }
}
