use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::time::Duration;
use tracing::{debug, error};

use super::{FeedSource, PriceAsset, PriceFeed, PriceFeedError, PriceObservation};

/// Coinbase REST price feed.
/// Polls the spot price endpoint at a configurable interval.
pub struct CoinbaseFeed {
    asset: PriceAsset,
    url: String,
    poll_interval: Duration,
    http: reqwest::Client,
}

impl CoinbaseFeed {
    pub fn new(asset: PriceAsset, url: &str, poll_interval: Duration) -> Self {
        Self {
            asset,
            url: url.to_string(),
            poll_interval,
            http: super::http_client(),
        }
    }

    async fn fetch_price(&self) -> Result<Decimal, PriceFeedError> {
        // Coinbase returns: {"data":{"amount":"67432.50","currency":"USD"}}
        let resp: serde_json::Value = self
            .http
            .get(&self.url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let price = resp
            .get("data")
            .and_then(|d| d.get("amount"))
            .and_then(|a| a.as_str())
            .and_then(|s| s.parse::<Decimal>().ok())
            .ok_or_else(|| PriceFeedError::Parse("failed to parse Coinbase response".into()))?;

        Ok(price)
    }
}

#[async_trait]
impl PriceFeed for CoinbaseFeed {
    fn source(&self) -> FeedSource {
        FeedSource::Coinbase
    }

    async fn run(
        &self,
        tx: tokio::sync::mpsc::Sender<PriceObservation>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), PriceFeedError> {
        let mut interval = tokio::time::interval(self.poll_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.fetch_price().await {
                        Ok(price) => {
                            debug!(source = "coinbase", %price, "price update");
                            let obs = PriceObservation {
                                asset: self.asset,
                                source: FeedSource::Coinbase,
                                price,
                                timestamp: Utc::now(),
                                bid: None,
                                ask: None,
                            };
                            if tx.send(obs).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            error!(source = "coinbase", error = %e, "failed to fetch price");
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }
}
