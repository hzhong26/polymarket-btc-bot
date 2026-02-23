use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::time::Duration;
use tracing::{debug, error, warn};

use super::{FeedSource, PriceAsset, PriceFeed, PriceFeedError, PriceObservation};

/// CoinGecko REST price feed.
/// Fetches all supported assets in a single batched API call to stay
/// well within the free-tier rate limit (~5-10 calls/min).
pub struct CoinGeckoFeed {
    url: String,
    poll_interval: Duration,
    http: reqwest::Client,
}

impl CoinGeckoFeed {
    /// Create a batched CoinGecko feed. The `base_url` should be the CoinGecko
    /// simple/price endpoint *without* the `ids` param — we build it ourselves
    /// from `PriceAsset::ALL`.
    ///
    /// Example base: `https://api.coingecko.com/api/v3/simple/price`
    pub fn new(base_url: &str, poll_interval: Duration) -> Self {
        // Enforce a minimum of 30s to avoid rate limits on free tier
        let poll_interval = poll_interval.max(Duration::from_secs(30));

        let ids: Vec<&str> = PriceAsset::ALL.iter().map(|a| a.coingecko_id()).collect();
        let url = format!("{}?ids={}&vs_currencies=usd", base_url, ids.join(","));

        Self {
            url,
            poll_interval,
            http: super::http_client(),
        }
    }

    /// Fetch prices for all assets in a single request.
    /// CoinGecko returns: `{"bitcoin":{"usd":97000},"ethereum":{"usd":2700},...}`
    async fn fetch_prices(&self) -> Result<Vec<(PriceAsset, Decimal)>, PriceFeedError> {
        let resp: serde_json::Value = self
            .http
            .get(&self.url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        // Check for rate-limit / error response
        if resp.get("status").is_some() {
            let msg = resp
                .get("status")
                .and_then(|s| s.get("error_message"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            warn!(source = "coingecko", "API error: {}", msg);
            return Err(PriceFeedError::Parse(format!(
                "CoinGecko API error: {}",
                msg
            )));
        }

        let mut prices = Vec::new();
        for asset in PriceAsset::ALL {
            if let Some(price) = resp
                .get(asset.coingecko_id())
                .and_then(|b| b.get("usd"))
                .and_then(|p| p.as_f64())
                .and_then(|f| Decimal::try_from(f).ok())
            {
                prices.push((asset, price));
            } else {
                warn!(
                    source = "coingecko",
                    asset = %asset,
                    "asset missing from CoinGecko response"
                );
            }
        }

        if prices.is_empty() {
            return Err(PriceFeedError::Parse(
                "no asset prices found in CoinGecko response".into(),
            ));
        }

        Ok(prices)
    }
}

#[async_trait]
impl PriceFeed for CoinGeckoFeed {
    fn source(&self) -> FeedSource {
        FeedSource::CoinGecko
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
                    match self.fetch_prices().await {
                        Ok(prices) => {
                            let now = Utc::now();
                            for (asset, price) in prices {
                                debug!(source = "coingecko", %asset, %price, "price update");
                                let obs = PriceObservation {
                                    asset,
                                    source: FeedSource::CoinGecko,
                                    price,
                                    timestamp: now,
                                    bid: None,
                                    ask: None,
                                };
                                if tx.send(obs).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                        Err(e) => {
                            error!(source = "coingecko", error = %e, "failed to fetch prices");
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
