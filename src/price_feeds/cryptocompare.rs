use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::time::Duration;
use tracing::{debug, error, warn};

use super::{FeedSource, PriceAsset, PriceFeed, PriceFeedError, PriceObservation};

/// CryptoCompare REST price feed.
pub struct CryptoCompareFeed {
    asset: PriceAsset,
    url: String,
    poll_interval: Duration,
    http: reqwest::Client,
}

impl CryptoCompareFeed {
    pub fn new(asset: PriceAsset, url: &str, poll_interval: Duration) -> Self {
        // With one feed per asset, very short polling can trigger free-tier rate limits.
        let poll_interval = poll_interval.max(Duration::from_secs(15));
        Self {
            asset,
            url: url.to_string(),
            poll_interval,
            http: super::http_client(),
        }
    }

    async fn fetch_price(&self) -> Result<Decimal, PriceFeedError> {
        // CryptoCompare returns: {"USD":67432.50}
        let resp: serde_json::Value = self
            .http
            .get(&self.url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        if resp
            .get("Response")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.eq_ignore_ascii_case("error"))
        {
            let message = resp
                .get("Message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown CryptoCompare error");
            return Err(PriceFeedError::Parse(format!(
                "CryptoCompare API error: {}",
                message
            )));
        }

        let price = resp
            .get("USD")
            .and_then(parse_decimal_value)
            // Support alternate nested shape (e.g., {"BTC":{"USD":...}}).
            .or_else(|| {
                resp.get(self.asset.symbol())
                    .and_then(|asset_obj| asset_obj.get("USD"))
                    .and_then(parse_decimal_value)
            })
            .ok_or_else(|| {
                PriceFeedError::Parse("failed to parse CryptoCompare response".into())
            })?;

        Ok(price)
    }
}

fn parse_decimal_value(value: &serde_json::Value) -> Option<Decimal> {
    match value {
        serde_json::Value::Number(n) => n.to_string().parse::<Decimal>().ok(),
        serde_json::Value::String(s) => s.parse::<Decimal>().ok(),
        _ => None,
    }
}

#[async_trait]
impl PriceFeed for CryptoCompareFeed {
    fn source(&self) -> FeedSource {
        FeedSource::CryptoCompare
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
                            debug!(source = "cryptocompare", %price, "price update");
                            let obs = PriceObservation {
                                asset: self.asset,
                                source: FeedSource::CryptoCompare,
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
                            if e.to_string().to_ascii_lowercase().contains("rate limit") {
                                warn!(
                                    source = "cryptocompare",
                                    error = %e,
                                    "rate-limited by CryptoCompare; keeping feed alive"
                                );
                                continue;
                            }
                            error!(source = "cryptocompare", error = %e, "failed to fetch price");
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

#[cfg(test)]
mod tests {
    use super::parse_decimal_value;
    use rust_decimal::Decimal;

    #[test]
    fn parses_numeric_and_string_decimals() {
        let n = serde_json::json!(123.45);
        let s = serde_json::json!("123.45");
        assert_eq!(parse_decimal_value(&n), Decimal::try_from(123.45).ok());
        assert_eq!(parse_decimal_value(&s), Decimal::try_from(123.45).ok());
    }
}
