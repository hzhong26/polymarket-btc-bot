use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use std::time::Duration;
use tokio_tungstenite::tungstenite;
use tracing::{debug, error, warn};

use super::{FeedSource, PriceAsset, PriceFeed, PriceFeedError, PriceObservation};

/// Binance multi-asset WebSocket feed.
/// Subscribes to ticker stream(s) for sub-second price updates.
/// This is a cross-check feed (not the resolution source).
pub struct BinanceFeed {
    ws_url: String,
}

impl BinanceFeed {
    pub fn new(ws_url: &str) -> Self {
        Self {
            ws_url: ws_url.to_string(),
        }
    }
}

#[async_trait]
impl PriceFeed for BinanceFeed {
    fn source(&self) -> FeedSource {
        FeedSource::Binance
    }

    async fn run(
        &self,
        tx: tokio::sync::mpsc::Sender<PriceObservation>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), PriceFeedError> {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            match tokio_tungstenite::connect_async(&self.ws_url).await {
                Ok((ws_stream, _)) => {
                    backoff = Duration::from_secs(1); // Reset backoff on successful connect
                    let (_, mut read) = ws_stream.split();

                    loop {
                        tokio::select! {
                            result = tokio::time::timeout(Duration::from_secs(30), read.next()) => {
                                match result {
                                    Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                                        if let Some(obs) = parse_binance_ticker(&text) {
                                            debug!(source = "binance", price = %obs.price, "price update");
                                            if tx.send(obs).await.is_err() {
                                                return Ok(()); // Receiver dropped
                                            }
                                        } else {
                                            debug!(source = "binance", "non-ticker message received");
                                        }
                                    }
                                    Ok(Some(Ok(tungstenite::Message::Close(_)))) => {
                                        warn!(source = "binance", "WebSocket closed by server");
                                        break;
                                    }
                                    Ok(Some(Err(e))) => {
                                        error!(source = "binance", error = %e, "WebSocket error");
                                        break;
                                    }
                                    Ok(None) => break, // Stream ended
                                    Ok(_) => {} // Ping/Pong/Binary
                                    Err(_) => {
                                        warn!(source = "binance", "no WebSocket message in 30s, reconnecting");
                                        break;
                                    }
                                }
                            }
                            _ = shutdown.cancelled() => {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        source = "binance",
                        error = %e,
                        backoff_secs = backoff.as_secs(),
                        "WebSocket connection failed, retrying"
                    );
                }
            }

            // Exponential backoff before reconnect
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown.cancelled() => { break; }
            }
            backoff = (backoff * 2).min(max_backoff);
        }

        Ok(())
    }
}

/// Parse Binance ticker WebSocket message.
/// Supports:
/// 1) Raw ticker payload: {"s":"BTCUSDT","c":"67432.50","b":"67430.00","a":"67435.00",...}
/// 2) Combined stream wrapper: {"stream":"btcusdt@ticker","data":{...raw ticker...}}
fn parse_binance_ticker(text: &str) -> Option<PriceObservation> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    let payload = v.get("data").unwrap_or(&v);
    let symbol = payload.get("s").and_then(|s| s.as_str()).unwrap_or("");
    let asset = match symbol {
        "BTCUSDT" => PriceAsset::Btc,
        "ETHUSDT" => PriceAsset::Eth,
        "XRPUSDT" => PriceAsset::Xrp,
        "SOLUSDT" => PriceAsset::Sol,
        _ => return None,
    };

    let price = payload
        .get("c")
        .and_then(|c| c.as_str())
        .and_then(|s| s.parse::<Decimal>().ok())?;

    let bid = payload
        .get("b")
        .and_then(|b| b.as_str())
        .and_then(|s| s.parse::<Decimal>().ok());

    let ask = payload
        .get("a")
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse::<Decimal>().ok());

    // Use Binance event time (E field, millisecond epoch) for accurate timestamps
    let timestamp = payload
        .get("E")
        .and_then(|e| e.as_i64())
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
        .unwrap_or_else(Utc::now);

    Some(PriceObservation {
        asset,
        source: FeedSource::Binance,
        price,
        timestamp,
        bid,
        ask,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_combined_stream_payload() {
        let msg = r#"{
            "stream":"ethusdt@ticker",
            "data":{"s":"ETHUSDT","c":"2800.10","b":"2799.90","a":"2800.20"}
        }"#;
        let obs = parse_binance_ticker(msg).expect("expected parsed observation");
        assert_eq!(obs.asset, PriceAsset::Eth);
        assert_eq!(obs.price, Decimal::new(280010, 2));
    }

    #[test]
    fn ignores_unknown_symbol() {
        let msg = r#"{"s":"DOGEUSDT","c":"0.12","b":"0.11","a":"0.13"}"#;
        assert!(parse_binance_ticker(msg).is_none());
    }
}
