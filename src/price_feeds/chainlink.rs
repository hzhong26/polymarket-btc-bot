use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

use super::{FeedSource, PriceAsset, PriceFeed, PriceFeedError, PriceObservation};

/// Chainlink BTC/USD price feed — the primary feed because
/// Polymarket 5-minute crypto markets resolve against Chainlink.
///
/// Reads the latest round data from the Chainlink aggregator contract
/// on Polygon via an RPC call (eth_call to latestRoundData()).
pub struct ChainlinkFeed {
    asset: PriceAsset,
    rpc_url: String,
    contract_address: String,
    poll_interval: Duration,
    http: reqwest::Client,
}

impl ChainlinkFeed {
    pub fn new(
        asset: PriceAsset,
        rpc_url: &str,
        contract_address: &str,
        poll_interval: Duration,
    ) -> Self {
        Self {
            asset,
            rpc_url: rpc_url.to_string(),
            contract_address: contract_address.to_string(),
            poll_interval,
            http: super::http_client(),
        }
    }

    /// Call latestRoundData() on the Chainlink aggregator.
    async fn fetch_latest_price(&self) -> Result<(Decimal, DateTime<Utc>), PriceFeedError> {
        fetch_chainlink_price_oneshot(&self.http, &self.rpc_url, &self.contract_address).await
    }
}

/// One-shot Chainlink price fetch via RPC eth_call to latestRoundData().
/// Function selector: 0xfeaf968c
/// Returns (roundId, answer, startedAt, updatedAt, answeredInRound)
/// answer has 8 decimal places for USD pairs.
pub async fn fetch_chainlink_price_oneshot(
    http: &reqwest::Client,
    rpc_url: &str,
    contract_address: &str,
) -> Result<(Decimal, DateTime<Utc>), PriceFeedError> {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": contract_address,
            "data": "0xfeaf968c"
        }, "latest"],
        "id": 1
    });

    let resp: serde_json::Value = http
        .post(rpc_url)
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let result = extract_eth_call_result(&resp)?;

    if result == "0x" {
        return Err(PriceFeedError::Parse(
            "RPC eth_call returned empty data (0x)".into(),
        ));
    }

    // latestRoundData returns 5 uint256 values (160 bytes = 320 hex chars + 0x prefix)
    // Validate full response length: 0x prefix (2) + 5 words * 64 hex chars = 322
    if result.len() < 322 {
        return Err(PriceFeedError::Parse("RPC result too short".into()));
    }

    // Extract answer (second 32-byte word, bytes 66..130 in hex)
    let answer_hex = &result[66..130];
    let answer_raw = i128::from_str_radix(answer_hex, 16)
        .map_err(|e| PriceFeedError::Parse(format!("failed to parse answer: {}", e)))?;

    // Chainlink USD pairs have 8 decimals
    let price = Decimal::from_i128_with_scale(answer_raw, 8);

    if price <= Decimal::ZERO {
        return Err(PriceFeedError::Parse("invalid Chainlink price".into()));
    }

    // Extract updatedAt (4th 32-byte word, bytes 194..258 in hex)
    let updated_at_hex = &result[194..258];
    let timestamp = i64::from_str_radix(updated_at_hex, 16)
        .ok()
        .and_then(|ts| Utc.timestamp_opt(ts, 0).single())
        .unwrap_or_else(|| {
            warn!(
                source = "chainlink",
                "failed to parse updatedAt, falling back to local time"
            );
            Utc::now()
        });

    Ok((price, timestamp))
}

fn extract_eth_call_result(resp: &serde_json::Value) -> Result<&str, PriceFeedError> {
    let root = match resp.as_array() {
        Some(arr) if !arr.is_empty() => arr
            .iter()
            .find(|v| v.get("id").and_then(|id| id.as_i64()) == Some(1))
            .unwrap_or(&arr[0]),
        _ => resp,
    };

    if let Some(err) = root.get("error").filter(|e| !e.is_null()) {
        return Err(PriceFeedError::Parse(format_rpc_error(err)));
    }

    if let Some(result) = root.get("result") {
        if let Some(hex) = result.as_str() {
            return Ok(hex);
        }
        if let Some(data) = result.get("data").and_then(|d| d.as_str()) {
            return Ok(data);
        }
        if let Some(output) = result.get("output").and_then(|d| d.as_str()) {
            return Ok(output);
        }
    }

    Err(PriceFeedError::Parse(format!(
        "no result in RPC response: {}",
        root
    )))
}

fn format_rpc_error(err: &serde_json::Value) -> String {
    if let Some(s) = err.as_str() {
        return format!("RPC error: {}", s);
    }

    if let Some(obj) = err.as_object() {
        let code = obj.get("code").and_then(|c| c.as_i64());
        let message = obj
            .get("message")
            .and_then(|m| m.as_str())
            .or_else(|| obj.get("data").and_then(|d| d.as_str()))
            .or_else(|| {
                obj.get("data")
                    .and_then(|d| d.get("message"))
                    .and_then(|m| m.as_str())
            })
            .unwrap_or("unknown RPC error");

        // Include data payload when available for debugging (e.g. "execution reverted").
        let data_hint = obj.get("data").map(|d| d.to_string()).unwrap_or_default();
        return match (code, data_hint.is_empty()) {
            (Some(c), true) => format!("RPC error {}: {}", c, message),
            (Some(c), false) => format!("RPC error {}: {} (data: {})", c, message, data_hint),
            (None, true) => format!("RPC error: {}", message),
            (None, false) => format!("RPC error: {} (data: {})", message, data_hint),
        };
    }

    format!("RPC error: {}", err)
}

fn is_quota_error(error: &PriceFeedError) -> bool {
    let msg = error.to_string().to_ascii_lowercase();
    msg.contains("quota")
        || msg.contains("rate limit")
        || msg.contains("exceeded the quota usage")
        || msg.contains("rpc error -32001")
}

#[async_trait]
impl PriceFeed for ChainlinkFeed {
    fn source(&self) -> FeedSource {
        FeedSource::Chainlink
    }

    async fn run(
        &self,
        tx: tokio::sync::mpsc::Sender<PriceObservation>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), PriceFeedError> {
        let mut interval = tokio::time::interval(self.poll_interval);
        let mut consecutive_quota_errors: u32 = 0;
        let mut quota_cooldown_until: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Some(until) = quota_cooldown_until {
                        if Instant::now() < until {
                            continue;
                        }
                        quota_cooldown_until = None;
                        warn!(
                            source = "chainlink",
                            "quota cooldown elapsed; retrying Chainlink polling"
                        );
                    }

                    match self.fetch_latest_price().await {
                        Ok((price, timestamp)) => {
                            if consecutive_quota_errors > 0 {
                                warn!(
                                    source = "chainlink",
                                    consecutive_quota_errors,
                                    "Chainlink recovered after quota backoff"
                                );
                            }
                            consecutive_quota_errors = 0;
                            debug!(source = "chainlink", %price, %timestamp, "price update");
                            let obs = PriceObservation {
                                asset: self.asset,
                                source: FeedSource::Chainlink,
                                price,
                                timestamp,
                                bid: None,
                                ask: None,
                            };
                            if tx.send(obs).await.is_err() {
                                break; // Receiver dropped
                            }
                        }
                        Err(e) => {
                            if is_quota_error(&e) {
                                consecutive_quota_errors = consecutive_quota_errors.saturating_add(1);
                                let exponent = consecutive_quota_errors.saturating_sub(1).min(5);
                                let backoff_secs = 15u64.saturating_mul(1u64 << exponent);
                                quota_cooldown_until =
                                    Some(Instant::now() + Duration::from_secs(backoff_secs));
                                warn!(
                                    source = "chainlink",
                                    error = %e,
                                    consecutive_quota_errors,
                                    backoff_secs,
                                    "Chainlink quota exceeded; backing off polling"
                                );
                            } else {
                                consecutive_quota_errors = 0;
                                error!(source = "chainlink", error = %e, "failed to fetch price");
                            }
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
    use super::{extract_eth_call_result, format_rpc_error, is_quota_error};
    use crate::price_feeds::PriceFeedError;

    #[test]
    fn extracts_standard_result_string() {
        let resp = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x1234"
        });
        let result = extract_eth_call_result(&resp).unwrap();
        assert_eq!(result, "0x1234");
    }

    #[test]
    fn extracts_nested_result_data() {
        let resp = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "data": "0x5678" }
        });
        let result = extract_eth_call_result(&resp).unwrap();
        assert_eq!(result, "0x5678");
    }

    #[test]
    fn surfaces_rpc_error_message() {
        let resp = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32005, "message": "rate limit exceeded" }
        });
        let err = extract_eth_call_result(&resp).unwrap_err();
        assert!(err.to_string().contains("rate limit exceeded"));
    }

    #[test]
    fn formats_rpc_error_with_data_fallback() {
        let err = serde_json::json!({"code": 3, "data": "execution reverted"});
        let message = format_rpc_error(&err);
        assert!(message.contains("execution reverted"));
    }

    #[test]
    fn detects_quota_errors() {
        let quota = PriceFeedError::Parse("RPC error -32001: Exceeded the quota usage".to_string());
        let other = PriceFeedError::Parse("RPC error: invalid response".to_string());
        assert!(is_quota_error(&quota));
        assert!(!is_quota_error(&other));
    }
}
