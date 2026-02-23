use alloy_primitives::{Address, U256};
use alloy_signer::{Signer, SignerSync};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{sol, Eip712Domain, SolStruct};
use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use rand::Rng;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::Sha256;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use tokio_tungstenite::tungstenite;
use tracing::{debug, warn};

use crate::config::InternationalVenueConfig;
use crate::market_scanner::TargetMarket;
use crate::timezone::to_et;

use super::{
    FeeSchedule, MarketFilter, MarketResolution, OrderBook, OrderBookLevel, OrderEvent, OrderId,
    OrderRequest, OrderType, Side, VenueClient, VenueError, VenueType,
};

sol! {
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

const POLY_CLOB_DOMAIN_NAME: &str = "Polymarket CTF Exchange";
const POLY_CLOB_DOMAIN_VERSION: &str = "1";
const POLY_EXCHANGE_MAINNET: &str = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e";
const POLY_EXCHANGE_AMOY: &str = "0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40";
const SIDE_BUY: u8 = 0;
const SIDE_SELL: u8 = 1;
const TOKEN_DECIMALS: i64 = 1_000_000;

#[derive(Debug, Clone)]
struct SignedOrderFields {
    salt: U256,
    maker: Address,
    signer: Address,
    taker: Address,
    token_id: U256,
    maker_amount: U256,
    taker_amount: U256,
    expiration: U256,
    nonce: U256,
    fee_rate_bps: U256,
    side_label: &'static str,
    signature_type: u8,
}

#[derive(Debug, Clone)]
struct SignedOrderEnvelope {
    fields: SignedOrderFields,
    signature: String,
}

#[derive(Debug, Serialize)]
struct ApiOrderPayload {
    salt: String,
    maker: String,
    signer: String,
    taker: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    expiration: String,
    nonce: String,
    #[serde(rename = "feeRateBps")]
    fee_rate_bps: String,
    side: &'static str,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    signature: String,
}

#[derive(Debug, Serialize)]
struct PlaceOrderPayload {
    order: ApiOrderPayload,
    owner: String,
    #[serde(rename = "orderType")]
    order_type: &'static str,
}

/// Polymarket International venue client.
/// Uses CLOB API with EIP-712 signing on Polygon.
pub struct InternationalClient {
    http: reqwest::Client,
    config: InternationalVenueConfig,
    fee_schedule: FeeSchedule,
    /// Backoff cache: condition_id -> backoff_until timestamp.
    /// Conditions that fail enrichment are skipped for 120s before retrying.
    enrichment_backoff: Mutex<HashMap<String, DateTime<Utc>>>,
    // In production: alloy LocalSigner for EIP-712 signing
    // and HMAC credentials for L2 auth headers
}

impl InternationalClient {
    pub fn new(config: InternationalVenueConfig) -> Result<Self, VenueError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(VenueError::Http)?;

        // 5-minute crypto markets: ~200bps taker fee (verify against current schedule)
        let fee_schedule = FeeSchedule {
            taker_fee_bps: Decimal::from(200),
            maker_rebate_bps: Decimal::ZERO,
        };

        Ok(Self {
            http,
            config,
            fee_schedule,
            enrichment_backoff: Mutex::new(HashMap::new()),
        })
    }

    /// Build HMAC-SHA256 authentication headers for L2 API requests.
    /// In production, this would sign (timestamp + method + path + body) with the API secret.
    fn auth_headers(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<reqwest::header::HeaderMap, VenueError> {
        let api_key = read_env_any(&["POLY_API_KEY", "POLYMARKET_API_KEY"]).ok_or_else(|| {
            VenueError::Auth("missing API key (set POLY_API_KEY or POLYMARKET_API_KEY)".into())
        })?;
        let api_secret =
            read_env_any(&["POLY_API_SECRET", "POLYMARKET_API_SECRET"]).ok_or_else(|| {
                VenueError::Auth(
                    "missing API secret (set POLY_API_SECRET or POLYMARKET_API_SECRET)".into(),
                )
            })?;
        let passphrase =
            read_env_any(&["POLY_PASSPHRASE", "POLYMARKET_PASSPHRASE"]).ok_or_else(|| {
                VenueError::Auth(
                    "missing API passphrase (set POLY_PASSPHRASE or POLYMARKET_PASSPHRASE)".into(),
                )
            })?;

        let timestamp = Utc::now().timestamp().to_string();
        let message = format!(
            "{}{}{}{}",
            timestamp,
            method.to_ascii_uppercase(),
            path,
            body
        );
        let signature = hmac_sha256_base64(&api_secret, &message)
            .map_err(|e| VenueError::Auth(format!("failed to build API signature: {}", e)))?;

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::HeaderName::from_static("poly_api_key"),
            reqwest::header::HeaderValue::from_str(&api_key)
                .map_err(|e| VenueError::Auth(format!("invalid API key header value: {}", e)))?,
        );
        headers.insert(
            reqwest::header::HeaderName::from_static("poly_passphrase"),
            reqwest::header::HeaderValue::from_str(&passphrase).map_err(|e| {
                VenueError::Auth(format!("invalid API passphrase header value: {}", e))
            })?,
        );
        headers.insert(
            reqwest::header::HeaderName::from_static("poly_timestamp"),
            reqwest::header::HeaderValue::from_str(&timestamp).map_err(|e| {
                VenueError::Auth(format!("invalid API timestamp header value: {}", e))
            })?,
        );
        headers.insert(
            reqwest::header::HeaderName::from_static("poly_signature"),
            reqwest::header::HeaderValue::from_str(&signature).map_err(|e| {
                VenueError::Auth(format!("invalid API signature header value: {}", e))
            })?,
        );

        // POLY_ADDRESS is required by the CLOB API for L2 auth; prefer configured proxy wallet.
        if let Some(address) = self
            .config
            .proxy_wallet
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .or_else(|| read_env_any(&["POLY_ADDRESS", "POLYMARKET_ADDRESS"]))
        {
            headers.insert(
                reqwest::header::HeaderName::from_static("poly_address"),
                reqwest::header::HeaderValue::from_str(&address).map_err(|e| {
                    VenueError::Auth(format!("invalid POLY_ADDRESS header value: {}", e))
                })?,
            );
        }
        Ok(headers)
    }

    fn parse_address(value: &str, label: &str) -> Result<Address, VenueError> {
        Address::from_str(value)
            .map_err(|e| VenueError::Auth(format!("invalid {} '{}': {}", label, value, e)))
    }

    fn signature_type(&self) -> u8 {
        if let Some(sig_type) = read_env_any(&["POLY_SIGNATURE_TYPE", "POLYMARKET_SIGNATURE_TYPE"])
            .and_then(|v| v.parse::<u8>().ok())
            .filter(|v| *v <= 2)
        {
            return sig_type;
        }

        if self.config.proxy_wallet.is_some() {
            2
        } else {
            0
        }
    }

    fn exchange_address(&self) -> Result<Address, VenueError> {
        if let Some(address) =
            read_env_any(&["POLY_EXCHANGE_ADDRESS", "POLYMARKET_EXCHANGE_ADDRESS"])
        {
            return Self::parse_address(&address, "exchange address");
        }

        let default = match self.config.chain_id {
            137 => POLY_EXCHANGE_MAINNET,
            80002 => POLY_EXCHANGE_AMOY,
            chain_id => {
                return Err(VenueError::Auth(format!(
                "unsupported chain_id {} for default exchange address; set POLY_EXCHANGE_ADDRESS",
                chain_id
            )))
            }
        };

        Self::parse_address(default, "exchange address")
    }

    fn eip712_domain(&self) -> Result<Eip712Domain, VenueError> {
        let name = read_env_any(&["POLY_EIP712_DOMAIN_NAME", "POLYMARKET_EIP712_DOMAIN_NAME"])
            .unwrap_or_else(|| POLY_CLOB_DOMAIN_NAME.to_string());
        let version = read_env_any(&[
            "POLY_EIP712_DOMAIN_VERSION",
            "POLYMARKET_EIP712_DOMAIN_VERSION",
        ])
        .unwrap_or_else(|| POLY_CLOB_DOMAIN_VERSION.to_string());

        let verifying_contract = self.exchange_address()?;

        Ok(Eip712Domain::new(
            Some(Cow::Owned(name)),
            Some(Cow::Owned(version)),
            Some(U256::from(self.config.chain_id)),
            Some(verifying_contract),
            None,
        ))
    }

    fn load_signer(&self) -> Result<PrivateKeySigner, VenueError> {
        let raw_private_key =
            read_env_any(&["PRIVATE_KEY", "POLY_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY"])
                .ok_or_else(|| {
                    VenueError::Auth(
                        "missing private key (set PRIVATE_KEY or POLY_PRIVATE_KEY)".into(),
                    )
                })?;
        let normalized = raw_private_key.trim_start_matches("0x");
        let mut signer = normalized.parse::<PrivateKeySigner>().map_err(|e| {
            VenueError::Auth(format!(
                "failed to parse private key for order signing: {}",
                e
            ))
        })?;
        signer.set_chain_id(Some(self.config.chain_id));
        Ok(signer)
    }

    fn maker_address(&self, signer: &PrivateKeySigner) -> Result<Address, VenueError> {
        self.config
            .proxy_wallet
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .or_else(|| {
                read_env_any(&[
                    "POLY_PROXY_WALLET",
                    "POLYMARKET_PROXY_WALLET",
                    "POLY_FUNDER",
                    "POLYMARKET_FUNDER",
                ])
            })
            .map(|maker| Self::parse_address(&maker, "maker/funder address"))
            .transpose()
            .map(|opt| opt.unwrap_or_else(|| signer.address()))
    }

    fn decimal_to_token_units(value: Decimal, label: &str) -> Result<U256, VenueError> {
        if value <= Decimal::ZERO {
            return Err(VenueError::Unsupported(format!(
                "{} must be > 0, got {}",
                label, value
            )));
        }

        let scaled = (value * Decimal::from(TOKEN_DECIMALS)).floor();
        if scaled <= Decimal::ZERO {
            return Err(VenueError::Unsupported(format!(
                "{} rounds to zero token units: {}",
                label, value
            )));
        }

        let as_u128 = scaled.to_u128().ok_or_else(|| {
            VenueError::Unsupported(format!(
                "{} is too large for token-unit conversion: {}",
                label, scaled
            ))
        })?;
        Ok(U256::from(as_u128))
    }

    fn compute_amounts(
        side: Side,
        price: Decimal,
        size: Decimal,
    ) -> Result<(U256, U256, u8, &'static str), VenueError> {
        if price <= Decimal::ZERO || price >= Decimal::ONE {
            return Err(VenueError::Unsupported(format!(
                "order price must be in (0,1), got {}",
                price
            )));
        }
        if size <= Decimal::ZERO {
            return Err(VenueError::Unsupported(format!(
                "order size must be > 0, got {}",
                size
            )));
        }

        // Match Polymarket order-utils amount convention:
        // BUY  => maker=USDC (size * price), taker=shares (size)
        // SELL => maker=shares (size),      taker=USDC (size * price)
        let (maker_raw, taker_raw, side_u8, side_label) = match side {
            Side::Buy => (size * price, size, SIDE_BUY, "BUY"),
            Side::Sell => (size, size * price, SIDE_SELL, "SELL"),
        };
        let maker_amount = Self::decimal_to_token_units(maker_raw, "maker amount")?;
        let taker_amount = Self::decimal_to_token_units(taker_raw, "taker amount")?;
        Ok((maker_amount, taker_amount, side_u8, side_label))
    }

    fn parse_u256_env(names: &[&str]) -> Option<U256> {
        read_env_any(names).and_then(|v| U256::from_str(v.trim()).ok())
    }

    fn build_signed_order(&self, order: &OrderRequest) -> Result<SignedOrderEnvelope, VenueError> {
        let signer = self.load_signer()?;
        let maker = self.maker_address(&signer)?;
        let signer_address = signer.address();
        let taker = Address::ZERO;

        let token_id = U256::from_str(order.token_id.trim()).map_err(|e| {
            VenueError::Unsupported(format!("invalid token_id '{}': {}", order.token_id, e))
        })?;

        let (maker_amount, taker_amount, side_u8, side_label) =
            Self::compute_amounts(order.side, order.price, order.size)?;

        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        let entropy: u64 = rand::thread_rng().r#gen();
        let salt = (U256::from(now_ms) << 64) | U256::from(entropy);
        let nonce = Self::parse_u256_env(&["POLY_ORDER_NONCE", "POLYMARKET_ORDER_NONCE"])
            .unwrap_or(U256::ZERO);
        let expiration =
            Self::parse_u256_env(&["POLY_ORDER_EXPIRATION", "POLYMARKET_ORDER_EXPIRATION"])
                .unwrap_or_else(|| match order.order_type {
                    OrderType::Fok => {
                        let ttl_secs = read_env_any(&["POLY_FOK_TTL_SECS"])
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(30);
                        U256::from((Utc::now().timestamp().max(0) as u64).saturating_add(ttl_secs))
                    }
                    OrderType::Gtc => U256::ZERO,
                });
        let fee_rate_bps =
            Self::parse_u256_env(&["POLY_ORDER_FEE_RATE_BPS", "POLYMARKET_ORDER_FEE_RATE_BPS"])
                .unwrap_or(U256::ZERO);
        let signature_type = self.signature_type();

        let typed_order = Order {
            salt,
            maker,
            signer: signer_address,
            taker,
            tokenId: token_id,
            makerAmount: maker_amount,
            takerAmount: taker_amount,
            expiration,
            nonce,
            feeRateBps: fee_rate_bps,
            side: side_u8,
            signatureType: signature_type,
        };
        let domain = self.eip712_domain()?;
        let signature_hash = typed_order.eip712_signing_hash(&domain);
        let signature = signer
            .sign_hash_sync(&signature_hash)
            .map_err(|e| VenueError::Auth(format!("failed to sign EIP-712 order: {}", e)))?
            .to_string();

        Ok(SignedOrderEnvelope {
            fields: SignedOrderFields {
                salt,
                maker,
                signer: signer_address,
                taker,
                token_id,
                maker_amount,
                taker_amount,
                expiration,
                nonce,
                fee_rate_bps,
                side_label,
                signature_type,
            },
            signature,
        })
    }

    fn build_api_order_payload(signed: SignedOrderEnvelope) -> ApiOrderPayload {
        ApiOrderPayload {
            salt: signed.fields.salt.to_string(),
            maker: signed.fields.maker.to_string(),
            signer: signed.fields.signer.to_string(),
            taker: signed.fields.taker.to_string(),
            token_id: signed.fields.token_id.to_string(),
            maker_amount: signed.fields.maker_amount.to_string(),
            taker_amount: signed.fields.taker_amount.to_string(),
            expiration: signed.fields.expiration.to_string(),
            nonce: signed.fields.nonce.to_string(),
            fee_rate_bps: signed.fields.fee_rate_bps.to_string(),
            side: signed.fields.side_label,
            signature_type: signed.fields.signature_type,
            signature: signed.signature,
        }
    }

    fn order_type_label(order_type: OrderType) -> &'static str {
        match order_type {
            OrderType::Gtc => "GTC",
            OrderType::Fok => "FOK",
        }
    }

    /// Sign an order using EIP-712 typed data signing.
    /// The order payload conforms to the Polymarket CLOB order schema.
    async fn sign_order(&self, order: &OrderRequest) -> Result<SignedOrderEnvelope, VenueError> {
        self.build_signed_order(order)
    }

    async fn fetch_gamma_json(&self, url: &str) -> Result<serde_json::Value, VenueError> {
        self.http
            .get(url)
            .send()
            .await?
            .error_for_status()
            .map_err(VenueError::Http)?
            .json()
            .await
            .map_err(VenueError::Http)
    }

    async fn enrich_missing_price_to_beat(
        &self,
        markets: &mut [TargetMarket],
    ) -> Result<usize, VenueError> {
        let now = Utc::now();
        const WINDOW_TOLERANCE_SECS: i64 = 30;
        const CONDITION_IDS_CHUNK_SIZE: usize = 25;

        let mut missing_by_condition: HashMap<String, Vec<usize>> = HashMap::new();
        let mut missing_by_token_pair: HashMap<String, Vec<usize>> = HashMap::new();
        let mut missing_by_token_id: HashMap<String, Vec<usize>> = HashMap::new();

        for (idx, market) in markets.iter().enumerate() {
            if !should_consider_for_price_to_beat_enrichment(market, now, WINDOW_TOLERANCE_SECS) {
                continue;
            }
            if let Some(cond) = normalize_condition_id(&market.condition_id) {
                missing_by_condition.entry(cond).or_default().push(idx);
            }
            if let Some(pair) = target_market_token_pair_key(market) {
                missing_by_token_pair.entry(pair).or_default().push(idx);
            }
            for token_id in target_market_token_ids(market) {
                missing_by_token_id.entry(token_id).or_default().push(idx);
            }
        }
        // Filter out conditions currently in backoff (failed enrichment recently).
        {
            let mut backoff = self.enrichment_backoff.lock().unwrap();
            prune_backed_off_candidates(
                now,
                &mut backoff,
                &mut missing_by_condition,
                &mut missing_by_token_pair,
                &mut missing_by_token_id,
            );
        }

        if missing_by_condition.is_empty()
            && missing_by_token_pair.is_empty()
            && missing_by_token_id.is_empty()
        {
            return Ok(0);
        }

        let target_conditions: HashSet<String> = missing_by_condition.keys().cloned().collect();
        let target_token_pairs: HashSet<String> = missing_by_token_pair.keys().cloned().collect();
        let target_token_ids: HashSet<String> = missing_by_token_id.keys().cloned().collect();
        let mut found_by_condition: HashMap<String, (Decimal, String)> = HashMap::new();
        let mut found_by_token_pair: HashMap<String, (Decimal, String)> = HashMap::new();
        let mut found_by_token_id: HashMap<String, (Decimal, String)> = HashMap::new();

        let sorted_condition_ids = sorted_unique_condition_ids(&target_conditions);
        let condition_id_chunks =
            chunk_condition_ids(&sorted_condition_ids, CONDITION_IDS_CHUNK_SIZE);

        let targeted_conditions_total = sorted_condition_ids.len();
        let targeted_chunks_total = condition_id_chunks.len();
        let mut targeted_chunks_failed = 0usize;
        let mut targeted_csv_retries = 0usize;
        let mut targeted_csv_retry_hits = 0usize;
        let mut targeted_chunks_no_condition_match = 0usize;
        let mut targeted_had_error = false;

        for (chunk_index, condition_ids_chunk) in condition_id_chunks.iter().enumerate() {
            let url = match build_gamma_markets_url_for_condition_ids(
                &self.config.gamma_url,
                condition_ids_chunk,
            ) {
                Ok(url) => url,
                Err(e) => {
                    targeted_had_error = true;
                    targeted_chunks_failed += 1;
                    warn!(
                        chunk_index,
                        chunk_size = condition_ids_chunk.len(),
                        error = %e,
                        "failed to build targeted Gamma /markets URL for condition_ids chunk"
                    );
                    continue;
                }
            };
            debug!(
                chunk_index,
                chunk_size = condition_ids_chunk.len(),
                url = %url,
                "fetching targeted Gamma /markets chunk to enrich missing price-to-beat"
            );
            let resp = match self.fetch_gamma_json(&url).await {
                Ok(resp) => resp,
                Err(e) => {
                    targeted_had_error = true;
                    targeted_chunks_failed += 1;
                    warn!(
                        chunk_index,
                        chunk_size = condition_ids_chunk.len(),
                        error = %e,
                        "targeted Gamma /markets request failed for condition_ids chunk"
                    );
                    continue;
                }
            };

            let Some(arr) = markets_array_from_response(&resp) else {
                targeted_had_error = true;
                targeted_chunks_failed += 1;
                warn!(
                    chunk_index,
                    chunk_size = condition_ids_chunk.len(),
                    "targeted Gamma /markets response missing markets array; using fallback"
                );
                continue;
            };

            let response_rows = arr.len();
            let matched_condition_rows =
                count_rows_matching_condition_ids(arr, condition_ids_chunk);
            if matched_condition_rows == 0 {
                targeted_chunks_no_condition_match += 1;
            }
            debug!(
                chunk_index,
                chunk_size = condition_ids_chunk.len(),
                response_rows,
                matched_condition_rows,
                "parsed targeted Gamma /markets chunk response"
            );

            collect_found_price_to_beat_from_markets(
                arr,
                &target_conditions,
                &target_token_pairs,
                &target_token_ids,
                &mut found_by_condition,
                &mut found_by_token_pair,
                &mut found_by_token_id,
            );

            // Compatibility retry: some Gamma deployments may interpret array query
            // params differently. If repeated params return no rows or no matching
            // condition IDs, try comma format.
            if arr.is_empty() || matched_condition_rows == 0 {
                let csv_url = match build_gamma_markets_url_for_condition_ids_csv(
                    &self.config.gamma_url,
                    condition_ids_chunk,
                ) {
                    Ok(url) => url,
                    Err(e) => {
                        debug!(
                            chunk_index,
                            chunk_size = condition_ids_chunk.len(),
                            error = %e,
                            "failed to build CSV compatibility URL for condition_ids chunk"
                        );
                        continue;
                    }
                };
                targeted_csv_retries += 1;
                debug!(
                    chunk_index,
                    chunk_size = condition_ids_chunk.len(),
                    url = %csv_url,
                    "re-trying targeted Gamma /markets chunk with comma-joined condition_ids"
                );
                let csv_resp = match self.fetch_gamma_json(&csv_url).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        // Best-effort compatibility retry; do not convert this into
                        // a hard error because the primary targeted request succeeded.
                        debug!(
                            chunk_index,
                            chunk_size = condition_ids_chunk.len(),
                            error = %e,
                            "CSV compatibility retry for condition_ids chunk failed"
                        );
                        continue;
                    }
                };
                if let Some(csv_arr) = markets_array_from_response(&csv_resp) {
                    let csv_matched_condition_rows =
                        count_rows_matching_condition_ids(csv_arr, condition_ids_chunk);
                    if !csv_arr.is_empty() {
                        targeted_csv_retry_hits += 1;
                    }
                    debug!(
                        chunk_index,
                        chunk_size = condition_ids_chunk.len(),
                        csv_response_rows = csv_arr.len(),
                        csv_matched_condition_rows,
                        "parsed CSV compatibility retry response for condition_ids chunk"
                    );
                    collect_found_price_to_beat_from_markets(
                        csv_arr,
                        &target_conditions,
                        &target_token_pairs,
                        &target_token_ids,
                        &mut found_by_condition,
                        &mut found_by_token_pair,
                        &mut found_by_token_id,
                    );
                }
            }
        }

        let mut fallback_used = false;
        let mut fallback_pages_attempted = 0usize;
        if targeted_had_error {
            fallback_used = true;
            fallback_pages_attempted = self
                .enrich_missing_price_to_beat_with_broad_fallback(
                    &target_conditions,
                    &target_token_pairs,
                    &target_token_ids,
                    &mut found_by_condition,
                    &mut found_by_token_pair,
                    &mut found_by_token_id,
                )
                .await?;
        }

        let mut enriched = 0usize;
        let mut enriched_indices: HashSet<usize> = HashSet::new();
        for (cond, indices) in missing_by_condition {
            let Some((value, source)) = found_by_condition.get(&cond) else {
                continue;
            };
            for idx in indices {
                if let Some(market) = markets.get_mut(idx) {
                    market.polymarket_price_to_beat = Some(*value);
                    market.polymarket_price_to_beat_source =
                        Some(format!("markets_endpoint:{}", source));
                    enriched += 1;
                    enriched_indices.insert(idx);
                }
            }
        }

        for (pair, indices) in missing_by_token_pair {
            let Some((value, source)) = found_by_token_pair.get(&pair) else {
                continue;
            };
            for idx in indices {
                if enriched_indices.contains(&idx) {
                    continue;
                }
                let Some(market) = markets.get_mut(idx) else {
                    continue;
                };
                if market.polymarket_price_to_beat.is_some() {
                    continue;
                }
                market.polymarket_price_to_beat = Some(*value);
                market.polymarket_price_to_beat_source =
                    Some(format!("markets_endpoint:{}", source));
                enriched += 1;
                enriched_indices.insert(idx);
            }
        }

        for (token_id, indices) in missing_by_token_id {
            let Some((value, source)) = found_by_token_id.get(&token_id) else {
                continue;
            };
            for idx in indices {
                if enriched_indices.contains(&idx) {
                    continue;
                }
                let Some(market) = markets.get_mut(idx) else {
                    continue;
                };
                if market.polymarket_price_to_beat.is_some() {
                    continue;
                }
                market.polymarket_price_to_beat = Some(*value);
                market.polymarket_price_to_beat_source =
                    Some(format!("markets_endpoint:{}", source));
                enriched += 1;
                enriched_indices.insert(idx);
            }
        }

        let unresolved: Vec<&TargetMarket> = markets
            .iter()
            .filter(|m| should_consider_for_price_to_beat_enrichment(m, now, WINDOW_TOLERANCE_SECS))
            .collect();
        let unresolved_samples: Vec<String> = unresolved
            .iter()
            .take(3)
            .map(|m| format!("{} | {}", m.condition_id, m.question))
            .collect();
        debug!(
            targeted_conditions_total,
            targeted_chunks_total,
            targeted_chunks_failed,
            targeted_chunks_no_condition_match,
            targeted_csv_retries,
            targeted_csv_retry_hits,
            fallback_used,
            fallback_pages_attempted,
            enriched_price_to_beat = enriched,
            unresolved_missing_price_to_beat = unresolved.len(),
            samples = ?unresolved_samples,
            "price-to-beat enrichment summary"
        );

        // Record backoff for unresolved conditions so we skip them for 120s.
        if !unresolved.is_empty() {
            let backoff_until = now + chrono::Duration::seconds(120);
            let mut backoff = self.enrichment_backoff.lock().unwrap();
            for m in &unresolved {
                if let Some(cond) = normalize_condition_id(&m.condition_id) {
                    backoff.insert(cond, backoff_until);
                }
                if let Some(pair) = target_market_token_pair_key(m) {
                    backoff.insert(pair, backoff_until);
                }
                for token_id in target_market_token_ids(m) {
                    backoff.insert(token_id, backoff_until);
                }
            }
        }

        Ok(enriched)
    }

    async fn enrich_missing_price_to_beat_with_broad_fallback(
        &self,
        target_conditions: &HashSet<String>,
        target_token_pairs: &HashSet<String>,
        target_token_ids: &HashSet<String>,
        found_by_condition: &mut HashMap<String, (Decimal, String)>,
        found_by_token_pair: &mut HashMap<String, (Decimal, String)>,
        found_by_token_id: &mut HashMap<String, (Decimal, String)>,
    ) -> Result<usize, VenueError> {
        const MAX_BROAD_FALLBACK_PAGES: usize = 2;
        let mut pages_attempted = 0usize;

        for page in 0..MAX_BROAD_FALLBACK_PAGES {
            let offset = page * 200;
            let url = build_gamma_markets_url(&self.config.gamma_url, true, false, Some(offset))?;
            pages_attempted += 1;
            debug!(
                page,
                url = %url,
                missing_conditions = target_conditions.len(),
                "fetching fallback Gamma /markets page to enrich missing price-to-beat"
            );
            let resp = self.fetch_gamma_json(&url).await?;
            let Some(arr) = markets_array_from_response(&resp) else {
                break;
            };

            collect_found_price_to_beat_from_markets(
                arr,
                target_conditions,
                target_token_pairs,
                target_token_ids,
                found_by_condition,
                found_by_token_pair,
                found_by_token_id,
            );

            let conditions_done =
                target_conditions.is_empty() || found_by_condition.len() >= target_conditions.len();
            let token_pairs_done = target_token_pairs.is_empty()
                || found_by_token_pair.len() >= target_token_pairs.len();
            let token_ids_done =
                target_token_ids.is_empty() || found_by_token_id.len() >= target_token_ids.len();
            if (conditions_done && token_pairs_done && token_ids_done) || arr.len() < 200 {
                break;
            }
        }

        Ok(pages_attempted)
    }
}

struct TagFetchResult {
    tag: String,
    markets: Vec<TargetMarket>,
    had_success: bool,
    had_any_markets: bool,
    pages_fetched: usize,
    error: Option<VenueError>,
}

async fn fetch_tag_pages(
    http: &reqwest::Client,
    gamma_url: &str,
    tag: &str,
    filter: &MarketFilter,
) -> TagFetchResult {
    let mut result = TagFetchResult {
        tag: tag.to_string(),
        markets: Vec::new(),
        had_success: false,
        had_any_markets: false,
        pages_fetched: 0,
        error: None,
    };

    for page in 0..5 {
        let offset = page * 200;
        let url = match build_gamma_events_url(gamma_url, Some(tag), Some(offset)) {
            Ok(u) => u,
            Err(e) => {
                result.error = Some(e);
                break;
            }
        };
        debug!(
            tag = %tag,
            page,
            offset,
            url = %url,
            "fetching tagged markets from Gamma API"
        );
        let resp = match http
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(r) => match r.json::<serde_json::Value>().await {
                Ok(v) => {
                    result.had_success = true;
                    result.pages_fetched += 1;
                    v
                }
                Err(e) => {
                    if !result.had_success {
                        warn!(tag = %tag, error = %e, "tagged Gamma query failed; scheduling fallback for this tag");
                        result.error = Some(VenueError::Http(e));
                    } else {
                        warn!(tag = %tag, page, error = %e, "tagged Gamma pagination failed after partial success");
                    }
                    break;
                }
            },
            Err(e) => {
                if !result.had_success {
                    warn!(tag = %tag, error = %e, "tagged Gamma query failed; scheduling fallback for this tag");
                    result.error = Some(VenueError::Http(e));
                } else {
                    warn!(tag = %tag, page, error = %e, "tagged Gamma pagination failed after partial success");
                }
                break;
            }
        };

        let batch_size = resp.as_array().map(|a| a.len()).unwrap_or(0);
        let parsed = parse_gamma_markets(&resp, filter, None);
        let parsed_count = parsed.len();
        if parsed_count > 0 {
            result.had_any_markets = true;
        }
        result.markets.extend(parsed);

        debug!(
            tag = %tag,
            page,
            offset,
            parsed_count,
            batch_size,
            total = result.markets.len(),
            "parsed tagged Gamma API response"
        );

        if batch_size < 200 {
            break;
        }
    }

    debug!(
        tag = %tag,
        pages_fetched = result.pages_fetched,
        had_any_markets = result.had_any_markets,
        total = result.markets.len(),
        "completed tagged Gamma pagination"
    );

    result
}

fn should_consider_for_price_to_beat_enrichment(
    market: &TargetMarket,
    now: DateTime<Utc>,
    window_tolerance_secs: i64,
) -> bool {
    if !is_up_down_market(&market.question, &[]) {
        return false;
    }
    if market.polymarket_price_to_beat.is_some() {
        return false;
    }
    let Some(start_time) = market.start_time else {
        return false;
    };
    if market.end_time <= now {
        return false;
    }
    if start_time > now + chrono::Duration::seconds(30) {
        return false;
    }
    let duration_secs = (market.end_time - start_time).num_seconds();
    (duration_secs - 300).abs() <= window_tolerance_secs
        || (duration_secs - 900).abs() <= window_tolerance_secs
}

fn prune_backed_off_candidates(
    now: DateTime<Utc>,
    backoff: &mut HashMap<String, DateTime<Utc>>,
    missing_by_condition: &mut HashMap<String, Vec<usize>>,
    missing_by_token_pair: &mut HashMap<String, Vec<usize>>,
    missing_by_token_id: &mut HashMap<String, Vec<usize>>,
) {
    backoff.retain(|_, until| *until > now);
    missing_by_condition.retain(|cond, _| !backoff.contains_key(cond));
    missing_by_token_pair.retain(|pair, _| !backoff.contains_key(pair));
    missing_by_token_id.retain(|tid, _| !backoff.contains_key(tid));
}

fn sorted_unique_condition_ids(condition_ids: &HashSet<String>) -> Vec<String> {
    let mut ids: Vec<String> = condition_ids.iter().cloned().collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn chunk_condition_ids(condition_ids: &[String], chunk_size: usize) -> Vec<Vec<String>> {
    if condition_ids.is_empty() {
        return Vec::new();
    }
    if chunk_size == 0 {
        return vec![condition_ids.to_vec()];
    }
    condition_ids
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn collect_found_price_to_beat_from_markets(
    market_rows: &[serde_json::Value],
    target_conditions: &HashSet<String>,
    target_token_pairs: &HashSet<String>,
    target_token_ids: &HashSet<String>,
    found_by_condition: &mut HashMap<String, (Decimal, String)>,
    found_by_token_pair: &mut HashMap<String, (Decimal, String)>,
    found_by_token_id: &mut HashMap<String, (Decimal, String)>,
) {
    for market in market_rows {
        let (value, source) = extract_polymarket_price_to_beat(market);
        let Some(value) = value else {
            continue;
        };
        let source = source.unwrap_or_else(|| "markets_endpoint".to_string());

        if let Some(cond) =
            market_condition_id_flexible(market).and_then(|c| normalize_condition_id(&c))
        {
            if target_conditions.contains(&cond) && !found_by_condition.contains_key(&cond) {
                found_by_condition.insert(cond, (value, source.clone()));
            }
        }

        if let Some(pair_key) = market_token_pair_key_from_json(market) {
            if target_token_pairs.contains(&pair_key)
                && !found_by_token_pair.contains_key(&pair_key)
            {
                found_by_token_pair.insert(pair_key, (value, source.clone()));
            }
        }

        for token_id in market_token_ids_from_json(market) {
            if target_token_ids.contains(&token_id) && !found_by_token_id.contains_key(&token_id) {
                found_by_token_id.insert(token_id, (value, source.clone()));
            }
        }
    }
}

fn count_rows_matching_condition_ids(
    market_rows: &[serde_json::Value],
    condition_ids: &[String],
) -> usize {
    if condition_ids.is_empty() {
        return 0;
    }
    let target: HashSet<String> = condition_ids.iter().cloned().collect();
    market_rows
        .iter()
        .filter_map(market_condition_id_flexible)
        .filter_map(|cond| normalize_condition_id(&cond))
        .filter(|cond| target.contains(cond))
        .count()
}

#[async_trait]
impl VenueClient for InternationalClient {
    async fn get_markets(&self, filter: &MarketFilter) -> Result<Vec<TargetMarket>, VenueError> {
        // Query Gamma API for active events.
        // Sort by startDate descending to get latest recurring markets first.
        let base_url = build_gamma_events_url(&self.config.gamma_url, None, None)?;
        let mut markets = Vec::new();
        let mut seen = HashSet::new();
        let mut tags_missing_results = BTreeSet::new();
        let mut successful_queries = 0usize;
        let mut last_error: Option<VenueError> = None;
        let mut tag_queries_total = 0usize;
        let mut tag_queries_success = 0usize;
        let mut tag_queries_empty = 0usize;
        let mut tag_queries_failed = 0usize;
        let mut fallback_pages_attempted = 0usize;

        if filter.tags.is_empty() {
            debug!(url = %base_url, "fetching markets from Gamma API");
            let resp = match self.fetch_gamma_json(&base_url).await {
                Ok(resp) => {
                    successful_queries += 1;
                    resp
                }
                Err(e) => {
                    return Err(e);
                }
            };
            let parsed = parse_gamma_markets(&resp, filter, None);
            merge_unique_markets(parsed, &mut markets, &mut seen);
        } else {
            // OR semantics across tags: query each tag separately and merge unique markets.
            // All tags run concurrently; pages within a tag remain sequential.
            let mut join_set = tokio::task::JoinSet::new();
            for tag in filter.tags.clone() {
                let http = self.http.clone();
                let gamma_url = self.config.gamma_url.clone();
                let filter_clone = filter.clone();
                join_set.spawn(async move {
                    fetch_tag_pages(&http, &gamma_url, &tag, &filter_clone).await
                });
            }
            tag_queries_total = filter.tags.len();

            while let Some(join_result) = join_set.join_next().await {
                let tag_result = match join_result {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(error = %e, "tag fetch task panicked");
                        tag_queries_failed += 1;
                        continue;
                    }
                };

                successful_queries += tag_result.pages_fetched;

                if tag_result.had_success {
                    tag_queries_success += 1;
                } else {
                    tag_queries_failed += 1;
                    tags_missing_results.insert(tag_result.tag.clone());
                    if let Some(e) = tag_result.error {
                        last_error = Some(e);
                    }
                    continue;
                }

                if !tag_result.had_any_markets {
                    tag_queries_empty += 1;
                    tags_missing_results.insert(tag_result.tag.clone());
                }

                merge_unique_markets(tag_result.markets, &mut markets, &mut seen);
            }
        }

        // Tag slugs can change over time; if a tag query returns no markets, fall back
        // to broader untagged pages and filter locally using asset keywords for missing tags.
        if !tags_missing_results.is_empty() {
            let missing_tags: Vec<String> = tags_missing_results.into_iter().collect();
            let fallback_keywords = derive_fallback_keywords(&missing_tags);
            warn!(
                tags = ?missing_tags,
                "some tagged Gamma queries returned no markets; trying untagged fallback pages"
            );

            for page in 0..5 {
                let offset = page * 200;
                fallback_pages_attempted += 1;
                let fallback_url =
                    build_gamma_events_url(&self.config.gamma_url, None, Some(offset))?;
                debug!(
                    page,
                    url = %fallback_url,
                    keywords = ?fallback_keywords,
                    "fetching fallback markets from Gamma API"
                );

                let fallback_resp = match self.fetch_gamma_json(&fallback_url).await {
                    Ok(resp) => {
                        successful_queries += 1;
                        resp
                    }
                    Err(e) => {
                        warn!(
                            page,
                            error = %e,
                            "fallback Gamma query failed; trying next page"
                        );
                        last_error = Some(e);
                        continue;
                    }
                };
                let batch_size = fallback_resp.as_array().map(|a| a.len()).unwrap_or(0);
                let parsed = parse_gamma_markets(&fallback_resp, filter, Some(&fallback_keywords));
                merge_unique_markets(parsed, &mut markets, &mut seen);

                if batch_size < 200 {
                    break;
                }
            }

            debug!(
                count = markets.len(),
                keywords = ?fallback_keywords,
                "parsed markets from untagged fallback pages"
            );
        }

        let enriched_price_to_beat = match self.enrich_missing_price_to_beat(&mut markets).await {
            Ok(count) => count,
            Err(e) => {
                warn!(error = %e, "failed to enrich price-to-beat from Gamma /markets endpoint");
                0
            }
        };

        debug!(
            tag_queries_total,
            tag_queries_success,
            tag_queries_empty,
            tag_queries_failed,
            fallback_pages_attempted,
            enriched_price_to_beat,
            markets_merged_total = markets.len(),
            "international market discovery summary"
        );

        if markets.is_empty() && successful_queries == 0 {
            if let Some(err) = last_error {
                return Err(err);
            }
        }

        Ok(markets)
    }

    async fn get_market_resolution(
        &self,
        condition_id: &str,
    ) -> Result<Option<MarketResolution>, VenueError> {
        const MAX_PAGES: usize = 6;
        let trimmed = condition_id.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let Some(normalized_condition_id) = normalize_condition_id(trimmed) else {
            return Ok(None);
        };

        let mut last_error: Option<VenueError> = None;
        let mut successful_pages = 0usize;
        let mut had_any_success = false;

        // First, try targeted /markets lookups by condition_id (official source, robust to paging).
        let condition_ids = vec![normalized_condition_id.clone()];
        for (mode, csv) in [("repeated", false), ("csv", true)] {
            let url = if csv {
                build_gamma_markets_url_for_condition_ids_with_flags(
                    &self.config.gamma_url,
                    &condition_ids,
                    false,
                    true,
                    true,
                )?
            } else {
                build_gamma_markets_url_for_condition_ids_with_flags(
                    &self.config.gamma_url,
                    &condition_ids,
                    false,
                    true,
                    false,
                )?
            };
            debug!(
                condition_id = trimmed,
                mode,
                url = %url,
                "fetching targeted Gamma /markets for resolution"
            );
            match self.fetch_gamma_json(&url).await {
                Ok(resp) => {
                    had_any_success = true;
                    if let Some(market) =
                        find_market_in_markets_by_condition_id(&resp, &normalized_condition_id)
                    {
                        let resolution = parse_market_resolution(market, trimmed);
                        debug!(
                            condition_id = trimmed,
                            mode,
                            resolved_yes_outcome = ?resolution.resolved_yes_outcome,
                            source = %resolution.resolution_source,
                            "parsed market resolution from targeted /markets"
                        );
                        return Ok(Some(resolution));
                    }
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        // Fallback: query without closed=true (handles markets still in transition)
        {
            let fallback_url = build_gamma_markets_url_for_condition_ids_with_flags(
                &self.config.gamma_url,
                &condition_ids,
                false, // active=false
                false, // closed — omit filter
                false, // csv=false
            )?;
            debug!(
                condition_id = trimmed,
                url = %fallback_url,
                "fetching Gamma /markets without closed filter for resolution"
            );
            match self.fetch_gamma_json(&fallback_url).await {
                Ok(resp) => {
                    had_any_success = true;
                    if let Some(market) =
                        find_market_in_markets_by_condition_id(&resp, &normalized_condition_id)
                    {
                        let resolution = parse_market_resolution(market, trimmed);
                        if resolution.resolved_yes_outcome.is_some() {
                            return Ok(Some(resolution));
                        }
                        // Market found but not yet resolved — return for price_to_beat metadata
                        return Ok(Some(resolution));
                    }
                }
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        for page in 0..MAX_PAGES {
            let offset = page * 200;
            let url = build_gamma_events_url_with_flags(
                &self.config.gamma_url,
                false,
                true,
                None,
                Some(offset),
            )?;
            debug!(
                condition_id = trimmed,
                page,
                url = %url,
                "fetching closed Gamma page for resolution"
            );

            let resp = match self.fetch_gamma_json(&url).await {
                Ok(resp) => {
                    successful_pages += 1;
                    had_any_success = true;
                    resp
                }
                Err(err) => {
                    last_error = Some(err);
                    continue;
                }
            };

            if let Some(market) =
                find_market_in_events_by_condition_id(&resp, &normalized_condition_id)
            {
                let resolution = parse_market_resolution(market, trimmed);
                debug!(
                    condition_id = trimmed,
                    resolved_yes_outcome = ?resolution.resolved_yes_outcome,
                    source = %resolution.resolution_source,
                    "parsed market resolution"
                );
                return Ok(Some(resolution));
            }

            if resp.as_array().map(|arr| arr.len()).unwrap_or(0) < 200 {
                break;
            }
        }

        if successful_pages == 0 && !had_any_success {
            if let Some(err) = last_error {
                return Err(err);
            }
        }

        Ok(None)
    }

    async fn get_orderbook(&self, token_id: &str) -> Result<OrderBook, VenueError> {
        let url = format!("{}/book?token_id={}", self.config.clob_url, token_id);
        debug!(url = %url, "fetching orderbook");

        let resp: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await?
            .error_for_status()
            .map_err(VenueError::Http)?
            .json()
            .await?;

        parse_orderbook(token_id, &resp)
    }

    async fn place_order(&self, order: &OrderRequest) -> Result<OrderId, VenueError> {
        let signed = self.sign_order(order).await?;
        let api_key = read_env_any(&["POLY_API_KEY", "POLYMARKET_API_KEY"]).ok_or_else(|| {
            VenueError::Auth("missing API key (set POLY_API_KEY or POLYMARKET_API_KEY)".into())
        })?;
        let payload = PlaceOrderPayload {
            order: Self::build_api_order_payload(signed),
            owner: api_key,
            order_type: Self::order_type_label(order.order_type),
        };

        let body = serde_json::to_string(&payload).map_err(|e| {
            VenueError::Unsupported(format!("failed to serialize order payload: {}", e))
        })?;
        let path = "/order";
        let url = format!("{}{}", self.config.clob_url, path);
        let headers = self.auth_headers("POST", path, &body)?;

        debug!(
            token_id = %order.token_id,
            side = ?order.side,
            price = %order.price,
            size = %order.size,
            order_type = ?order.order_type,
            "submitting signed order to CLOB"
        );

        let response = self
            .http
            .post(&url)
            .headers(headers)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await.map_err(VenueError::Http)?;
        let json: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|_| {
            serde_json::json!({
                "raw": text
            })
        });

        if !status.is_success() {
            let msg = json
                .get("errorMsg")
                .and_then(|v| v.as_str())
                .or_else(|| json.get("error").and_then(|v| v.as_str()))
                .or_else(|| json.get("message").and_then(|v| v.as_str()))
                .unwrap_or("unknown order rejection");
            return Err(VenueError::OrderRejected(format!(
                "HTTP {}: {}",
                status.as_u16(),
                msg
            )));
        }

        let order_id = parse_place_order_response(&json).ok_or_else(|| {
            VenueError::Unsupported(format!("could not parse order id from response: {}", json))
        })?;
        Ok(OrderId(order_id))
    }

    async fn cancel_order(&self, order_id: &OrderId) -> Result<(), VenueError> {
        let url = format!("{}/order/{}", self.config.clob_url, order_id.0);
        let headers = self.auth_headers("DELETE", &format!("/order/{}", order_id.0), "")?;

        self.http
            .delete(&url)
            .headers(headers)
            .send()
            .await?
            .error_for_status()
            .map_err(VenueError::Http)?;

        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), VenueError> {
        let url = format!("{}/cancel-all", self.config.clob_url);
        let headers = self.auth_headers("DELETE", "/cancel-all", "")?;

        self.http
            .delete(&url)
            .headers(headers)
            .send()
            .await?
            .error_for_status()
            .map_err(VenueError::Http)?;

        Ok(())
    }

    async fn subscribe_fills(
        &self,
        tx: tokio::sync::mpsc::Sender<OrderEvent>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), VenueError> {
        let api_key = read_env_any(&["POLY_API_KEY", "POLYMARKET_API_KEY"]).ok_or_else(|| {
            VenueError::Auth("missing API key (set POLY_API_KEY or POLYMARKET_API_KEY)".into())
        })?;
        let api_secret =
            read_env_any(&["POLY_API_SECRET", "POLYMARKET_API_SECRET"]).ok_or_else(|| {
                VenueError::Auth(
                    "missing API secret (set POLY_API_SECRET or POLYMARKET_API_SECRET)".into(),
                )
            })?;
        let passphrase =
            read_env_any(&["POLY_PASSPHRASE", "POLYMARKET_PASSPHRASE"]).ok_or_else(|| {
                VenueError::Auth(
                    "missing API passphrase (set POLY_PASSPHRASE or POLYMARKET_PASSPHRASE)".into(),
                )
            })?;

        let markets: Vec<String> = read_env_any(&["POLY_WS_MARKETS"])
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        let subscribe_payload = serde_json::json!({
            "auth": {
                "apiKey": api_key,
                "secret": api_secret,
                "passphrase": passphrase
            },
            "markets": markets,
            "type": "USER"
        })
        .to_string();

        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            debug!(ws_url = %self.config.ws_url, "connecting user websocket");
            match tokio_tungstenite::connect_async(&self.config.ws_url).await {
                Ok((ws_stream, _)) => {
                    backoff = Duration::from_secs(1);
                    let (mut write, mut read) = ws_stream.split();

                    write
                        .send(tungstenite::Message::Text(subscribe_payload.clone().into()))
                        .await
                        .map_err(|e| {
                            VenueError::WebSocket(format!("subscribe send failed: {}", e))
                        })?;

                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                let _ = write.send(tungstenite::Message::Close(None)).await;
                                return Ok(());
                            }
                            maybe_msg = read.next() => {
                                match maybe_msg {
                                    Some(Ok(tungstenite::Message::Text(text))) => {
                                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                                            for event in parse_user_ws_events(&value) {
                                                if tx.send(event).await.is_err() {
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(tungstenite::Message::Close(_))) => {
                                        warn!("user websocket closed by server");
                                        break;
                                    }
                                    Some(Ok(_)) => {}
                                    Some(Err(e)) => {
                                        warn!(error = %e, "user websocket read error");
                                        break;
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        backoff_secs = backoff.as_secs(),
                        "failed to connect user websocket, retrying"
                    );
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown.cancelled() => break,
            }
            backoff = (backoff * 2).min(max_backoff);
        }

        Ok(())
    }

    async fn get_balance(&self) -> Result<Decimal, VenueError> {
        let path = "/balance-allowance";
        let url = format!("{}{}", self.config.clob_url, path);
        let headers = self.auth_headers("GET", path, "")?;

        // Signature type: defaults to 2 (proxy wallet), overridable via env.
        let signature_type = read_env_any(&["POLY_SIGNATURE_TYPE"])
            .filter(|v| matches!(v.as_str(), "0" | "1" | "2"))
            .unwrap_or_else(|| {
                if self.config.proxy_wallet.is_some() {
                    "2".to_string()
                } else {
                    "0".to_string()
                }
            });

        debug!(
            url = %url,
            signature_type = %signature_type,
            "fetching CLOB balance-allowance"
        );

        let resp: serde_json::Value = self
            .http
            .get(&url)
            .query(&[
                ("asset_type", "COLLATERAL"),
                ("signature_type", signature_type.as_str()),
            ])
            .headers(headers)
            .send()
            .await?
            .error_for_status()
            .map_err(VenueError::Http)?
            .json()
            .await
            .map_err(VenueError::Http)?;

        parse_balance_allowance_response(&resp).ok_or_else(|| {
            VenueError::Unsupported(format!("failed to parse balance from response: {}", resp))
        })
    }

    fn fee_schedule(&self) -> &FeeSchedule {
        &self.fee_schedule
    }

    fn venue_type(&self) -> VenueType {
        VenueType::International
    }
}

/// Parse Gamma API response into TargetMarket structs.
fn parse_gamma_markets(
    resp: &serde_json::Value,
    filter: &MarketFilter,
    fallback_keywords: Option<&[String]>,
) -> Vec<TargetMarket> {
    let mut markets = Vec::new();

    let events = match resp.as_array() {
        Some(arr) => arr,
        None => return markets,
    };

    let now = Utc::now();
    let re_dollar = regex::Regex::new(r"\$([0-9,]+(?:\.\d+)?)").ok();
    let re_keyword =
        regex::Regex::new(r"(?i)(?:above|below|over|under)\s+\$?([0-9,]+(?:\.\d+)?)").ok();
    let re_updown_window = regex::Regex::new(
        r"(?i)[,\-\s]([A-Za-z]+)\s+(\d{1,2}),\s*(\d{1,2}:\d{2}\s*[AP]M)\s*-\s*(\d{1,2}:\d{2}\s*[AP]M)\s*ET",
    )
    .ok();

    for event in events {
        let event_title = event
            .get("title")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let event_slug = event
            .get("slug")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();

        let event_markets = match event.get("markets").and_then(|m| m.as_array()) {
            Some(m) => m,
            None => continue,
        };

        for market in event_markets {
            if filter.active_only {
                if market
                    .get("active")
                    .and_then(|v| v.as_bool())
                    .is_some_and(|active| !active)
                {
                    continue;
                }
                if market
                    .get("closed")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    continue;
                }
                if market
                    .get("archived")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    continue;
                }
            }

            let question = market
                .get("question")
                .or_else(|| market.get("groupItemTitle"))
                .and_then(|q| q.as_str())
                .unwrap_or("");
            let question_lc = question.to_ascii_lowercase();
            let market_slug = market
                .get("slug")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();

            if let Some(keywords) = fallback_keywords {
                let matches_keyword = keywords.iter().any(|kw| {
                    question_lc.contains(kw)
                        || market_slug.contains(kw)
                        || event_title.contains(kw)
                        || event_slug.contains(kw)
                });
                if !matches_keyword {
                    continue;
                }
            }

            let end_date_str = market
                .get("endDate")
                .or_else(|| market.get("end_date"))
                .and_then(|d| d.as_str())
                .unwrap_or("");

            let end_time = match end_date_str.parse::<chrono::DateTime<Utc>>() {
                Ok(t) => t,
                Err(_) => continue,
            };

            // Skip already-expired markets
            if end_time < now {
                continue;
            }

            // Parse CLOB token IDs
            let clob_token_ids = parse_string_list(
                market
                    .get("clobTokenIds")
                    .or_else(|| market.get("clob_token_ids")),
            );

            if clob_token_ids.len() < 2 {
                continue;
            }

            // Parse outcome prices
            let outcome_prices = parse_decimal_list(market.get("outcomePrices"));
            let outcomes = parse_string_list(market.get("outcomes"));

            let (yes_price, no_price) = if outcome_prices.len() >= 2 {
                (outcome_prices[0], outcome_prices[1])
            } else {
                continue;
            };

            // Parse strike price from question
            let strike = parse_strike_price(question, re_dollar.as_ref(), re_keyword.as_ref())
                .unwrap_or(Decimal::ZERO);
            let up_down_market = is_up_down_market(question, &outcomes);
            let (polymarket_price_to_beat, polymarket_price_to_beat_source) =
                extract_polymarket_price_to_beat_from_event_market(event, market);
            if strike <= Decimal::ZERO && !up_down_market {
                continue;
            }
            let start_time = parse_market_start_time(
                market,
                question,
                end_time,
                up_down_market,
                re_updown_window.as_ref(),
            );

            // Check duration constraint using actual market window when available
            let market_duration_secs = if let Some(start) = start_time {
                (end_time - start).num_seconds()
            } else {
                (end_time - now).num_seconds()
            };
            if market_duration_secs <= 0 || market_duration_secs > filter.max_duration_secs as i64 {
                continue;
            }

            let volume = market
                .get("volume")
                .and_then(parse_decimal_value)
                .unwrap_or(Decimal::ZERO);

            let liquidity = market
                .get("liquidity")
                .and_then(parse_decimal_value)
                .unwrap_or(Decimal::ZERO);

            let condition_id = market
                .get("conditionId")
                .or_else(|| market.get("condition_id"))
                .or_else(|| market.get("id"))
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();

            markets.push(TargetMarket {
                condition_id,
                token_id_yes: clob_token_ids[0].clone(),
                token_id_no: clob_token_ids[1].clone(),
                question: question.to_string(),
                strike_price: strike,
                start_time,
                end_time,
                current_yes_price: yes_price,
                current_no_price: no_price,
                polymarket_price_to_beat,
                polymarket_price_to_beat_source,
                liquidity,
                volume,
            });
        }
    }

    markets
}

fn parse_strike_price(
    question: &str,
    re_dollar: Option<&regex::Regex>,
    re_keyword: Option<&regex::Regex>,
) -> Option<Decimal> {
    let from_dollar = re_dollar
        .and_then(|r| r.captures(question))
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()));
    let from_keyword = re_keyword
        .and_then(|r| r.captures(question))
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()));

    from_dollar
        .or(from_keyword)
        .and_then(|s| s.replace(',', "").parse::<Decimal>().ok())
}

fn is_up_down_market(question: &str, outcomes: &[String]) -> bool {
    let question_lc = question.to_ascii_lowercase();
    if question_lc.contains("up or down") || question_lc.contains("up/down") {
        return true;
    }

    if outcomes.len() < 2 {
        return false;
    }

    let first = outcomes[0].trim().to_ascii_lowercase();
    let second = outcomes[1].trim().to_ascii_lowercase();
    (first == "up" && second == "down") || (first == "down" && second == "up")
}

fn parse_market_start_time(
    market: &serde_json::Value,
    question: &str,
    end_time: DateTime<Utc>,
    up_down_market: bool,
    re_updown_window: Option<&regex::Regex>,
) -> Option<DateTime<Utc>> {
    for field in ["eventStartTime", "startTime", "startDate"] {
        let parsed = market
            .get(field)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
            .and_then(|t| sanitize_market_start_time(t, end_time));
        if parsed.is_some() {
            return parsed;
        }
    }

    if up_down_market {
        return parse_up_down_start_from_question(question, end_time, re_updown_window)
            .and_then(|t| sanitize_market_start_time(t, end_time));
    }

    None
}

fn sanitize_market_start_time(
    candidate: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let delta = end_time - candidate;
    if delta.num_seconds() <= 0 {
        return None;
    }
    if delta > chrono::Duration::hours(24) {
        return None;
    }
    Some(candidate)
}

fn parse_up_down_start_from_question(
    question: &str,
    end_time: DateTime<Utc>,
    re_updown_window: Option<&regex::Regex>,
) -> Option<DateTime<Utc>> {
    let re = re_updown_window?;
    let caps = re.captures(question)?;

    let month_name = caps.get(1)?.as_str();
    let day: u32 = caps.get(2)?.as_str().parse().ok()?;
    let start_time_raw = caps.get(3)?.as_str();

    let month = parse_month_name(month_name)?;
    let start_local_time = parse_et_time(start_time_raw)?;
    let end_local_date = to_et(end_time).date_naive();

    let mut year = end_local_date.year();
    let mut start_date = NaiveDate::from_ymd_opt(year, month, day)?;
    if start_date > end_local_date + chrono::Duration::days(1) {
        year -= 1;
        start_date = NaiveDate::from_ymd_opt(year, month, day)?;
    }

    let offset_hours = if is_us_eastern_dst(start_date) { 4 } else { 5 };
    let local = start_date.and_time(start_local_time);
    let utc_naive = local + chrono::Duration::hours(offset_hours);
    Some(Utc.from_utc_datetime(&utc_naive))
}

fn parse_month_name(name: &str) -> Option<u32> {
    match name.trim().to_ascii_lowercase().as_str() {
        "january" | "jan" => Some(1),
        "february" | "feb" => Some(2),
        "march" | "mar" => Some(3),
        "april" | "apr" => Some(4),
        "may" => Some(5),
        "june" | "jun" => Some(6),
        "july" | "jul" => Some(7),
        "august" | "aug" => Some(8),
        "september" | "sep" | "sept" => Some(9),
        "october" | "oct" => Some(10),
        "november" | "nov" => Some(11),
        "december" | "dec" => Some(12),
        _ => None,
    }
}

fn parse_et_time(raw: &str) -> Option<NaiveTime> {
    let normalized = raw.trim().to_ascii_uppercase().replace(' ', "");
    NaiveTime::parse_from_str(&normalized, "%-I:%M%p")
        .ok()
        .or_else(|| NaiveTime::parse_from_str(&normalized, "%I:%M%p").ok())
}

fn is_us_eastern_dst(date: NaiveDate) -> bool {
    let year = date.year();
    let dst_start = nth_weekday_of_month(year, 3, Weekday::Sun, 2);
    let dst_end = nth_weekday_of_month(year, 11, Weekday::Sun, 1);
    match (dst_start, dst_end) {
        (Some(start), Some(end)) => date >= start && date < end,
        _ => false,
    }
}

fn nth_weekday_of_month(year: i32, month: u32, weekday: Weekday, n: u32) -> Option<NaiveDate> {
    if n == 0 {
        return None;
    }
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let first_wday = first.weekday().num_days_from_monday() as i32;
    let target_wday = weekday.num_days_from_monday() as i32;
    let offset = (target_wday - first_wday).rem_euclid(7);
    let day = 1 + offset as u32 + (n - 1) * 7;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn derive_fallback_keywords(tags: &[String]) -> Vec<String> {
    let mut keywords = HashSet::new();

    for tag in tags {
        let t = tag.trim().to_ascii_lowercase();
        if t.is_empty() {
            continue;
        }
        keywords.insert(t.clone());

        match t.as_str() {
            "bitcoin" => {
                keywords.insert("btc".to_string());
            }
            "btc" => {
                keywords.insert("bitcoin".to_string());
            }
            "ethereum" => {
                keywords.insert("eth".to_string());
            }
            "eth" => {
                keywords.insert("ethereum".to_string());
            }
            "solana" => {
                keywords.insert("sol".to_string());
            }
            "sol" => {
                keywords.insert("solana".to_string());
            }
            "xrp" => {
                keywords.insert("ripple".to_string());
            }
            "ripple" => {
                keywords.insert("xrp".to_string());
            }
            "crypto" | "cryptocurrency" | "cryptocurrencies" => {
                keywords.insert("crypto".to_string());
                keywords.insert("bitcoin".to_string());
                keywords.insert("btc".to_string());
                keywords.insert("ethereum".to_string());
                keywords.insert("eth".to_string());
                keywords.insert("xrp".to_string());
                keywords.insert("ripple".to_string());
                keywords.insert("solana".to_string());
                keywords.insert("sol".to_string());
            }
            _ => {}
        }
    }

    if keywords.is_empty() {
        keywords.insert("bitcoin".to_string());
        keywords.insert("btc".to_string());
    }

    let mut out: Vec<String> = keywords.into_iter().collect();
    out.sort_unstable();
    out
}

fn build_gamma_events_url(
    gamma_url: &str,
    tag_slug: Option<&str>,
    offset: Option<usize>,
) -> Result<String, VenueError> {
    build_gamma_events_url_with_flags(gamma_url, true, false, tag_slug, offset)
}

fn build_gamma_events_url_with_flags(
    gamma_url: &str,
    active: bool,
    closed: bool,
    tag_slug: Option<&str>,
    offset: Option<usize>,
) -> Result<String, VenueError> {
    let base = gamma_url.trim_end_matches('/');
    let mut url = reqwest::Url::parse(&format!("{base}/events")).map_err(|e| {
        VenueError::Unsupported(format!("invalid gamma_url '{}': {}", gamma_url, e))
    })?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("active", if active { "true" } else { "false" });
        query.append_pair("closed", if closed { "true" } else { "false" });
        query.append_pair("order", "startDate");
        query.append_pair("ascending", "false");
        query.append_pair("limit", "200");
        if let Some(tag) = tag_slug {
            query.append_pair("tag_slug", tag);
        }
        if let Some(off) = offset {
            query.append_pair("offset", &off.to_string());
        }
    }
    Ok(url.to_string())
}

fn build_gamma_markets_url(
    gamma_url: &str,
    active: bool,
    closed: bool,
    offset: Option<usize>,
) -> Result<String, VenueError> {
    let base = gamma_url.trim_end_matches('/');
    let mut url = reqwest::Url::parse(&format!("{base}/markets")).map_err(|e| {
        VenueError::Unsupported(format!("invalid gamma_url '{}': {}", gamma_url, e))
    })?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("active", if active { "true" } else { "false" });
        query.append_pair("closed", if closed { "true" } else { "false" });
        query.append_pair("order", "startDate");
        query.append_pair("ascending", "false");
        query.append_pair("limit", "200");
        if let Some(off) = offset {
            query.append_pair("offset", &off.to_string());
        }
    }
    Ok(url.to_string())
}

fn build_gamma_markets_url_for_condition_ids(
    gamma_url: &str,
    condition_ids: &[String],
) -> Result<String, VenueError> {
    build_gamma_markets_url_for_condition_ids_with_flags(
        gamma_url,
        condition_ids,
        true,
        false,
        false,
    )
}

fn build_gamma_markets_url_for_condition_ids_with_flags(
    gamma_url: &str,
    condition_ids: &[String],
    active: bool,
    closed: bool,
    csv: bool,
) -> Result<String, VenueError> {
    if condition_ids.is_empty() {
        return Err(VenueError::Unsupported(
            "condition_ids cannot be empty for targeted Gamma /markets query".to_string(),
        ));
    }

    let base = gamma_url.trim_end_matches('/');
    let mut url = reqwest::Url::parse(&format!("{base}/markets")).map_err(|e| {
        VenueError::Unsupported(format!("invalid gamma_url '{}': {}", gamma_url, e))
    })?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("active", if active { "true" } else { "false" });
        query.append_pair("closed", if closed { "true" } else { "false" });
        query.append_pair("order", "startDate");
        query.append_pair("ascending", "false");
        query.append_pair("limit", "200");
        if csv {
            query.append_pair("condition_ids", &condition_ids.join(","));
        } else {
            for condition_id in condition_ids {
                query.append_pair("condition_ids", condition_id);
            }
        }
    }
    Ok(url.to_string())
}

fn build_gamma_markets_url_for_condition_ids_csv(
    gamma_url: &str,
    condition_ids: &[String],
) -> Result<String, VenueError> {
    build_gamma_markets_url_for_condition_ids_with_flags(
        gamma_url,
        condition_ids,
        true,
        false,
        true,
    )
}

fn market_dedupe_key(market: &TargetMarket) -> String {
    if !market.condition_id.is_empty() {
        format!("condition:{}", market.condition_id)
    } else {
        format!("tokens:{}:{}", market.token_id_yes, market.token_id_no)
    }
}

fn merge_unique_markets(
    incoming: Vec<TargetMarket>,
    into: &mut Vec<TargetMarket>,
    seen: &mut HashSet<String>,
) {
    for market in incoming {
        let key = market_dedupe_key(&market);
        if seen.insert(key) {
            into.push(market);
        }
    }
}

fn parse_decimal_value(value: &serde_json::Value) -> Option<Decimal> {
    match value {
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            if let Ok(v) = trimmed.parse::<Decimal>() {
                return Some(v);
            }

            let mut normalized = trimmed.replace(',', "");
            if let Some(stripped) = normalized.strip_prefix('$') {
                normalized = stripped.trim().to_string();
            }
            let lowered = normalized.to_ascii_lowercase();
            if lowered.ends_with("usdc") {
                normalized = normalized[..normalized.len().saturating_sub(4)]
                    .trim()
                    .to_string();
            } else if lowered.ends_with("usd") {
                normalized = normalized[..normalized.len().saturating_sub(3)]
                    .trim()
                    .to_string();
            }
            if let Ok(v) = normalized.parse::<Decimal>() {
                return Some(v);
            }

            // Last resort for noisy strings like "threshold: 66,899.60 (source...)"
            let re = regex::Regex::new(r"([0-9][0-9,]*(?:\.[0-9]+)?)").ok()?;
            let caps = re.captures(trimmed)?;
            let token = caps.get(1)?.as_str();
            decimal_from_capture(token)
        }
        serde_json::Value::Number(n) => n.to_string().parse::<Decimal>().ok(),
        serde_json::Value::Object(map) => {
            for key in [
                "value",
                "price",
                "amount",
                "threshold",
                "referencePrice",
                "priceToBeat",
                "targetPrice",
                "raw",
            ] {
                if let Some(parsed) = map.get(key).and_then(parse_decimal_value) {
                    return Some(parsed);
                }
            }
            map.values().find_map(parse_decimal_value)
        }
        serde_json::Value::Array(arr) => arr.iter().find_map(parse_decimal_value),
        _ => None,
    }
}

fn decimal_from_capture(raw: &str) -> Option<Decimal> {
    raw.replace(',', "").trim().parse::<Decimal>().ok()
}

fn get_nested<'a>(value: &'a serde_json::Value, dotted_key: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for part in dotted_key.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

fn extract_decimal_with_source(
    market: &serde_json::Value,
    keys: &[&str],
) -> Option<(Decimal, String)> {
    for key in keys {
        if let Some(v) = get_nested(market, key).and_then(parse_decimal_value) {
            if v <= Decimal::ZERO {
                continue;
            }
            return Some((v, format!("field:{}", key)));
        }
    }
    None
}

fn normalize_json_key(key: &str) -> String {
    key.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

fn extract_decimal_with_source_recursive(
    value: &serde_json::Value,
    keys: &[&str],
) -> Option<(Decimal, String)> {
    let key_set: HashSet<String> = keys.iter().map(|k| normalize_json_key(k)).collect();
    let mut stack: Vec<(String, &serde_json::Value)> = vec![(String::new(), value)];

    while let Some((path, node)) = stack.pop() {
        match node {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let child_path = if path.is_empty() {
                        key.to_string()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    if key_set.contains(&normalize_json_key(key)) {
                        if let Some(parsed) = parse_decimal_value(child) {
                            if parsed > Decimal::ZERO {
                                return Some((parsed, format!("field:{}", child_path)));
                            }
                        }
                    }
                    stack.push((child_path, child));
                }
            }
            serde_json::Value::Array(arr) => {
                for (idx, child) in arr.iter().enumerate().rev() {
                    let child_path = if path.is_empty() {
                        format!("[{}]", idx)
                    } else {
                        format!("{}[{}]", path, idx)
                    };
                    stack.push((child_path, child));
                }
            }
            _ => {}
        }
    }

    None
}

fn extract_decimal_from_text_with_source(
    market: &serde_json::Value,
    fields: &[&str],
    patterns: &[regex::Regex],
) -> Option<(Decimal, String)> {
    for field in fields {
        let Some(text) = get_nested(market, field).and_then(|v| v.as_str()) else {
            continue;
        };
        for re in patterns {
            let Some(caps) = re.captures(text) else {
                continue;
            };
            let Some(m) = caps.get(1) else {
                continue;
            };
            if let Some(v) = decimal_from_capture(m.as_str()) {
                if v <= Decimal::ZERO {
                    continue;
                }
                return Some((v, format!("text:{}", field)));
            }
        }
    }
    None
}

fn extract_decimal_from_any_text_with_source(
    value: &serde_json::Value,
    patterns: &[regex::Regex],
) -> Option<(Decimal, String)> {
    let mut stack: Vec<(String, &serde_json::Value)> = vec![(String::new(), value)];

    while let Some((path, node)) = stack.pop() {
        match node {
            serde_json::Value::String(text) => {
                for re in patterns {
                    let Some(caps) = re.captures(text) else {
                        continue;
                    };
                    let Some(m) = caps.get(1) else {
                        continue;
                    };
                    if let Some(v) = decimal_from_capture(m.as_str()) {
                        if v > Decimal::ZERO {
                            let source_path = if path.is_empty() { "<root>" } else { &path };
                            return Some((v, format!("text:{}", source_path)));
                        }
                    }
                }
            }
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let child_path = if path.is_empty() {
                        key.to_string()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    stack.push((child_path, child));
                }
            }
            serde_json::Value::Array(arr) => {
                for (idx, child) in arr.iter().enumerate().rev() {
                    let child_path = if path.is_empty() {
                        format!("[{}]", idx)
                    } else {
                        format!("{}[{}]", path, idx)
                    };
                    stack.push((child_path, child));
                }
            }
            _ => {}
        }
    }

    None
}

fn price_to_beat_patterns() -> Vec<regex::Regex> {
    [
        r"(?i)price\s*to\s*beat[^0-9$-]*\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"(?i)reference\s*price[^0-9$-]*\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"(?i)(?:higher|lower|above|below)\s+than\s+\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"(?i)(?:above|below|over|under)\s+\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"(?i)target(?:\s*price)?[^0-9$-]*\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"(?i)threshold[^0-9$-]*\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
    ]
    .iter()
    .filter_map(|p| regex::Regex::new(p).ok())
    .collect()
}

fn final_price_patterns() -> Vec<regex::Regex> {
    [
        r"(?i)final\s*price[^0-9$-]*\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"(?i)settlement\s*price[^0-9$-]*\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
        r"(?i)closed\s*at[^0-9$-]*\$?([0-9][0-9,]*(?:\.[0-9]+)?)",
    ]
    .iter()
    .filter_map(|p| regex::Regex::new(p).ok())
    .collect()
}

fn extract_polymarket_price_to_beat(
    market: &serde_json::Value,
) -> (Option<Decimal>, Option<String>) {
    const NUMERIC_KEYS: [&str; 21] = [
        "priceToBeat",
        "price_to_beat",
        "priceToBeatValue",
        "targetPrice",
        "target_price",
        "referencePrice",
        "reference_price",
        "startPrice",
        "start_price",
        "initialValue",
        "initial_value",
        "groupItemThreshold",
        "group_item_threshold",
        "xAxisValue",
        "x_axis_value",
        "yAxisValue",
        "y_axis_value",
        "lowerBound",
        "upperBound",
        "metadata.priceToBeat",
        "metadata.referencePrice",
    ];
    if let Some((value, source)) = extract_decimal_with_source(market, &NUMERIC_KEYS) {
        return (Some(value), Some(source));
    }

    const RECURSIVE_NUMERIC_KEYS: [&str; 20] = [
        "priceToBeat",
        "price_to_beat",
        "priceToBeatValue",
        "targetPrice",
        "target_price",
        "referencePrice",
        "reference_price",
        "startPrice",
        "start_price",
        "initialValue",
        "initial_value",
        "groupItemThreshold",
        "group_item_threshold",
        "xAxisValue",
        "x_axis_value",
        "yAxisValue",
        "y_axis_value",
        "lowerBound",
        "upperBound",
        "threshold",
    ];
    if let Some((value, source)) =
        extract_decimal_with_source_recursive(market, &RECURSIVE_NUMERIC_KEYS)
    {
        return (Some(value), Some(source));
    }

    const TEXT_FIELDS: [&str; 9] = [
        "description",
        "rules",
        "resolutionSource",
        "resolution_source",
        "question",
        "groupItemTitle",
        "group_item_title",
        "title",
        "subtitle",
    ];
    let patterns = price_to_beat_patterns();
    if let Some((value, source)) =
        extract_decimal_from_text_with_source(market, &TEXT_FIELDS, &patterns)
    {
        return (Some(value), Some(source));
    }

    if let Some((value, source)) = extract_decimal_from_any_text_with_source(market, &patterns) {
        return (Some(value), Some(source));
    }

    (None, None)
}

fn extract_polymarket_price_to_beat_from_event_market(
    event: &serde_json::Value,
    market: &serde_json::Value,
) -> (Option<Decimal>, Option<String>) {
    let (market_value, market_source) = extract_polymarket_price_to_beat(market);
    if market_value.is_some() {
        return (market_value, market_source);
    }

    // Avoid scanning sibling market objects under event.markets, which can carry
    // unrelated thresholds for different time slots.
    let event_without_markets = if let Some(map) = event.as_object() {
        let mut shallow = map.clone();
        shallow.remove("markets");
        serde_json::Value::Object(shallow)
    } else {
        event.clone()
    };
    let (event_value, event_source) = extract_polymarket_price_to_beat(&event_without_markets);
    if event_value.is_some() {
        return (event_value, event_source.map(|s| format!("event:{}", s)));
    }

    (None, None)
}

fn extract_official_final_price(market: &serde_json::Value) -> (Option<Decimal>, Option<String>) {
    const NUMERIC_KEYS: [&str; 12] = [
        "finalPrice",
        "final_price",
        "finalValue",
        "final_value",
        "settlementPrice",
        "settlement_price",
        "resolvedPrice",
        "resolved_price",
        "indexFinal",
        "index_final",
        "metadata.finalPrice",
        "metadata.settlementPrice",
    ];
    if let Some((value, source)) = extract_decimal_with_source(market, &NUMERIC_KEYS) {
        return (Some(value), Some(source));
    }

    const TEXT_FIELDS: [&str; 4] = [
        "resolutionSource",
        "resolution_source",
        "description",
        "rules",
    ];
    let patterns = final_price_patterns();
    if let Some((value, source)) =
        extract_decimal_from_text_with_source(market, &TEXT_FIELDS, &patterns)
    {
        return (Some(value), Some(source));
    }

    (None, None)
}

fn parse_yes_outcome_label(value: &serde_json::Value) -> Option<bool> {
    match value {
        serde_json::Value::Bool(v) => Some(*v),
        serde_json::Value::Number(n) => {
            let idx = n.as_i64()?;
            match idx {
                0 => Some(true),
                1 => Some(false),
                _ => None,
            }
        }
        serde_json::Value::String(s) => {
            let normalized = s.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "yes" | "up" | "true" | "1" => Some(true),
                "no" | "down" | "false" | "0" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

fn extract_resolved_yes_outcome(market: &serde_json::Value) -> (Option<bool>, Option<String>) {
    const OUTCOME_KEYS: [&str; 11] = [
        "resolvedOutcome",
        "resolved_outcome",
        "winningOutcome",
        "winning_outcome",
        "winner",
        "result",
        "resolution",
        "resolved",
        "outcome",
        "outcomeIndex",
        "winningOutcomeIndex",
    ];
    for key in OUTCOME_KEYS {
        let Some(value) = get_nested(market, key) else {
            continue;
        };
        if let Some(outcome) = parse_yes_outcome_label(value) {
            return (Some(outcome), Some(format!("field:{}", key)));
        }
    }

    let prices = parse_decimal_list(market.get("outcomePrices"));
    if prices.len() >= 2 {
        let yes = prices[0];
        let no = prices[1];
        if yes >= Decimal::new(95, 2) && no <= Decimal::new(5, 2) {
            return (Some(true), Some("field:outcomePrices".to_string()));
        }
        if yes <= Decimal::new(5, 2) && no >= Decimal::new(95, 2) {
            return (Some(false), Some("field:outcomePrices".to_string()));
        }
    }

    (None, None)
}

fn parse_resolved_at(market: &serde_json::Value) -> Option<DateTime<Utc>> {
    for key in [
        "resolvedAt",
        "resolved_at",
        "resolutionDate",
        "resolution_date",
        "updatedAt",
        "updated_at",
        "closedTime",
        "closed_time",
    ] {
        if let Some(value) = get_nested(market, key)
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<DateTime<Utc>>().ok())
        {
            return Some(value);
        }
    }
    None
}

fn market_condition_id(market: &serde_json::Value) -> Option<String> {
    market
        .get("conditionId")
        .or_else(|| market.get("condition_id"))
        .or_else(|| market.get("id"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

fn find_string_value_for_keys_recursive(
    value: &serde_json::Value,
    normalized_keys: &HashSet<String>,
) -> Option<String> {
    let mut stack = vec![value];
    while let Some(node) = stack.pop() {
        match node {
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    if normalized_keys.contains(&normalize_json_key(k)) {
                        if let Some(s) = v.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                            return Some(s.to_string());
                        }
                    }
                    stack.push(v);
                }
            }
            serde_json::Value::Array(arr) => {
                for v in arr.iter().rev() {
                    stack.push(v);
                }
            }
            _ => {}
        }
    }
    None
}

fn market_condition_id_flexible(market: &serde_json::Value) -> Option<String> {
    market_condition_id(market).or_else(|| {
        let keys: HashSet<String> = ["conditionId", "condition_id"]
            .iter()
            .map(|k| normalize_json_key(k))
            .collect();
        find_string_value_for_keys_recursive(market, &keys)
    })
}

fn normalize_token_id(token_id: &str) -> Option<String> {
    let trimmed = token_id.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

fn canonical_token_pair_key(a: &str, b: &str) -> Option<String> {
    let Some(left) = normalize_token_id(a) else {
        return None;
    };
    let Some(right) = normalize_token_id(b) else {
        return None;
    };

    let mut parts = [left, right];
    parts.sort_unstable();
    Some(format!("{}|{}", parts[0], parts[1]))
}

fn target_market_token_pair_key(market: &TargetMarket) -> Option<String> {
    canonical_token_pair_key(&market.token_id_yes, &market.token_id_no)
}

fn target_market_token_ids(market: &TargetMarket) -> Vec<String> {
    let mut ids = Vec::with_capacity(2);
    if let Some(id) = normalize_token_id(&market.token_id_yes) {
        ids.push(id);
    }
    if let Some(id) = normalize_token_id(&market.token_id_no) {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    ids
}

fn market_token_ids_from_json(market: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut push_token = |raw: &str| {
        if let Some(id) = normalize_token_id(raw) {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    };

    for token in parse_string_list(
        market
            .get("clobTokenIds")
            .or_else(|| market.get("clob_token_ids")),
    ) {
        push_token(&token);
    }

    for key in ["clobTokenId", "clob_token_id", "tokenId", "token_id"] {
        if let Some(token) = market.get(key).and_then(|v| v.as_str()) {
            push_token(token);
        }
    }

    // Some Gamma /markets responses nest token ids under arrays/objects.
    let keys: HashSet<String> = ["clobTokenId", "clob_token_id", "tokenId", "token_id"]
        .iter()
        .map(|k| normalize_json_key(k))
        .collect();
    let mut stack = vec![market];
    while let Some(node) = stack.pop() {
        match node {
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    if keys.contains(&normalize_json_key(k)) {
                        if let Some(token) = v.as_str() {
                            push_token(token);
                        }
                    }
                    stack.push(v);
                }
            }
            serde_json::Value::Array(arr) => {
                for v in arr.iter().rev() {
                    stack.push(v);
                }
            }
            _ => {}
        }
    }

    out
}

fn market_token_pair_key_from_json(market: &serde_json::Value) -> Option<String> {
    let tokens = market_token_ids_from_json(market);
    if tokens.len() < 2 {
        return None;
    }
    canonical_token_pair_key(&tokens[0], &tokens[1])
}

fn normalize_condition_id(condition_id: &str) -> Option<String> {
    let trimmed = condition_id.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

fn markets_array_from_response<'a>(
    resp: &'a serde_json::Value,
) -> Option<&'a Vec<serde_json::Value>> {
    resp.as_array()
        .or_else(|| resp.get("data").and_then(|v| v.as_array()))
        .or_else(|| resp.get("markets").and_then(|v| v.as_array()))
}

fn find_market_in_events_by_condition_id<'a>(
    resp: &'a serde_json::Value,
    condition_id: &str,
) -> Option<&'a serde_json::Value> {
    let events = resp.as_array()?;
    for event in events {
        let Some(markets) = event.get("markets").and_then(|v| v.as_array()) else {
            continue;
        };
        for market in markets {
            if market_matches_condition_id(market, condition_id) {
                return Some(market);
            }
        }
    }
    None
}

fn find_market_in_markets_by_condition_id<'a>(
    resp: &'a serde_json::Value,
    condition_id: &str,
) -> Option<&'a serde_json::Value> {
    let rows = markets_array_from_response(resp)?;
    rows.iter()
        .find(|market| market_matches_condition_id(market, condition_id))
}

fn market_matches_condition_id(market: &serde_json::Value, condition_id: &str) -> bool {
    let Some(target) = normalize_condition_id(condition_id) else {
        return false;
    };
    market_condition_id_flexible(market)
        .and_then(|id| normalize_condition_id(&id))
        .is_some_and(|id| id == target)
}

fn parse_market_resolution(market: &serde_json::Value, condition_id: &str) -> MarketResolution {
    let (resolved_yes_outcome, resolved_source) = extract_resolved_yes_outcome(market);
    let (official_final_price, final_source) = extract_official_final_price(market);
    let (official_price_to_beat, price_to_beat_source) = extract_polymarket_price_to_beat(market);
    let resolved_at = parse_resolved_at(market);

    let resolution_source = resolved_source
        .or(final_source)
        .or(price_to_beat_source)
        .unwrap_or_else(|| "unknown".to_string());

    MarketResolution {
        condition_id: condition_id.to_string(),
        resolved_yes_outcome,
        official_final_price,
        official_price_to_beat,
        resolved_at,
        resolution_source,
    }
}

fn parse_user_ws_events(value: &serde_json::Value) -> Vec<OrderEvent> {
    let mut out = Vec::new();
    collect_user_events(value, &mut out);
    out
}

fn collect_user_events(value: &serde_json::Value, out: &mut Vec<OrderEvent>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(event) = parse_single_trade_event(value) {
                out.push(event);
            }
            for key in ["event", "data", "payload", "orders", "trades"] {
                if let Some(next) = map.get(key) {
                    collect_user_events(next, out);
                }
            }
            for v in map.values() {
                if matches!(v, serde_json::Value::Array(_)) {
                    collect_user_events(v, out);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_user_events(v, out);
            }
        }
        _ => {}
    }
}

fn parse_single_trade_event(value: &serde_json::Value) -> Option<OrderEvent> {
    let event_type = value
        .get("event_type")
        .or_else(|| value.get("type"))
        .or_else(|| value.get("channel"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_uppercase();

    let status = value
        .get("trade_status")
        .or_else(|| value.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_uppercase();

    let looks_like_trade =
        event_type.contains("TRADE") || value.get("trade_status").is_some() || !status.is_empty();
    if !looks_like_trade {
        return None;
    }

    let order_id = value
        .get("order_id")
        .or_else(|| value.get("orderID"))
        .or_else(|| value.get("id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())?;

    let fill_price = value
        .get("price")
        .or_else(|| value.get("fill_price"))
        .or_else(|| value.get("avg_price"))
        .or_else(|| value.get("matched_price"))
        .and_then(parse_decimal_value)
        .unwrap_or(Decimal::ZERO);

    let fill_amount = value
        .get("size")
        .or_else(|| value.get("fill_amount"))
        .or_else(|| value.get("filled_size"))
        .or_else(|| value.get("matched_size"))
        .and_then(parse_decimal_value)
        .unwrap_or(Decimal::ZERO);

    if matches!(
        status.as_str(),
        "MATCHED" | "MINED" | "CONFIRMED" | "FILLED" | "TRADE"
    ) {
        return Some(OrderEvent::Filled {
            order_id,
            fill_price,
            fill_amount,
        });
    }

    if status.contains("PARTIAL") {
        return Some(OrderEvent::PartialFill {
            order_id,
            fill_price,
            fill_amount,
        });
    }

    if matches!(status.as_str(), "CANCELED" | "CANCELLED") {
        return Some(OrderEvent::Cancelled { order_id });
    }

    if matches!(status.as_str(), "FAILED" | "REJECTED" | "ERROR") {
        return Some(OrderEvent::Rejected {
            order_id,
            reason: status,
        });
    }

    None
}

fn parse_balance_allowance_response(resp: &serde_json::Value) -> Option<Decimal> {
    // Primary shape from CLOB client: {"balance":"...", "allowances":{...}}
    if let Some(balance) = resp.get("balance").and_then(parse_decimal_value) {
        return Some(balance);
    }

    // Common wrapped shape: {"data":{"balance":"..."}}
    if let Some(balance) = resp
        .get("data")
        .and_then(|v| v.get("balance"))
        .and_then(parse_decimal_value)
    {
        return Some(balance);
    }

    // Accept nested balance object with known fields.
    for key in ["balance", "available", "amount", "total", "usdc"] {
        if let Some(v) = resp
            .get("balance")
            .and_then(|b| b.get(key))
            .and_then(parse_decimal_value)
        {
            return Some(v);
        }
        if let Some(v) = resp.get(key).and_then(parse_decimal_value) {
            return Some(v);
        }
    }

    None
}

fn parse_place_order_response(resp: &serde_json::Value) -> Option<String> {
    for key in ["orderID", "orderId", "order_id", "id"] {
        if let Some(id) = resp.get(key).and_then(|v| v.as_str()) {
            let trimmed = id.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }

    if resp.get("success").and_then(|v| v.as_bool()) == Some(false) {
        return None;
    }

    for key in ["data", "order", "result"] {
        if let Some(nested) = resp.get(key) {
            if let Some(order_id) = parse_place_order_response(nested) {
                return Some(order_id);
            }
        }
    }

    None
}

fn read_env_any(names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn hmac_sha256_base64(secret_base64: &str, message: &str) -> Result<String, String> {
    let secret = base64::engine::general_purpose::STANDARD
        .decode(secret_base64)
        .map_err(|e| format!("invalid base64 secret: {}", e))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret)
        .map_err(|e| format!("failed to initialize HMAC: {}", e))?;
    mac.update(message.as_bytes());
    let signature_bytes = mac.finalize().into_bytes();
    Ok(base64::engine::general_purpose::STANDARD.encode(signature_bytes))
}

fn parse_string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    match value {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| match v {
                serde_json::Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .collect(),
        Some(serde_json::Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Vec::new();
            }

            if let Ok(serde_json::Value::Array(arr)) =
                serde_json::from_str::<serde_json::Value>(trimmed)
            {
                return arr
                    .iter()
                    .filter_map(|v| match v {
                        serde_json::Value::String(s) if !s.trim().is_empty() => {
                            Some(s.trim().to_string())
                        }
                        serde_json::Value::Number(n) => Some(n.to_string()),
                        _ => None,
                    })
                    .collect();
            }

            vec![trimmed.to_string()]
        }
        _ => Vec::new(),
    }
}

fn parse_decimal_list(value: Option<&serde_json::Value>) -> Vec<Decimal> {
    match value {
        Some(serde_json::Value::Array(arr)) => arr.iter().filter_map(parse_decimal_value).collect(),
        Some(serde_json::Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Vec::new();
            }

            if let Ok(serde_json::Value::Array(arr)) =
                serde_json::from_str::<serde_json::Value>(trimmed)
            {
                return arr.iter().filter_map(parse_decimal_value).collect();
            }

            parse_decimal_value(&serde_json::Value::String(trimmed.to_string()))
                .into_iter()
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Parse CLOB /book response into OrderBook.
fn parse_orderbook(token_id: &str, resp: &serde_json::Value) -> Result<OrderBook, VenueError> {
    let parse_levels = |key: &str| -> Vec<OrderBookLevel> {
        resp.get(key)
            .and_then(|side| side.as_array())
            .map(|levels| {
                levels
                    .iter()
                    .filter_map(|level| {
                        let price = level
                            .get("price")
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<Decimal>().ok())?;
                        let size = level
                            .get("size")
                            .and_then(|s| s.as_str())
                            .and_then(|s| s.parse::<Decimal>().ok())?;
                        Some(OrderBookLevel { price, size })
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    let mut bids = parse_levels("bids");
    let mut asks = parse_levels("asks");

    // Bids sorted descending (highest first) for best_bid → .first()
    bids.sort_by(|a, b| b.price.cmp(&a.price));
    // Asks sorted ascending (lowest first) for best_ask → .first()
    asks.sort_by(|a, b| a.price.cmp(&b.price));

    // TODO: Parse tick_size, min_order_size from /book metadata
    let tick_size = Decimal::new(1, 2); // 0.01 default
    let min_order_size = Decimal::new(1, 0); // $1 default

    Ok(OrderBook {
        token_id: token_id.to_string(),
        bids,
        asks,
        tick_size,
        min_order_size,
        neg_risk: false,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use serde_json::json;

    fn market_filter(max_duration_secs: u64) -> MarketFilter {
        MarketFilter {
            tags: Vec::new(),
            max_duration_secs,
            active_only: true,
        }
    }

    fn market_for_dedupe(condition_id: &str, token_yes: &str, token_no: &str) -> TargetMarket {
        TargetMarket {
            condition_id: condition_id.to_string(),
            token_id_yes: token_yes.to_string(),
            token_id_no: token_no.to_string(),
            question: "Will BTC be above $100,000?".to_string(),
            strike_price: Decimal::from(100_000),
            start_time: None,
            end_time: Utc::now() + Duration::seconds(300),
            current_yes_price: Decimal::new(52, 2),
            current_no_price: Decimal::new(48, 2),
            polymarket_price_to_beat: None,
            polymarket_price_to_beat_source: None,
            liquidity: Decimal::from(1000),
            volume: Decimal::from(2000),
        }
    }

    #[test]
    fn parse_gamma_markets_keeps_up_down_without_strike() {
        let end_time = Utc::now() + Duration::hours(12);
        let start_time = end_time - Duration::seconds(300);
        let resp = json!([
            {
                "title": "Ethereum Up or Down",
                "slug": "eth-up-down",
                "markets": [
                    {
                        "active": true,
                        "closed": false,
                        "archived": false,
                        "question": "Ethereum Up or Down - test",
                        "endDate": end_time.to_rfc3339(),
                        "eventStartTime": start_time.to_rfc3339(),
                        "clobTokenIds": "[\"yes-token\",\"no-token\"]",
                        "outcomes": "[\"Up\",\"Down\"]",
                        "outcomePrices": "[\"0.52\",\"0.48\"]",
                        "conditionId": "cond-updown",
                        "liquidity": "1000",
                        "volume": "2000"
                    }
                ]
            }
        ]);

        let markets = parse_gamma_markets(&resp, &market_filter(900), None);
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].strike_price, Decimal::ZERO);
        assert_eq!(markets[0].token_id_yes, "yes-token");
        assert_eq!(markets[0].token_id_no, "no-token");
        assert_eq!(markets[0].start_time, Some(start_time));
    }

    #[test]
    fn parse_gamma_markets_drops_non_updown_without_strike() {
        let end_time = Utc::now() + Duration::hours(12);
        let start_time = end_time - Duration::seconds(300);
        let resp = json!([
            {
                "title": "No Strike Market",
                "slug": "no-strike",
                "markets": [
                    {
                        "active": true,
                        "closed": false,
                        "archived": false,
                        "question": "Will XRP be higher later?",
                        "endDate": end_time.to_rfc3339(),
                        "eventStartTime": start_time.to_rfc3339(),
                        "clobTokenIds": "[\"yes-token\",\"no-token\"]",
                        "outcomes": "[\"Yes\",\"No\"]",
                        "outcomePrices": "[\"0.52\",\"0.48\"]",
                        "conditionId": "cond-no-strike",
                        "liquidity": "1000",
                        "volume": "2000"
                    }
                ]
            }
        ]);

        let markets = parse_gamma_markets(&resp, &market_filter(900), None);
        assert!(markets.is_empty());
    }

    #[test]
    fn parse_gamma_markets_uses_event_start_time_when_present() {
        let end_time = Utc::now() + Duration::hours(12);
        let start_time = end_time - Duration::seconds(300);
        let resp = json!([
            {
                "title": "Ethereum Up or Down",
                "slug": "eth-up-down",
                "markets": [
                    {
                        "active": true,
                        "closed": false,
                        "archived": false,
                        "question": "Ethereum Up or Down - test",
                        "endDate": end_time.to_rfc3339(),
                        "eventStartTime": start_time.to_rfc3339(),
                        "clobTokenIds": "[\"yes-token\",\"no-token\"]",
                        "outcomes": "[\"Up\",\"Down\"]",
                        "outcomePrices": "[\"0.52\",\"0.48\"]",
                        "conditionId": "cond-updown",
                        "liquidity": "1000",
                        "volume": "2000"
                    }
                ]
            }
        ]);

        let markets = parse_gamma_markets(&resp, &market_filter(900), None);
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].start_time, Some(start_time));
    }

    #[test]
    fn parse_gamma_markets_prefers_condition_id_snake_case() {
        let end_time = Utc::now() + Duration::hours(1);
        let start_time = end_time - Duration::seconds(300);
        let resp = json!([
            {
                "title": "Solana Up or Down",
                "slug": "sol-up-down",
                "markets": [
                    {
                        "active": true,
                        "closed": false,
                        "archived": false,
                        "question": "Solana Up or Down - test",
                        "endDate": end_time.to_rfc3339(),
                        "eventStartTime": start_time.to_rfc3339(),
                        "clobTokenIds": "[\"yes-token\",\"no-token\"]",
                        "outcomes": "[\"Up\",\"Down\"]",
                        "outcomePrices": "[\"0.52\",\"0.48\"]",
                        "condition_id": "cond-snake",
                        "id": "market-id-only",
                        "liquidity": "1000",
                        "volume": "2000"
                    }
                ]
            }
        ]);

        let markets = parse_gamma_markets(&resp, &market_filter(900), None);
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].condition_id, "cond-snake");
    }

    #[test]
    fn parse_gamma_markets_filters_by_window_duration_not_time_to_expiry() {
        // Regression: old code computed duration as (end_time - now), which rejected
        // markets created hours before their 5-minute window.
        let end_time = Utc::now() + Duration::hours(24);
        let start_time = end_time - Duration::seconds(300); // 5-minute window
        let resp = json!([
            {
                "title": "Bitcoin Up or Down",
                "slug": "btc-up-down",
                "markets": [
                    {
                        "active": true,
                        "closed": false,
                        "archived": false,
                        "question": "Bitcoin Up or Down - test",
                        "endDate": end_time.to_rfc3339(),
                        "eventStartTime": start_time.to_rfc3339(),
                        "clobTokenIds": "[\"yes-token\",\"no-token\"]",
                        "outcomes": "[\"Up\",\"Down\"]",
                        "outcomePrices": "[\"0.52\",\"0.48\"]",
                        "conditionId": "cond-regression",
                        "liquidity": "1000",
                        "volume": "2000"
                    }
                ]
            }
        ]);

        // max_duration_secs=600 allows the 300s window but would reject 86400s (end_time - now)
        let markets = parse_gamma_markets(&resp, &market_filter(600), None);
        assert_eq!(
            markets.len(),
            1,
            "market with 5-min window should pass duration filter"
        );
        assert_eq!(markets[0].condition_id, "cond-regression");
    }

    #[test]
    fn parse_up_down_start_from_question_extracts_et_window() {
        let re = regex::Regex::new(
            r"(?i)[,\-\s]([A-Za-z]+)\s+(\d{1,2}),\s*(\d{1,2}:\d{2}\s*[AP]M)\s*-\s*(\d{1,2}:\d{2}\s*[AP]M)\s*ET",
        )
        .ok();
        let end_time = "2026-02-18T18:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let question = "XRP Up or Down - February 18, 1:15PM-1:30PM ET";
        let parsed = parse_up_down_start_from_question(question, end_time, re.as_ref()).unwrap();
        let expected = "2026-02-18T18:15:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn extract_price_to_beat_prefers_numeric_fields() {
        let market = json!({
            "priceToBeat": "66899.60",
            "description": "Price to beat is $1.00"
        });
        let (value, source) = extract_polymarket_price_to_beat(&market);
        assert_eq!(value, Some(Decimal::new(6689960, 2)));
        assert_eq!(source.as_deref(), Some("field:priceToBeat"));
    }

    #[test]
    fn extract_price_to_beat_from_text_when_numeric_missing() {
        let market = json!({
            "description": "For this market, the price to beat is $66,899.60 at expiry."
        });
        let (value, source) = extract_polymarket_price_to_beat(&market);
        assert_eq!(value, Some(Decimal::new(6689960, 2)));
        assert_eq!(source.as_deref(), Some("text:description"));
    }

    #[test]
    fn extract_price_to_beat_reads_group_item_threshold() {
        let market = json!({
            "groupItemThreshold": "82.39"
        });
        let (value, source) = extract_polymarket_price_to_beat(&market);
        assert_eq!(value, Some(Decimal::new(8239, 2)));
        assert_eq!(source.as_deref(), Some("field:groupItemThreshold"));
    }

    #[test]
    fn extract_price_to_beat_rejects_zero_group_item_threshold() {
        let market = json!({
            "groupItemThreshold": "0"
        });
        let (value, source) = extract_polymarket_price_to_beat(&market);
        assert!(value.is_none());
        assert!(source.is_none());
    }

    #[test]
    fn extract_price_to_beat_reads_nested_threshold_with_currency_formatting() {
        let market = json!({
            "groupItem": {
                "threshold": "$66,899.60"
            }
        });
        let (value, source) = extract_polymarket_price_to_beat(&market);
        assert_eq!(value, Some(Decimal::new(6689960, 2)));
        assert_eq!(source.as_deref(), Some("field:groupItem.threshold"));
    }

    #[test]
    fn extract_price_to_beat_falls_back_to_event_fields() {
        let event = json!({
            "description": "Price to beat: $1,917.65"
        });
        let market = json!({
            "question": "Ethereum Up or Down - February 19, 1:00PM-1:15PM ET"
        });

        let (value, source) = extract_polymarket_price_to_beat_from_event_market(&event, &market);
        assert_eq!(value, Some(Decimal::new(191765, 2)));
        assert_eq!(source.as_deref(), Some("event:text:description"));
    }

    #[test]
    fn extract_price_to_beat_scans_nested_text_fields() {
        let market = json!({
            "meta": {
                "rules": {
                    "summary": "Official reference price: $1,917.65"
                }
            }
        });
        let (value, source) = extract_polymarket_price_to_beat(&market);
        assert_eq!(value, Some(Decimal::new(191765, 2)));
        assert_eq!(source.as_deref(), Some("text:meta.rules.summary"));
    }

    #[test]
    fn parse_decimal_value_accepts_currency_strings() {
        let v = parse_decimal_value(&json!("$66,899.60 USD")).expect("should parse");
        assert_eq!(v, Decimal::new(6689960, 2));
    }

    #[test]
    fn parse_market_resolution_uses_terminal_outcome_prices() {
        let market = json!({
            "conditionId": "cond-1",
            "outcomePrices": "[\"1\",\"0\"]",
            "resolutionSource": "Resolved by market rules"
        });

        let parsed = parse_market_resolution(&market, "cond-1");
        assert_eq!(parsed.condition_id, "cond-1");
        assert_eq!(parsed.resolved_yes_outcome, Some(true));
        assert!(parsed.official_final_price.is_none());
    }

    #[test]
    fn parse_market_resolution_can_be_unresolved() {
        let market = json!({
            "conditionId": "cond-2",
            "outcomePrices": "[\"0.61\",\"0.39\"]"
        });

        let parsed = parse_market_resolution(&market, "cond-2");
        assert_eq!(parsed.resolved_yes_outcome, None);
    }

    #[test]
    fn find_market_by_condition_id_allows_unresolved_resolution_parsing() {
        let resp = json!([
            {
                "markets": [
                    {
                        "conditionId": "cond-3",
                        "outcomePrices": "[\"0.55\",\"0.45\"]"
                    }
                ]
            }
        ]);

        let market =
            find_market_in_events_by_condition_id(&resp, "cond-3").expect("market should be found");
        let parsed = parse_market_resolution(market, "cond-3");
        assert_eq!(parsed.resolved_yes_outcome, None);
    }

    #[test]
    fn canonical_token_pair_key_is_order_independent() {
        let left = canonical_token_pair_key("A", "B");
        let right = canonical_token_pair_key("b", "a");
        assert_eq!(left, right);
        assert_eq!(left.as_deref(), Some("a|b"));
    }

    #[test]
    fn market_token_pair_key_from_json_parses_stringified_array() {
        let market = json!({
            "clobTokenIds": "[\"yes-token\", \"no-token\"]"
        });
        let pair = market_token_pair_key_from_json(&market);
        assert_eq!(pair.as_deref(), Some("no-token|yes-token"));
    }

    #[test]
    fn market_token_ids_from_json_reads_singular_fields() {
        let market = json!({
            "clobTokenId": "Yes-Token",
            "token_id": "no-token"
        });
        let ids = market_token_ids_from_json(&market);
        assert_eq!(ids, vec!["yes-token".to_string(), "no-token".to_string()]);
    }

    #[test]
    fn market_token_ids_from_json_reads_nested_token_ids() {
        let market = json!({
            "tokens": [
                {"token_id": "YES-NESTED"},
                {"tokenId": "no-nested"}
            ]
        });
        let ids = market_token_ids_from_json(&market);
        assert_eq!(ids, vec!["yes-nested".to_string(), "no-nested".to_string()]);
    }

    #[test]
    fn market_condition_id_flexible_reads_nested_condition_id() {
        let market = json!({
            "meta": {
                "condition_id": "0xabc123"
            }
        });
        let condition = market_condition_id_flexible(&market);
        assert_eq!(condition.as_deref(), Some("0xabc123"));
    }

    #[test]
    fn parse_decimal_value_reads_nested_object_value() {
        let value = json!({"value": "$66,899.60"});
        let parsed = parse_decimal_value(&value).expect("should parse");
        assert_eq!(parsed, Decimal::new(6689960, 2));
    }

    #[test]
    fn parse_decimal_value_extracts_number_from_noisy_string() {
        let value = json!("threshold: 66,899.60 (UTC source)");
        let parsed = parse_decimal_value(&value).expect("should parse");
        assert_eq!(parsed, Decimal::new(6689960, 2));
    }

    #[test]
    fn parse_balance_allowance_response_primary_shape() {
        let resp = json!({
            "balance": "1234.56",
            "allowances": {"default": "0"}
        });
        let parsed = parse_balance_allowance_response(&resp).unwrap();
        assert_eq!(parsed, Decimal::new(123456, 2));
    }

    #[test]
    fn parse_balance_allowance_response_wrapped_shape() {
        let resp = json!({
            "data": {
                "balance": "99.01"
            }
        });
        let parsed = parse_balance_allowance_response(&resp).unwrap();
        assert_eq!(parsed, Decimal::new(9901, 2));
    }

    #[test]
    fn hmac_sha256_base64_rejects_invalid_secret() {
        let err = hmac_sha256_base64("not-base64", "abc").unwrap_err();
        assert!(err.contains("invalid base64 secret"));
    }

    #[test]
    fn parse_user_ws_events_extracts_filled_trade() {
        let msg = json!({
            "event_type": "TRADE",
            "status": "MINED",
            "order_id": "ord-1",
            "price": "0.53",
            "size": "25"
        });

        let events = parse_user_ws_events(&msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            OrderEvent::Filled {
                order_id,
                fill_price,
                fill_amount,
            } => {
                assert_eq!(order_id, "ord-1");
                assert_eq!(*fill_price, Decimal::new(53, 2));
                assert_eq!(*fill_amount, Decimal::from(25));
            }
            _ => panic!("expected filled event"),
        }
    }

    #[test]
    fn parse_user_ws_events_extracts_partial_from_nested_payload() {
        let msg = json!({
            "event": {
                "event_type": "TRADE",
                "trade_status": "PARTIAL",
                "id": "ord-2",
                "matched_price": "0.44",
                "matched_size": "10"
            }
        });

        let events = parse_user_ws_events(&msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            OrderEvent::PartialFill {
                order_id,
                fill_price,
                fill_amount,
            } => {
                assert_eq!(order_id, "ord-2");
                assert_eq!(*fill_price, Decimal::new(44, 2));
                assert_eq!(*fill_amount, Decimal::from(10));
            }
            _ => panic!("expected partial fill event"),
        }
    }

    #[test]
    fn derive_fallback_keywords_expands_crypto_asset_aliases() {
        let tags = vec![
            "bitcoin".to_string(),
            "eth".to_string(),
            "xrp".to_string(),
            "sol".to_string(),
            "crypto".to_string(),
        ];
        let out = derive_fallback_keywords(&tags);
        let set: HashSet<String> = out.into_iter().collect();

        for keyword in [
            "bitcoin", "btc", "ethereum", "eth", "xrp", "ripple", "solana", "sol", "crypto",
        ] {
            assert!(
                set.contains(keyword),
                "expected keyword '{}' in fallback set",
                keyword
            );
        }
    }

    #[test]
    fn derive_fallback_keywords_dedupes_and_sorts() {
        let tags = vec![
            " BTC ".to_string(),
            "bitcoin".to_string(),
            "btc".to_string(),
            "Bitcoin".to_string(),
        ];
        let out = derive_fallback_keywords(&tags);
        let mut sorted = out.clone();
        sorted.sort_unstable();
        assert_eq!(out, sorted, "keywords should be sorted");

        let unique: HashSet<&String> = out.iter().collect();
        assert_eq!(unique.len(), out.len(), "keywords should be deduplicated");
    }

    #[test]
    fn merge_unique_markets_prefers_unique_condition_or_token_pair() {
        let mut into = Vec::new();
        let mut seen = HashSet::new();

        merge_unique_markets(
            vec![
                market_for_dedupe("cond-1", "yes-1", "no-1"),
                market_for_dedupe("cond-1", "yes-9", "no-9"),
                market_for_dedupe("", "yes-2", "no-2"),
                market_for_dedupe("", "yes-2", "no-2"),
                market_for_dedupe("cond-3", "yes-3", "no-3"),
            ],
            &mut into,
            &mut seen,
        );

        assert_eq!(into.len(), 3);
    }

    #[test]
    fn parse_gamma_markets_with_fallback_keywords_filters_nonmatching_events() {
        let end_date = (Utc::now() + Duration::seconds(300)).to_rfc3339();
        let resp = json!([
            {
                "title": "Bitcoin Markets",
                "slug": "bitcoin-markets",
                "markets": [{
                    "active": true,
                    "closed": false,
                    "archived": false,
                    "question": "Will Bitcoin be above $100,000 by 1pm ET?",
                    "endDate": end_date,
                    "clobTokenIds": "[\"yes-btc\",\"no-btc\"]",
                    "outcomes": "[\"Yes\",\"No\"]",
                    "outcomePrices": "[\"0.51\",\"0.49\"]",
                    "conditionId": "cond-btc",
                    "liquidity": "1000",
                    "volume": "2000"
                }]
            },
            {
                "title": "Politics",
                "slug": "election",
                "markets": [{
                    "active": true,
                    "closed": false,
                    "archived": false,
                    "question": "Will Candidate A get above $1 support index?",
                    "endDate": end_date,
                    "clobTokenIds": "[\"yes-pol\",\"no-pol\"]",
                    "outcomes": "[\"Yes\",\"No\"]",
                    "outcomePrices": "[\"0.51\",\"0.49\"]",
                    "conditionId": "cond-pol",
                    "liquidity": "1000",
                    "volume": "2000"
                }]
            }
        ]);

        let keywords = vec!["bitcoin".to_string()];
        let markets = parse_gamma_markets(&resp, &market_filter(900), Some(&keywords));
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].condition_id, "cond-btc");
    }

    #[test]
    fn build_gamma_events_url_percent_encodes_tag_slug() {
        let url = build_gamma_events_url(
            "https://gamma-api.polymarket.com/",
            Some("bitcoin & ethereum"),
            Some(200),
        )
        .expect("url should be built");
        let parsed = reqwest::Url::parse(&url).expect("url should parse");
        let pairs: std::collections::HashMap<String, String> =
            parsed.query_pairs().into_owned().collect();

        assert_eq!(parsed.path(), "/events");
        assert_eq!(pairs.get("active").map(String::as_str), Some("true"));
        assert_eq!(pairs.get("closed").map(String::as_str), Some("false"));
        assert_eq!(pairs.get("order").map(String::as_str), Some("startDate"));
        assert_eq!(pairs.get("ascending").map(String::as_str), Some("false"));
        assert_eq!(pairs.get("limit").map(String::as_str), Some("200"));
        assert_eq!(
            pairs.get("tag_slug").map(String::as_str),
            Some("bitcoin & ethereum")
        );
        assert_eq!(pairs.get("offset").map(String::as_str), Some("200"));
    }

    #[test]
    fn build_gamma_markets_url_for_condition_ids_uses_repeated_query_params() {
        let condition_ids = vec!["0xbbb".to_string(), "0xaaa".to_string()];
        let url = build_gamma_markets_url_for_condition_ids(
            "https://gamma-api.polymarket.com/",
            &condition_ids,
        )
        .expect("url should be built");
        let parsed = reqwest::Url::parse(&url).expect("url should parse");
        let condition_values: Vec<String> = parsed
            .query_pairs()
            .filter_map(|(k, v)| {
                if k == "condition_ids" {
                    Some(v.into_owned())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(parsed.path(), "/markets");
        assert_eq!(condition_values, condition_ids);
    }

    #[test]
    fn build_gamma_markets_url_for_condition_ids_rejects_empty_input() {
        let err =
            build_gamma_markets_url_for_condition_ids("https://gamma-api.polymarket.com/", &[])
                .expect_err("empty condition IDs should be rejected");
        assert!(
            err.to_string().contains("condition_ids cannot be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_gamma_markets_url_for_condition_ids_csv_uses_single_comma_joined_param() {
        let condition_ids = vec!["0xbbb".to_string(), "0xaaa".to_string()];
        let url = build_gamma_markets_url_for_condition_ids_csv(
            "https://gamma-api.polymarket.com/",
            &condition_ids,
        )
        .expect("url should be built");
        let parsed = reqwest::Url::parse(&url).expect("url should parse");
        let condition_values: Vec<String> = parsed
            .query_pairs()
            .filter_map(|(k, v)| {
                if k == "condition_ids" {
                    Some(v.into_owned())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(parsed.path(), "/markets");
        assert_eq!(condition_values, vec!["0xbbb,0xaaa".to_string()]);
    }

    #[test]
    fn build_gamma_markets_url_for_condition_ids_with_flags_supports_closed_queries() {
        let condition_ids = vec!["0xabc".to_string()];
        let url = build_gamma_markets_url_for_condition_ids_with_flags(
            "https://gamma-api.polymarket.com/",
            &condition_ids,
            false,
            true,
            false,
        )
        .expect("url should be built");
        let parsed = reqwest::Url::parse(&url).expect("url should parse");
        let pairs: std::collections::HashMap<String, String> =
            parsed.query_pairs().into_owned().collect();
        assert_eq!(pairs.get("active").map(String::as_str), Some("false"));
        assert_eq!(pairs.get("closed").map(String::as_str), Some("true"));
    }

    #[test]
    fn find_market_in_markets_by_condition_id_matches_case_insensitive() {
        let resp = json!({
            "data": [{
                "conditionId": "0xABCDEF"
            }]
        });
        let found =
            find_market_in_markets_by_condition_id(&resp, "0xabcdef").expect("market should match");
        assert_eq!(
            market_condition_id_flexible(found).as_deref(),
            Some("0xABCDEF")
        );
    }

    #[test]
    fn find_market_in_events_by_condition_id_matches_case_insensitive() {
        let resp = json!([
            {
                "markets": [
                    {"conditionId": "0xABCDEF"}
                ]
            }
        ]);
        let found =
            find_market_in_events_by_condition_id(&resp, "0xabcdef").expect("market should match");
        assert_eq!(
            market_condition_id_flexible(found).as_deref(),
            Some("0xABCDEF")
        );
    }

    #[test]
    fn chunk_condition_ids_sorts_dedupes_and_chunks() {
        let condition_ids: HashSet<String> = vec![
            "0xccc".to_string(),
            "0xaaa".to_string(),
            "0xbbb".to_string(),
            "0xaaa".to_string(),
        ]
        .into_iter()
        .collect();
        let sorted = sorted_unique_condition_ids(&condition_ids);
        let chunks = chunk_condition_ids(&sorted, 2);

        assert_eq!(
            chunks,
            vec![
                vec!["0xaaa".to_string(), "0xbbb".to_string()],
                vec!["0xccc".to_string()],
            ]
        );
    }

    #[test]
    fn prune_backed_off_candidates_filters_active_backoff_only() {
        let now = Utc::now();
        let mut backoff = HashMap::new();
        backoff.insert(
            "cond:active".to_string(),
            now + chrono::Duration::seconds(120),
        );
        backoff.insert(
            "cond:expired".to_string(),
            now - chrono::Duration::seconds(5),
        );

        let mut missing_by_condition = HashMap::from([
            ("cond:active".to_string(), vec![0usize]),
            ("cond:expired".to_string(), vec![1usize]),
            ("cond:other".to_string(), vec![2usize]),
        ]);
        let mut missing_by_token_pair = HashMap::from([
            ("cond:active".to_string(), vec![0usize]),
            ("pair:other".to_string(), vec![1usize]),
        ]);
        let mut missing_by_token_id = HashMap::from([
            ("cond:active".to_string(), vec![0usize]),
            ("token:other".to_string(), vec![1usize]),
        ]);

        prune_backed_off_candidates(
            now,
            &mut backoff,
            &mut missing_by_condition,
            &mut missing_by_token_pair,
            &mut missing_by_token_id,
        );

        assert!(backoff.contains_key("cond:active"));
        assert!(!backoff.contains_key("cond:expired"));
        assert!(!missing_by_condition.contains_key("cond:active"));
        assert!(missing_by_condition.contains_key("cond:expired"));
        assert!(missing_by_condition.contains_key("cond:other"));
        assert!(!missing_by_token_pair.contains_key("cond:active"));
        assert!(missing_by_token_pair.contains_key("pair:other"));
        assert!(!missing_by_token_id.contains_key("cond:active"));
        assert!(missing_by_token_id.contains_key("token:other"));
    }

    #[test]
    fn collect_found_price_to_beat_from_markets_matches_condition_pair_and_token() {
        let rows = vec![
            json!({
                "conditionId": "0xcond1",
                "groupItemThreshold": "66899.60",
                "clobTokenIds": "[\"yes-1\", \"no-1\"]"
            }),
            json!({
                "groupItemThreshold": "1917.65",
                "clobTokenIds": "[\"yes-2\", \"no-2\"]"
            }),
            json!({
                "groupItemThreshold": "82.39",
                "token_id": "token-3"
            }),
        ];
        let target_conditions = HashSet::from(["0xcond1".to_string()]);
        let target_token_pairs = HashSet::from(["no-2|yes-2".to_string()]);
        let target_token_ids = HashSet::from(["token-3".to_string()]);
        let mut found_by_condition: HashMap<String, (Decimal, String)> = HashMap::new();
        let mut found_by_token_pair: HashMap<String, (Decimal, String)> = HashMap::new();
        let mut found_by_token_id: HashMap<String, (Decimal, String)> = HashMap::new();

        collect_found_price_to_beat_from_markets(
            &rows,
            &target_conditions,
            &target_token_pairs,
            &target_token_ids,
            &mut found_by_condition,
            &mut found_by_token_pair,
            &mut found_by_token_id,
        );

        assert_eq!(
            found_by_condition.get("0xcond1").map(|(v, _)| *v),
            Some(Decimal::new(6689960, 2))
        );
        assert_eq!(
            found_by_token_pair.get("no-2|yes-2").map(|(v, _)| *v),
            Some(Decimal::new(191765, 2))
        );
        assert_eq!(
            found_by_token_id.get("token-3").map(|(v, _)| *v),
            Some(Decimal::new(8239, 2))
        );
    }

    #[test]
    fn count_rows_matching_condition_ids_counts_only_requested_conditions() {
        let rows = vec![
            json!({"conditionId": "0xaaa"}),
            json!({"condition_id": "0xbbb"}),
            json!({"conditionId": "0xccc"}),
            json!({"id": "market-without-condition"}),
        ];
        let condition_ids = vec!["0xaaa".to_string(), "0xbbb".to_string()];
        let matched = count_rows_matching_condition_ids(&rows, &condition_ids);
        assert_eq!(matched, 2);
    }

    #[test]
    fn compute_amounts_buy_side_matches_order_utils_convention() {
        let price = Decimal::new(55, 2);
        let size = Decimal::from(100);
        let (maker, taker, side_u8, side_label) =
            InternationalClient::compute_amounts(Side::Buy, price, size).unwrap();

        assert_eq!(side_u8, SIDE_BUY);
        assert_eq!(side_label, "BUY");
        assert_eq!(maker, U256::from(55_000_000u64));
        assert_eq!(taker, U256::from(100_000_000u64));
    }

    #[test]
    fn compute_amounts_sell_side_matches_order_utils_convention() {
        let price = Decimal::new(40, 2);
        let size = Decimal::from(25);
        let (maker, taker, side_u8, side_label) =
            InternationalClient::compute_amounts(Side::Sell, price, size).unwrap();

        assert_eq!(side_u8, SIDE_SELL);
        assert_eq!(side_label, "SELL");
        assert_eq!(maker, U256::from(25_000_000u64));
        assert_eq!(taker, U256::from(10_000_000u64));
    }

    #[test]
    fn parse_place_order_response_handles_nested_shapes() {
        let primary = json!({
            "success": true,
            "orderID": "ord-primary"
        });
        assert_eq!(
            parse_place_order_response(&primary).as_deref(),
            Some("ord-primary")
        );

        let nested = json!({
            "success": true,
            "data": {
                "orderId": "ord-nested"
            }
        });
        assert_eq!(
            parse_place_order_response(&nested).as_deref(),
            Some("ord-nested")
        );
    }

    #[test]
    fn test_parse_orderbook_sorts_asks_ascending() {
        let book_json = json!({
            "bids": [],
            "asks": [
                {"price": "0.99", "size": "100"},
                {"price": "0.55", "size": "200"},
                {"price": "0.52", "size": "50"},
            ]
        });
        let book = parse_orderbook("tok1", &book_json).unwrap();
        assert_eq!(book.best_ask(), Some(Decimal::new(52, 2)));
        assert_eq!(book.asks[0].price, Decimal::new(52, 2));
        assert_eq!(book.asks[1].price, Decimal::new(55, 2));
        assert_eq!(book.asks[2].price, Decimal::new(99, 2));
    }

    #[test]
    fn test_parse_orderbook_sorts_bids_descending() {
        let book_json = json!({
            "bids": [
                {"price": "0.40", "size": "100"},
                {"price": "0.48", "size": "200"},
                {"price": "0.53", "size": "50"},
            ],
            "asks": []
        });
        let book = parse_orderbook("tok1", &book_json).unwrap();
        assert_eq!(book.best_bid(), Some(Decimal::new(53, 2)));
        assert_eq!(book.bids[0].price, Decimal::new(53, 2));
        assert_eq!(book.bids[1].price, Decimal::new(48, 2));
        assert_eq!(book.bids[2].price, Decimal::new(40, 2));
    }

    #[test]
    fn test_parse_orderbook_handles_empty_sides() {
        let book_json = json!({
            "bids": [],
            "asks": []
        });
        let book = parse_orderbook("tok1", &book_json).unwrap();
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }
}
