use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::bayesian::BayesianUpdater;
use crate::edge::{self, EdgeResult, EdgeSide, FeeModel};
use crate::kelly::{BetSide, KellySizer, PositionSize};
use crate::market_scanner::TargetMarket;
use crate::price_feeds::{AggregatedPrice, PriceAsset};
use crate::risk::RiskManager;
use crate::venue::{OrderBook, Side};

const CONFIDENCE_PENALTY_MULT: f64 = 0.05;
const SPREAD_PENALTY_MULT: f64 = 1.5;
const SPREAD_PENALTY_CAP: f64 = 0.03;
const CALIBRATION_PENALTY_MULT: f64 = 0.03;
const MAX_EFFECTIVE_THRESHOLD: f64 = 0.25;

/// Result of evaluating a single market in the trading pipeline.
#[derive(Debug)]
pub struct PipelineResult {
    pub market: TargetMarket,
    pub edge_result: Option<EdgeResult>,
    pub position: Option<PositionSize>,
    pub diagnostics: PipelineDiagnostics,
    pub action: PipelineAction,
}

/// Human-readable diagnostics for each pipeline stage.
#[derive(Debug, Clone)]
pub struct PipelineDiagnostics {
    pub market_asset: &'static str,
    pub aggregated_spot: Option<f64>,
    pub confidence_score: f64,
    pub spread_ratio: Option<f64>,
    pub strike_price: f64,
    pub reference_price: Option<f64>,
    pub reference_source: Option<String>,
    pub expiry_time: DateTime<Utc>,
    pub time_to_expiry_secs: f64,
    pub p_model: Option<f64>,
    pub model_volatility: Option<f64>,
    pub ewma_variance: Option<f64>,
    pub ewma_observations: Option<usize>,
    pub ewma_warmed_up: Option<bool>,
    pub model_type: &'static str,
    pub market_implied_p: Option<f64>,
    pub posterior: Option<f64>,
    pub edge_side: Option<&'static str>,
    pub executable_price: Option<f64>,
    pub gross_edge: Option<f64>,
    pub cost_fee: Option<f64>,
    pub cost_slippage: Option<f64>,
    pub cost_latency: Option<f64>,
    pub cost_total: Option<f64>,
    pub net_edge: Option<f64>,
    pub edge_threshold: f64,
    pub threshold_pass: Option<bool>,
    pub kelly_raw: Option<f64>,
    pub kelly_adjusted: Option<f64>,
    pub kelly_final: Option<f64>,
    pub kelly_stake_usdc: Option<Decimal>,
    pub kelly_size_tokens: Option<Decimal>,
    pub fee_asymmetry_flip: Option<bool>,
    pub prob_pre_shrinkage: Option<f64>,
    pub prob_post_shrinkage: Option<f64>,
    pub prob_shrinkage_alpha: Option<f64>,
}

/// What action (if any) should be taken for this market.
#[derive(Debug)]
pub enum PipelineAction {
    /// No trade — not enough edge or blocked by risk
    NoTrade { reason: String },
    /// Place an order
    PlaceOrder {
        token_id: String,
        side: Side,
        price: Decimal,
        amount: Decimal,
    },
}

fn kelly_win_probability(posterior_yes: f64, side: BetSide) -> f64 {
    match side {
        BetSide::Yes => posterior_yes,
        BetSide::No => 1.0 - posterior_yes,
    }
}

fn compute_effective_threshold(
    base_threshold: f64,
    confidence: f64,
    spread_ratio: f64,
    model_uncertainty: f64,
) -> f64 {
    let confidence_penalty = (1.0 - confidence.clamp(0.0, 1.0)) * CONFIDENCE_PENALTY_MULT;
    let spread_penalty = (spread_ratio.max(0.0) * SPREAD_PENALTY_MULT).min(SPREAD_PENALTY_CAP);
    let calibration_penalty = model_uncertainty.clamp(0.0, 1.0) * CALIBRATION_PENALTY_MULT;

    (base_threshold + confidence_penalty + spread_penalty + calibration_penalty)
        .max(0.0)
        .min(MAX_EFFECTIVE_THRESHOLD)
}

fn populate_edge_diagnostics(diagnostics: &mut PipelineDiagnostics, edge_result: &EdgeResult) {
    diagnostics.edge_side = Some(match edge_result.side {
        EdgeSide::BuyYes => "BUY YES",
        EdgeSide::BuyNo => "BUY NO",
    });
    diagnostics.executable_price = Some(edge_result.executable_price);
    diagnostics.gross_edge = Some(edge_result.gross_edge);
    diagnostics.cost_fee = edge_result.trading_cost.taker_fee.to_f64();
    diagnostics.cost_slippage = edge_result.trading_cost.slippage.to_f64();
    diagnostics.cost_latency = edge_result.trading_cost.latency_penalty.to_f64();
    diagnostics.cost_total = edge_result.trading_cost.total.to_f64();
    diagnostics.net_edge = Some(edge_result.net_edge);
    diagnostics.fee_asymmetry_flip = Some(edge_result.fee_asymmetry_flip);
}

/// Evaluate a single market against current price data.
///
/// Pipeline:
/// 1. Bayesian update -> posterior probability
/// 2. Quality filters
/// 3. Preliminary edge with probe size
/// 4. Kelly sizing from preliminary edge (stake in USDC)
/// 5. Recompute edge with actual token size (size-dependent slippage)
/// 6. Dynamic threshold gate
/// 7. Risk check and order action
#[allow(clippy::too_many_arguments)]
pub fn evaluate_market(
    market: &TargetMarket,
    price: &AggregatedPrice,
    book: &OrderBook,
    bayesian: &mut BayesianUpdater,
    fee_model: &FeeModel,
    kelly: &KellySizer,
    risk: &RiskManager,
    current_exposure: Decimal,
    current_bankroll: Decimal,
    open_order_count: u32,
    model_uncertainty: f64,
    base_edge_threshold: f64,
    min_confidence: f64,
    max_spread_ratio: f64,
    min_liquidity: f64,
    min_time_to_expiry_secs: i64,
    max_net_edge: Option<f64>,
    yes_edge_threshold_mult: f64,
    no_edge_threshold_mult: f64,
    prob_shrinkage_alpha: Option<f64>,
) -> PipelineResult {
    let market_asset = PriceAsset::from_market_question(&market.question);
    let aggregated_spot = price.spot_usd.to_f64();
    let spread_ratio = match (price.spread.to_f64(), price.spot_usd.to_f64()) {
        (Some(spread), Some(spot)) if spot > 0.0 => Some((spread / spot).max(0.0)),
        _ => None,
    };

    let mut diagnostics = PipelineDiagnostics {
        market_asset: market_asset.map(|a| a.symbol()).unwrap_or("UNKNOWN"),
        aggregated_spot,
        confidence_score: price.confidence.to_f64().unwrap_or(0.0),
        spread_ratio,
        strike_price: market.strike_price.to_f64().unwrap_or(0.0),
        reference_price: None,
        reference_source: market.polymarket_price_to_beat_source.clone(),
        expiry_time: market.end_time,
        time_to_expiry_secs: (market.end_time - Utc::now()).num_seconds() as f64,
        p_model: None,
        model_volatility: None,
        ewma_variance: None,
        ewma_observations: None,
        ewma_warmed_up: None,
        model_type: if market.strike_price > Decimal::ZERO {
            "strike_gbm"
        } else {
            "updown_directional"
        },
        market_implied_p: None,
        posterior: None,
        edge_side: None,
        executable_price: None,
        gross_edge: None,
        cost_fee: None,
        cost_slippage: None,
        cost_latency: None,
        cost_total: None,
        net_edge: None,
        edge_threshold: base_edge_threshold,
        threshold_pass: None,
        kelly_raw: None,
        kelly_adjusted: None,
        kelly_final: None,
        kelly_stake_usdc: None,
        kelly_size_tokens: None,
        fee_asymmetry_flip: None,
        prob_pre_shrinkage: None,
        prob_post_shrinkage: None,
        prob_shrinkage_alpha: None,
    };

    if let Some(asset) = market_asset {
        if asset != price.asset {
            return PipelineResult {
                market: market.clone(),
                edge_result: None,
                position: None,
                diagnostics,
                action: PipelineAction::NoTrade {
                    reason: format!(
                        "asset mismatch: market={} aggregated_price={}",
                        asset, price.asset
                    ),
                },
            };
        }
    } else {
        return PipelineResult {
            market: market.clone(),
            edge_result: None,
            position: None,
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: "unsupported market asset in question text".into(),
            },
        };
    }

    // Step 1: Bayesian update
    let posterior_details = match bayesian.update_with_details(price, market) {
        Ok(d) => d,
        Err(e) => {
            return PipelineResult {
                market: market.clone(),
                edge_result: None,
                position: None,
                diagnostics,
                action: PipelineAction::NoTrade {
                    reason: format!("bayesian update failed: {}", e),
                },
            };
        }
    };
    diagnostics.p_model = Some(posterior_details.model_posterior);
    diagnostics.model_volatility = Some(posterior_details.volatility);
    diagnostics.model_type = posterior_details.model_type;
    diagnostics.reference_price = posterior_details.reference_spot;
    diagnostics.market_implied_p = Some(posterior_details.market_implied);
    diagnostics.posterior = Some(posterior_details.adjusted_posterior);
    diagnostics.time_to_expiry_secs = posterior_details.time_to_expiry_secs;
    diagnostics.ewma_variance = bayesian.ewma_variance();
    diagnostics.ewma_observations = Some(bayesian.ewma_total_observations());
    diagnostics.ewma_warmed_up = Some(bayesian.ewma_warmed_up());

    let confidence = diagnostics.confidence_score;
    let spread = diagnostics.spread_ratio.unwrap_or(f64::INFINITY);
    let time_to_expiry = (market.end_time - Utc::now()).num_seconds();
    let liquidity = market.liquidity.to_f64().unwrap_or(0.0);

    // Step 2: hard pre-filters before edge/Kelly.
    if confidence < min_confidence {
        return PipelineResult {
            market: market.clone(),
            edge_result: None,
            position: None,
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: format!(
                    "confidence {:.3} below minimum {:.3}",
                    confidence, min_confidence
                ),
            },
        };
    }
    if spread > max_spread_ratio {
        return PipelineResult {
            market: market.clone(),
            edge_result: None,
            position: None,
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: format!(
                    "spread ratio {:.4} above max {:.4}",
                    spread, max_spread_ratio
                ),
            },
        };
    }
    if liquidity < min_liquidity {
        return PipelineResult {
            market: market.clone(),
            edge_result: None,
            position: None,
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: format!(
                    "liquidity {:.2} below minimum {:.2}",
                    liquidity, min_liquidity
                ),
            },
        };
    }
    if time_to_expiry < min_time_to_expiry_secs {
        return PipelineResult {
            market: market.clone(),
            edge_result: None,
            position: None,
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: format!(
                    "time to expiry {}s below minimum {}s",
                    time_to_expiry, min_time_to_expiry_secs
                ),
            },
        };
    }

    let mut posterior = posterior_details.adjusted_posterior;

    // P9: Probability shrinkage toward market-implied price.
    if let Some(alpha) = prob_shrinkage_alpha {
        let pre = posterior;
        let market_p = posterior_details.market_implied;
        posterior = alpha * pre + (1.0 - alpha) * market_p;
        posterior = posterior.clamp(0.001, 0.999);
        diagnostics.prob_pre_shrinkage = Some(pre);
        diagnostics.prob_post_shrinkage = Some(posterior);
        diagnostics.prob_shrinkage_alpha = Some(alpha);
    }
    diagnostics.posterior = Some(posterior);

    let effective_threshold =
        compute_effective_threshold(base_edge_threshold, confidence, spread, model_uncertainty);
    diagnostics.edge_threshold = effective_threshold;

    // Step 3: preliminary edge on probe size.
    let probe_size = if book.min_order_size > Decimal::ZERO {
        book.min_order_size
    } else {
        Decimal::ONE
    };

    let preliminary_edge = match edge::calculate_edge(posterior, book, fee_model, probe_size) {
        Ok(e) => e,
        Err(e) => {
            return PipelineResult {
                market: market.clone(),
                edge_result: None,
                position: None,
                diagnostics,
                action: PipelineAction::NoTrade {
                    reason: format!("edge calculation failed: {}", e),
                },
            };
        }
    };
    populate_edge_diagnostics(&mut diagnostics, &preliminary_edge);
    diagnostics.threshold_pass = Some(preliminary_edge.net_edge >= effective_threshold);

    // Step 4: Kelly sizing (stake in USDC) based on preliminary edge.
    let prelim_bet_side = match preliminary_edge.side {
        EdgeSide::BuyYes => BetSide::Yes,
        EdgeSide::BuyNo => BetSide::No,
    };
    let prelim_kelly_win_prob = kelly_win_probability(posterior, prelim_bet_side);

    let position = match kelly.size(
        prelim_kelly_win_prob,
        preliminary_edge.executable_price,
        preliminary_edge.net_edge,
        current_bankroll,
        model_uncertainty,
        prelim_bet_side,
    ) {
        Some(p) => p,
        None => {
            let debug_net_edge = preliminary_edge.net_edge;
            let debug_executable_price = preliminary_edge.executable_price;
            return PipelineResult {
                market: market.clone(),
                edge_result: Some(preliminary_edge),
                position: None,
                diagnostics,
                action: PipelineAction::NoTrade {
                    reason: format!(
                        "kelly sizer returned no position (net_edge={:.4}, executable_price={:.4}, posterior={:.4}, uncertainty={:.4}, bankroll={})",
                        debug_net_edge,
                        debug_executable_price,
                        prelim_kelly_win_prob,
                        model_uncertainty,
                        current_bankroll.round_dp(2)
                    ),
                },
            };
        }
    };

    // Step 5: recompute edge with actual token size for slippage consistency.
    let size_tokens = position.size_tokens.max(Decimal::ZERO);
    if size_tokens <= Decimal::ZERO {
        return PipelineResult {
            market: market.clone(),
            edge_result: Some(preliminary_edge),
            position: None,
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: "kelly stake converted to zero token size".into(),
            },
        };
    }

    let edge_result = match edge::calculate_edge(posterior, book, fee_model, size_tokens) {
        Ok(e) => e,
        Err(e) => {
            return PipelineResult {
                market: market.clone(),
                edge_result: Some(preliminary_edge),
                position: Some(position),
                diagnostics,
                action: PipelineAction::NoTrade {
                    reason: format!("size-aware edge calculation failed: {}", e),
                },
            };
        }
    };

    if edge_result.side != preliminary_edge.side {
        populate_edge_diagnostics(&mut diagnostics, &edge_result);
        diagnostics.threshold_pass = Some(edge_result.net_edge >= effective_threshold);
        return PipelineResult {
            market: market.clone(),
            edge_result: Some(edge_result),
            position: Some(position),
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: "edge side changed after size-aware slippage recompute".into(),
            },
        };
    }

    populate_edge_diagnostics(&mut diagnostics, &edge_result);

    diagnostics.kelly_raw = Some(position.raw_kelly);
    diagnostics.kelly_adjusted = Some(position.adjusted_kelly);
    diagnostics.kelly_final = Some(position.final_kelly);
    diagnostics.kelly_stake_usdc = Some(position.stake_usdc);
    diagnostics.kelly_size_tokens = Some(position.size_tokens);

    // Step 6: threshold check using dynamic effective threshold.
    // Apply side-specific multiplier so each side can have its own edge bar.
    let (side_threshold, side_mult) = match edge_result.side {
        EdgeSide::BuyYes => (effective_threshold * yes_edge_threshold_mult, yes_edge_threshold_mult),
        EdgeSide::BuyNo => (effective_threshold * no_edge_threshold_mult, no_edge_threshold_mult),
    };
    diagnostics.edge_threshold = side_threshold;
    diagnostics.threshold_pass = Some(edge_result.net_edge >= side_threshold);
    if edge_result.net_edge < side_threshold {
        let net_edge = edge_result.net_edge;
        return PipelineResult {
            market: market.clone(),
            edge_result: Some(edge_result),
            position: Some(position),
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: format!(
                    "net edge {:.4} below effective threshold {:.4} (side_mult={:.2})",
                    net_edge, side_threshold, side_mult
                ),
            },
        };
    }

    // Max net edge cap — reject suspiciously large edges.
    if let Some(cap) = max_net_edge {
        if edge_result.net_edge > cap {
            let net_edge = edge_result.net_edge;
            return PipelineResult {
                market: market.clone(),
                edge_result: Some(edge_result),
                position: Some(position),
                diagnostics,
                action: PipelineAction::NoTrade {
                    reason: format!(
                        "net edge {:.4} above max cap {:.4}",
                        net_edge, cap
                    ),
                },
            };
        }
    }

    // Step 7: Risk check in USDC stake units.
    if let Err(e) = risk.check(
        position.stake_usdc,
        current_exposure,
        current_bankroll,
        open_order_count,
    ) {
        return PipelineResult {
            market: market.clone(),
            edge_result: Some(edge_result),
            position: Some(position),
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: format!("risk check failed: {}", e),
            },
        };
    }

    let capped_stake = crate::risk::limits::effective_max_position(
        position.stake_usdc,
        Decimal::from(2000),
        current_exposure,
        position.stake_usdc,
    );

    let executable_price =
        Decimal::try_from(edge_result.executable_price).unwrap_or(Decimal::new(50, 2));
    let capped_size_tokens = if executable_price > Decimal::ZERO {
        (capped_stake / executable_price).round_dp(4)
    } else {
        Decimal::ZERO
    };
    if capped_size_tokens <= Decimal::ZERO {
        return PipelineResult {
            market: market.clone(),
            edge_result: Some(edge_result),
            position: Some(position),
            diagnostics,
            action: PipelineAction::NoTrade {
                reason: "capped stake converted to zero token size".into(),
            },
        };
    }

    let token_id = match position.side {
        BetSide::Yes => &market.token_id_yes,
        BetSide::No => &market.token_id_no,
    };

    PipelineResult {
        market: market.clone(),
        edge_result: Some(edge_result),
        position: Some(position),
        diagnostics,
        action: PipelineAction::PlaceOrder {
            token_id: token_id.clone(),
            side: Side::Buy,
            price: executable_price,
            amount: capped_size_tokens,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::kelly_win_probability;
    use crate::kelly::BetSide;

    #[test]
    fn kelly_probability_uses_yes_posterior_for_yes_side() {
        let p_yes = 0.37;
        assert!((kelly_win_probability(p_yes, BetSide::Yes) - p_yes).abs() < 1e-12);
    }

    #[test]
    fn kelly_probability_inverts_for_no_side() {
        let p_yes = 0.37;
        assert!((kelly_win_probability(p_yes, BetSide::No) - 0.63).abs() < 1e-12);
    }
}
