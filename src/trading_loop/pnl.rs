use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::Value;

use crate::edge::FeeRegime;
use crate::market_scanner::TargetMarket;
use crate::price_feeds::PriceAsset;
use crate::venue::{MarketResolution, OrderBook, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeTokenSide {
    Yes,
    No,
}

#[derive(Debug, Clone)]
pub struct FillContext {
    pub token_id: String,
    pub asset: PriceAsset,
    pub market_question: String,
    pub side: Side,
    pub condition_id: Option<String>,
    pub end_time: Option<DateTime<Utc>>,
    pub window_start: Option<DateTime<Utc>>,
    pub strike_price: Option<Decimal>,
    pub reference_price: Option<Decimal>,
    pub official_price_to_beat: Option<Decimal>,
    pub model_type: Option<String>,
    pub predicted_yes_prob: Option<f64>,
    pub model_yes_prob: Option<f64>,
    pub outcome_side: Option<OutcomeTokenSide>,
}

#[derive(Debug, Clone)]
pub struct PositionState {
    pub token_id: String,
    pub condition_id: Option<String>,
    pub asset: PriceAsset,
    pub market_question: String,
    pub quantity: Decimal,
    pub avg_entry_price: Decimal,
    pub last_mark_price: Decimal,
    pub end_time: Option<DateTime<Utc>>,
    pub window_start: Option<DateTime<Utc>>,
    pub strike_price: Decimal,
    pub reference_price: Option<Decimal>,
    pub official_price_to_beat: Option<Decimal>,
    pub model_type: String,
    pub predicted_yes_prob_avg: f64,
    pub model_yes_prob_avg: f64,
    pub opened_at: DateTime<Utc>,
    pub entry_side: Side,
    pub outcome_side: OutcomeTokenSide,
}

#[derive(Debug, Clone)]
pub struct ClosedTradeRecord {
    pub opened_at: DateTime<Utc>,
    pub closed_at: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub window_start: Option<DateTime<Utc>>,
    pub asset: PriceAsset,
    pub token_id: String,
    pub condition_id: Option<String>,
    pub market_question: String,
    pub outcome_side: OutcomeTokenSide,
    pub avg_entry_price: Decimal,
    pub quantity: Decimal,
    pub predicted_yes_prob: f64,
    pub model_yes_prob: f64,
    pub actual_yes_outcome: bool,
    pub payout_price: Decimal,
    pub realized_pnl: Decimal,
    pub model_type: String,
    pub official_price_to_beat: Option<Decimal>,
    pub official_final_price: Option<Decimal>,
    pub settlement_source: String,
    pub resolution_available_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct AssetPnlRow {
    pub asset: PriceAsset,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub open_positions: usize,
}

#[derive(Debug, Clone)]
pub struct PnlSnapshot {
    pub starting_capital: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub equity: Decimal,
    pub roi_pct: Decimal,
    pub total_fees_paid: Decimal,
    pub open_positions: usize,
    pub fills: u64,
    pub trades: u64,
    pub closed_trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub win_rate: Decimal,
    pub per_asset: Vec<AssetPnlRow>,
}

#[derive(Debug, Clone, Default)]
struct AssetAccumulator {
    realized: Decimal,
    unrealized: Decimal,
    open_positions: usize,
}

/// Session-scoped P&L tracker.
/// Tracks long-only outcome inventory with fill-based accounting,
/// mark-to-market updates, and paper-mode expiry settlement.
pub struct PnlTracker {
    starting_capital: Decimal,
    realized_pnl: Decimal,
    total_fees_paid: Decimal,
    fee_regime: FeeRegime,
    positions: HashMap<String, PositionState>,
    order_context: HashMap<String, FillContext>,
    settled_condition_ids: HashSet<String>,
    realized_by_asset: HashMap<PriceAsset, Decimal>,
    fills: u64,
    trades: u64,
    closed_trades: u64,
    wins: u64,
    losses: u64,
}

impl PnlTracker {
    pub fn new(starting_capital: Decimal, fee_regime: FeeRegime) -> Self {
        Self {
            starting_capital,
            realized_pnl: Decimal::ZERO,
            total_fees_paid: Decimal::ZERO,
            fee_regime,
            positions: HashMap::new(),
            order_context: HashMap::new(),
            settled_condition_ids: HashSet::new(),
            realized_by_asset: HashMap::new(),
            fills: 0,
            trades: 0,
            closed_trades: 0,
            wins: 0,
            losses: 0,
        }
    }

    pub fn register_order_context(&mut self, order_id: String, context: FillContext) {
        let is_new = self.order_context.insert(order_id, context).is_none();
        if is_new {
            self.trades += 1;
        }
    }

    pub fn clear_order_context(&mut self, order_id: &str) {
        self.order_context.remove(order_id);
    }

    /// Paper/live exposure equivalent: open notional on currently held positions.
    pub fn deployed_capital(&self) -> Decimal {
        self.positions
            .values()
            .filter(|pos| pos.quantity > Decimal::ZERO)
            .map(|pos| pos.avg_entry_price * pos.quantity)
            .sum()
    }

    /// Conservative sizing bankroll:
    /// starting + realized - currently deployed (ignores unrealized P&L).
    pub fn available_capital(&self) -> Decimal {
        (self.starting_capital + self.realized_pnl - self.deployed_capital()).max(Decimal::ZERO)
    }

    pub fn open_position_count(&self) -> u32 {
        self.positions
            .values()
            .filter(|pos| pos.quantity > Decimal::ZERO)
            .count() as u32
    }

    /// Condition-level dedup guard.
    /// Returns true when condition is currently open, pending, or already settled this session.
    pub fn has_active_or_settled_position(&self, condition_id: &str) -> bool {
        let Some(cond) = normalize_condition_id(condition_id) else {
            return false;
        };

        if self.settled_condition_ids.contains(cond.as_str()) {
            return true;
        }

        let open_position_exists = self.positions.values().any(|pos| {
            pos.quantity > Decimal::ZERO
                && pos
                    .condition_id
                    .as_deref()
                    .and_then(normalize_condition_id)
                    .is_some_and(|c| c == cond)
        });
        if open_position_exists {
            return true;
        }

        self.order_context.values().any(|ctx| {
            ctx.condition_id
                .as_deref()
                .and_then(normalize_condition_id)
                .is_some_and(|c| c == cond)
        })
    }

    pub fn apply_live_fill(
        &mut self,
        order_id: &str,
        fill_price: Decimal,
        fill_amount: Decimal,
    ) -> bool {
        let Some(context) = self.order_context.get(order_id).cloned() else {
            return false;
        };
        self.apply_fill_with_context(&context, fill_price, fill_amount);
        true
    }

    pub fn apply_simulated_fill(
        &mut self,
        context: FillContext,
        fill_price: Decimal,
        fill_amount: Decimal,
    ) {
        self.trades += 1;
        self.apply_fill_with_context(&context, fill_price, fill_amount);
    }

    pub fn update_market_marks(
        &mut self,
        market: &TargetMarket,
        yes_book: &OrderBook,
        no_book: Option<&OrderBook>,
    ) {
        let Some(yes_mark) = derive_yes_mark(yes_book) else {
            return;
        };

        if let Some(yes_pos) = self.positions.get_mut(&market.token_id_yes) {
            yes_pos.last_mark_price = yes_mark;
        }
        if let Some(no_pos) = self.positions.get_mut(&market.token_id_no) {
            // Prefer the NO token's own order book midpoint for accuracy —
            // YES and NO books can have different spreads on Polymarket.
            let no_mark = no_book
                .and_then(|b| b.midpoint().or_else(|| b.best_bid()))
                .map(clamp_probability)
                .unwrap_or_else(|| clamp_probability(Decimal::ONE - yes_mark));
            no_pos.last_mark_price = no_mark;
        }
    }

    /// Returns true if there is an active (quantity > 0) NO-side position for this market.
    pub fn has_active_no_position(&self, market: &TargetMarket) -> bool {
        self.positions
            .get(&market.token_id_no)
            .map(|p| p.quantity > Decimal::ZERO)
            .unwrap_or(false)
    }

    /// Look up the asset and window_start for a position by condition_id.
    /// Used for post-hoc reference price validation at settlement.
    pub fn asset_and_window(&self, condition_id: &str) -> Option<(PriceAsset, DateTime<Utc>)> {
        let cond = normalize_condition_id(condition_id)?;
        self.positions.values().find_map(|pos| {
            let pos_cond = pos.condition_id.as_deref().and_then(normalize_condition_id)?;
            if pos_cond == cond {
                Some((pos.asset, pos.window_start?))
            } else {
                None
            }
        })
    }

    /// Look up the reference (open) price stored on a position by condition_id.
    /// Prefers `official_price_to_beat`, falls back to `reference_price`.
    pub fn position_reference_price(&self, condition_id: &str) -> Option<Decimal> {
        let cond = normalize_condition_id(condition_id)?;
        self.positions.values().find_map(|pos| {
            let pos_cond = pos.condition_id.as_deref().and_then(normalize_condition_id)?;
            if pos_cond == cond {
                pos.official_price_to_beat.or(pos.reference_price)
            } else {
                None
            }
        })
    }

    /// Returns the set of normalized condition IDs that have open (unsettled) positions.
    pub fn open_condition_ids(&self) -> HashSet<String> {
        self.positions
            .values()
            .filter(|p| p.quantity > Decimal::ZERO)
            .filter_map(|p| {
                p.condition_id
                    .as_deref()
                    .and_then(normalize_condition_id)
            })
            .collect()
    }

    pub fn due_condition_ids(&self, now: DateTime<Utc>, grace_secs: i64) -> Vec<String> {
        let mut ids = HashSet::new();
        for position in self.positions.values() {
            let Some(end_time) = position.end_time else {
                continue;
            };
            if now < end_time + chrono::Duration::seconds(grace_secs) {
                continue;
            }
            let Some(condition_id) = position
                .condition_id
                .as_deref()
                .and_then(normalize_condition_id)
            else {
                continue;
            };
            ids.insert(condition_id);
        }

        let mut out: Vec<String> = ids.into_iter().collect();
        out.sort_unstable();
        out
    }

    pub fn settle_expired_positions(
        &mut self,
        now: DateTime<Utc>,
        grace_secs: i64,
        resolutions_by_condition: &HashMap<String, MarketResolution>,
    ) -> Vec<ClosedTradeRecord> {
        let mut closed = Vec::new();
        let keys: Vec<String> = self.positions.keys().cloned().collect();

        for token_id in keys {
            let should_settle = self
                .positions
                .get(&token_id)
                .and_then(|p| {
                    p.end_time
                        .map(|t| t + chrono::Duration::seconds(grace_secs))
                })
                .is_some_and(|settle_at| now >= settle_at);
            if !should_settle {
                continue;
            }

            let Some(position) = self.positions.get(&token_id).cloned() else {
                continue;
            };

            let Some(condition_id) = position
                .condition_id
                .as_deref()
                .and_then(normalize_condition_id)
            else {
                continue;
            };

            let Some(resolution) = resolutions_by_condition.get(condition_id.as_str()) else {
                continue;
            };
            let Some(actual_yes) = resolution.resolved_yes_outcome else {
                continue;
            };

            let position_won = match position.outcome_side {
                OutcomeTokenSide::Yes => actual_yes,
                OutcomeTokenSide::No => !actual_yes,
            };
            let payout = if position_won {
                Decimal::ONE
            } else {
                Decimal::ZERO
            };
            let realized = (payout - position.avg_entry_price) * position.quantity;

            self.realized_pnl += realized;
            *self
                .realized_by_asset
                .entry(position.asset)
                .or_insert(Decimal::ZERO) += realized;

            self.closed_trades += 1;
            if realized > Decimal::ZERO {
                self.wins += 1;
            } else {
                self.losses += 1;
            }

            self.positions.remove(&token_id);
            self.settled_condition_ids.insert(condition_id);

            closed.push(ClosedTradeRecord {
                opened_at: position.opened_at,
                closed_at: now,
                end_time: position.end_time,
                window_start: position.window_start,
                asset: position.asset,
                token_id: position.token_id,
                condition_id: position.condition_id,
                market_question: position.market_question,
                outcome_side: position.outcome_side,
                avg_entry_price: position.avg_entry_price,
                quantity: position.quantity,
                predicted_yes_prob: position.predicted_yes_prob_avg,
                model_yes_prob: position.model_yes_prob_avg,
                actual_yes_outcome: actual_yes,
                payout_price: payout,
                realized_pnl: realized,
                model_type: position.model_type,
                official_price_to_beat: resolution
                    .official_price_to_beat
                    .or(position.official_price_to_beat),
                official_final_price: resolution.official_final_price,
                settlement_source: resolution.resolution_source.clone(),
                resolution_available_at: resolution.resolved_at,
            });
        }

        closed
    }

    pub fn snapshot(&self) -> PnlSnapshot {
        let mut by_asset: HashMap<PriceAsset, AssetAccumulator> = PriceAsset::ALL
            .into_iter()
            .map(|asset| (asset, AssetAccumulator::default()))
            .collect();

        for (asset, realized) in &self.realized_by_asset {
            if let Some(acc) = by_asset.get_mut(asset) {
                acc.realized = *realized;
            }
        }

        let mut open_positions = 0usize;
        let mut unrealized_total = Decimal::ZERO;
        for pos in self.positions.values() {
            if pos.quantity <= Decimal::ZERO {
                continue;
            }
            let unrealized = position_unrealized_pnl(pos);
            unrealized_total += unrealized;
            open_positions += 1;
            if let Some(acc) = by_asset.get_mut(&pos.asset) {
                acc.unrealized += unrealized;
                acc.open_positions += 1;
            }
        }

        let total_pnl = self.realized_pnl + unrealized_total;
        let equity = self.starting_capital + total_pnl;
        let roi_pct = if self.starting_capital > Decimal::ZERO {
            (total_pnl / self.starting_capital) * Decimal::from(100)
        } else {
            Decimal::ZERO
        };
        let win_rate = if self.closed_trades > 0 {
            (Decimal::from(self.wins) / Decimal::from(self.closed_trades)) * Decimal::from(100)
        } else {
            Decimal::ZERO
        };

        let per_asset = PriceAsset::ALL
            .iter()
            .map(|asset| {
                let acc = by_asset.get(asset).cloned().unwrap_or_default();
                AssetPnlRow {
                    asset: *asset,
                    realized_pnl: acc.realized,
                    unrealized_pnl: acc.unrealized,
                    total_pnl: acc.realized + acc.unrealized,
                    open_positions: acc.open_positions,
                }
            })
            .collect();

        PnlSnapshot {
            starting_capital: self.starting_capital,
            realized_pnl: self.realized_pnl,
            unrealized_pnl: unrealized_total,
            total_pnl,
            equity,
            roi_pct,
            total_fees_paid: self.total_fees_paid,
            open_positions,
            fills: self.fills,
            trades: self.trades,
            closed_trades: self.closed_trades,
            wins: self.wins,
            losses: self.losses,
            win_rate,
            per_asset,
        }
    }

    pub fn open_position_cards(&self) -> Vec<Value> {
        let now = Utc::now();
        self.positions
            .values()
            .filter(|p| p.quantity > Decimal::ZERO)
            .map(|p| {
                let unrealized = position_unrealized_pnl(p);
                let cost_basis = p.avg_entry_price * p.quantity;
                let roi_pct = if cost_basis > Decimal::ZERO {
                    (unrealized / cost_basis * Decimal::from(100))
                        .to_f64()
                        .unwrap_or(0.0)
                } else {
                    0.0
                };
                let timeframe = match (p.window_start, p.end_time) {
                    (Some(s), Some(e)) => {
                        let secs = (e - s).num_seconds();
                        if (secs - 300).abs() <= 30 {
                            "5min".to_string()
                        } else if (secs - 900).abs() <= 30 {
                            "15min".to_string()
                        } else {
                            format!("{}min", secs / 60)
                        }
                    }
                    _ => "unknown".to_string(),
                };
                let pending_settlement = p.end_time.map(|t| now >= t).unwrap_or(false);
                serde_json::json!({
                    "token_id": p.token_id,
                    "condition_id": p.condition_id,
                    "asset": p.asset.symbol(),
                    "question": p.market_question,
                    "timeframe": timeframe,
                    "entry_price": p.avg_entry_price.to_f64(),
                    "current_price": p.last_mark_price.to_f64(),
                    "quantity": p.quantity.to_f64(),
                    "price_to_beat": p.official_price_to_beat
                        .or(p.reference_price)
                        .and_then(|v| v.to_f64()),
                    "unrealized_pnl": unrealized.to_f64(),
                    "unrealized_roi_pct": roi_pct,
                    "opened_at": p.opened_at.to_rfc3339(),
                    "end_time": p.end_time.map(|t| t.to_rfc3339()),
                    "side": match p.outcome_side {
                        OutcomeTokenSide::Yes => "YES",
                        OutcomeTokenSide::No  => "NO",
                    },
                    "model_type": p.model_type,
                    "predicted_prob": p.predicted_yes_prob_avg,
                    "pending_settlement": pending_settlement,
                })
            })
            .collect()
    }

    fn apply_fill_with_context(
        &mut self,
        context: &FillContext,
        fill_price: Decimal,
        fill_amount: Decimal,
    ) {
        if fill_amount <= Decimal::ZERO || fill_price <= Decimal::ZERO {
            return;
        }

        match context.side {
            Side::Buy => {
                let fee_per_share = self.fee_regime.fee_per_share(fill_price);
                let fee = fill_amount * fee_per_share;
                let now = Utc::now();
                let outcome_side = context.outcome_side.unwrap_or(OutcomeTokenSide::Yes);
                let mut predicted_prob = context
                    .predicted_yes_prob
                    .unwrap_or(0.5)
                    .clamp(0.001, 0.999);
                let mut model_prob = context
                    .model_yes_prob
                    .unwrap_or(predicted_prob)
                    .clamp(0.001, 0.999);

                let pos = self
                    .positions
                    .entry(context.token_id.clone())
                    .or_insert_with(|| PositionState {
                        token_id: context.token_id.clone(),
                        condition_id: context.condition_id.clone(),
                        asset: context.asset,
                        market_question: context.market_question.clone(),
                        quantity: Decimal::ZERO,
                        avg_entry_price: fill_price,
                        last_mark_price: fill_price,
                        end_time: context.end_time,
                        window_start: context.window_start,
                        strike_price: context.strike_price.unwrap_or(Decimal::ZERO),
                        reference_price: context.reference_price,
                        official_price_to_beat: context.official_price_to_beat,
                        model_type: context
                            .model_type
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                        predicted_yes_prob_avg: predicted_prob,
                        model_yes_prob_avg: model_prob,
                        opened_at: now,
                        entry_side: context.side,
                        outcome_side,
                    });

                if context.condition_id.is_some() {
                    pos.condition_id = context.condition_id.clone();
                }
                if context.end_time.is_some() {
                    pos.end_time = context.end_time;
                }
                if context.strike_price.is_some() {
                    pos.strike_price = context.strike_price.unwrap_or(Decimal::ZERO);
                }
                if context.reference_price.is_some() {
                    pos.reference_price = context.reference_price;
                }
                if context.official_price_to_beat.is_some() {
                    pos.official_price_to_beat = context.official_price_to_beat;
                }
                if let Some(model_type) = &context.model_type {
                    pos.model_type = model_type.clone();
                }
                if context.outcome_side.is_some() {
                    pos.outcome_side = outcome_side;
                }

                let prev_qty = pos.quantity.max(Decimal::ZERO);
                let prev_qty_f64 = prev_qty.to_f64().unwrap_or(0.0);
                let fill_qty_f64 = fill_amount.to_f64().unwrap_or(0.0);
                let new_qty = prev_qty + fill_amount;
                let new_cost = (pos.avg_entry_price * prev_qty) + (fill_price * fill_amount) + fee;

                if context.predicted_yes_prob.is_none() {
                    predicted_prob = pos.predicted_yes_prob_avg;
                }
                if context.model_yes_prob.is_none() {
                    model_prob = pos.model_yes_prob_avg;
                }
                let weighted_prob = if (prev_qty_f64 + fill_qty_f64) > 0.0 {
                    ((pos.predicted_yes_prob_avg * prev_qty_f64) + (predicted_prob * fill_qty_f64))
                        / (prev_qty_f64 + fill_qty_f64)
                } else {
                    pos.predicted_yes_prob_avg
                };
                let weighted_model_prob = if (prev_qty_f64 + fill_qty_f64) > 0.0 {
                    ((pos.model_yes_prob_avg * prev_qty_f64) + (model_prob * fill_qty_f64))
                        / (prev_qty_f64 + fill_qty_f64)
                } else {
                    pos.model_yes_prob_avg
                };

                pos.quantity = new_qty;
                pos.avg_entry_price = if new_qty > Decimal::ZERO {
                    new_cost / new_qty
                } else {
                    fill_price
                };
                pos.last_mark_price = fill_price;
                pos.predicted_yes_prob_avg = weighted_prob.clamp(0.001, 0.999);
                pos.model_yes_prob_avg = weighted_model_prob.clamp(0.001, 0.999);
                self.total_fees_paid += fee;
            }
            Side::Sell => {
                let Some(pos) = self.positions.get_mut(&context.token_id) else {
                    return;
                };
                let close_qty = fill_amount.min(pos.quantity.max(Decimal::ZERO));
                if close_qty <= Decimal::ZERO {
                    return;
                }

                let fee_per_share = self.fee_regime.fee_per_share(fill_price);
                let fee = close_qty * fee_per_share;
                let realized = (fill_price - pos.avg_entry_price) * close_qty - fee;
                self.realized_pnl += realized;
                *self
                    .realized_by_asset
                    .entry(context.asset)
                    .or_insert(Decimal::ZERO) += realized;
                self.total_fees_paid += fee;
                pos.quantity -= close_qty;
                pos.last_mark_price = fill_price;
                if pos.quantity <= Decimal::ZERO {
                    self.positions.remove(&context.token_id);
                    self.closed_trades += 1;
                    if realized > Decimal::ZERO {
                        self.wins += 1;
                    } else {
                        self.losses += 1;
                    }
                }
            }
        }

        self.fills += 1;
    }
}

fn clamp_probability(value: Decimal) -> Decimal {
    value.max(Decimal::ZERO).min(Decimal::ONE)
}

fn derive_yes_mark(book: &OrderBook) -> Option<Decimal> {
    // Only use midpoint or best_bid.  best_ask alone is unreliable — after a
    // market resolves to NO winning, the YES book keeps stale high asks while
    // bids vanish, inflating the mark and corrupting unrealized PnL.
    book.midpoint()
        .or_else(|| book.best_bid())
        .map(clamp_probability)
}

fn position_unrealized_pnl(pos: &PositionState) -> Decimal {
    if pos.quantity <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (pos.last_mark_price - pos.avg_entry_price) * pos.quantity
}

fn normalize_condition_id(condition_id: &str) -> Option<String> {
    let trimmed = condition_id.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    use crate::edge::FeeRegime;
    use crate::market_scanner::TargetMarket;
    use crate::venue::OrderBookLevel;

    fn ctx(token_id: &str, asset: PriceAsset, outcome_side: OutcomeTokenSide) -> FillContext {
        FillContext {
            token_id: token_id.to_string(),
            asset,
            market_question: format!("{} test market", asset.symbol()),
            side: Side::Buy,
            condition_id: Some("cond-1".to_string()),
            end_time: Some(Utc::now() + chrono::Duration::seconds(300)),
            window_start: None,
            strike_price: Some(Decimal::new(100, 0)),
            reference_price: None,
            official_price_to_beat: Some(Decimal::new(100, 0)),
            model_type: Some("strike_gbm".to_string()),
            predicted_yes_prob: Some(0.62),
            model_yes_prob: Some(0.60),
            outcome_side: Some(outcome_side),
        }
    }

    fn updown_ctx(
        token_id: &str,
        asset: PriceAsset,
        outcome_side: OutcomeTokenSide,
    ) -> FillContext {
        FillContext {
            token_id: token_id.to_string(),
            asset,
            market_question: format!("{} up/down", asset.symbol()),
            side: Side::Buy,
            condition_id: Some("cond-up".to_string()),
            end_time: Some(Utc::now() + chrono::Duration::seconds(300)),
            window_start: None,
            strike_price: Some(Decimal::ZERO),
            reference_price: Some(Decimal::new(1500, 0)),
            official_price_to_beat: Some(Decimal::new(1500, 0)),
            model_type: Some("updown_directional".to_string()),
            predicted_yes_prob: Some(0.58),
            model_yes_prob: Some(0.55),
            outcome_side: Some(outcome_side),
        }
    }

    fn resolution(
        condition_id: &str,
        resolved_yes_outcome: Option<bool>,
        official_final_price: Option<Decimal>,
    ) -> MarketResolution {
        MarketResolution {
            condition_id: condition_id.to_string(),
            resolved_yes_outcome,
            official_final_price,
            official_price_to_beat: Some(Decimal::new(100, 0)),
            resolved_at: Some(Utc::now()),
            resolution_source: "test".to_string(),
        }
    }

    fn make_market(token_yes: &str, token_no: &str) -> TargetMarket {
        TargetMarket {
            condition_id: "cond".into(),
            token_id_yes: token_yes.into(),
            token_id_no: token_no.into(),
            question: "Will BTC be up?".into(),
            strike_price: Decimal::ZERO,
            start_time: None,
            end_time: Utc::now(),
            current_yes_price: Decimal::new(50, 2),
            current_no_price: Decimal::new(50, 2),
            polymarket_price_to_beat: None,
            polymarket_price_to_beat_source: None,
            liquidity: Decimal::ZERO,
            volume: Decimal::ZERO,
        }
    }

    fn make_book(best_bid: Option<Decimal>, best_ask: Option<Decimal>) -> OrderBook {
        OrderBook {
            token_id: "yes-token".into(),
            bids: best_bid
                .map(|p| {
                    vec![OrderBookLevel {
                        price: p,
                        size: Decimal::from(100),
                    }]
                })
                .unwrap_or_default(),
            asks: best_ask
                .map(|p| {
                    vec![OrderBookLevel {
                        price: p,
                        size: Decimal::from(100),
                    }]
                })
                .unwrap_or_default(),
            tick_size: Decimal::new(1, 2),
            min_order_size: Decimal::ONE,
            neg_risk: false,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn simulated_buy_fill_updates_position_and_fees() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::new(2, 2) }); // 2%
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes),
            Decimal::new(50, 2),
            Decimal::from(100),
        );

        let pos = tracker.positions.get("yes-token").unwrap();
        assert_eq!(pos.quantity, Decimal::from(100));
        // fee = 100 × 0.02 = 2.0; avg_entry = (0.50 × 100 + 2.0) / 100 = 0.52
        assert_eq!(pos.avg_entry_price.round_dp(4), Decimal::new(5200, 4)); // 0.5200
        assert_eq!(tracker.total_fees_paid.round_dp(2), Decimal::new(2, 0));
        assert_eq!(tracker.trades, 1);
        assert_eq!(tracker.fills, 1);
    }

    #[test]
    fn weighted_average_entry_with_multiple_fills() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::new(2, 2) }); // 2%
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Eth, OutcomeTokenSide::Yes),
            Decimal::new(50, 2),
            Decimal::from(100),
        );
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Eth, OutcomeTokenSide::Yes),
            Decimal::new(60, 2),
            Decimal::from(100),
        );

        let pos = tracker.positions.get("yes-token").unwrap();
        assert_eq!(pos.quantity, Decimal::from(200));
        // fee1 = 100 × 0.02 = 2.0; fee2 = 100 × 0.02 = 2.0
        // total_cost = (50 + 2) + (60 + 2) = 114; avg = 114/200 = 0.5700
        assert_eq!(pos.avg_entry_price.round_dp(4), Decimal::new(5700, 4)); // 0.5700
    }

    #[test]
    fn yes_no_mark_propagation_updates_both_tokens() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::new(2, 2) });
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Xrp, OutcomeTokenSide::Yes),
            Decimal::new(45, 2),
            Decimal::from(10),
        );
        tracker.apply_simulated_fill(
            ctx("no-token", PriceAsset::Xrp, OutcomeTokenSide::No),
            Decimal::new(55, 2),
            Decimal::from(10),
        );

        let market = make_market("yes-token", "no-token");
        let book = make_book(Some(Decimal::new(48, 2)), Some(Decimal::new(52, 2))); // midpoint 0.50
        tracker.update_market_marks(&market, &book, None);

        assert_eq!(
            tracker.positions.get("yes-token").unwrap().last_mark_price,
            Decimal::new(50, 2)
        );
        assert_eq!(
            tracker.positions.get("no-token").unwrap().last_mark_price,
            Decimal::new(50, 2)
        );
    }

    #[test]
    fn unrealized_pnl_positive_and_negative() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::new(2, 2) });
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Sol, OutcomeTokenSide::Yes),
            Decimal::new(40, 2),
            Decimal::from(100),
        );
        let market = make_market("yes-token", "no-token");

        let up_book = make_book(Some(Decimal::new(59, 2)), Some(Decimal::new(61, 2))); // midpoint 0.60
        tracker.update_market_marks(&market, &up_book, None);
        let up_snap = tracker.snapshot();
        assert!(up_snap.unrealized_pnl > Decimal::ZERO);

        let down_book = make_book(Some(Decimal::new(19, 2)), Some(Decimal::new(21, 2))); // midpoint 0.20
        tracker.update_market_marks(&market, &down_book, None);
        let down_snap = tracker.snapshot();
        assert!(down_snap.unrealized_pnl < Decimal::ZERO);
    }

    #[test]
    fn settlement_yes_win_realizes_and_removes_position() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        let mut c = ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes);
        c.end_time = Some(Utc::now() - chrono::Duration::seconds(20));
        c.strike_price = Some(Decimal::new(100, 0));
        tracker.apply_simulated_fill(c, Decimal::new(50, 2), Decimal::from(10));

        let mut resolutions = HashMap::new();
        resolutions.insert("cond-1".to_string(), resolution("cond-1", Some(true), None));

        let closed = tracker.settle_expired_positions(Utc::now(), 10, &resolutions);
        assert_eq!(closed.len(), 1);
        assert_eq!(tracker.positions.len(), 0);
        assert_eq!(tracker.closed_trades, 1);
        assert_eq!(tracker.wins, 1);
        assert_eq!(tracker.losses, 0);
        assert!(tracker.realized_pnl > Decimal::ZERO);
    }

    #[test]
    fn settlement_no_win_realizes_and_removes_position() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        let mut c = updown_ctx("no-token", PriceAsset::Eth, OutcomeTokenSide::No);
        c.end_time = Some(Utc::now() - chrono::Duration::seconds(20));
        c.reference_price = Some(Decimal::new(2000, 0));
        tracker.apply_simulated_fill(c, Decimal::new(40, 2), Decimal::from(10));

        let mut resolutions = HashMap::new();
        resolutions.insert(
            "cond-up".to_string(),
            resolution("cond-up", Some(false), None),
        );

        let closed = tracker.settle_expired_positions(Utc::now(), 10, &resolutions);
        assert_eq!(closed.len(), 1);
        assert_eq!(tracker.positions.len(), 0);
        assert_eq!(tracker.closed_trades, 1);
        assert_eq!(tracker.wins, 1);
        assert_eq!(tracker.losses, 0);
        assert!(tracker.realized_pnl > Decimal::ZERO);
    }

    #[test]
    fn settlement_waits_for_grace_and_official_resolution() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        let end = Utc::now() - chrono::Duration::seconds(5);
        let mut c = ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes);
        c.end_time = Some(end);
        tracker.apply_simulated_fill(c, Decimal::new(50, 2), Decimal::from(10));

        let mut resolutions = HashMap::new();
        resolutions.insert("cond-1".to_string(), resolution("cond-1", Some(true), None));

        // not enough grace elapsed
        let closed = tracker.settle_expired_positions(Utc::now(), 10, &resolutions);
        assert!(closed.is_empty());

        // enough grace and official resolution available
        let closed = tracker.settle_expired_positions(
            Utc::now() + chrono::Duration::seconds(10),
            10,
            &resolutions,
        );
        assert_eq!(closed.len(), 1);
    }

    #[test]
    fn settlement_does_not_close_without_official_outcome() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        let now = Utc::now();
        let end = now - chrono::Duration::seconds(30);
        let mut c = ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes);
        c.end_time = Some(end);
        tracker.apply_simulated_fill(c, Decimal::new(50, 2), Decimal::from(10));

        let mut resolutions = HashMap::new();
        resolutions.insert("cond-1".to_string(), resolution("cond-1", None, None));

        let closed = tracker.settle_expired_positions(now, 10, &resolutions);
        assert!(closed.is_empty());
        assert_eq!(tracker.positions.len(), 1);
    }

    #[test]
    fn settlement_allows_missing_numeric_final_price_when_outcome_exists() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        let now = Utc::now();
        let end = now - chrono::Duration::seconds(30);
        let mut c = ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes);
        c.end_time = Some(end);
        tracker.apply_simulated_fill(c, Decimal::new(50, 2), Decimal::from(10));

        let mut resolutions = HashMap::new();
        resolutions.insert("cond-1".to_string(), resolution("cond-1", Some(true), None));

        let closed = tracker.settle_expired_positions(now, 10, &resolutions);
        assert_eq!(closed.len(), 1);
        assert_eq!(tracker.positions.len(), 0);
        assert_eq!(tracker.closed_trades, 1);
        assert!(closed[0].official_final_price.is_none());
    }

    #[test]
    fn snapshot_win_rate_uses_closed_trades() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });

        let mut win_ctx = ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes);
        win_ctx.end_time = Some(Utc::now() - chrono::Duration::seconds(20));
        win_ctx.strike_price = Some(Decimal::new(100, 0));
        tracker.apply_simulated_fill(win_ctx, Decimal::new(50, 2), Decimal::from(10));

        let mut lose_ctx = ctx("yes-token-2", PriceAsset::Btc, OutcomeTokenSide::Yes);
        lose_ctx.end_time = Some(Utc::now() - chrono::Duration::seconds(20));
        lose_ctx.strike_price = Some(Decimal::new(130, 0));
        lose_ctx.condition_id = Some("cond-2".to_string());
        tracker.apply_simulated_fill(lose_ctx, Decimal::new(90, 2), Decimal::from(10));

        let mut resolutions = HashMap::new();
        resolutions.insert("cond-1".to_string(), resolution("cond-1", Some(true), None));
        resolutions.insert(
            "cond-2".to_string(),
            resolution("cond-2", Some(false), None),
        );
        let _ = tracker.settle_expired_positions(Utc::now(), 10, &resolutions);

        let snap = tracker.snapshot();
        assert_eq!(snap.closed_trades, 2);
        assert_eq!(snap.wins, 1);
        assert_eq!(snap.losses, 1);
        assert_eq!(snap.win_rate.round_dp(2), Decimal::new(5000, 2));
    }

    #[test]
    fn has_active_position_blocks_duplicate_entry() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes),
            Decimal::new(50, 2),
            Decimal::from(10),
        );

        assert!(tracker.has_active_or_settled_position("cond-1"));
        assert!(!tracker.has_active_or_settled_position("cond-999"));
    }

    #[test]
    fn available_capital_decreases_after_fill() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Eth, OutcomeTokenSide::Yes),
            Decimal::new(50, 2),
            Decimal::from(100),
        );

        assert_eq!(tracker.deployed_capital(), Decimal::from(50));
        assert_eq!(tracker.available_capital(), Decimal::from(950));
    }

    #[test]
    fn deployed_capital_reflects_open_positions() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Xrp, OutcomeTokenSide::Yes),
            Decimal::new(50, 2),
            Decimal::from(100),
        );
        let mut second_ctx = ctx("yes-token-2", PriceAsset::Xrp, OutcomeTokenSide::Yes);
        second_ctx.condition_id = Some("cond-2".to_string());
        tracker.apply_simulated_fill(second_ctx, Decimal::new(40, 2), Decimal::from(50));

        assert_eq!(tracker.deployed_capital(), Decimal::from(70));
        assert_eq!(tracker.available_capital(), Decimal::from(930));
    }

    #[test]
    fn settled_condition_blocks_reentry() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        let mut c = ctx("yes-token", PriceAsset::Btc, OutcomeTokenSide::Yes);
        c.end_time = Some(Utc::now() - chrono::Duration::seconds(20));
        tracker.apply_simulated_fill(c, Decimal::new(50, 2), Decimal::from(10));

        let mut resolutions = HashMap::new();
        resolutions.insert("cond-1".to_string(), resolution("cond-1", Some(true), None));
        let closed = tracker.settle_expired_positions(Utc::now(), 10, &resolutions);

        assert_eq!(closed.len(), 1);
        assert!(tracker.has_active_or_settled_position("cond-1"));
        assert!(!tracker.has_active_or_settled_position("cond-999"));
    }

    #[test]
    fn open_position_count_tracks_active_positions() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Sol, OutcomeTokenSide::Yes),
            Decimal::new(40, 2),
            Decimal::from(10),
        );
        let mut second_ctx = ctx("no-token", PriceAsset::Sol, OutcomeTokenSide::No);
        second_ctx.condition_id = Some("cond-2".to_string());
        tracker.apply_simulated_fill(second_ctx, Decimal::new(60, 2), Decimal::from(10));

        assert_eq!(tracker.open_position_count(), 2);
    }

    #[test]
    fn pending_order_context_blocks_duplicate_entry() {
        let mut tracker = PnlTracker::new(Decimal::from(1000), FeeRegime::Flat { rate: Decimal::ZERO });
        let mut pending = ctx("pending-token", PriceAsset::Btc, OutcomeTokenSide::Yes);
        pending.condition_id = Some("cond-pending".to_string());
        tracker.register_order_context("order-1".to_string(), pending);

        assert!(tracker.has_active_or_settled_position("cond-pending"));

        tracker.clear_order_context("order-1");
        assert!(!tracker.has_active_or_settled_position("cond-pending"));
    }

    #[test]
    fn available_capital_clamps_to_zero_when_deployed_exceeds_cash() {
        let mut tracker = PnlTracker::new(Decimal::from(10), FeeRegime::Flat { rate: Decimal::ZERO });
        tracker.apply_simulated_fill(
            ctx("yes-token", PriceAsset::Eth, OutcomeTokenSide::Yes),
            Decimal::new(90, 2),
            Decimal::from(20),
        );

        assert_eq!(tracker.deployed_capital(), Decimal::from(18));
        assert_eq!(tracker.available_capital(), Decimal::ZERO);
    }
}
