use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tracing::info;

use crate::venue::{OrderBook, Side};

use super::dashboard::{print_market_dashboard, print_pnl_dashboard};
use super::pipeline::{PipelineAction, PipelineDiagnostics, PipelineResult};
use super::pnl::{FillContext, PnlSnapshot};

/// A pending paper-mode order that simulates the real GTC limit order lifecycle.
/// The order is placed on one tick and only fills when a subsequent order book
/// shows sufficient matching liquidity at or better than the limit price.
#[derive(Debug, Clone)]
pub struct PaperPendingOrder {
    pub paper_order_id: String,
    pub token_id: String,
    pub condition_id: String,
    pub side: Side,
    pub limit_price: Decimal,
    pub amount: Decimal,
    pub placed_at: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub fill_context: FillContext,
    pub diagnostics: PipelineDiagnostics,
    pub market_question: String,
}

/// Paper trading mode — logs all decisions without executing orders.
/// Used for validating the full pipeline before going live.
pub struct PaperTrader {
    eval_count: u64,
    trade_count: u64,
    dashboard_enabled: bool,
    pending_orders: HashMap<String, PaperPendingOrder>,
}

impl PaperTrader {
    pub fn new(dashboard_enabled: bool) -> Self {
        Self {
            eval_count: 0,
            trade_count: 0,
            dashboard_enabled,
            pending_orders: HashMap::new(),
        }
    }

    /// Store a new pending order. Returns the synthetic paper_order_id.
    pub fn place_pending(&mut self, order: PaperPendingOrder) -> String {
        let id = order.paper_order_id.clone();
        self.trade_count += 1;
        self.pending_orders.insert(order.condition_id.clone(), order);
        id
    }

    /// Check whether a pending order for `condition_id` can be filled against the
    /// given order book. For a BUY limit order at price P we walk the ask side and
    /// require cumulative size >= order amount at levels where ask_price <= P.
    /// Returns the order (removed from pending) if it would fill, None otherwise.
    pub fn check_fill(
        &mut self,
        condition_id: &str,
        book: &OrderBook,
    ) -> Option<PaperPendingOrder> {
        let order = self.pending_orders.get(condition_id)?;
        let limit = order.limit_price;
        let needed = order.amount;

        // Walk the ask side: accumulate levels where ask_price <= limit_price
        let mut available = Decimal::ZERO;
        for level in &book.asks {
            if level.price > limit {
                break; // asks are sorted ascending
            }
            available += level.size;
            if available >= needed {
                return self.pending_orders.remove(condition_id);
            }
        }

        None
    }

    /// Remove orders whose market has expired (past end_time). Returns expired orders
    /// so the caller can clean up order contexts and log expiry events.
    pub fn expire_stale(&mut self, now: DateTime<Utc>) -> Vec<PaperPendingOrder> {
        let expired_cids: Vec<String> = self
            .pending_orders
            .iter()
            .filter(|(_, order)| now >= order.end_time)
            .map(|(cid, _)| cid.clone())
            .collect();

        expired_cids
            .into_iter()
            .filter_map(|cid| self.pending_orders.remove(&cid))
            .collect()
    }

    /// Returns true if there is a pending (unfilled) order for this condition_id.
    pub fn has_pending(&self, condition_id: &str) -> bool {
        self.pending_orders.contains_key(condition_id)
    }

    /// Returns the set of condition_ids that have pending orders.
    /// Used to keep markets in the active set so their books are still fetched.
    pub fn pending_condition_ids(&self) -> HashSet<String> {
        self.pending_orders.keys().cloned().collect()
    }

    /// Returns true if there is a pending order for the NO token of this market.
    pub fn has_pending_no_order(&self, token_id_no: &str) -> bool {
        self.pending_orders
            .values()
            .any(|order| order.token_id == token_id_no)
    }

    /// Process a pipeline result in paper mode.
    /// Logs the decision but does NOT execute any orders.
    pub fn process(&mut self, result: &PipelineResult, snapshot: &PnlSnapshot) {
        self.eval_count += 1;

        if self.dashboard_enabled {
            print_market_dashboard(result, self.eval_count, self.trade_count + 1);
            print_pnl_dashboard(snapshot, "paper", "evaluation", None);
        }

        match &result.action {
            PipelineAction::NoTrade { reason } => {
                tracing::debug!(
                    market = %result.market.question,
                    reason,
                    "PAPER: no trade"
                );
            }
            PipelineAction::PlaceOrder {
                token_id,
                side,
                price,
                amount,
            } => {
                let edge = result
                    .edge_result
                    .as_ref()
                    .map(|e| e.net_edge)
                    .unwrap_or(0.0);

                let kelly = result
                    .position
                    .as_ref()
                    .map(|p| p.final_kelly)
                    .unwrap_or(0.0);

                info!(
                    trade_num = self.trade_count,
                    market = %result.market.question,
                    token_id,
                    side = ?side,
                    price = %price,
                    amount = %amount,
                    net_edge = %format!("{:.4}", edge),
                    kelly_fraction = %format!("{:.4}", kelly),
                    session_total_pnl = %snapshot.total_pnl.round_dp(2),
                    "PAPER TRADE: pending order placed (awaiting fill)"
                );
            }
        }
    }

    pub fn trade_count(&self) -> u64 {
        self.trade_count
    }

    pub fn pending_count(&self) -> usize {
        self.pending_orders.len()
    }
}
