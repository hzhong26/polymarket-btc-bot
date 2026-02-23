use super::dashboard::{print_market_dashboard, print_pnl_dashboard};
use super::pipeline::{PipelineAction, PipelineResult};
use super::pnl::PnlSnapshot;
use tracing::info;

/// Paper trading mode — logs all decisions without executing orders.
/// Used for validating the full pipeline before going live.
pub struct PaperTrader {
    eval_count: u64,
    trade_count: u64,
    dashboard_enabled: bool,
}

impl PaperTrader {
    pub fn new(dashboard_enabled: bool) -> Self {
        Self {
            eval_count: 0,
            trade_count: 0,
            dashboard_enabled,
        }
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
                self.trade_count += 1;

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
                    "PAPER TRADE: would place order"
                );
            }
        }
    }

    pub fn trade_count(&self) -> u64 {
        self.trade_count
    }
}
