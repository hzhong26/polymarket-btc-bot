pub mod clock;
pub mod kill_switch;
pub mod limits;

use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::config::RiskConfig;

#[derive(Debug, thiserror::Error)]
pub enum RiskError {
    #[error("max position size exceeded: {current} / {max}")]
    MaxPositionExceeded { current: Decimal, max: Decimal },
    #[error("max total exposure exceeded: {current} / {max}")]
    MaxExposureExceeded { current: Decimal, max: Decimal },
    #[error("drawdown limit breached: {drawdown_pct} > {max_pct}")]
    DrawdownBreached {
        drawdown_pct: Decimal,
        max_pct: Decimal,
    },
    #[error("cooldown active: {remaining_secs}s remaining")]
    CooldownActive { remaining_secs: i64 },
    #[error("too many open orders: {current} / {max}")]
    TooManyOpenOrders { current: u32, max: u32 },
    #[error("stale market data: {age_ms}ms > {max_ms}ms")]
    StaleData { age_ms: u64, max_ms: u64 },
}

/// Record of a completed trade for PnL tracking.
#[derive(Debug, Clone)]
pub struct TradeResult {
    pub timestamp: DateTime<Utc>,
    pub pnl: Decimal,
    pub market_question: String,
}

/// Risk manager — the final gate before order execution.
/// Enforces position limits, drawdown protection, cooldowns,
/// and stale-data guards.
pub struct RiskManager {
    config: RiskConfig,
    peak_bankroll: Decimal,
    consecutive_losses: u32,
    cooldown_until: Option<DateTime<Utc>>,
    trade_log: VecDeque<TradeResult>,
    halted: bool,
}

impl RiskManager {
    pub fn new(config: RiskConfig, starting_bankroll: Decimal) -> Self {
        Self {
            config,
            peak_bankroll: starting_bankroll,
            consecutive_losses: 0,
            cooldown_until: None,
            trade_log: VecDeque::with_capacity(1000),
            halted: false,
        }
    }

    /// Check if a proposed trade passes all risk checks.
    /// Returns Ok(()) if allowed, Err(RiskError) if blocked.
    pub fn check(
        &self,
        proposed_amount: Decimal,
        current_exposure: Decimal,
        current_bankroll: Decimal,
        open_order_count: u32,
    ) -> Result<(), RiskError> {
        if self.halted {
            return Err(RiskError::DrawdownBreached {
                drawdown_pct: self.current_drawdown_pct(current_bankroll),
                max_pct: self.config.max_drawdown_pct,
            });
        }

        // 1. Check cooldown
        if let Some(until) = self.cooldown_until {
            let now = Utc::now();
            if now < until {
                let remaining = (until - now).num_seconds();
                return Err(RiskError::CooldownActive {
                    remaining_secs: remaining,
                });
            }
        }

        // 2. Check position size
        if proposed_amount > self.config.max_position_usdc {
            return Err(RiskError::MaxPositionExceeded {
                current: proposed_amount,
                max: self.config.max_position_usdc,
            });
        }

        // 3. Check total exposure
        let new_exposure = current_exposure + proposed_amount;
        if new_exposure > self.config.max_total_exposure {
            return Err(RiskError::MaxExposureExceeded {
                current: new_exposure,
                max: self.config.max_total_exposure,
            });
        }

        // 4. Check drawdown
        let drawdown = self.current_drawdown_pct(current_bankroll);
        if drawdown > self.config.max_drawdown_pct {
            return Err(RiskError::DrawdownBreached {
                drawdown_pct: drawdown,
                max_pct: self.config.max_drawdown_pct,
            });
        }

        // 5. Check open order count
        if open_order_count >= self.config.max_open_orders {
            return Err(RiskError::TooManyOpenOrders {
                current: open_order_count,
                max: self.config.max_open_orders,
            });
        }

        Ok(())
    }

    /// Record a completed trade result.
    pub fn record_trade(&mut self, result: TradeResult) {
        if result.pnl < Decimal::ZERO {
            self.consecutive_losses += 1;
            if self.consecutive_losses >= self.config.max_consecutive_losses {
                let cooldown =
                    chrono::Duration::milliseconds(self.config.cooldown_after_loss_ms as i64);
                self.cooldown_until = Some(Utc::now() + cooldown);
                tracing::warn!(
                    consecutive_losses = self.consecutive_losses,
                    cooldown_ms = self.config.cooldown_after_loss_ms,
                    "cooldown activated"
                );
            }
        } else {
            self.consecutive_losses = 0;
            self.cooldown_until = None;
        }

        self.trade_log.push_back(result);
        if self.trade_log.len() > 1000 {
            self.trade_log.pop_front();
        }
    }

    /// Update peak bankroll (call after checking balance).
    pub fn update_peak(&mut self, current_bankroll: Decimal) {
        if current_bankroll > self.peak_bankroll {
            self.peak_bankroll = current_bankroll;
        }
    }

    /// Check if the bot should be halted entirely.
    pub fn should_halt(&self) -> bool {
        self.halted
    }

    /// Manually halt the bot (e.g. from kill-switch).
    pub fn halt(&mut self) {
        self.halted = true;
        tracing::error!("risk manager: bot HALTED");
    }

    /// Returns true if the current drawdown exceeds the configured maximum.
    pub fn is_drawdown_breached(&self, current_bankroll: Decimal) -> bool {
        self.halted || self.current_drawdown_pct(current_bankroll) > self.config.max_drawdown_pct
    }

    /// Returns (current_drawdown_pct, max_drawdown_pct) for reporting.
    pub fn drawdown_status(&self, current_bankroll: Decimal) -> (Decimal, Decimal) {
        (self.current_drawdown_pct(current_bankroll), self.config.max_drawdown_pct)
    }

    /// Current drawdown from peak as a fraction (e.g. 0.10 = 10%).
    pub fn current_drawdown_pct(&self, current_bankroll: Decimal) -> Decimal {
        if self.peak_bankroll <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let drawdown = self.peak_bankroll - current_bankroll;
        if drawdown <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        drawdown / self.peak_bankroll
    }

    /// Get cumulative PnL.
    pub fn cumulative_pnl(&self) -> Decimal {
        self.trade_log.iter().map(|t| t.pnl).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> RiskConfig {
        RiskConfig {
            max_position_usdc: Decimal::from(500),
            max_total_exposure: Decimal::from(2000),
            max_drawdown_pct: Decimal::new(15, 2), // 0.15
            cooldown_after_loss_ms: 60000,
            max_consecutive_losses: 3,
            max_open_orders: 10,
            max_quote_age_ms: 5000,
            ws_disconnect_action: "cancel-all".into(),
            live: None,
        }
    }

    #[test]
    fn test_check_passes() {
        let mgr = RiskManager::new(default_config(), Decimal::from(1000));
        let result = mgr.check(
            Decimal::from(100),  // proposed
            Decimal::from(500),  // current exposure
            Decimal::from(1000), // bankroll
            2,                   // open orders
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_max_position_exceeded() {
        let mgr = RiskManager::new(default_config(), Decimal::from(1000));
        let result = mgr.check(
            Decimal::from(600), // > 500 max
            Decimal::ZERO,
            Decimal::from(1000),
            0,
        );
        assert!(matches!(result, Err(RiskError::MaxPositionExceeded { .. })));
    }

    #[test]
    fn test_max_exposure_exceeded() {
        let mgr = RiskManager::new(default_config(), Decimal::from(1000));
        let result = mgr.check(
            Decimal::from(100),
            Decimal::from(1950), // 1950 + 100 = 2050 > 2000
            Decimal::from(1000),
            0,
        );
        assert!(matches!(result, Err(RiskError::MaxExposureExceeded { .. })));
    }

    #[test]
    fn test_drawdown_breached() {
        let mgr = RiskManager::new(default_config(), Decimal::from(1000));
        let result = mgr.check(
            Decimal::from(100),
            Decimal::ZERO,
            Decimal::from(800), // 20% drawdown > 15% max
            0,
        );
        assert!(matches!(result, Err(RiskError::DrawdownBreached { .. })));
    }

    #[test]
    fn test_cooldown_after_losses() {
        let mut mgr = RiskManager::new(default_config(), Decimal::from(1000));
        for _ in 0..3 {
            mgr.record_trade(TradeResult {
                timestamp: Utc::now(),
                pnl: Decimal::from(-10),
                market_question: "test".into(),
            });
        }
        let result = mgr.check(Decimal::from(100), Decimal::ZERO, Decimal::from(1000), 0);
        assert!(matches!(result, Err(RiskError::CooldownActive { .. })));
    }

    #[test]
    fn test_too_many_orders() {
        let mgr = RiskManager::new(default_config(), Decimal::from(1000));
        let result = mgr.check(Decimal::from(100), Decimal::ZERO, Decimal::from(1000), 10);
        assert!(matches!(result, Err(RiskError::TooManyOpenOrders { .. })));
    }

    #[test]
    fn test_winning_resets_consecutive_losses() {
        let mut mgr = RiskManager::new(default_config(), Decimal::from(1000));
        mgr.record_trade(TradeResult {
            timestamp: Utc::now(),
            pnl: Decimal::from(-10),
            market_question: "test".into(),
        });
        mgr.record_trade(TradeResult {
            timestamp: Utc::now(),
            pnl: Decimal::from(-10),
            market_question: "test".into(),
        });
        // Win resets counter
        mgr.record_trade(TradeResult {
            timestamp: Utc::now(),
            pnl: Decimal::from(20),
            market_question: "test".into(),
        });
        // Another loss — but counter was reset, so still < 3
        mgr.record_trade(TradeResult {
            timestamp: Utc::now(),
            pnl: Decimal::from(-10),
            market_question: "test".into(),
        });

        let result = mgr.check(Decimal::from(100), Decimal::ZERO, Decimal::from(1000), 0);
        assert!(result.is_ok());
    }
}
