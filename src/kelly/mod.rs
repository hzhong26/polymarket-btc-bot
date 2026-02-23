pub mod types;

pub use types::{BetSide, PositionSize};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

/// Kelly Criterion position sizer with uncertainty haircut.
///
/// Computes optimal bet size for binary prediction markets:
/// 1. Full Kelly: f* = (b*p - q) / b
/// 2. Uncertainty haircut: f_adj = f* × (1 - model_uncertainty)
/// 3. Fractional multiplier: f_final = kelly_fraction × f_adj
pub struct KellySizer {
    /// Fractional Kelly multiplier (e.g. 0.25 = quarter Kelly)
    fraction: f64,
    /// Minimum net edge to generate a position (pre-filter)
    min_edge: f64,
    /// Maximum model uncertainty before refusing to size (e.g. 0.5)
    max_uncertainty: f64,
}

impl KellySizer {
    pub fn new(fraction: f64, min_edge: f64, max_uncertainty: f64) -> Self {
        Self {
            fraction: fraction.clamp(0.01, 1.0),
            min_edge: min_edge.max(0.0),
            max_uncertainty: max_uncertainty.clamp(0.0, 1.0),
        }
    }

    /// Calculate optimal position size using uncertainty-adjusted fractional Kelly.
    ///
    /// Arguments:
    /// - `posterior_prob`: estimated probability of winning (from Bayesian model)
    /// - `executable_price`: the price we'd pay to enter (best ask for buys)
    /// - `net_edge`: edge after fees, slippage, latency
    /// - `bankroll`: current available capital in USDC
    /// - `model_uncertainty`: calibration-derived uncertainty [0, 1]
    /// - `side`: which side the edge favors
    ///
    /// Returns None if:
    /// - Net edge < min_edge
    /// - Model uncertainty > max_uncertainty
    /// - Kelly fraction would be negative
    /// - Bankroll is zero
    pub fn size(
        &self,
        posterior_prob: f64,
        executable_price: f64,
        net_edge: f64,
        bankroll: Decimal,
        model_uncertainty: f64,
        side: BetSide,
    ) -> Option<PositionSize> {
        // Pre-filters
        if net_edge < self.min_edge {
            return None;
        }
        if model_uncertainty > self.max_uncertainty {
            return None;
        }
        if bankroll <= Decimal::ZERO {
            return None;
        }

        let bankroll_f64 = bankroll.to_f64()?;
        if bankroll_f64 <= 0.0 {
            return None;
        }

        // Validate inputs
        let p = posterior_prob.clamp(0.001, 0.999);
        let q = 1.0 - p;
        let price = executable_price.clamp(0.001, 0.999);

        // Decimal odds: b = (1/price) - 1
        // This is the profit per dollar risked if the bet wins.
        // E.g. buying at 0.55 → b = 0.818, so you win $0.818 per $1 risked
        let b = (1.0 / price) - 1.0;
        if b <= 0.0 {
            return None;
        }

        // Full Kelly: f* = (b*p - q) / b
        let raw_kelly = (b * p - q) / b;
        if raw_kelly <= 0.0 {
            return None; // No positive edge → no bet
        }

        // Uncertainty haircut: reduce proportionally to model uncertainty
        let adjusted_kelly = raw_kelly * (1.0 - model_uncertainty);
        if adjusted_kelly <= 0.0 {
            return None;
        }

        // Apply fractional multiplier
        let final_kelly = self.fraction * adjusted_kelly;

        // Convert to USDC amount
        let stake_f64 = bankroll_f64 * final_kelly;
        let stake_usdc = Decimal::try_from(stake_f64).ok()?.round_dp(2);

        if stake_usdc <= Decimal::ZERO {
            return None;
        }

        let expected_value = Decimal::try_from(net_edge * stake_f64).ok()?.round_dp(2);
        let size_tokens = (stake_usdc / Decimal::try_from(price).ok()?).round_dp(4);

        Some(PositionSize {
            raw_kelly,
            adjusted_kelly,
            final_kelly,
            stake_usdc,
            size_tokens,
            side,
            edge: net_edge,
            expected_value,
            model_uncertainty,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_kelly_sizing() {
        let sizer = KellySizer::new(0.25, 0.01, 0.5);

        // posterior = 0.70, price = 0.55, net_edge = 0.12
        // b = (1/0.55) - 1 = 0.818
        // raw kelly = (0.818 * 0.70 - 0.30) / 0.818 = (0.573 - 0.30) / 0.818 = 0.334
        // adjusted = 0.334 * (1 - 0.1) = 0.300
        // final = 0.25 * 0.300 = 0.075
        // amount = 1000 * 0.075 = 75

        let result = sizer
            .size(0.70, 0.55, 0.12, Decimal::from(1000), 0.1, BetSide::Yes)
            .unwrap();

        assert!(result.raw_kelly > 0.3);
        assert!(result.final_kelly > 0.0);
        assert!(result.final_kelly < result.raw_kelly);
        assert!(result.stake_usdc > Decimal::ZERO);
        assert_eq!(result.side, BetSide::Yes);
    }

    #[test]
    fn test_edge_below_minimum() {
        let sizer = KellySizer::new(0.25, 0.08, 0.5);
        // Net edge of 5% is below the 8% minimum
        let result = sizer.size(0.60, 0.55, 0.05, Decimal::from(1000), 0.1, BetSide::Yes);
        assert!(result.is_none());
    }

    #[test]
    fn test_high_uncertainty_blocks_trade() {
        let sizer = KellySizer::new(0.25, 0.01, 0.5);
        // Model uncertainty at 0.6 > max 0.5 → no trade
        let result = sizer.size(0.70, 0.55, 0.12, Decimal::from(1000), 0.6, BetSide::Yes);
        assert!(result.is_none());
    }

    #[test]
    fn test_zero_bankroll() {
        let sizer = KellySizer::new(0.25, 0.01, 0.5);
        let result = sizer.size(0.70, 0.55, 0.12, Decimal::ZERO, 0.1, BetSide::Yes);
        assert!(result.is_none());
    }

    #[test]
    fn test_negative_kelly() {
        let sizer = KellySizer::new(0.25, 0.0, 0.5);
        // posterior = 0.40, price = 0.55 → model says worth less than market asks
        // b = 0.818, raw kelly = (0.818 * 0.40 - 0.60) / 0.818 = negative
        let result = sizer.size(0.40, 0.55, -0.10, Decimal::from(1000), 0.1, BetSide::Yes);
        assert!(result.is_none());
    }

    #[test]
    fn test_uncertainty_haircut_effect() {
        let sizer = KellySizer::new(1.0, 0.01, 0.9); // Full Kelly, high max uncertainty

        let low_uncertainty = sizer
            .size(0.70, 0.55, 0.12, Decimal::from(1000), 0.0, BetSide::Yes)
            .unwrap();

        let high_uncertainty = sizer
            .size(0.70, 0.55, 0.12, Decimal::from(1000), 0.4, BetSide::Yes)
            .unwrap();

        // Higher uncertainty → smaller position
        assert!(high_uncertainty.stake_usdc < low_uncertainty.stake_usdc);
        assert!(high_uncertainty.adjusted_kelly < low_uncertainty.adjusted_kelly);
    }

    #[test]
    fn test_fractional_kelly_effect() {
        let full_kelly = KellySizer::new(1.0, 0.01, 0.5);
        let quarter_kelly = KellySizer::new(0.25, 0.01, 0.5);

        let full = full_kelly
            .size(0.70, 0.55, 0.12, Decimal::from(1000), 0.1, BetSide::Yes)
            .unwrap();

        let quarter = quarter_kelly
            .size(0.70, 0.55, 0.12, Decimal::from(1000), 0.1, BetSide::Yes)
            .unwrap();

        // Quarter Kelly should be ~25% of full Kelly
        let ratio = quarter.stake_usdc.to_f64().unwrap() / full.stake_usdc.to_f64().unwrap();
        assert!((ratio - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_bet_side_no() {
        let sizer = KellySizer::new(0.25, 0.01, 0.5);
        let result = sizer
            .size(0.70, 0.55, 0.12, Decimal::from(1000), 0.1, BetSide::No)
            .unwrap();
        assert_eq!(result.side, BetSide::No);
    }
}
