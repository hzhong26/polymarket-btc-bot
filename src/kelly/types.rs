use rust_decimal::Decimal;

/// The side of a binary market to bet on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BetSide {
    /// Buy Yes tokens (market underprices Yes)
    Yes,
    /// Buy No tokens (market underprices No)
    No,
}

/// Result of Kelly position sizing.
#[derive(Debug, Clone)]
pub struct PositionSize {
    /// Raw Kelly fraction (before uncertainty haircut and cap)
    pub raw_kelly: f64,
    /// Kelly fraction after uncertainty haircut
    pub adjusted_kelly: f64,
    /// Final Kelly fraction after applying the fractional multiplier
    pub final_kelly: f64,
    /// USDC stake amount (final_kelly * bankroll)
    pub stake_usdc: Decimal,
    /// Token quantity implied by stake and executable entry price.
    pub size_tokens: Decimal,
    /// Which side to bet
    pub side: BetSide,
    /// The net edge driving this position
    pub edge: f64,
    /// Expected value of the bet (edge * amount)
    pub expected_value: Decimal,
    /// Model uncertainty used for the haircut
    pub model_uncertainty: f64,
}
