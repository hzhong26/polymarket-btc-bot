pub mod math;

#[derive(Debug, thiserror::Error)]
pub enum LmsrError {
    #[error("invalid liquidity parameter b: {0} (must be > 0)")]
    InvalidB(f64),
    #[error("numerical overflow in LMSR computation")]
    Overflow,
}

/// LMSR pricing engine for binary outcome markets.
///
/// Computes fair prices and trade costs using the Logarithmic Market Scoring Rule.
/// In the Polymarket context, this serves as a theoretical cross-check for
/// CLOB-based market prices, not as the primary pricing mechanism.
pub struct LmsrEngine {
    /// Liquidity parameter. Controls price sensitivity.
    /// Higher b = more liquidity, less price impact per share.
    /// Max market maker loss = b * ln(2) for binary markets.
    b: f64,
}

impl LmsrEngine {
    pub fn new(b: f64) -> Result<Self, LmsrError> {
        if b <= 0.0 || !b.is_finite() {
            return Err(LmsrError::InvalidB(b));
        }
        Ok(Self { b })
    }

    /// Get the liquidity parameter.
    pub fn b(&self) -> f64 {
        self.b
    }

    /// Compute prices for Yes and No outcomes given outstanding share quantities.
    /// Returns (p_yes, p_no) where both are in [0, 1] and sum to 1.
    pub fn price(&self, q_yes: f64, q_no: f64) -> (f64, f64) {
        math::softmax_binary(self.b, q_yes, q_no)
    }

    /// Cost to buy `delta` additional Yes shares at current state.
    pub fn cost_to_buy_yes(&self, q_yes: f64, q_no: f64, delta: f64) -> f64 {
        math::cost_to_buy_yes(self.b, q_yes, q_no, delta)
    }

    /// Cost to buy `delta` additional No shares at current state.
    pub fn cost_to_buy_no(&self, q_yes: f64, q_no: f64, delta: f64) -> f64 {
        math::cost_to_buy_no(self.b, q_yes, q_no, delta)
    }

    /// Given market prices, return the implied probabilities.
    /// For a CLOB, the market price of Yes IS the implied P(Yes).
    /// This method normalizes to ensure they sum to 1.
    pub fn implied_probability(&self, market_yes_price: f64, market_no_price: f64) -> (f64, f64) {
        let sum = market_yes_price + market_no_price;
        if sum <= 0.0 {
            return (0.5, 0.5);
        }
        (market_yes_price / sum, market_no_price / sum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn test_new_valid_b() {
        assert!(LmsrEngine::new(10.0).is_ok());
        assert!(LmsrEngine::new(0.001).is_ok());
    }

    #[test]
    fn test_new_invalid_b() {
        assert!(LmsrEngine::new(0.0).is_err());
        assert!(LmsrEngine::new(-1.0).is_err());
        assert!(LmsrEngine::new(f64::NAN).is_err());
        assert!(LmsrEngine::new(f64::INFINITY).is_err());
    }

    #[test]
    fn test_implied_probability_normalization() {
        let engine = LmsrEngine::new(1.0).unwrap();
        // Market showing 0.60 yes / 0.50 no (overround)
        let (p_yes, p_no) = engine.implied_probability(0.60, 0.50);
        assert_abs_diff_eq!(p_yes + p_no, 1.0, epsilon = 1e-10);
        assert_abs_diff_eq!(p_yes, 0.60 / 1.10, epsilon = 1e-10);
    }
}
