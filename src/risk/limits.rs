// Position and exposure limit calculations.
// Core logic is in mod.rs (RiskManager::check).
// This module can be extended with additional limit types.

use rust_decimal::Decimal;

/// Compute the maximum position size given current constraints.
/// Returns the minimum of: max_position, remaining exposure capacity, and kelly-recommended size.
pub fn effective_max_position(
    max_position: Decimal,
    max_total_exposure: Decimal,
    current_exposure: Decimal,
    kelly_amount: Decimal,
) -> Decimal {
    let remaining_exposure = (max_total_exposure - current_exposure).max(Decimal::ZERO);
    kelly_amount.min(max_position).min(remaining_exposure)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effective_max_position() {
        let result = effective_max_position(
            Decimal::from(500),  // max per position
            Decimal::from(2000), // max total
            Decimal::from(1800), // current
            Decimal::from(300),  // kelly says 300
        );
        // Remaining exposure = 200, which is the binding constraint
        assert_eq!(result, Decimal::from(200));
    }

    #[test]
    fn test_kelly_is_binding() {
        let result = effective_max_position(
            Decimal::from(500),
            Decimal::from(2000),
            Decimal::ZERO,
            Decimal::from(100), // kelly says only 100
        );
        assert_eq!(result, Decimal::from(100));
    }
}
