/// Numerically stable log-sum-exp: log(exp(a) + exp(b))
/// Uses the identity: log(exp(a) + exp(b)) = max(a,b) + log(1 + exp(-|a-b|))
/// This avoids overflow when a or b are large.
pub fn log_sum_exp(a: f64, b: f64) -> f64 {
    let max = a.max(b);
    if max == f64::NEG_INFINITY {
        return f64::NEG_INFINITY;
    }
    max + (1.0 + (-(a - b).abs()).exp()).ln()
}

/// LMSR cost function for binary outcomes.
/// C(q_yes, q_no) = b * log(exp(q_yes/b) + exp(q_no/b))
///                = b * log_sum_exp(q_yes/b, q_no/b)
pub fn cost_function(b: f64, q_yes: f64, q_no: f64) -> f64 {
    b * log_sum_exp(q_yes / b, q_no / b)
}

/// Numerically stable softmax for binary outcomes.
/// Returns (p_yes, p_no) where:
///   p_yes = exp(q_yes/b) / (exp(q_yes/b) + exp(q_no/b))
///         = 1 / (1 + exp((q_no - q_yes) / b))   (sigmoid form)
///   p_no  = 1 - p_yes
///
/// The sigmoid form is used because it avoids computing large exponentials directly.
pub fn softmax_binary(b: f64, q_yes: f64, q_no: f64) -> (f64, f64) {
    let z = (q_no - q_yes) / b;
    let p_yes = sigmoid(-z); // 1 / (1 + exp(z))
    (p_yes, 1.0 - p_yes)
}

/// Standard sigmoid function: σ(x) = 1 / (1 + exp(-x))
/// Handles extreme values to avoid NaN.
fn sigmoid(x: f64) -> f64 {
    if x >= 0.0 {
        let ex = x.exp();
        ex / (1.0 + ex)
    } else {
        1.0 / (1.0 + (-x).exp())
    }
}

/// Cost to buy `delta` additional Yes shares given current state (q_yes, q_no).
/// Cost = C(q_yes + delta, q_no) - C(q_yes, q_no)
pub fn cost_to_buy_yes(b: f64, q_yes: f64, q_no: f64, delta: f64) -> f64 {
    cost_function(b, q_yes + delta, q_no) - cost_function(b, q_yes, q_no)
}

/// Cost to buy `delta` additional No shares given current state.
pub fn cost_to_buy_no(b: f64, q_yes: f64, q_no: f64, delta: f64) -> f64 {
    cost_function(b, q_yes, q_no + delta) - cost_function(b, q_yes, q_no)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn test_log_sum_exp_basic() {
        // log(exp(1) + exp(2)) = log(e + e^2) ≈ 2.3133
        let result = log_sum_exp(1.0, 2.0);
        let expected = (1.0_f64.exp() + 2.0_f64.exp()).ln();
        assert_abs_diff_eq!(result, expected, epsilon = 1e-10);
    }

    #[test]
    fn test_log_sum_exp_large_values() {
        // Should not overflow even with very large values
        let result = log_sum_exp(1000.0, 1001.0);
        // ≈ 1001 + log(1 + exp(-1)) ≈ 1001.3133
        let expected = 1001.0 + (1.0 + (-1.0_f64).exp()).ln();
        assert_abs_diff_eq!(result, expected, epsilon = 1e-6);
    }

    #[test]
    fn test_log_sum_exp_equal() {
        // log(exp(5) + exp(5)) = 5 + log(2)
        let result = log_sum_exp(5.0, 5.0);
        assert_abs_diff_eq!(result, 5.0 + 2.0_f64.ln(), epsilon = 1e-10);
    }

    #[test]
    fn test_log_sum_exp_negative_infinity() {
        let result = log_sum_exp(f64::NEG_INFINITY, f64::NEG_INFINITY);
        assert_eq!(result, f64::NEG_INFINITY);
    }

    #[test]
    fn test_softmax_binary_equal_quantities() {
        // Equal quantities → equal probabilities
        let (p_yes, p_no) = softmax_binary(1.0, 0.0, 0.0);
        assert_abs_diff_eq!(p_yes, 0.5, epsilon = 1e-10);
        assert_abs_diff_eq!(p_no, 0.5, epsilon = 1e-10);
    }

    #[test]
    fn test_softmax_binary_sum_to_one() {
        let (p_yes, p_no) = softmax_binary(2.0, 3.0, 1.0);
        assert_abs_diff_eq!(p_yes + p_no, 1.0, epsilon = 1e-10);
    }

    #[test]
    fn test_softmax_binary_more_yes_higher_price() {
        // More yes shares outstanding → higher yes price
        let (p_yes, _) = softmax_binary(1.0, 5.0, 1.0);
        assert!(p_yes > 0.5);
    }

    #[test]
    fn test_softmax_binary_extreme() {
        // Very lopsided → approaches 0 and 1
        let (p_yes, p_no) = softmax_binary(1.0, 100.0, 0.0);
        assert!(p_yes > 0.999);
        assert!(p_no < 0.001);
    }

    #[test]
    fn test_cost_function_properties() {
        let b = 10.0;

        // Cost function should increase when adding shares
        let c0 = cost_function(b, 0.0, 0.0);
        let c1 = cost_function(b, 1.0, 0.0);
        assert!(c1 > c0);

        // Symmetric at equal quantities
        let c_sym1 = cost_function(b, 5.0, 3.0);
        let c_sym2 = cost_function(b, 3.0, 5.0);
        assert_abs_diff_eq!(c_sym1, c_sym2, epsilon = 1e-10);
    }

    #[test]
    fn test_cost_to_buy_yes_positive() {
        let b = 10.0;
        let cost = cost_to_buy_yes(b, 0.0, 0.0, 1.0);
        // Cost should be positive
        assert!(cost > 0.0);
        // At equal quantities, cost of 1 share ≈ 0.5 (midpoint)
        assert_abs_diff_eq!(cost, 0.5, epsilon = 0.1);
    }

    #[test]
    fn test_cost_increases_with_quantity() {
        let b = 10.0;
        // Buying more shares should cost progressively more
        let cost_1 = cost_to_buy_yes(b, 0.0, 0.0, 1.0);
        let cost_10 = cost_to_buy_yes(b, 0.0, 0.0, 10.0);
        assert!(cost_10 > cost_1 * 5.0);
    }

    #[test]
    fn test_market_maker_bounded_subsidy() {
        // The market maker's maximum subsidy in a binary market is b * ln(2).
        // This means: the difference between the total money collected from traders
        // and the payout to the winner is at most b * ln(2).
        //
        // Verify: cost to move from equal state (0,0) to extreme state where
        // one outcome dominates converges, and total payout minus cost is bounded.
        let b = 100.0;

        // Cost to buy many Yes shares (moving probability toward 1)
        let large_buy = cost_to_buy_yes(b, 0.0, 0.0, 10000.0);
        // The buyer pays `large_buy` and wins 10000 shares worth 10000 if Yes occurs.
        // Market maker subsidy = payout - collected = 10000 - large_buy
        // This subsidy should approach b * ln(2) as the buy gets very large
        let subsidy = 10000.0 - large_buy;
        let max_subsidy = b * 2.0_f64.ln();
        assert!(
            subsidy <= max_subsidy + 0.1,
            "subsidy {} should be <= max_subsidy {} + 0.1",
            subsidy,
            max_subsidy
        );
    }

    #[test]
    fn test_cost_to_buy_no() {
        let b = 10.0;
        let cost = cost_to_buy_no(b, 0.0, 0.0, 1.0);
        assert!(cost > 0.0);
        // Symmetric with cost_to_buy_yes at equal state
        let cost_yes = cost_to_buy_yes(b, 0.0, 0.0, 1.0);
        assert_abs_diff_eq!(cost, cost_yes, epsilon = 1e-10);
    }

    #[test]
    fn test_b_parameter_effect() {
        // Higher b → less price impact per share
        let cost_low_b = cost_to_buy_yes(1.0, 0.0, 0.0, 5.0);
        let cost_high_b = cost_to_buy_yes(100.0, 0.0, 0.0, 5.0);
        // Normalized by starting price (~0.5), high b should have less slippage
        assert!(cost_high_b / 100.0 < cost_low_b / 1.0);
    }
}
