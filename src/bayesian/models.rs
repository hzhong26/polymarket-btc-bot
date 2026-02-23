use std::f64::consts::{FRAC_1_SQRT_2, PI};

/// Standard normal CDF approximation (Abramowitz & Stegun, formula 26.2.17).
/// Accurate to approximately 1e-7.
///
/// Φ(x) = P(Z ≤ x) for standard normal Z.
pub fn normal_cdf(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }
    // Use the erfc-based formulation for numerical stability
    0.5 * erfc(-x * FRAC_1_SQRT_2)
}

/// Standard normal PDF: φ(x) = (1/√(2π)) * exp(-x²/2)
pub fn normal_pdf(x: f64) -> f64 {
    (1.0 / (2.0 * PI).sqrt()) * (-0.5 * x * x).exp()
}

/// Complementary error function approximation.
/// Uses Horner form of the rational approximation from Abramowitz & Stegun.
fn erfc(x: f64) -> f64 {
    // For negative x: erfc(-x) = 2 - erfc(x)
    if x < 0.0 {
        return 2.0 - erfc(-x);
    }

    // Rational approximation constants (A&S 7.1.26)
    let p = 0.3275911;
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;

    let t = 1.0 / (1.0 + p * x);
    let t2 = t * t;
    let t3 = t2 * t;
    let t4 = t3 * t;
    let t5 = t4 * t;

    let poly = a1 * t + a2 * t2 + a3 * t3 + a4 * t4 + a5 * t5;
    poly * (-x * x).exp()
}

/// Black-Scholes d2 for a binary call option.
///
/// d2 = (ln(S/K) + (r - 0.5 * σ²) * T) / (σ * √T)
///
/// For 5-minute crypto markets, we set r = 0 (no drift on short horizon):
/// d2 = (ln(S/K) - 0.5 * σ² * T) / (σ * √T)
///
/// Arguments:
/// - `spot`: current BTC price (S)
/// - `strike`: market threshold price (K)
/// - `time_to_expiry`: time to expiry in seconds
/// - `volatility`: annualized volatility (σ)
/// - `risk_free_rate`: risk-free rate (r), typically 0 for short horizons
pub fn black_scholes_d2(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    volatility: f64,
    risk_free_rate: f64,
) -> f64 {
    if spot <= 0.0 || strike <= 0.0 || time_to_expiry <= 0.0 || volatility <= 0.0 {
        return 0.0;
    }

    // Convert time from seconds to years for the standard formula
    let t_years = time_to_expiry / (365.25 * 24.0 * 3600.0);
    let sqrt_t = t_years.sqrt();

    let d2 = ((spot / strike).ln() + (risk_free_rate - 0.5 * volatility * volatility) * t_years)
        / (volatility * sqrt_t);

    d2
}

/// Probability that BTC > strike at expiry under Geometric Brownian Motion.
///
/// P(S_T > K) = N(d2)
///
/// where d2 is the Black-Scholes d2 parameter.
///
/// This is the probability of a binary call paying off.
///
/// Arguments:
/// - `spot`: current BTC price
/// - `strike`: price threshold
/// - `time_to_expiry`: time in seconds until market resolution
/// - `volatility`: annualized volatility
pub fn binary_call_probability(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    volatility: f64,
) -> f64 {
    let d2 = black_scholes_d2(spot, strike, time_to_expiry, volatility, 0.0);
    normal_cdf(d2)
}

/// Probability that BTC < strike at expiry (binary put).
/// P(S_T < K) = N(-d2) = 1 - N(d2)
pub fn binary_put_probability(spot: f64, strike: f64, time_to_expiry: f64, volatility: f64) -> f64 {
    1.0 - binary_call_probability(spot, strike, time_to_expiry, volatility)
}

/// Directional probability for Up/Down markets over a horizon.
///
/// Computes P(S_T >= S_ref) under GBM with drift:
/// ln(S_T / S_now) ~ N(mu_log * T, sigma^2 * T)
///
/// where:
/// - `current_spot` = S_now
/// - `reference_spot` = S_ref (interval-start anchor)
/// - `annualized_volatility` = sigma
/// - `annualized_drift` = mu_log (expected log-return drift)
pub fn directional_up_probability(
    current_spot: f64,
    reference_spot: f64,
    time_to_expiry: f64,
    annualized_volatility: f64,
    annualized_drift: f64,
) -> f64 {
    if current_spot <= 0.0 || reference_spot <= 0.0 {
        return 0.5;
    }
    if time_to_expiry <= 0.0 {
        return if current_spot >= reference_spot {
            1.0
        } else {
            0.0
        };
    }

    let t_years = time_to_expiry / (365.25 * 24.0 * 3600.0);
    if t_years <= 0.0 || !t_years.is_finite() {
        return 0.5;
    }

    let sigma = annualized_volatility.max(1e-6);
    let sigma_sqrt_t = sigma * t_years.sqrt();
    let log_threshold = (reference_spot / current_spot).ln();
    let mean = annualized_drift * t_years;
    let z = (mean - log_threshold) / sigma_sqrt_t;

    normal_cdf(z).clamp(0.001, 0.999)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn test_normal_cdf_zero() {
        // Φ(0) = 0.5
        assert_abs_diff_eq!(normal_cdf(0.0), 0.5, epsilon = 1e-6);
    }

    #[test]
    fn test_normal_cdf_positive() {
        // Φ(1) ≈ 0.8413
        assert_abs_diff_eq!(normal_cdf(1.0), 0.8413, epsilon = 1e-3);
        // Φ(2) ≈ 0.9772
        assert_abs_diff_eq!(normal_cdf(2.0), 0.9772, epsilon = 1e-3);
    }

    #[test]
    fn test_normal_cdf_negative() {
        // Φ(-1) ≈ 0.1587
        assert_abs_diff_eq!(normal_cdf(-1.0), 0.1587, epsilon = 1e-3);
        // Φ(-x) = 1 - Φ(x)
        assert_abs_diff_eq!(normal_cdf(-2.0), 1.0 - normal_cdf(2.0), epsilon = 1e-6);
    }

    #[test]
    fn test_normal_cdf_extreme() {
        // Φ(10) ≈ 1.0
        assert!(normal_cdf(10.0) > 0.9999);
        // Φ(-10) ≈ 0.0
        assert!(normal_cdf(-10.0) < 0.0001);
    }

    #[test]
    fn test_normal_pdf_zero() {
        // φ(0) = 1/√(2π) ≈ 0.3989
        let expected = 1.0 / (2.0 * PI).sqrt();
        assert_abs_diff_eq!(normal_pdf(0.0), expected, epsilon = 1e-6);
    }

    #[test]
    fn test_normal_pdf_symmetric() {
        assert_abs_diff_eq!(normal_pdf(1.5), normal_pdf(-1.5), epsilon = 1e-10);
    }

    #[test]
    fn test_binary_call_at_the_money() {
        // S = K, 5 minutes to expiry, ~80% annualized vol (typical BTC)
        // At the money, P should be close to 0.5 (slightly less due to drift term)
        let p = binary_call_probability(68000.0, 68000.0, 300.0, 0.80);
        assert_abs_diff_eq!(p, 0.5, epsilon = 0.01);
    }

    #[test]
    fn test_binary_call_deep_itm() {
        // BTC at 70000, strike at 65000, 5 minutes, 80% vol
        // Deeply in the money → probability close to 1
        let p = binary_call_probability(70000.0, 65000.0, 300.0, 0.80);
        assert!(p > 0.9);
    }

    #[test]
    fn test_binary_call_deep_otm() {
        // BTC at 60000, strike at 70000, 5 minutes, 80% vol
        // Deeply out of the money → probability close to 0
        let p = binary_call_probability(60000.0, 70000.0, 300.0, 0.80);
        assert!(p < 0.1);
    }

    #[test]
    fn test_binary_call_higher_vol_wider_range() {
        // Higher volatility makes deep OTM more likely
        let p_low_vol = binary_call_probability(65000.0, 70000.0, 300.0, 0.30);
        let p_high_vol = binary_call_probability(65000.0, 70000.0, 300.0, 1.50);
        assert!(p_high_vol > p_low_vol);
    }

    #[test]
    fn test_binary_call_more_time_to_expiry() {
        // More time → more uncertainty → OTM options worth more
        let p_5min = binary_call_probability(65000.0, 70000.0, 300.0, 0.80);
        let p_60min = binary_call_probability(65000.0, 70000.0, 3600.0, 0.80);
        assert!(p_60min > p_5min);
    }

    #[test]
    fn test_binary_put_probability() {
        let p_call = binary_call_probability(68000.0, 68000.0, 300.0, 0.80);
        let p_put = binary_put_probability(68000.0, 68000.0, 300.0, 0.80);
        assert_abs_diff_eq!(p_call + p_put, 1.0, epsilon = 1e-10);
    }

    #[test]
    fn test_directional_up_probability_above_reference() {
        let p = directional_up_probability(68050.0, 68000.0, 120.0, 0.80, 0.0);
        assert!(p > 0.5);
    }

    #[test]
    fn test_directional_up_probability_below_reference() {
        let p = directional_up_probability(67950.0, 68000.0, 120.0, 0.80, 0.0);
        assert!(p < 0.5);
    }

    #[test]
    fn test_directional_up_probability_positive_drift_increases_prob() {
        let p_low = directional_up_probability(68000.0, 68000.0, 300.0, 0.80, -0.5);
        let p_high = directional_up_probability(68000.0, 68000.0, 300.0, 0.80, 0.5);
        assert!(p_high > p_low);
    }

    #[test]
    fn test_black_scholes_d2_at_the_money() {
        // ATM with r=0: d2 = (ln(1) - 0.5*σ²*T) / (σ*√T) = -0.5*σ*√T
        let d2 = black_scholes_d2(68000.0, 68000.0, 300.0, 0.80, 0.0);
        let t_years: f64 = 300.0 / (365.25 * 24.0 * 3600.0);
        let expected = -0.5 * 0.80 * t_years.sqrt();
        assert_abs_diff_eq!(d2, expected, epsilon = 1e-6);
    }

    #[test]
    fn test_invalid_inputs_return_zero() {
        assert_eq!(black_scholes_d2(0.0, 68000.0, 300.0, 0.80, 0.0), 0.0);
        assert_eq!(black_scholes_d2(68000.0, 0.0, 300.0, 0.80, 0.0), 0.0);
        assert_eq!(black_scholes_d2(68000.0, 68000.0, 0.0, 0.80, 0.0), 0.0);
        assert_eq!(black_scholes_d2(68000.0, 68000.0, 300.0, 0.0, 0.0), 0.0);
    }
}
