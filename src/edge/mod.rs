pub mod fees;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::venue::{OrderBook, Side};

pub use fees::{FeeModel, FeeRegime, TradingCost};

#[derive(Debug, thiserror::Error)]
pub enum EdgeError {
    #[error("no executable price available on {0:?} side")]
    NoExecutablePrice(Side),
    #[error("insufficient liquidity for order size {0}")]
    InsufficientLiquidity(Decimal),
}

/// Result of an executable edge calculation.
#[derive(Debug, Clone)]
pub struct EdgeResult {
    /// The posterior probability from the Bayesian model
    pub posterior: f64,
    /// The executable price (best ask for buys, best bid for sells)
    pub executable_price: f64,
    /// Net edge after all costs
    pub net_edge: f64,
    /// Gross edge before costs
    pub gross_edge: f64,
    /// Total trading costs
    pub trading_cost: TradingCost,
    /// Which side the edge favors
    pub side: EdgeSide,
    /// Whether fee asymmetry flipped the side vs what gross edge would have chosen
    pub fee_asymmetry_flip: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EdgeSide {
    /// Market underprices Yes → buy Yes tokens
    BuyYes,
    /// Market underprices No → buy No tokens
    BuyNo,
}

/// Calculate the executable edge for a market.
///
/// Computes net edge (gross - costs) for **both** sides and picks the winner.
/// This accounts for fee asymmetry: Polymarket's dynamic fee formula is price-dependent,
/// so the cheaper-fee side can win even with slightly lower gross edge.
///
/// Uses side-aware book prices (best ask for buys, best bid for sells), NOT midpoint.
pub fn calculate_edge(
    posterior: f64,
    book: &OrderBook,
    fee_model: &FeeModel,
    order_size: Decimal,
) -> Result<EdgeResult, EdgeError> {
    let yes_ask = book.best_ask().and_then(|p| p.to_f64()).unwrap_or(1.0);
    let no_bid = book.best_bid().and_then(|p| p.to_f64()).unwrap_or(0.0);

    // Gross edge on each side
    let yes_gross_edge = posterior - yes_ask;
    let no_gross_edge = (1.0 - posterior) - (1.0 - no_bid); // = no_bid - posterior

    // Compute net edge for each side that has an executable price
    let yes_exec = FeeModel::executable_price(book, Side::Buy);
    let no_exec = FeeModel::executable_price(book, Side::Sell);

    let yes_net = yes_exec.map(|exec_price| {
        let cost = fee_model.total_cost(book, Side::Buy, order_size, exec_price);
        let cost_f64 = cost.total.to_f64().unwrap_or(0.0);
        (yes_gross_edge, yes_gross_edge - cost_f64, exec_price, cost)
    });

    let no_net = no_exec.map(|exec_price| {
        let cost = fee_model.total_cost(book, Side::Sell, order_size, exec_price);
        let cost_f64 = cost.total.to_f64().unwrap_or(0.0);
        (no_gross_edge, no_gross_edge - cost_f64, exec_price, cost)
    });

    // Pick the side with higher net edge, falling back if one side has no executable price
    let (side, gross_edge, net_edge, exec_price, trading_cost, fee_asymmetry_flip) =
        match (yes_net, no_net) {
            (Some((y_gross, y_net, y_exec, y_cost)), Some((n_gross, n_net, n_exec, n_cost))) => {
                let gross_would_pick_yes = y_gross > n_gross;
                if y_net >= n_net {
                    let flip = !gross_would_pick_yes;
                    (EdgeSide::BuyYes, y_gross, y_net, y_exec, y_cost, flip)
                } else {
                    let flip = gross_would_pick_yes;
                    (EdgeSide::BuyNo, n_gross, n_net, n_exec, n_cost, flip)
                }
            }
            (Some((y_gross, y_net, y_exec, y_cost)), None) => {
                (EdgeSide::BuyYes, y_gross, y_net, y_exec, y_cost, false)
            }
            (None, Some((n_gross, n_net, n_exec, n_cost))) => {
                (EdgeSide::BuyNo, n_gross, n_net, n_exec, n_cost, false)
            }
            (None, None) => {
                return Err(EdgeError::NoExecutablePrice(Side::Buy));
            }
        };

    let executable_price = exec_price.to_f64().unwrap_or(0.5);

    Ok(EdgeResult {
        posterior,
        executable_price,
        net_edge,
        gross_edge,
        trading_cost,
        side,
        fee_asymmetry_flip,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::{FeeSchedule, OrderBookLevel};
    use chrono::Utc;

    fn make_book(best_bid: f64, best_ask: f64) -> OrderBook {
        OrderBook {
            token_id: "test".into(),
            bids: vec![OrderBookLevel {
                price: Decimal::try_from(best_bid).unwrap(),
                size: Decimal::from(1000),
            }],
            asks: vec![OrderBookLevel {
                price: Decimal::try_from(best_ask).unwrap(),
                size: Decimal::from(1000),
            }],
            tick_size: Decimal::new(1, 2),
            min_order_size: Decimal::ONE,
            neg_risk: false,
            timestamp: Utc::now(),
        }
    }

    fn make_fee_model() -> FeeModel {
        let schedule = FeeSchedule {
            taker_fee_bps: Decimal::from(200), // 2%
            maker_rebate_bps: Decimal::ZERO,
        };
        FeeModel::new(&schedule, Decimal::new(5, 3)) // 0.5% latency
    }

    #[test]
    fn test_positive_edge_buy_yes() {
        let book = make_book(0.50, 0.55);
        let fee_model = make_fee_model();

        // Model says 70% probability, market asks 55% for Yes
        let result = calculate_edge(0.70, &book, &fee_model, Decimal::from(100)).unwrap();
        assert_eq!(result.side, EdgeSide::BuyYes);
        assert!(result.gross_edge > 0.0);
        // Gross edge = 0.70 - 0.55 = 0.15
        assert!((result.gross_edge - 0.15).abs() < 0.01);
        // Net edge should be less than gross (costs subtracted)
        assert!(result.net_edge < result.gross_edge);
        assert!(result.net_edge > 0.0); // Still profitable after costs
    }

    #[test]
    fn test_edge_wiped_by_costs() {
        let book = make_book(0.50, 0.55);
        let fee_model = make_fee_model();

        // Small edge that gets eaten by costs
        let result = calculate_edge(0.57, &book, &fee_model, Decimal::from(100)).unwrap();
        // Gross edge = 0.57 - 0.55 = 0.02
        // Costs ≈ 0.02 (fee) + 0.005 (latency) = 0.025
        // Net edge should be negative
        assert!(result.net_edge < 0.0);
    }

    #[test]
    fn test_edge_buy_no() {
        let book = make_book(0.85, 0.90);
        let fee_model = make_fee_model();

        // Model says only 60% Yes, but market has Yes at 85-90 → buy No
        let result = calculate_edge(0.60, &book, &fee_model, Decimal::from(100)).unwrap();
        assert_eq!(result.side, EdgeSide::BuyNo);
        assert!(result.gross_edge > 0.0);
    }

    #[test]
    fn test_fee_asymmetry_flips_side() {
        // Dynamic fees are highly price-dependent:
        //   at p=0.50: fee = 0.25 × (0.50×0.50)^2 = 0.015625
        //   at p=0.90: fee = 0.25 × (0.90×0.10)^2 = 0.002025
        //
        // Construct a book where ask=0.50 (expensive fee zone) and bid=0.90 (cheap fee zone).
        // With posterior=0.705:
        //   BuyYes gross = 0.705 - 0.50 = 0.205  (higher)
        //   BuyNo  gross = 0.90 - 0.705 = 0.195  (lower)
        //   → Gross edge picks BuyYes
        //
        //   BuyYes net = 0.205 - 0.015625 - 0.005 ≈ 0.1844
        //   BuyNo  net = 0.195 - 0.002025 - 0.005 ≈ 0.1880
        //   → Net edge picks BuyNo (fee asymmetry flip!)
        let book = make_book(0.90, 0.50);
        let fee_model = FeeModel::with_dynamic(
            Decimal::new(25, 2), // 0.25 fee_rate
            2,                   // exponent
            Decimal::new(5, 3),  // 0.005 latency penalty
        );

        let result = calculate_edge(0.705, &book, &fee_model, Decimal::ONE).unwrap();

        // Net edge should pick BuyNo despite BuyYes having higher gross
        assert_eq!(result.side, EdgeSide::BuyNo);
        assert!(result.fee_asymmetry_flip, "should report a fee asymmetry flip");
        assert!(result.net_edge > 0.0, "BuyNo net edge should be positive");
    }

    #[test]
    fn test_no_flip_with_flat_fees() {
        // With flat fees, both sides pay the same fee, so no asymmetry flip possible
        let book = make_book(0.50, 0.55);
        let fee_model = make_fee_model();

        let result = calculate_edge(0.70, &book, &fee_model, Decimal::from(100)).unwrap();
        assert_eq!(result.side, EdgeSide::BuyYes);
        assert!(!result.fee_asymmetry_flip);
    }
}
