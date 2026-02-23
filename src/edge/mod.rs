pub mod fees;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::venue::{OrderBook, Side};

pub use fees::{FeeModel, TradingCost};

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
/// net_edge = posterior - executable_price - taker_fee - slippage - latency_penalty
///
/// Uses side-aware book prices (best ask for buys, best bid for sells), NOT midpoint.
pub fn calculate_edge(
    posterior: f64,
    book: &OrderBook,
    fee_model: &FeeModel,
    order_size: Decimal,
) -> Result<EdgeResult, EdgeError> {
    // Determine which side has edge
    let yes_ask = book.best_ask().and_then(|p| p.to_f64()).unwrap_or(1.0);
    let no_bid = book.best_bid().and_then(|p| p.to_f64()).unwrap_or(0.0);

    // Edge on Yes side: model says Yes is worth `posterior`, market asks `yes_ask`
    let yes_gross_edge = posterior - yes_ask;

    // Edge on No side: model says No is worth `1 - posterior`, market asks `1 - no_bid`
    // Equivalently: buying No is profitable when the market overprices Yes
    let no_gross_edge = (1.0 - posterior) - (1.0 - no_bid);

    // Pick the side with more edge
    let (side, gross_edge, executable_price_decimal) = if yes_gross_edge > no_gross_edge {
        (
            EdgeSide::BuyYes,
            yes_gross_edge,
            FeeModel::executable_price(book, Side::Buy)
                .ok_or(EdgeError::NoExecutablePrice(Side::Buy))?,
        )
    } else {
        (
            EdgeSide::BuyNo,
            no_gross_edge,
            FeeModel::executable_price(book, Side::Sell)
                .ok_or(EdgeError::NoExecutablePrice(Side::Sell))?,
        )
    };

    let book_side = match side {
        EdgeSide::BuyYes => Side::Buy,
        EdgeSide::BuyNo => Side::Sell,
    };

    let trading_cost = fee_model.total_cost(book, book_side, order_size);
    let cost_f64 = trading_cost.total.to_f64().unwrap_or(0.0);
    let net_edge = gross_edge - cost_f64;
    let executable_price = executable_price_decimal.to_f64().unwrap_or(0.5);

    Ok(EdgeResult {
        posterior,
        executable_price,
        net_edge,
        gross_edge,
        trading_cost,
        side,
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
}
