use rust_decimal::Decimal;

use crate::venue::{FeeSchedule, OrderBook, Side};

/// Fee regime: how to compute the per-share taker fee.
#[derive(Debug, Clone, Copy)]
pub enum FeeRegime {
    /// Flat rate per share (legacy, e.g. 0.02).
    Flat { rate: Decimal },
    /// Polymarket dynamic: fee_rate × (p × (1 - p))^exponent.
    Dynamic { fee_rate: Decimal, exponent: u32 },
}

impl FeeRegime {
    /// Compute the taker fee per share at a given execution price.
    pub fn fee_per_share(&self, price: Decimal) -> Decimal {
        match self {
            FeeRegime::Flat { rate } => *rate,
            FeeRegime::Dynamic { fee_rate, exponent } => {
                let p = price.max(Decimal::ZERO).min(Decimal::ONE);
                let base = p * (Decimal::ONE - p);
                let mut result = Decimal::ONE;
                for _ in 0..*exponent {
                    result *= base;
                }
                *fee_rate * result
            }
        }
    }
}

/// Fee and slippage model for executable edge calculation.
pub struct FeeModel {
    /// Fee regime (flat or dynamic)
    pub fee_regime: FeeRegime,
    /// Latency penalty as a fraction of price (e.g. 0.005 = 0.5%)
    pub latency_penalty: Decimal,
}

impl FeeModel {
    /// Create a FeeModel with a flat fee from the venue's fee schedule.
    pub fn new(fee_schedule: &FeeSchedule, latency_penalty: Decimal) -> Self {
        Self {
            fee_regime: FeeRegime::Flat {
                rate: fee_schedule.taker_fee_rate(),
            },
            latency_penalty,
        }
    }

    /// Create a FeeModel with Polymarket's dynamic fee formula.
    pub fn with_dynamic(fee_rate: Decimal, exponent: u32, latency_penalty: Decimal) -> Self {
        Self {
            fee_regime: FeeRegime::Dynamic { fee_rate, exponent },
            latency_penalty,
        }
    }

    /// Compute the total trading cost (fee + slippage + latency penalty)
    /// for a given order on the given side of the book.
    ///
    /// `executable_price` is the price at which the order will execute,
    /// used to compute the price-dependent taker fee.
    pub fn total_cost(
        &self,
        book: &OrderBook,
        side: Side,
        order_size: Decimal,
        executable_price: Decimal,
    ) -> TradingCost {
        let slippage = match side {
            Side::Buy => book
                .estimate_buy_slippage(order_size)
                .unwrap_or(Decimal::ZERO),
            Side::Sell => book
                .estimate_sell_slippage(order_size)
                .unwrap_or(Decimal::ZERO),
        };

        let taker_fee = self.fee_regime.fee_per_share(executable_price);

        TradingCost {
            taker_fee,
            slippage,
            latency_penalty: self.latency_penalty,
            total: taker_fee + slippage + self.latency_penalty,
        }
    }

    /// Get the executable price after accounting for the spread.
    /// For buys: best ask (we pay the ask to get filled immediately).
    /// For sells: best bid (we receive the bid to get filled immediately).
    pub fn executable_price(book: &OrderBook, side: Side) -> Option<Decimal> {
        match side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradingCost {
    pub taker_fee: Decimal,
    pub slippage: Decimal,
    pub latency_penalty: Decimal,
    pub total: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::OrderBookLevel;
    use chrono::Utc;

    fn make_book() -> OrderBook {
        OrderBook {
            token_id: "test".into(),
            bids: vec![
                OrderBookLevel {
                    price: Decimal::new(55, 2),
                    size: Decimal::from(100),
                },
                OrderBookLevel {
                    price: Decimal::new(54, 2),
                    size: Decimal::from(200),
                },
            ],
            asks: vec![
                OrderBookLevel {
                    price: Decimal::new(57, 2),
                    size: Decimal::from(100),
                },
                OrderBookLevel {
                    price: Decimal::new(58, 2),
                    size: Decimal::from(200),
                },
            ],
            tick_size: Decimal::new(1, 2),
            min_order_size: Decimal::ONE,
            neg_risk: false,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn test_executable_price_buy() {
        let book = make_book();
        let price = FeeModel::executable_price(&book, Side::Buy);
        assert_eq!(price, Some(Decimal::new(57, 2))); // best ask
    }

    #[test]
    fn test_executable_price_sell() {
        let book = make_book();
        let price = FeeModel::executable_price(&book, Side::Sell);
        assert_eq!(price, Some(Decimal::new(55, 2))); // best bid
    }

    #[test]
    fn test_trading_cost_flat() {
        let book = make_book();
        let fee_schedule = FeeSchedule {
            taker_fee_bps: Decimal::from(200),
            maker_rebate_bps: Decimal::ZERO,
        };
        let model = FeeModel::new(&fee_schedule, Decimal::new(5, 3)); // 0.5% latency

        let cost = model.total_cost(&book, Side::Buy, Decimal::from(50), Decimal::new(57, 2));
        assert!(cost.taker_fee > Decimal::ZERO);
        assert_eq!(cost.taker_fee, Decimal::new(2, 2)); // flat 2%
        assert!(cost.latency_penalty > Decimal::ZERO);
        assert!(cost.total > Decimal::ZERO);
    }

    #[test]
    fn test_fee_regime_dynamic_at_midpoint() {
        // At p=0.50: fee = 0.25 × (0.50 × 0.50)^2 = 0.25 × 0.0625 = 0.015625
        let regime = FeeRegime::Dynamic {
            fee_rate: Decimal::new(25, 2),
            exponent: 2,
        };
        let fee = regime.fee_per_share(Decimal::new(50, 2));
        assert_eq!(fee, Decimal::new(15625, 6)); // 0.015625
    }

    #[test]
    fn test_fee_regime_dynamic_at_extreme_low() {
        // At p=0.10: fee = 0.25 × (0.10 × 0.90)^2 = 0.25 × 0.09^2 = 0.25 × 0.0081 = 0.002025
        let regime = FeeRegime::Dynamic {
            fee_rate: Decimal::new(25, 2),
            exponent: 2,
        };
        let fee = regime.fee_per_share(Decimal::new(10, 2));
        assert_eq!(fee, Decimal::new(2025, 6)); // 0.002025
    }

    #[test]
    fn test_fee_regime_dynamic_at_extreme_high() {
        // At p=0.90: fee = 0.25 × (0.90 × 0.10)^2 = same as p=0.10 by symmetry
        let regime = FeeRegime::Dynamic {
            fee_rate: Decimal::new(25, 2),
            exponent: 2,
        };
        let fee = regime.fee_per_share(Decimal::new(90, 2));
        assert_eq!(fee, Decimal::new(2025, 6)); // 0.002025
    }

    #[test]
    fn test_fee_regime_dynamic_at_boundary() {
        // At p=0.0 or p=1.0: fee = 0.25 × 0^2 = 0
        let regime = FeeRegime::Dynamic {
            fee_rate: Decimal::new(25, 2),
            exponent: 2,
        };
        assert_eq!(regime.fee_per_share(Decimal::ZERO), Decimal::ZERO);
        assert_eq!(regime.fee_per_share(Decimal::ONE), Decimal::ZERO);
    }

    #[test]
    fn test_trading_cost_dynamic() {
        let book = make_book();
        let model = FeeModel::with_dynamic(
            Decimal::new(25, 2), // 0.25
            2,
            Decimal::new(5, 3), // 0.5% latency
        );

        // Buy at best ask = 0.57
        // fee = 0.25 × (0.57 × 0.43)^2 = 0.25 × 0.2451^2 = 0.25 × 0.06007401 ≈ 0.01501850
        let cost = model.total_cost(&book, Side::Buy, Decimal::from(50), Decimal::new(57, 2));
        assert!(cost.taker_fee > Decimal::ZERO);
        assert!(cost.taker_fee < Decimal::new(2, 2)); // less than flat 2%
        assert!(cost.total > Decimal::ZERO);
    }
}
