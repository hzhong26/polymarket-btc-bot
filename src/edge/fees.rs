use rust_decimal::Decimal;

use crate::venue::{FeeSchedule, OrderBook, Side};

/// Fee and slippage model for executable edge calculation.
pub struct FeeModel {
    /// Taker fee from the venue's fee schedule
    pub taker_fee_rate: Decimal,
    /// Latency penalty as a fraction of price (e.g. 0.005 = 0.5%)
    pub latency_penalty: Decimal,
}

impl FeeModel {
    pub fn new(fee_schedule: &FeeSchedule, latency_penalty: Decimal) -> Self {
        Self {
            taker_fee_rate: fee_schedule.taker_fee_rate(),
            latency_penalty,
        }
    }

    /// Compute the total trading cost (fee + slippage + latency penalty)
    /// for a given order on the given side of the book.
    ///
    /// Returns the cost as a fraction of the order amount (e.g. 0.03 = 3%).
    pub fn total_cost(&self, book: &OrderBook, side: Side, order_size: Decimal) -> TradingCost {
        let slippage = match side {
            Side::Buy => book
                .estimate_buy_slippage(order_size)
                .unwrap_or(Decimal::ZERO),
            Side::Sell => book
                .estimate_sell_slippage(order_size)
                .unwrap_or(Decimal::ZERO),
        };

        TradingCost {
            taker_fee: self.taker_fee_rate,
            slippage,
            latency_penalty: self.latency_penalty,
            total: self.taker_fee_rate + slippage + self.latency_penalty,
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
    fn test_trading_cost() {
        let book = make_book();
        let fee_schedule = FeeSchedule {
            taker_fee_bps: Decimal::from(200),
            maker_rebate_bps: Decimal::ZERO,
        };
        let model = FeeModel::new(&fee_schedule, Decimal::new(5, 3)); // 0.5% latency

        let cost = model.total_cost(&book, Side::Buy, Decimal::from(50));
        assert!(cost.taker_fee > Decimal::ZERO);
        assert!(cost.latency_penalty > Decimal::ZERO);
        assert!(cost.total > Decimal::ZERO);
    }
}
