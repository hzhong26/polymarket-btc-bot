use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::venue::{OrderBook, OrderEvent, OrderId, OrderRequest, OrderType, Side, VenueClient};

use super::ExecutorError;

/// Tracks an active order through its lifecycle.
#[derive(Debug, Clone)]
pub struct TrackedOrder {
    pub order_id: String,
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub order_type: OrderType,
    pub submitted_at: DateTime<Utc>,
    pub status: OrderStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderStatus {
    Pending,
    Live,
    PartiallyFilled { filled: Decimal },
    Filled,
    Cancelled,
    Rejected(String),
}

#[derive(Debug, Clone)]
pub struct OrderFillUpdate {
    pub order_id: String,
    pub fill_price: Decimal,
    pub fill_amount: Decimal,
    pub is_terminal: bool,
}

/// Manages the order lifecycle: creation, submission, tracking.
/// Enforces book constraints (tick_size, min_order_size, neg_risk).
pub struct OrderManager {
    open_orders: HashMap<String, TrackedOrder>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            open_orders: HashMap::new(),
        }
    }

    /// Place a limit order (GTC) on the venue.
    /// Validates against book constraints before submission.
    pub async fn place_limit_order(
        &mut self,
        venue: &dyn VenueClient,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        book: &OrderBook,
    ) -> Result<String, ExecutorError> {
        // Enforce book constraints
        self.validate_order(price, size, book)?;

        // Round price to tick size
        let rounded_price = round_to_tick(price, book.tick_size);

        let order = OrderRequest {
            token_id: token_id.to_string(),
            side,
            price: rounded_price,
            size,
            order_type: OrderType::Gtc,
        };

        let order_id = venue.place_order(&order).await?;

        let tracked = TrackedOrder {
            order_id: order_id.0.clone(),
            token_id: token_id.to_string(),
            side,
            price: rounded_price,
            size,
            order_type: OrderType::Gtc,
            submitted_at: Utc::now(),
            status: OrderStatus::Pending,
        };

        info!(
            order_id = %order_id.0,
            token_id,
            side = ?side,
            price = %rounded_price,
            size = %size,
            "order placed"
        );

        self.open_orders.insert(order_id.0.clone(), tracked);
        Ok(order_id.0)
    }

    /// Cancel a specific order.
    pub async fn cancel_order(
        &mut self,
        venue: &dyn VenueClient,
        order_id: &str,
    ) -> Result<(), ExecutorError> {
        venue.cancel_order(&OrderId(order_id.to_string())).await?;

        if let Some(order) = self.open_orders.get_mut(order_id) {
            order.status = OrderStatus::Cancelled;
        }

        info!(order_id, "order cancelled");
        Ok(())
    }

    /// Cancel all open orders (e.g. on shutdown or risk breach).
    pub async fn cancel_all(&mut self, venue: &dyn VenueClient) -> Result<(), ExecutorError> {
        venue.cancel_all().await?;

        for order in self.open_orders.values_mut() {
            order.status = OrderStatus::Cancelled;
        }

        info!(count = self.open_orders.len(), "all orders cancelled");
        self.open_orders
            .retain(|_, o| o.status != OrderStatus::Cancelled);
        Ok(())
    }

    /// Update order status from a WebSocket event.
    pub fn handle_event(&mut self, event: &OrderEvent) -> Option<OrderFillUpdate> {
        match event {
            OrderEvent::Filled {
                order_id,
                fill_price,
                fill_amount,
            } => {
                if let Some(order) = self.open_orders.get_mut(order_id) {
                    order.status = OrderStatus::Filled;
                    info!(
                        order_id,
                        %fill_price,
                        %fill_amount,
                        "order filled"
                    );
                }
                Some(OrderFillUpdate {
                    order_id: order_id.clone(),
                    fill_price: *fill_price,
                    fill_amount: *fill_amount,
                    is_terminal: true,
                })
            }
            OrderEvent::PartialFill {
                order_id,
                fill_price,
                fill_amount,
                ..
            } => {
                if let Some(order) = self.open_orders.get_mut(order_id) {
                    let current = match &order.status {
                        OrderStatus::PartiallyFilled { filled } => *filled,
                        _ => Decimal::ZERO,
                    };
                    order.status = OrderStatus::PartiallyFilled {
                        filled: current + fill_amount,
                    };
                }
                Some(OrderFillUpdate {
                    order_id: order_id.clone(),
                    fill_price: *fill_price,
                    fill_amount: *fill_amount,
                    is_terminal: false,
                })
            }
            OrderEvent::Cancelled { order_id } => {
                if let Some(order) = self.open_orders.get_mut(order_id) {
                    order.status = OrderStatus::Cancelled;
                }
                None
            }
            OrderEvent::Rejected { order_id, reason } => {
                if let Some(order) = self.open_orders.get_mut(order_id) {
                    order.status = OrderStatus::Rejected(reason.clone());
                    warn!(order_id, reason, "order rejected");
                }
                None
            }
        }
    }

    /// Get all currently open (non-terminal) orders.
    pub fn open_orders(&self) -> Vec<&TrackedOrder> {
        self.open_orders
            .values()
            .filter(|o| {
                matches!(
                    o.status,
                    OrderStatus::Pending | OrderStatus::Live | OrderStatus::PartiallyFilled { .. }
                )
            })
            .collect()
    }

    /// Total exposure across all open orders.
    pub fn total_exposure(&self) -> Decimal {
        self.open_orders().iter().map(|o| o.price * o.size).sum()
    }

    /// Validate an order against book constraints.
    fn validate_order(
        &self,
        price: Decimal,
        size: Decimal,
        book: &OrderBook,
    ) -> Result<(), ExecutorError> {
        if size < book.min_order_size {
            return Err(ExecutorError::BookConstraintViolation(format!(
                "order size {} below minimum {}",
                size, book.min_order_size
            )));
        }

        if price <= Decimal::ZERO || price >= Decimal::ONE {
            return Err(ExecutorError::BookConstraintViolation(format!(
                "price {} out of valid range (0, 1)",
                price
            )));
        }

        // Verify tick size alignment
        let ticks = price / book.tick_size;
        if ticks.fract() != Decimal::ZERO {
            return Err(ExecutorError::BookConstraintViolation(format!(
                "price {} not aligned to tick size {}",
                price, book.tick_size
            )));
        }

        Ok(())
    }
}

/// Round a price to the nearest valid tick size.
fn round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return price;
    }
    (price / tick_size).round() * tick_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_round_to_tick() {
        let tick = Decimal::new(1, 2); // 0.01
        assert_eq!(
            round_to_tick(Decimal::new(555, 3), tick),
            Decimal::new(56, 2)
        ); // 0.555 → 0.56
        assert_eq!(
            round_to_tick(Decimal::new(551, 3), tick),
            Decimal::new(55, 2)
        ); // 0.551 → 0.55
    }

    #[test]
    fn test_total_exposure_empty() {
        let mgr = OrderManager::new();
        assert_eq!(mgr.total_exposure(), Decimal::ZERO);
    }

    #[test]
    fn test_handle_event_returns_fill_update_for_partial_and_filled() {
        let mut mgr = OrderManager::new();
        mgr.open_orders.insert(
            "ord-1".into(),
            TrackedOrder {
                order_id: "ord-1".into(),
                token_id: "token-yes".into(),
                side: Side::Buy,
                price: Decimal::new(50, 2),
                size: Decimal::from(10),
                order_type: OrderType::Gtc,
                submitted_at: Utc::now(),
                status: OrderStatus::Pending,
            },
        );

        let partial = mgr.handle_event(&OrderEvent::PartialFill {
            order_id: "ord-1".into(),
            fill_price: Decimal::new(51, 2),
            fill_amount: Decimal::from(3),
        });
        let partial = partial.expect("expected partial fill update");
        assert_eq!(partial.order_id, "ord-1");
        assert!(!partial.is_terminal);

        let filled = mgr.handle_event(&OrderEvent::Filled {
            order_id: "ord-1".into(),
            fill_price: Decimal::new(52, 2),
            fill_amount: Decimal::from(7),
        });
        let filled = filled.expect("expected filled update");
        assert_eq!(filled.order_id, "ord-1");
        assert!(filled.is_terminal);
    }
}
