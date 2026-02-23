pub mod order_manager;
pub mod state;
pub mod ws_monitor;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("order rejected by CLOB: {0}")]
    OrderRejected(String),
    #[error("signing failed: {0}")]
    SigningFailed(String),
    #[error("order timed out: {order_id}")]
    Timeout { order_id: String },
    #[error("venue error: {0}")]
    Venue(#[from] crate::venue::VenueError),
    #[error("order violates book constraints: {0}")]
    BookConstraintViolation(String),
}
