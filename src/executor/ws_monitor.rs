use tracing::warn;

/// WebSocket fill monitor.
///
/// In production, this subscribes to the Polymarket WebSocket
/// USER channel for real-time fill/cancel/reject events.
/// Currently a placeholder — the venue client's subscribe_fills
/// method handles the WebSocket connection.
pub struct WebSocketMonitor;

impl WebSocketMonitor {
    pub fn new() -> Self {
        warn!("WebSocket monitor initialized (using venue client subscription)");
        Self
    }
}
