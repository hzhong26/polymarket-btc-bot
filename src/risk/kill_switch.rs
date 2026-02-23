use std::time::Duration;

use tracing::{error, warn};

/// Kill-switch that triggers on WebSocket disconnection or other critical events.
/// Sends a cancel-all signal and halts the bot.
pub struct KillSwitch {
    /// Action to take on trigger ("cancel-all" or "halt")
    action: String,
    /// Whether the kill switch has been triggered
    triggered: bool,
}

impl KillSwitch {
    pub fn new(action: &str) -> Self {
        Self {
            action: action.to_string(),
            triggered: false,
        }
    }

    /// Trigger the kill switch.
    /// Returns true if this is the first trigger (action should be taken).
    pub fn trigger(&mut self, reason: &str) -> bool {
        if self.triggered {
            return false;
        }
        error!(reason, action = %self.action, "KILL SWITCH TRIGGERED");
        self.triggered = true;
        true
    }

    /// Check if the kill switch has been triggered.
    pub fn is_triggered(&self) -> bool {
        self.triggered
    }

    /// Reset the kill switch (e.g. after successful reconnection).
    pub fn reset(&mut self) {
        if self.triggered {
            warn!("kill switch reset");
            self.triggered = false;
        }
    }

    /// Get the configured action.
    pub fn action(&self) -> &str {
        &self.action
    }
}

/// WebSocket health monitor.
/// Tracks the last successful message time and triggers the kill switch
/// if no message is received within the timeout.
pub struct WsHealthMonitor {
    last_message: std::time::Instant,
    timeout: Duration,
}

impl WsHealthMonitor {
    pub fn new(timeout: Duration) -> Self {
        Self {
            last_message: std::time::Instant::now(),
            timeout,
        }
    }

    /// Record that a message was received.
    pub fn heartbeat(&mut self) {
        self.last_message = std::time::Instant::now();
    }

    /// Check if the WebSocket connection is considered dead.
    pub fn is_dead(&self) -> bool {
        self.last_message.elapsed() > self.timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kill_switch_trigger_once() {
        let mut ks = KillSwitch::new("cancel-all");
        assert!(ks.trigger("test"));
        assert!(!ks.trigger("test")); // Second trigger returns false
        assert!(ks.is_triggered());
    }

    #[test]
    fn test_kill_switch_reset() {
        let mut ks = KillSwitch::new("cancel-all");
        ks.trigger("test");
        ks.reset();
        assert!(!ks.is_triggered());
        assert!(ks.trigger("test again")); // Can trigger again after reset
    }
}
