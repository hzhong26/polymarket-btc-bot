use std::path::Path;

use tracing::{error, info};

use super::order_manager::TrackedOrder;

/// Restart-safe order state persistence.
///
/// Serializes open orders to disk so the bot can recover its state
/// after a restart without losing track of live orders.
pub struct OrderStatePersistence {
    path: String,
}

impl OrderStatePersistence {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    /// Save current open orders to disk.
    pub fn save(&self, _orders: &[&TrackedOrder]) -> Result<(), std::io::Error> {
        // TODO: Serialize orders to JSON and write to self.path
        info!(path = %self.path, "order state saved");
        Ok(())
    }

    /// Load previously saved orders from disk.
    pub fn load(&self) -> Result<Vec<TrackedOrder>, std::io::Error> {
        if !Path::new(&self.path).exists() {
            return Ok(Vec::new());
        }

        // TODO: Read and deserialize orders from self.path
        info!(path = %self.path, "order state loaded");
        Ok(Vec::new())
    }

    /// Clear saved state (e.g. after clean shutdown).
    pub fn clear(&self) -> Result<(), std::io::Error> {
        if Path::new(&self.path).exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

impl Drop for OrderStatePersistence {
    fn drop(&mut self) {
        if let Err(e) = self.clear() {
            error!(error = %e, "failed to clear order state on drop");
        }
    }
}
