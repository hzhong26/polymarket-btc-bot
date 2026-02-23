use chrono::{DateTime, Datelike, Utc};

/// Clock health checks.
///
/// Validates that quotes are not stale and the system clock is reasonable.
pub struct ClockGuard {
    max_quote_age_ms: u64,
}

impl ClockGuard {
    pub fn new(max_quote_age_ms: u64) -> Self {
        Self { max_quote_age_ms }
    }

    /// Check if a quote timestamp is still fresh.
    pub fn is_fresh(&self, quote_time: DateTime<Utc>) -> bool {
        let age = Utc::now() - quote_time;
        age.num_milliseconds() >= 0 && (age.num_milliseconds() as u64) < self.max_quote_age_ms
    }

    /// Get the age of a quote in milliseconds.
    pub fn quote_age_ms(&self, quote_time: DateTime<Utc>) -> u64 {
        let age = Utc::now() - quote_time;
        age.num_milliseconds().max(0) as u64
    }

    /// Check if the system clock seems reasonable.
    /// Detects gross clock skew by comparing against expected bounds.
    pub fn clock_seems_reasonable(&self) -> bool {
        let now = Utc::now();
        // Sanity check: year should be reasonable
        now.year() >= 2024 && now.year() <= 2030
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_fresh_quote() {
        let guard = ClockGuard::new(5000);
        assert!(guard.is_fresh(Utc::now()));
    }

    #[test]
    fn test_stale_quote() {
        let guard = ClockGuard::new(5000);
        let old = Utc::now() - chrono::Duration::seconds(10);
        assert!(!guard.is_fresh(old));
    }

    #[test]
    fn test_clock_reasonable() {
        let guard = ClockGuard::new(5000);
        assert!(guard.clock_seems_reasonable());
    }
}
