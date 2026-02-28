use std::collections::VecDeque;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Calibration tracker using Brier score and log-loss.
///
/// After each market resolves, we record (predicted_probability, actual_outcome)
/// and compute rolling calibration metrics. If calibration degrades, the bot
/// should widen its edge threshold or pause trading.
pub struct CalibrationTracker {
    /// Rolling window of (prediction, outcome) pairs
    history: VecDeque<CalibrationEntry>,
    /// Maximum history size
    max_history: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationEntry {
    pub timestamp: DateTime<Utc>,
    pub predicted_prob: f64,
    pub actual_outcome: bool, // true if event occurred (e.g. BTC > strike)
    pub market_question: String,
}

#[derive(Debug, Clone)]
pub struct CalibrationReport {
    pub brier_score: f64,
    pub log_loss: f64,
    pub sample_count: usize,
    /// Model uncertainty derived from calibration quality.
    /// 0.0 = perfectly calibrated, approaches 1.0 as calibration degrades.
    pub model_uncertainty: f64,
    /// Reliability bins over predicted probabilities.
    pub reliability_bins: Vec<ReliabilityBin>,
}

#[derive(Debug, Clone)]
pub struct ReliabilityBin {
    pub lower: f64,
    pub upper: f64,
    pub count: usize,
    pub avg_predicted: f64,
    pub empirical_rate: f64,
}

impl CalibrationTracker {
    pub fn new(max_history: usize) -> Self {
        Self {
            history: VecDeque::with_capacity(max_history),
            max_history,
        }
    }

    /// Record a resolved market outcome.
    pub fn record(&mut self, predicted_prob: f64, actual_outcome: bool, market_question: String) {
        let entry = CalibrationEntry {
            timestamp: Utc::now(),
            predicted_prob: predicted_prob.clamp(0.001, 0.999), // avoid log(0)
            actual_outcome,
            market_question,
        };

        self.history.push_back(entry);
        if self.history.len() > self.max_history {
            self.history.pop_front();
        }
    }

    /// Compute current calibration metrics.
    /// Returns None if insufficient data.
    pub fn report(&self) -> Option<CalibrationReport> {
        if self.history.len() < 5 {
            return None;
        }

        let n = self.history.len() as f64;
        let mut brier_sum = 0.0;
        let mut log_loss_sum = 0.0;

        for entry in &self.history {
            let outcome = if entry.actual_outcome { 1.0 } else { 0.0 };
            let p = entry.predicted_prob;

            // Brier score: mean squared error of probability forecasts
            // BS = (1/N) * Σ (p_i - o_i)²
            brier_sum += (p - outcome).powi(2);

            // Log-loss: -(1/N) * Σ [o_i * ln(p_i) + (1 - o_i) * ln(1 - p_i)]
            log_loss_sum += -(outcome * p.ln() + (1.0 - outcome) * (1.0 - p).ln());
        }

        let brier_score = brier_sum / n;
        let log_loss = log_loss_sum / n;

        // Model uncertainty: map Brier score to [0, 1]
        // Perfect = 0.0 Brier → 0.0 uncertainty
        // Random (0.25 Brier for binary) → ~0.5 uncertainty
        // Inverse (0.5+ Brier) → high uncertainty
        let model_uncertainty = (brier_score * 2.0).min(1.0);

        Some(CalibrationReport {
            brier_score,
            log_loss,
            sample_count: self.history.len(),
            model_uncertainty,
            reliability_bins: self.reliability_bins(10),
        })
    }

    /// Get the current model uncertainty.
    /// Returns a default moderate uncertainty if not enough data.
    pub fn model_uncertainty(&self) -> f64 {
        self.report().map(|r| r.model_uncertainty).unwrap_or(0.3) // Default: moderate uncertainty when uncalibrated
    }

    /// Get model uncertainty with warmup awareness.
    /// Returns a low default when fewer than `min_samples` settled outcomes exist,
    /// allowing trades to flow through the Kelly gate while calibration data accumulates.
    pub fn model_uncertainty_warmup(&self, min_samples: usize) -> f64 {
        if self.history.len() < min_samples {
            return 0.30; // Conservative default during warmup — halve Kelly sizing until calibrated
        }
        self.report().map(|r| r.model_uncertainty).unwrap_or(0.30)
    }

    /// Save calibration history to a JSON file.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let entries: Vec<&CalibrationEntry> = self.history.iter().collect();
        let json = serde_json::to_string_pretty(&entries)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Load calibration history from a JSON file.
    /// Returns a new tracker populated with the persisted entries.
    pub fn load(path: &Path, max_history: usize) -> anyhow::Result<Self> {
        let data = std::fs::read_to_string(path)?;
        let entries: Vec<CalibrationEntry> = serde_json::from_str(&data)?;
        let len = entries.len();
        let mut history = VecDeque::with_capacity(max_history);
        // If persisted data exceeds max_history, keep only the most recent entries.
        let skip = len.saturating_sub(max_history);
        for entry in entries.into_iter().skip(skip) {
            history.push_back(entry);
        }
        info!(
            path = %path.display(),
            loaded_samples = len,
            retained = history.len(),
            "loaded calibration history"
        );
        Ok(Self {
            history,
            max_history,
        })
    }

    /// Check if calibration has degraded beyond acceptable threshold.
    pub fn is_degraded(&self, max_brier: f64) -> bool {
        self.report()
            .map(|r| r.brier_score > max_brier)
            .unwrap_or(false)
    }

    /// Number of recorded outcomes.
    pub fn sample_count(&self) -> usize {
        self.history.len()
    }

    fn reliability_bins(&self, bin_count: usize) -> Vec<ReliabilityBin> {
        if bin_count == 0 {
            return Vec::new();
        }
        #[derive(Default, Clone)]
        struct BinAcc {
            count: usize,
            pred_sum: f64,
            outcome_sum: f64,
        }

        let mut bins = vec![BinAcc::default(); bin_count];
        for entry in &self.history {
            let mut idx = (entry.predicted_prob * bin_count as f64).floor() as usize;
            if idx >= bin_count {
                idx = bin_count - 1;
            }
            bins[idx].count += 1;
            bins[idx].pred_sum += entry.predicted_prob;
            bins[idx].outcome_sum += if entry.actual_outcome { 1.0 } else { 0.0 };
        }

        bins.into_iter()
            .enumerate()
            .map(|(i, acc)| {
                let lower = i as f64 / bin_count as f64;
                let upper = (i + 1) as f64 / bin_count as f64;
                if acc.count == 0 {
                    return ReliabilityBin {
                        lower,
                        upper,
                        count: 0,
                        avg_predicted: 0.0,
                        empirical_rate: 0.0,
                    };
                }
                ReliabilityBin {
                    lower,
                    upper,
                    count: acc.count,
                    avg_predicted: acc.pred_sum / acc.count as f64,
                    empirical_rate: acc.outcome_sum / acc.count as f64,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tracker() {
        let tracker = CalibrationTracker::new(100);
        assert!(tracker.report().is_none());
        assert_eq!(tracker.model_uncertainty(), 0.3);
    }

    #[test]
    fn test_perfect_calibration() {
        let mut tracker = CalibrationTracker::new(100);
        // Perfect predictions: high confidence when correct
        for _ in 0..10 {
            tracker.record(0.95, true, "test".into());
            tracker.record(0.05, false, "test".into());
        }
        let report = tracker.report().unwrap();
        assert!(report.brier_score < 0.01);
        assert!(report.model_uncertainty < 0.05);
    }

    #[test]
    fn test_random_calibration() {
        let mut tracker = CalibrationTracker::new(100);
        // Predict 50% for everything → Brier should be 0.25
        for i in 0..20 {
            tracker.record(0.5, i % 2 == 0, "test".into());
        }
        let report = tracker.report().unwrap();
        assert!((report.brier_score - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_terrible_calibration() {
        let mut tracker = CalibrationTracker::new(100);
        // Predict opposite of what happens
        for _ in 0..10 {
            tracker.record(0.95, false, "test".into());
            tracker.record(0.05, true, "test".into());
        }
        let report = tracker.report().unwrap();
        assert!(report.brier_score > 0.8);
        assert!(report.model_uncertainty > 0.9);
    }

    #[test]
    fn test_is_degraded() {
        let mut tracker = CalibrationTracker::new(100);
        for _ in 0..10 {
            tracker.record(0.95, true, "test".into());
        }
        assert!(!tracker.is_degraded(0.25)); // Good calibration
    }

    #[test]
    fn test_window_eviction() {
        let mut tracker = CalibrationTracker::new(5);
        for _ in 0..10 {
            tracker.record(0.5, true, "test".into());
        }
        assert_eq!(tracker.sample_count(), 5);
    }

    #[test]
    fn test_warmup_returns_conservative_default() {
        let mut tracker = CalibrationTracker::new(100);
        // With < 30 samples, warmup should return 0.30 (conservative)
        for _ in 0..10 {
            tracker.record(0.5, true, "test".into());
        }
        assert_eq!(tracker.model_uncertainty_warmup(30), 0.30);
    }

    #[test]
    fn test_warmup_uses_real_uncertainty_after_threshold() {
        let mut tracker = CalibrationTracker::new(100);
        // Perfect predictions — real uncertainty should be very low
        for _ in 0..30 {
            tracker.record(0.95, true, "test".into());
            tracker.record(0.05, false, "test".into());
        }
        let warmup_val = tracker.model_uncertainty_warmup(30);
        // Should use real Brier-based value, not the warmup default
        assert!(warmup_val < 0.1, "expected real uncertainty < 0.1, got {warmup_val}");
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cal.json");

        let mut tracker = CalibrationTracker::new(100);
        for i in 0..10 {
            tracker.record(0.5 + (i as f64 * 0.03), i % 2 == 0, format!("q{i}"));
        }

        tracker.save(&path).unwrap();
        let loaded = CalibrationTracker::load(&path, 100).unwrap();
        assert_eq!(loaded.sample_count(), 10);

        // Brier scores should match
        let orig_report = tracker.report().unwrap();
        let loaded_report = loaded.report().unwrap();
        assert!((orig_report.brier_score - loaded_report.brier_score).abs() < 1e-10);
    }

    #[test]
    fn test_load_respects_max_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cal.json");

        let mut tracker = CalibrationTracker::new(100);
        for i in 0..50 {
            tracker.record(0.5, i % 2 == 0, "test".into());
        }
        tracker.save(&path).unwrap();

        // Load with smaller max_history — should keep only the most recent 20
        let loaded = CalibrationTracker::load(&path, 20).unwrap();
        assert_eq!(loaded.sample_count(), 20);
    }

    #[test]
    fn test_load_missing_file_returns_error() {
        let result = CalibrationTracker::load(Path::new("/nonexistent/path.json"), 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_reliability_bins_cover_all_samples() {
        let mut tracker = CalibrationTracker::new(100);
        for i in 0..20 {
            tracker.record(i as f64 / 20.0, i % 2 == 0, "test".into());
        }
        let report = tracker.report().unwrap();
        assert_eq!(report.reliability_bins.len(), 10);
        let total: usize = report.reliability_bins.iter().map(|b| b.count).sum();
        assert_eq!(total, report.sample_count);
    }
}
